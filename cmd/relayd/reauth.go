package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	claudebk "github.com/jeanhaley32/agent-relay/internal/endpoint/claude"
)

// reauthApprovalWindow bounds how long the daemon waits for an admin to
// /allow or /deny a force_reauth request before giving up. Kept strictly
// shorter than the shim's reauthTimeout so the shim gets a real verdict (or
// this explicit timeout) rather than its own generic "no confirmation" error.
const reauthApprovalWindow = 150 * time.Second

// reauthGate holds force_reauth requests awaiting an admin decision, keyed by
// the short id echoed in the /allow <id> / /deny <id> prompt. It is the bridge
// that lets the existing admin approval command surface gate a tool the model
// itself triggered: the model cannot revoke an admin session unilaterally: a
// human must resolve the pending request first. Safe for concurrent use.
type reauthGate struct {
	mu      sync.Mutex
	pending map[string]chan bool
}

func newReauthGate() *reauthGate { return &reauthGate{pending: map[string]chan bool{}} }

// register creates a pending request under id and returns the channel its
// verdict will arrive on. cancel must be called when the waiter gives up.
func (g *reauthGate) register(id string) chan bool {
	ch := make(chan bool, 1)
	g.mu.Lock()
	g.pending[id] = ch
	g.mu.Unlock()
	return ch
}

func (g *reauthGate) cancel(id string) {
	g.mu.Lock()
	delete(g.pending, id)
	g.mu.Unlock()
}

// decide resolves a pending request. It reports whether id matched a pending
// force_reauth request, so the shared /allow /deny handler can fall through to
// the Claude Code permission path when it does not.
func (g *reauthGate) decide(id string, allow bool) bool {
	g.mu.Lock()
	ch, ok := g.pending[id]
	if ok {
		delete(g.pending, id)
	}
	g.mu.Unlock()
	if !ok {
		return false
	}
	ch <- allow
	return true
}

// reauthReqID returns a short random id short enough to type after /allow.
func reauthReqID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("reauth: crypto/rand failed: %v", err))
	}
	return "ra-" + hex.EncodeToString(b)
}

// serveReauth services force_reauth tool calls: each is gated behind an admin
// approval, then (only if approved) revokes the target admin's session. Every
// request is handled in its own goroutine because handleReauth blocks for the
// whole approval window; a slow decision must not stall other requests.
func serveReauth(back *claudebk.Endpoint, gate *reauthGate, isGatedAdmin func(string) bool,
	revoke func(string), notify func(string) error, logger *log.Logger) {
	for req := range back.Reauths() {
		go func(req claudebk.ReauthRequest) {
			result, errText := handleReauth(req, gate, isGatedAdmin, revoke, notify, reauthApprovalWindow)
			if err := back.ReauthRespond(req.ReqID, result, errText); err != nil {
				logger.Printf("reauth respond: %v", err)
			}
		}(req)
	}
}

// handleReauth is the gated core: it validates the target is a gated admin,
// asks admins to approve via /allow, and revokes the target's session only on
// an explicit approval. It returns (result, errText) for the tool call. Pure
// enough to unit-test: all side effects (notify, revoke) are injected.
//
// Security properties, all provable in isolation:
//   - a non-admin target is refused before any approval is even requested, so
//     the tool can never re-auth (or probe) a non-admin identity;
//   - revoke fires only on a true verdict from the gate, so no approval (deny
//     or timeout) means the session is never touched.
func handleReauth(req claudebk.ReauthRequest, gate *reauthGate, isGatedAdmin func(string) bool,
	revoke func(string), notify func(string) error, window time.Duration) (result, errText string) {
	target := req.Target
	if target == "" {
		target = req.ChatID
	}
	if target == "" {
		return "", "force_reauth: no target admin given and no conversation to infer one from"
	}
	if !isGatedAdmin(target) {
		return "", fmt.Sprintf("force_reauth refused: %q is not a gated admin — this tool can only re-auth admin sessions", target)
	}

	id := reauthReqID()
	ch := gate.register(id)
	defer gate.cancel(id)

	if err := notify(fmt.Sprintf(
		"🔐 Claude wants to force a re-auth challenge for admin %s (revokes their session).\n\napprove: /allow %s   deny: /deny %s",
		target, id, id)); err != nil {
		return "", "force_reauth: could not reach any admin to approve this action: " + err.Error()
	}

	select {
	case allow := <-ch:
		if !allow {
			return "", "force_reauth denied by admin — target session untouched"
		}
		revoke(target)
		return fmt.Sprintf("re-auth challenge armed for admin %s — their next message will require tailnet re-approval", target), ""
	case <-time.After(window):
		return "", "force_reauth: no admin decision within the approval window — target session untouched"
	}
}
