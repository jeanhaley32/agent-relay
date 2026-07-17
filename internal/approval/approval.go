// Package approval implements a two-listener approval flow for high-risk
// actions: a loopback-only endpoint used internally to create requests, and
// a Tailscale-only endpoint that renders the approve/deny page. Because the
// approve page is bound to the tailnet interface, being able to load it is
// itself proof the caller is on the tailnet - a stronger identity signal
// than a Telegram chat_id alone, which is spoofable if that account is ever
// compromised.
package approval

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"sync"
	"time"
)

type status string

const (
	StatusPending  status = "pending"
	StatusApproved status = "approved"
	StatusDenied   status = "denied"
	StatusExpired  status = "expired"
)

type request struct {
	desc       string
	actionHash string // binds the token to a specific action; empty = unbound (legacy callers)
	created    time.Time
	expires    time.Time
	status     status
	consumed   bool // true once a caller has successfully Consume()'d an approved decision
}

// Manager tracks in-flight approval requests. Safe for concurrent use.
type Manager struct {
	mu          sync.Mutex
	pending     map[string]*request
	approveBase string // e.g. http://100.99.212.119:9212, the tailnet-only base URL
}

func NewManager(approveBase string) *Manager {
	return &Manager{pending: make(map[string]*request), approveBase: approveBase}
}

func newToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (m *Manager) gc() {
	now := time.Now()
	for tok, req := range m.pending {
		if req.status == StatusPending && now.After(req.expires) {
			req.status = StatusExpired
		}
		// Drop terminal requests after they've sat for a while so the map
		// doesn't grow unbounded.
		if req.status != StatusPending && now.Sub(req.expires) > 10*time.Minute {
			delete(m.pending, tok)
		}
	}
}

// Create makes a new pending request and returns its token and approval
// link, for callers that want to drive the approve/poll cycle directly
// in-process (rather than via the loopback HTTP API). The token is not bound
// to any specific action - use CreateBound for anything Consume() will later
// gate a real effect on (delete, send, etc).
func (m *Manager) Create(desc string, ttl time.Duration) (token, link string) {
	return m.create(desc, "", ttl)
}

// CreateBound makes a new pending request whose approval can only be
// consumed by a caller presenting the same actionHash - e.g. a hash of the
// exact action being authorized (message id + operation), so an approved
// token can't be replayed against a different or later-substituted action.
func (m *Manager) CreateBound(desc, actionHash string, ttl time.Duration) (token, link string) {
	return m.create(desc, actionHash, ttl)
}

// Status returns the current status of a token, or false if unknown. This is
// a read-only peek for display/polling purposes - it does not consume the
// decision, so a still-approved token remains valid until either Consume()'d
// or it expires. Callers that will perform a real effect based on the result
// must use Consume, not Status, or the same approval can be replayed for the
// whole TTL window.
func (m *Manager) Status(token string) (status, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gc()
	req, ok := m.pending[token]
	if !ok {
		return "", false
	}
	if req.status == StatusPending && time.Now().After(req.expires) {
		req.status = StatusExpired
	}
	return req.status, true
}

// Consume atomically checks that token is approved, unconsumed, and (if the
// token was created with CreateBound) that actionHash matches the hash it
// was bound to, then marks it consumed so no later call can succeed for the
// same token - one approval authorizes exactly one action, once. Pass "" for
// actionHash for a token created via the unbound Create.
func (m *Manager) Consume(token, actionHash string) (status, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gc()
	req, ok := m.pending[token]
	if !ok {
		return "", false
	}
	if req.status == StatusPending && time.Now().After(req.expires) {
		req.status = StatusExpired
	}
	if req.status != StatusApproved {
		return req.status, true
	}
	if req.consumed {
		return StatusExpired, true // already spent; treat as no-longer-valid to callers
	}
	if req.actionHash != "" && req.actionHash != actionHash {
		return StatusDenied, true // approved, but not for this action
	}
	req.consumed = true
	return StatusApproved, true
}

// create makes a new pending request and returns its token and approval link.
func (m *Manager) create(desc, actionHash string, ttl time.Duration) (token, link string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gc()
	token = newToken()
	m.pending[token] = &request{desc: desc, actionHash: actionHash, created: time.Now(), expires: time.Now().Add(ttl), status: StatusPending}
	link = fmt.Sprintf("%s/approve/%s", m.approveBase, token)
	return token, link
}

func (m *Manager) get(token string) (*request, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gc()
	req, ok := m.pending[token]
	if ok && req.status == StatusPending && time.Now().After(req.expires) {
		req.status = StatusExpired
	}
	return req, ok
}

// RequestHandler serves the loopback-only side: internal callers (relayd
// itself, or this agent via curl) POST a description and get back a token
// plus the tailnet-only link to send the human, then poll for the decision.
func (m *Manager) RequestHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/request", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		desc := r.FormValue("desc")
		if desc == "" {
			http.Error(w, "desc is required", http.StatusBadRequest)
			return
		}
		ttl := 10 * time.Minute
		token, link := m.create(desc, r.FormValue("action_hash"), ttl)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"token": token, "link": link})
	})
	mux.HandleFunc("/status/", func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Path[len("/status/"):]
		st, ok := m.Status(token)
		if !ok {
			http.Error(w, "unknown token", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": string(st)})
	})
	return mux
}

const pageStyle = `<style>
body{font-family:-apple-system,system-ui,sans-serif;background:#0b0f14;color:#e6e9ef;
	display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;padding:24px}
.card{max-width:420px;width:100%%;text-align:center}
h2{font-size:1.3rem;margin-bottom:.5rem}
p{color:#9aa4b2;margin-bottom:1.5rem;word-break:break-word}
form{display:flex;flex-direction:column;gap:14px}
button{font-size:1.2rem;padding:20px;border-radius:14px;border:none;font-weight:600;
	-webkit-tap-highlight-color:transparent}
button[value=approve]{background:#2e7d46;color:#fff}
button[value=deny]{background:#7d2e2e;color:#fff}
</style>`

const approvePage = pageStyle + `<html><body><div class="card">
<h2>Approval requested</h2>
<p>%s</p>
<form method="POST">
<button name="decision" value="approve">Approve</button>
<button name="decision" value="deny">Deny</button>
</form>
</div></body></html>`

const resultPage = pageStyle + `<html><body><div class="card"><h2>%s.</h2></div></body></html>`

// ApproveHandler serves the tailnet-only side: the human-facing page a
// request link points at. Bind this listener to the Tailscale interface
// address only, never 0.0.0.0 - reachability is the auth.
func (m *Manager) ApproveHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/approve/", func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Path[len("/approve/"):]

		if r.Method == http.MethodPost {
			// Decide atomically: re-check pending and write the decision
			// under the same lock acquisition, so a concurrent request for
			// the same token can never observe/overwrite a stale status.
			m.mu.Lock()
			m.gc()
			req, ok := m.pending[token]
			if !ok {
				m.mu.Unlock()
				http.Error(w, "unknown or already-used request", http.StatusNotFound)
				return
			}
			if req.status == StatusPending && !time.Now().After(req.expires) {
				if r.FormValue("decision") == "approve" {
					req.status = StatusApproved
				} else {
					req.status = StatusDenied
				}
			} else if req.status == StatusPending {
				req.status = StatusExpired
			}
			decided := req.status
			m.mu.Unlock()
			fmt.Fprintf(w, resultPage, html.EscapeString(string(decided)))
			return
		}

		req, ok := m.get(token)
		if !ok {
			http.Error(w, "unknown or already-used request", http.StatusNotFound)
			return
		}
		m.mu.Lock()
		st, desc := req.status, req.desc
		m.mu.Unlock()
		if st != StatusPending {
			fmt.Fprintf(w, resultPage, html.EscapeString(string(st)))
			return
		}
		fmt.Fprintf(w, approvePage, html.EscapeString(desc))
	})
	return mux
}
