// Package relay is the transport-neutral core: the symmetric Endpoint interface,
// the Message envelope that crosses it, and a Broker that wires a frontend to a
// backend through a control chain (slash commands + budget gate).
//
// Nothing here knows about Telegram, Claude Code, or Ollama — those are
// Endpoint implementations that live in their own packages. This keeps the core
// reusable: any pair of Endpoints can be brokered together.
package relay

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/approval"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/senderr"
	"github.com/jeanhaley32/agent-relay/internal/session"
)

// Role identifies who authored a Message.
type Role string

const (
	User      Role = "user"
	Assistant Role = "assistant"
	System    Role = "system"
)

// Message is the neutral envelope carried in either direction.
type Message struct {
	ConversationID string
	Role           Role
	Text           string
	Meta           map[string]string // sender id, chat id, model, severity, ...
}

// UserMsg and AssistantMsg are convenience constructors.
func UserMsg(conv, text string) Message {
	return Message{ConversationID: conv, Role: User, Text: text}
}
func AssistantMsg(conv, text string) Message {
	return Message{ConversationID: conv, Role: Assistant, Text: text}
}

// Endpoint is the single symmetric abstraction for BOTH sides of a
// conversation. A frontend (Telegram, CLI) and a backend (Claude, Ollama, echo)
// implement the same interface; the Broker does not care which is which.
type Endpoint interface {
	Name() string
	Recv() <-chan Message                      // messages originating at this endpoint
	Send(ctx context.Context, m Message) error // deliver a message to this endpoint
	Close() error
}

// Estimator approximates the token cost of a piece of text. The default is a
// rough chars/4 heuristic; swap in a real tokenizer later.
type Estimator func(text string) int

// DefaultEstimator is ~4 characters per token.
func DefaultEstimator(text string) int {
	n := len(text) / 4
	if n < 1 {
		n = 1
	}
	return n
}

// FrontendSendTimeout bounds a single synchronous Frontend.Send call from the
// backend-reply pump. It must stay shorter than the caller's own ack-wait
// timeout so the broker can't cancel later than the caller already gave up,
// which would let a send land after the model was told it failed.
const FrontendSendTimeout = 12 * time.Second

// Broker connects a Frontend to a Backend, intercepting slash commands and
// gating model turns through a budget Meter. It is the deterministic,
// zero-token machinery that decides whether (and where) a message reaches a
// model — exactly the event-queue/trigger layer.
type Broker struct {
	Frontend Endpoint
	Backend  Endpoint
	Commands *command.Registry
	Meter    *budget.Meter
	Estimate Estimator
	// OutboundAllowed, if set, gates backend (model) replies: a reply whose
	// target chat is not allowed is dropped, never delivered to the frontend.
	// This stops the model from messaging non-allowlisted chats even though the
	// allowlist only gates inbound. nil ⇒ no outbound gating.
	OutboundAllowed func(chatID string) bool

	// OnBackendReply, if set, is called for every reply the backend (model)
	// emits, AFTER the outbound gate passes and the reply is delivered to the
	// frontend - used to infer that a fired trigger was handled (a reply
	// promptly following it on that chat auto-resolves it). A reply dropped
	// by the gate, or one Send actually failed to deliver, is never reported
	// here - that's AckBackendReply's job. nil ⇒ no hook.
	OnBackendReply func(m Message)

	// AckBackendReply, if set, is called after every Frontend.Send attempt
	// (success or failure) for a backend reply, so whatever originated the
	// reply can learn the real outcome instead of it always looking like
	// "sent" regardless of what actually happened. nil ⇒ no hook.
	AckBackendReply func(m Message, sendErr error)

	// Session gate: if Session and Approval are both set, inbound messages
	// from any user_id (from_id) in SessionGatedUsers require an active,
	// non-idle-expired session before being processed, keyed on the
	// sender's permanent account id (not chat_id, which only equals
	// from_id by convention). An expired/missing session triggers a
	// tailnet re-auth challenge (via Approval) instead of processing the
	// message. nil Session ⇒ no gating.
	Session           *session.Manager
	Approval          *approval.Manager
	SessionGatedUsers map[string]bool
	SessionTTL        time.Duration // approval request validity window

	// Lockdown, when set, blocks every message from a non-admin sender
	// before it reaches slash commands or the model - only b.Commands.IsAdmin
	// senders get through. Admin-only to toggle (enforced by the /lockdown
	// command itself being Admin: true), affects only non-admins.
	Lockdown atomic.Bool

	// ConversationCaps optionally bounds cumulative estimated tokens for a
	// specific chat_id within a rolling ConversationCapWindow, independent
	// of and tighter than the global Meter budget - per-contact spend
	// limiting rather than relay-wide. Mutate only via SetCaps (capsMu
	// guarded) so config changes apply without a restart. nil map ⇒ no
	// explicit caps.
	ConversationCaps map[string]int64

	// DefaultConversationCap, if > 0, applies to any chat_id NOT explicitly
	// listed in ConversationCaps and not exempted by ConversationCapExempt -
	// a blanket per-individual cap rather than requiring every contact to
	// be added by hand. Same construction/SetCaps mutation rule as
	// ConversationCaps.
	DefaultConversationCap int64

	// ConversationCapExempt, if set, reports whether chatID should never be
	// capped regardless of ConversationCaps/DefaultConversationCap - wired
	// in cmd/relayd to exempt admins, since a blanket default cap is meant
	// for arbitrary allowlisted contacts, not the operator. nil ⇒ nothing
	// exempted (only relevant when DefaultConversationCap > 0, since an
	// explicit ConversationCaps entry is an explicit choice either way).
	ConversationCapExempt func(chatID string) bool

	// ConversationCapWindow is how long a conversation's usage counts
	// against its cap before rolling over to a fresh window - a rate limit,
	// not a lifetime ban. Zero ⇒ defaultConversationCapWindow.
	ConversationCapWindow time.Duration

	// clock is overridden in tests; nil ⇒ time.Now.
	clock func() time.Time

	capsMu sync.RWMutex // guards ConversationCaps/DefaultConversationCap after construction

	conversationUsedMu sync.Mutex
	conversationUsed   map[string]*conversationWindow

	// challengeMu guards challengeInFlight, which dedupes challengeSession
	// per userID: a gated user who sends several messages before re-auth
	// would otherwise get a fresh link and poller per message.
	challengeMu       sync.Mutex
	challengeInFlight map[string]bool
}

// SetCaps atomically replaces both ConversationCaps and
// DefaultConversationCap - the only way either is mutated after Run starts.
// Existing per-conversation usage/window state is left untouched, so a live
// cap change takes effect on the very next message without resetting
// anyone's accumulated usage.
func (b *Broker) SetCaps(caps map[string]int64, defaultCap int64) {
	b.capsMu.Lock()
	defer b.capsMu.Unlock()
	b.ConversationCaps = caps
	b.DefaultConversationCap = defaultCap
}

// capLimit returns the effective cap for chatID and whether it's capped at
// all: an explicit ConversationCaps entry always wins; otherwise
// DefaultConversationCap applies unless ConversationCapExempt(chatID).
func (b *Broker) capLimit(chatID string) (limit int64, capped bool) {
	b.capsMu.RLock()
	defer b.capsMu.RUnlock()
	if limit, ok := b.ConversationCaps[chatID]; ok {
		return limit, true
	}
	if b.DefaultConversationCap <= 0 {
		return 0, false
	}
	if b.ConversationCapExempt != nil && b.ConversationCapExempt(chatID) {
		return 0, false
	}
	return b.DefaultConversationCap, true
}

const defaultConversationCapWindow = 3 * time.Hour

type conversationWindow struct {
	tokens      int64
	windowStart time.Time
}

func (b *Broker) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}
	return time.Now()
}

func (b *Broker) capWindow() time.Duration {
	if b.ConversationCapWindow > 0 {
		return b.ConversationCapWindow
	}
	return defaultConversationCapWindow
}

// rollIfExpired resets chatID's window if ConversationCapWindow has elapsed
// since it started. Caller holds conversationUsedMu.
func (b *Broker) rollIfExpired(chatID string) {
	w, ok := b.conversationUsed[chatID]
	if !ok {
		return
	}
	if b.now().Sub(w.windowStart) >= b.capWindow() {
		w.tokens = 0
		w.windowStart = b.now()
	}
}

// conversationCapExceeded reports whether chatID has a configured cap and
// has already reached or passed it within the current window - checked
// BEFORE forwarding an inbound message to the backend, so a capped
// conversation stops consuming inference tokens entirely once it hits the
// limit, not just outbound send tokens. Rolls over to a fresh window first
// if ConversationCapWindow has elapsed, so this is a rate limit, not a
// lifetime ban.
func (b *Broker) conversationCapExceeded(chatID string) bool {
	limit, capped := b.capLimit(chatID)
	if !capped {
		return false
	}
	b.conversationUsedMu.Lock()
	defer b.conversationUsedMu.Unlock()
	b.rollIfExpired(chatID)
	w := b.conversationUsed[chatID]
	if w == nil {
		return false
	}
	return w.tokens >= limit
}

// conversationCapRejectionNotice builds the message sent to a conversation
// every time a message is rejected for being at/over its cap. Every
// rejection (not just the one that first crosses the cap) explains why, the
// limit, current usage, and time until the window resets, rather than the
// conversation just going silent.
func (b *Broker) conversationCapRejectionNotice(chatID string) string {
	limit, _ := b.capLimit(chatID)
	b.conversationUsedMu.Lock()
	w := b.conversationUsed[chatID]
	var used int64
	var resetIn time.Duration
	if w != nil {
		used = w.tokens
		resetIn = b.capWindow() - b.now().Sub(w.windowStart)
		if resetIn < 0 {
			resetIn = 0
		}
	}
	b.conversationUsedMu.Unlock()
	return fmt.Sprintf(
		"This conversation is over its token budget and this message wasn't processed. "+
			"Limit: %d tokens per %s. Used: %d. Resets in: %s.",
		limit, b.capWindow().Round(time.Minute), used, resetIn.Round(time.Minute))
}

// addConversationUsage adds tokens to chatID's running total for the
// current window, a no-op if chatID has no configured cap (avoids growing
// the map for every conversation when caps are rarely configured).
func (b *Broker) addConversationUsage(chatID string, tokens int) {
	if _, capped := b.capLimit(chatID); !capped {
		return
	}
	b.conversationUsedMu.Lock()
	defer b.conversationUsedMu.Unlock()
	if b.conversationUsed == nil {
		b.conversationUsed = map[string]*conversationWindow{}
	}
	b.rollIfExpired(chatID)
	w := b.conversationUsed[chatID]
	if w == nil {
		w = &conversationWindow{windowStart: b.now()}
		b.conversationUsed[chatID] = w
	}
	w.tokens += int64(tokens)
}

// SetConversationUsage overwrites (does not add to) chatID's usage for the
// current window with an authoritative real number, replacing the interim
// chars/4 text-length estimate (which only sees reply text, not
// reasoning/tool-call tokens) once real per-conversation usage is known.
// A no-op if chatID has no configured cap. Rolls the window first (same as
// every other accessor) so a call arriving just after a rollover doesn't
// stomp on a legitimately-fresh window with a stale pre-rollover number.
func (b *Broker) SetConversationUsage(chatID string, tokens int64) {
	if _, capped := b.capLimit(chatID); !capped {
		return
	}
	b.conversationUsedMu.Lock()
	defer b.conversationUsedMu.Unlock()
	if b.conversationUsed == nil {
		b.conversationUsed = map[string]*conversationWindow{}
	}
	b.rollIfExpired(chatID)
	w := b.conversationUsed[chatID]
	if w == nil {
		w = &conversationWindow{windowStart: b.now()}
		b.conversationUsed[chatID] = w
	}
	w.tokens = tokens
}

// LockdownMessage is sent to any non-admin sender while lockdown is active.
const LockdownMessage = "This assistant is currently in lockdown - only admins can send messages right now. Try again later."

// challengeSession sends a fresh tailnet re-auth link to the conversation
// and, in the background, activates userID's session once approved. A no-op
// if a challenge for userID is already in flight, so a gated user sending
// several messages before re-authing gets one link and one poller, not N.
func (b *Broker) challengeSession(ctx context.Context, conv, userID string) {
	b.challengeMu.Lock()
	if b.challengeInFlight == nil {
		b.challengeInFlight = map[string]bool{}
	}
	if b.challengeInFlight[userID] {
		b.challengeMu.Unlock()
		return
	}
	b.challengeInFlight[userID] = true
	b.challengeMu.Unlock()

	clearInFlight := func() {
		b.challengeMu.Lock()
		delete(b.challengeInFlight, userID)
		b.challengeMu.Unlock()
	}

	ttl := b.SessionTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	// Bind the token to this specific userID so an approved decision can
	// only ever activate the session it was issued for, and Consume (not
	// Status) so the same approval can't be replayed to re-activate the
	// session again later within the TTL window.
	actionHash := "session-reauth:" + userID
	token, link := b.Approval.CreateBound("relayd session re-authentication", actionHash, ttl)
	_ = b.Frontend.Send(ctx, AssistantMsg(conv,
		"Session expired - re-authenticate over the tailnet, then resend your message: "+link))

	go func() {
		defer clearInFlight()
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		deadline := time.Now().Add(ttl)
		for time.Now().Before(deadline) {
			<-ticker.C
			st, ok := b.Approval.Consume(token, actionHash)
			if !ok {
				return
			}
			switch st {
			case approval.StatusApproved:
				b.Session.Activate(userID)
				_ = b.Frontend.Send(ctx, AssistantMsg(conv, "Session re-authenticated - go ahead and resend."))
				return
			case approval.StatusDenied, approval.StatusExpired:
				return
			}
		}
	}()
}

// Run pumps both directions until the frontend's Recv channel closes. Backend
// replies are metered (Record) and forwarded to the frontend; frontend messages
// are screened by commands then the budget gate before reaching the backend.
func (b *Broker) Run(ctx context.Context) error {
	if b.Estimate == nil {
		b.Estimate = DefaultEstimator
	}

	// Backend -> frontend: meter the reply cost, then deliver. Tracked by wg so
	// Run can drain in-flight replies before returning (avoids an exit race
	// where the process quits before async replies are flushed).
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for m := range b.Backend.Recv() {
			estimate := b.Estimate(m.Text)
			b.Meter.Record(estimate)
			b.addConversationUsage(m.Meta["chat_id"], estimate)
			// Outbound gate: the model may target any chat via its reply tool;
			// only deliver to allowed chats.
			if b.OutboundAllowed != nil && !b.OutboundAllowed(m.Meta["chat_id"]) {
				if b.AckBackendReply != nil {
					b.AckBackendReply(m, senderr.Permanent{Err: fmt.Errorf("chat_id %q is not an allowed destination", m.Meta["chat_id"])})
				}
				continue // dropped (the gate func is responsible for logging)
			}
			sendCtx, cancel := context.WithTimeout(ctx, FrontendSendTimeout)
			sendErr := b.Frontend.Send(sendCtx, m)
			cancel()
			if b.AckBackendReply != nil {
				b.AckBackendReply(m, sendErr)
			}
			if sendErr == nil && b.OnBackendReply != nil {
				b.OnBackendReply(m)
			}
		}
	}()

	// Once the frontend closes, shut the backend so its Recv drains and the
	// pump goroutine exits; then wait for it before returning.
	defer func() {
		_ = b.Backend.Close()
		wg.Wait()
	}()

	// Frontend -> backend: commands and budget gate first.
	for m := range b.Frontend.Recv() {
		if m.Role != User {
			continue
		}
		// -2. Identity-pair invariant: chat_id must equal from_id whenever
		// both are present. This holds by construction for every private
		// 1:1 conversation on every current frontend (a Telegram private
		// chat's id IS the sender's own id; Discord DMs synthesize chat_id
		// = from_id for the same reason) - so for those it should never
		// actually fire, which is what makes it a good tripwire there: a hit
		// means some upstream assumption broke.
		//
		// Discord guild (multi-party) channels are a real, deliberate
		// exception: chat_id there is the shared
		// channel id, necessarily distinct from whichever member's from_id
		// sent a given message. Messages that declare guild_id in Meta are
		// understood to be multi-party, so the invariant is enforced only
		// for messages that don't declare themselves multi-party - a
		// frontend-aware exception keyed off the message itself, not a
		// blanket carve-out.
		if fromID, chatID := m.Meta["from_id"], m.Meta["chat_id"]; fromID != "" && chatID != "" && fromID != chatID && m.Meta["guild_id"] == "" {
			log.Printf("relay: dropped message with mismatched from_id=%q chat_id=%q - identity-pair invariant violated", fromID, chatID)
			continue
		}
		// -1. Lockdown: non-admin senders are blocked entirely while active.
		if b.Lockdown.Load() {
			isAdmin := b.Commands != nil && b.Commands.IsAdmin != nil && b.Commands.IsAdmin(m.Meta["from_id"])
			if !isAdmin {
				_ = b.Frontend.Send(ctx, AssistantMsg(m.ConversationID, LockdownMessage))
				continue
			}
		}
		// 0. Session gate: guarded user must have an active, non-idle
		// session before anything else runs, including slash commands.
		// Keyed on from_id (see SessionGatedUsers doc comment above).
		if b.Session != nil && b.Approval != nil && b.SessionGatedUsers[m.Meta["from_id"]] {
			if !b.Session.Active(m.Meta["from_id"]) {
				b.challengeSession(ctx, m.ConversationID, m.Meta["from_id"])
				continue
			}
			b.Session.Touch(m.Meta["from_id"])
		}
		// 1. Escaped command (`\/…`): strip the backslash and send the literal
		// "/…" to the model instead of intercepting it as a relay command.
		if unescaped, esc := command.Escaped(m.Text); esc {
			m.Text = unescaped
		} else {
			// Slash commands are handled locally — never hit the model. Sender
			// identity is threaded through so handlers can gate (e.g. admin-only).
			cctx := command.Context{SenderID: m.Meta["from_id"], ChatID: m.Meta["chat_id"]}
			if reply, handled := b.Commands.Dispatch(cctx, m.Text); handled {
				_ = b.Frontend.Send(ctx, AssistantMsg(m.ConversationID, reply))
				continue
			}
		}
		// 2. Budget / circuit gate.
		if ok, why := b.Meter.Allow(b.Estimate(m.Text)); !ok {
			_ = b.Frontend.Send(ctx, AssistantMsg(m.ConversationID, why))
			continue
		}
		// 2.5. Per-conversation token cap (see ConversationCaps' doc comment).
		// Charge-then-reject, not check-only: the message is charged via
		// addConversationUsage below before the re-check rejects it, so a
		// single huge message can't slip through uncounted and dodge the cap.
		if b.conversationCapExceeded(m.Meta["chat_id"]) {
			_ = b.Frontend.Send(ctx, AssistantMsg(m.ConversationID, b.conversationCapRejectionNotice(m.Meta["chat_id"])))
			continue
		}
		b.addConversationUsage(m.Meta["chat_id"], b.Estimate(m.Text))
		if b.conversationCapExceeded(m.Meta["chat_id"]) {
			_ = b.Frontend.Send(ctx, AssistantMsg(m.ConversationID, b.conversationCapRejectionNotice(m.Meta["chat_id"])))
			continue
		}
		// 3. Admitted: forward to the backend (which replies via its Recv).
		if err := b.Backend.Send(ctx, m); err != nil {
			_ = b.Frontend.Send(ctx, AssistantMsg(m.ConversationID, "backend error: "+err.Error()))
		}
	}
	return nil
}
