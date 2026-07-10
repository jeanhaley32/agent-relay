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
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/approval"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
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
	// emits, with its chat_id in Meta, AFTER the outbound gate passes and the
	// reply is delivered to the frontend. The pending-event tracker uses it to
	// infer that a fired trigger was handled (a reply promptly following the
	// trigger on that chat auto-resolves it). A reply dropped by the gate is
	// never reported. nil ⇒ no hook.
	OnBackendReply func(m Message)

	// Session gate: if Session and Approval are both set, inbound messages
	// from any user_id (from_id) in SessionGatedUsers require an active,
	// non-idle-expired session before being processed - independently
	// tracked per user, so one admin's idle timeout doesn't affect
	// another's. Keyed on the sender's permanent account id, not chat_id:
	// admin-privilege checks elsewhere (command dispatch, lockdown) already
	// key on from_id, and chat_id only equals from_id by Telegram-frontend
	// convention (private 1:1 chats) - keying the gate on chat_id made its
	// correctness silently depend on that invariant holding two layers
	// away, in a different package. An expired/missing session triggers a
	// tailnet re-auth challenge (via Approval) instead of processing the
	// message; the sender must click the approval link, then resend. nil
	// Session ⇒ no gating (all other senders are unaffected regardless).
	Session           *session.Manager
	Approval          *approval.Manager
	SessionGatedUsers map[string]bool
	SessionTTL        time.Duration // approval request validity window

	// Lockdown, when set, blocks every message from a non-admin sender
	// before it reaches slash commands or the model - only b.Commands.IsAdmin
	// senders get through. Admin-only to toggle (enforced by the /lockdown
	// command itself being Admin: true), affects only non-admins.
	Lockdown atomic.Bool
}

// LockdownMessage is sent to any non-admin sender while lockdown is active.
const LockdownMessage = "This assistant is currently in lockdown - only admins can send messages right now. Try again later."

// challengeSession sends a fresh tailnet re-auth link to the conversation
// and, in the background, activates userID's session once approved.
func (b *Broker) challengeSession(ctx context.Context, conv, userID string) {
	ttl := b.SessionTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	token, link := b.Approval.Create("relayd session re-authentication", ttl)
	_ = b.Frontend.Send(ctx, AssistantMsg(conv,
		"Session expired - re-authenticate over the tailnet, then resend your message: "+link))

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		deadline := time.Now().Add(ttl)
		for time.Now().Before(deadline) {
			<-ticker.C
			st, ok := b.Approval.Status(token)
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
			b.Meter.Record(b.Estimate(m.Text))
			// Outbound gate: the model may target any chat via its reply tool;
			// only deliver to allowed chats.
			if b.OutboundAllowed != nil && !b.OutboundAllowed(m.Meta["chat_id"]) {
				continue // dropped (the gate func is responsible for logging)
			}
			_ = b.Frontend.Send(ctx, m)
			// Reply-inferred ack runs only AFTER the gate passes and the reply
			// is actually delivered — a reply dropped by the gate never reached
			// the user, so it is not evidence the trigger was handled.
			if b.OnBackendReply != nil {
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
		// both are present. This holds by construction for every current
		// frontend (a private 1:1 Telegram chat's id IS the sender's own
		// id; the Telegram frontend also independently rejects any non-
		// private chat before a message ever reaches here) - so this
		// should never actually fire. That's exactly what makes it a good
		// tripwire: a hit means some upstream assumption broke (a new
		// frontend, a regression, group/channel traffic slipping through),
		// and every downstream gate here (session, admin checks) silently
		// depends on that assumption holding. Fail closed rather than
		// silently mis-gating.
		if fromID, chatID := m.Meta["from_id"], m.Meta["chat_id"]; fromID != "" && chatID != "" && fromID != chatID {
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
		// 3. Admitted: forward to the backend (which replies via its Recv).
		if err := b.Backend.Send(ctx, m); err != nil {
			_ = b.Frontend.Send(ctx, AssistantMsg(m.ConversationID, "backend error: "+err.Error()))
		}
	}
	return nil
}
