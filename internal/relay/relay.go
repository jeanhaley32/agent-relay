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
	"sync"

	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
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
	Recv() <-chan Message                    // messages originating at this endpoint
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
			_ = b.Frontend.Send(ctx, m)
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
		// 1. Slash commands are handled locally — never hit the model.
		if reply, handled := b.Commands.Dispatch(m.Text); handled {
			_ = b.Frontend.Send(ctx, AssistantMsg(m.ConversationID, reply))
			continue
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
