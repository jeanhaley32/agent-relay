// Package echo is a trivial backend Endpoint used to exercise the broker,
// commands, and budget gate without a real model. It echoes each user message
// back as an assistant reply. Because it satisfies relay.Endpoint, it can be
// swapped for a Claude or Ollama backend with no change to the broker.
package echo

import (
	"context"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// Backend is an echo Endpoint.
type Backend struct {
	out chan relay.Message
}

// New returns a started echo backend.
func New() *Backend {
	return &Backend{out: make(chan relay.Message, 64)}
}

func (b *Backend) Name() string               { return "echo" }
func (b *Backend) Recv() <-chan relay.Message { return b.out }
func (b *Backend) Close() error               { close(b.out); return nil }

// Send produces the echoed reply on the endpoint's Recv channel.
func (b *Backend) Send(ctx context.Context, m relay.Message) error {
	reply := relay.AssistantMsg(m.ConversationID, "echo: "+m.Text)
	select {
	case b.out <- reply:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
