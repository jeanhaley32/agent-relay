package relay

import (
	"context"
	"testing"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
)

// capFrontend captures everything Send'd to it.
type capFrontend struct {
	recv chan Message
	sent chan Message
}

func (f *capFrontend) Name() string                            { return "front" }
func (f *capFrontend) Recv() <-chan Message                    { return f.recv }
func (f *capFrontend) Send(_ context.Context, m Message) error { f.sent <- m; return nil }
func (f *capFrontend) Close() error                            { return nil }

// emitBackend emits replies the test pushes onto recv.
type emitBackend struct{ recv chan Message }

func (b *emitBackend) Name() string                        { return "back" }
func (b *emitBackend) Recv() <-chan Message                { return b.recv }
func (b *emitBackend) Send(context.Context, Message) error { return nil }
func (b *emitBackend) Close() error                        { close(b.recv); return nil }

// TestOutboundGate: the model's replies to non-allowlisted chats are dropped;
// replies to allowed chats are delivered.
func TestOutboundGate(t *testing.T) {
	front := &capFrontend{recv: make(chan Message), sent: make(chan Message, 8)}
	back := &emitBackend{recv: make(chan Message, 8)}
	b := &Broker{
		Frontend:        front,
		Backend:         back,
		Commands:        command.NewRegistry(),
		Meter:           budget.New("pro", nil),
		OutboundAllowed: func(chatID string) bool { return chatID == "111" }, // only 111 allowed
	}
	go b.Run(context.Background())
	defer close(front.recv) // ends Run

	// Allowed reply is delivered.
	back.recv <- Message{Role: Assistant, Text: "hi", Meta: map[string]string{"chat_id": "111"}}
	// Reply to a stranger is dropped.
	back.recv <- Message{Role: Assistant, Text: "leak", Meta: map[string]string{"chat_id": "999"}}

	select {
	case m := <-front.sent:
		if m.Meta["chat_id"] != "111" || m.Text != "hi" {
			t.Fatalf("expected the allowed reply, got %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("allowed reply was not delivered")
	}

	// The blocked reply must not arrive.
	select {
	case m := <-front.sent:
		t.Fatalf("outbound gate leaked a reply to a stranger: %+v", m)
	case <-time.After(300 * time.Millisecond):
		// good — dropped
	}
}
