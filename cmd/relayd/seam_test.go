package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jeanhaley32/agent-relay/internal/command"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// fakeBackend is deliberately NOT the Claude endpoint. Passing it to verdict is
// the assertion: if the backend seam is ever widened back to a concrete type,
// this file stops compiling.
type fakeBackend struct{ decided bool }

func (f *fakeBackend) Decide(string, bool) error { f.decided = true; return nil }

func TestVerdictTakesAnyBackendThatCanDecide(t *testing.T) {
	f := &fakeBackend{}
	got := verdict(f, nil, true)(command.Context{}, []string{"req-1"})
	if !f.decided {
		t.Fatal("verdict did not reach the backend")
	}
	if !strings.Contains(got, "req-1") {
		t.Fatalf("verdict did not confirm the request id: %q", got)
	}
}

// Receipts moving off the admin fan-out is the whole point of the change, so
// each branch is proved rather than assumed: a configured room captures them,
// and an absent room or an absent frontend leaves the fan-out untouched.
func TestRoomReceiptsRoutesToTheRoom(t *testing.T) {
	var got relay.Message
	fellBack := false
	send := roomReceipts("!receipts:example.test",
		func(_ context.Context, m relay.Message) error { got = m; return nil },
		func(string) { fellBack = true })

	send("✓ handled: something")

	if fellBack {
		t.Fatal("fell back to the admin fan-out despite a configured room")
	}
	if got.ConversationID != "!receipts:example.test" {
		t.Errorf("ConversationID = %q, want the receipt room", got.ConversationID)
	}
	if got.Meta["chat_id"] != "!receipts:example.test" {
		t.Errorf("Meta chat_id = %q, want the receipt room", got.Meta["chat_id"])
	}
	if got.Text != "✓ handled: something" {
		t.Errorf("Text = %q, want the receipt text unchanged", got.Text)
	}
}

func TestRoomReceiptsFallsBackWhenUnconfigured(t *testing.T) {
	for _, tc := range []struct {
		name string
		room string
		send func(context.Context, relay.Message) error
	}{
		{"no room", "", func(context.Context, relay.Message) error { return nil }},
		{"no frontend", "!receipts:example.test", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fellBack := false
			roomReceipts(tc.room, tc.send, func(string) { fellBack = true })("✓ handled: x")
			if !fellBack {
				t.Error("did not fall back to the admin fan-out")
			}
		})
	}
}
