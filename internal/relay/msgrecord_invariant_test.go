package relay

import (
	"context"
	"testing"
)

// The persistence record built at the relay chokepoint must carry the message's
// identity and full content, for every event kind including gate-denials. This
// asserts the "tagged AND saved with its body" half of the invariant.
func TestMsgRecordCapturesTaggedContent(t *testing.T) {
	m := Message{
		Role: User,
		Text: "a message that must be captured verbatim",
		Meta: map[string]string{
			"msg_id": "abc123", "chat_id": "999", "from_id": "999",
			"from_name": "tester", "platform": "telegram", "guild_id": "g1",
		},
	}
	// Even a gate-denied inbound must produce a full record.
	r := msgRecord(m, "gate_blocked", "anomaly flag", "")
	if r.MsgID != "abc123" {
		t.Fatalf("msg_id not carried: %q", r.MsgID)
	}
	if r.Content != m.Text {
		t.Fatalf("content not captured: %q", r.Content)
	}
	if r.Direction != "inbound" {
		t.Fatalf("direction wrong: %q", r.Direction)
	}
	if r.Event != "gate_blocked" {
		t.Fatalf("event not carried: %q", r.Event)
	}
	if r.Frontend != "telegram" || r.ChatID != "999" || r.FromName != "tester" {
		t.Fatalf("tag fields not carried: %+v", r)
	}
	if r.Meta["guild_id"] != "g1" {
		t.Fatalf("non-promoted meta (guild_id) not carried: %+v", r.Meta)
	}
	// An assistant reply maps to outbound.
	if got := msgRecord(Message{Role: Assistant, Meta: map[string]string{}}, "reply", "", "").Direction; got != "outbound" {
		t.Fatalf("assistant direction wrong: %q", got)
	}
}

// A reply carries only its conversation id, so the store must take its
// platform from the frontend that actually delivers it, and must leave it
// empty rather than guess when no frontend owns the id.
func TestOutboundRecordTaggedWithDeliveringPlatform(t *testing.T) {
	tg := newFakeEndpoint("telegram")
	mx := newFakeEndpoint("matrix")
	mf := NewMultiFrontend(tg, mx)
	mx.recv <- Message{ConversationID: "@admin:example.org", Role: User}
	<-mf.Recv()
	b := &Broker{Frontend: mf}

	reply := Message{ConversationID: "@admin:example.org", Role: Assistant, Meta: map[string]string{"chat_id": "@admin:example.org"}}
	if err := mf.Send(context.Background(), reply); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(mx.sent) != 1 {
		t.Fatalf("reply was not delivered on matrix")
	}
	if got := b.platformFor(reply); got != mx.Name() {
		t.Fatalf("reply delivered on matrix was tagged %q", got)
	}

	unseen := Message{ConversationID: "never-seen", Role: Assistant}
	if got := b.platformFor(unseen); got != "" {
		t.Fatalf("unrouted reply tagged %q, want empty", got)
	}
	if got := (&Broker{Frontend: (*MultiFrontend)(nil)}).platformFor(reply); got != "" {
		t.Fatalf("typed-nil frontend tagged %q, want empty", got)
	}
	if got := (&Broker{Frontend: tg}).platformFor(reply); got != "telegram" {
		t.Fatalf("single frontend should tag by its own name, got %q", got)
	}
}
