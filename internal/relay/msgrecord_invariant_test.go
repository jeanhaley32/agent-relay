package relay

import "testing"

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
