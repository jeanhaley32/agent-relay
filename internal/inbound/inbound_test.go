package inbound

import (
	"testing"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

func TestEnvelopeCanonicalShape(t *testing.T) {
	fixed := time.Date(2026, 8, 9, 17, 30, 0, 0, time.UTC)
	now = func() time.Time { return fixed }
	defer func() { now = time.Now }()

	m := Envelope("telegram", "conv1", "hello", "conv1", "user1", "Casey")

	if m.Role != relay.User {
		t.Fatalf("role = %q, want user", m.Role)
	}
	if m.ConversationID != "conv1" || m.Text != "hello" {
		t.Fatalf("conv/text not set: %+v", m)
	}
	want := map[string]string{
		"platform":  "telegram",
		"chat_id":   "conv1",
		"from_id":   "user1",
		"from_name": "Casey",
		"ts":        "2026-08-09T17:30:00Z",
	}
	for k, v := range want {
		if m.Meta[k] != v {
			t.Errorf("Meta[%q] = %q, want %q", k, m.Meta[k], v)
		}
	}
	if m.Meta["msg_id"] == "" {
		t.Error("msg_id not stamped")
	}
	if _, err := time.Parse(TimeFormat, m.Meta["ts"]); err != nil {
		t.Errorf("ts not RFC3339: %v", err)
	}
}

func TestEnvelopeMintsDistinctMsgIDs(t *testing.T) {
	a := Envelope("discord", "c", "x", "c", "u", "n")
	b := Envelope("discord", "c", "x", "c", "u", "n")
	if a.Meta["msg_id"] == b.Meta["msg_id"] {
		t.Fatal("two envelopes shared a msg_id — must be unique per message")
	}
}
