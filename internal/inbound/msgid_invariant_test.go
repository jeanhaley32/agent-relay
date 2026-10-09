package inbound

import "testing"

// Every inbound message, from every frontend, must be tagged with a unique
// msg_id at ingress — this is the property that makes a message answerable and
// the persistence pipeline complete. Envelope() is the one constructor all
// frontends go through, so asserting it here locks the invariant for all of them.
func TestEnvelopeStampsMsgIDForEverySource(t *testing.T) {
	cases := []struct{ platform, conv, text, chat, from, name string }{
		{"telegram", "c1", "hello from telegram", "111", "111", "alice"},
		{"discord", "c2", "hello from discord", "222", "333", "bob"},
	}
	seen := map[string]bool{}
	for _, c := range cases {
		m := Envelope(c.platform, c.conv, c.text, c.chat, c.from, c.name)
		id := m.Meta["msg_id"]
		if id == "" {
			t.Fatalf("%s: msg_id not stamped", c.platform)
		}
		if seen[id] {
			t.Fatalf("%s: msg_id %q collided", c.platform, id)
		}
		seen[id] = true
		if m.Meta["platform"] != c.platform || m.Meta["chat_id"] != c.chat || m.Meta["from_id"] != c.from {
			t.Fatalf("%s: tag fields not carried: %+v", c.platform, m.Meta)
		}
		if m.Text != c.text {
			t.Fatalf("%s: content not carried", c.platform)
		}
	}
}
