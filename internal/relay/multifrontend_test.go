package relay

import (
	"context"
	"testing"
)

// fakeEndpoint is a minimal Endpoint for routing tests.
type fakeEndpoint struct {
	name string
	recv chan Message
	sent []Message
}

func newFakeEndpoint(name string) *fakeEndpoint {
	return &fakeEndpoint{name: name, recv: make(chan Message)}
}

func (f *fakeEndpoint) Name() string         { return f.name }
func (f *fakeEndpoint) Recv() <-chan Message { return f.recv }
func (f *fakeEndpoint) Close() error         { close(f.recv); return nil }
func (f *fakeEndpoint) Send(ctx context.Context, m Message) error {
	f.sent = append(f.sent, m)
	return nil
}

// claimerEndpoint additionally implements Claimer, owning any conversation
// id at or above a configured floor (modeling discord's snowflake-floor
// disambiguation).
type claimerEndpoint struct {
	*fakeEndpoint
	floor int64
}

func newClaimerEndpoint(name string, floor int64) *claimerEndpoint {
	return &claimerEndpoint{fakeEndpoint: newFakeEndpoint(name), floor: floor}
}

func (c *claimerEndpoint) OwnsConversationID(id string) bool {
	var n int64
	for _, ch := range id {
		if ch < '0' || ch > '9' {
			return false
		}
		n = n*10 + int64(ch-'0')
	}
	return n >= c.floor
}

func TestMultiFrontendSendRouting(t *testing.T) {
	tests := []struct {
		name        string
		seedInbound map[string]int // conversationID -> index of frontend that delivered it inbound (into frontends slice below)
		send        string
		wantIdx     int // index into frontends slice that should have received the Send
	}{
		{
			name:        "routes to owner seen inbound",
			seedInbound: map[string]int{"telegram-42": 0},
			send:        "telegram-42",
			wantIdx:     0,
		},
		{
			name:        "routes to owner seen inbound on second frontend",
			seedInbound: map[string]int{"999999999999999999": 1},
			send:        "999999999999999999",
			wantIdx:     1,
		},
		{
			name:    "never seen inbound falls back to Claimer match",
			send:    "999999999999999999", // above the discord-like floor
			wantIdx: 1,
		},
		{
			name:    "never seen inbound and no Claimer claims it falls back to frontends[0]",
			send:    "42", // small id, below floor, not claimed
			wantIdx: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tg := newFakeEndpoint("telegram")                          // frontends[0], no Claimer
			dc := newClaimerEndpoint("discord", 1_000_000_000_000_000) // frontends[1], implements Claimer

			mf := &MultiFrontend{
				frontends: []Endpoint{tg, dc},
				owner:     map[string]Endpoint{},
				recv:      make(chan Message, 1),
			}
			for convID, idx := range tc.seedInbound {
				var fe Endpoint
				if idx == 0 {
					fe = tg
				} else {
					fe = dc
				}
				mf.owner[convID] = fe
			}

			if err := mf.Send(context.Background(), Message{ConversationID: tc.send, Text: "hi"}); err != nil {
				t.Fatalf("Send: %v", err)
			}

			wantTG := tc.wantIdx == 0
			wantDC := tc.wantIdx == 1

			if wantTG && len(tg.sent) != 1 {
				t.Errorf("expected telegram to receive the send, got %d messages", len(tg.sent))
			}
			if !wantTG && len(tg.sent) != 0 {
				t.Errorf("expected telegram to NOT receive the send, got %d messages", len(tg.sent))
			}
			if wantDC && len(dc.sent) != 1 {
				t.Errorf("expected discord to receive the send, got %d messages", len(dc.sent))
			}
			if !wantDC && len(dc.sent) != 0 {
				t.Errorf("expected discord to NOT receive the send, got %d messages", len(dc.sent))
			}
		})
	}
}
