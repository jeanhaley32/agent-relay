package relay

import (
	"context"
	"errors"
	"testing"
	"time"
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

// failingCloseEndpoint wraps a fakeEndpoint but returns an error from Close,
// so tests can verify MultiFrontend.Close aggregates sub-frontend errors
// instead of silently swallowing them.
type failingCloseEndpoint struct {
	*fakeEndpoint
	closeErr error
}

func newFailingCloseEndpoint(name string, closeErr error) *failingCloseEndpoint {
	return &failingCloseEndpoint{fakeEndpoint: newFakeEndpoint(name), closeErr: closeErr}
}

func (f *failingCloseEndpoint) Close() error {
	close(f.recv)
	return f.closeErr
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

// TestNewMultiFrontendFanInLearnsOwnership exercises the actual goroutine
// NewMultiFrontend starts: pushing a Message through a sub-frontend's Recv()
// channel must both surface it on the fanned-in Recv() and record ownership,
// so a later Send with the same ConversationID routes back to that
// sub-frontend (not frontends[0]).
func TestNewMultiFrontendFanInLearnsOwnership(t *testing.T) {
	tg := newFakeEndpoint("telegram") // frontends[0]
	dc := newFakeEndpoint("discord")  // frontends[1]

	mf := NewMultiFrontend(tg, dc)

	in := Message{ConversationID: "disc-123", Text: "hello"}
	dc.recv <- in

	select {
	case got := <-mf.Recv():
		if got.ConversationID != in.ConversationID {
			t.Fatalf("fanned-in message mismatch: got %+v want %+v", got, in)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message pushed into a sub-frontend's Recv() was not fanned in")
	}

	if err := mf.Send(context.Background(), Message{ConversationID: "disc-123", Text: "reply"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(dc.sent) != 1 {
		t.Fatalf("expected the reply to route to discord (the learned owner), got %d messages", len(dc.sent))
	}
	if len(tg.sent) != 0 {
		t.Fatalf("expected telegram (frontends[0]) to NOT receive the reply, got %d messages", len(tg.sent))
	}
}

// TestNewMultiFrontendCloseLifecycle covers Close()'s fan-in shutdown: once
// every sub-frontend's Recv() channel is closed, the wg.Wait goroutine must
// close mf.recv so a caller ranging over mf.Recv() terminates instead of
// blocking forever.
func TestNewMultiFrontendCloseLifecycle(t *testing.T) {
	tg := newFakeEndpoint("telegram")
	dc := newFakeEndpoint("discord")

	mf := NewMultiFrontend(tg, dc)

	if err := mf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case _, ok := <-mf.Recv():
		if ok {
			t.Fatal("expected mf.Recv() to be closed (no pending messages) after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mf.Recv() was not closed after Close() closed every sub-frontend")
	}
}

// TestMultiFrontendCloseAggregatesFirstError verifies Close() attempts every
// sub-frontend's Close (even after one fails) and returns the first error
// encountered, per its documented contract.
func TestMultiFrontendCloseAggregatesFirstError(t *testing.T) {
	wantErr := errors.New("telegram close failed")
	tg := newFailingCloseEndpoint("telegram", wantErr)
	dc := newFakeEndpoint("discord")

	mf := NewMultiFrontend(tg, dc)

	if err := mf.Close(); err != wantErr {
		t.Fatalf("Close() = %v, want %v", err, wantErr)
	}

	// Both sub-frontends' Recv channels must be closed regardless of the
	// first one's error, proving Close attempted all of them.
	if _, ok := <-tg.Recv(); ok {
		t.Fatal("expected telegram's Recv() to be closed")
	}
	if _, ok := <-dc.Recv(); ok {
		t.Fatal("expected discord's Recv() to be closed")
	}
}
