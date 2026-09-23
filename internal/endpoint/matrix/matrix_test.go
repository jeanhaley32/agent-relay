package matrix

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// TestRunDoesNotSyncUntilPrimed is the regression for the replay bug: an
// unprimed sync omits `since`, the homeserver answers with each room's recent
// timeline, and those events are delivered as fresh admin commands. Matrix is
// the gate-bypass path, so they would run unchallenged.
//
// The fake fails the priming sync once, then succeeds. No /sync request may
// carry an empty `since` at any point.
func TestRunDoesNotSyncUntilPrimed(t *testing.T) {
	var primeCalls, unprimedSyncs atomic.Int64
	delivered := make(chan struct{}, 8)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/sync") {
			http.NotFound(w, r)
			return
		}
		since := r.URL.Query().Get("since")
		timeout := r.URL.Query().Get("timeout")

		// The priming call is the one made with no `since` AND no long-poll
		// timeout; the loop's calls always long-poll.
		if since == "" && timeout == "0" {
			if primeCalls.Add(1) == 1 {
				http.Error(w, "homeserver still starting", http.StatusBadGateway)
				return
			}
			_, _ = io.WriteString(w, `{"next_batch":"s1"}`)
			return
		}

		if since == "" {
			// A loop sync with no token: the bug. Answer the way a real
			// homeserver would, with history, so a regression delivers.
			unprimedSyncs.Add(1)
			_, _ = io.WriteString(w, `{"next_batch":"s2","rooms":{"join":{"!r:example.org":{"timeline":{"events":[
				{"type":"m.room.message","sender":"@admin:example.org","event_id":"$1","origin_server_ts":1,
				 "content":{"msgtype":"m.text","body":"restart the deploy"}}]}}}}}`)
			return
		}
		_, _ = io.WriteString(w, `{"next_batch":"s3"}`)
	}))
	defer srv.Close()

	f := New(srv.URL, "tok", []string{"@admin:example.org"}, "", log.New(io.Discard, "", 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for range f.Recv() {
			delivered <- struct{}{}
		}
	}()
	go f.Run(ctx, "@bot:example.org")

	// Long enough for the failed prime, its backoff, the retry, and several
	// loop syncs.
	time.Sleep(1500 * time.Millisecond)
	cancel()
	_ = f.Close()

	if n := unprimedSyncs.Load(); n != 0 {
		t.Fatalf("%d sync(s) ran without a since token — history would be replayed as commands", n)
	}
	if n := primeCalls.Load(); n < 2 {
		t.Fatalf("priming was attempted %d time(s), want a retry after the failure", n)
	}
	select {
	case <-delivered:
		t.Fatal("a historical message was delivered as a live command")
	default:
	}
}

// fakeHomeserver serves the handful of Client-Server endpoints this frontend
// uses, so the tests exercise real request/response plumbing rather than
// stubbing the frontend's own methods.
type fakeHomeserver struct {
	mu        sync.Mutex
	sent      []sentMessage
	syncBody  string
	whoamiID  string
	authSeen  []string
	syncCalls int
}

type sentMessage struct{ room, body, txnID string }

func (h *fakeHomeserver) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.authSeen = append(h.authSeen, r.Header.Get("Authorization"))
		h.mu.Unlock()

		switch {
		case strings.HasSuffix(r.URL.Path, "/account/whoami"):
			_, _ = io.WriteString(w, `{"user_id":"`+h.whoamiID+`"}`)

		case strings.Contains(r.URL.Path, "/send/m.room.message/"):
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			room, txn := parts[len(parts)-4], parts[len(parts)-1]
			var body struct {
				MsgType string `json:"msgtype"`
				Body    string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			h.mu.Lock()
			h.sent = append(h.sent, sentMessage{room: room, body: body.Body, txnID: txn})
			h.mu.Unlock()
			_, _ = io.WriteString(w, `{"event_id":"$evt"}`)

		case strings.Contains(r.URL.Path, "/sync"):
			h.mu.Lock()
			h.syncCalls++
			body := h.syncBody
			h.mu.Unlock()
			if body == "" {
				body = `{"next_batch":"s1"}`
			}
			_, _ = io.WriteString(w, body)

		default:
			http.NotFound(w, r)
		}
	})
}

func (h *fakeHomeserver) sentMessages() []sentMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]sentMessage(nil), h.sent...)
}

func TestConnectReturnsOwnUserIDAndAuthenticates(t *testing.T) {
	h := &fakeHomeserver{whoamiID: "@relaybot:example.org"}
	srv := httptest.NewServer(h.handler())
	defer srv.Close()

	f := New(srv.URL, "secret-token", []string{"@admin:example.org"}, "", log.New(io.Discard, "", 0))
	id, err := f.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if id != "@relaybot:example.org" {
		t.Fatalf("Connect returned %q", id)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.authSeen) == 0 || h.authSeen[0] != "Bearer secret-token" {
		t.Fatalf("token not sent as a bearer credential: %v", h.authSeen)
	}
}

// Send has to resolve a room from the conversation id, and the DM-style
// conversation id is not itself a room. These are the four routes it accepts.
func TestSendResolvesRoomFromEveryAcceptedSource(t *testing.T) {
	h := &fakeHomeserver{whoamiID: "@relaybot:example.org"}
	srv := httptest.NewServer(h.handler())
	defer srv.Close()
	f := New(srv.URL, "tok", []string{"@admin:example.org"}, "", log.New(io.Discard, "", 0))

	// 1. room id carried in Meta.
	err := f.Send(context.Background(), relay.Message{
		ConversationID: "@admin:example.org",
		Text:           "via meta room_id",
		Meta:           map[string]string{"room_id": "!a:example.org"},
	})
	if err != nil {
		t.Fatalf("send via Meta room_id: %v", err)
	}

	// 2. a '!'-prefixed conversation id is itself the room.
	if err := f.Send(context.Background(), relay.Message{
		ConversationID: "!b:example.org", Text: "via conv id",
	}); err != nil {
		t.Fatalf("send via room-shaped ConversationID: %v", err)
	}

	// 3. unresolvable: no mapping, no room-shaped hint anywhere.
	err = f.Send(context.Background(), relay.Message{
		ConversationID: "@stranger:example.org", Text: "nowhere to put this",
	})
	if err == nil {
		t.Fatal("send with no resolvable room should fail rather than guess")
	}

	got := h.sentMessages()
	if len(got) != 2 {
		t.Fatalf("sent %d message(s), want 2: %+v", len(got), got)
	}
	if got[0].room != "!a:example.org" || got[1].room != "!b:example.org" {
		t.Fatalf("wrong rooms: %+v", got)
	}
	// Transaction ids must be unique, or Matrix dedupes the second send away.
	if got[0].txnID == got[1].txnID {
		t.Fatalf("duplicate transaction id %q — the homeserver would drop one", got[0].txnID)
	}
}
