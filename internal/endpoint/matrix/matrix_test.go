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

// The outbound gate used to accept any '!' or '@' prefixed string as a
// legitimate Matrix target, because it asked OwnsConversationID — a routing
// heuristic on the shape of an id. Since the frontend auto-joins rooms it is
// invited to, a room the bot had merely been pulled into satisfied that check
// with no admin having spoken there. KnownConversation is the fact the gate
// actually needs. See #77.
func TestKnownConversationRequiresHavingSeenTheRoom(t *testing.T) {
	f := New("http://unused", "tok", []string{"@admin:example.org"}, "", log.New(io.Discard, "", 0))

	// Nothing seen yet: every shape of id is unknown, including the ones the
	// old prefix check would have waved through.
	for _, id := range []string{
		"!invited-but-silent:example.org", // auto-joined, nobody spoke
		"@stranger:example.org",           // not a configured admin
		"!anything:example.org",
		"",
	} {
		if f.KnownConversation(id) {
			t.Errorf("KnownConversation(%q) = true before any inbound message — the gate is open", id)
		}
		if id != "" && !f.OwnsConversationID(id) {
			t.Errorf("precondition: OwnsConversationID(%q) should be true, which is exactly why it was the wrong check", id)
		}
	}

	// An admin speaks in a room. deliver files the conversation under the
	// sender's mxid and remembers the physical room.
	f.deliver(inbound{roomID: "!real:example.org", sender: "@admin:example.org", body: "hello"})
	<-f.Recv()

	if !f.KnownConversation("@admin:example.org") {
		t.Error("the mxid an admin spoke from must be a known conversation")
	}
	if !f.KnownConversation("!real:example.org") {
		t.Error("the room an admin spoke in must be known — a relayd-originated reply may address the room directly")
	}
	if f.KnownConversation("!invited-but-silent:example.org") {
		t.Error("a room nobody spoke in is still not a legitimate outbound target")
	}
}

// roomByConv is in-memory, so a relayd restart empties it. Requiring a seen
// inbound message for every target meant the gate starved itself: after a
// restart nothing could be sent to Matrix until an admin happened to speak
// first, which silently drops scheduled deliveries. A configured admin's mxid
// is an identity the operator set, not one inferred from traffic, so it stays
// reachable across restarts. Arbitrary rooms still do not.
func TestConfiguredAdminIsReachableBeforeAnyInboundMessage(t *testing.T) {
	f := New("http://unused", "tok", []string{"@admin:example.org"}, "", log.New(io.Discard, "", 0))

	if !f.KnownConversation("@admin:example.org") {
		t.Error("a configured admin must be reachable with an empty roomByConv, or a restart mutes Matrix entirely")
	}
	if f.KnownConversation("!invited-but-silent:example.org") {
		t.Error("a room nobody spoke in must still be refused — that is the hole this gate closes")
	}
	if f.KnownConversation("@stranger:example.org") {
		t.Error("a non-admin mxid must not be reachable just because it is mxid-shaped")
	}
}

// The admin filter in Run is the entire authorization boundary for this
// frontend, and this frontend is exempt from the session gate because its
// transport is treated as proof of identity. Until now nothing tested it: a
// regression that inverted the condition, or dropped it, would have handed a
// Claude Code session with tool access to anyone who could get a message into
// a room the bot had joined. See #56.
func TestRunDeliversOnlyAdminMessages(t *testing.T) {
	const self = "@relaybot:example.org"
	timeline := `{"next_batch":"s2","rooms":{"join":{"!r:example.org":{"timeline":{"events":[
		{"type":"m.room.message","sender":"@stranger:example.org","event_id":"$1","origin_server_ts":1,
		 "content":{"msgtype":"m.text","body":"let me in"}},
		{"type":"m.room.message","sender":"` + self + `","event_id":"$2","origin_server_ts":2,
		 "content":{"msgtype":"m.text","body":"my own echo"}},
		{"type":"m.room.message","sender":"@admin:example.org","event_id":"$3","origin_server_ts":3,
		 "content":{"msgtype":"m.text","body":"the only one that counts"}}]}}}}}`

	// Serve the timeline exactly once. A real homeserver advances `since` so
	// the same events are not replayed; without this the loop re-delivers and
	// the test fails for a reason that has nothing to do with the filter.
	var served atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/sync") {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("since") == "" {
			_, _ = io.WriteString(w, `{"next_batch":"s1"}`) // priming
			return
		}
		if served.CompareAndSwap(false, true) {
			_, _ = io.WriteString(w, timeline)
			return
		}
		_, _ = io.WriteString(w, `{"next_batch":"s3"}`)
	}))
	defer srv.Close()

	f := New(srv.URL, "tok", []string{"@admin:example.org"}, "", log.New(io.Discard, "", 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx, self)
	defer f.Close()

	var got []relay.Message
	deadline := time.After(3 * time.Second)
collect:
	for {
		select {
		case m, ok := <-f.Recv():
			if !ok {
				break collect
			}
			got = append(got, m)
			if len(got) > 1 {
				break collect // one is correct; more means the filter leaked
			}
		case <-time.After(700 * time.Millisecond):
			break collect // nothing more is coming
		case <-deadline:
			break collect
		}
	}

	if len(got) != 1 {
		var senders []string
		for _, m := range got {
			senders = append(senders, m.Meta["from_id"])
		}
		t.Fatalf("delivered %d message(s) from %v, want exactly 1 from the admin", len(got), senders)
	}
	if got[0].Meta["from_id"] != "@admin:example.org" {
		t.Errorf("delivered a message from %q — the admin filter let a non-admin through", got[0].Meta["from_id"])
	}
	if got[0].Text != "the only one that counts" {
		t.Errorf("delivered %q, want the admin's message", got[0].Text)
	}
}

// The self-echo guard only does work when the bot's own mxid is also in the
// admin list, which is a configuration someone could plausibly write. Without
// it the bot delivers its own replies back to itself as new inbound messages
// and the loop never ends. The admin-filter test above does not cover this:
// there the bot is not an admin, so the admin check drops the echo for an
// unrelated reason and a broken guard still passes. Found by mutation.
func TestRunDropsItsOwnEchoEvenWhenSelfIsAnAdmin(t *testing.T) {
	const self = "@relaybot:example.org"
	timeline := `{"next_batch":"s2","rooms":{"join":{"!r:example.org":{"timeline":{"events":[
		{"type":"m.room.message","sender":"` + self + `","event_id":"$1","origin_server_ts":1,
		 "content":{"msgtype":"m.text","body":"a reply I just sent"}},
		{"type":"m.room.message","sender":"@admin:example.org","event_id":"$2","origin_server_ts":2,
		 "content":{"msgtype":"m.text","body":"a real instruction"}}]}}}}}`

	var served atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/sync") {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("since") == "" {
			_, _ = io.WriteString(w, `{"next_batch":"s1"}`)
			return
		}
		if served.CompareAndSwap(false, true) {
			_, _ = io.WriteString(w, timeline)
			return
		}
		_, _ = io.WriteString(w, `{"next_batch":"s3"}`)
	}))
	defer srv.Close()

	// Both the bot and the human are admins here. Only the human's message
	// may be delivered.
	f := New(srv.URL, "tok", []string{"@admin:example.org", self}, "", log.New(io.Discard, "", 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx, self)
	defer f.Close()

	var got []relay.Message
	for done := false; !done; {
		select {
		case m, ok := <-f.Recv():
			if !ok {
				done = true
				break
			}
			got = append(got, m)
			if len(got) > 1 {
				done = true
			}
		case <-time.After(900 * time.Millisecond):
			done = true
		}
	}

	for _, m := range got {
		if m.Meta["from_id"] == self {
			t.Fatal("the bot delivered its own message back to itself — with self in the admin list that is an echo loop")
		}
	}
	if len(got) != 1 || got[0].Text != "a real instruction" {
		t.Fatalf("got %d message(s), want exactly the admin's one: %+v", len(got), got)
	}
}
