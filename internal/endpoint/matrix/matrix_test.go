package matrix

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// TestGateSenderAllowlist covers DESIGN.md §3: an allowed sender's DM is
// relayed, a denied sender's DM is dropped and recorded as pending.
// Exercised directly against gate() — no sync loop/homeserver needed, per
// DESIGN.md §6's "unit testable without a real homeserver" requirement.
func TestGateSenderAllowlist(t *testing.T) {
	rec := &recordingAuth{allowed: map[string]bool{"@jean:example.org": true}}
	f := &Frontend{auth: rec, logger: testLogger(t)}

	allowedMsg := inboundMessage{
		eventID:    "$1",
		roomID:     "!room1:example.org",
		senderID:   "@jean:example.org",
		senderName: "@jean:example.org",
		content:    "hello",
	}
	msg, ok := f.gate(allowedMsg)
	if !ok {
		t.Fatalf("expected allowed sender's DM to be relayed")
	}
	if msg.Text != "hello" || msg.Meta["from_id"] != "@jean:example.org" {
		t.Fatalf("unexpected relay.Message: %+v", msg)
	}
	// DM: chat_id should equal the sender id (Option B, §3), not the room id.
	if msg.Meta["chat_id"] != "@jean:example.org" || msg.ConversationID != "@jean:example.org" {
		t.Fatalf("chat_id should be the sender id for DMs, got %q", msg.Meta["chat_id"])
	}
	if msg.Meta["room_id"] != "!room1:example.org" {
		t.Fatalf("room_id should still track the physical room, got %q", msg.Meta["room_id"])
	}

	deniedMsg := inboundMessage{
		eventID:    "$2",
		roomID:     "!room2:example.org",
		senderID:   "@stranger:example.org",
		senderName: "@stranger:example.org",
		content:    "spam",
	}
	if _, ok := f.gate(deniedMsg); ok {
		t.Fatalf("expected denied sender's DM to be dropped")
	}
	if !rec.recorded["@stranger:example.org"] {
		t.Fatalf("expected denied sender to be recorded as a pending request")
	}
}

// TestGateDropsOwnEcho ensures messages sent by the bot's own account
// (senderIsMe) are never relayed, to avoid bot-loops.
func TestGateDropsOwnEcho(t *testing.T) {
	f := &Frontend{auth: &recordingAuth{allowed: map[string]bool{"@bot:example.org": true}}, logger: testLogger(t)}
	if _, ok := f.gate(inboundMessage{senderID: "@bot:example.org", senderIsMe: true, content: "hi"}); ok {
		t.Fatalf("expected self-authored message to be dropped")
	}
}

// TestGateDropsEmptyContent covers DESIGN.md §2's "genuinely empty message"
// drop path, distinct from the undecryptable-content case handled upstream
// in onMessageEvent (before gate() is ever called).
func TestGateDropsEmptyContent(t *testing.T) {
	f := &Frontend{auth: &recordingAuth{allowed: map[string]bool{"@jean:example.org": true}}, logger: testLogger(t)}
	if _, ok := f.gate(inboundMessage{senderID: "@jean:example.org", content: ""}); ok {
		t.Fatalf("expected empty-content message to be dropped")
	}
}

// TestGateMultiMemberRoomUsesRoomIDAsChatID covers DESIGN.md §3's
// multi-member room decision: chat_id becomes the room id (not the sender
// id) once memberCount indicates more than a 1:1 DM.
func TestGateMultiMemberRoomUsesRoomIDAsChatID(t *testing.T) {
	f := &Frontend{auth: &recordingAuth{allowed: map[string]bool{"@jean:example.org": true}}, logger: testLogger(t)}
	msg, ok := f.gate(inboundMessage{
		roomID: "!group:example.org", senderID: "@jean:example.org", content: "hi", memberCount: 5,
	})
	if !ok {
		t.Fatalf("expected allowed sender's group message to be relayed")
	}
	if msg.ConversationID != "!group:example.org" || msg.Meta["chat_id"] != "!group:example.org" {
		t.Fatalf("expected chat_id to be the room id for a multi-member room, got %+v", msg)
	}
	if msg.Meta["from_id"] != "@jean:example.org" {
		t.Fatalf("from_id should still be the sender id: %+v", msg)
	}
	// The Broker's identity-pair invariant (relay.go) drops from_id!=chat_id
	// messages unless Meta["guild_id"] is set — a multi-member room's
	// chat_id (room id) is never equal to from_id (sender id), so this
	// message can only survive the Broker with the exemption marker set.
	if msg.Meta["guild_id"] != "!group:example.org" {
		t.Fatalf("expected guild_id exemption marker for a multi-member room, got %+v", msg)
	}
}

// TestSendTooLong verifies maxMessageLen is enforced before any API call,
// classified permanent (non-retryable) — mirrors telegram/discord's
// maxMessageLen handling per DESIGN.md §4.
func TestSendTooLong(t *testing.T) {
	f := &Frontend{logger: testLogger(t), api: &fakeAPI{}}
	longText := strings.Repeat("x", maxMessageLen+1)
	err := f.sendOnce(context.Background(), relay.Message{
		Text: longText,
		Meta: map[string]string{"room_id": "!room:example.org"},
	})
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
	var perm permanentSendError
	if !errors.As(err, &perm) {
		t.Fatalf("expected a permanent (non-retryable) error, got %v (%T)", err, err)
	}
}

// TestSendMissingRoomID verifies a missing destination is also permanent
// (DESIGN.md §4): retrying can't conjure a room id that was never set.
func TestSendMissingRoomID(t *testing.T) {
	f := &Frontend{logger: testLogger(t), api: &fakeAPI{}}
	err := f.sendOnce(context.Background(), relay.Message{Text: "hi"})
	if err == nil {
		t.Fatal("expected error for missing room_id")
	}
	var perm permanentSendError
	if !errors.As(err, &perm) {
		t.Fatalf("expected a permanent (non-retryable) error, got %v (%T)", err, err)
	}
}

// TestSendRoundTripFake exercises Send()'s happy path against the
// MautrixAPI fake (DESIGN.md §6's most direct test seam) — no HTTP at all.
func TestSendRoundTripFake(t *testing.T) {
	fake := &fakeAPI{}
	f := &Frontend{logger: testLogger(t), api: fake, recv: make(chan relay.Message, 1), retryQueue: make(chan retryItem, 10)}

	if err := f.Send(context.Background(), relay.Message{
		Text: "hi",
		Meta: map[string]string{"room_id": "!room:example.org"},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(fake.sent) != 1 || fake.sent[0].roomID != "!room:example.org" || fake.sent[0].body != "hi" {
		t.Fatalf("unexpected sent messages: %+v", fake.sent)
	}
}

// TestSendOversizedSplitAndDelivered locks in senderr.Split usage: a
// message over maxMessageLen is split and every chunk delivered in order,
// mirroring discord.go's 2026-07-14 incident fix.
func TestSendOversizedSplitAndDelivered(t *testing.T) {
	fake := &fakeAPI{}
	f := &Frontend{logger: testLogger(t), api: fake, retryQueue: make(chan retryItem, 10)}

	oversized := strings.Repeat("x", maxMessageLen*2+500)
	if err := f.Send(context.Background(), relay.Message{
		Text: oversized,
		Meta: map[string]string{"room_id": "!room:example.org"},
	}); err != nil {
		t.Fatalf("Send returned an error for an oversized message that should have been split: %v", err)
	}
	if len(fake.sent) < 2 {
		t.Errorf("SendMessageEvent called %d time(s), want at least 2 - the message should have been split into multiple chunks", len(fake.sent))
	}
}

// TestSend403Permanent covers DESIGN.md §7's incident-class review: a 403
// (M_FORBIDDEN, or any other non-429 4xx) must be classified permanent, via
// a real httptest server so the mautrix.HTTPError classification path is
// actually exercised, not just Meta/length checks.
func TestSend403Permanent(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_matrix/client/v3/rooms/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"errcode":"M_FORBIDDEN","error":"not in room"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f, err := New(srv.URL, WithHTTPClient(srv.Client()), WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer f.Close()

	err = f.sendOnce(context.Background(), relay.Message{Text: "hi", Meta: map[string]string{"room_id": "!room:example.org"}})
	if err == nil {
		t.Fatal("expected an error for a 403 response")
	}
	var perm permanentSendError
	if !errors.As(err, &perm) {
		t.Fatalf("expected a permanent (non-retryable) error for 403, got %v (%T)", err, err)
	}
}

// TestSendRoundTripHTTP exercises Send() against a real httptest server via
// New()'s normal mautrix.NewClient construction path (not the MautrixAPI
// fake), verifying the outbound payload actually reaches the wire in the
// shape a homeserver expects. No real homeserver or credentials involved.
func TestSendRoundTripHTTP(t *testing.T) {
	var gotBody string
	mux := http.NewServeMux()
	mux.HandleFunc("/_matrix/client/v3/rooms/", func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(b)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"event_id":"$abc123"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f, err := New(srv.URL, WithHTTPClient(srv.Client()), WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer f.Close()

	if err := f.Send(context.Background(), relay.Message{
		Text: "hi",
		Meta: map[string]string{"room_id": "!room:example.org"},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !strings.Contains(gotBody, `"hi"`) {
		t.Fatalf("expected outbound payload to contain the message body, got %q", gotBody)
	}
}

// --- test helpers ------------------------------------------------------------

type recordingAuth struct {
	allowed  map[string]bool
	recorded map[string]bool
}

func (r *recordingAuth) Allowed(id string) bool { return r.allowed[id] }
func (r *recordingAuth) Record(id string, name string) {
	if r.recorded == nil {
		r.recorded = map[string]bool{}
	}
	r.recorded[id] = true
}

func testLogger(t *testing.T) *log.Logger { return log.New(testWriter{t}, "", 0) }

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// fakeAPI is a hand-written MautrixAPI fake — DESIGN.md §6's "most direct
// test seam", letting Send()'s classification logic be exercised with zero
// real or fake HTTP transport at all.
type fakeAPI struct {
	sent []fakeSent
	// failStatus, if non-zero, makes SendMessageEvent return an HTTPError
	// with this status for every call.
	failStatus int
}

type fakeSent struct {
	roomID string
	body   string
}

func (f *fakeAPI) SetCredentials(id.UserID, string) {}
func (f *fakeAPI) Login(context.Context, *mautrix.ReqLogin) (*mautrix.RespLogin, error) {
	return &mautrix.RespLogin{}, nil
}
func (f *fakeAPI) SyncWithContext(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
func (f *fakeAPI) SendMessageEvent(ctx context.Context, roomID id.RoomID, eventType event.Type, content interface{}, extra ...mautrix.ReqSendEvent) (*mautrix.RespSendEvent, error) {
	if f.failStatus != 0 {
		return nil, mautrix.HTTPError{
			Response:  &http.Response{StatusCode: f.failStatus},
			RespError: &mautrix.RespError{ErrCode: "M_FORBIDDEN", StatusCode: f.failStatus},
		}
	}
	msg, _ := content.(event.MessageEventContent)
	f.sent = append(f.sent, fakeSent{roomID: string(roomID), body: msg.Body})
	return &mautrix.RespSendEvent{EventID: "$fake"}, nil
}
func (f *fakeAPI) JoinedMembers(context.Context, id.RoomID) (*mautrix.RespJoinedMembers, error) {
	return &mautrix.RespJoinedMembers{}, nil
}
func (f *fakeAPI) JoinRoomByID(context.Context, id.RoomID) (*mautrix.RespJoinRoom, error) {
	return &mautrix.RespJoinRoom{}, nil
}
func (f *fakeAPI) Whoami(context.Context) (*mautrix.RespWhoami, error) {
	return &mautrix.RespWhoami{UserID: "@bot:example.org"}, nil
}
