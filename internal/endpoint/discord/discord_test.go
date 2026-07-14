package discord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/disgoorg/snowflake/v2"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

const testToken = "TESTTOKEN"

// TestGateSenderAllowlist covers DESIGN.md §3: an allowed sender's DM is
// relayed, a denied sender's DM is dropped and recorded as pending. Exercised
// directly against gate() — no gateway/websocket needed, per DESIGN.md §8's
// "injectable ... unit-testable without a real bot token" requirement.
func TestGateSenderAllowlist(t *testing.T) {
	rec := &recordingAuth{allowed: map[snowflake.ID]bool{111: true}}
	f := &Frontend{
		auth:                  rec,
		logger:                testLogger(t),
		allowedGuildIDs:       map[snowflake.ID]bool{},
		requireMentionInGuild: true,
	}

	allowedMsg := inboundMessage{
		messageID:  1,
		channelID:  9001,
		authorID:   111,
		authorName: "jean",
		content:    "hello",
	}
	msg, ok := f.gate(allowedMsg)
	if !ok {
		t.Fatalf("expected allowed sender's DM to be relayed")
	}
	if msg.Text != "hello" || msg.Meta["from_id"] != "111" || msg.ConversationID != "111" {
		t.Fatalf("unexpected relay.Message: %+v", msg)
	}
	// DM: chat_id should equal the sender id (Option B, §3), not the channel id.
	if msg.Meta["chat_id"] != "111" {
		t.Fatalf("chat_id should be the sender id for DMs, got %q", msg.Meta["chat_id"])
	}
	if msg.Meta["channel_id"] != "9001" {
		t.Fatalf("channel_id should still track the physical channel, got %q", msg.Meta["channel_id"])
	}

	deniedMsg := inboundMessage{
		messageID:  2,
		channelID:  9002,
		authorID:   999,
		authorName: "stranger",
		content:    "spam",
	}
	if _, ok := f.gate(deniedMsg); ok {
		t.Fatalf("expected denied sender's DM to be dropped")
	}
	if !rec.recorded[999] {
		t.Fatalf("expected denied sender to be recorded as a pending request")
	}
}

// TestGateGuildPolicy covers DESIGN.md §3's guild policy: non-allowed guild
// dropped before the sender check even runs; allowed guild but no
// mention/reply dropped; allowed guild with a mention relayed.
func TestGateGuildPolicy(t *testing.T) {
	allowedGuild := snowflake.ID(500)
	f := &Frontend{
		auth:                  &recordingAuth{allowed: map[snowflake.ID]bool{111: true}},
		logger:                testLogger(t),
		allowGuildMessages:    true,
		allowedGuildIDs:       map[snowflake.ID]bool{allowedGuild: true},
		requireMentionInGuild: true,
	}

	otherGuild := snowflake.ID(999)
	if _, ok := f.gate(inboundMessage{authorID: 111, guildID: &otherGuild, content: "hi", mentionsBot: true}); ok {
		t.Fatalf("expected message from non-allowed guild to be dropped")
	}

	if _, ok := f.gate(inboundMessage{authorID: 111, guildID: &allowedGuild, content: "ambient chatter"}); ok {
		t.Fatalf("expected unaddressed guild message to be dropped")
	}

	msg, ok := f.gate(inboundMessage{
		authorID: 111, authorName: "jean", channelID: 42, guildID: &allowedGuild,
		content: "@bot hi", mentionsBot: true,
	})
	if !ok {
		t.Fatalf("expected mentioned message in allowed guild to be relayed")
	}
	// Guild messages: chat_id is the channel id (per-channel conversation scoping).
	if msg.Meta["chat_id"] != "42" || msg.ConversationID != "42" {
		t.Fatalf("unexpected guild chat_id/ConversationID: %+v", msg)
	}
	if msg.Meta["guild_id"] != "500" {
		t.Fatalf("expected guild_id meta, got %+v", msg)
	}

	// A reply to the bot's own message satisfies the mention requirement too.
	if _, ok := f.gate(inboundMessage{
		authorID: 111, guildID: &allowedGuild, content: "yes", isReplyToBot: true,
	}); !ok {
		t.Fatalf("expected reply-to-bot in allowed guild to be relayed")
	}
}

// TestGateDropsBots ensures messages authored by other bots (including the
// frontend's own echoes) are never relayed, to avoid bot-loops.
func TestGateDropsBots(t *testing.T) {
	f := &Frontend{auth: &recordingAuth{allowed: map[snowflake.ID]bool{111: true}}, logger: testLogger(t)}
	if _, ok := f.gate(inboundMessage{authorID: 111, authorIsBot: true, content: "hi"}); ok {
		t.Fatalf("expected bot-authored message to be dropped")
	}
}

// TestSendTooLong verifies Discord's 2000-char cap is enforced before any
// REST call, and classified as a permanent (non-retryable) error — mirroring
// telegram's maxMessageLen handling per DESIGN.md §4.
func TestSendTooLong(t *testing.T) {
	f := &Frontend{logger: testLogger(t)}
	longText := strings.Repeat("x", maxMessageLen+1)
	err := f.sendOnce(context.Background(), relay.Message{
		Text: longText,
		Meta: map[string]string{"channel_id": "123"},
	})
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
	var perm permanentSendError
	if !errors.As(err, &perm) {
		t.Fatalf("expected a permanent (non-retryable) error, got %v (%T)", err, err)
	}
}

// TestSendMissingChannelID verifies a missing destination is also permanent
// (DESIGN.md §4): retrying can't conjure a channel id that was never set.
func TestSendMissingChannelID(t *testing.T) {
	f := &Frontend{logger: testLogger(t)}
	err := f.sendOnce(context.Background(), relay.Message{Text: "hi"})
	if err == nil {
		t.Fatal("expected error for missing channel_id")
	}
	var perm permanentSendError
	if !errors.As(err, &perm) {
		t.Fatalf("expected a permanent (non-retryable) error, got %v (%T)", err, err)
	}
}

// TestSendRoundTrip exercises Send()'s happy path against a real REST
// (httptest) server, mirroring telegram_test.go's TestPollGateAndSend
// send-half: New() wires up disgo's rest.Channels pointed at the fake
// server, and we verify the outbound payload.
func TestSendRoundTrip(t *testing.T) {
	sent := make(chan map[string]any, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/channels/123/messages", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		sent <- body
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"1","channel_id":"123","content":"hi","author":{"id":"1","username":"bot"}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f, err := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer f.Close()

	if err := f.Send(context.Background(), relay.Message{
		Text: "hi",
		Meta: map[string]string{"channel_id": "123"},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case body := <-sent:
		if body["content"] != "hi" {
			t.Fatalf("unexpected outbound payload: %+v", body)
		}
	default:
		t.Fatal("expected the fake server to receive a CreateMessage request")
	}
}

// TestSendOversizedSplitAndDelivered locks in the fix for the real
// 2026-07-14 incident: a message over Discord's 2000-char limit used to be
// permanently dropped with no error surfaced and no message ever delivered.
// Send now splits it via senderr.Split and delivers every chunk in order.
func TestSendOversizedSplitAndDelivered(t *testing.T) {
	var callCount int
	mux := http.NewServeMux()
	mux.HandleFunc("/channels/123/messages", func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"1","channel_id":"123","content":"hi","author":{"id":"1","username":"bot"}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f, err := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer f.Close()

	oversized := strings.Repeat("x", maxMessageLen*2+500)
	if err := f.Send(context.Background(), relay.Message{
		Text: oversized,
		Meta: map[string]string{"channel_id": "123"},
	}); err != nil {
		t.Fatalf("Send returned an error for an oversized message that should have been split: %v", err)
	}
	if callCount < 2 {
		t.Errorf("CreateMessage called %d time(s), want at least 2 - the message should have been split into multiple chunks", callCount)
	}
}

// TestSend403Permanent covers DESIGN.md §8's incident-class review: a 403
// (missing SEND_MESSAGES permission, or any other non-429 4xx) must be
// classified permanent so it's never retried — the failure is guaranteed to
// repeat identically. Exercises sendOnce's *rest.Error branch for real
// against an httptest server, not just Meta/length checks.
func TestSend403Permanent(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/channels/123/messages", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"code":50013,"message":"Missing Permissions"}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f, err := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer f.Close()

	err = f.sendOnce(context.Background(), relay.Message{Text: "hi", Meta: map[string]string{"channel_id": "123"}})
	if err == nil {
		t.Fatal("expected an error for a 403 response")
	}
	var perm permanentSendError
	if !errors.As(err, &perm) {
		t.Fatalf("expected a permanent (non-retryable) error for 403, got %v (%T)", err, err)
	}
}

// TestSend429Retryable covers the other half of the same review item: a 429
// (rate limited) must NOT be classified permanent, even if one slips past
// disgo's own rate limiter and surfaces here.
func TestSend429Retryable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/channels/123/messages", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"message":"You are being rate limited.","retry_after":0.01,"global":false}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f, err := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer f.Close()

	err = f.sendOnce(context.Background(), relay.Message{Text: "hi", Meta: map[string]string{"channel_id": "123"}})
	if err == nil {
		t.Fatal("expected an error for a 429 response (disgo's rate limiter is expected to retry internally and eventually give up here)")
	}
	var perm permanentSendError
	if errors.As(err, &perm) {
		t.Fatalf("429 must NOT be classified permanent, got %v (%T)", err, err)
	}
}

// TestSendTooLongUsesRuneCount pins the byte-vs-codepoint fix: Discord's
// 2000 limit is 2000 CHARACTERS (code points), not bytes. A message with
// 1200 multibyte runes is well over 2000 bytes but must NOT be rejected.
func TestSendTooLongUsesRuneCount(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/channels/123/messages", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"1","channel_id":"123","content":"hi","author":{"id":"1","username":"bot"}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f, err := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer f.Close()

	text := strings.Repeat("é", 1200) // 1200 runes, 2400 bytes (UTF-8 2 bytes/rune)
	sendErr := f.sendOnce(context.Background(), relay.Message{
		Text: text,
		Meta: map[string]string{"channel_id": "123"},
	})
	var perm permanentSendError
	if errors.As(sendErr, &perm) {
		t.Fatalf("1200-rune message falsely classified too-long (byte count used instead of rune count): %v", sendErr)
	}
}

// TestSendResolvesDMChannelFromConversationID covers the DM-routing gap
// (DESIGN.md §3): a relayd-originated message (scheduler/admin/mcp reply)
// carries only ConversationID, never Meta["channel_id"]. For a DM,
// ConversationID is the recipient's user id (see gate()'s convID comment) —
// sendOnce must resolve/create the DM channel via CreateDMChannel rather
// than calling CreateMessage(<user id>) directly, which would 404.
func TestSendResolvesDMChannelFromConversationID(t *testing.T) {
	const userID = "111"
	const dmChannelID = "999"
	var gotDMPost, gotMessagePost bool

	mux := http.NewServeMux()
	mux.HandleFunc("/users/@me/channels", func(w http.ResponseWriter, r *http.Request) {
		gotDMPost = true
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["recipient_id"] != userID {
			t.Fatalf("expected recipient_id %q, got %v", userID, body["recipient_id"])
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":%q,"type":1}`, dmChannelID)
	})
	mux.HandleFunc("/channels/"+dmChannelID+"/messages", func(w http.ResponseWriter, r *http.Request) {
		gotMessagePost = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"1","channel_id":"999","content":"hi","author":{"id":"1","username":"bot"}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f, err := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer f.Close()

	// No Meta at all, and never previously seen inbound — exactly the shape
	// of a scheduled reminder built by cmd/relayd/main.go.
	if err := f.Send(context.Background(), relay.Message{ConversationID: userID, Text: "hi"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !gotDMPost {
		t.Fatal("expected a POST /users/@me/channels to resolve the DM channel")
	}
	if !gotMessagePost {
		t.Fatal("expected the message to be posted to the resolved DM channel")
	}
}

// --- test helpers ------------------------------------------------------------

type recordingAuth struct {
	allowed  map[snowflake.ID]bool
	recorded map[snowflake.ID]bool
}

func (r *recordingAuth) Allowed(id snowflake.ID) bool { return r.allowed[id] }
func (r *recordingAuth) Record(id snowflake.ID, name string) {
	if r.recorded == nil {
		r.recorded = map[snowflake.ID]bool{}
	}
	r.recorded[id] = true
}

// testLogger routes Frontend log output through t.Logf instead of stdout.
func testLogger(t *testing.T) *log.Logger { return log.New(testWriter{t}, "", 0) }

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
