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
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
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

// TestGateGuildMessagesDisallowedByDefault ensures that with
// allowGuildMessages=false (the default), every guild message is dropped
// regardless of allowedGuildIDs, mention state, or sender allowlist status.
func TestGateGuildMessagesDisallowedByDefault(t *testing.T) {
	someGuild := snowflake.ID(500)
	f := &Frontend{
		auth:                  &recordingAuth{allowed: map[snowflake.ID]bool{111: true}},
		logger:                testLogger(t),
		allowGuildMessages:    false,
		allowedGuildIDs:       map[snowflake.ID]bool{someGuild: true},
		requireMentionInGuild: true,
	}

	if _, ok := f.gate(inboundMessage{
		authorID: 111, guildID: &someGuild, content: "@bot hi", mentionsBot: true,
	}); ok {
		t.Fatalf("expected guild message to be dropped when allowGuildMessages is false")
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

// TestSendOversizedSplitAndDelivered verifies that a message over Discord's
// 2000-char limit is split via senderr.Split and every chunk is delivered
// in order, rather than being dropped.
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

// TestOwnsConversationID covers the snowflake-magnitude heuristic that
// disambiguates a bare numeric ConversationID between frontends: below the
// floor, non-numeric, and a real Discord-sized id.
func TestOwnsConversationID(t *testing.T) {
	f := &Frontend{}

	if f.OwnsConversationID("12345") {
		t.Fatalf("expected a small (Telegram-range) numeric id to not be owned")
	}
	if f.OwnsConversationID("not-a-number") {
		t.Fatalf("expected a non-numeric id to not be owned")
	}
	if !f.OwnsConversationID("111111111111111111") {
		t.Fatalf("expected a real-magnitude Discord snowflake to be owned")
	}
}

// TestKnownConversationGuildChannelStaysAllowed covers the guild-channel
// branch: once gate() has recorded a channel id in convChannels, it stays
// known even though it's never a valid entry in the user-id-keyed auth
// manager.
func TestKnownConversationGuildChannelStaysAllowed(t *testing.T) {
	f := &Frontend{auth: &recordingAuth{allowed: map[snowflake.ID]bool{}}}
	f.convChannels.Store("42", snowflake.ID(42))
	// Deliberately not stored in dmConvs - mirrors gate() never marking a
	// guild channel id as a DM.
	if !f.KnownConversation("42") {
		t.Fatalf("expected a known guild channel id to remain known")
	}
}

// TestKnownConversationDMRevoked covers the security-relevant revoke path: a
// DM sender removed from the allowlist after an earlier authorized DM must
// no longer be reachable for outbound replies, even though convChannels
// still has a cached entry for them.
func TestKnownConversationDMRevoked(t *testing.T) {
	auth := &recordingAuth{allowed: map[snowflake.ID]bool{111: true}}
	f := &Frontend{auth: auth}
	f.convChannels.Store("111", snowflake.ID(111))
	f.dmConvs.Store("111", struct{}{})

	if !f.KnownConversation("111") {
		t.Fatalf("expected a currently-allowed DM sender to be known")
	}

	delete(auth.allowed, 111)
	if f.KnownConversation("111") {
		t.Fatalf("expected a DM sender removed from the allowlist to no longer be known")
	}
}

// TestOnMessageCreateRecvFullDrop covers the recv-full drop path in
// onMessageCreate directly (not via gate(), which every other inbound test
// uses): when f.recv is still full after the 5s wait, the message must be
// counted in recvDrops rather than blocking forever.
func TestOnMessageCreateRecvFullDrop(t *testing.T) {
	f := &Frontend{
		auth:            &recordingAuth{allowed: map[snowflake.ID]bool{111: true}},
		logger:          testLogger(t),
		allowedGuildIDs: map[snowflake.ID]bool{},
		recv:            make(chan relay.Message, 1),
	}
	// Fill the buffered recv channel so the next send has nowhere to go.
	f.recv <- relay.Message{Text: "filler"}

	msg := &events.MessageCreate{GenericMessage: &events.GenericMessage{
		Message: discord.Message{
			ID:        1,
			ChannelID: 9001,
			Author:    discord.User{ID: 111, Username: "jean"},
			Content:   "hello",
		},
	}}

	start := time.Now()
	f.onMessageCreate(msg)
	if elapsed := time.Since(start); elapsed < 5*time.Second {
		t.Fatalf("expected onMessageCreate to wait out the full 5s send timeout, only waited %s", elapsed)
	}
	if got := f.RecvDrops(); got != 1 {
		t.Fatalf("RecvDrops() = %d, want 1", got)
	}
}

// TestEnqueueRetryQueueFullEviction covers the queue-full eviction path: when
// the retry queue is at capacity, enqueueRetry must drop the oldest item
// (counting it in permanentDrops and decrementing queueDepth) rather than
// blocking the caller, and still admit the new item.
func TestEnqueueRetryQueueFullEviction(t *testing.T) {
	f := &Frontend{
		logger:     testLogger(t),
		retryQueue: make(chan retryItem, 2),
	}

	f.enqueueRetry(relay.Message{Text: "one"})
	f.enqueueRetry(relay.Message{Text: "two"})
	if f.queueDepth.Load() != 2 {
		t.Fatalf("expected queueDepth 2 after filling capacity, got %d", f.queueDepth.Load())
	}

	// Queue is now full (capacity 2); this must evict "one" rather than block.
	f.enqueueRetry(relay.Message{Text: "three"})

	if f.permanentDrops.Load() != 1 {
		t.Fatalf("expected permanentDrops 1 after eviction, got %d", f.permanentDrops.Load())
	}
	if f.queueDepth.Load() != 2 {
		t.Fatalf("expected queueDepth back at 2 after eviction, got %d", f.queueDepth.Load())
	}

	var got []string
	close(f.retryQueue)
	for item := range f.retryQueue {
		got = append(got, item.msg.Text)
	}
	if len(got) != 2 || got[0] != "two" || got[1] != "three" {
		t.Fatalf("expected remaining queue [two three], got %v", got)
	}
}

// TestStartRetryWorkerDeliversQueuedMessage exercises the retry worker
// end-to-end: a message queued via enqueueRetry is eventually delivered by
// startRetryWorker's background loop, with queueDepth dropping back to 0 on
// success.
func TestStartRetryWorkerDeliversQueuedMessage(t *testing.T) {
	delivered := make(chan struct{}, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/channels/123/messages", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"1","channel_id":"123","content":"hi","author":{"id":"1","username":"bot"}}`)
		select {
		case delivered <- struct{}{}:
		default:
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f, err := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithLogger(testLogger(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer f.Close()

	f.enqueueRetry(relay.Message{Text: "hi", Meta: map[string]string{"channel_id": "123"}})
	if f.queueDepth.Load() != 1 {
		t.Fatalf("expected queueDepth 1 right after enqueue, got %d", f.queueDepth.Load())
	}

	select {
	case <-delivered:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the retry worker to deliver the queued message")
	}

	deadline := time.Now().Add(2 * time.Second)
	for f.queueDepth.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if f.queueDepth.Load() != 0 {
		t.Fatalf("expected queueDepth 0 after successful retry, got %d", f.queueDepth.Load())
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
