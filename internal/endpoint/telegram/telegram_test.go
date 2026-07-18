package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/endpoint/senderr"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

const testToken = "TESTTOKEN"

// TestMeHandshake covers the getMe connection handshake: success identifies the
// bot; a non-ok response is a clear error.
func TestMeHandshake(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+testToken+"/getMe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true,"result":{"id":8656983200,"username":"thinkpt480bot","first_name":"thinkbot"}}`)
	})
	mux.HandleFunc("/botBADTOKEN/getMe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":false,"description":"Unauthorized"}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Valid token → bot identity.
	good := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithPollTimeout(0))
	defer good.Close()
	info, err := good.Me(context.Background())
	if err != nil {
		t.Fatalf("Me: %v", err)
	}
	if info.Username != "thinkpt480bot" || info.ID != 8656983200 {
		t.Fatalf("wrong bot info: %+v", info)
	}

	// Bad token → clear error.
	bad := New("BADTOKEN", WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithPollTimeout(0))
	defer bad.Close()
	if _, err := bad.Me(context.Background()); err == nil {
		t.Fatal("expected error for bad token")
	}
}

// TestPollGateAndSend verifies inbound allowlist gating, message normalization,
// and outbound sendMessage — all against an httptest server, no real bot.
func TestPollGateAndSend(t *testing.T) {
	sent := make(chan map[string]any, 1)

	mux := http.NewServeMux()
	// getUpdates: first call (no offset) returns two messages — one allowed
	// (from 111), one not (from 999). Later calls (with offset) return empty.
	mux.HandleFunc("/bot"+testToken+"/getUpdates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("offset") == "" {
			fmt.Fprint(w, `{"ok":true,"result":[
				{"update_id":10,"message":{"message_id":1,"from":{"id":111,"username":"jean"},"chat":{"id":222,"type":"private"},"text":"hello"}},
				{"update_id":11,"message":{"message_id":2,"from":{"id":999,"username":"stranger"},"chat":{"id":333,"type":"private"},"text":"spam"}}
			]}`)
			return
		}
		fmt.Fprint(w, `{"ok":true,"result":[]}`)
	})
	// sendMessage: capture the payload.
	mux.HandleFunc("/bot"+testToken+"/sendMessage", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		sent <- body
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true}`)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := New(testToken,
		WithBaseURL(srv.URL),
		WithHTTPClient(srv.Client()),
		WithAllowlist(111), // only user 111 is permitted
		WithPollTimeout(0),
	)
	defer f.Close()

	// 1. Inbound: only the allowed sender's message should surface.
	select {
	case msg := <-f.Recv():
		if msg.Text != "hello" {
			t.Fatalf("expected 'hello', got %q", msg.Text)
		}
		if msg.ConversationID != "222" || msg.Meta["chat_id"] != "222" {
			t.Fatalf("wrong conversation/chat id: %+v", msg)
		}
		if msg.Meta["from_id"] != "111" {
			t.Fatalf("wrong from_id: %v", msg.Meta["from_id"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for allowed inbound message")
	}

	// 2. The disallowed sender (999) must NOT produce a second message.
	select {
	case extra := <-f.Recv():
		t.Fatalf("unexpected message from gated sender: %+v", extra)
	case <-time.After(300 * time.Millisecond):
		// good: nothing leaked through
	}

	// 3. Outbound: Send hits sendMessage with the right chat_id + text.
	err := f.Send(context.Background(), relay.Message{
		ConversationID: "222",
		Role:           relay.Assistant,
		Text:           "hi back",
		Meta:           map[string]string{"chat_id": "222"},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case body := <-sent:
		if body["chat_id"] != "222" || body["text"] != "hi back" {
			t.Fatalf("wrong sendMessage payload: %+v", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sendMessage was not called")
	}
}

// TestGroupChatDropped verifies messages from a non-private chat (group/
// supergroup/channel) are refused outright, even from an allowlisted
// sender - the admin session gate keys on chat_id under the assumption
// that chat_id == from_id, which only holds in a private 1:1 chat.
func TestGroupChatDropped(t *testing.T) {
	sent := make(chan map[string]any, 4)

	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+testToken+"/getUpdates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("offset") == "" {
			fmt.Fprint(w, `{"ok":true,"result":[
				{"update_id":10,"message":{"message_id":1,"from":{"id":111,"username":"jean"},"chat":{"id":555,"type":"group"},"text":"hello from a group"}},
				{"update_id":11,"message":{"message_id":2,"from":{"id":111,"username":"jean"},"chat":{"id":111,"type":"private"},"text":"hello from DM"}}
			]}`)
			return
		}
		fmt.Fprint(w, `{"ok":true,"result":[]}`)
	})
	mux.HandleFunc("/bot"+testToken+"/sendMessage", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		sent <- body
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true}`)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := New(testToken,
		WithBaseURL(srv.URL),
		WithHTTPClient(srv.Client()),
		WithAllowlist(111),
		WithPollTimeout(0),
	)
	defer f.Close()

	// Only the private-chat message should surface - the group one is dropped.
	select {
	case msg := <-f.Recv():
		if msg.Text != "hello from DM" {
			t.Fatalf("expected the DM message, got %q (group message leaked through)", msg.Text)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for the private-chat message")
	}
	select {
	case extra := <-f.Recv():
		t.Fatalf("group-chat message leaked through: %+v", extra)
	case <-time.After(300 * time.Millisecond):
		// good — dropped
	}
}

// TestSendRetryQueue verifies a failed Send is retried in the background and
// eventually delivered once the endpoint recovers, since every caller
// discards Send's error (`_ = frontend.Send(...)`) and relies on this
// background path to keep a transient failure from silently vanishing.
func TestSendRetryQueue(t *testing.T) {
	sent := make(chan map[string]any, 4)
	var failFirst atomic.Bool
	failFirst.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+testToken+"/getUpdates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true,"result":[]}`)
	})
	mux.HandleFunc("/bot"+testToken+"/sendMessage", func(w http.ResponseWriter, r *http.Request) {
		if failFirst.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		sent <- body
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true}`)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithPollTimeout(0))
	defer f.Close()

	// First attempt fails - Send() still returns the error immediately
	// (unchanged caller-facing behavior), but the message is queued.
	err := f.Send(context.Background(), relay.Message{
		ConversationID: "222",
		Text:           "resend me",
		Meta:           map[string]string{"chat_id": "222"},
	})
	if err == nil {
		t.Fatal("expected the first attempt to fail")
	}
	if got := f.SendFailures(); got != 1 {
		t.Fatalf("SendFailures = %d, want 1", got)
	}
	if got := f.QueueDepth(); got != 1 {
		t.Fatalf("QueueDepth = %d, want 1", got)
	}

	// Endpoint recovers - the background retry worker's first backoff is
	// 1s, so give it a few seconds to succeed.
	failFirst.Store(false)
	select {
	case body := <-sent:
		if body["chat_id"] != "222" || body["text"] != "resend me" {
			t.Fatalf("wrong redelivered payload: %+v", body)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("queued message was never redelivered")
	}
}

// TestOversizedMessageSplitAndDelivered verifies that a message over
// Telegram's 4096-char limit is split via senderr.Split and every chunk is
// delivered in order, rather than being dropped or merely reported as an
// error with nothing reaching the user.
func TestOversizedMessageSplitAndDelivered(t *testing.T) {
	var sendMessageCalls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+testToken+"/getUpdates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true,"result":[]}`)
	})
	mux.HandleFunc("/bot"+testToken+"/sendMessage", func(w http.ResponseWriter, r *http.Request) {
		sendMessageCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":1}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithPollTimeout(0))
	defer f.Close()

	oversized := strings.Repeat("x", maxMessageLen*2+500)
	err := f.Send(context.Background(), relay.Message{
		ConversationID: "333",
		Text:           oversized,
		Meta:           map[string]string{"chat_id": "333"},
	})
	if err != nil {
		t.Fatalf("Send returned an error for an oversized message that should have been split: %v", err)
	}
	if got := sendMessageCalls.Load(); got < 2 {
		t.Errorf("sendMessage called %d time(s), want at least 2 - the message should have been split into multiple chunks", got)
	}
	if got := f.QueueDepth(); got != 0 {
		t.Errorf("QueueDepth = %d, want 0 - a fully successful split-send must not be queued for retry", got)
	}
}

// TestPermanentHTTPErrorNotQueued mirrors the oversized-message case for a
// deterministic 4xx Telegram actually returns (as opposed to the client-side
// length guard) - a 400 will fail identically on retry, so it must not be
// queued either. 429 is the one 4xx that IS worth retrying and is exempted.
func TestPermanentHTTPErrorNotQueued(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+testToken+"/getUpdates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true,"result":[]}`)
	})
	mux.HandleFunc("/bot"+testToken+"/sendMessage", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"ok":false,"description":"Bad Request: message is too long"}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithPollTimeout(0))
	defer f.Close()

	err := f.Send(context.Background(), relay.Message{
		ConversationID: "444",
		Text:           "short but the server still 400s",
		Meta:           map[string]string{"chat_id": "444"},
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := f.QueueDepth(); got != 0 {
		t.Errorf("QueueDepth = %d, want 0 - a deterministic 400 must not be queued for retry", got)
	}
}

// TestSplitMidFailureQueuesRemainingInOrder verifies that when a chunk in the
// middle of a split reply hits a transient failure, later chunks are not
// sent immediately (which could deliver them before the retried chunk and
// scramble the reply's order) - they're queued for background retry instead,
// in original order.
func TestSplitMidFailureQueuesRemainingInOrder(t *testing.T) {
	var sendMessageCalls atomic.Int64
	var failSecondOnward atomic.Bool

	mux := http.NewServeMux()
	mux.HandleFunc("/bot"+testToken+"/getUpdates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true,"result":[]}`)
	})
	mux.HandleFunc("/bot"+testToken+"/sendMessage", func(w http.ResponseWriter, r *http.Request) {
		n := sendMessageCalls.Add(1)
		if n >= 2 && failSecondOnward.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":1}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := New(testToken, WithBaseURL(srv.URL), WithHTTPClient(srv.Client()), WithPollTimeout(0))
	defer f.Close()

	failSecondOnward.Store(true)
	oversized := strings.Repeat("x", maxMessageLen*3+500)
	err := f.Send(context.Background(), relay.Message{
		ConversationID: "555",
		Text:           oversized,
		Meta:           map[string]string{"chat_id": "555"},
	})
	if err == nil {
		t.Fatal("expected an error from the mid-split transient failure")
	}

	// Chunk 0 succeeds and chunk 1 is attempted and fails - that's the only
	// two synchronous sendMessage calls. Every chunk after the failure must
	// be queued rather than attempted out of turn.
	if got := sendMessageCalls.Load(); got != 2 {
		t.Fatalf("sendMessage called %d time(s) synchronously, want exactly 2 (remaining chunks must be queued, not sent immediately)", got)
	}
	wantQueued := int64(len(senderr.Split(oversized, maxMessageLen))) - 1
	if got := f.QueueDepth(); got != wantQueued {
		t.Fatalf("QueueDepth = %d, want %d (the failed chunk plus every chunk after it)", got, wantQueued)
	}
}
