package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
				{"update_id":10,"message":{"message_id":1,"from":{"id":111,"username":"jean"},"chat":{"id":222},"text":"hello"}},
				{"update_id":11,"message":{"message_id":2,"from":{"id":999,"username":"stranger"},"chat":{"id":333},"text":"spam"}}
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
