package web

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// clientIP decides which address `tailscale whois` is asked about, and that
// answer decides whether the caller is treated as the tailnet owner — i.e. a
// relay admin. A caller who can set that value can claim to be the owner, so
// these cases are the authorization boundary, not a formatting detail.
func TestClientIPTrustsForwardedHeadersOnlyFromLoopback(t *testing.T) {
	const ownerIP = "100.64.0.1"

	tests := []struct {
		name       string
		remoteAddr string
		realIP     string
		forwardFor string
		want       string
	}{
		{
			name:       "direct caller cannot claim another address",
			remoteAddr: "100.64.0.9:44444",
			realIP:     ownerIP,
			want:       "100.64.0.9",
		},
		{
			name:       "direct caller cannot claim via X-Forwarded-For",
			remoteAddr: "203.0.113.7:44444",
			forwardFor: ownerIP + ", 10.0.0.1",
			want:       "203.0.113.7",
		},
		{
			name:       "loopback proxy may set X-Real-IP",
			remoteAddr: "127.0.0.1:51000",
			realIP:     ownerIP,
			want:       ownerIP,
		},
		{
			name:       "loopback proxy may set X-Forwarded-For",
			remoteAddr: "127.0.0.1:51000",
			forwardFor: ownerIP + ", 10.0.0.1",
			want:       ownerIP,
		},
		{
			name:       "IPv6 loopback counts as loopback",
			remoteAddr: "[::1]:51000",
			realIP:     ownerIP,
			want:       ownerIP,
		},
		{
			name:       "loopback with no headers falls back to itself",
			remoteAddr: "127.0.0.1:51000",
			want:       "127.0.0.1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &http.Request{RemoteAddr: tc.remoteAddr, Header: http.Header{}}
			if tc.realIP != "" {
				r.Header.Set("X-Real-IP", tc.realIP)
			}
			if tc.forwardFor != "" {
				r.Header.Set("X-Forwarded-For", tc.forwardFor)
			}
			if got := clientIP(r); got != tc.want {
				t.Fatalf("clientIP() = %q, want %q", got, tc.want)
			}
		})
	}
}

// handleSend is the only inbound path, and it is gated by verify() → whois →
// owner comparison. These cover the refusals; the accept path needs a real
// tailnet peer and belongs in a live check rather than a unit test.
func TestHandleSendRefusals(t *testing.T) {
	f := &Frontend{
		convID: "web-test", fromName: "Test", owner: "owner@example.com",
		logger:  log.New(io.Discard, "", 0),
		out:     make(chan relay.Message, 4),
		clients: map[chan string]struct{}{},
	}

	// Wrong method.
	rec := httptest.NewRecorder()
	f.handleSend(rec, httptest.NewRequest(http.MethodGet, "/send", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /send: got %d, want 405", rec.Code)
	}

	// An address that cannot resolve to the owner must be refused. Whether
	// whois errors or returns some other login, the answer is the same: deny.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/send", strings.NewReader(`{"text":"hello"}`))
	req.RemoteAddr = "203.0.113.9:40000"
	f.handleSend(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("unverified POST: got %d, want 403", rec.Code)
	}
	select {
	case m := <-f.out:
		t.Fatalf("an unverified request reached the broker: %q", m.Text)
	default:
	}
}

// Send fans out to every connected SSE client and must not block on one that
// has stopped reading — a wedged browser tab cannot be allowed to stall a turn.
func TestSendFansOutAndDropsForSlowClients(t *testing.T) {
	f := &Frontend{
		convID: "web-test", logger: log.New(io.Discard, "", 0),
		out:     make(chan relay.Message, 4),
		clients: map[chan string]struct{}{},
	}

	live := make(chan string, 1)
	wedged := make(chan string) // unbuffered and never read
	f.mu.Lock()
	f.clients[live] = struct{}{}
	f.clients[wedged] = struct{}{}
	f.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- f.Send(context.Background(), relay.AssistantMsg("web-test", "hello there")) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on a client that stopped reading")
	}

	select {
	case got := <-live:
		if !strings.Contains(got, "hello there") {
			t.Fatalf("live client got %q", got)
		}
	default:
		t.Fatal("live client received nothing")
	}
}
