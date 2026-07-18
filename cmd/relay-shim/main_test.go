package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jeanhaley32/agent-relay/internal/ipc"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// TestReplyReminder: the per-message reinforcement must name the reply tool and
// carry the chat_id so the served model can act on it directly, and must warn
// that plain text is not delivered. This is the guard against the model drifting
// back to terminal-only answers over a long session.
func TestReplyReminder(t *testing.T) {
	r := replyReminder("6369276467")
	for _, want := range []string{"reply tool", `chat_id="6369276467"`, "NOT delivered"} {
		if !strings.Contains(r, want) {
			t.Fatalf("reminder missing %q: %s", want, r)
		}
	}
	// It must be a trailing addendum, not a prefix, so the user's own text leads.
	if !strings.HasPrefix(r, "\n") {
		t.Fatalf("reminder should start on its own line, got %q", r)
	}
}

// TestReplyAckTimeoutExceedsFrontendSendTimeout enforces the cross-package
// invariant asserted in both constants' comments: relay.FrontendSendTimeout
// must stay strictly less than replyAckTimeout here, so the broker gives up
// on a send and hands it to background retry before the shim's own wait
// expires.
func TestReplyAckTimeoutExceedsFrontendSendTimeout(t *testing.T) {
	if relay.FrontendSendTimeout >= replyAckTimeout {
		t.Fatalf("relay.FrontendSendTimeout (%s) must be strictly less than replyAckTimeout (%s)",
			relay.FrontendSendTimeout, replyAckTimeout)
	}
}

// TestReplyHandlerSurfacesAckError locks in the request/response reply tool's
// new behavior: a reply_ack frame carrying a non-empty Err must come back as
// a real error from the tool call, not be swallowed as a successful send.
func TestReplyHandlerSurfacesAckError(t *testing.T) {
	cl := &client{out: make(chan ipc.Frame, 1)}
	handler := replyHandler(cl)

	// Simulate the daemon's ack arriving on another goroutine once the
	// outbound reply frame has been enqueued, mirroring how onFrame->resolve
	// is wired up in main().
	go func() {
		f := <-cl.out
		cl.resolve(ipc.Frame{Kind: ipc.KindReplyAck, RequestID: f.RequestID, Err: "telegram: message too long"})
	}()

	err := handler(context.Background(), "6369276467", "hello")
	if err == nil {
		t.Fatal("expected an error from a reply_ack carrying resp.Err, got nil")
	}
	if err.Error() != "telegram: message too long" {
		t.Fatalf("unexpected error text: %v", err)
	}
}

// TestReplyHandlerSuccessReturnsNilError checks the counterpart: an ack with
// no Err must not be turned into an error.
func TestReplyHandlerSuccessReturnsNilError(t *testing.T) {
	cl := &client{out: make(chan ipc.Frame, 1)}
	handler := replyHandler(cl)

	go func() {
		f := <-cl.out
		cl.resolve(ipc.Frame{Kind: ipc.KindReplyAck, RequestID: f.RequestID})
	}()

	if err := handler(context.Background(), "6369276467", "hello"); err != nil {
		t.Fatalf("expected nil error on a successful ack, got %v", err)
	}
}
