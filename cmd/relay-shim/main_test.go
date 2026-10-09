package main

import (
	"context"
	"io"
	"log"
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
	r := replyReminder("555000111")
	// Assert the contract, not its phrasing: name the tool, name the exact
	// destination, say terminal text is not delivered, and say the "[to: ...]"
	// label is not itself a send - the model read the label as a delivery and
	// stopped calling the tool (2026-07-20).
	for _, want := range []string{"reply tool", `chat_id="555000111"`,
		"never delivered", "does not send it", "[to: terminal]"} {
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

	err := handler(context.Background(), "555000111", "hello")
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

	if err := handler(context.Background(), "555000111", "hello"); err != nil {
		t.Fatalf("expected nil error on a successful ack, got %v", err)
	}
}

// A request that times out tells the caller the message may not have been
// delivered and to re-send. The queued frame must therefore not also flush
// when the daemon returns — otherwise a brief outage produces two deliveries,
// or two armed schedules for schedule_message.
func TestAbandonedFrameIsDroppedNotFlushedOnReconnect(t *testing.T) {
	c := &client{logger: log.New(io.Discard, "", 0), out: make(chan ipc.Frame, 4)}

	// One frame whose caller gave up, one that is still wanted.
	c.abandon("7")
	c.out <- ipc.Frame{Kind: "reply", RequestID: "7", Text: "duplicate"}
	c.out <- ipc.Frame{Kind: "reply", RequestID: "8", Text: "wanted"}
	close(c.out)

	var sent []ipc.Frame
	done := make(chan struct{})
	go func() {
		defer close(done)
		for f := range c.out {
			if c.takeAbandoned(f.RequestID) {
				continue
			}
			sent = append(sent, f)
		}
	}()
	<-done

	if len(sent) != 1 {
		t.Fatalf("sent %d frame(s), want 1: %+v", len(sent), sent)
	}
	if sent[0].RequestID != "8" {
		t.Fatalf("sent frame %q, want the one still wanted (8)", sent[0].RequestID)
	}
}

func TestTakeAbandonedIsOneShotAndIgnoresEmptyIDs(t *testing.T) {
	c := &client{logger: log.New(io.Discard, "", 0)}

	// Frames with no RequestID are fire-and-forget; they must never be
	// mistaken for abandoned.
	if c.takeAbandoned("") {
		t.Fatal("empty RequestID reported as abandoned")
	}

	c.abandon("3")
	if !c.takeAbandoned("3") {
		t.Fatal("first take should report abandoned")
	}
	if c.takeAbandoned("3") {
		t.Fatal("second take should not: the record is consumed")
	}

	// forget clears a record for a frame that went out before its caller
	// gave up, so the map does not accumulate.
	c.abandon("4")
	c.forget("4")
	if c.takeAbandoned("4") {
		t.Fatal("forget should have cleared the record")
	}
}
