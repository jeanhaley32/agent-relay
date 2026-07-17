package main

import (
	"strings"
	"testing"

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

// TestReplyAckTimeoutMatchesFrontendSendTimeout enforces the cross-package
// invariant asserted in both constants' comments: replyAckTimeout here and
// relay.FrontendSendTimeout must stay equal, or a send still outstanding at
// the shim's deadline stops being deterministically classifiable as failed,
// risking a duplicate reply once the daemon's own send eventually lands.
func TestReplyAckTimeoutMatchesFrontendSendTimeout(t *testing.T) {
	if replyAckTimeout != relay.FrontendSendTimeout {
		t.Fatalf("replyAckTimeout (%s) != relay.FrontendSendTimeout (%s) - these must be kept equal",
			replyAckTimeout, relay.FrontendSendTimeout)
	}
}
