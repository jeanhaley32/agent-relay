package main

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/jeanhaley32/agent-relay/internal/access"
	"github.com/jeanhaley32/agent-relay/internal/command"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/senderr"
)

// TestAckErrTextClassification exercises the permanent-vs-transient
// classification used by the real AckBackendReply closure: only a
// senderr.Permanent failure should be surfaced back to the reply tool call,
// so a transient failure that Frontend.Send has already queued for
// background retry doesn't invite the model to resend and duplicate
// delivery once the retry lands.
func TestAckErrTextClassification(t *testing.T) {
	permErr := senderr.Permanent{Err: errors.New("chat_id is not an allowed destination")}
	transientErr := errors.New("telegram sendMessage status 500: try again")

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil error", nil, ""},
		{"transient error suppressed", transientErr, ""},
		{"permanent error surfaced", permErr, permErr.Error()},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ackErrText(c.err)
			if got != c.want {
				t.Errorf("ackErrText(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}

	// A permanent error wrapped by fmt.Errorf("%w", ...) must still be
	// detected via errors.As, not just a direct type assertion.
	wrapped := errWrap{permErr}
	if got := ackErrText(wrapped); got != wrapped.Error() {
		t.Errorf("ackErrText(wrapped permanent) = %q, want %q", got, wrapped.Error())
	}
}

type errWrap struct{ err error }

func (e errWrap) Error() string { return "wrapped: " + e.err.Error() }
func (e errWrap) Unwrap() error { return e.err }

// TestOutboundAllowed exercises the multi-frontend outbound gate: Telegram
// int64 ids, Discord snowflake ids, and the KnownConversation fallback for
// Discord guild channels/DMs the frontend has already gated inbound.
func TestOutboundAllowed(t *testing.T) {
	tgAcc := access.New([]int64{1}, []int64{42}, "", nil)
	discAcc := access.New([]int64{2}, []int64{99}, "", nil)
	known := func(id string) bool { return id == "known-chan" }

	cases := []struct {
		name       string
		chatID     string
		discordAcc *access.Manager
		known      func(string) bool
		want       bool
	}{
		{"telegram allowlisted", "42", discAcc, known, true},
		{"telegram not allowlisted", "7", discAcc, known, false},
		{"discord snowflake allowlisted", strconv.FormatInt(99, 10), discAcc, nil, true},
		{"known conversation fallback", "known-chan", discAcc, known, true},
		{"unknown non-numeric chat", "unknown-chan", discAcc, known, false},
		{"nil discordAcc and known", "42", nil, nil, true},
		{"nil discordAcc, unknown", "not-allowed", nil, nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := outboundAllowed(c.chatID, tgAcc, c.discordAcc, c.known)
			if got != c.want {
				t.Errorf("outboundAllowed(%q) = %v, want %v", c.chatID, got, c.want)
			}
		})
	}

	// A real Discord snowflake (large, non-Telegram-colliding value) allowed
	// only in the Discord manager must pass via the Discord branch.
	discOnly := access.New(nil, []int64{123456789012345678}, "", nil)
	if !outboundAllowed("123456789012345678", access.New(nil, nil, "", nil), discOnly, nil) {
		t.Error("outboundAllowed: expected discord-allowlisted snowflake to pass")
	}
}

// TestHandshakeMergesManagers verifies that /handshake listing and
// approve/deny try every manager in turn, so an admin on either frontend can
// resolve a request recorded by any manager without knowing which one it
// came from.
func TestHandshakeMergesManagers(t *testing.T) {
	m1 := access.New(nil, nil, "", nil)
	m2 := access.New(nil, nil, "", nil)
	m1.Record(111, "alice")
	m2.Record(222, "bob")

	managers := func() []*access.Manager { return []*access.Manager{m1, m2} }
	h := handshake(managers)

	list := h(command.Context{}, nil)
	if !contains(list, "alice") || !contains(list, "bob") {
		t.Errorf("handshake listing missing entries, got: %q", list)
	}

	// Approve the request recorded on m2 while only m1 "knows" about m1's
	// own pending id - approve must fall through to whichever manager has it.
	got := h(command.Context{}, []string{"approve", "222"})
	if !contains(got, "approved 222") {
		t.Errorf("approve on m2's id via merged managers: got %q", got)
	}
	if !m2.Allowed(222) {
		t.Error("expected id 222 to be allowed on m2 after approve")
	}

	// Deny the request recorded on m1.
	got = h(command.Context{}, []string{"deny", "111"})
	if !contains(got, "denied 111") {
		t.Errorf("deny on m1's id via merged managers: got %q", got)
	}

	// An id no manager has pending must fail on both approve and deny.
	got = h(command.Context{}, []string{"approve", "999"})
	if !contains(got, "not approved") {
		t.Errorf("approve on unknown id: got %q", got)
	}
	got = h(command.Context{}, []string{"deny", "999"})
	if !contains(got, "not pending") {
		t.Errorf("deny on unknown id: got %q", got)
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
