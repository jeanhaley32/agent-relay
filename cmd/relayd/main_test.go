package main

import (
	"errors"
	"testing"

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
