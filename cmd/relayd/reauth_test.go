package main

import (
	"errors"
	"testing"
	"time"

	claudebk "github.com/jeanhaley32/agent-relay/internal/endpoint/claude"
)

// admins used across the reauth tests.
const (
	adminID    = "6369276467"
	strangerID = "999"
)

func gatedAdmin(id string) bool { return id == adminID }

// recordRevoke captures whether (and whom) the session revoke was called for.
type recordRevoke struct{ got []string }

func (r *recordRevoke) fn(id string) { r.got = append(r.got, id) }

// TestForceReauthDeniedDoesNotRevoke proves that without an admin approval
// (an explicit /deny) the target session is never revoked.
func TestForceReauthDeniedDoesNotRevoke(t *testing.T) {
	gate := newReauthGate()
	var rev recordRevoke
	var prompt string
	notify := func(text string) error { prompt = text; return nil }

	done := make(chan struct{})
	var result, errText string
	go func() {
		result, errText = handleReauth(
			claudebk.ReauthRequest{ChatID: adminID}, gate, gatedAdmin, rev.fn, notify, 5*time.Second)
		close(done)
	}()

	id := waitForPending(t, gate)
	if prompt == "" {
		t.Fatal("expected admins to be notified with an approval prompt")
	}
	if !gate.decide(id, false) {
		t.Fatalf("gate.decide did not recognize pending id %q", id)
	}
	<-done

	if len(rev.got) != 0 {
		t.Fatalf("denied request must not revoke; revoked %v", rev.got)
	}
	if errText == "" || result != "" {
		t.Fatalf("expected a denial error, got result=%q err=%q", result, errText)
	}
}

// TestForceReauthTimeoutDoesNotRevoke proves that no decision at all (the
// approval window elapses) also leaves the session untouched.
func TestForceReauthTimeoutDoesNotRevoke(t *testing.T) {
	gate := newReauthGate()
	var rev recordRevoke
	_, errText := handleReauth(
		claudebk.ReauthRequest{ChatID: adminID}, gate, gatedAdmin, rev.fn,
		func(string) error { return nil }, 20*time.Millisecond)

	if len(rev.got) != 0 {
		t.Fatalf("timed-out request must not revoke; revoked %v", rev.got)
	}
	if errText == "" {
		t.Fatal("expected a timeout error")
	}
}

// TestForceReauthApprovedRevokes proves that an admin /allow triggers exactly
// the /reauth effect (a session revoke) for the target admin.
func TestForceReauthApprovedRevokes(t *testing.T) {
	gate := newReauthGate()
	var rev recordRevoke

	done := make(chan struct{})
	var result, errText string
	go func() {
		result, errText = handleReauth(
			claudebk.ReauthRequest{ChatID: adminID}, gate, gatedAdmin, rev.fn,
			func(string) error { return nil }, 5*time.Second)
		close(done)
	}()

	id := waitForPending(t, gate)
	if !gate.decide(id, true) {
		t.Fatalf("gate.decide did not recognize pending id %q", id)
	}
	<-done

	if len(rev.got) != 1 || rev.got[0] != adminID {
		t.Fatalf("approved request must revoke the target once; revoked %v", rev.got)
	}
	if errText != "" || result == "" {
		t.Fatalf("expected a success result, got result=%q err=%q", result, errText)
	}
}

// TestForceReauthTargetsExplicitAdmin proves the explicit `admin` argument
// wins over the requesting conversation.
func TestForceReauthTargetsExplicitAdmin(t *testing.T) {
	gate := newReauthGate()
	// A second gated admin distinct from the requesting conversation.
	const otherAdmin = "111"
	isGated := func(id string) bool { return id == adminID || id == otherAdmin }
	var rev recordRevoke

	done := make(chan struct{})
	go func() {
		handleReauth(
			claudebk.ReauthRequest{ChatID: adminID, Target: otherAdmin}, gate, isGated, rev.fn,
			func(string) error { return nil }, 5*time.Second)
		close(done)
	}()
	id := waitForPending(t, gate)
	gate.decide(id, true)
	<-done

	if len(rev.got) != 1 || rev.got[0] != otherAdmin {
		t.Fatalf("expected revoke of explicit target %q, got %v", otherAdmin, rev.got)
	}
}

// TestForceReauthRefusesNonAdmin proves a non-admin target can never be
// re-authed (or even probed): it is refused before any approval is requested,
// so no prompt is sent and the session is never touched.
func TestForceReauthRefusesNonAdmin(t *testing.T) {
	gate := newReauthGate()
	var rev recordRevoke
	notified := false
	notify := func(string) error { notified = true; return nil }

	result, errText := handleReauth(
		claudebk.ReauthRequest{ChatID: strangerID}, gate, gatedAdmin, rev.fn, notify, 5*time.Second)

	if notified {
		t.Fatal("a non-admin target must not even generate an approval prompt")
	}
	if len(rev.got) != 0 {
		t.Fatalf("a non-admin target must never be revoked; revoked %v", rev.got)
	}
	if result != "" || errText == "" {
		t.Fatalf("expected a refusal error, got result=%q err=%q", result, errText)
	}
}

// TestForceReauthTargetOverrideCannotEscapeAdminCheck proves the explicit
// `admin` argument is itself subject to the gated-admin check — the model
// can't pass a non-admin id via `admin` to bypass the conversation default.
func TestForceReauthTargetOverrideCannotEscapeAdminCheck(t *testing.T) {
	gate := newReauthGate()
	var rev recordRevoke
	_, errText := handleReauth(
		claudebk.ReauthRequest{ChatID: adminID, Target: strangerID}, gate, gatedAdmin, rev.fn,
		func(string) error { return nil }, 5*time.Second)

	if len(rev.got) != 0 {
		t.Fatalf("non-admin explicit target must be refused; revoked %v", rev.got)
	}
	if errText == "" {
		t.Fatal("expected a refusal error for a non-admin explicit target")
	}
}

// TestReauthGateDecideUnknownID proves the gate reports non-matching ids so
// /allow /deny can fall through to the Claude Code permission path.
func TestReauthGateDecideUnknownID(t *testing.T) {
	gate := newReauthGate()
	if gate.decide("nope", true) {
		t.Fatal("decide must return false for an unknown id")
	}
}

// TestForceReauthNoAdminReachable proves that if no admin can be notified the
// action fails closed (no revoke) rather than proceeding unapproved.
func TestForceReauthNoAdminReachable(t *testing.T) {
	gate := newReauthGate()
	var rev recordRevoke
	_, errText := handleReauth(
		claudebk.ReauthRequest{ChatID: adminID}, gate, gatedAdmin, rev.fn,
		func(string) error { return errors.New("no admin configured") }, 5*time.Second)

	if len(rev.got) != 0 {
		t.Fatalf("unreachable-admin request must not revoke; revoked %v", rev.got)
	}
	if errText == "" {
		t.Fatal("expected an error when no admin can approve")
	}
}

// waitForPending spins until exactly one request is registered in the gate and
// returns its id, so a test can resolve it without racing the goroutine that
// registers it.
func waitForPending(t *testing.T, gate *reauthGate) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		var id string
		var n int
		for k := range gate.pending {
			id = k
			n++
		}
		gate.mu.Unlock()
		if n == 1 {
			return id
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for a pending reauth request")
	return ""
}
