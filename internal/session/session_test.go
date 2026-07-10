package session

import (
	"testing"
	"time"
)

func TestSessionLifecycle(t *testing.T) {
	m := NewManager(20 * time.Millisecond)

	if m.Active("admin") {
		t.Fatal("expected no session before Activate")
	}

	m.Activate("admin")
	if !m.Active("admin") {
		t.Fatal("expected active session right after Activate")
	}

	// Touch should slide the window forward.
	time.Sleep(15 * time.Millisecond)
	m.Touch("admin")
	time.Sleep(15 * time.Millisecond)
	if !m.Active("admin") {
		t.Fatal("expected session still active after Touch extended it")
	}

	// Let it fully idle out.
	time.Sleep(30 * time.Millisecond)
	if m.Active("admin") {
		t.Fatal("expected session to expire after idling past TTL")
	}

	// Touch on an already-expired/nonexistent session must not resurrect it.
	m.Touch("admin")
	if m.Active("admin") {
		t.Fatal("Touch must not create a new session")
	}
}

func TestSessionIsolatedPerChat(t *testing.T) {
	m := NewManager(time.Minute)
	m.Activate("admin")
	if m.Active("someone-else") {
		t.Fatal("session for one chat_id must not leak to another")
	}
}
