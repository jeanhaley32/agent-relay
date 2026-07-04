package access

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAllowAdminAndApproveFlow(t *testing.T) {
	save := filepath.Join(t.TempDir(), "allowlist.json")
	m := New([]int64{1}, []int64{2}, save, nil) // admin 1, seed-allowed 2

	if !m.Allowed(1) || !m.IsAdmin(1) {
		t.Fatal("admin should be allowed and admin")
	}
	if !m.Allowed(2) || m.IsAdmin(2) {
		t.Fatal("seed-allowed should be allowed but not admin")
	}
	if m.Allowed(99) {
		t.Fatal("unknown id must not be allowed")
	}

	m.Record(99, "stranger")
	m.Record(99, "stranger") // idempotent
	if p := m.Pending(); len(p) != 1 || p[0].ID != 99 {
		t.Fatalf("expected one pending 99, got %v", p)
	}

	if !m.Approve(99) {
		t.Fatal("approve should report it was pending")
	}
	if !m.Allowed(99) || len(m.Pending()) != 0 {
		t.Fatal("approved id should be allowed and pending cleared")
	}

	// Persistence: a fresh manager loads the approved id.
	m2 := New([]int64{1}, nil, save, nil)
	if !m2.Allowed(99) {
		t.Fatal("approved id should persist across restart")
	}
}

func TestRejectsNonPositiveIDs(t *testing.T) {
	m := New([]int64{0, -5, 1}, []int64{-1, 2}, "", nil)
	if m.Allowed(0) || m.IsAdmin(0) || m.Allowed(-5) || m.Allowed(-1) {
		t.Fatal("ids <= 0 must never be allowed/admin")
	}
	if !m.IsAdmin(1) || !m.Allowed(2) {
		t.Fatal("valid ids should survive")
	}
	m.Record(0, "x")
	m.Record(-1, "x")
	if len(m.Pending()) != 0 {
		t.Fatal("ids <= 0 must not be recorded as pending")
	}
	if m.Approve(0) || m.Approve(-1) {
		t.Fatal("approving id <= 0 must fail")
	}
}

func TestApproveOnlyPending(t *testing.T) {
	m := New(nil, nil, "", nil)
	if m.Approve(555) { // never pending
		t.Fatal("approving a non-pending id must return false")
	}
	if m.Allowed(555) {
		t.Fatal("a non-pending id must not be granted access by Approve")
	}
}

func TestDenyBlocksRequeue(t *testing.T) {
	m := New(nil, nil, "", nil)
	m.Record(7, "pest")
	if !m.Deny(7) {
		t.Fatal("deny of a pending id should return true")
	}
	m.Record(7, "pest again") // should be ignored — denied
	if len(m.Pending()) != 0 {
		t.Fatal("denied id must not re-queue")
	}
}

func TestDenyOnlyPending(t *testing.T) {
	m := New(nil, nil, "", nil)
	if m.Deny(123) { // never pending — must be a no-op
		t.Fatal("deny of a non-pending id should return false")
	}
	// A stray deny must NOT permanently block a later genuine request.
	m.Record(123, "later")
	if len(m.Pending()) != 1 {
		t.Fatal("a non-pending deny must not denylist the id")
	}
}

func TestApproveUnDenies(t *testing.T) {
	m := New(nil, nil, "", nil)
	m.Record(8, "x")
	m.Deny(8)              // now denied, not pending
	if !m.Approve(8) {     // approve must reverse the denial
		t.Fatal("approve should un-deny a previously denied id")
	}
	if !m.Allowed(8) {
		t.Fatal("un-denied id should be allowed")
	}
	m.Record(8, "x") // already allowed → no new pending
	if len(m.Pending()) != 0 {
		t.Fatal("allowed id should not re-queue")
	}
}

func TestPendingBounded(t *testing.T) {
	m := New(nil, nil, "", nil)
	for i := int64(1); i <= maxPending+50; i++ {
		m.Record(i, "u")
	}
	if got := len(m.Pending()); got > maxPending {
		t.Fatalf("pending queue must be bounded to %d, got %d", maxPending, got)
	}
}

func TestNameSanitized(t *testing.T) {
	m := New(nil, nil, "", nil)
	m.Record(3, "evil\n  999 — fake")
	name := m.Pending()[0].Name
	if strings.ContainsAny(name, "\n\r") {
		t.Fatalf("name must not contain newlines: %q", name)
	}
	long := strings.Repeat("x", 200)
	m.Record(4, long)
	for _, r := range m.Pending() {
		if len(r.Name) > maxNameLen {
			t.Fatalf("name must be truncated to %d, got %d", maxNameLen, len(r.Name))
		}
	}
}

func TestCorruptFileDoesNotCrash(t *testing.T) {
	save := filepath.Join(t.TempDir(), "allowlist.json")
	os.WriteFile(save, []byte("{ this is not valid json"), 0o600)
	m := New([]int64{1}, nil, save, nil) // must not panic
	if !m.IsAdmin(1) {
		t.Fatal("admin should still work despite a corrupt file")
	}
}

func TestLegacyArrayFormatLoads(t *testing.T) {
	save := filepath.Join(t.TempDir(), "allowlist.json")
	os.WriteFile(save, []byte("[42, 43]"), 0o600)
	m := New(nil, nil, save, nil)
	if !m.Allowed(42) || !m.Allowed(43) {
		t.Fatal("legacy array format should load as allowed ids")
	}
}
