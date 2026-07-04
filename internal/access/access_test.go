package access

import (
	"path/filepath"
	"testing"
)

func TestAllowAdminAndApproveFlow(t *testing.T) {
	save := filepath.Join(t.TempDir(), "allowlist.json")
	m := New([]int64{1}, []int64{2}, save) // admin 1, seed-allowed 2

	if !m.Allowed(1) || !m.IsAdmin(1) {
		t.Fatal("admin should be allowed and admin")
	}
	if !m.Allowed(2) || m.IsAdmin(2) {
		t.Fatal("seed-allowed should be allowed but not admin")
	}
	if m.Allowed(99) {
		t.Fatal("unknown id must not be allowed")
	}

	// A stranger's message records a pending request.
	m.Record(99, "stranger")
	m.Record(99, "stranger") // idempotent
	if p := m.Pending(); len(p) != 1 || p[0].ID != 99 {
		t.Fatalf("expected one pending 99, got %v", p)
	}

	// Approve grants access, clears pending, and persists.
	if !m.Approve(99) {
		t.Fatal("approve should report it was pending")
	}
	if !m.Allowed(99) {
		t.Fatal("approved id should be allowed")
	}
	if len(m.Pending()) != 0 {
		t.Fatal("pending should be empty after approve")
	}

	// Persistence: a fresh manager loads the approved id from the file.
	m2 := New([]int64{1}, nil, save)
	if !m2.Allowed(99) {
		t.Fatal("approved id should persist across restart")
	}

	// Deny drops a pending request without granting.
	m2.Record(50, "someone")
	if !m2.Deny(50) || m2.Allowed(50) {
		t.Fatal("deny should drop pending without granting")
	}
}
