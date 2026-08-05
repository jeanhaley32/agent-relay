package adminbind

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBindUnbindDevice(t *testing.T) {
	m := New("", nil)
	if _, ok := m.Device("jean"); ok {
		t.Fatal("unbound sender should report ok=false")
	}
	m.Bind("jean", "jean-phone")
	h, ok := m.Device("jean")
	if !ok || h != "jean-phone" {
		t.Fatalf("Device(jean) = %q, %v; want jean-phone, true", h, ok)
	}
	m.Unbind("jean")
	if _, ok := m.Device("jean"); ok {
		t.Fatal("Unbind should remove the binding")
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "adminbind.json")

	m1 := New(path, nil)
	m1.Bind("jean", "jean-phone")

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected persisted file: %v", err)
	}

	m2 := New(path, nil)
	h, ok := m2.Device("jean")
	if !ok || h != "jean-phone" {
		t.Fatalf("after reload, Device(jean) = %q, %v", h, ok)
	}
}
