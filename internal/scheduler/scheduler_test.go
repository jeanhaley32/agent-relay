package scheduler

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// rewriteFireAt backdates a one-shot's FireAt directly on disk to simulate the
// process being down past its fire time.
func rewriteFireAt(t *testing.T, path, id string, at time.Time) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var items []*Schedule
	if err := json.Unmarshal(b, &items); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, sc := range items {
		if sc.ID == id {
			sc.FireAt = at
		}
	}
	out, _ := json.MarshalIndent(items, "", "  ")
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// collector records deliveries.
type collector struct {
	mu   sync.Mutex
	got  []string
	fire chan struct{}
}

func newCollector() *collector { return &collector{fire: make(chan struct{}, 8)} }
func (c *collector) deliver(scheduleID, chatID, text string) error {
	c.mu.Lock()
	c.got = append(c.got, chatID+"|"+text)
	c.mu.Unlock()
	c.fire <- struct{}{}
	return nil
}
func (c *collector) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.got) }

func newTestSched(t *testing.T, c *collector) *Scheduler {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sched.json")
	s, err := New(path, time.UTC, c.deliver, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// TestOneShotFires: a short-delay one-shot fires once and is then forgotten.
func TestOneShotFires(t *testing.T) {
	c := newCollector()
	s := newTestSched(t, c)
	if _, err := s.Create("do pushups", "", 40*time.Millisecond, "42"); err != nil {
		t.Fatalf("create: %v", err)
	}
	select {
	case <-c.fire:
	case <-time.After(2 * time.Second):
		t.Fatal("one-shot never fired")
	}
	if got := c.got[0]; got != "42|do pushups" {
		t.Fatalf("wrong delivery: %q", got)
	}
	// After firing, it should be gone from the set.
	if n := len(s.List()); n != 0 {
		t.Fatalf("one-shot not removed after firing: %d left", n)
	}
}

// TestCreateValidation rejects bad input and a malformed cron spec.
func TestCreateValidation(t *testing.T) {
	c := newCollector()
	s := newTestSched(t, c)
	cases := []struct {
		name, text, cron string
		in               time.Duration
	}{
		{"empty text", "", "", time.Minute},
		{"neither cron nor delay", "hi", "", 0},
		{"both cron and delay", "hi", "0 9 * * *", time.Minute},
		{"bad cron", "hi", "not a cron", 0},
	}
	for _, tc := range cases {
		if _, err := s.Create(tc.text, tc.cron, tc.in, "1"); err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
		}
	}
	// A valid daily cron is accepted and has a future next-fire.
	sc, err := s.Create("stretch", "0 9 * * *", 0, "1")
	if err != nil {
		t.Fatalf("valid cron rejected: %v", err)
	}
	if next := s.Next(sc); !next.After(time.Now()) {
		t.Fatalf("cron next-fire not in the future: %v", next)
	}
}

// TestCancel disarms a schedule so it never fires.
func TestCancel(t *testing.T) {
	c := newCollector()
	s := newTestSched(t, c)
	sc, _ := s.Create("later", "", 200*time.Millisecond, "7")
	if !s.Cancel(sc.ID) {
		t.Fatal("cancel returned false for a live schedule")
	}
	if s.Cancel(sc.ID) {
		t.Fatal("second cancel should return false")
	}
	time.Sleep(400 * time.Millisecond)
	if n := c.count(); n != 0 {
		t.Fatalf("cancelled schedule still fired (%d)", n)
	}
}

// TestPersistReload: a recurring schedule survives a restart (reload from disk).
func TestPersistReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sched.json")
	c1 := newCollector()
	s1, err := New(path, time.UTC, c1.deliver, nil)
	if err != nil {
		t.Fatalf("new1: %v", err)
	}
	sc, _ := s1.Create("weekly review", "0 9 * * 1", 0, "9")
	s1.Close()

	c2 := newCollector()
	s2, err := New(path, time.UTC, c2.deliver, nil)
	if err != nil {
		t.Fatalf("new2: %v", err)
	}
	defer s2.Close()
	list := s2.List()
	if len(list) != 1 || list[0].ID != sc.ID || list[0].Cron != "0 9 * * 1" {
		t.Fatalf("recurring schedule did not survive reload: %+v", list)
	}
}

// TestMissedOneShotFiresOnLoad: a one-shot whose FireAt already passed while the
// process was down fires shortly after reload instead of being lost.
func TestMissedOneShotFiresOnLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sched.json")
	c1 := newCollector()
	s1, err := New(path, time.UTC, c1.deliver, nil)
	if err != nil {
		t.Fatalf("new1: %v", err)
	}
	// Create a one-shot far enough out that it won't fire before we close.
	sc, _ := s1.Create("missed me", "", time.Hour, "5")
	s1.Close()

	// Simulate downtime past the fire time by backdating FireAt on disk.
	rewriteFireAt(t, path, sc.ID, time.Now().Add(-time.Minute))

	c2 := newCollector()
	s2, err := New(path, time.UTC, c2.deliver, nil)
	if err != nil {
		t.Fatalf("new2: %v", err)
	}
	defer s2.Close()
	select {
	case <-c2.fire:
	case <-time.After(2 * time.Second):
		t.Fatal("missed one-shot did not fire on load")
	}
}
