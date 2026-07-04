package budget

import (
	"testing"
	"time"
)

// fakeClock is a controllable time source for deterministic tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }

func TestCircuitTripsAndRecovers(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	m := New("free", clk.now) // free: 20k tokens, OpenAt 0.9 => limit 18k, cooldown 15m

	// Under the limit: allowed and closed.
	if ok, _ := m.Allow(1000); !ok {
		t.Fatal("expected small turn to be allowed")
	}
	m.Record(1000)
	if s := m.Snapshot(); s.State != Closed {
		t.Fatalf("expected closed, got %s", s.State)
	}

	// Push usage over the 18k limit -> breaker trips open.
	m.Record(18_000)
	if s := m.Snapshot(); s.State != Open {
		t.Fatalf("expected open after exceeding limit, got %s (used=%d/%d)", s.State, s.Used, s.Limit)
	}

	// While open (before cooldown) turns are rejected.
	if ok, why := m.Allow(10); ok {
		t.Fatal("expected rejection while circuit open")
	} else if why == "" {
		t.Fatal("expected a reason string when rejected")
	}

	// After cooldown the breaker probes (half-open) but usage is still over the
	// ceiling, so the turn is still rejected — cooldown does not refill budget.
	clk.add(16 * time.Minute)
	if ok, _ := m.Allow(10); ok {
		t.Fatal("expected rejection: still over budget even after cooldown")
	}
	if s := m.Snapshot(); s.State != Open {
		t.Fatalf("expected breaker re-open (over budget), got %s", s.State)
	}

	// The real recovery for a usage trip is the window roll: usage resets and
	// the breaker clears.
	clk.add(5 * time.Hour)
	if s := m.Snapshot(); s.Used != 0 || s.State != Closed {
		t.Fatalf("expected reset+closed after window, got used=%d state=%s", s.Used, s.State)
	}
	// And traffic flows again.
	if ok, _ := m.Allow(1000); !ok {
		t.Fatal("expected traffic allowed after window reset")
	}
}

func TestManualPause(t *testing.T) {
	m := New("pro", nil)
	m.Pause()
	if ok, _ := m.Allow(1); ok {
		t.Fatal("paused meter must reject")
	}
	m.Resume()
	if ok, _ := m.Allow(1); !ok {
		t.Fatal("resumed meter must allow")
	}
}

func TestUnknownTierFallsBack(t *testing.T) {
	m := New("nonsense", nil)
	if s := m.Snapshot(); s.Tier != "pro" {
		t.Fatalf("expected fallback to pro, got %s", s.Tier)
	}
}
