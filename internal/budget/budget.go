// Package budget provides account-tier rate limiting and a circuit breaker for
// relay traffic. It is self-contained and has no dependency on any transport,
// model, or the rest of the relay — feed it usage estimates, ask it whether a
// turn may proceed, and it enforces a rolling-window limit plus a breaker.
//
// Nothing here contacts Anthropic. Tiers are local, tunable estimates: you
// declare which subscription level you have and the meter enforces that tier's
// configured ceiling. Adjust the numbers in DefaultTiers to match observed
// limits — they are deliberately conservative placeholders.
package budget

import (
	"fmt"
	"sync"
	"time"
)

// Tier describes the limits for one Anthropic account level. All values are
// local estimates, not values reported by Anthropic.
type Tier struct {
	Name         string        // "pro", "max5", ...
	WindowTokens int           // approx token budget per rolling window
	Window       time.Duration // length of the rolling window
	OpenAt       float64       // breaker trips when used/limit >= this (e.g. 0.9)
	Cooldown     time.Duration // how long the breaker stays open before a probe
}

// DefaultTiers are placeholder budgets keyed by level. Tune to taste — Anthropic
// does not publish hard token numbers for Claude Code subscription limits, and
// they vary, so treat these as a safety governor rather than an exact mirror.
var DefaultTiers = map[string]Tier{
	"free":  {Name: "free", WindowTokens: 20_000, Window: 5 * time.Hour, OpenAt: 0.9, Cooldown: 15 * time.Minute},
	"pro":   {Name: "pro", WindowTokens: 300_000, Window: 5 * time.Hour, OpenAt: 0.9, Cooldown: 10 * time.Minute},
	"max5":  {Name: "max5", WindowTokens: 1_500_000, Window: 5 * time.Hour, OpenAt: 0.9, Cooldown: 10 * time.Minute},
	"max20": {Name: "max20", WindowTokens: 6_000_000, Window: 5 * time.Hour, OpenAt: 0.9, Cooldown: 10 * time.Minute},
}

// State is the circuit breaker state.
type State int

const (
	Closed   State = iota // normal: traffic flows
	Open                  // tripped: traffic rejected until cooldown elapses
	HalfOpen              // probing: allow one turn to test recovery
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Meter enforces one tier's rolling-window budget plus a circuit breaker.
// Safe for concurrent use. The clock is injectable for tests.
type Meter struct {
	mu          sync.Mutex
	tier        Tier
	used        int       // tokens consumed in the current window
	windowStart time.Time // start of the current fixed window
	state       State
	openedAt    time.Time // when the breaker last opened
	pausedManul bool      // manual /pause override (independent of usage)
	now         func() time.Time
}

// New builds a Meter for the named tier (from DefaultTiers). Unknown names fall
// back to "pro". Pass clock=nil to use the real clock.
func New(tierName string, clock func() time.Time) *Meter {
	if clock == nil {
		clock = time.Now
	}
	t, ok := DefaultTiers[tierName]
	if !ok {
		t = DefaultTiers["pro"]
	}
	return &Meter{tier: t, windowStart: clock(), state: Closed, now: clock}
}

// rollWindow resets usage if the current window has elapsed. Caller holds mu.
func (m *Meter) rollWindow() {
	if m.now().Sub(m.windowStart) >= m.tier.Window {
		m.used = 0
		m.windowStart = m.now()
		if m.state == Open && !m.pausedManul {
			m.state = Closed // fresh window clears a usage-tripped breaker
		}
	}
}

// limit is the tier's usage ceiling for tripping, in tokens. Caller holds mu.
func (m *Meter) limit() int { return int(float64(m.tier.WindowTokens) * m.tier.OpenAt) }

// Allow reports whether a turn estimated at `estTokens` may proceed. When it
// returns false, reason is a user-facing explanation to send back over the
// frontend (no model turn is spent).
func (m *Meter) Allow(estTokens int) (ok bool, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rollWindow()

	if m.pausedManul {
		return false, "⛔ relay is paused (/resume to re-enable)"
	}

	switch m.state {
	case Open:
		if m.now().Sub(m.openedAt) >= m.tier.Cooldown {
			m.state = HalfOpen // time to probe
		} else {
			left := m.tier.Cooldown - m.now().Sub(m.openedAt)
			return false, fmt.Sprintf("⛔ circuit open (near %s limit) — retry in %s",
				m.tier.Name, left.Round(time.Second))
		}
	}

	// Pre-admission usage check: would this turn cross the ceiling?
	if m.used+estTokens > m.limit() {
		m.trip()
		return false, fmt.Sprintf("⛔ would exceed %s budget (%d/%d tokens this window) — circuit opened",
			m.tier.Name, m.used, m.limit())
	}
	return true, ""
}

// Record books the actual cost of a completed turn and updates the breaker.
func (m *Meter) Record(tokens int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rollWindow()
	m.used += tokens
	if m.state == HalfOpen {
		m.state = Closed // successful probe closes the breaker
	}
	if m.used >= m.limit() && m.state == Closed {
		m.trip()
	}
}

// trip opens the breaker. Caller holds mu.
func (m *Meter) trip() {
	m.state = Open
	m.openedAt = m.now()
}

// Pause and Resume are manual overrides driven by slash commands.
func (m *Meter) Pause()  { m.mu.Lock(); m.pausedManul = true; m.mu.Unlock() }
func (m *Meter) Resume() { m.mu.Lock(); m.pausedManul = false; m.state = Closed; m.mu.Unlock() }

// SetTier switches the active tier at runtime (e.g. via /tier). Preserves used.
func (m *Meter) SetTier(name string) error {
	t, ok := DefaultTiers[name]
	if !ok {
		return fmt.Errorf("unknown tier %q", name)
	}
	m.mu.Lock()
	m.tier = t
	m.mu.Unlock()
	return nil
}

// Status is a snapshot for reporting (e.g. /rate, /status).
type Status struct {
	Tier        string
	Used        int
	Limit       int
	WindowLeft  time.Duration
	State       State
	Paused      bool
	PercentUsed float64
}

// Snapshot returns the current metering state.
func (m *Meter) Snapshot() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rollWindow()
	lim := m.limit()
	pct := 0.0
	if lim > 0 {
		pct = 100 * float64(m.used) / float64(lim)
	}
	return Status{
		Tier:        m.tier.Name,
		Used:        m.used,
		Limit:       lim,
		WindowLeft:  m.tier.Window - m.now().Sub(m.windowStart),
		State:       m.state,
		Paused:      m.pausedManul,
		PercentUsed: pct,
	}
}
