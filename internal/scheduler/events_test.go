package scheduler

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// errTestSend simulates a failed outbound send in fallback tests.
var errTestSend = errors.New("simulated send failure")

// trackerHarness records inject/fallback/receipt calls for assertions.
type trackerHarness struct {
	mu          sync.Mutex
	injects     []string // "chatID|text"
	fallbacks   []string
	receipts    []string
	connected   bool  // what inject reports as delivered
	fallbackErr error // when set, fallback() returns it and records nothing
}

func (h *trackerHarness) inject(chatID, text string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.injects = append(h.injects, chatID+"|"+text)
	return h.connected
}
func (h *trackerHarness) fallback(text string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fallbackErr != nil {
		return h.fallbackErr
	}
	h.fallbacks = append(h.fallbacks, text)
	return nil
}
func (h *trackerHarness) receipt(text string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.receipts = append(h.receipts, text)
}
func (h *trackerHarness) counts() (inj, fb, rc int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.injects), len(h.fallbacks), len(h.receipts)
}

// newTestTracker builds a Tracker with a very long tick (so the background loop
// never interferes) and a controllable clock. Tests drive Reconcile directly.
func newTestTracker(t *testing.T, h *trackerHarness) (*Tracker, *time.Time) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.json")
	now := time.Now()
	clock := &now
	// Inject the clock via cfg.Now (a closure over *clock) rather than
	// mutating a Tracker field after construction, so the reconciliation
	// goroutine never races a field write.
	tr, err := NewTracker(path, h.inject, h.fallback, h.receipt, TrackerConfig{
		NudgeAfter:     5 * time.Minute,
		EscalateAfter:  12 * time.Minute,
		TickEvery:      time.Hour, // effectively disable the auto loop for tests
		ReplyAckWindow: 2 * time.Minute,
		Now:            func() time.Time { return *clock },
	}, nil)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	t.Cleanup(tr.Close)
	return tr, clock
}

func TestFireCreatesAndDelivers(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, _ := newTestTracker(t, h)
	ev, err := tr.Fire("sched1", "42", "do pushups")
	if err != nil {
		t.Fatalf("fire: %v", err)
	}
	if inj, _, _ := h.counts(); inj != 1 {
		t.Fatalf("expected 1 inject, got %d", inj)
	}
	if got := tr.ListPending(); len(got) != 1 || got[0].ID != ev.ID {
		t.Fatalf("pending list wrong: %+v", got)
	}
	// Delivered because the harness reported connected.
	if tr.ListPending()[0].DeliveredAt.IsZero() {
		t.Fatal("expected DeliveredAt set when connected")
	}
}

func TestFireNotDeliveredWhenDisconnected(t *testing.T) {
	h := &trackerHarness{connected: false}
	tr, _ := newTestTracker(t, h)
	tr.Fire("s", "1", "text")
	if !tr.ListPending()[0].DeliveredAt.IsZero() {
		t.Fatal("DeliveredAt should be zero when not connected")
	}
}

func TestCoalescing(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, _ := newTestTracker(t, h)
	ev1, _ := tr.Fire("sched1", "42", "first")
	ev2, _ := tr.Fire("sched1", "42", "second")
	if ev1.ID != ev2.ID {
		t.Fatalf("re-fire of same schedule should coalesce onto same event: %s vs %s", ev1.ID, ev2.ID)
	}
	if got := tr.ListPending(); len(got) != 1 {
		t.Fatalf("expected 1 open event after coalesce, got %d", len(got))
	}
	if got := tr.ListPending()[0]; got.FireCount != 2 || got.Text != "second" {
		t.Fatalf("coalesce didn't bump counter/update text: count=%d text=%q", got.FireCount, got.Text)
	}
	// Only the first fire injects; the coalesced re-fire does not re-inject.
	if inj, _, _ := h.counts(); inj != 1 {
		t.Fatalf("coalesced fire should not re-inject, got %d injects", inj)
	}
}

// TestStuckReFireResetsAndReinjects: once an open event has been through the
// full escalation ladder (fallback sent) without being acked, a re-fire of the
// same schedule must not silently coalesce — it should reset the ladder
// timestamps and re-inject the prompt, restarting the nudge/escalate cycle.
func TestStuckReFireResetsAndReinjects(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, clock := newTestTracker(t, h)
	ev1, _ := tr.Fire("sched1", "42", "first")
	*clock = clock.Add(13 * time.Minute) // past EscalateAfter (12m)
	tr.Reconcile(*clock)
	if got := tr.ListPending()[0]; got.FallbackSentAt.IsZero() {
		t.Fatal("expected FallbackSentAt set after escalation reconcile")
	}
	if _, fb, _ := h.counts(); fb != 1 {
		t.Fatalf("expected 1 fallback sent before re-fire, got %d", fb)
	}

	ev2, err := tr.Fire("sched1", "42", "second")
	if err != nil {
		t.Fatalf("re-fire: %v", err)
	}
	if ev1.ID != ev2.ID {
		t.Fatalf("re-fire of same schedule should still coalesce onto same event id: %s vs %s", ev1.ID, ev2.ID)
	}
	got := tr.ListPending()[0]
	if got.FallbackSentAt.IsZero() == false {
		t.Fatal("expected FallbackSentAt reset on stuck re-fire")
	}
	if got.LastNudgeAt.IsZero() == false {
		t.Fatal("expected LastNudgeAt reset on stuck re-fire")
	}
	if !got.FiredAt.Equal(*clock) {
		t.Fatalf("expected FiredAt reset to the re-fire time %v, got %v", *clock, got.FiredAt)
	}
	if got.DeliveredAt.IsZero() {
		t.Fatal("expected DeliveredAt set again after re-inject (harness reports connected)")
	}
	if got.FireCount != 2 {
		t.Fatalf("expected FireCount bumped to 2, got %d", got.FireCount)
	}
	if inj, _, _ := h.counts(); inj != 2 {
		t.Fatalf("stuck re-fire should re-inject the prompt, expected 2 injects, got %d", inj)
	}
}

func TestExplicitAck(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, _ := newTestTracker(t, h)
	ev, _ := tr.Fire("s", "42", "text")
	if err := tr.Ack(ev.ID, "handled it, restarted the service"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if len(tr.ListPending()) != 0 {
		t.Fatal("event should be closed after ack")
	}
	if _, _, rc := h.counts(); rc != 1 {
		t.Fatalf("ack should send a receipt to Jean, got %d", rc)
	}
}

func TestAckEmptyNoteRejected(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, _ := newTestTracker(t, h)
	ev, _ := tr.Fire("s", "42", "text")
	if err := tr.Ack(ev.ID, "   "); err == nil {
		t.Fatal("empty/whitespace note should be rejected")
	}
	if len(tr.ListPending()) != 1 {
		t.Fatal("event should remain open after rejected ack")
	}
}

func TestReplyInferredAck(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, clock := newTestTracker(t, h)
	tr.Fire("s", "42", "text")
	// A reply on the same chat AFTER the fire resolves it.
	*clock = clock.Add(time.Minute)
	tr.NoteReply("42", *clock)
	if len(tr.ListPending()) != 0 {
		t.Fatal("reply after fire should auto-resolve the event")
	}
}

func TestReplyBeforeFireDoesNotAck(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, clock := newTestTracker(t, h)
	// A reply observed BEFORE the fire (earlier timestamp) must not resolve it.
	replyAt := *clock
	*clock = clock.Add(time.Minute)
	tr.Fire("s", "42", "text")
	tr.NoteReply("42", replyAt)
	if len(tr.ListPending()) != 1 {
		t.Fatal("a reply predating the fire must not resolve the event")
	}
}

func TestReplyDifferentChatDoesNotAck(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, clock := newTestTracker(t, h)
	tr.Fire("s", "42", "text")
	*clock = clock.Add(time.Minute)
	tr.NoteReply("99", *clock) // different chat
	if len(tr.ListPending()) != 1 {
		t.Fatal("a reply on another chat must not resolve the event")
	}
}

// TestEscalationLifecycle drives the full cadence via the injected clock:
// fire → 6m → nudge re-inject → 13m → direct-to-Jean fallback.
func TestEscalationLifecycle(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, clock := newTestTracker(t, h)
	tr.Fire("s", "42", "important thing")

	inj0, _, _ := h.counts()
	if inj0 != 1 {
		t.Fatalf("expected initial inject, got %d", inj0)
	}

	// Before 5m: reconcile does nothing new.
	*clock = clock.Add(3 * time.Minute)
	tr.Reconcile(*clock)
	if inj, fb, _ := h.counts(); inj != 1 || fb != 0 {
		t.Fatalf("premature escalation: inj=%d fb=%d", inj, fb)
	}

	// After 6m: one nudge re-inject, no fallback yet.
	*clock = clock.Add(3 * time.Minute) // t=6m
	tr.Reconcile(*clock)
	if inj, fb, _ := h.counts(); inj != 2 || fb != 0 {
		t.Fatalf("expected nudge at 6m: inj=%d fb=%d", inj, fb)
	}
	// Nudge fires at most once.
	*clock = clock.Add(2 * time.Minute) // t=8m
	tr.Reconcile(*clock)
	if inj, fb, _ := h.counts(); inj != 2 || fb != 0 {
		t.Fatalf("nudge should fire once only: inj=%d fb=%d", inj, fb)
	}

	// After 13m: direct-to-Jean fallback, no further agent inject.
	*clock = clock.Add(5 * time.Minute) // t=13m
	tr.Reconcile(*clock)
	if inj, fb, _ := h.counts(); inj != 2 || fb != 1 {
		t.Fatalf("expected fallback at 13m: inj=%d fb=%d", inj, fb)
	}
	// Fallback fires once only.
	*clock = clock.Add(5 * time.Minute) // t=18m
	tr.Reconcile(*clock)
	if inj, fb, _ := h.counts(); inj != 2 || fb != 1 {
		t.Fatalf("fallback should fire once only: inj=%d fb=%d", inj, fb)
	}
}

// TestNoRetrySpamWhileDisconnected: an event injected while the shim is down is
// injected exactly once (the link is lossless-buffered). Reconcile must NOT
// re-inject the trigger on every tick while it stays undelivered — that spam
// could evict other buffered frames. Only the single nudge at NudgeAfter adds a
// re-inject.
func TestNoRetrySpamWhileDisconnected(t *testing.T) {
	h := &trackerHarness{connected: false}
	tr, clock := newTestTracker(t, h)
	tr.Fire("s", "42", "text")
	if inj, _, _ := h.counts(); inj != 1 {
		t.Fatalf("expected exactly one initial inject, got %d", inj)
	}
	// Several reconciles well before the nudge window: no additional injects.
	for i := 0; i < 4; i++ {
		*clock = clock.Add(time.Minute)
		tr.Reconcile(*clock)
	}
	if inj, _, _ := h.counts(); inj != 1 {
		t.Fatalf("undelivered event must not be re-injected every tick: got %d injects", inj)
	}
	// The single nudge at ~6m is the only additional re-inject.
	*clock = clock.Add(2 * time.Minute) // t=6m
	tr.Reconcile(*clock)
	if inj, _, _ := h.counts(); inj != 2 {
		t.Fatalf("expected exactly one nudge re-inject, got %d total injects", inj)
	}
}

// TestReplyOutsideWindowDoesNotAck: a reply landing well after the fire (past
// ReplyAckWindow) is ordinary chatter, not evidence — it must NOT auto-resolve.
func TestReplyOutsideWindowDoesNotAck(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, clock := newTestTracker(t, h)
	tr.Fire("s", "42", "text")
	*clock = clock.Add(3 * time.Minute) // beyond the 2m window
	tr.NoteReply("42", *clock)
	if len(tr.ListPending()) != 1 {
		t.Fatal("a reply outside the ack window must not resolve the event")
	}
}

// TestReplyWindowResetByNudge: after a nudge, the window is measured from the
// nudge, so a prompt reply following the nudge still auto-resolves.
func TestReplyWindowResetByNudge(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, clock := newTestTracker(t, h)
	tr.Fire("s", "42", "text")
	*clock = clock.Add(6 * time.Minute) // trigger the nudge
	tr.Reconcile(*clock)
	*clock = clock.Add(time.Minute) // 1m after the nudge, within window
	tr.NoteReply("42", *clock)
	if len(tr.ListPending()) != 0 {
		t.Fatal("a reply promptly after a nudge should auto-resolve the event")
	}
}

// TestFirePersistFailureIsSurfaced: if the pending event cannot be durably
// persisted, Fire must return an error and NOT keep an in-memory-only event, so
// the caller (scheduler.fire) leaves the one-shot schedule intact for retry.
func TestFirePersistFailureIsSurfaced(t *testing.T) {
	h := &trackerHarness{connected: true}
	// A path whose parent directory does not exist makes the atomic write fail.
	path := filepath.Join(t.TempDir(), "no-such-dir", "events.json")
	tr, err := NewTracker(path, h.inject, h.fallback, h.receipt, TrackerConfig{TickEvery: time.Hour}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(tr.Close)
	ev, err := tr.Fire("s", "42", "text")
	if err == nil {
		t.Fatal("Fire should return an error when persist fails")
	}
	if ev.ID != "" {
		t.Fatalf("Fire should return a zero-value event on persist failure, got %+v", ev)
	}
	if len(tr.ListPending()) != 0 {
		t.Fatal("an unpersisted event must not linger in memory")
	}
}

// TestFallbackRetryOnSendFailure: if the fallback send fails, FallbackSentAt
// stays unset so the next reconcile retries; once it succeeds it is marked once.
func TestFallbackRetryOnSendFailure(t *testing.T) {
	h := &trackerHarness{connected: true, fallbackErr: errTestSend}
	tr, clock := newTestTracker(t, h)
	tr.Fire("s", "42", "text")
	*clock = clock.Add(13 * time.Minute) // past escalation
	tr.Reconcile(*clock)
	if _, fb, _ := h.counts(); fb != 0 {
		t.Fatalf("failed fallback send must not be recorded: fb=%d", fb)
	}
	if !tr.ListPending()[0].FallbackSentAt.IsZero() {
		t.Fatal("FallbackSentAt must stay zero after a failed send so it retries")
	}
	// Send now succeeds: the next tick retries and marks it once.
	h.mu.Lock()
	h.fallbackErr = nil
	h.mu.Unlock()
	*clock = clock.Add(time.Minute)
	tr.Reconcile(*clock)
	if _, fb, _ := h.counts(); fb != 1 {
		t.Fatalf("expected exactly one fallback after recovery: fb=%d", fb)
	}
	if tr.ListPending()[0].FallbackSentAt.IsZero() {
		t.Fatal("FallbackSentAt should be set after a successful send")
	}
}

func TestPersistReloadEvents(t *testing.T) {
	h := &trackerHarness{connected: true}
	path := filepath.Join(t.TempDir(), "events.json")
	tr, err := NewTracker(path, h.inject, h.fallback, h.receipt, TrackerConfig{TickEvery: time.Hour}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ev, _ := tr.Fire("sched1", "42", "survive restart")
	tr.Close()

	h2 := &trackerHarness{connected: true}
	tr2, err := NewTracker(path, h2.inject, h2.fallback, h2.receipt, TrackerConfig{TickEvery: time.Hour}, nil)
	if err != nil {
		t.Fatalf("new2: %v", err)
	}
	t.Cleanup(tr2.Close)
	got := tr2.ListPending()
	if len(got) != 1 || got[0].ID != ev.ID {
		t.Fatalf("event did not survive reload: %+v", got)
	}
}

// TestRetentionPruning confirms an acknowledged event is kept until
// cfg.Retention has elapsed since AckedAt, then pruned on the next Reconcile,
// exactly once.
func TestRetentionPruning(t *testing.T) {
	h := &trackerHarness{connected: true}
	path := filepath.Join(t.TempDir(), "events.json")
	now := time.Now()
	clock := &now
	tr, err := NewTracker(path, h.inject, h.fallback, h.receipt, TrackerConfig{
		NudgeAfter:     5 * time.Minute,
		EscalateAfter:  12 * time.Minute,
		Retention:      10 * time.Minute,
		TickEvery:      time.Hour,
		ReplyAckWindow: 2 * time.Minute,
		Now:            func() time.Time { return *clock },
	}, nil)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}
	t.Cleanup(tr.Close)

	ev, _ := tr.Fire("s", "42", "text")
	if err := tr.Ack(ev.ID, "handled"); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Just before the retention window elapses: still present.
	*clock = clock.Add(9 * time.Minute)
	tr.Reconcile(*clock)
	tr.mu.Lock()
	_, stillThere := tr.events[ev.ID]
	tr.mu.Unlock()
	if !stillThere {
		t.Fatal("acknowledged event pruned before retention window elapsed")
	}

	// Past the retention window: pruned on the next reconcile.
	*clock = clock.Add(2 * time.Minute)
	tr.Reconcile(*clock)
	tr.mu.Lock()
	_, stillThere = tr.events[ev.ID]
	tr.mu.Unlock()
	if stillThere {
		t.Fatal("acknowledged event should be pruned once retention window elapses")
	}

	// A further reconcile is a no-op (nothing left to prune, no panic/error).
	*clock = clock.Add(time.Hour)
	tr.Reconcile(*clock)
}

func TestMetricsAccessors(t *testing.T) {
	h := &trackerHarness{connected: true}
	tr, clock := newTestTracker(t, h)
	if tr.OpenCount() != 0 || tr.OldestOpenAge() != 0 {
		t.Fatal("expected empty metrics initially")
	}
	tr.Fire("s", "42", "text")
	*clock = clock.Add(90 * time.Second)
	if tr.OpenCount() != 1 {
		t.Fatalf("open count = %d", tr.OpenCount())
	}
	if age := tr.OldestOpenAge(); age < 89*time.Second || age > 91*time.Second {
		t.Fatalf("oldest age unexpected: %v", age)
	}
}
