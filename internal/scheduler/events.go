// This file adds a pending-event tracker to the scheduler package. Where the
// Scheduler decides WHEN something should fire, the Tracker records that a fire
// actually happened and follows it until the agent acknowledges handling it —
// closing the long-standing gap where a fired trigger could be silently buried
// in a busy Claude session and never acted on.
//
// Design (crash-safe by construction): the Tracker does NOT arm a per-event
// timer. It persists only facts (when each event fired, whether it was
// delivered/nudged/escalated) and a single reconciliation ticker periodically
// derives what is due purely from wall-clock comparisons against those
// timestamps. A restart at any point loses no state and needs no re-arming:
// the next tick recomputes the correct action from the persisted file alone.
package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Event status values.
const (
	StatusPending      = "pending"
	StatusAcknowledged = "acknowledged"
)

// PendingEvent is one fired trigger being followed until it is acknowledged.
// It is persisted as JSON; all timing decisions are derived from its timestamps
// so the process can restart at any point without losing track of it.
type PendingEvent struct {
	ID          string    `json:"id"`
	ScheduleID  string    `json:"schedule_id,omitempty"` // which schedule fired this (empty for non-schedule sources)
	ChatID      string    `json:"chat_id"`
	Text        string    `json:"text"`
	FiredAt     time.Time `json:"fired_at"`
	DeliveredAt time.Time `json:"delivered_at,omitempty"`  // zero until confirmed injected into a live session
	LastNudgeAt time.Time `json:"last_nudge_at,omitempty"` // zero until the 5-min re-inject happened
	Status      string    `json:"status"`
	AckNote     string    `json:"ack_note,omitempty"`
	AckedAt     time.Time `json:"acked_at,omitempty"`

	// Bookkeeping beyond the core model:
	FireCount      int       `json:"fire_count"`                 // times the source (re)fired while still open — coalescing counter
	FallbackSentAt time.Time `json:"fallback_sent_at,omitempty"` // zero until the direct-to-Jean escalation fired
}

// InjectFunc delivers text into the agent's session. It reports whether the
// text reached a currently-connected session (true) vs was only buffered while
// disconnected (false) — the Tracker uses that to decide whether to mark the
// event delivered. It must not block for long.
type InjectFunc func(chatID, text string) (delivered bool)

// FallbackFunc sends a message directly to the admin (Jean) via the frontend,
// used when the agent has ignored an event past the escalation window. It
// returns an error if the send failed so the Tracker can avoid marking the
// escalation as sent (and retry it on the next tick) — this fallback is the
// last line of defense, so a lost one must not be recorded as done.
type FallbackFunc func(text string) error

// ReceiptFunc sends a brief one-line ack receipt to the admin, so every ack
// leaves a visible human-skimmable trail rather than a silent state change.
type ReceiptFunc func(note string)

// TrackerConfig tunes the escalation cadence and reconciliation interval.
// Zero values fall back to sensible defaults.
type TrackerConfig struct {
	NudgeAfter    time.Duration // re-inject once after this long unacked (default 5m)
	EscalateAfter time.Duration // fall back to Jean after this long unacked (default 12m)
	TickEvery     time.Duration // reconciliation scan interval (default 30s)
	Retention     time.Duration // how long to keep acknowledged events before pruning (default 1h)

	// ReplyAckWindow bounds reply-inferred acknowledgment: a model reply only
	// auto-resolves an event if it lands within this window after the event
	// fired (or after its last nudge). Outside the window a reply is treated as
	// ordinary unrelated chatter, NOT evidence the trigger was handled, so the
	// explicit ack_event tool is still required. Default 2m.
	ReplyAckWindow time.Duration

	// Now is an injectable clock (tests drive time deterministically). Defaults
	// to time.Now. Injected via config rather than mutated post-construction so
	// the reconciliation goroutine never races a field write.
	Now func() time.Time
}

func (c TrackerConfig) withDefaults() TrackerConfig {
	if c.NudgeAfter <= 0 {
		c.NudgeAfter = 5 * time.Minute
	}
	if c.EscalateAfter <= 0 {
		c.EscalateAfter = 12 * time.Minute
	}
	if c.TickEvery <= 0 {
		c.TickEvery = 30 * time.Second
	}
	if c.Retention <= 0 {
		c.Retention = time.Hour
	}
	if c.ReplyAckWindow <= 0 {
		c.ReplyAckWindow = 2 * time.Minute
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Tracker follows fired events until acknowledged. Its methods are safe for
// concurrent use: the reconciliation goroutine, MCP tool handlers, the
// schedule-fire callback, and reply-inferred acks all mutate through the mutex.
type Tracker struct {
	path     string
	inject   InjectFunc
	fallback FallbackFunc
	receipt  ReceiptFunc
	logger   *log.Logger
	cfg      TrackerConfig
	now      func() time.Time // injectable clock (tests drive time)

	mu     sync.Mutex
	events map[string]*PendingEvent
	stop   chan struct{}
	closed bool
}

// NewTracker builds a Tracker, loads any persisted events from path, and starts
// the reconciliation ticker. inject/fallback/receipt wire it to the agent
// session and the admin's Telegram. loc is unused here (timestamps are stored
// as absolute instants); the clock defaults to time.Now.
func NewTracker(path string, inject InjectFunc, fallback FallbackFunc, receipt ReceiptFunc, cfg TrackerConfig, logger *log.Logger) (*Tracker, error) {
	if inject == nil {
		return nil, errors.New("scheduler: nil inject")
	}
	if fallback == nil {
		return nil, errors.New("scheduler: nil fallback")
	}
	if receipt == nil {
		return nil, errors.New("scheduler: nil receipt")
	}
	if logger == nil {
		logger = log.New(os.Stderr, "[events] ", log.LstdFlags)
	}
	c := cfg.withDefaults()
	t := &Tracker{
		path:     path,
		inject:   inject,
		fallback: fallback,
		receipt:  receipt,
		logger:   logger,
		cfg:      c,
		now:      c.Now,
		events:   map[string]*PendingEvent{},
		stop:     make(chan struct{}),
	}
	if err := t.load(); err != nil {
		return nil, err
	}
	go t.runLoop()
	return t, nil
}

// Fire records that a trigger fired and injects it into the agent's session
// with an instruction to ack when done. If scheduleID is non-empty and an open
// (pending) event from the SAME schedule already exists, it does NOT spawn a
// sibling: it bumps that event's counter and refreshes its text instead, so a
// wedged agent can't accumulate a pile of duplicate escalating events.
//
// The pending event is persisted BEFORE the caller does any schedule cleanup,
// so a crash between firing and one-shot deletion can never lose the event with
// no trace. Returns the (new or coalesced) event.
func (t *Tracker) Fire(scheduleID, chatID, text string) (*PendingEvent, error) {
	t.mu.Lock()
	// Coalesce onto an existing open event from the same schedule.
	if scheduleID != "" {
		if ex := t.openBySchedule(scheduleID); ex != nil {
			ex.FireCount++
			ex.Text = text
			err := t.persist()
			t.mu.Unlock()
			if err != nil {
				// Not durably persisted — surface so the caller keeps the
				// schedule for a retry rather than deleting it.
				return ex, fmt.Errorf("persist coalesced event %s: %w", ex.ID, err)
			}
			t.logger.Printf("coalesced re-fire of schedule %s onto event %s (count=%d)", scheduleID, ex.ID, ex.FireCount)
			return ex, nil
		}
	}
	ev := &PendingEvent{
		ID:         newID(),
		ScheduleID: scheduleID,
		ChatID:     chatID,
		Text:       text,
		FiredAt:    t.now(),
		Status:     StatusPending,
		FireCount:  1,
	}
	t.events[ev.ID] = ev
	// Persist first — the durable record of the event must exist before any
	// caller-side schedule deletion, so a crash in between doesn't lose it. If
	// persistence fails the event is NOT durable: drop it from memory and
	// return an error so the caller aborts the one-shot deletion and lets the
	// schedule survive to be retried, instead of silently keeping an in-memory-
	// only event that a crash would lose with no trace.
	if err := t.persist(); err != nil {
		delete(t.events, ev.ID)
		t.mu.Unlock()
		return nil, fmt.Errorf("persist pending event %s: %w", ev.ID, err)
	}
	t.mu.Unlock()

	// Inject the trigger exactly once, here. The Claude endpoint's Send is
	// lossless-buffered (it queues durably and the shim flushes on reconnect),
	// so a "not yet delivered" event is NOT lost and must not be re-injected on
	// every reconcile tick. Connected() is used only as a best-effort signal to
	// stamp DeliveredAt for metrics — never as a gate that re-sends the prompt.
	delivered := t.inject(chatID, t.initialPrompt(ev))
	if delivered {
		t.mu.Lock()
		if e, ok := t.events[ev.ID]; ok && e.Status == StatusPending {
			e.DeliveredAt = t.now()
			_ = t.persist()
		}
		t.mu.Unlock()
	}
	return ev, nil
}

// Ack marks an event acknowledged. A non-empty note is required by design: it
// forces the agent to state what it actually did, creating a lightweight audit
// trail (a reflexive empty ack is rejected). A brief receipt is sent to Jean.
func (t *Tracker) Ack(id, note string) error {
	note = strings.TrimSpace(note)
	if note == "" {
		return errors.New("ack_event requires a non-empty note describing what you did — a bare ack is not accepted")
	}
	t.mu.Lock()
	ev, ok := t.events[id]
	if !ok {
		t.mu.Unlock()
		return fmt.Errorf("no pending event with id %s", id)
	}
	if ev.Status == StatusAcknowledged {
		t.mu.Unlock()
		return fmt.Errorf("event %s was already acknowledged", id)
	}
	ev.Status = StatusAcknowledged
	ev.AckNote = note
	ev.AckedAt = t.now()
	_ = t.persist()
	t.mu.Unlock()

	t.receipt("✓ handled: " + note)
	return nil
}

// NoteReply auto-resolves a still-open event for chatID ONLY when a reply lands
// promptly after it fired (or after its last nudge) — within cfg.ReplyAckWindow.
// That narrow window is real evidence the agent immediately engaged with the
// trigger; a reply outside it is ordinary unrelated chatter on a busy chat and
// must NOT silently ack the event (that would defeat the feature's whole point),
// leaving the explicit ack_event tool as the only path. This supplements, never
// replaces, ack_event. at is the time the reply was observed, and must be a
// reply that was actually delivered to the user (the caller only invokes this
// after the outbound gate passes).
func (t *Tracker) NoteReply(chatID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	changed := false
	for _, ev := range t.events {
		if ev.Status != StatusPending || ev.ChatID != chatID {
			continue
		}
		// Reference is the most recent prompt the agent saw for this event.
		ref := ev.FiredAt
		if ev.LastNudgeAt.After(ref) {
			ref = ev.LastNudgeAt
		}
		if at.After(ref) && at.Sub(ref) <= t.cfg.ReplyAckWindow {
			ev.Status = StatusAcknowledged
			ev.AckNote = "inferred from prompt reply"
			ev.AckedAt = at
			changed = true
			t.logger.Printf("event %s auto-resolved (reply within %s of fire/nudge on chat %s)", ev.ID, t.cfg.ReplyAckWindow, chatID)
		}
	}
	if changed {
		_ = t.persist()
	}
}

// ListPending returns copies of all currently-open events, oldest fire first.
func (t *Tracker) ListPending() []PendingEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]PendingEvent, 0, len(t.events))
	for _, ev := range t.events {
		if ev.Status == StatusPending {
			out = append(out, *ev)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FiredAt.Before(out[j].FiredAt) })
	return out
}

// OpenCount reports the number of currently-open (pending) events.
func (t *Tracker) OpenCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, ev := range t.events {
		if ev.Status == StatusPending {
			n++
		}
	}
	return n
}

// OldestOpenAge reports the age of the oldest still-open event (0 if none).
func (t *Tracker) OldestOpenAge() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	var oldest time.Duration
	for _, ev := range t.events {
		if ev.Status != StatusPending {
			continue
		}
		if age := now.Sub(ev.FiredAt); age > oldest {
			oldest = age
		}
	}
	return oldest
}

// Close stops the reconciliation loop.
func (t *Tracker) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	close(t.stop)
	t.mu.Unlock()
}

// --- internals ---

func (t *Tracker) runLoop() {
	tick := time.NewTicker(t.cfg.TickEvery)
	defer tick.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-tick.C:
			t.Reconcile(t.now())
		}
	}
}

// Reconcile scans every open event and applies exactly the action due at now.
// It is exported so tests can drive escalation timing deterministically with an
// injected clock instead of waiting on the real ticker. Each stage is guarded
// by a persisted timestamp so it happens at most once and survives restarts.
func (t *Tracker) Reconcile(now time.Time) {
	// Gather actions under the lock, then perform I/O (inject/fallback) outside
	// it so a slow send can't block ack/list/fire.
	type action struct {
		eventID      string // the specific event this action escalates
		chatID, text string
		fallback     bool
	}
	var actions []action

	t.mu.Lock()
	changed := false
	for id, ev := range t.events {
		if ev.Status != StatusPending {
			// Prune acknowledged events past the retention window.
			if !ev.AckedAt.IsZero() && now.Sub(ev.AckedAt) > t.cfg.Retention {
				delete(t.events, id)
				changed = true
			}
			continue
		}
		age := now.Sub(ev.FiredAt)
		// NOTE: no undelivered-retry inject here. The initial trigger is
		// injected exactly once at Fire time over a lossless-buffered link, so
		// re-injecting it every tick while the shim is briefly down would just
		// spam duplicate prompts (and could evict other buffered frames). The
		// nudge below is the only escalating re-inject, and it fires at most
		// once by design.
		switch {
		case age >= t.cfg.EscalateAfter:
			// Past escalation: stop nagging the agent, tell Jean directly, once.
			// FallbackSentAt is NOT marked here — it is stamped only after the
			// send actually succeeds (below), so a crash or send failure in the
			// gap doesn't record this last-line-of-defense ping as done.
			if ev.FallbackSentAt.IsZero() {
				mins := int(age.Minutes())
				actions = append(actions, action{ev.ID, "", t.fallbackText(ev, mins), true})
			}
		case age >= t.cfg.NudgeAfter:
			// One escalating re-inject into the agent's own session. Lower
			// stakes than the fallback, so kept at-most-once (marked before the
			// send): a duplicated nudge matters less than a duplicated ping.
			if ev.LastNudgeAt.IsZero() {
				actions = append(actions, action{ev.ID, ev.ChatID, t.nudgePrompt(ev, int(age.Minutes())), false})
				ev.LastNudgeAt = now
				changed = true
			}
		}
	}
	if changed {
		_ = t.persist()
	}
	t.mu.Unlock()

	for _, a := range actions {
		if a.fallback {
			// Persist FallbackSentAt only AFTER the send succeeds. On error,
			// leave it unset so the next tick retries — a possible duplicate
			// ping in a crash-between-send-and-persist window is benign and far
			// better than a silently lost escalation.
			if err := t.fallback(a.text); err != nil {
				t.logger.Printf("fallback send failed for event %s, will retry next tick: %v", a.eventID, err)
				continue
			}
			t.mu.Lock()
			if ev, ok := t.events[a.eventID]; ok && ev.Status == StatusPending {
				ev.FallbackSentAt = now
				_ = t.persist()
			}
			t.mu.Unlock()
			continue
		}
		delivered := t.inject(a.chatID, a.text)
		if delivered {
			// Confirm delivery for exactly the event this action escalated —
			// not every same-chat undelivered event, which could wrongly mark
			// ones that fired between this snapshot and now.
			t.mu.Lock()
			if ev, ok := t.events[a.eventID]; ok && ev.Status == StatusPending && ev.DeliveredAt.IsZero() {
				ev.DeliveredAt = now
				_ = t.persist()
			}
			t.mu.Unlock()
		}
	}
}

// openBySchedule returns an open event fired by scheduleID, or nil. Caller holds mu.
func (t *Tracker) openBySchedule(scheduleID string) *PendingEvent {
	for _, ev := range t.events {
		if ev.Status == StatusPending && ev.ScheduleID == scheduleID {
			return ev
		}
	}
	return nil
}

func (t *Tracker) initialPrompt(ev *PendingEvent) string {
	return "[scheduled trigger you set earlier fired] " + ev.Text +
		"\n\nAct on it now. If it is a reminder for the user, deliver it by calling the " +
		"reply tool with chat_id=\"" + ev.ChatID + "\". If it is a self-wakeup to resume work, continue that work." +
		"\n\nWhen you have handled it, call ack_event with id=\"" + ev.ID + "\" and a short note saying what you did."
}

func (t *Tracker) nudgePrompt(ev *PendingEvent, mins int) string {
	return fmt.Sprintf("[reminder — still unacknowledged] The trigger \"%s\" (event id %s) fired about %d min ago "+
		"and has not been acknowledged. If you already handled it, call ack_event with id=\"%s\" and a note. "+
		"If not, act on it now, then ack_event.", ev.Text, ev.ID, mins, ev.ID)
}

func (t *Tracker) fallbackText(ev *PendingEvent, mins int) string {
	return fmt.Sprintf("⚠️ I missed \"%s\" — it fired %d min ago and hasn't been handled by the agent. "+
		"(event id %s)", ev.Text, mins, ev.ID)
}

// load reads persisted events. Missing file ⇒ empty set.
func (t *Tracker) load() error {
	if t.path == "" {
		return nil
	}
	b, err := os.ReadFile(t.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var items []*PendingEvent
	if err := json.Unmarshal(b, &items); err != nil {
		return fmt.Errorf("parse %s: %w", t.path, err)
	}
	for _, ev := range items {
		if ev.ID == "" {
			continue
		}
		if ev.Status == "" {
			ev.Status = StatusPending
		}
		t.events[ev.ID] = ev
	}
	t.logger.Printf("loaded %d pending event(s) from %s", len(t.events), t.path)
	return nil
}

// persist atomically writes the current set (tmp file + rename). Caller holds
// mu. It returns an error on any failure so callers whose durability matters
// (notably Fire, ahead of a caller-side one-shot schedule deletion) can react
// rather than losing the event silently.
func (t *Tracker) persist() error {
	if t.path == "" {
		return nil
	}
	items := make([]*PendingEvent, 0, len(t.events))
	for _, ev := range t.events {
		items = append(items, ev)
	}
	b, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		t.logger.Printf("warning: marshal pending events: %v", err)
		return err
	}
	tmp := t.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		t.logger.Printf("warning: persist pending events: %v", err)
		return err
	}
	if err := os.Rename(tmp, filepath.Clean(t.path)); err != nil {
		t.logger.Printf("warning: rename pending events: %v", err)
		return err
	}
	return nil
}
