// Package scheduler is a small, persistent reminder engine. It fires a delivery
// callback either on a recurring cron spec (e.g. "0 9 * * *" — 9am daily) or
// once after a delay (e.g. 20 minutes from now). Schedules survive process
// restarts: they are stored as JSON and re-armed on load.
//
// It knows nothing about the relay, Claude, or Telegram — it holds text + a
// target key (chatID) and calls Deliver when a schedule is due. The daemon wires
// Deliver to inject a reminder into the Claude session.
package scheduler

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// Schedule is one reminder. Exactly one of Cron or FireAt is set: Cron ⇒
// recurring, FireAt ⇒ one-shot at that instant.
type Schedule struct {
	ID        string    `json:"id"`
	Text      string    `json:"text"`
	Cron      string    `json:"cron,omitempty"`
	FireAt    time.Time `json:"fire_at,omitempty"`
	ChatID    string    `json:"chat_id"`
	CreatedAt time.Time `json:"created_at"`
}

// Recurring reports whether this is a cron schedule (vs a one-shot).
func (s *Schedule) Recurring() bool { return s.Cron != "" }

// DeliverFunc is invoked when a schedule is due. It must not block for long; the
// daemon's implementation just enqueues an inject.
type DeliverFunc func(chatID, text string)

// Scheduler owns the cron runner, one-shot timers, and the persisted set.
type Scheduler struct {
	path    string
	loc     *time.Location
	deliver DeliverFunc
	logger  *log.Logger

	mu      sync.Mutex
	cron    *cron.Cron
	items   map[string]*Schedule
	entries map[string]cron.EntryID // recurring: schedule id -> cron entry
	timers  map[string]*time.Timer  // one-shot: schedule id -> timer
}

// New builds a Scheduler, loads any persisted schedules from path, re-arms them,
// and starts the cron runner. loc is the timezone cron specs are interpreted in
// (nil ⇒ time.Local). Missed one-shots (FireAt already past) fire once shortly
// after load so a restart during downtime doesn't silently eat a reminder.
func New(path string, loc *time.Location, deliver DeliverFunc, logger *log.Logger) (*Scheduler, error) {
	if deliver == nil {
		return nil, errors.New("scheduler: nil deliver")
	}
	if loc == nil {
		loc = time.Local
	}
	if logger == nil {
		logger = log.New(os.Stderr, "[scheduler] ", log.LstdFlags)
	}
	s := &Scheduler{
		path:    path,
		loc:     loc,
		deliver: deliver,
		logger:  logger,
		cron:    cron.New(cron.WithLocation(loc)),
		items:   map[string]*Schedule{},
		entries: map[string]cron.EntryID{},
		timers:  map[string]*time.Timer{},
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	s.cron.Start()
	return s, nil
}

// Create validates and arms a new schedule. Provide exactly one of cronSpec (a
// standard 5-field cron spec or @descriptor) or in (a positive delay for a
// one-shot). It persists the set and returns the stored schedule.
func (s *Scheduler) Create(text, cronSpec string, in time.Duration, chatID string) (*Schedule, error) {
	if text == "" {
		return nil, errors.New("empty reminder text")
	}
	if (cronSpec == "") == (in <= 0) {
		return nil, errors.New("provide exactly one of a cron spec or a positive delay")
	}
	if cronSpec != "" {
		if _, err := cron.ParseStandard(cronSpec); err != nil {
			return nil, fmt.Errorf("invalid cron spec %q: %w", cronSpec, err)
		}
	}
	sc := &Schedule{
		ID:        newID(),
		Text:      text,
		ChatID:    chatID,
		Cron:      cronSpec,
		CreatedAt: time.Now().In(s.loc),
	}
	if cronSpec == "" {
		sc.FireAt = time.Now().In(s.loc).Add(in)
	}

	s.mu.Lock()
	s.items[sc.ID] = sc
	if err := s.arm(sc); err != nil {
		delete(s.items, sc.ID)
		s.mu.Unlock()
		return nil, err
	}
	err := s.save()
	s.mu.Unlock()
	if err != nil {
		s.logger.Printf("warning: persist failed: %v", err)
	}
	return sc, nil
}

// List returns the active schedules, soonest next-fire first.
func (s *Scheduler) List() []*Schedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Schedule, 0, len(s.items))
	for _, sc := range s.items {
		out = append(out, sc)
	}
	sort.Slice(out, func(i, j int) bool { return s.next(out[i]).Before(s.next(out[j])) })
	return out
}

// Cancel disarms and forgets a schedule. Returns false if the id is unknown.
func (s *Scheduler) Cancel(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; !ok {
		return false
	}
	s.disarm(id)
	delete(s.items, id)
	if err := s.save(); err != nil {
		s.logger.Printf("warning: persist failed: %v", err)
	}
	return true
}

// Next reports when a schedule will next fire (zero if it never will).
func (s *Scheduler) Next(sc *Schedule) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next(sc)
}

// Close stops the cron runner and all one-shot timers.
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.timers {
		t.Stop()
	}
	s.cron.Stop()
}

// --- internals (callers hold s.mu unless noted) ---

func (s *Scheduler) next(sc *Schedule) time.Time {
	if sc.Recurring() {
		sched, err := cron.ParseStandard(sc.Cron)
		if err != nil {
			return time.Time{}
		}
		return sched.Next(time.Now().In(s.loc))
	}
	return sc.FireAt
}

// arm registers a schedule with the cron runner or a one-shot timer.
func (s *Scheduler) arm(sc *Schedule) error {
	if sc.Recurring() {
		id, err := s.cron.AddFunc(sc.Cron, func() { s.fire(sc.ID) })
		if err != nil {
			return err
		}
		s.entries[sc.ID] = id
		return nil
	}
	d := time.Until(sc.FireAt)
	if d < 0 {
		d = 0 // missed while down: fire ~immediately
	}
	s.timers[sc.ID] = time.AfterFunc(d, func() { s.fire(sc.ID) })
	return nil
}

func (s *Scheduler) disarm(id string) {
	if eid, ok := s.entries[id]; ok {
		s.cron.Remove(eid)
		delete(s.entries, id)
	}
	if t, ok := s.timers[id]; ok {
		t.Stop()
		delete(s.timers, id)
	}
}

// fire delivers a schedule. One-shots are removed after firing; recurring stay
// armed. Runs on a cron/timer goroutine, so it takes the lock itself.
func (s *Scheduler) fire(id string) {
	s.mu.Lock()
	sc, ok := s.items[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	chatID, text, oneShot := sc.ChatID, sc.Text, !sc.Recurring()
	if oneShot {
		s.disarm(id)
		delete(s.items, id)
		if err := s.save(); err != nil {
			s.logger.Printf("warning: persist failed: %v", err)
		}
	}
	s.mu.Unlock()

	s.logger.Printf("firing schedule %s -> chat %s", id, chatID)
	s.deliver(chatID, text)
}

// load reads persisted schedules and arms them. Missing file ⇒ empty set. Holds
// s.mu because arming a missed one-shot can fire (on another goroutine) before
// load returns.
func (s *Scheduler) load() error {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var items []*Schedule
	if err := json.Unmarshal(b, &items); err != nil {
		return fmt.Errorf("parse %s: %w", s.path, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sc := range items {
		if sc.ID == "" {
			continue
		}
		s.items[sc.ID] = sc
		if err := s.arm(sc); err != nil {
			s.logger.Printf("warning: dropping unarmable schedule %s: %v", sc.ID, err)
			delete(s.items, sc.ID)
		}
	}
	s.logger.Printf("loaded %d schedule(s) from %s", len(s.items), s.path)
	return nil
}

// save atomically writes the current set (tmp file + rename).
func (s *Scheduler) save() error {
	if s.path == "" {
		return nil
	}
	items := make([]*Schedule, 0, len(s.items))
	for _, sc := range s.items {
		items = append(items, sc)
	}
	b, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Clean(s.path))
}

// newID returns a short random hex id, stable across restarts (unlike a counter).
func newID() string {
	var b [5]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
