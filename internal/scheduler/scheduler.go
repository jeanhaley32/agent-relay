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

	"github.com/fsnotify/fsnotify"
	"github.com/robfig/cron/v3"
)

// Provenance values for Schedule.Source. SourceTool is stamped by Create();
// SourceFile is stamped when the file watcher finds a schedule that appeared
// in the persisted file without going through Create() — e.g. a hand-edit.
// Legacy records persisted before this field existed load with Source == ""
// and are treated as SourceTool (List/metrics normalize the empty case).
const (
	SourceTool = "tool"
	SourceFile = "file"
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
	// Source records how this schedule entered the running process:
	// SourceTool (via Create()) or SourceFile (found in schedules.json by the
	// watcher without a matching Create() call — e.g. a hand-edit while
	// relayd was running). Purely informational; file-sourced schedules are
	// armed exactly like any other, never gated on this field.
	Source string `json:"source,omitempty"`
}

// EffectiveSource returns Source, normalizing legacy empty values (schedules
// persisted before this field existed) to SourceTool.
func (s *Schedule) EffectiveSource() string {
	if s.Source == "" {
		return SourceTool
	}
	return s.Source
}

// Recurring reports whether this is a cron schedule (vs a one-shot).
func (s *Schedule) Recurring() bool { return s.Cron != "" }

// DeliverFunc is invoked when a schedule is due. scheduleID is the firing
// schedule's id (so the daemon can coalesce repeat fires of a recurring
// schedule onto one pending event). It must not block for long; the daemon's
// implementation just records a pending event and enqueues an inject. It
// returns an error if the fired event could not be durably recorded, so fire()
// can skip deleting a one-shot schedule and leave it for a retry.
type DeliverFunc func(scheduleID, chatID, text string) error

// ExternalFunc is invoked when the file watcher finds a schedule that entered
// schedules.json without going through Create() (a hand-edit while relayd was
// running). kind is "added" or "modified". It must not block for long — the
// daemon's implementation just injects a heads-up into the agent's session.
type ExternalFunc func(sc *Schedule, kind string)

// Scheduler owns the cron runner, one-shot timers, and the persisted set.
type Scheduler struct {
	path     string
	loc      *time.Location
	deliver  DeliverFunc
	external ExternalFunc // optional; nil ⇒ no notification on file-detected changes
	logger   *log.Logger

	mu      sync.Mutex
	cron    *cron.Cron
	items   map[string]*Schedule
	entries map[string]cron.EntryID // recurring: schedule id -> cron entry
	timers  map[string]*time.Timer  // one-shot: schedule id -> timer

	watcher   *fsnotify.Watcher
	watchDone chan struct{}
}

// New builds a Scheduler, loads any persisted schedules from path, re-arms them,
// and starts the cron runner. loc is the timezone cron specs are interpreted in
// (nil ⇒ time.Local). Missed one-shots (FireAt already past) fire once shortly
// after load so a restart during downtime doesn't silently eat a reminder.
// external, if non-nil, is called whenever the file watcher detects a schedule
// that was added or modified directly in schedules.json rather than via
// Create() — see ExternalFunc. A nil external disables the watcher entirely
// (kept optional so tests that don't care about it don't need a temp-dir
// watch and its inherent event latency).
func New(path string, loc *time.Location, deliver DeliverFunc, external ExternalFunc, logger *log.Logger) (*Scheduler, error) {
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
		path:     path,
		loc:      loc,
		deliver:  deliver,
		external: external,
		logger:   logger,
		cron:     cron.New(cron.WithLocation(loc)),
		items:    map[string]*Schedule{},
		entries:  map[string]cron.EntryID{},
		timers:   map[string]*time.Timer{},
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	s.cron.Start()
	if external != nil && path != "" {
		if err := s.startWatch(); err != nil {
			// Non-fatal: the scheduler is fully functional without the
			// watcher, it just loses live detection of hand-edits until
			// the next restart. Don't fail startup over it.
			s.logger.Printf("warning: file watch disabled: %v", err)
		}
	}
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
		Source:    SourceTool,
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

// OutOfBandCount reports how many currently-armed schedules were last
// detected as file-sourced (SourceFile) by the watcher rather than created
// via Create(). For dashboard visibility, not an alerting signal on its own.
func (s *Scheduler) OutOfBandCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sc := range s.items {
		if sc.Source == SourceFile {
			n++
		}
	}
	return n
}

// Close stops the cron runner, all one-shot timers, and the file watcher.
func (s *Scheduler) Close() {
	s.mu.Lock()
	for _, t := range s.timers {
		t.Stop()
	}
	s.cron.Stop()
	w := s.watcher
	done := s.watchDone
	s.mu.Unlock()
	if w != nil {
		_ = w.Close()
	}
	if done != nil {
		<-done
	}
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
	schedID, chatID, text, oneShot := sc.ID, sc.ChatID, sc.Text, !sc.Recurring()
	s.mu.Unlock()

	s.logger.Printf("firing schedule %s -> chat %s", id, chatID)
	// Deliver BEFORE any one-shot cleanup so the durable pending event is
	// created (and persisted by the deliver callback) before this schedule is
	// deleted from its own file. A crash in between then loses neither: worst
	// case the schedule fires again on reload, which the tracker coalesces.
	if err := s.deliver(schedID, chatID, text); err != nil {
		// The pending event was NOT durably recorded. Keep the schedule intact
		// rather than deleting it and losing the event. Recurring schedules
		// retry on their next cron tick; a one-shot's timer has already fired
		// and is not re-armed here, so it only retries if the process
		// restarts (load re-arms missed one-shots).
		s.logger.Printf("warning: deliver of schedule %s failed, keeping it for retry: %v", id, err)
		return
	}

	if oneShot {
		s.mu.Lock()
		s.disarm(id)
		delete(s.items, id)
		if err := s.save(); err != nil {
			s.logger.Printf("warning: persist failed: %v", err)
		}
		s.mu.Unlock()
	}
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

// startWatch arms an fsnotify watch on the directory containing s.path (not
// the file itself — save()'s tmp+rename replaces the inode on every write, so
// a watch on the file alone would be silently dropped after the first save).
// Events for other files in the same directory are filtered out.
func (s *Scheduler) startWatch() error {
	dir := filepath.Dir(s.path)
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	if err := w.Add(dir); err != nil {
		_ = w.Close()
		return fmt.Errorf("watch %s: %w", dir, err)
	}
	s.watcher = w
	s.watchDone = make(chan struct{})
	go s.watchLoop(w)
	s.logger.Printf("watching %s for external edits", s.path)
	return nil
}

// watchLoop debounces bursts of fsnotify events (a single save() can produce
// more than one, e.g. the tmp-file write plus the rename) into one
// reconcileFromFile call ~300ms after the last relevant event.
func (s *Scheduler) watchLoop(w *fsnotify.Watcher) {
	defer close(s.watchDone)
	target := filepath.Clean(s.path)
	var debounce *time.Timer
	defer func() {
		if debounce != nil {
			debounce.Stop()
		}
	}()
	for {
		select {
		case ev, ok := <-w.Events:
			if !ok {
				return
			}
			if filepath.Clean(ev.Name) != target {
				continue
			}
			if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
				continue
			}
			if debounce != nil {
				debounce.Stop()
			}
			debounce = time.AfterFunc(300*time.Millisecond, s.reconcileFromFile)
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			s.logger.Printf("warning: file watch error: %v", err)
		}
	}
}

// reconcileFromFile re-reads schedules.json and diffs it against the live,
// in-memory set. This is what makes hand-edits to the file take effect
// without a restart:
//   - an id present in the file but not in memory is armed and tagged
//     SourceFile (unless the file entry already carries an explicit Source);
//   - an id present in both, whose content differs, is re-armed from the new
//     definition and tagged SourceFile (a hand-edit overriding a tool-created
//     schedule still counts as external provenance going forward);
//   - an id present in memory but missing from the file is left alone —
//     deliberately NOT disarmed, so an external edit can never silently kill
//     a running schedule; it's simply re-persisted on the next Create/Cancel.
//
// Re-firing this on the scheduler's own save() writes is expected and
// harmless: at that point the file matches s.items exactly, so the diff finds
// nothing to do and returns without notifying or re-saving.
func (s *Scheduler) reconcileFromFile() {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			s.logger.Printf("warning: file watch: read failed: %v", err)
		}
		return
	}
	var fileItems []*Schedule
	if err := json.Unmarshal(b, &fileItems); err != nil {
		s.logger.Printf("warning: file watch: parse failed: %v", err)
		return
	}

	s.mu.Lock()
	var notify []*Schedule
	var kinds []string
	changed := false
	for _, fsc := range fileItems {
		if fsc.ID == "" {
			continue
		}
		cur, exists := s.items[fsc.ID]
		var kind string
		switch {
		case !exists:
			kind = "added"
		case cur.Text != fsc.Text || cur.Cron != fsc.Cron || !cur.FireAt.Equal(fsc.FireAt) || cur.ChatID != fsc.ChatID:
			kind = "modified"
			fsc.CreatedAt = cur.CreatedAt // preserve original creation time
		default:
			continue // identical to what's already armed — likely our own save()
		}
		if fsc.Source == "" {
			fsc.Source = SourceFile
		}
		if exists {
			s.disarm(fsc.ID)
		}
		if err := s.arm(fsc); err != nil {
			s.logger.Printf("warning: file watch: dropping unarmable schedule %s: %v", fsc.ID, err)
			if exists {
				delete(s.items, fsc.ID) // it was armed a moment ago; now genuinely gone
			}
			continue
		}
		s.items[fsc.ID] = fsc
		changed = true
		notify = append(notify, fsc)
		kinds = append(kinds, kind)
	}
	if changed {
		if err := s.save(); err != nil {
			s.logger.Printf("warning: file watch: persist after reconcile failed: %v", err)
		}
	}
	ext := s.external
	s.mu.Unlock()

	for i, sc := range notify {
		s.logger.Printf("external schedule %s (%s): id=%s cron=%q chat=%s", kinds[i], SourceFile, sc.ID, sc.Cron, sc.ChatID)
		if ext != nil {
			ext(sc, kinds[i])
		}
	}
}

// newID returns a short random hex id, stable across restarts (unlike a counter).
func newID() string {
	var b [5]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
