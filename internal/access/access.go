// Package access manages who may use the relay: a mutable allowlist, an admin
// set, a bounded queue of pending access requests, and a denylist. Approvals and
// denials persist to a JSON file. It is self-contained and concurrency-safe.
//
// It satisfies the telegram.Authorizer contract (Allowed + Record), so the
// Telegram frontend can defer authorization to it without importing the
// admin/approval machinery.
package access

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// maxPending bounds the pending-request queue so strangers can't exhaust
	// memory or bury a real request; oldest entries are evicted past this.
	maxPending = 100
	// maxNameLen truncates attacker-controlled sender names.
	maxNameLen = 64
)

// Request is a pending access request from a non-allowlisted sender.
type Request struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	FirstSeen time.Time `json:"first_seen"`
}

// Manager tracks allowlist, admins, pending requests, and denials.
type Manager struct {
	mu      sync.RWMutex
	allowed map[int64]bool
	admins  map[int64]bool
	pending map[int64]Request
	order   []int64 // FIFO of pending ids, for bounded eviction
	denied  map[int64]bool
	// totalRecorded counts every genuinely NEW pending request ever queued
	// (not re-attempts from the same still-pending sender) - the metric
	// behind an "unrecognized access attempt" alert, distinct from
	// PendingCount() which only reflects the current unresolved queue.
	totalRecorded int64

	savePath string
	saveMu   sync.Mutex // serializes disk writes (independent of mu)
	logger   *log.Logger
	now      func() time.Time
}

// persisted is the on-disk format. A legacy plain array of ids is also accepted
// on load for backward compatibility.
type persisted struct {
	Allowed []int64 `json:"allowed"`
	Denied  []int64 `json:"denied"`
}

// New builds a Manager. Admins are implicitly allowed. Ids ≤ 0 are rejected.
// If savePath is non-empty it is loaded (and future changes persisted). logger
// may be nil.
func New(admins, allowed []int64, savePath string, logger *log.Logger) *Manager {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	m := &Manager{
		allowed:  map[int64]bool{},
		admins:   map[int64]bool{},
		pending:  map[int64]Request{},
		denied:   map[int64]bool{},
		savePath: savePath,
		logger:   logger,
		now:      time.Now,
	}
	for _, id := range admins {
		if id <= 0 {
			logger.Printf("access: ignoring invalid admin id %d", id)
			continue
		}
		m.admins[id] = true
		m.allowed[id] = true
	}
	for _, id := range allowed {
		if id > 0 {
			m.allowed[id] = true
		} else {
			logger.Printf("access: ignoring invalid allowlist id %d", id)
		}
	}
	m.load()
	return m
}

// Allowed reports whether id may use the relay. Ids ≤ 0 are never allowed.
func (m *Manager) Allowed(id int64) bool {
	if id <= 0 {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.allowed[id]
}

// IsAdmin reports whether id may run admin commands. Ids ≤ 0 are never admin.
func (m *Manager) IsAdmin(id int64) bool {
	if id <= 0 {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.admins[id]
}

// Record queues a request from a non-allowlisted sender. No-op for ids ≤ 0, or
// if already allowed, denied, or pending. Names are sanitized and truncated; the
// queue is bounded (oldest evicted).
func (m *Manager) Record(id int64, name string) {
	if id <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.allowed[id] || m.denied[id] {
		return
	}
	if _, ok := m.pending[id]; ok {
		return
	}
	for len(m.pending) >= maxPending && len(m.order) > 0 {
		oldest := m.order[0]
		m.order = m.order[1:]
		delete(m.pending, oldest)
	}
	m.pending[id] = Request{ID: id, Name: sanitizeName(name), FirstSeen: m.now()}
	m.order = append(m.order, id)
	m.totalRecorded++
}

// TotalRecorded returns totalRecorded.
func (m *Manager) TotalRecorded() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalRecorded
}

// sanitizeName strips control characters/newlines and truncates, so a sender's
// name can't forge lines in the /handshake listing.
func sanitizeName(s string) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if utf8.RuneCountInString(s) > maxNameLen {
		r := []rune(s)
		s = strings.TrimSpace(string(r[:maxNameLen])) // truncate on a rune boundary
	}
	return s
}

// Pending returns the outstanding requests, oldest first.
func (m *Manager) Pending() []Request {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Request, 0, len(m.pending))
	for _, r := range m.pending {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FirstSeen.Before(out[j].FirstSeen) })
	return out
}

// Approve grants access to an id that is currently pending OR previously denied
// (clearing the denial — this is the un-deny path). Returns false (and grants
// nothing) if the id is neither pending nor denied — so a typo can't silently
// authorize a never-seen stranger.
func (m *Manager) Approve(id int64) bool {
	if id <= 0 {
		return false
	}
	m.mu.Lock()
	_, pending := m.pending[id]
	denied := m.denied[id]
	if !pending && !denied {
		m.mu.Unlock()
		return false
	}
	m.removePending(id)
	delete(m.denied, id) // reverse a prior Deny
	m.allowed[id] = true
	m.mu.Unlock()
	m.save()
	return true
}

// Deny drops a pending request and remembers the denial (persisted) so the
// sender can't immediately re-queue. It only denylists ids that were actually
// pending — a stray Deny of an unknown id is a no-op (returns false), so it
// can't permanently block someone who never asked. Reverse a denial with Approve.
func (m *Manager) Deny(id int64) bool {
	if id <= 0 {
		return false
	}
	m.mu.Lock()
	if _, ok := m.pending[id]; !ok {
		m.mu.Unlock()
		return false
	}
	m.removePending(id)
	m.denied[id] = true
	m.mu.Unlock()
	m.save()
	return true
}

// removePending deletes from the pending map and order slice. Caller holds mu.
func (m *Manager) removePending(id int64) {
	delete(m.pending, id)
	for i, x := range m.order {
		if x == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
}

// Allowlist returns the current allowed ids (sorted).
func (m *Manager) Allowlist() []int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return sortedKeys(m.allowed)
}

func sortedKeys(set map[int64]bool) []int64 {
	out := make([]int64, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// --- persistence (atomic, serialized, logged) -------------------------------

func (m *Manager) load() {
	if m.savePath == "" {
		return
	}
	b, err := os.ReadFile(m.savePath)
	if err != nil {
		if !os.IsNotExist(err) {
			m.logger.Printf("access: could not read %s: %v", m.savePath, err)
		}
		return
	}
	_ = os.Chmod(m.savePath, 0o600) // tighten a pre-existing loose file

	var p persisted
	if err := json.Unmarshal(b, &p); err != nil {
		var ids []int64 // legacy format: a bare array of allowed ids
		if err2 := json.Unmarshal(b, &ids); err2 != nil {
			m.logger.Printf("access: %s is unreadable — approvals NOT loaded: %v", m.savePath, err)
			return
		}
		p.Allowed = ids
	}
	n := 0
	for _, id := range p.Allowed {
		if id > 0 {
			m.allowed[id] = true
			n++
		}
	}
	for _, id := range p.Denied {
		if id > 0 {
			m.denied[id] = true
		}
	}
	m.logger.Printf("access: loaded %d allowed id(s) from %s", n, m.savePath)
}

func (m *Manager) save() {
	if m.savePath == "" {
		return
	}
	m.saveMu.Lock()
	defer m.saveMu.Unlock()

	m.mu.RLock()
	p := persisted{Allowed: sortedKeys(m.allowed), Denied: sortedKeys(m.denied)}
	m.mu.RUnlock()

	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		m.logger.Printf("access: marshal allowlist: %v", err)
		return
	}
	tmp := m.savePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		m.logger.Printf("access: write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, m.savePath); err != nil { // atomic replace
		m.logger.Printf("access: rename %s: %v", tmp, err)
	}
}
