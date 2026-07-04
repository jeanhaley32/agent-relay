// Package access manages who may use the relay: a mutable allowlist, an admin
// set, and a queue of pending access requests that an admin approves via the
// /handshake command. It is self-contained and concurrency-safe. Approved ids
// can be persisted to a JSON file so they survive restarts.
//
// It satisfies the telegram.Authorizer contract (Allowed + Record), so the
// Telegram frontend can defer authorization decisions to it without importing
// this package's admin/approval machinery.
package access

import (
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"
)

// Request is a pending access request from a non-allowlisted sender.
type Request struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	FirstSeen time.Time `json:"first_seen"`
}

// Manager tracks allowlist, admins, and pending requests.
type Manager struct {
	mu       sync.RWMutex
	allowed  map[int64]bool
	admins   map[int64]bool
	pending  map[int64]Request
	savePath string // optional: persist approved allowlist here
	now      func() time.Time
}

// New builds a Manager. Admins are implicitly allowed. If savePath is non-empty
// and exists, previously-approved ids are loaded and merged.
func New(admins, allowed []int64, savePath string) *Manager {
	m := &Manager{
		allowed:  map[int64]bool{},
		admins:   map[int64]bool{},
		pending:  map[int64]Request{},
		savePath: savePath,
		now:      time.Now,
	}
	for _, id := range admins {
		m.admins[id] = true
		m.allowed[id] = true
	}
	for _, id := range allowed {
		m.allowed[id] = true
	}
	m.load()
	return m
}

// Allowed reports whether id may use the relay.
func (m *Manager) Allowed(id int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.allowed[id]
}

// IsAdmin reports whether id may run admin commands.
func (m *Manager) IsAdmin(id int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.admins[id]
}

// Record notes a request from a non-allowlisted sender. No-op if already
// allowed or already pending (keeps the original FirstSeen).
func (m *Manager) Record(id int64, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.allowed[id] {
		return
	}
	if _, ok := m.pending[id]; ok {
		return
	}
	m.pending[id] = Request{ID: id, Name: name, FirstSeen: m.now()}
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

// Approve moves a pending id (or any id) into the allowlist and persists.
// Returns false if the id was not pending.
func (m *Manager) Approve(id int64) bool {
	m.mu.Lock()
	_, wasPending := m.pending[id]
	delete(m.pending, id)
	m.allowed[id] = true
	m.mu.Unlock()
	m.save()
	return wasPending
}

// Deny drops a pending request without granting access.
func (m *Manager) Deny(id int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.pending[id]
	delete(m.pending, id)
	return ok
}

// Allowlist returns the current allowed ids (sorted).
func (m *Manager) Allowlist() []int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]int64, 0, len(m.allowed))
	for id := range m.allowed {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// --- persistence (best-effort) ----------------------------------------------

func (m *Manager) load() {
	if m.savePath == "" {
		return
	}
	b, err := os.ReadFile(m.savePath)
	if err != nil {
		return // no file yet is fine
	}
	var ids []int64
	if json.Unmarshal(b, &ids) == nil {
		for _, id := range ids {
			m.allowed[id] = true
		}
	}
}

func (m *Manager) save() {
	if m.savePath == "" {
		return
	}
	b, err := json.MarshalIndent(m.Allowlist(), "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(m.savePath, b, 0o600)
}
