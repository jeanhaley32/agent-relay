// Package session tracks a per-identity idle-expiring session used to gate
// admin message processing behind tailnet re-authentication. The threat this
// closes: relayd's only identity signal for an inbound message is the
// sender's user_id, which is spoofable if that account is ever compromised.
// Requiring the admin to periodically re-prove tailnet membership (via the
// approval package's tailnet-only /approve page) means a compromised account
// alone isn't enough to keep issuing commands past the idle window.
package session

import (
	"sync"
	"time"
)

// Manager tracks session state for a small set of guarded identities
// (expected usage: just the admin user_id). Safe for concurrent use.
type Manager struct {
	mu     sync.Mutex
	ttl    time.Duration
	active map[string]time.Time // userID -> expiry time
}

func NewManager(ttl time.Duration) *Manager {
	return &Manager{ttl: ttl, active: make(map[string]time.Time)}
}

// Active reports whether userID currently has a live, unexpired session.
func (m *Manager) Active(userID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	expiry, ok := m.active[userID]
	if !ok {
		return false
	}
	if time.Now().After(expiry) {
		delete(m.active, userID)
		return false
	}
	return true
}

// Touch extends an already-active session by the TTL, sliding the idle
// window forward. It does not create a new session - use Activate for that.
func (m *Manager) Touch(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.active[userID]; !ok {
		return
	}
	m.active[userID] = time.Now().Add(m.ttl)
}

// Activate starts (or restarts) a session for userID, valid for the TTL from
// now. Called after a successful tailnet re-authentication.
func (m *Manager) Activate(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active[userID] = time.Now().Add(m.ttl)
}

// ExpireAll immediately invalidates every tracked session, forcing every
// guarded user_id (including whoever called this) to re-authenticate on
// their next message. Used as a manual "kick everyone off" control, e.g. if
// compromise is suspected.
func (m *Manager) ExpireAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active = make(map[string]time.Time)
}
