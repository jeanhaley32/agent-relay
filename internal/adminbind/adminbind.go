// Package adminbind is a small, JSON-persisted map from an admin's sender id
// to the Tailscale device that must show as present before their
// admin-flagged commands are allowed to run. This is deliberately separate
// from the contacts directory (internal/contacts): contacts is about
// *routing* to already-authorized people, this is about *trust* — binding
// stays admin-only and opt-in, and an id with no binding is unaffected (the
// existing admin check behaves exactly as before).
package adminbind

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
	"sync"
)

// Manager is the concurrency-safe, persisted store of {sender_id -> device}.
type Manager struct {
	mu       sync.RWMutex
	bindings map[string]string

	savePath string
	saveMu   sync.Mutex
	logger   *log.Logger
}

// New builds a Manager, loading savePath if non-empty and present.
func New(savePath string, logger *log.Logger) *Manager {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	m := &Manager{bindings: map[string]string{}, savePath: savePath, logger: logger}
	m.load()
	return m
}

// Bind ties senderID's admin-command trust to hostname — an admin-only,
// deliberate action (never inferred).
func (m *Manager) Bind(senderID, hostname string) {
	m.mu.Lock()
	m.bindings[senderID] = hostname
	m.mu.Unlock()
	m.save()
}

// Unbind removes any device binding for senderID, reverting them to the
// unbound (no additional gate) behavior.
func (m *Manager) Unbind(senderID string) {
	m.mu.Lock()
	delete(m.bindings, senderID)
	m.mu.Unlock()
	m.save()
}

// Device returns the bound hostname for senderID, if any.
func (m *Manager) Device(senderID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.bindings[senderID]
	return h, ok
}

// List returns all bindings, sorted by sender id.
func (m *Manager) List() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]string, len(m.bindings))
	for k, v := range m.bindings {
		out[k] = v
	}
	return out
}

func (m *Manager) load() {
	if m.savePath == "" {
		return
	}
	b, err := os.ReadFile(m.savePath)
	if err != nil {
		if !os.IsNotExist(err) {
			m.logger.Printf("adminbind: could not read %s: %v", m.savePath, err)
		}
		return
	}
	_ = os.Chmod(m.savePath, 0o600)
	var bindings map[string]string
	if err := json.Unmarshal(b, &bindings); err != nil {
		m.logger.Printf("adminbind: %s is unreadable — bindings NOT loaded: %v", m.savePath, err)
		return
	}
	m.bindings = bindings
	m.logger.Printf("adminbind: loaded %d binding(s) from %s", len(bindings), m.savePath)
}

func (m *Manager) save() {
	if m.savePath == "" {
		return
	}
	m.saveMu.Lock()
	defer m.saveMu.Unlock()

	m.mu.RLock()
	keys := make([]string, 0, len(m.bindings))
	for k := range m.bindings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]string, len(keys))
	for _, k := range keys {
		ordered[k] = m.bindings[k]
	}
	m.mu.RUnlock()

	b, err := json.MarshalIndent(ordered, "", "  ")
	if err != nil {
		m.logger.Printf("adminbind: marshal: %v", err)
		return
	}
	tmp := m.savePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		m.logger.Printf("adminbind: write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, m.savePath); err != nil {
		m.logger.Printf("adminbind: rename %s: %v", tmp, err)
	}
}
