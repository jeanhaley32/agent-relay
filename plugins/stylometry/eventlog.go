package stylometry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// EventLog appends one JSON line per scored window to a plain file — the
// "open interface" for inspecting what caused an alert: readable with
// `cat`/`grep`/`jq`, no query API needed, since whoever's asking "what
// caused that" already has shell access to the machine running this.
type EventLog struct {
	Path string

	mu sync.Mutex
}

// Append writes e as one JSON line, creating the parent directory and file
// if needed. Safe for concurrent use.
func (l *EventLog) Append(e Explanation) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(l.Path), 0o755); err != nil {
		return fmt.Errorf("stylometry: event log: mkdir: %w", err)
	}
	f, err := os.OpenFile(l.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("stylometry: event log: open: %w", err)
	}
	defer f.Close()

	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("stylometry: event log: marshal: %w", err)
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("stylometry: event log: write: %w", err)
	}
	return nil
}
