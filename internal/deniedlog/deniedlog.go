// Package deniedlog is the shared denied-sender audit log for every relay
// frontend. A non-allowlisted sender's attempts (all of them, not just the
// first — Authorizer.Record goes quiet after the first pending entry) are
// captured here as one JSON line each, so an operator can see exactly what
// unauthorized senders tried to say, on any platform, in one place.
//
// Previously this lived inside package telegram, so Discord had no equivalent
// and denied Discord attempts went unrecorded — the reason it's factored out
// here and given a platform field.
package deniedlog

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Logger records a denied sender's attempt. A nil interface value is not
// valid; use Noop for the off switch so call sites never nil-check.
type Logger interface {
	LogDenied(platform string, id int64, name, chatID, text string)
}

// Noop discards every denial — the default when no path is configured.
type Noop struct{}

func (Noop) LogDenied(string, int64, string, string, string) {}

// Entry is one recorded denial (exported so tests and log readers can decode
// it). platform distinguishes which frontend the attempt came in on, since a
// single log now aggregates all of them.
type Entry struct {
	At       string `json:"at"`
	Platform string `json:"platform"`
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	ChatID   string `json:"chat_id"`
	Text     string `json:"text"`
}

// FileDeniedLogger appends one JSON line per denied-sender attempt to a file,
// creating it (and any parent directories) if needed. Safe for concurrent use.
type FileDeniedLogger struct {
	mu   sync.Mutex
	file *os.File
}

// NewFileDeniedLogger opens path for append, creating it if it doesn't exist.
func NewFileDeniedLogger(path string) (*FileDeniedLogger, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileDeniedLogger{file: f}, nil
}

func (d *FileDeniedLogger) LogDenied(platform string, id int64, name, chatID, text string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	line, err := json.Marshal(Entry{
		At:       time.Now().UTC().Format(time.RFC3339),
		Platform: platform,
		ID:       id,
		Name:     name,
		ChatID:   chatID,
		Text:     text,
	})
	if err != nil {
		return
	}
	line = append(line, '\n')
	_, _ = d.file.Write(line)
}

func (d *FileDeniedLogger) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}
