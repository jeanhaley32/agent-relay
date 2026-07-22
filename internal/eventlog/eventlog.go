// Package eventlog is the relay's durable, append-only audit trail: one JSON
// object per line, one line per thing that happens to a message.
//
// Why this exists: every silent-loss incident so far (half-open socket, the
// dropped-reply `default:` branch, replies written as plain text and never
// sent) was invisible because a message had no identity you could follow. The
// journal only held ad-hoc Printf lines with no correlation, so answering "what
// happened to the message I sent at 16:31?" meant an hour of forensics.
//
// Every record carries msg_id, assigned once at ingress and propagated through
// Meta, so `grep <msg_id> relay-events.jsonl` reconstructs a message's entire
// lifecycle: received -> gated/injected -> replied -> sent/failed.
//
// Writes are line-atomic (a single Write of one newline-terminated JSON object)
// and mutex-guarded, so concurrent producers can't interleave partial lines.
// Failures to log are never fatal: observability must not take the relay down.
package eventlog

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Event names. Keep these stable — they are what you grep and alert on.
const (
	Received    = "received"     // inbound message accepted from a frontend
	GateBlocked = "gate_blocked" // refused before reaching the model (allowlist, session, lockdown, budget)
	Injected    = "injected"     // handed to the Claude session
	Buffered    = "buffered"     // queued because no session was connected
	Reply       = "reply"        // the model produced an outbound reply
	SendOK      = "send_ok"      // frontend confirmed delivery
	SendFailed  = "send_failed"  // delivery failed (err is populated)
	Dropped     = "dropped"      // discarded internally — the class that used to be silent
)

// Record is one line in the log.
type Record struct {
	TS        string `json:"ts"`
	MsgID     string `json:"msg_id"`
	Event     string `json:"event"`
	Frontend  string `json:"frontend,omitempty"` // telegram | discord
	ChatID    string `json:"chat_id,omitempty"`
	FromID    string `json:"from_id,omitempty"`
	InReplyTo string `json:"in_reply_to,omitempty"` // links a reply back to the inbound msg_id
	Bytes     int    `json:"bytes,omitempty"`       // message length; body is NOT logged
	Detail    string `json:"detail,omitempty"`      // reason for gate_blocked/dropped, provider id for send_ok
	Err       string `json:"err,omitempty"`
}

// Logger appends Records to a JSONL file.
type Logger struct {
	mu sync.Mutex
	f  *os.File
}

// Open opens (creating if needed) the JSONL log for appending. A nil Logger is
// valid and simply discards, so callers never need a nil check.
func Open(path string) (*Logger, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &Logger{f: f}, nil
}

// Log appends one record. Safe for concurrent use; never returns an error,
// because a failure to record must not break message flow.
func (l *Logger) Log(r Record) {
	if l == nil || l.f == nil {
		return
	}
	if r.TS == "" {
		r.TS = time.Now().UTC().Format(time.RFC3339Nano)
	}
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	b = append(b, '\n')
	l.mu.Lock()
	_, _ = l.f.Write(b) // single Write => line-atomic
	l.mu.Unlock()
}

// Close flushes and closes the file.
func (l *Logger) Close() error {
	if l == nil || l.f == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

// NewMsgID returns a short, unique id stamped on a message at ingress. Short
// enough to eyeball in a log line and to pass through a <channel> tag, random
// enough not to collide across restarts (unlike a counter).
func NewMsgID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fall back to time-based rather than returning empty: an id that is
		// merely weak still preserves traceability, whereas "" breaks it.
		return hex.EncodeToString([]byte(time.Now().UTC().Format("150405.000")))
	}
	return hex.EncodeToString(b[:])
}
