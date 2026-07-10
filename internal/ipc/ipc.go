// Package ipc defines the tiny wire protocol between the relay daemon and the
// per-session stdio shim that Claude Code spawns. It is newline-delimited JSON
// over any io.ReadWriteCloser (in practice a unix-domain socket).
//
// Two frame kinds cross the link:
//
//	inject  daemon -> shim : push a message into the Claude session
//	reply   shim  -> daemon: Claude called the reply tool
//
// This mirrors the split described in DESIGN.md: the daemon is long-lived and
// hosts the broker; the shim is a thin bridge Claude Code owns over stdio.
package ipc

import (
	"encoding/json"
	"io"
	"sync"
)

// Kind is the frame type.
type Kind string

const (
	KindInject      Kind = "inject"       // daemon -> shim: push a message into the session
	KindReply       Kind = "reply"        // shim -> daemon: Claude called the reply tool
	KindPermRequest Kind = "perm_request" // shim -> daemon: Claude wants a tool approved
	KindPermVerdict Kind = "perm_verdict" // daemon -> shim: allow/deny a pending tool request
	KindSchedReq    Kind = "sched_req"    // shim -> daemon: a schedule tool was called
	KindSchedResp   Kind = "sched_resp"   // daemon -> shim: result of a schedule op
)

// Schedule op names carried in Frame.Op for KindSchedReq.
const (
	OpScheduleCreate = "create"
	OpScheduleList   = "list"
	OpScheduleCancel = "cancel"

	// Pending-event ops (carried in Frame.Op for KindSchedReq): the agent
	// acknowledges a fired event or lists currently-open events.
	OpEventAck  = "event_ack"
	OpEventList = "event_list"
)

// Frame is one message on the link. Message fields (ChatID/Text/Meta) are used
// by inject/reply; permission fields (RequestID/Tool/Detail/Allow) by the
// perm_* kinds.
type Frame struct {
	Kind   Kind              `json:"kind"`
	ChatID string            `json:"chat_id,omitempty"`
	Text   string            `json:"text,omitempty"`
	Meta   map[string]string `json:"meta,omitempty"`

	// Permission-relay fields.
	RequestID string `json:"request_id,omitempty"` // id of the tool-approval request
	Tool      string `json:"tool,omitempty"`       // tool name (e.g. "Bash")
	Detail    string `json:"detail,omitempty"`     // human-readable description
	Allow     bool   `json:"allow,omitempty"`      // verdict (perm_verdict only)

	// Scheduler fields. RequestID doubles as the correlation id matching a
	// sched_resp to its sched_req; Text/ChatID carry the reminder.
	Op        string `json:"op,omitempty"`         // create | list | cancel (sched_req)
	Cron      string `json:"cron,omitempty"`       // recurring cron spec (create)
	InSeconds int64  `json:"in_seconds,omitempty"` // one-shot delay in seconds (create)
	SchedID   string `json:"sched_id,omitempty"`   // schedule id (cancel req / create resp)
	Result    string `json:"result,omitempty"`     // human-readable result (sched_resp)
	Err       string `json:"err,omitempty"`        // error text, empty on success (sched_resp)
}

// Conn is a framed JSON connection. Send is safe for concurrent use; Recv must
// be called from a single goroutine (one reader).
type Conn struct {
	rwc io.ReadWriteCloser
	dec *json.Decoder

	mu  sync.Mutex // guards enc
	enc *json.Encoder
}

// NewConn wraps a stream (e.g. a unix socket) in the framed protocol.
func NewConn(rwc io.ReadWriteCloser) *Conn {
	return &Conn{
		rwc: rwc,
		dec: json.NewDecoder(rwc),
		enc: json.NewEncoder(rwc), // Encode appends '\n' => framing
	}
}

// Send writes one frame. Safe for concurrent callers.
func (c *Conn) Send(f Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enc.Encode(f)
}

// Recv reads the next frame, blocking until one arrives or the stream ends
// (io.EOF). Call from a single goroutine.
func (c *Conn) Recv() (Frame, error) {
	var f Frame
	err := c.dec.Decode(&f)
	return f, err
}

// Close closes the underlying stream.
func (c *Conn) Close() error { return c.rwc.Close() }
