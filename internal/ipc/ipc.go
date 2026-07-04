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
	KindInject Kind = "inject" // daemon -> shim
	KindReply  Kind = "reply"  // shim -> daemon
)

// Frame is one message on the link.
type Frame struct {
	Kind   Kind              `json:"kind"`
	ChatID string            `json:"chat_id"`
	Text   string            `json:"text"`
	Meta   map[string]string `json:"meta,omitempty"`
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
