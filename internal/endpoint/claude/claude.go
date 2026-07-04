// Package claude implements the daemon-side Claude Code backend as a
// relay.Endpoint. It listens on a unix socket for the stdio shim (which Claude
// Code spawns) to connect. Send pushes an "inject" frame to the shim (→ becomes
// a <channel> event in the Claude session); replies arrive as "reply" frames
// and surface on Recv. The broker sees an ordinary Endpoint; the stdio inversion
// is contained here + in cmd/relay-shim.
//
// Session model (v1): a single connected shim/session. If a new shim connects,
// it replaces the previous one. Per-conversation sessions are a future step.
package claude

import (
	"context"
	"net"
	"os"
	"sync"

	"github.com/jeanhaley32/agent-relay/internal/ipc"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// PermRequest is a tool-approval request surfaced from the Claude session.
type PermRequest struct {
	ID     string // request id to echo in the verdict
	Tool   string // tool name (e.g. "Bash")
	Detail string // human-readable description
}

// Endpoint is the Claude Code backend.
type Endpoint struct {
	socketPath string
	ln         net.Listener
	recv       chan relay.Message
	perms      chan PermRequest

	mu   sync.Mutex
	conn *ipc.Conn // current shim connection (nil until one connects)

	closeOnce sync.Once
}

// New starts listening on socketPath for the shim and returns the endpoint. The
// caller launches Claude Code with cmd/relay-shim pointed at the same socket.
func New(socketPath string) (*Endpoint, error) {
	_ = os.Remove(socketPath) // clear any stale socket
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	e := &Endpoint{
		socketPath: socketPath,
		ln:         ln,
		recv:       make(chan relay.Message, 32),
		perms:      make(chan PermRequest, 32),
	}
	go e.acceptLoop()
	return e, nil
}

func (e *Endpoint) Name() string               { return "claude" }
func (e *Endpoint) Recv() <-chan relay.Message { return e.recv }

// Permissions delivers tool-approval requests from the Claude session. Consume
// these and call Decide to answer. If no one consumes them, tool calls that need
// approval will simply wait (Claude's local dialog stays open).
func (e *Endpoint) Permissions() <-chan PermRequest { return e.perms }

// Decide answers a pending permission request: allow=true proceeds, false rejects.
func (e *Endpoint) Decide(requestID string, allow bool) error {
	e.mu.Lock()
	c := e.conn
	e.mu.Unlock()
	if c == nil {
		return ErrNoSession
	}
	return c.Send(ipc.Frame{Kind: ipc.KindPermVerdict, RequestID: requestID, Allow: allow})
}

// acceptLoop accepts shim connections. Only the most recent is used for Send;
// each connection's reply stream is pumped onto recv.
func (e *Endpoint) acceptLoop() {
	defer close(e.recv)
	defer close(e.perms)
	for {
		nc, err := e.ln.Accept()
		if err != nil {
			return // listener closed
		}
		c := ipc.NewConn(nc)
		e.mu.Lock()
		e.conn = c
		e.mu.Unlock()
		e.readReplies(c)
	}
}

// readReplies pumps reply frames from one shim connection onto recv until it
// closes, then loops back to accept the next connection.
func (e *Endpoint) readReplies(c *ipc.Conn) {
	for {
		f, err := c.Recv()
		if err != nil {
			e.mu.Lock()
			if e.conn == c {
				e.conn = nil
			}
			e.mu.Unlock()
			return
		}
		switch f.Kind {
		case ipc.KindReply:
			msg := relay.Message{
				ConversationID: f.ChatID,
				Role:           relay.Assistant,
				Text:           f.Text,
				Meta:           map[string]string{"chat_id": f.ChatID},
			}
			select {
			case e.recv <- msg:
			default: // drop if the consumer is gone/slow rather than block accept loop
			}
		case ipc.KindPermRequest:
			select {
			case e.perms <- PermRequest{ID: f.RequestID, Tool: f.Tool, Detail: f.Detail}:
			default:
			}
		default:
			// ignore unexpected frames
		}
	}
}

// Send injects a message into the connected Claude session. Returns an error if
// no shim is currently connected.
func (e *Endpoint) Send(_ context.Context, m relay.Message) error {
	e.mu.Lock()
	c := e.conn
	e.mu.Unlock()
	if c == nil {
		return ErrNoSession
	}
	chatID := m.Meta["chat_id"]
	if chatID == "" {
		chatID = m.ConversationID
	}
	return c.Send(ipc.Frame{
		Kind:   ipc.KindInject,
		ChatID: chatID,
		Text:   m.Text,
		Meta:   m.Meta,
	})
}

// Close stops listening and removes the socket.
func (e *Endpoint) Close() error {
	var err error
	e.closeOnce.Do(func() {
		err = e.ln.Close()
		_ = os.Remove(e.socketPath)
	})
	return err
}

// ErrNoSession is returned by Send when no Claude session is connected.
var ErrNoSession = errNoSession{}

type errNoSession struct{}

func (errNoSession) Error() string { return "claude backend: no session connected" }
