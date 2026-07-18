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
	"time"

	"github.com/jeanhaley32/agent-relay/internal/eventlog"
	"github.com/jeanhaley32/agent-relay/internal/ipc"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// inboundBuffer bounds how many inject frames are held while the shim is
// disconnected (e.g. across a relayd/Claude reconnect), so a message sent in
// that window is delivered on reconnect instead of silently dropped. Oldest
// are evicted past this.
const inboundBuffer = 256

// PermRequest is a tool-approval request surfaced from the Claude session.
type PermRequest struct {
	ID     string // request id to echo in the verdict
	Tool   string // tool name (e.g. "Bash")
	Detail string // human-readable description
}

// SchedRequest is a schedule- or event-tool call surfaced from the Claude
// session. The daemon performs the op and answers with SchedRespond, echoing
// ReqID. Event ops (OpEventAck/OpEventList) reuse the schedule fields:
// Text carries the ack note and SchedID carries the event id.
type SchedRequest struct {
	ReqID     string // correlation id to echo in the response
	Op        string // ipc.OpSchedule*/OpEvent* (create | list | cancel | ack)
	Text      string // reminder text (schedule create); ack note (event ack)
	Cron      string // recurring cron spec (create)
	InSeconds int64  // one-shot delay seconds (create)
	SchedID   string // schedule id (cancel); event id (event ack)
	ChatID    string // requesting conversation (create target / audit)
}

// Endpoint is the Claude Code backend.
type Endpoint struct {
	socketPath string
	ln         net.Listener
	recv       chan relay.Message
	perms      chan PermRequest
	schedreq   chan SchedRequest
	out        chan ipc.Frame // inject frames queued for the shim (buffered)
	done       chan struct{}

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
	// Restrict the socket to the owner so other local users can't connect and
	// hijack the channel (inject replies / approve tool use). connect() needs
	// write permission, so 0600 blocks everyone but us.
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	e := &Endpoint{
		socketPath: socketPath,
		ln:         ln,
		recv:       make(chan relay.Message, 32),
		perms:      make(chan PermRequest, 32),
		schedreq:   make(chan SchedRequest, 32),
		out:        make(chan ipc.Frame, inboundBuffer),
		done:       make(chan struct{}),
	}
	go e.acceptLoop()
	go e.writeLoop() // deliver queued inject frames across reconnects
	return e, nil
}

func (e *Endpoint) Name() string               { return "claude" }
func (e *Endpoint) Recv() <-chan relay.Message { return e.recv }

// Connected reports whether a shim session is currently connected. The
// pending-event tracker uses this to tell a real delivery (injected into a live
// session) from a frame merely buffered while the shim is down — a meaningfully
// better signal than Send's unconditional nil return.
func (e *Endpoint) Connected() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.conn != nil
}

// Permissions delivers tool-approval requests from the Claude session. Consume
// these and call Decide to answer. If no one consumes them, tool calls that need
// approval will simply wait (Claude's local dialog stays open).
func (e *Endpoint) Permissions() <-chan PermRequest { return e.perms }

// Schedules delivers schedule-tool calls from the Claude session. Consume these
// and call SchedRespond to answer each (the tool call blocks until then).
func (e *Endpoint) Schedules() <-chan SchedRequest { return e.schedreq }

// SchedRespond answers a pending schedule request by its correlation id. result
// is a human-readable summary; errText is empty on success.
func (e *Endpoint) SchedRespond(reqID, result, errText string) error {
	e.mu.Lock()
	c := e.conn
	e.mu.Unlock()
	if c == nil {
		return ErrNoSession
	}
	return c.Send(ipc.Frame{Kind: ipc.KindSchedResp, RequestID: reqID, Result: result, Err: errText})
}

// ReplyRespond answers a pending reply frame by its correlation id, so the
// reply tool call can surface a real delivery failure (e.g. Telegram's 4096-
// char limit) back to the model instead of the fire-and-forget behavior that
// let failed sends look successful.
func (e *Endpoint) ReplyRespond(reqID, errText string) error {
	if reqID == "" {
		return nil // caller (an older shim, or a reply with no correlation id) isn't waiting
	}
	e.mu.Lock()
	c := e.conn
	e.mu.Unlock()
	if c == nil {
		return ErrNoSession
	}
	return c.Send(ipc.Frame{Kind: ipc.KindReplyAck, RequestID: reqID, Err: errText})
}

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
	defer close(e.schedreq)
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
			// Close the socket, don't just drop our reference to it. Without
			// this the connection stays half-open: we stop reading but the shim
			// never sees EOF, so its reconnect loop never fires and it keeps
			// writing replies into a socket nobody reads - outbound dies
			// silently while inbound looks fine (real incident 2026-07-18).
			_ = c.Close()
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
				// reply_id carries the correlation id back to ReplyRespond,
				// so the broker can report a real delivery failure to the
				// waiting tool call instead of it always returning "sent".
				// Outbound replies get their own msg_id too: "uniquely identify
				// EVERY message" means model-generated ones as well, otherwise a
				// reply can be seen in the audit trail but never referred to.
				Meta: map[string]string{"chat_id": f.ChatID, "reply_id": f.RequestID, "msg_id": eventlog.NewMsgID()},
			}
			select {
			case e.recv <- msg:
			default:
				// The consumer is gone or not draining, so this reply will
				// never be delivered. Do NOT drop it silently: the model is
				// blocked on this reply_id and, with no answer, its tool call
				// only times out with a generic "daemon didn't respond" - so it
				// assumes success and tells the user "sent" when nothing was.
				// Answer with the real reason instead, so it can retry or say
				// so honestly (mirrors the scheduler's busy-path below).
				_ = e.ReplyRespond(f.RequestID,
					"reply was NOT delivered: relay outbound queue full / consumer not draining - retry, and do not report this message as sent")
			}
		case ipc.KindPermRequest:
			select {
			case e.perms <- PermRequest{ID: f.RequestID, Tool: f.Tool, Detail: f.Detail}:
			default:
			}
		case ipc.KindSchedReq:
			req := SchedRequest{
				ReqID: f.RequestID, Op: f.Op, Text: f.Text, Cron: f.Cron,
				InSeconds: f.InSeconds, SchedID: f.SchedID, ChatID: f.ChatID,
			}
			select {
			case e.schedreq <- req:
			default: // consumer gone: answer with a busy error so the tool doesn't hang
				_ = e.SchedRespond(f.RequestID, "", "scheduler busy")
			}
		default:
			// ignore unexpected frames
		}
	}
}

// Send queues a message for the Claude session. It never blocks the broker and
// never drops on a momentary disconnect: the inject frame is buffered and
// delivered by writeLoop once a shim is connected. If the buffer is full the
// oldest frame is evicted.
func (e *Endpoint) Send(_ context.Context, m relay.Message) error {
	chatID := m.Meta["chat_id"]
	if chatID == "" {
		chatID = m.ConversationID
	}
	f := ipc.Frame{Kind: ipc.KindInject, ChatID: chatID, Text: m.Text, Meta: m.Meta}
	select {
	case e.out <- f:
	default: // buffer full: drop the oldest to make room, then enqueue
		select {
		case <-e.out:
		default:
		}
		select {
		case e.out <- f:
		default:
		}
	}
	return nil
}

// writeLoop delivers queued inject frames, retrying on the current connection
// until each succeeds. This makes a shim reconnect lossless: messages sent while
// disconnected wait in the queue and flush on reconnect (in order).
func (e *Endpoint) writeLoop() {
	for {
		var f ipc.Frame
		select {
		case f = <-e.out:
		case <-e.done:
			return
		}
		for {
			e.mu.Lock()
			c := e.conn
			e.mu.Unlock()
			if c != nil && c.Send(f) == nil {
				break // delivered
			}
			select {
			case <-e.done:
				return
			case <-time.After(200 * time.Millisecond): // wait for (re)connection
			}
		}
	}
}

// Close stops listening and removes the socket.
func (e *Endpoint) Close() error {
	var err error
	e.closeOnce.Do(func() {
		close(e.done)
		err = e.ln.Close()
		// Closing the listener only unblocks Accept(). acceptLoop spends nearly
		// all its time inside readReplies, blocked on c.Recv() of the live
		// connection - which a listener close does not touch. Without also
		// closing that connection, readReplies never returns, so acceptLoop
		// never runs its `defer close(e.recv)`, the broker's outbound pump
		// blocks forever on `range Backend.Recv()`, and Broker.Run hangs in
		// wg.Wait() - shutdown never completes and systemd has to SIGKILL
		// ("Failed with result 'timeout'", observed 2026-07-18).
		e.mu.Lock()
		if e.conn != nil {
			_ = e.conn.Close()
			e.conn = nil
		}
		e.mu.Unlock()
		_ = os.Remove(e.socketPath)
	})
	return err
}

// ErrNoSession is returned by Decide when no Claude session is connected (a
// permission verdict is time-sensitive, so it is not buffered like Send).
var ErrNoSession = errNoSession{}

type errNoSession struct{}

func (errNoSession) Error() string { return "claude backend: no session connected" }
