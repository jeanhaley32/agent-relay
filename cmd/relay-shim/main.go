// Command relay-shim is the thin bridge Claude Code spawns over stdio. It
// connects to the relay daemon over a unix socket and translates in both
// directions:
//
//	daemon "inject" frame   → channel.Inject(...)   → Claude sees a <channel> event
//	daemon "perm_verdict"   → channel.SendVerdict   → answers a tool prompt
//	Claude reply tool call  → "reply" frame         → daemon
//	Claude perm prompt      → "perm_request" frame  → daemon (→ admin's Telegram)
//
// The daemon connection auto-reconnects, so the Claude session survives a
// `relayd` restart: the shim re-dials the socket and resumes without losing
// Claude's context.
//
// Claude Code launches it via .mcp.json; point it at the daemon socket with
// --socket or the RELAY_SOCKET env var. All logging goes to stderr (stdout is
// reserved for JSON-RPC).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/channel"
	"github.com/jeanhaley32/agent-relay/internal/ipc"
	"github.com/jeanhaley32/agent-relay/internal/mcp"
)

func main() {
	socket := flag.String("socket", os.Getenv("RELAY_SOCKET"), "daemon unix socket path")
	flag.Parse()

	logger := log.New(os.Stderr, "[relay-shim] ", log.LstdFlags)
	if *socket == "" {
		logger.Fatal("no --socket (or RELAY_SOCKET) provided")
	}

	cl := &client{socket: *socket, logger: logger, out: make(chan ipc.Frame, outboundBuffer)}

	// Claude → daemon: reply tool + tool-approval prompts become frames.
	srv := channel.New("relay", "0.0.1",
		`Events arrive as <channel source="relay" chat_id="...">. When you have an `+
			`answer, call the reply tool with the chat_id from the tag. `+
			`You can also schedule things for later: when the user asks to be reminded `+
			`or to run something on a schedule, call schedule_message (cron for recurring, `+
			`in_seconds for a one-shot). You may also schedule a self-wakeup to resume a `+
			`long-running task — the scheduled text is injected back into this session when `+
			`it fires. Use list_schedules and cancel_schedule to manage them. A fired event `+
			`arrives prefixed with "[scheduled trigger you set earlier fired]". When you have `+
			`handled a fired trigger, call ack_event with its id and a short note of what you did, `+
			`or it will keep escalating; list_pending_events shows what is still open.`,
		replyHandler(cl))
	srv.EnablePermissionRelay(func(req channel.PermissionRequest) {
		detail := req.Description
		if req.InputPreview != "" {
			detail += " — " + req.InputPreview
		}
		if err := cl.send(ipc.Frame{
			Kind: ipc.KindPermRequest, RequestID: req.RequestID, Tool: req.ToolName, Detail: detail,
		}); err != nil {
			logger.Printf("forward permission request: %v", err)
		}
	})

	// Scheduling tools: the model creates/lists/cancels reminders that relayd
	// fires later (by poking this session to deliver them). Each round-trips to
	// the daemon and returns its result to the model.
	registerScheduleTools(srv, cl)
	registerEventTools(srv, cl)

	// daemon → Claude: inject messages, apply verdicts.
	cl.onFrame = func(f ipc.Frame) {
		switch f.Kind {
		case ipc.KindInject:
			meta := f.Meta
			if meta == nil {
				meta = map[string]string{}
			}
			if _, ok := meta["chat_id"]; !ok {
				meta["chat_id"] = f.ChatID
			}
			if err := srv.Inject(f.Text+replyReminder(meta["chat_id"]), meta); err != nil {
				logger.Printf("inject error: %v", err)
			}
		case ipc.KindPermVerdict:
			if err := srv.SendVerdict(f.RequestID, f.Allow); err != nil {
				logger.Printf("send verdict error: %v", err)
			}
		case ipc.KindSchedResp, ipc.KindReplyAck:
			cl.resolve(f)
		}
	}

	go cl.run()       // reconnecting daemon link (inbound)
	go cl.writeLoop() // outbound: buffers + retries frames across reconnects

	logger.Printf("channel shim ready on stdio")
	if err := srv.Serve(context.Background(), os.Stdin, os.Stdout); err != nil {
		logger.Printf("serve ended: %v", err)
	}
}

// outboundBuffer bounds how many frames the shim holds while disconnected from
// the daemon. Sized for restart windows; if it fills, the oldest are dropped.
const outboundBuffer = 256

// replyReminder is appended to every injected event body. The "reply via the
// reply tool" contract is stated once in the channel's MCP init instructions,
// but over a long or resumed session the model drifts off it and answers in
// plain text — which never leaves the terminal, so the user sees silence.
// Re-asserting the contract on each message (terse, clearly namespaced) makes
// that drift impossible. chatID is echoed because the reply tool needs it.
func replyReminder(chatID string) string {
	return "\n\n(relay: reply by calling the reply tool with chat_id=\"" + chatID +
		"\". A plain-text answer stays in the terminal and is NOT delivered.)"
}

// schedTimeout bounds how long a schedule tool waits for the daemon to answer.
const schedTimeout = 10 * time.Second

// replyAckTimeout bounds how long the reply tool call waits for the daemon
// to confirm delivery. Must stay strictly greater than relay.FrontendSendTimeout
// so the broker's own send deadline expires first, narrowing (though not
// eliminating - see relay.FrontendSendTimeout's doc) the window in which a
// send lands after this call has already given up.
const replyAckTimeout = 15 * time.Second

// replyHandler builds the reply-tool callback: a request/response round-trip
// (not fire-and-forget) so the daemon's reply_ack frame carrying the real
// delivery outcome (e.g. Telegram's 4096-char limit) surfaces a failed send
// as a genuine tool error the model can react to - split the message, retry,
// etc. - instead of always looking like "sent".
func replyHandler(cl *client) channel.ReplyFunc {
	return func(_ context.Context, chatID, text string) error {
		resp, err := cl.request(ipc.Frame{Kind: ipc.KindReply, ChatID: chatID, Text: text}, replyAckTimeout)
		if err != nil {
			return err
		}
		if resp.Err != "" {
			return errors.New(resp.Err)
		}
		return nil
	}
}

// registerScheduleTools adds the schedule_message/list_schedules/cancel_schedule
// tools. Each turns a call into a sched_req frame, waits for the daemon's
// sched_resp, and returns its result (or error) to the model.
func registerScheduleTools(srv *channel.Server, cl *client) {
	do := func(f ipc.Frame) (string, error) {
		f.Kind = ipc.KindSchedReq
		resp, err := cl.request(f, schedTimeout)
		if err != nil {
			return "", err
		}
		if resp.Err != "" {
			return "", errors.New(resp.Err)
		}
		return resp.Result, nil
	}

	srv.RegisterTool(mcp.Tool{
		Name: "schedule_message",
		Description: "Schedule a reminder to be delivered to the user later. Provide EXACTLY one " +
			"of `cron` (a standard 5-field cron spec, e.g. \"0 9 * * *\" for 9am daily, in the " +
			"host's local timezone) or `in_seconds` (a one-shot delay, e.g. 1200 for 20 minutes). " +
			"`text` is the reminder to deliver. Returns the schedule id and next fire time.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"text":       map[string]any{"type": "string", "description": "The reminder text to deliver to the user"},
				"cron":       map[string]any{"type": "string", "description": "Recurring: standard 5-field cron spec (local time). Omit for one-shot."},
				"in_seconds": map[string]any{"type": "integer", "description": "One-shot: seconds from now to fire. Omit for recurring."},
				"chat_id":    map[string]any{"type": "string", "description": "The conversation to remind (from the channel tag)"},
			},
			"required": []string{"text"},
		},
		Handler: func(_ context.Context, args json.RawMessage) (string, error) {
			var a struct {
				Text      string `json:"text"`
				Cron      string `json:"cron"`
				InSeconds int64  `json:"in_seconds"`
				ChatID    string `json:"chat_id"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return "", err
			}
			return do(ipc.Frame{
				Op: ipc.OpScheduleCreate, Text: a.Text, Cron: a.Cron,
				InSeconds: a.InSeconds, ChatID: a.ChatID,
			})
		},
	})

	srv.RegisterTool(mcp.Tool{
		Name:        "list_schedules",
		Description: "List the user's active scheduled reminders (id, schedule, next fire, text).",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		Handler: func(_ context.Context, _ json.RawMessage) (string, error) {
			return do(ipc.Frame{Op: ipc.OpScheduleList})
		},
	})

	srv.RegisterTool(mcp.Tool{
		Name:        "cancel_schedule",
		Description: "Cancel a scheduled reminder by its id (from list_schedules or schedule_message).",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{"id": map[string]any{"type": "string", "description": "The schedule id to cancel"}},
			"required":   []string{"id"},
		},
		Handler: func(_ context.Context, args json.RawMessage) (string, error) {
			var a struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return "", err
			}
			return do(ipc.Frame{Op: ipc.OpScheduleCancel, SchedID: a.ID})
		},
	})
}

// registerEventTools adds ack_event/list_pending_events. A fired scheduled
// trigger becomes a "pending event" the daemon follows until the agent
// acknowledges it; ignoring one escalates (re-inject, then a direct message to
// the admin). These tools let the model close the loop and inspect open events.
func registerEventTools(srv *channel.Server, cl *client) {
	do := func(f ipc.Frame) (string, error) {
		f.Kind = ipc.KindSchedReq
		resp, err := cl.request(f, schedTimeout)
		if err != nil {
			return "", err
		}
		if resp.Err != "" {
			return "", errors.New(resp.Err)
		}
		return resp.Result, nil
	}

	srv.RegisterTool(mcp.Tool{
		Name: "ack_event",
		Description: "Acknowledge a fired scheduled trigger once you have handled it, so it stops " +
			"escalating. A non-empty `note` describing what you actually did is REQUIRED (a bare ack " +
			"is rejected) — it becomes a visible audit trail. `id` is the event id from the fired " +
			"trigger message or list_pending_events.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id":   map[string]any{"type": "string", "description": "The pending event id to acknowledge"},
				"note": map[string]any{"type": "string", "description": "Short description of what you did to handle it (required)"},
			},
			"required": []string{"id", "note"},
		},
		Handler: func(_ context.Context, args json.RawMessage) (string, error) {
			var a struct {
				ID   string `json:"id"`
				Note string `json:"note"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return "", err
			}
			return do(ipc.Frame{Op: ipc.OpEventAck, SchedID: a.ID, Text: a.Note})
		},
	})

	srv.RegisterTool(mcp.Tool{
		Name:        "list_pending_events",
		Description: "List currently-open (unacknowledged) fired triggers, with their fired-at and last-nudge times, so you can see what still needs handling.",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		Handler: func(_ context.Context, _ json.RawMessage) (string, error) {
			return do(ipc.Frame{Op: ipc.OpEventList})
		},
	})
}

// client is a reconnecting IPC link to the daemon. Outbound frames are queued
// and retried across reconnects so a relayd restart is lossless; onFrame is
// invoked for each inbound frame. send is safe for concurrent use.
type client struct {
	socket  string
	logger  *log.Logger
	onFrame func(ipc.Frame)
	out     chan ipc.Frame

	mu   sync.Mutex
	conn *ipc.Conn

	seq     atomic.Uint64
	waitMu  sync.Mutex
	waiters map[string]chan ipc.Frame // correlation id -> response channel
}

// request sends a frame and blocks until the daemon answers with a frame
// carrying the same RequestID, or the timeout elapses. Used by the schedule
// tools, which need a result to return to Claude.
func (c *client) request(f ipc.Frame, timeout time.Duration) (ipc.Frame, error) {
	id := strconv.FormatUint(c.seq.Add(1), 10)
	f.RequestID = id
	ch := make(chan ipc.Frame, 1)
	c.waitMu.Lock()
	if c.waiters == nil {
		c.waiters = map[string]chan ipc.Frame{}
	}
	c.waiters[id] = ch
	c.waitMu.Unlock()
	defer func() {
		c.waitMu.Lock()
		delete(c.waiters, id)
		c.waitMu.Unlock()
	}()

	if err := c.send(f); err != nil {
		return ipc.Frame{}, err
	}
	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(timeout):
		return ipc.Frame{}, errors.New("timed out waiting for the daemon")
	}
}

// resolve delivers a response frame to a pending request waiter, if any.
func (c *client) resolve(f ipc.Frame) {
	c.waitMu.Lock()
	ch := c.waiters[f.RequestID]
	c.waitMu.Unlock()
	if ch != nil {
		select {
		case ch <- f:
		default:
		}
	}
}

// run dials the daemon and pumps inbound frames, reconnecting on any failure.
func (c *client) run() {
	for {
		nc, err := net.Dial("unix", c.socket)
		if err != nil {
			c.logger.Printf("daemon not reachable (%v) — retrying", err)
			time.Sleep(time.Second)
			continue
		}
		conn := ipc.NewConn(nc)
		c.set(conn)
		c.logger.Printf("connected to daemon at %s", c.socket)
		for {
			f, err := conn.Recv()
			if err != nil {
				break
			}
			if c.onFrame != nil {
				c.onFrame(f)
			}
		}
		c.set(nil)
		c.logger.Printf("daemon connection lost — reconnecting")
		time.Sleep(time.Second)
	}
}

func (c *client) set(conn *ipc.Conn) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
}

func (c *client) get() *ipc.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// send enqueues a frame for delivery. It never blocks the caller (Claude's tool
// handler): frames are buffered and flushed by writeLoop once connected. If the
// buffer is full it drops the frame rather than stall.
func (c *client) send(f ipc.Frame) error {
	select {
	case c.out <- f:
		return nil
	default:
		c.logger.Printf("outbound buffer full (%d) — dropping %s frame", cap(c.out), f.Kind)
		return errors.New("outbound buffer full")
	}
}

// writeLoop drains queued frames, retrying each on the current connection until
// it succeeds. This preserves order and makes a daemon restart lossless: frames
// wait in the queue while disconnected and flush on reconnect.
func (c *client) writeLoop() {
	for f := range c.out {
		for {
			if conn := c.get(); conn != nil && conn.Send(f) == nil {
				break // delivered
			}
			time.Sleep(200 * time.Millisecond) // wait for (re)connection
		}
	}
}
