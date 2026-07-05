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
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/channel"
	"github.com/jeanhaley32/agent-relay/internal/ipc"
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
			`answer, call the reply tool with the chat_id from the tag.`,
		func(_ context.Context, chatID, text string) error {
			return cl.send(ipc.Frame{Kind: ipc.KindReply, ChatID: chatID, Text: text})
		})
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
