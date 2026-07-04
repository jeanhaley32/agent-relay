// Command relay-shim is the thin bridge Claude Code spawns over stdio. It
// connects to the relay daemon over a unix socket and translates in both
// directions:
//
//	daemon "inject" frame  → channel.Inject(...)  → Claude sees a <channel> event
//	Claude reply tool call → "reply" frame        → daemon
//
// Claude Code launches it via .mcp.json; point it at the daemon socket with
// --socket or the RELAY_SOCKET env var. All logging goes to stderr (stdout is
// reserved for JSON-RPC).
//
//	.mcp.json:
//	{ "mcpServers": { "relay": {
//	    "command": "./bin/relay-shim",
//	    "args": ["--socket", "/run/user/1000/agent-relay.sock"] } } }
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"

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

	nc, err := net.Dial("unix", *socket)
	if err != nil {
		logger.Fatalf("dial daemon socket %s: %v", *socket, err)
	}
	conn := ipc.NewConn(nc)
	logger.Printf("connected to daemon at %s", *socket)

	// Claude → daemon: when Claude calls the reply tool, forward a reply frame.
	srv := channel.New("relay", "0.0.1",
		`Events arrive as <channel source="relay" chat_id="...">. When you have an `+
			`answer, call the reply tool with the chat_id from the tag.`,
		func(_ context.Context, chatID, text string) error {
			return conn.Send(ipc.Frame{Kind: ipc.KindReply, ChatID: chatID, Text: text})
		})

	// daemon → Claude: read inject frames and push them into the session.
	go func() {
		for {
			f, err := conn.Recv()
			if err != nil {
				logger.Printf("daemon connection closed: %v", err)
				return
			}
			if f.Kind != ipc.KindInject {
				continue
			}
			meta := f.Meta
			if meta == nil {
				meta = map[string]string{}
			}
			if _, ok := meta["chat_id"]; !ok {
				meta["chat_id"] = f.ChatID
			}
			if err := srv.Inject(f.Text, meta); err != nil {
				logger.Printf("inject error: %v", err)
			}
		}
	}()

	logger.Printf("channel shim ready on stdio")
	if err := srv.Serve(context.Background(), os.Stdin, os.Stdout); err != nil {
		logger.Printf("serve ended: %v", err)
	}
}
