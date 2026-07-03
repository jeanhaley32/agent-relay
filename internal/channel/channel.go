// Package channel layers Claude Code "channel" semantics on top of the generic
// internal/mcp server. A channel is an MCP server that Claude Code spawns over
// stdio; it declares the claude/channel capability, pushes inbound events as
// notifications/claude/channel, and (for two-way channels) exposes a reply tool
// Claude calls to send messages back out.
//
// This package is the single place that knows the Claude Code channel contract.
// Everything above it (a Telegram endpoint, the relay broker) deals in ordinary
// messages, not MCP wire details.
package channel

import (
	"context"
	"encoding/json"
	"io"

	"github.com/jeanhaley32/agent-relay/internal/mcp"
)

// Channel-specific protocol constants (Claude Code extensions to MCP).
const (
	capabilityKey  = "claude/channel"
	notifyMethod   = "notifications/claude/channel"
	replyToolName  = "reply"
)

// ReplyFunc is invoked when Claude calls the reply tool. chatID is the routing
// key echoed from the inbound event's meta; text is Claude's message.
type ReplyFunc func(ctx context.Context, chatID, text string) error

// Server is a two-way Claude Code channel built on an mcp.Server.
type Server struct {
	mcp *mcp.Server
}

// New builds a two-way channel server.
//
//	name         - channel source name (appears as <channel source="name">)
//	instructions - guidance injected into Claude's system prompt
//	onReply      - handler for outbound replies (nil makes it effectively one-way)
func New(name, version, instructions string, onReply ReplyFunc) *Server {
	m := mcp.New(name, version, instructions)
	m.AddExperimentalCapability(capabilityKey)

	if onReply != nil {
		m.RegisterTool(mcp.Tool{
			Name:        replyToolName,
			Description: "Send a message back over this channel",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"chat_id": map[string]any{"type": "string", "description": "The conversation to reply in"},
					"text":    map[string]any{"type": "string", "description": "The message to send"},
				},
				"required": []string{"chat_id", "text"},
			},
			Handler: func(ctx context.Context, args json.RawMessage) (string, error) {
				var a struct {
					ChatID string `json:"chat_id"`
					Text   string `json:"text"`
				}
				if err := json.Unmarshal(args, &a); err != nil {
					return "", err
				}
				if err := onReply(ctx, a.ChatID, a.Text); err != nil {
					return "", err
				}
				return "sent", nil
			},
		})
	}

	return &Server{mcp: m}
}

// Inject pushes an inbound event into the Claude session. content is the message
// body; meta entries become <channel> tag attributes (identifier-char keys only,
// e.g. chat_id, sender). Safe for concurrent use with Serve.
func (s *Server) Inject(content string, meta map[string]string) error {
	return s.mcp.Notify(notifyMethod, map[string]any{
		"content": content,
		"meta":    meta,
	})
}

// Serve runs the stdio protocol loop until in reaches EOF or ctx is cancelled.
// Claude Code supplies stdin/stdout; nothing but JSON-RPC may go to out.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	return s.mcp.Serve(ctx, in, out)
}
