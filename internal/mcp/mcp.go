// Package mcp is a minimal, dependency-free implementation of the Model Context
// Protocol (MCP) over a newline-delimited JSON-RPC 2.0 stdio transport.
//
// It is deliberately generic: it knows about the MCP handshake, tool listing,
// tool calls, and server-initiated notifications, but nothing about any
// particular application (Claude Code channels, etc.). Higher layers compose
// on top of it — see package channel for the Claude Code channel semantics.
//
// Concurrency: Serve reads requests on one goroutine; Notify may be called from
// any goroutine. All writes to the output stream are serialized by a mutex so
// responses and notifications never interleave on the wire.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// defaultProtocolVersion is used if the client does not send one at initialize.
const defaultProtocolVersion = "2025-06-18"

// maxLine bounds a single JSON-RPC message read from stdin (10 MiB, matching
// Claude Code's headless stdin cap).
const maxLine = 10 << 20

// ToolHandler implements a single MCP tool. args is the raw JSON of the call's
// "arguments" object. The returned string is delivered back to the caller as
// the tool result text.
type ToolHandler func(ctx context.Context, args json.RawMessage) (string, error)

// Tool is a registered MCP tool: its advertised schema plus its handler.
type Tool struct {
	Name        string
	Description string
	InputSchema map[string]any
	Handler     ToolHandler
}

// NotificationHandler handles an inbound JSON-RPC notification (no response).
type NotificationHandler func(ctx context.Context, params json.RawMessage)

// Server is a generic MCP stdio server. Construct with New, configure with the
// Add* methods before calling Serve, then Notify at any time while Serve runs.
type Server struct {
	name         string
	version      string
	instructions string

	experimental map[string]any // capabilities.experimental
	tools        map[string]Tool
	notifs       map[string]NotificationHandler // inbound notification handlers

	mu  sync.Mutex // guards enc
	enc *json.Encoder
}

// New creates a server. instructions is injected into the model's system prompt
// (may be empty).
func New(name, version, instructions string) *Server {
	return &Server{
		name:         name,
		version:      version,
		instructions: instructions,
		experimental: map[string]any{},
		tools:        map[string]Tool{},
		notifs:       map[string]NotificationHandler{},
	}
}

// AddExperimentalCapability declares an experimental capability key (value {}).
// e.g. "claude/channel". Must be called before Serve.
func (s *Server) AddExperimentalCapability(key string) {
	s.experimental[key] = map[string]any{}
}

// RegisterTool adds a callable tool. Must be called before Serve.
func (s *Server) RegisterTool(t Tool) { s.tools[t.Name] = t }

// OnNotification registers a handler for an inbound notification method.
func (s *Server) OnNotification(method string, h NotificationHandler) {
	s.notifs[method] = h
}

// --- wire types -------------------------------------------------------------

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// inbound is a superset of request+notification we parse off the wire.
type inbound struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"` // present => request
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type notification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// write serializes one value to the output stream under the mutex.
func (s *Server) write(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(v) // json.Encoder.Encode appends a newline => framing
}

// Notify sends a server-initiated JSON-RPC notification. Safe for concurrent
// use with Serve. Returns once the message is written to the transport (not
// acknowledged — MCP notifications are fire-and-forget).
func (s *Server) Notify(method string, params any) error {
	return s.write(notification{JSONRPC: "2.0", Method: method, Params: params})
}

// Serve runs the read loop until in reaches EOF or ctx is cancelled. It is the
// only reader of in; out is written by both Serve and Notify (serialized).
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	s.mu.Lock()
	s.enc = json.NewEncoder(out)
	s.mu.Unlock()

	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)

	for sc.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg inbound
		if err := json.Unmarshal(line, &msg); err != nil {
			continue // ignore malformed frames rather than kill the session
		}
		if len(msg.ID) == 0 {
			s.handleNotification(ctx, msg)
			continue
		}
		s.handleRequest(ctx, msg)
	}
	return sc.Err()
}

func (s *Server) handleNotification(ctx context.Context, msg inbound) {
	if h, ok := s.notifs[msg.Method]; ok {
		h(ctx, msg.Params)
	}
	// "notifications/initialized" and unknown notifications are no-ops.
}

func (s *Server) handleRequest(ctx context.Context, msg inbound) {
	switch msg.Method {
	case "initialize":
		s.write(response{JSONRPC: "2.0", ID: msg.ID, Result: s.initializeResult(msg.Params)})
	case "tools/list":
		s.write(response{JSONRPC: "2.0", ID: msg.ID, Result: s.toolsListResult()})
	case "tools/call":
		s.write(s.callTool(ctx, msg))
	case "ping":
		s.write(response{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{}})
	default:
		s.write(response{JSONRPC: "2.0", ID: msg.ID,
			Error: &rpcError{Code: -32601, Message: "method not found: " + msg.Method}})
	}
}

func (s *Server) initializeResult(params json.RawMessage) map[string]any {
	proto := defaultProtocolVersion
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if json.Unmarshal(params, &p) == nil && p.ProtocolVersion != "" {
		proto = p.ProtocolVersion // echo the client's version for compatibility
	}
	caps := map[string]any{}
	if len(s.experimental) > 0 {
		caps["experimental"] = s.experimental
	}
	if len(s.tools) > 0 {
		caps["tools"] = map[string]any{}
	}
	res := map[string]any{
		"protocolVersion": proto,
		"capabilities":    caps,
		"serverInfo":      map[string]any{"name": s.name, "version": s.version},
	}
	if s.instructions != "" {
		res["instructions"] = s.instructions
	}
	return res
}

func (s *Server) toolsListResult() map[string]any {
	list := make([]map[string]any, 0, len(s.tools))
	for _, t := range s.tools {
		list = append(list, map[string]any{
			"name":        t.Name,
			"description": t.Description,
			"inputSchema": t.InputSchema,
		})
	}
	return map[string]any{"tools": list}
}

func (s *Server) callTool(ctx context.Context, msg inbound) response {
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(msg.Params, &call); err != nil {
		return response{JSONRPC: "2.0", ID: msg.ID,
			Error: &rpcError{Code: -32602, Message: "invalid params"}}
	}
	t, ok := s.tools[call.Name]
	if !ok {
		return response{JSONRPC: "2.0", ID: msg.ID,
			Error: &rpcError{Code: -32601, Message: "unknown tool: " + call.Name}}
	}
	text, err := t.Handler(ctx, call.Arguments)
	if err != nil {
		// Tool errors are reported in-band so the model can react (MCP convention).
		return response{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{
			"content": []map[string]any{{"type": "text", "text": "error: " + err.Error()}},
			"isError": true,
		}}
	}
	return response{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
	}}
}

// TextResult is a helper for tool handlers that just need to confirm success.
func TextResult(format string, a ...any) (string, error) {
	return fmt.Sprintf(format, a...), nil
}
