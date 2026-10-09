package channel

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// TestChannelHandshake drives the channel server exactly as Claude Code would:
// initialize -> tools/list -> (inject event) -> tools/call reply. It proves our
// hand-rolled Go server speaks the Claude Code channel dialect end-to-end,
// without needing a live Claude session.
//
// os.Pipe (kernel-buffered) is used rather than io.Pipe so writes don't block in
// lockstep with reads — matching how Claude Code's stdio transport behaves.
func TestChannelHandshake(t *testing.T) {
	inR, inW, _ := os.Pipe()   // test -> server (server's stdin)
	outR, outW, _ := os.Pipe() // server -> test (server's stdout)

	replies := make(chan [2]string, 1)
	s := New("telegram", "0.0.1",
		"Messages arrive as <channel source=\"telegram\" chat_id=\"...\">. Reply with the reply tool.",
		func(_ context.Context, chatID, text string) error {
			replies <- [2]string{chatID, text}
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Serve(ctx, inR, outW) }()

	enc := json.NewEncoder(inW)
	sc := bufio.NewScanner(outR)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	read := func() map[string]any {
		t.Helper()
		done := make(chan map[string]any, 1)
		go func() {
			if sc.Scan() {
				var m map[string]any
				_ = json.Unmarshal(sc.Bytes(), &m)
				done <- m
			} else {
				done <- nil
			}
		}()
		select {
		case m := <-done:
			if m == nil {
				t.Fatal("no message from server (stream closed)")
			}
			return m
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for server message")
			return nil
		}
	}

	// 1. initialize -> must advertise the claude/channel capability.
	_ = enc.Encode(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "initialize",
		"params": map[string]any{"protocolVersion": "2025-06-18"},
	})
	init := read()
	res, _ := init["result"].(map[string]any)
	caps, _ := res["capabilities"].(map[string]any)
	exp, _ := caps["experimental"].(map[string]any)
	if _, ok := exp["claude/channel"]; !ok {
		t.Fatalf("initialize did not declare claude/channel capability: %v", caps)
	}
	if info, _ := res["serverInfo"].(map[string]any); info["name"] != "telegram" {
		t.Fatalf("wrong serverInfo: %v", res["serverInfo"])
	}

	// 2. initialized notification (no response expected).
	_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})

	// 3. tools/list -> must expose the reply tool.
	_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
	tl := read()
	tres, _ := tl["result"].(map[string]any)
	tools, _ := tres["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("expected 1 tool, got %v", tools)
	}
	if tools[0].(map[string]any)["name"] != "reply" {
		t.Fatalf("expected reply tool, got %v", tools[0])
	}

	// 4. inject an inbound event -> server emits notifications/claude/channel.
	if err := s.Inject("what's in my working dir?", map[string]string{"chat_id": "42"}); err != nil {
		t.Fatalf("inject: %v", err)
	}
	ev := read()
	if ev["method"] != "notifications/claude/channel" {
		t.Fatalf("expected channel notification, got %v", ev["method"])
	}
	params, _ := ev["params"].(map[string]any)
	if params["content"] != "what's in my working dir?" {
		t.Fatalf("wrong content: %v", params["content"])
	}
	if meta, _ := params["meta"].(map[string]any); meta["chat_id"] != "42" {
		t.Fatalf("wrong meta: %v", params["meta"])
	}

	// 5. tools/call reply -> handler fires, returns "sent".
	_ = enc.Encode(map[string]any{
		"jsonrpc": "2.0", "id": 3, "method": "tools/call",
		"params": map[string]any{
			"name":      "reply",
			"arguments": map[string]any{"chat_id": "42", "text": "3 files"},
		},
	})
	call := read()
	cres, _ := call["result"].(map[string]any)
	content, _ := cres["content"].([]any)
	if len(content) == 0 || content[0].(map[string]any)["text"] != "sent" {
		t.Fatalf("expected 'sent' result, got %v", cres)
	}
	select {
	case got := <-replies:
		if got[0] != "42" || got[1] != "3 files" {
			t.Fatalf("reply handler got wrong args: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("reply handler was not invoked")
	}
}
