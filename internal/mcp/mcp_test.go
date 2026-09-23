package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// A slow tools/call used to block the JSON-RPC read loop for its whole
// duration, so ping went unanswered and permission notifications were not
// forwarded until the slow tool returned.
func TestSlowToolDoesNotBlockTheReadLoop(t *testing.T) {
	s := New("test", "1.0", "")
	release := make(chan struct{})
	s.RegisterTool(Tool{
		Name:        "slow",
		Description: "blocks until released",
		Handler: func(ctx context.Context, args json.RawMessage) (string, error) {
			<-release
			return "done", nil
		},
	})

	in, inWriter := io.Pipe()
	var out bytes.Buffer
	outWriter := &syncWriter{w: &out}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Serve(ctx, in, outWriter) }()

	// A slow call, then a ping that must be answered while it is still running.
	_, _ = inWriter.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"slow","arguments":{}}}` + "\n"))
	time.Sleep(50 * time.Millisecond)
	_, _ = inWriter.Write([]byte(`{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(outWriter.String(), `"id":2`) {
			close(release)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(release)
	t.Fatal("ping was not answered while a slow tool was running: the read loop is blocked")
}

// syncWriter guards the buffer, since responses now arrive from more than one
// goroutine.
type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (s *syncWriter) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.String()
}
