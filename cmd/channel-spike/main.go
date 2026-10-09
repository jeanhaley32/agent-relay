// Command channel-spike is PoC-1: a minimal, real Claude Code channel server in
// Go. Claude Code spawns it over stdio (via .mcp.json); it also listens on a
// local HTTP port so you can inject messages with curl. Anything POSTed becomes
// a <channel> event in the Claude session; when Claude calls the reply tool, the
// reply is written to stderr and an SSE stream so you can watch both directions.
//
// Run it live against Claude Code (from the repo root):
//
//	go build -o bin/channel-spike ./cmd/channel-spike
//	claude --dangerously-load-development-channels server:channel-spike
//	# in another terminal:
//	curl -sN localhost:8799/events            # watch outbound replies
//	curl -s  localhost:8799 -d "what's in my working directory?"   # inject inbound
//
// IMPORTANT: stdout carries JSON-RPC only. All logging goes to stderr.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/jeanhaley32/agent-relay/internal/channel"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8799", "local HTTP address for injection + SSE")
	flag.Parse()

	logger := log.New(os.Stderr, "[channel-spike] ", log.LstdFlags)

	// SSE fan-out so `curl -N /events` can watch outbound replies live.
	var mu sync.Mutex
	listeners := map[chan string]struct{}{}
	broadcast := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		for ch := range listeners {
			select {
			case ch <- s:
			default:
			}
		}
	}

	srv := channel.New("channel-spike", "0.0.1",
		`Events arrive as <channel source="channel-spike" chat_id="...">. `+
			`When you have an answer, call the reply tool with the chat_id from the tag.`,
		func(_ context.Context, chatID, text string) error {
			logger.Printf("REPLY chat_id=%s: %s", chatID, text)
			broadcast(fmt.Sprintf("reply chat_id=%s: %s", chatID, text))
			return nil
		})

	// HTTP: POST / injects an inbound event; GET /events streams outbound.
	var nextID int
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/events" {
			streamEvents(w, r, &mu, listeners)
			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		nextID++
		id := strconv.Itoa(nextID)
		mu.Unlock()
		if err := srv.Inject(string(body), map[string]string{"chat_id": id}); err != nil {
			logger.Printf("inject error: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		logger.Printf("INJECT chat_id=%s: %s", id, string(body))
		fmt.Fprintln(w, "ok chat_id="+id)
	})

	go func() {
		logger.Printf("HTTP inject/SSE on http://%s  (POST / to inject, GET /events to watch)", *addr)
		if err := http.ListenAndServe(*addr, mux); err != nil {
			logger.Printf("http server stopped: %v", err)
		}
	}()

	// Run the MCP stdio loop (blocks until Claude Code closes stdin).
	logger.Printf("channel server ready on stdio")
	if err := srv.Serve(context.Background(), os.Stdin, os.Stdout); err != nil {
		logger.Printf("serve ended: %v", err)
	}
}

func streamEvents(w http.ResponseWriter, r *http.Request, mu *sync.Mutex, listeners map[chan string]struct{}) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	ch := make(chan string, 16)
	mu.Lock()
	listeners[ch] = struct{}{}
	mu.Unlock()
	defer func() { mu.Lock(); delete(listeners, ch); mu.Unlock() }()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()
	for {
		select {
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			fl.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
