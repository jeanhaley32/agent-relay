// Package web is a self-hosted browser frontend for the relay: a tiny HTTP
// server that lets a web page (e.g. the vessel-writer chat pane) talk to the
// model exactly like Telegram/Discord/Matrix do, over the tailnet.
//
// Transport is deliberately dependency-free: Server-Sent Events (a plain
// long-lived HTTP GET) carry the model's replies down to the browser, and a
// plain POST carries the user's typed messages up. No websocket library, no
// upgrade handshake — chat is low-frequency and SSE is trivially reverse-
// proxyable (nginx just needs proxy_buffering off on the stream path).
//
// Identity is a deterministic tailnet gate, not a claim. The page is reachable
// only over Tailscale (nginx binds the tailnet IP; ufw opens :80 only on
// tailscale0), and on every request this endpoint runs `tailscale whois` on the
// real client address (forwarded by nginx as X-Real-IP) and requires it to
// resolve to the configured tailnet owner. Only then is the message treated as
// that verified user. Because the transport is itself authenticated this way,
// the endpoint's identity is registered as a relay admin OUTSIDE
// SessionGatedUsers — the tailnet proof stands in for the session gate, exactly
// as the Matrix homeserver path does.
package web

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/inbound"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// Frontend implements relay.Endpoint (and relay.Claimer) over SSE+POST.
type Frontend struct {
	convID   string // the single 1:1 conversation id (== chat_id == from_id)
	fromName string
	owner    string // required tailnet login (whois LoginName), e.g. jeanhaley32@gmail.com
	logger   *log.Logger

	out chan relay.Message
	srv *http.Server

	mu        sync.Mutex
	clients   map[chan string]struct{} // connected SSE writers
	closed    bool
	recvDrops atomic.Int64
}

// New starts the HTTP server on listenAddr and returns the Frontend. convID is
// the conversation/sender id used for every message from this channel; owner is
// the tailnet LoginName a connection must resolve to.
func New(listenAddr, convID, fromName, owner string, logger *log.Logger) (*Frontend, error) {
	f := &Frontend{
		convID: convID, fromName: fromName, owner: owner, logger: logger,
		out:     make(chan relay.Message, 64),
		clients: map[chan string]struct{}{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/stream", f.handleStream)
	mux.HandleFunc("/send", f.handleSend)
	mux.HandleFunc("/whoami", f.handleWhoami)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	f.srv = &http.Server{Handler: mux}
	go func() {
		if err := f.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Printf("web: server stopped: %v", err)
		}
	}()
	return f, nil
}

// --- relay.Endpoint / relay.Claimer ---

func (f *Frontend) Name() string                     { return "web" }
func (f *Frontend) Recv() <-chan relay.Message        { return f.out }
func (f *Frontend) OwnsConversationID(id string) bool { return id == f.convID }

func (f *Frontend) Send(ctx context.Context, m relay.Message) error {
	payload, _ := json.Marshal(map[string]string{
		"text": m.Text,
		"ts":   time.Now().UTC().Format(time.RFC3339),
	})
	f.mu.Lock()
	defer f.mu.Unlock()
	for ch := range f.clients {
		select {
		case ch <- string(payload):
		default: // slow/backed-up client — drop this frame rather than block the broker
		}
	}
	return nil
}

func (f *Frontend) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = f.srv.Shutdown(ctx)
	close(f.out)
	return nil
}

// --- identity gate ---

func clientIP(r *http.Request) string {
	if ip := strings.TrimSpace(r.Header.Get("X-Real-IP")); ip != "" {
		return ip
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// tailscaleWhois resolves a tailnet IP to its owning account's LoginName by
// shelling out to the tailscale CLI (same approach as internal/tailnet).
func tailscaleWhois(ip string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "tailscale", "whois", "--json", ip).Output()
	if err != nil {
		return "", err
	}
	var v struct {
		UserProfile struct {
			LoginName string `json:"LoginName"`
		} `json:"UserProfile"`
	}
	if err := json.Unmarshal(out, &v); err != nil {
		return "", err
	}
	return v.UserProfile.LoginName, nil
}

// verify returns true only if the request originates from a tailnet device
// owned by the configured owner. Fail-closed on any error.
func (f *Frontend) verify(r *http.Request) bool {
	ip := clientIP(r)
	if ip == "" {
		return false
	}
	login, err := tailscaleWhois(ip)
	if err != nil {
		f.logger.Printf("web: whois %s failed (denying): %v", ip, err)
		return false
	}
	if login != f.owner {
		f.logger.Printf("web: denying %s — resolved to %q, not owner %q", ip, login, f.owner)
		return false
	}
	return true
}

// --- HTTP handlers ---

func (f *Frontend) handleWhoami(w http.ResponseWriter, r *http.Request) {
	ok := f.verify(r)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"verified": ok, "conv_id": f.convID})
}

func (f *Frontend) handleStream(w http.ResponseWriter, r *http.Request) {
	if !f.verify(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // belt-and-suspenders for proxies
	w.WriteHeader(http.StatusOK)

	ch := make(chan string, 32)
	f.mu.Lock()
	f.clients[ch] = struct{}{}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.clients, ch)
		f.mu.Unlock()
	}()

	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()
	ka := time.NewTicker(25 * time.Second)
	defer ka.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ka.C:
			fmt.Fprint(w, ": ka\n\n")
			fl.Flush()
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			fl.Flush()
		}
	}
}

func (f *Frontend) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !f.verify(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var body struct {
		Text  string `json:"text"`
		Quote string `json:"quote"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	text := strings.TrimSpace(body.Text)
	if text == "" {
		http.Error(w, "empty message", http.StatusBadRequest)
		return
	}
	// A selection sent from the editor is prepended as quoted context so the
	// model sees exactly what the user was pointing at.
	full := text
	if q := strings.TrimSpace(body.Quote); q != "" {
		full = "Selected passage:\n> " + strings.ReplaceAll(q, "\n", "\n> ") + "\n\n" + text
	}
	msg := inbound.Envelope("web", f.convID, full, f.convID, f.convID, f.fromName)
	select {
	case f.out <- msg:
		w.WriteHeader(http.StatusNoContent)
	default:
		f.recvDrops.Add(1)
		http.Error(w, "relay busy", http.StatusServiceUnavailable)
	}
}
