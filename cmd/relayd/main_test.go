package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jeanhaley32/agent-relay/internal/access"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
	claudebk "github.com/jeanhaley32/agent-relay/internal/endpoint/claude"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/senderr"
	"github.com/jeanhaley32/agent-relay/internal/ipc"
	"github.com/jeanhaley32/agent-relay/internal/relay"
	"github.com/jeanhaley32/agent-relay/internal/scheduler"
)

// TestAckErrTextClassification exercises the permanent-vs-transient
// classification used by the real AckBackendReply closure: only a
// senderr.Permanent failure should be surfaced back to the reply tool call,
// so a transient failure that Frontend.Send has already queued for
// background retry doesn't invite the model to resend and duplicate
// delivery once the retry lands.
func TestAckErrTextClassification(t *testing.T) {
	permErr := senderr.Permanent{Err: errors.New("chat_id is not an allowed destination")}
	transientErr := errors.New("telegram sendMessage status 500: try again")

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil error", nil, ""},
		{"transient error suppressed", transientErr, ""},
		{"permanent error surfaced", permErr, permErr.Error()},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ackErrText(c.err)
			if got != c.want {
				t.Errorf("ackErrText(%v) = %q, want %q", c.err, got, c.want)
			}
		})
	}

	// A permanent error wrapped by fmt.Errorf("%w", ...) must still be
	// detected via errors.As, not just a direct type assertion.
	wrapped := errWrap{permErr}
	if got := ackErrText(wrapped); got != wrapped.Error() {
		t.Errorf("ackErrText(wrapped permanent) = %q, want %q", got, wrapped.Error())
	}
}

type errWrap struct{ err error }

func (e errWrap) Error() string { return "wrapped: " + e.err.Error() }
func (e errWrap) Unwrap() error { return e.err }

// TestOutboundAllowed exercises the multi-frontend outbound gate: Telegram
// int64 ids, Discord snowflake ids, and the KnownConversation fallback for
// Discord guild channels/DMs the frontend has already gated inbound.
func TestOutboundAllowed(t *testing.T) {
	tgAcc := access.New([]int64{1}, []int64{42}, "", nil)
	discAcc := access.New([]int64{2}, []int64{99}, "", nil)
	known := func(id string) bool { return id == "known-chan" }

	cases := []struct {
		name       string
		chatID     string
		discordAcc *access.Manager
		known      func(string) bool
		want       bool
	}{
		{"telegram allowlisted", "42", discAcc, known, true},
		{"telegram not allowlisted", "7", discAcc, known, false},
		{"discord snowflake allowlisted", strconv.FormatInt(99, 10), discAcc, nil, true},
		{"known conversation fallback", "known-chan", discAcc, known, true},
		{"unknown non-numeric chat", "unknown-chan", discAcc, known, false},
		{"nil discordAcc and known", "42", nil, nil, true},
		{"nil discordAcc, unknown", "not-allowed", nil, nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := outboundAllowed(c.chatID, tgAcc, c.discordAcc, c.known)
			if got != c.want {
				t.Errorf("outboundAllowed(%q) = %v, want %v", c.chatID, got, c.want)
			}
		})
	}

	// A real Discord snowflake (large, non-Telegram-colliding value) allowed
	// only in the Discord manager must pass via the Discord branch.
	discOnly := access.New(nil, []int64{123456789012345678}, "", nil)
	if !outboundAllowed("123456789012345678", access.New(nil, nil, "", nil), discOnly, nil) {
		t.Error("outboundAllowed: expected discord-allowlisted snowflake to pass")
	}
}

// TestHandshakeMergesManagers verifies that /handshake listing and
// approve/deny try every manager in turn, so an admin on either frontend can
// resolve a request recorded by any manager without knowing which one it
// came from.
func TestHandshakeMergesManagers(t *testing.T) {
	m1 := access.New(nil, nil, "", nil)
	m2 := access.New(nil, nil, "", nil)
	m1.Record(111, "alice")
	m2.Record(222, "bob")

	managers := func() []*access.Manager { return []*access.Manager{m1, m2} }
	h := handshake(managers)

	list := h(command.Context{}, nil)
	if !contains(list, "alice") || !contains(list, "bob") {
		t.Errorf("handshake listing missing entries, got: %q", list)
	}

	// Approve the request recorded on m2 while only m1 "knows" about m1's
	// own pending id - approve must fall through to whichever manager has it.
	got := h(command.Context{}, []string{"approve", "222"})
	if !contains(got, "approved 222") {
		t.Errorf("approve on m2's id via merged managers: got %q", got)
	}
	if !m2.Allowed(222) {
		t.Error("expected id 222 to be allowed on m2 after approve")
	}

	// Deny the request recorded on m1.
	got = h(command.Context{}, []string{"deny", "111"})
	if !contains(got, "denied 111") {
		t.Errorf("deny on m1's id via merged managers: got %q", got)
	}

	// An id no manager has pending must fail on both approve and deny.
	got = h(command.Context{}, []string{"approve", "999"})
	if !contains(got, "not approved") {
		t.Errorf("approve on unknown id: got %q", got)
	}
	got = h(command.Context{}, []string{"deny", "999"})
	if !contains(got, "not pending") {
		t.Errorf("deny on unknown id: got %q", got)
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

// newTestTracker builds a Tracker backed by a temp-dir persistence file, with
// inject/fallback/receipt stubbed out — enough to drive handleEvent's
// ack/list ops without a live session or frontend.
func newTestTracker(t *testing.T) *scheduler.Tracker {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.json")
	tr, err := scheduler.NewTracker(path,
		func(chatID, text string) bool { return true },
		func(text string) error { return nil },
		func(note string) {},
		scheduler.TrackerConfig{},
		nil)
	if err != nil {
		t.Fatalf("NewTracker: %v", err)
	}
	t.Cleanup(tr.Close)
	return tr
}

// TestHandleEventListAndAck exercises handleEvent's OpEventList/OpEventAck
// dispatch (also reachable via serveSchedules' switch), covering the empty
// list, a populated list, a successful ack, and the unknown-op error path.
func TestHandleEventListAndAck(t *testing.T) {
	tr := newTestTracker(t)

	if got, errText := handleEvent(tr, claudebk.SchedRequest{Op: ipc.OpEventList}); errText != "" || got != "no open pending events" {
		t.Errorf("empty list: got (%q, %q)", got, errText)
	}

	ev, err := tr.Fire("", "chat1", "reminder text")
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}

	got, errText := handleEvent(tr, claudebk.SchedRequest{Op: ipc.OpEventList})
	if errText != "" {
		t.Fatalf("list after fire: errText=%q", errText)
	}
	if !contains(got, ev.ID) || !contains(got, "reminder text") {
		t.Errorf("list after fire: got %q, want it to mention %q and the reminder text", got, ev.ID)
	}

	got, errText = handleEvent(tr, claudebk.SchedRequest{Op: ipc.OpEventAck, SchedID: ev.ID, Text: "done"})
	if errText != "" {
		t.Fatalf("ack: errText=%q", errText)
	}
	if !contains(got, "acknowledged "+ev.ID) {
		t.Errorf("ack: got %q", got)
	}

	if got, errText := handleEvent(tr, claudebk.SchedRequest{Op: ipc.OpEventList}); errText != "" || got != "no open pending events" {
		t.Errorf("list after ack: got (%q, %q)", got, errText)
	}

	if _, errText := handleEvent(tr, claudebk.SchedRequest{Op: "bogus"}); errText == "" {
		t.Error("unknown op: expected an error, got none")
	}
}

// newTestMux builds a relayd webhook mux with minimal live dependencies
// (a claude backend endpoint on a temp socket, an in-memory budget meter,
// and the shared test tracker) - enough to drive the method-guard and
// payload-decode branches of each /webhook/* handler without a real relayd
// process.
func newTestMux(t *testing.T, adminChatID string) (*http.ServeMux, *relay.Broker) {
	t.Helper()
	back, err := claudebk.New(filepath.Join(t.TempDir(), "shim.sock"))
	if err != nil {
		t.Fatalf("claudebk.New: %v", err)
	}
	t.Cleanup(func() { _ = back.Close() })

	meter := budget.New("free", nil)
	tr := newTestTracker(t)
	logger := log.New(io.Discard, "", 0)
	b := &relay.Broker{Meter: meter}

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(cfgPath, []byte(`{"budget":{"tier":"free"},"telegram":{"admins":[1]}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	acc := access.New(nil, nil, "", logger)
	mux := newRelaydMux(back, adminChatID, acc, meter, nil, nil, tr, nil, logger, b, cfgPath)
	return mux, b
}

func TestWebhookReplyDriftHandler(t *testing.T) {
	mux, _ := newTestMux(t, "")

	// Wrong method is rejected.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/webhook/reply-drift", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: got status %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}

	before := replyDriftTotal.Load()
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhook/reply-drift", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("POST: got status %d, want %d", rec.Code, http.StatusOK)
	}
	if got := replyDriftTotal.Load(); got != before+1 {
		t.Errorf("replyDriftTotal: got %d, want %d", got, before+1)
	}
}

func TestWebhookTokenUsageHandler(t *testing.T) {
	mux, b := newTestMux(t, "")
	b.SetCaps(map[string]int64{"chat1": 1000}, 0)

	// Wrong method is rejected.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/webhook/token-usage", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: got status %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}

	// Bad payload is rejected.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhook/token-usage", bytes.NewBufferString("not json")))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("bad payload: got status %d, want %d", rec.Code, http.StatusBadRequest)
	}

	// Valid payload applies usage.
	body, _ := json.Marshal(map[string]any{"usage": map[string]int64{"chat1": 500}})
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhook/token-usage", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Errorf("valid payload: got status %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestWebhookReloadCapsHandler(t *testing.T) {
	mux, _ := newTestMux(t, "")

	// Wrong method is rejected.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/webhook/reload-caps", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: got status %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhook/reload-caps", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("POST: got status %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// TestMetricsHandler exercises /metrics with front=nil (as newTestMux always
// passes) to guard against the handler unconditionally dereferencing the
// Telegram frontend - safe in production where front is always non-nil, but
// this is the largest handler on the mux and previously had zero coverage.
func TestMetricsHandler(t *testing.T) {
	mux, _ := newTestMux(t, "")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics: got status %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"relayd_budget_percent_used",
		"relayd_circuit_breaker_state",
		"relayd_telegram_send_failures_total 0",
		"relayd_discord_send_failures_total 0",
		"relayd_pending_events_open 0",
	} {
		if !contains(body, want) {
			t.Errorf("/metrics body missing %q; got:\n%s", want, body)
		}
	}
}

// TestWebhookGrafanaHandler covers the method guard and the adminChatID gate
// that replaced the old whole-mux early-return: with no admin chat configured
// the handler itself now refuses the alert (503) instead of every other
// endpoint on the mux silently never existing.
func TestWebhookGrafanaHandler(t *testing.T) {
	muxNoAdmin, _ := newTestMux(t, "")
	rec := httptest.NewRecorder()
	body, _ := json.Marshal(map[string]any{"alerts": []any{}})
	muxNoAdmin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhook/grafana", bytes.NewReader(body)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("no admin chat: got status %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	muxAdmin, _ := newTestMux(t, "12345")

	rec = httptest.NewRecorder()
	muxAdmin.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/webhook/grafana", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: got status %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}

	rec = httptest.NewRecorder()
	muxAdmin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhook/grafana", bytes.NewBufferString("not json")))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("bad payload: got status %d, want %d", rec.Code, http.StatusBadRequest)
	}

	payload, _ := json.Marshal(map[string]any{
		"alerts": []map[string]any{
			{
				"status":      "firing",
				"labels":      map[string]string{"alertname": "TestAlert", "severity": "critical"},
				"annotations": map[string]string{"summary": "something broke"},
			},
		},
	})
	rec = httptest.NewRecorder()
	muxAdmin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhook/grafana", bytes.NewReader(payload)))
	if rec.Code != http.StatusOK {
		t.Errorf("valid payload: got status %d, want %d", rec.Code, http.StatusOK)
	}
}
