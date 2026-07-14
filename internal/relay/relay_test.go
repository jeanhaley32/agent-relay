package relay

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/approval"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
	"github.com/jeanhaley32/agent-relay/internal/session"
)

// capFrontend captures everything Send'd to it.
type capFrontend struct {
	recv chan Message
	sent chan Message
}

func (f *capFrontend) Name() string                            { return "front" }
func (f *capFrontend) Recv() <-chan Message                    { return f.recv }
func (f *capFrontend) Send(_ context.Context, m Message) error { f.sent <- m; return nil }
func (f *capFrontend) Close() error                            { return nil }

// failFrontend fails every Send with a fixed error - used to test that a
// failed delivery is reported via AckBackendReply and does NOT fire
// OnBackendReply (a failed send never reached the user).
type failFrontend struct {
	recv chan Message
	err  error
}

func (f *failFrontend) Name() string                            { return "failfront" }
func (f *failFrontend) Recv() <-chan Message                    { return f.recv }
func (f *failFrontend) Send(_ context.Context, _ Message) error { return f.err }
func (f *failFrontend) Close() error                            { return nil }

// emitBackend emits replies the test pushes onto recv.
type emitBackend struct{ recv chan Message }

func (b *emitBackend) Name() string                        { return "back" }
func (b *emitBackend) Recv() <-chan Message                { return b.recv }
func (b *emitBackend) Send(context.Context, Message) error { return nil }
func (b *emitBackend) Close() error                        { close(b.recv); return nil }

// recordBackend captures what the broker forwards to the model.
type recordBackend struct {
	got  chan Message
	recv chan Message
}

func (b *recordBackend) Name() string                            { return "rec" }
func (b *recordBackend) Recv() <-chan Message                    { return b.recv }
func (b *recordBackend) Send(_ context.Context, m Message) error { b.got <- m; return nil }
func (b *recordBackend) Close() error                            { close(b.recv); return nil }

// TestCommandEscape: `\/help` is unescaped and forwarded to the model; a real
// `/help` is handled by the relay and never reaches the backend.
func TestCommandEscape(t *testing.T) {
	front := &capFrontend{recv: make(chan Message), sent: make(chan Message, 8)}
	back := &recordBackend{got: make(chan Message, 8), recv: make(chan Message, 8)}
	cmds := command.NewRegistry() // has the built-in /help
	cmds.IsAdmin = func(string) bool { return true }
	b := &Broker{Frontend: front, Backend: back, Commands: cmds, Meter: budget.New("pro", nil)}
	go b.Run(context.Background())
	defer close(front.recv)

	// Escaped: reaches the backend as "/help".
	front.recv <- Message{Role: User, Text: `\/help`, Meta: map[string]string{"chat_id": "1"}}
	select {
	case m := <-back.got:
		if m.Text != "/help" {
			t.Fatalf("escaped command should reach backend as /help, got %q", m.Text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("escaped command never reached the backend")
	}

	// Real command: handled locally, must NOT reach the backend.
	front.recv <- Message{Role: User, Text: `/help`, Meta: map[string]string{"chat_id": "1"}}
	select {
	case m := <-back.got:
		t.Fatalf("real command leaked to backend: %q", m.Text)
	case <-time.After(300 * time.Millisecond):
		// good — dispatched locally
	}
	// ...and its reply went to the frontend.
	select {
	case <-front.sent:
	case <-time.After(time.Second):
		t.Fatal("expected a /help reply to the frontend")
	}
}

// TestOutboundGate: the model's replies to non-allowlisted chats are dropped;
// replies to allowed chats are delivered.
func TestOutboundGate(t *testing.T) {
	front := &capFrontend{recv: make(chan Message), sent: make(chan Message, 8)}
	back := &emitBackend{recv: make(chan Message, 8)}
	b := &Broker{
		Frontend:        front,
		Backend:         back,
		Commands:        command.NewRegistry(),
		Meter:           budget.New("pro", nil),
		OutboundAllowed: func(chatID string) bool { return chatID == "111" }, // only 111 allowed
	}
	go b.Run(context.Background())
	defer close(front.recv) // ends Run

	// Allowed reply is delivered.
	back.recv <- Message{Role: Assistant, Text: "hi", Meta: map[string]string{"chat_id": "111"}}
	// Reply to a stranger is dropped.
	back.recv <- Message{Role: Assistant, Text: "leak", Meta: map[string]string{"chat_id": "999"}}

	select {
	case m := <-front.sent:
		if m.Meta["chat_id"] != "111" || m.Text != "hi" {
			t.Fatalf("expected the allowed reply, got %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("allowed reply was not delivered")
	}

	// The blocked reply must not arrive.
	select {
	case m := <-front.sent:
		t.Fatalf("outbound gate leaked a reply to a stranger: %+v", m)
	case <-time.After(300 * time.Millisecond):
		// good — dropped
	}
}

// TestAckBackendReplyReportsFailureAndSuppressesOnBackendReply: a Send
// failure must be reported via AckBackendReply (so the reply tool call can
// surface it to the model - real incident 2026-07-11, replies over
// Telegram's 4096-char limit were silently dropped with no error anywhere),
// and must NOT fire OnBackendReply, since the reply never actually reached
// the user and is not evidence a trigger was handled.
func TestAckBackendReplyReportsFailureAndSuppressesOnBackendReply(t *testing.T) {
	sendErr := errors.New("telegram sendMessage status 400: message is too long")
	front := &failFrontend{recv: make(chan Message), err: sendErr}
	back := &emitBackend{recv: make(chan Message, 8)}
	acked := make(chan error, 8)
	onReplyFired := make(chan string, 8)
	b := &Broker{
		Frontend:        front,
		Backend:         back,
		Commands:        command.NewRegistry(),
		Meter:           budget.New("pro", nil),
		AckBackendReply: func(_ Message, err error) { acked <- err },
		OnBackendReply:  func(m Message) { onReplyFired <- m.Meta["chat_id"] },
	}
	go b.Run(context.Background())
	defer close(front.recv)

	back.recv <- Message{Role: Assistant, Text: "too long", Meta: map[string]string{"chat_id": "111"}}

	select {
	case err := <-acked:
		if err == nil || err.Error() != sendErr.Error() {
			t.Fatalf("AckBackendReply err = %v, want %v", err, sendErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AckBackendReply never fired for a failed send")
	}
	select {
	case id := <-onReplyFired:
		t.Fatalf("OnBackendReply fired for a failed send (chat %q) - a failed send never reached the user", id)
	case <-time.After(300 * time.Millisecond):
		// good — suppressed
	}
}

// TestOnBackendReplyGated: OnBackendReply must fire only for replies that pass
// the outbound gate and are actually delivered — a reply dropped by the gate is
// not evidence a trigger was handled and must not auto-ack anything.
func TestOnBackendReplyGated(t *testing.T) {
	front := &capFrontend{recv: make(chan Message), sent: make(chan Message, 8)}
	back := &emitBackend{recv: make(chan Message, 8)}
	seen := make(chan string, 8)
	b := &Broker{
		Frontend:        front,
		Backend:         back,
		Commands:        command.NewRegistry(),
		Meter:           budget.New("pro", nil),
		OutboundAllowed: func(chatID string) bool { return chatID == "111" },
		OnBackendReply:  func(m Message) { seen <- m.Meta["chat_id"] },
	}
	go b.Run(context.Background())
	defer close(front.recv)

	back.recv <- Message{Role: Assistant, Text: "hi", Meta: map[string]string{"chat_id": "111"}}   // allowed
	back.recv <- Message{Role: Assistant, Text: "leak", Meta: map[string]string{"chat_id": "999"}} // dropped

	select {
	case id := <-seen:
		if id != "111" {
			t.Fatalf("OnBackendReply fired for the wrong (dropped) chat: %q", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnBackendReply never fired for the delivered reply")
	}
	// The dropped reply must not trigger OnBackendReply.
	select {
	case id := <-seen:
		t.Fatalf("OnBackendReply fired for a gate-dropped reply (chat %q)", id)
	case <-time.After(300 * time.Millisecond):
		// good — gated out
	}
}

// TestLockdown verifies that when active, non-admin senders are blocked
// entirely (never reach commands or the backend) while admin senders pass
// through normally.
func TestLockdown(t *testing.T) {
	front := &capFrontend{recv: make(chan Message), sent: make(chan Message, 8)}
	back := &recordBackend{got: make(chan Message, 8), recv: make(chan Message, 8)}
	cmds := command.NewRegistry()
	cmds.IsAdmin = func(id string) bool { return id == "admin-id" }
	b := &Broker{Frontend: front, Backend: back, Commands: cmds, Meter: budget.New("pro", nil)}
	b.Lockdown.Store(true)
	go b.Run(context.Background())
	defer close(front.recv)

	// Non-admin sender: blocked, gets the lockdown message, never reaches backend.
	front.recv <- Message{Role: User, Text: "hi", Meta: map[string]string{"chat_id": "stranger-id", "from_id": "stranger-id"}}
	select {
	case m := <-front.sent:
		if m.Text != LockdownMessage {
			t.Fatalf("expected lockdown message, got %q", m.Text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected a lockdown reply to the non-admin sender")
	}
	select {
	case m := <-back.got:
		t.Fatalf("lockdown leaked a non-admin message to the backend: %+v", m)
	case <-time.After(300 * time.Millisecond):
	}

	// Admin sender: unaffected.
	front.recv <- Message{Role: User, Text: "hello", Meta: map[string]string{"chat_id": "admin-id", "from_id": "admin-id"}}
	select {
	case m := <-back.got:
		if m.Text != "hello" {
			t.Fatalf("wrong message reached backend: %q", m.Text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admin message was blocked by lockdown")
	}
}

// TestIdentityPairInvariant verifies a message whose chat_id and from_id are
// both present but mismatched (which should never happen given the Telegram
// frontend's own private-chat-only guarantee, but every downstream gate here
// silently depends on that invariant holding) is dropped rather than
// processed under a possibly-wrong identity.
func TestIdentityPairInvariant(t *testing.T) {
	front := &capFrontend{recv: make(chan Message), sent: make(chan Message, 8)}
	back := &recordBackend{got: make(chan Message, 8), recv: make(chan Message, 8)}
	cmds := command.NewRegistry()
	cmds.IsAdmin = func(string) bool { return true }
	b := &Broker{Frontend: front, Backend: back, Commands: cmds, Meter: budget.New("pro", nil)}
	go b.Run(context.Background())
	defer close(front.recv)

	// Mismatched pair: must be dropped entirely - no backend forward, no
	// frontend reply.
	front.recv <- Message{Role: User, Text: "hello", Meta: map[string]string{"chat_id": "group-999", "from_id": "111"}}
	select {
	case m := <-back.got:
		t.Fatalf("mismatched identity pair reached the backend: %+v", m)
	case <-time.After(300 * time.Millisecond):
		// good — dropped
	}
	select {
	case m := <-front.sent:
		t.Fatalf("mismatched identity pair got a reply instead of being silently dropped: %+v", m)
	case <-time.After(300 * time.Millisecond):
		// good
	}

	// A matched pair (or from_id absent entirely) proceeds normally.
	front.recv <- Message{Role: User, Text: "hello", Meta: map[string]string{"chat_id": "111", "from_id": "111"}}
	select {
	case m := <-back.got:
		if m.Text != "hello" {
			t.Fatalf("wrong message reached backend: %q", m.Text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("matched identity pair was incorrectly dropped")
	}
}

// TestSessionGate exercises the real, end-to-end integration path: an
// expired admin session blocks everything (including slash commands) until
// the tailnet-side approval page is hit, at which point a background
// poller activates the session and the admin's next message is admitted.
func TestSessionGate(t *testing.T) {
	front := &capFrontend{recv: make(chan Message), sent: make(chan Message, 8)}
	back := &recordBackend{got: make(chan Message, 8), recv: make(chan Message, 8)}
	cmds := command.NewRegistry()
	cmds.IsAdmin = func(string) bool { return true }
	appr := approval.NewManager("http://tailnet.example")

	b := &Broker{
		Frontend:          front,
		Backend:           back,
		Commands:          cmds,
		Meter:             budget.New("pro", nil),
		Session:           session.NewManager(30 * time.Minute),
		Approval:          appr,
		SessionGatedUsers: map[string]bool{"admin-chat": true},
		SessionTTL:        2 * time.Second,
	}
	go b.Run(context.Background())
	defer close(front.recv)

	// 1. First message from the gated user, with no active session: must be
	// dropped (not forwarded to the backend, not dispatched as a command),
	// and a challenge with an approval link must go to the frontend instead.
	// chat_id == from_id, matching the private-1:1-chat invariant the real
	// Telegram frontend enforces.
	front.recv <- Message{Role: User, Text: "/help", Meta: map[string]string{"chat_id": "admin-chat", "from_id": "admin-chat"}}

	var link string
	select {
	case m := <-front.sent:
		if !strings.Contains(m.Text, "http://tailnet.example/approve/") {
			t.Fatalf("expected a challenge with an approval link, got %q", m.Text)
		}
		link = m.Text[strings.Index(m.Text, "http://tailnet.example/approve/"):]
	case <-time.After(2 * time.Second):
		t.Fatal("expected a session-expired challenge on the frontend")
	}
	select {
	case m := <-back.got:
		t.Fatalf("expired session leaked a message to the backend: %+v", m)
	case <-time.After(300 * time.Millisecond):
		// good — the /help command was NOT dispatched either (no reply here
		// would come from the frontend queue, and back.got must stay empty)
	}

	token := link[strings.LastIndex(link, "/")+1:]

	// 2. Approve via the same HTTP surface a human would hit (the tailnet
	// page), not by poking the Manager directly.
	appSrv := httptest.NewServer(appr.ApproveHandler())
	defer appSrv.Close()
	resp, err := http.PostForm(appSrv.URL+"/approve/"+token, url.Values{"decision": {"approve"}})
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	resp.Body.Close()

	// 3. The background poller should activate the session and notify the
	// frontend.
	select {
	case m := <-front.sent:
		if !strings.Contains(m.Text, "re-authenticated") {
			t.Fatalf("expected a re-authenticated confirmation, got %q", m.Text)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("session was never activated after approval")
	}

	// 4. A message sent now should be admitted normally.
	front.recv <- Message{Role: User, Text: "hello", Meta: map[string]string{"chat_id": "admin-chat", "from_id": "admin-chat"}}
	select {
	case m := <-back.got:
		if m.Text != "hello" {
			t.Fatalf("wrong message reached backend: %q", m.Text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message after re-auth was not forwarded to the backend")
	}
}

// TestConversationCap covers the real 2026-07-14 incident: an
// allowlisted-but-non-admin contact testing how much inference the relay
// would spend on an open-ended request. A per-conversation cap must stop
// inbound messages from reaching the backend at all once the cap is hit -
// not just gate the outbound reply - since the real cost is inference
// tokens, spent the moment a message is forwarded to the model.
func TestConversationCap(t *testing.T) {
	front := &capFrontend{recv: make(chan Message), sent: make(chan Message, 8)}
	back := &recordBackend{got: make(chan Message, 8), recv: make(chan Message, 8)}
	b := &Broker{
		Frontend:         front,
		Backend:          back,
		Commands:         command.NewRegistry(),
		Meter:            budget.New("pro", nil),
		Estimate:         func(text string) int { return len(text) }, // 1 token/char for exact test math
		ConversationCaps: map[string]int64{"999": 10},                // tiny cap, easy to exceed
	}
	go b.Run(context.Background())
	defer close(front.recv)

	// First message (5 tokens) is well under the cap of 10 - forwarded.
	front.recv <- Message{Role: User, Text: "hello", ConversationID: "999",
		Meta: map[string]string{"chat_id": "999", "from_id": "999"}}
	select {
	case m := <-back.got:
		if m.Text != "hello" {
			t.Fatalf("expected 'hello' forwarded to backend, got %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first message (under cap) was never forwarded to the backend")
	}

	// Second message (7 tokens) pushes cumulative usage (5+7=12) over the
	// cap of 10 - this is the message that CROSSES the cap, so it must NOT
	// itself reach the backend (a single oversized message could otherwise
	// blow through an arbitrarily small cap); only the cap-hit notice goes
	// out.
	front.recv <- Message{Role: User, Text: "1234567", ConversationID: "999",
		Meta: map[string]string{"chat_id": "999", "from_id": "999"}}
	select {
	case m := <-back.got:
		t.Fatalf("the message that crosses the cap must not reach the backend: %+v", m)
	case <-time.After(300 * time.Millisecond):
		// good - the crossing message itself is not forwarded
	}
	select {
	case m := <-front.sent:
		if !strings.Contains(m.Text, "token cap") {
			t.Fatalf("expected a cap-hit notice, got %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected a cap-hit notice to be sent to the conversation")
	}

	// Third message: now genuinely over cap - must be dropped BEFORE
	// reaching the backend at all (no more inference spent on this
	// conversation), and no further notice sent (only fires once).
	front.recv <- Message{Role: User, Text: "should not reach backend", ConversationID: "999",
		Meta: map[string]string{"chat_id": "999", "from_id": "999"}}
	select {
	case m := <-back.got:
		t.Fatalf("conversation cap did not stop a message from reaching the backend: %+v", m)
	case <-time.After(300 * time.Millisecond):
		// good - dropped before the backend
	}
	select {
	case m := <-front.sent:
		t.Fatalf("expected no further notice after the cap was already hit once, got %+v", m)
	case <-time.After(300 * time.Millisecond):
		// good
	}

	// A DIFFERENT, uncapped conversation must be entirely unaffected.
	front.recv <- Message{Role: User, Text: "unrelated", ConversationID: "111",
		Meta: map[string]string{"chat_id": "111", "from_id": "111"}}
	select {
	case m := <-back.got:
		if m.Text != "unrelated" {
			t.Fatalf("expected the uncapped conversation's message forwarded, got %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an uncapped conversation must be unaffected by another conversation's cap")
	}
}

// TestConversationCapRollsOverOnWindow covers Jean's explicit request
// (2026-07-14): the cap is a rate limit ("see the cap at every 3 hours"),
// not a lifetime ban - usage must reset once the configured window elapses.
func TestConversationCapRollsOverOnWindow(t *testing.T) {
	front := &capFrontend{recv: make(chan Message), sent: make(chan Message, 8)}
	back := &recordBackend{got: make(chan Message, 8), recv: make(chan Message, 8)}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	b := &Broker{
		Frontend:              front,
		Backend:               back,
		Commands:              command.NewRegistry(),
		Meter:                 budget.New("pro", nil),
		Estimate:              func(text string) int { return len(text) },
		ConversationCaps:      map[string]int64{"999": 10},
		ConversationCapWindow: time.Hour,
		clock:                 func() time.Time { return now },
	}
	go b.Run(context.Background())
	defer close(front.recv)

	// Well under the cap of 10 - forwarded.
	front.recv <- Message{Role: User, Text: "12345", ConversationID: "999",
		Meta: map[string]string{"chat_id": "999", "from_id": "999"}}
	select {
	case <-back.got:
	case <-time.After(2 * time.Second):
		t.Fatal("first message never reached the backend")
	}

	// Pushes cumulative usage (5+7=12) over the cap - dropped before the
	// backend, one notice sent.
	front.recv <- Message{Role: User, Text: "1234567", ConversationID: "999",
		Meta: map[string]string{"chat_id": "999", "from_id": "999"}}
	select {
	case m := <-back.got:
		t.Fatalf("expected drop while still within the window, got %+v", m)
	case <-time.After(300 * time.Millisecond):
	}
	select {
	case <-front.sent:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a cap-hit notice")
	}

	// Advance past the 1-hour window - usage should roll over to zero.
	now = now.Add(time.Hour + time.Minute)
	front.recv <- Message{Role: User, Text: "y", ConversationID: "999",
		Meta: map[string]string{"chat_id": "999", "from_id": "999"}}
	select {
	case m := <-back.got:
		if m.Text != "y" {
			t.Fatalf("unexpected message forwarded: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message after the window rolled over should have been forwarded - cap must reset, not stay a lifetime ban")
	}
}

// TestSetConversationUsageOverridesEstimate covers the 2026-07-14 real
// attribution hook: SetConversationUsage overwrites the interim chars/4
// estimate with an authoritative real number, and takes effect immediately
// for the next cap check - no double counting with prior estimate-based
// addConversationUsage calls.
func TestSetConversationUsageOverridesEstimate(t *testing.T) {
	b := &Broker{
		ConversationCaps: map[string]int64{"999": 100},
	}

	// Estimate-based tracking says 10 tokens used - well under cap.
	b.addConversationUsage("999", 10)
	if b.conversationCapExceeded("999") {
		t.Fatal("should not be exceeded yet (10 < 100)")
	}

	// Real attribution says the actual usage was much higher - overwrite,
	// not add (10 + 150 would also exceed, but this proves it's a set).
	b.SetConversationUsage("999", 150)
	if !b.conversationCapExceeded("999") {
		t.Fatal("expected the real usage override (150) to exceed the cap (100)")
	}

	// A lower real number can also bring a conversation back under cap -
	// proves it's a true overwrite, not a monotonic add.
	b.SetConversationUsage("999", 5)
	if b.conversationCapExceeded("999") {
		t.Fatal("expected the corrected-down real usage (5) to no longer exceed the cap")
	}

	// No-op for an uncapped chat_id.
	b.SetConversationUsage("111", 999999)
	if b.conversationCapExceeded("111") {
		t.Fatal("SetConversationUsage must be a no-op for a chat_id with no configured cap")
	}
}
