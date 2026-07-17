package router

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// stubFrontend is an in-memory SubFrontend for tests: pushes are made
// directly onto recvCh, and Send/Close are observed via the exported fields.
type stubFrontend struct {
	name   string
	recvCh chan relay.Message

	mu     sync.Mutex
	sent   []relay.Message
	closed bool
}

func newStub(name string) *stubFrontend {
	return &stubFrontend{name: name, recvCh: make(chan relay.Message, 16)}
}

func (s *stubFrontend) Name() string                  { return s.name }
func (s *stubFrontend) Recv() <-chan relay.Message     { return s.recvCh }
func (s *stubFrontend) push(m relay.Message)           { s.recvCh <- m }
func (s *stubFrontend) Send(_ context.Context, m relay.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, m)
	return nil
}
func (s *stubFrontend) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	close(s.recvCh)
	return nil
}
func (s *stubFrontend) sentMessages() []relay.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]relay.Message, len(s.sent))
	copy(out, s.sent)
	return out
}

// allowAll admits everything unchanged.
type allowAll struct{}

func (allowAll) Clear(m relay.Message) (relay.Message, bool) { return m, true }

// denyContains rejects any message whose Text contains the given substring.
type denyContains struct{ substr string }

func (d denyContains) Clear(m relay.Message) (relay.Message, bool) {
	if len(d.substr) > 0 && contains(m.Text, d.substr) {
		return m, false
	}
	return m, true
}

func contains(s, sub string) bool {
	return len(sub) <= len(s) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

// rewriteFrom injects Meta["from_id"] on every cleared message.
type rewriteFrom struct{ id string }

func (rw rewriteFrom) Clear(m relay.Message) (relay.Message, bool) {
	if m.Meta == nil {
		m.Meta = map[string]string{}
	}
	m.Meta["from_id"] = rw.id
	return m, true
}

func drain(t *testing.T, r *Router, n int, timeout time.Duration) []relay.Message {
	t.Helper()
	got := make([]relay.Message, 0, n)
	deadline := time.After(timeout)
	for len(got) < n {
		select {
		case m := <-r.Recv():
			got = append(got, m)
		case <-deadline:
			t.Fatalf("timed out waiting for %d message(s), got %d", n, len(got))
		}
	}
	return got
}

func TestFanInTagsFrontendAndDeliversBoth(t *testing.T) {
	r := New("test-router")
	a := newStub("A")
	b := newStub("B")
	if err := r.Register(a, allowAll{}); err != nil {
		t.Fatalf("register A: %v", err)
	}
	if err := r.Register(b, allowAll{}); err != nil {
		t.Fatalf("register B: %v", err)
	}

	a.push(relay.Message{ConversationID: "c1", Text: "hello from A"})
	b.push(relay.Message{ConversationID: "c2", Text: "hello from B"})

	got := drain(t, r, 2, 2*time.Second)
	seen := map[string]string{}
	for _, m := range got {
		seen[m.Meta[FrontendMetaKey]] = m.Text
	}
	if seen["A"] != "hello from A" {
		t.Errorf("expected A's message tagged frontend=A, got %+v", seen)
	}
	if seen["B"] != "hello from B" {
		t.Errorf("expected B's message tagged frontend=B, got %+v", seen)
	}
}

func TestReplyRoutesToCorrectSubFrontendOnly(t *testing.T) {
	r := New("test-router")
	a := newStub("A")
	b := newStub("B")
	_ = r.Register(a, allowAll{})
	_ = r.Register(b, allowAll{})

	err := r.Send(context.Background(), relay.Message{
		Text: "reply for A",
		Meta: map[string]string{FrontendMetaKey: "A"},
	})
	if err != nil {
		t.Fatalf("Send to A: %v", err)
	}

	if got := a.sentMessages(); len(got) != 1 || got[0].Text != "reply for A" {
		t.Errorf("stub A should have received the reply, got %+v", got)
	}
	if got := b.sentMessages(); len(got) != 0 {
		t.Errorf("stub B should NOT have received A's reply, got %+v", got)
	}
}

func TestSendMissingOrUnknownFrontendTagErrorsAndDeliversNowhere(t *testing.T) {
	r := New("test-router")
	a := newStub("A")
	_ = r.Register(a, allowAll{})

	if err := r.Send(context.Background(), relay.Message{Text: "no tag"}); err == nil {
		t.Error("expected error for message with no Meta[frontend], got nil")
	}
	if err := r.Send(context.Background(), relay.Message{
		Text: "bad tag",
		Meta: map[string]string{FrontendMetaKey: "nonexistent"},
	}); err == nil {
		t.Error("expected error for unregistered frontend name, got nil")
	}
	if got := a.sentMessages(); len(got) != 0 {
		t.Errorf("stub A should have received nothing, got %+v", got)
	}
}

func TestCloseClosesAllSubFrontendsAndDrainsOutput(t *testing.T) {
	r := New("test-router")
	a := newStub("A")
	b := newStub("B")
	_ = r.Register(a, allowAll{})
	_ = r.Register(b, allowAll{})

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !a.closed || !b.closed {
		t.Errorf("expected both stubs closed, got A=%v B=%v", a.closed, b.closed)
	}
	if _, ok := <-r.Recv(); ok {
		t.Error("expected Router.Recv() to be closed after Close()")
	}
}

func TestConcurrentRecvAndSendRace(t *testing.T) {
	r := New("test-router")
	a := newStub("A")
	b := newStub("B")
	_ = r.Register(a, allowAll{})
	_ = r.Register(b, allowAll{})

	const n = 50
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			a.push(relay.Message{Text: "a"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			b.push(relay.Message{Text: "b"})
		}
	}()

	drain(t, r, 2*n, 5*time.Second)
	wg.Wait()

	var sendWG sync.WaitGroup
	sendWG.Add(2)
	go func() {
		defer sendWG.Done()
		for i := 0; i < n; i++ {
			_ = r.Send(context.Background(), relay.Message{Meta: map[string]string{FrontendMetaKey: "A"}})
		}
	}()
	go func() {
		defer sendWG.Done()
		for i := 0; i < n; i++ {
			_ = r.Send(context.Background(), relay.Message{Meta: map[string]string{FrontendMetaKey: "B"}})
		}
	}()
	sendWG.Wait()
}

// --- admin-gate-enforcement tests ---

func TestNilAdminLayerRejectsRegistration(t *testing.T) {
	r := New("test-router")
	a := newStub("A")
	if err := r.Register(a, nil); err == nil {
		t.Fatal("expected error registering with nil AdminLayer, got nil")
	}
	a.push(relay.Message{Text: "should never surface"})
	select {
	case m := <-r.Recv():
		t.Fatalf("unregistered stub's message reached Recv(): %+v", m)
	case <-time.After(200 * time.Millisecond):
		// expected: nothing arrives
	}
}

func TestAdminLayerRejectsAndCountsDeniedMessages(t *testing.T) {
	r := New("test-router")
	a := newStub("A")
	if err := r.Register(a, denyContains{substr: "deny"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	a.push(relay.Message{Text: "this is fine"})
	a.push(relay.Message{Text: "please deny this"})
	a.push(relay.Message{Text: "also fine"})

	got := drain(t, r, 2, 2*time.Second)
	for _, m := range got {
		if m.Text == "please deny this" {
			t.Errorf("denied message reached Recv(): %+v", m)
		}
	}

	// stats update happens on the same goroutine that increments before
	// looping again; poll briefly to avoid a race with the test's own read.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.Stats()["A"] == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected Stats()[A] == 1 rejection, got %v", r.Stats())
}

func TestAdminLayerRewriteIsWhatRecvEmits(t *testing.T) {
	r := New("test-router")
	a := newStub("A")
	if err := r.Register(a, rewriteFrom{id: "user-42"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	a.push(relay.Message{Text: "hi"})
	got := drain(t, r, 1, 2*time.Second)
	if got[0].Meta["from_id"] != "user-42" {
		t.Errorf("expected admin-layer rewrite to survive to Recv(), got Meta=%v", got[0].Meta)
	}
}
