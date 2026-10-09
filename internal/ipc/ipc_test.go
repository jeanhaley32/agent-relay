package ipc

import (
	"io"
	"net"
	"strings"
	"testing"
)

// TestFrameRoundTrip sends frames both directions over a net.Pipe and checks
// they decode identically.
func TestFrameRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ca, cb := NewConn(a), NewConn(b)

	want := Frame{Kind: KindInject, ChatID: "42", Text: "ping", Meta: map[string]string{"from": "jean"}}
	go func() { _ = ca.Send(want) }()

	got, err := cb.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got.Kind != want.Kind || got.ChatID != want.ChatID || got.Text != want.Text || got.Meta["from"] != "jean" {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
	}

	// reply direction
	reply := Frame{Kind: KindReply, ChatID: "42", Text: "pong"}
	go func() { _ = cb.Send(reply) }()
	got2, err := ca.Recv()
	if err != nil {
		t.Fatalf("recv reply: %v", err)
	}
	if got2.Kind != KindReply || got2.Text != "pong" {
		t.Fatalf("reply mismatch: %+v", got2)
	}
}

// Frames were decoded with a bare json.Decoder, so a peer could force
// unbounded allocation. The socket is 0600 in a private runtime dir, so this
// is defense in depth rather than an exposed hole — but nothing else bounded
// it.
func TestFrameSizeIsCapped(t *testing.T) {
	// One frame larger than the cap must fail rather than allocate.
	huge := `{"kind":"reply","text":"` + strings.Repeat("a", MaxFrameBytes+1024) + `"}` + "\n"
	c := NewConn(nopCloser{strings.NewReader(huge)})
	if _, err := c.Recv(); err == nil {
		t.Fatal("an oversized frame was accepted")
	}
}

// The cap must be per frame, not per connection: io.LimitReader would bound
// the whole stream, so a long-lived socket would stop carrying frames once it
// had passed the limit in total.
func TestCapIsPerFrameNotPerConnection(t *testing.T) {
	// Each frame is ~1 MiB; several of them exceed any per-connection budget
	// of the same size but are individually fine.
	one := `{"kind":"reply","text":"` + strings.Repeat("b", 1<<20) + `"}` + "\n"
	var sb strings.Builder
	for i := 0; i < 12; i++ {
		sb.WriteString(one)
	}
	c := NewConn(nopCloser{strings.NewReader(sb.String())})
	for i := 0; i < 12; i++ {
		f, err := c.Recv()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if f.Kind != KindReply {
			t.Fatalf("frame %d: kind %q", i, f.Kind)
		}
	}
}

type nopCloser struct{ io.Reader }

func (nopCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopCloser) Close() error                { return nil }
