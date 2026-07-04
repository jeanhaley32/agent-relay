package ipc

import (
	"net"
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
