package claude

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/ipc"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// TestEndpointBridge exercises the daemon-side endpoint against a fake shim over
// a real unix socket: Send should emit an inject frame; a reply frame from the
// shim should surface on Recv. No live Claude needed.
func TestEndpointBridge(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "s.sock")
	e, err := New(sock)
	if err != nil {
		t.Fatalf("new endpoint: %v", err)
	}
	defer e.Close()

	// The socket must be owner-only (0600) so other local users can't attach.
	if fi, err := os.Stat(sock); err != nil {
		t.Fatalf("stat socket: %v", err)
	} else if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket perms = %o, want 600", perm)
	}

	// Inbound buffering: a message sent BEFORE any shim connects is buffered
	// (Send never errors) and delivered once the shim connects.
	inject := relay.Message{ConversationID: "42", Role: relay.User, Text: "ping",
		Meta: map[string]string{"chat_id": "42"}}
	if err := e.Send(context.Background(), inject); err != nil {
		t.Fatalf("Send should buffer, not error, before connect: %v", err)
	}

	// Fake shim connects.
	nc, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()
	shim := ipc.NewConn(nc)

	// The buffered inject frame flushes to the shim on connect.
	f, err := shim.Recv()
	if err != nil {
		t.Fatalf("shim recv: %v", err)
	}
	if f.Kind != ipc.KindInject || f.ChatID != "42" || f.Text != "ping" {
		t.Fatalf("wrong inject frame: %+v", f)
	}

	// Shim sends a reply -> endpoint surfaces it on Recv.
	if err := shim.Send(ipc.Frame{Kind: ipc.KindReply, ChatID: "42", Text: "pong"}); err != nil {
		t.Fatalf("shim send reply: %v", err)
	}
	select {
	case m := <-e.Recv():
		if m.Role != relay.Assistant || m.ConversationID != "42" || m.Text != "pong" {
			t.Fatalf("wrong reply message: %+v", m)
		}
		if m.Meta["chat_id"] != "42" {
			t.Fatalf("missing chat_id meta: %+v", m.Meta)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for reply on Recv")
	}

	// Permission relay: shim forwards a tool request -> surfaces on Permissions;
	// Decide -> shim receives a verdict frame.
	if err := shim.Send(ipc.Frame{Kind: ipc.KindPermRequest, RequestID: "abcde", Tool: "Bash", Detail: "run ls"}); err != nil {
		t.Fatalf("shim send perm request: %v", err)
	}
	select {
	case p := <-e.Permissions():
		if p.ID != "abcde" || p.Tool != "Bash" {
			t.Fatalf("wrong perm request: %+v", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for permission request")
	}

	if err := e.Decide("abcde", true); err != nil {
		t.Fatalf("decide: %v", err)
	}
	v, err := shim.Recv()
	if err != nil {
		t.Fatalf("shim recv verdict: %v", err)
	}
	if v.Kind != ipc.KindPermVerdict || v.RequestID != "abcde" || !v.Allow {
		t.Fatalf("wrong verdict frame: %+v", v)
	}
}

// TestInboundBufferAcrossReconnect reproduces the live failure: a message sent
// while the shim is momentarily disconnected must not be dropped — it is
// buffered and delivered when a new shim connects.
func TestInboundBufferAcrossReconnect(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "s.sock")
	e, err := New(sock)
	if err != nil {
		t.Fatalf("new endpoint: %v", err)
	}
	defer e.Close()

	// First shim connects and receives a message.
	nc1, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial 1: %v", err)
	}
	shim1 := ipc.NewConn(nc1)
	if err := e.Send(context.Background(), relay.Message{Text: "one", Meta: map[string]string{"chat_id": "1"}}); err != nil {
		t.Fatalf("send one: %v", err)
	}
	if f, err := shim1.Recv(); err != nil || f.Text != "one" {
		t.Fatalf("first delivery: %+v (err %v)", f, err)
	}

	// Shim drops. Give the endpoint a moment to notice and clear the connection.
	nc1.Close()
	time.Sleep(100 * time.Millisecond)

	// Message sent during the disconnect window: buffered, not dropped.
	if err := e.Send(context.Background(), relay.Message{Text: "two", Meta: map[string]string{"chat_id": "1"}}); err != nil {
		t.Fatalf("send two: %v", err)
	}

	// A new shim reconnects — the buffered message flushes to it.
	nc2, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer nc2.Close()
	shim2 := ipc.NewConn(nc2)
	f, err := shim2.Recv()
	if err != nil {
		t.Fatalf("shim2 recv: %v", err)
	}
	if f.Kind != ipc.KindInject || f.Text != "two" {
		t.Fatalf("buffered message not delivered on reconnect: %+v", f)
	}
}
