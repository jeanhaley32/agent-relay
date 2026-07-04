package claude

import (
	"context"
	"net"
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

	// Before any shim connects, Send fails cleanly.
	if err := e.Send(context.Background(), relay.UserMsg("1", "hi")); err != ErrNoSession {
		t.Fatalf("expected ErrNoSession before connect, got %v", err)
	}

	// Fake shim connects.
	nc, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()
	shim := ipc.NewConn(nc)

	// Send retries until the endpoint has registered the shim. Failed attempts
	// return ErrNoSession WITHOUT sending, so exactly one inject frame is sent.
	inject := relay.Message{ConversationID: "42", Role: relay.User, Text: "ping",
		Meta: map[string]string{"chat_id": "42"}}
	var sendErr error
	for i := 0; i < 100; i++ {
		if sendErr = e.Send(context.Background(), inject); sendErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sendErr != nil {
		t.Fatalf("send never succeeded: %v", sendErr)
	}

	// The shim should receive exactly that inject frame.
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
