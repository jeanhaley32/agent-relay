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

// TestScheduleRoundTrip: a sched_req frame from the shim surfaces on Schedules,
// and SchedRespond returns a sched_resp the shim receives (correlation echoed).
func TestScheduleRoundTrip(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "s.sock")
	e, err := New(sock)
	if err != nil {
		t.Fatalf("new endpoint: %v", err)
	}
	defer e.Close()

	nc, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()
	shim := ipc.NewConn(nc)

	// Shim calls schedule_message -> sched_req frame.
	if err := shim.Send(ipc.Frame{
		Kind: ipc.KindSchedReq, RequestID: "c1", Op: ipc.OpScheduleCreate,
		Text: "train", Cron: "0 9 * * *", ChatID: "42",
	}); err != nil {
		t.Fatalf("shim send sched_req: %v", err)
	}

	select {
	case req := <-e.Schedules():
		if req.ReqID != "c1" || req.Op != ipc.OpScheduleCreate || req.Text != "train" || req.Cron != "0 9 * * *" {
			t.Fatalf("wrong sched request: %+v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for schedule request")
	}

	if err := e.SchedRespond("c1", "scheduled (id abc) — recurring", ""); err != nil {
		t.Fatalf("sched respond: %v", err)
	}
	resp, err := shim.Recv()
	if err != nil {
		t.Fatalf("shim recv resp: %v", err)
	}
	if resp.Kind != ipc.KindSchedResp || resp.RequestID != "c1" || resp.Err != "" || resp.Result == "" {
		t.Fatalf("wrong sched response: %+v", resp)
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

// TestReadErrorClosesSocket guards the 2026-07-18 outage: when the daemon's
// reader gives up on a connection it must CLOSE it, not just drop its
// reference. Otherwise the socket stays half-open - the shim never sees EOF,
// so its reconnect loop never fires and it keeps writing replies into a socket
// nobody reads. Outbound dies silently while inbound looks perfectly healthy.
func TestReadErrorClosesSocket(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "s.sock")
	e, err := New(sock)
	if err != nil {
		t.Fatalf("new endpoint: %v", err)
	}
	defer e.Close()

	shim, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer shim.Close()

	// Let the daemon accept and start reading.
	if err := ipc.NewConn(shim).Send(ipc.Frame{Kind: ipc.KindReply, ChatID: "1", Text: "hi"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case <-e.Recv():
	case <-time.After(2 * time.Second):
		t.Fatal("daemon never received the first reply")
	}

	// Force the daemon's reader to error by sending garbage (undecodable JSON).
	if _, err := shim.Write([]byte("}{ not json\n")); err != nil {
		t.Fatalf("write garbage: %v", err)
	}

	// The daemon must close its end. The shim should therefore observe EOF
	// rather than hanging forever on a half-open socket.
	_ = shim.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	if _, err := shim.Read(buf); err == nil {
		t.Fatal("shim read succeeded; daemon left the socket half-open (regression)")
	} else if os.IsTimeout(err) {
		t.Fatal("shim read timed out: daemon did not close the socket - half-open leak (regression)")
	}
}

// TestCloseUnblocksReadReplies guards the 2026-07-18 shutdown hang: Close()
// must terminate the LIVE connection, not just the listener. acceptLoop is
// almost always parked inside readReplies on c.Recv(), which a listener close
// does not interrupt - so without this, acceptLoop never returns, its
// `defer close(e.recv)` never runs, the broker's outbound pump blocks forever
// on `range Backend.Recv()`, and Broker.Run hangs in wg.Wait() until systemd
// SIGKILLs it.
func TestCloseUnblocksReadReplies(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "s.sock")
	e, err := New(sock)
	if err != nil {
		t.Fatalf("new endpoint: %v", err)
	}

	// Establish a connection and leave it idle, so acceptLoop is parked inside
	// readReplies blocked on c.Recv() - exactly the real shutdown situation.
	shim, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer shim.Close()
	if err := ipc.NewConn(shim).Send(ipc.Frame{Kind: ipc.KindReply, ChatID: "1", Text: "hi"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case <-e.Recv():
	case <-time.After(2 * time.Second):
		t.Fatal("endpoint never received the reply")
	}

	// Close() must cause Recv() to drain and close, the way Broker.Run's
	// shutdown depends on. If it hangs, shutdown would hang in production.
	_ = e.Close()
	drained := make(chan struct{})
	go func() {
		for range e.Recv() { // ranges until the channel is closed
		}
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("Recv() never closed after Close(): acceptLoop still stuck in readReplies - shutdown would hang (regression)")
	}
}
