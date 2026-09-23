package matrix

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestRunDoesNotSyncUntilPrimed is the regression for the replay bug: an
// unprimed sync omits `since`, the homeserver answers with each room's recent
// timeline, and those events are delivered as fresh admin commands. Matrix is
// the gate-bypass path, so they would run unchallenged.
//
// The fake fails the priming sync once, then succeeds. No /sync request may
// carry an empty `since` at any point.
func TestRunDoesNotSyncUntilPrimed(t *testing.T) {
	var primeCalls, unprimedSyncs atomic.Int64
	delivered := make(chan struct{}, 8)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/sync") {
			http.NotFound(w, r)
			return
		}
		since := r.URL.Query().Get("since")
		timeout := r.URL.Query().Get("timeout")

		// The priming call is the one made with no `since` AND no long-poll
		// timeout; the loop's calls always long-poll.
		if since == "" && timeout == "0" {
			if primeCalls.Add(1) == 1 {
				http.Error(w, "homeserver still starting", http.StatusBadGateway)
				return
			}
			_, _ = io.WriteString(w, `{"next_batch":"s1"}`)
			return
		}

		if since == "" {
			// A loop sync with no token: the bug. Answer the way a real
			// homeserver would, with history, so a regression delivers.
			unprimedSyncs.Add(1)
			_, _ = io.WriteString(w, `{"next_batch":"s2","rooms":{"join":{"!r:example.org":{"timeline":{"events":[
				{"type":"m.room.message","sender":"@admin:example.org","event_id":"$1","origin_server_ts":1,
				 "content":{"msgtype":"m.text","body":"restart the deploy"}}]}}}}}`)
			return
		}
		_, _ = io.WriteString(w, `{"next_batch":"s3"}`)
	}))
	defer srv.Close()

	f := New(srv.URL, "tok", []string{"@admin:example.org"}, "", log.New(io.Discard, "", 0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for range f.Recv() {
			delivered <- struct{}{}
		}
	}()
	go f.Run(ctx, "@bot:example.org")

	// Long enough for the failed prime, its backoff, the retry, and several
	// loop syncs.
	time.Sleep(1500 * time.Millisecond)
	cancel()
	_ = f.Close()

	if n := unprimedSyncs.Load(); n != 0 {
		t.Fatalf("%d sync(s) ran without a since token — history would be replayed as commands", n)
	}
	if n := primeCalls.Load(); n < 2 {
		t.Fatalf("priming was attempted %d time(s), want a retry after the failure", n)
	}
	select {
	case <-delivered:
		t.Fatal("a historical message was delivered as a live command")
	default:
	}
}
