package tailnet

import (
	"errors"
	"testing"
	"time"
)

const sampleStatus = `{
  "Peer": {
    "n1": {"HostName": "jean-phone", "DNSName": "jean-phone.tailnet.ts.net.", "Online": true, "CurAddr": "192.168.8.5:41641", "Relay": ""},
    "n2": {"HostName": "jean-laptop-away", "DNSName": "jean-laptop-away.tailnet.ts.net.", "Online": true, "CurAddr": "", "Relay": "nyc"},
    "n3": {"HostName": "jean-offline", "DNSName": "jean-offline.tailnet.ts.net.", "Online": false, "CurAddr": "", "Relay": ""}
  }
}`

func TestPeerDirectVsRelayed(t *testing.T) {
	c := New(time.Minute)
	c.run = func() ([]byte, error) { return []byte(sampleStatus), nil }

	p, ok := c.Peer("jean-phone")
	if !ok || !p.Direct() {
		t.Fatalf("jean-phone should be online+direct, got %+v ok=%v", p, ok)
	}

	p, ok = c.Peer("jean-laptop-away")
	if !ok || p.Direct() || !p.Online {
		t.Fatalf("jean-laptop-away should be online but relayed (not direct), got %+v ok=%v", p, ok)
	}

	p, ok = c.Peer("jean-offline")
	if !ok || p.Online {
		t.Fatalf("jean-offline should be known but offline, got %+v ok=%v", p, ok)
	}

	_, ok = c.Peer("never-seen")
	if ok {
		t.Fatal("unknown device should report ok=false")
	}
}

func TestCacheTTL(t *testing.T) {
	calls := 0
	c := New(50 * time.Millisecond)
	c.run = func() ([]byte, error) {
		calls++
		return []byte(sampleStatus), nil
	}

	c.Peer("jean-phone")
	c.Peer("jean-phone")
	if calls != 1 {
		t.Fatalf("expected 1 exec call within TTL, got %d", calls)
	}

	time.Sleep(60 * time.Millisecond)
	c.Peer("jean-phone")
	if calls != 2 {
		t.Fatalf("expected a refetch after TTL expiry, got %d calls", calls)
	}
}

func TestExecErrorFailsClosedWithNoStaleCache(t *testing.T) {
	c := New(time.Minute)
	c.run = func() ([]byte, error) { return nil, errors.New("tailscale not running") }

	_, ok := c.Peer("jean-phone")
	if ok {
		t.Fatal("exec error with no prior cache should report every peer as unknown (ok=false)")
	}
}

func TestExecErrorFallsBackToLastKnownGood(t *testing.T) {
	c := New(10 * time.Millisecond)
	good := true
	c.run = func() ([]byte, error) {
		if good {
			return []byte(sampleStatus), nil
		}
		return nil, errors.New("transient failure")
	}

	p, ok := c.Peer("jean-phone")
	if !ok || !p.Online {
		t.Fatalf("expected initial good fetch, got %+v ok=%v", p, ok)
	}

	good = false
	time.Sleep(15 * time.Millisecond)
	p, ok = c.Peer("jean-phone")
	if !ok || !p.Online {
		t.Fatalf("expected stale-but-known result on exec failure, got %+v ok=%v", p, ok)
	}
}

// A dead tailscaled must not leave the admin presence gate open. The gate
// reads Online from this snapshot, so serving a cached Online:true forever
// would mean taking the bound device off the tailnet — the documented
// killswitch — no longer revokes anything.
func TestStaleSnapshotIsDroppedAfterStalenessBound(t *testing.T) {
	now := time.Now()
	calls := 0
	c := New(5 * time.Second)
	c.now = func() time.Time { return now }
	c.run = func() ([]byte, error) {
		calls++
		if calls == 1 {
			return []byte(`{"Peer":{"k1":{"HostName":"laptop","Online":true}}}`), nil
		}
		return nil, errors.New("tailscaled is not running")
	}

	if p, ok := c.Peer("laptop"); !ok || !p.Online {
		t.Fatalf("first fetch: got %+v ok=%v, want online", p, ok)
	}

	// Inside the bound: the cache still stands in, so a brief CLI blip does
	// not flap the gate.
	now = now.Add(10 * time.Second)
	if p, ok := c.Peer("laptop"); !ok || !p.Online {
		t.Fatalf("within staleness bound: got %+v ok=%v, want the cached entry", p, ok)
	}

	// Past it: the cache is discarded and the peer reads as offline.
	now = now.Add(10 * time.Second)
	if p, ok := c.Peer("laptop"); ok || p.Online {
		t.Fatalf("past staleness bound: got %+v ok=%v, want offline", p, ok)
	}
}

func TestUnparsableOutputAlsoExpires(t *testing.T) {
	now := time.Now()
	calls := 0
	c := New(time.Second)
	c.now = func() time.Time { return now }
	c.run = func() ([]byte, error) {
		calls++
		if calls == 1 {
			return []byte(`{"Peer":{"k1":{"HostName":"laptop","Online":true}}}`), nil
		}
		return []byte("not json"), nil
	}
	if _, ok := c.Peer("laptop"); !ok {
		t.Fatal("first fetch should populate the cache")
	}
	now = now.Add(10 * time.Second)
	if p, ok := c.Peer("laptop"); ok || p.Online {
		t.Fatalf("garbage output past the bound: got %+v ok=%v, want offline", p, ok)
	}
}
