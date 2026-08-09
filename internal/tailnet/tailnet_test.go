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
