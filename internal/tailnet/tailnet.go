// Package tailnet reads this host's own Tailscale peer status (via
// `tailscale status --json`) to answer "is a given device currently on the
// tailnet, and is it a direct LAN connection or relayed (away)?" — the
// signal an admin-trust gate can use instead of adding re-auth friction.
//
// Status is cached with a short TTL so a burst of gated commands doesn't
// shell out repeatedly; a stale cache just means a device that dropped
// offline a few seconds ago is still treated as present, which is the safe
// direction to be wrong in for a read-mostly cache (worst case is a short
// window where a just-disconnected killswitch hasn't taken effect yet).
package tailnet

import (
	"encoding/json"
	"os/exec"
	"sync"
	"time"
)

// Peer is one tailnet device's live state, as reported by this host's own
// `tailscale status`.
type Peer struct {
	Online bool   `json:"online"`
	Relay  string `json:"relay"` // non-empty ⇒ connection is DERP-relayed, not direct LAN
}

// Direct reports whether this peer is connected via a direct (LAN or P2P)
// path rather than relayed through a DERP server - the stronger "physically
// on the network" signal.
func (p Peer) Direct() bool { return p.Online && p.Relay == "" }

// statusJSON mirrors the subset of `tailscale status --json` output we need.
type statusJSON struct {
	Peer map[string]struct {
		HostName string `json:"HostName"`
		DNSName  string `json:"DNSName"`
		Online   bool   `json:"Online"`
		CurAddr  string `json:"CurAddr"` // non-empty ⇒ direct connection
		Relay    string `json:"Relay"`   // DERP region if relayed
		ExitNode bool   `json:"ExitNode"`
	} `json:"Peer"`
}

// Checker caches parsed tailnet status for TTL, keyed by hostname (case
// as reported by `tailscale status`).
type Checker struct {
	mu      sync.Mutex
	ttl     time.Duration
	cached  map[string]Peer
	fetched time.Time
	// run is overridden in tests; nil ⇒ actually exec `tailscale status --json`.
	run func() ([]byte, error)
}

// New builds a Checker with the given cache TTL. ttl <= 0 defaults to 5s.
func New(ttl time.Duration) *Checker {
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	return &Checker{ttl: ttl}
}

// Peer returns the named device's current state. hostname matches
// Tailscale's own HostName/DNSName (case-insensitive prefix on DNSName is
// NOT done here — pass the exact hostname as configured). ok=false means
// the device is unknown to this tailnet (never seen, or hostname mismatch)
// — callers should treat unknown the same as offline for a fail-closed gate.
func (c *Checker) Peer(hostname string) (Peer, bool) {
	m := c.snapshot()
	p, ok := m[hostname]
	return p, ok
}

func (c *Checker) snapshot() map[string]Peer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cached != nil && time.Since(c.fetched) < c.ttl {
		return c.cached
	}
	out, err := c.exec()
	if err != nil {
		// Fail closed: on a read error, return the last-known snapshot if we
		// have one (better than nothing), else an empty map (every lookup
		// then reports ok=false, which callers treat as offline).
		if c.cached != nil {
			return c.cached
		}
		return map[string]Peer{}
	}
	var sj statusJSON
	if err := json.Unmarshal(out, &sj); err != nil {
		if c.cached != nil {
			return c.cached
		}
		return map[string]Peer{}
	}
	m := make(map[string]Peer, len(sj.Peer))
	for _, p := range sj.Peer {
		peer := Peer{Online: p.Online, Relay: p.Relay}
		if p.HostName != "" {
			m[p.HostName] = peer
		}
		if p.DNSName != "" {
			m[p.DNSName] = peer
		}
	}
	c.cached = m
	c.fetched = time.Now()
	return m
}

func (c *Checker) exec() ([]byte, error) {
	if c.run != nil {
		return c.run()
	}
	return exec.Command("tailscale", "status", "--json").Output()
}
