package config

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// tailscaleCGNAT and tailscaleULA are the ranges Tailscale assigns to nodes.
var (
	tailscaleCGNAT = mustCIDR("100.64.0.0/10")
	tailscaleULA   = mustCIDR("fd7a:115c:a1e0::/48")
)

func mustCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic("bad CIDR " + s + ": " + err.Error())
	}
	return n
}

// IsTailnetHost reports whether rawURL points at something only reachable over
// a tailnet: an address in Tailscale's assigned ranges, or a MagicDNS name.
//
// Hostnames other than *.ts.net are not resolved here. A DNS lookup at startup
// would make the check depend on resolver state and could pass once and fail
// later, which is worse than a clear rule the operator can read off the config.
func IsTailnetHost(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if host == "" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return tailscaleCGNAT.Contains(ip) || tailscaleULA.Contains(ip)
	}
	return strings.HasSuffix(strings.ToLower(strings.TrimSuffix(host, ".")), ".ts.net")
}

// CheckTailnetHomeserver enforces the Matrix frontend's security argument: the
// homeserver is reachable only over Tailscale, so reachability is
// authentication and the path is exempt from the session and approval gates
// (see internal/endpoint/matrix). Nothing else enforced that, so a typo or a
// copied config pointing at a public homeserver would run with the gate bypass
// intact.
//
// allowNonTailnet is the deliberate escape hatch; callers should log loudly
// when it is used.
func CheckTailnetHomeserver(rawURL string, allowNonTailnet bool) error {
	if allowNonTailnet || IsTailnetHost(rawURL) {
		return nil
	}
	return fmt.Errorf(
		"matrix.homeserver_url %q is not a tailnet address: the Matrix path is exempt from the session and approval gates because the homeserver is only reachable over Tailscale, so pointing it at a public host removes the only control left. "+
			"Use a 100.64.0.0/10 address or a *.ts.net name, or set matrix.allow_non_tailnet_homeserver: true if you genuinely mean it",
		rawURL)
}
