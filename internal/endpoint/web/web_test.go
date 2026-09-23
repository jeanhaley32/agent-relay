package web

import (
	"net/http"
	"testing"
)

// clientIP decides which address `tailscale whois` is asked about, and that
// answer decides whether the caller is treated as the tailnet owner — i.e. a
// relay admin. A caller who can set that value can claim to be the owner, so
// these cases are the authorization boundary, not a formatting detail.
func TestClientIPTrustsForwardedHeadersOnlyFromLoopback(t *testing.T) {
	const ownerIP = "100.64.0.1"

	tests := []struct {
		name       string
		remoteAddr string
		realIP     string
		forwardFor string
		want       string
	}{
		{
			name:       "direct caller cannot claim another address",
			remoteAddr: "100.64.0.9:44444",
			realIP:     ownerIP,
			want:       "100.64.0.9",
		},
		{
			name:       "direct caller cannot claim via X-Forwarded-For",
			remoteAddr: "203.0.113.7:44444",
			forwardFor: ownerIP + ", 10.0.0.1",
			want:       "203.0.113.7",
		},
		{
			name:       "loopback proxy may set X-Real-IP",
			remoteAddr: "127.0.0.1:51000",
			realIP:     ownerIP,
			want:       ownerIP,
		},
		{
			name:       "loopback proxy may set X-Forwarded-For",
			remoteAddr: "127.0.0.1:51000",
			forwardFor: ownerIP + ", 10.0.0.1",
			want:       ownerIP,
		},
		{
			name:       "IPv6 loopback counts as loopback",
			remoteAddr: "[::1]:51000",
			realIP:     ownerIP,
			want:       ownerIP,
		},
		{
			name:       "loopback with no headers falls back to itself",
			remoteAddr: "127.0.0.1:51000",
			want:       "127.0.0.1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &http.Request{RemoteAddr: tc.remoteAddr, Header: http.Header{}}
			if tc.realIP != "" {
				r.Header.Set("X-Real-IP", tc.realIP)
			}
			if tc.forwardFor != "" {
				r.Header.Set("X-Forwarded-For", tc.forwardFor)
			}
			if got := clientIP(r); got != tc.want {
				t.Fatalf("clientIP() = %q, want %q", got, tc.want)
			}
		})
	}
}
