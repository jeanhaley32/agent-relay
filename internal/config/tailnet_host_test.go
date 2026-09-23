package config

import "testing"

func TestIsTailnetHost(t *testing.T) {
	tests := []struct {
		url  string
		want bool
	}{
		// Tailscale's assigned ranges.
		{"http://100.99.212.119:8008", true},
		{"https://100.64.0.1", true},
		{"https://100.127.255.254:8448", true},
		{"http://[fd7a:115c:a1e0::1]:8008", true},
		// MagicDNS, with and without a trailing dot.
		{"https://host.example.ts.net:8448", true},
		{"https://HOST.EXAMPLE.TS.NET:8448", true},
		{"https://host.example.ts.net.:8448", true},

		// Public hosts — the case this check exists for.
		{"https://matrix.org", false},
		{"https://matrix.example.com:8448", false},
		// Just outside CGNAT (100.64.0.0/10 ends at 100.127.255.255).
		{"http://100.128.0.1:8008", false},
		{"http://100.63.255.255:8008", false},
		// Private but not tailnet: reachable from the LAN, which is the point.
		{"http://192.168.1.10:8008", false},
		{"http://10.0.0.5:8008", false},
		{"http://127.0.0.1:8008", false},
		// Not a name ending in .ts.net, just containing it.
		{"https://ts.net.evil.example.com", false},
		{"", false},
		{"://nonsense", false},
	}
	for _, tc := range tests {
		if got := IsTailnetHost(tc.url); got != tc.want {
			t.Errorf("IsTailnetHost(%q) = %v, want %v", tc.url, got, tc.want)
		}
	}
}

func TestCheckTailnetHomeserver(t *testing.T) {
	if err := CheckTailnetHomeserver("http://100.99.212.119:8008", false); err != nil {
		t.Fatalf("tailnet address rejected: %v", err)
	}
	err := CheckTailnetHomeserver("https://matrix.org", false)
	if err == nil {
		t.Fatal("a public homeserver was accepted")
	}
	// The error has to explain the consequence, not just the rule — this is
	// the one control left on a gate-exempt path.
	if !contains(err.Error(), "exempt from the session and approval gates") {
		t.Fatalf("error should say why it matters, got: %v", err)
	}
	if err := CheckTailnetHomeserver("https://matrix.org", true); err != nil {
		t.Fatalf("escape hatch did not work: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
