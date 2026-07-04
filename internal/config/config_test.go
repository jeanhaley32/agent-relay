package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// loadErr writes body to a temp config and returns the Load error (nil if valid).
func loadErr(t *testing.T, body string) error {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(p)
	return err
}

func TestValidation(t *testing.T) {
	cases := []struct {
		name, body, wantErr string
	}{
		{"no admins or allowlist", `{"telegram":{}}`, "serve nobody"},
		{"unknown tier", `{"telegram":{"admins":[1]},"budget":{"tier":"mega"}}`, "unknown"},
		{"negative admin id", `{"telegram":{"admins":[-1]}}`, "invalid id"},
		{"zero allowlist id", `{"telegram":{"allowlist":[0]}}`, "invalid id"},
		{"negative poll timeout", `{"telegram":{"admins":[1],"poll_timeout":-1}}`, "poll_timeout"},
		{"valid", `{"telegram":{"admins":[1]},"budget":{"tier":"max5"}}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := loadErr(t, tc.body)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected valid, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestLoadAndDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	// Omit poll_timeout, tier, socket to exercise defaults.
	os.WriteFile(path, []byte(`{
		"telegram": {"token_env": "MY_BOT_TOKEN", "allowlist": [111, 222]}
	}`), 0o600)

	c, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Telegram.PollTimeout != DefaultPollTimeout {
		t.Fatalf("poll timeout default: got %d", c.Telegram.PollTimeout)
	}
	if c.Claude.Socket != defaultSocket() {
		t.Fatalf("socket default: got %q, want %q", c.Claude.Socket, defaultSocket())
	}
	if c.Budget.Tier != DefaultTier {
		t.Fatalf("tier default: got %q", c.Budget.Tier)
	}
	if len(c.Telegram.Allowlist) != 2 || c.Telegram.Allowlist[0] != 111 {
		t.Fatalf("allowlist: got %v", c.Telegram.Allowlist)
	}

	// Token resolves from the named env var.
	if _, err := c.Token(); err == nil {
		t.Fatal("expected error when token env unset")
	}
	t.Setenv("MY_BOT_TOKEN", "secret123")
	tok, err := c.Token()
	if err != nil || tok != "secret123" {
		t.Fatalf("token: got %q err=%v", tok, err)
	}
}
