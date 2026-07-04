package config

import (
	"os"
	"path/filepath"
	"testing"
)

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
	if c.Claude.Socket != DefaultSocket {
		t.Fatalf("socket default: got %q", c.Claude.Socket)
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
