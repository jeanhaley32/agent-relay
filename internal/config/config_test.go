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
		// An empty telegram block no longer implies "you meant Telegram":
		// Telegram is opt-in now, so the honest answer is that nothing is
		// enabled. The telegram-specific error is still produced when the
		// block shows intent — the next two cases.
		{"empty telegram block, nothing else on", `{"telegram":{}}`, "no frontend is enabled"},
		{"telegram explicitly on with no admins", `{"telegram":{"enabled":true}}`, "serve nobody"},
		{"telegram off, another frontend on", `{"telegram":{"enabled":false},"matrix":{"enabled":true,"homeserver_url":"http://100.64.0.1:8008","admins":["@a:b"]}}`, ""},
		{"telegram off and nothing else on", `{"telegram":{"enabled":false}}`, "no frontend is enabled"},
		{"unknown tier", `{"telegram":{"admins":[1]},"budget":{"tier":"mega"}}`, "unknown"},
		{"negative admin id", `{"telegram":{"admins":[-1]}}`, "invalid id"},
		{"zero allowlist id", `{"telegram":{"allowlist":[0]}}`, "invalid id"},
		{"negative poll timeout", `{"telegram":{"admins":[1],"poll_timeout":-1}}`, "poll_timeout"},
		{"valid", `{"telegram":{"admins":[1]},"budget":{"tier":"max5"}}`, ""},
		{"discord enabled with no admins or allowlist", `{"telegram":{"admins":[1]},"discord":{"enabled":true}}`, "discord: enabled but no admins or allowlist"},
		{"discord malformed admin snowflake", `{"telegram":{"admins":[1]},"discord":{"enabled":true,"admins":["not-a-snowflake"]}}`, "invalid snowflake"},
		{"discord malformed allowlist snowflake", `{"telegram":{"admins":[1]},"discord":{"enabled":true,"allowlist":["not-a-snowflake"]}}`, "invalid snowflake"},
		{"discord malformed guild snowflake", `{"telegram":{"admins":[1]},"discord":{"enabled":true,"admins":["123"],"allowed_guild_ids":["not-a-snowflake"]}}`, "invalid snowflake"},
		{"discord valid with admins", `{"telegram":{"admins":[1]},"discord":{"enabled":true,"admins":["123456789012345678"]}}`, ""},
		{"discord disabled with malformed ids not validated", `{"telegram":{"admins":[1]},"discord":{"enabled":false,"admins":["not-a-snowflake"]}}`, ""},
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
	if c.Telegram.PollTimeout() != DefaultPollTimeout {
		t.Fatalf("poll timeout default: got %d", c.Telegram.PollTimeout())
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

func TestRequireMentionInGuildDefault(t *testing.T) {
	cases := []struct {
		name string
		raw  *bool
		want bool
	}{
		{"omitted defaults to true (fail-closed)", nil, true},
		{"explicit false is honored", boolPtr(false), false},
		{"explicit true is honored", boolPtr(true), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := DiscordConfig{RequireMentionInGuildRaw: tc.raw}
			if got := d.RequireMentionInGuild(); got != tc.want {
				t.Fatalf("RequireMentionInGuild() = %v, want %v", got, tc.want)
			}
		})
	}
}

func boolPtr(b bool) *bool { return &b }

func TestDiscordToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"telegram": {"admins": [1]},
		"discord": {"enabled": true, "admins": ["123456789012345678"], "token_env": "MY_DISCORD_TOKEN"}
	}`), 0o600)

	c, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Discord.TokenEnv != "MY_DISCORD_TOKEN" {
		t.Fatalf("token_env: got %q", c.Discord.TokenEnv)
	}
	if _, err := c.DiscordToken(); err == nil {
		t.Fatal("expected error when discord token env unset")
	}
	t.Setenv("MY_DISCORD_TOKEN", "dsecret")
	tok, err := c.DiscordToken()
	if err != nil || tok != "dsecret" {
		t.Fatalf("discord token: got %q err=%v", tok, err)
	}

	ids, err := c.Discord.AdminIDs()
	if err != nil || len(ids) != 1 {
		t.Fatalf("admin ids: got %v err=%v", ids, err)
	}
}

func TestDiscordDefaultTokenEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	os.WriteFile(path, []byte(`{
		"telegram": {"admins": [1]},
		"discord": {"enabled": true, "admins": ["123456789012345678"]}
	}`), 0o600)

	c, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Discord.TokenEnv != DefaultDiscordTokenEnv {
		t.Fatalf("discord token_env default: got %q, want %q", c.Discord.TokenEnv, DefaultDiscordTokenEnv)
	}
}

func TestStatePath(t *testing.T) {
	// Empty StateDir must keep resolving to the working directory: these files
	// used to hang off filepath.Dir(telegram.allowlist_file), which for the
	// common "allowlist.json" value was ".". Existing deployments must not have
	// their state silently move.
	var c Config
	if got := c.StatePath("adminbind.json"); got != "adminbind.json" {
		t.Fatalf("empty StateDir: got %q, want %q", got, "adminbind.json")
	}

	c.StateDir = "/var/lib/relayd"
	if got := c.StatePath("contacts.json"); got != "/var/lib/relayd/contacts.json" {
		t.Fatalf("set StateDir: got %q", got)
	}

	c.StateDir = "state"
	if got := c.StatePath("contacts.json"); got != "state/contacts.json" {
		t.Fatalf("relative StateDir: got %q", got)
	}
}

// poll_timeout: 0 used to be indistinguishable from "key omitted", so an
// operator choosing short polling silently got 30s long-polling.
func TestPollTimeoutDistinguishesExplicitZero(t *testing.T) {
	var omitted TelegramConfig
	if got := omitted.PollTimeout(); got != DefaultPollTimeout {
		t.Fatalf("omitted: got %d, want the default %d", got, DefaultPollTimeout)
	}

	zero := 0
	explicit := TelegramConfig{PollTimeoutRaw: &zero}
	if got := explicit.PollTimeout(); got != 0 {
		t.Fatalf("explicit 0: got %d, want 0 — the operator's choice was overridden", got)
	}

	five := 5
	set := TelegramConfig{PollTimeoutRaw: &five}
	if got := set.PollTimeout(); got != 5 {
		t.Fatalf("explicit 5: got %d, want 5", got)
	}
}

// And it must survive an actual JSON round trip, which is where the
// omitted-vs-zero distinction is really made.
func TestPollTimeoutFromJSON(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		return p
	}
	base := `"admins":[1],"token_env":"T"`

	omitted := write("a.json", `{"telegram":{`+base+`},"claude":{"socket":"/tmp/s"}}`)
	c, err := Load(omitted)
	if err != nil {
		t.Fatalf("load omitted: %v", err)
	}
	if got := c.Telegram.PollTimeout(); got != DefaultPollTimeout {
		t.Fatalf("omitted from JSON: got %d, want %d", got, DefaultPollTimeout)
	}

	explicit := write("b.json", `{"telegram":{`+base+`,"poll_timeout":0},"claude":{"socket":"/tmp/s"}}`)
	c2, err := Load(explicit)
	if err != nil {
		t.Fatalf("load explicit: %v", err)
	}
	if got := c2.Telegram.PollTimeout(); got != 0 {
		t.Fatalf("explicit 0 in JSON: got %d, want 0", got)
	}
}
