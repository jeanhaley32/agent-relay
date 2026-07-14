// Package config loads the relay daemon's configuration from a JSON file.
// JSON (not YAML) keeps the project dependency-free. Secrets (the bot token)
// are never stored in the file — only the name of the env var that holds them.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/disgoorg/snowflake/v2"

	"github.com/jeanhaley32/agent-relay/internal/budget"
)

// Config is the top-level daemon configuration.
type Config struct {
	Telegram  TelegramConfig  `json:"telegram"`
	Discord   DiscordConfig   `json:"discord"`
	Claude    ClaudeConfig    `json:"claude"`
	Budget    BudgetConfig    `json:"budget"`
	Scheduler SchedulerConfig `json:"scheduler"`
}

type SchedulerConfig struct {
	// File is where reminders are persisted (empty ⇒ "schedules.json"). Relative
	// paths are resolved against relayd's working directory.
	File string `json:"file"`
	// TZ is the timezone cron specs are interpreted in (empty ⇒ host local),
	// e.g. "America/New_York".
	TZ string `json:"tz"`
}

// TelegramConfig configures the Telegram frontend.
type TelegramConfig struct {
	TokenEnv      string  `json:"token_env"`      // env var holding the bot token
	Admins        []int64 `json:"admins"`         // ids that may run /handshake (also allowed)
	Allowlist     []int64 `json:"allowlist"`      // permitted sender user ids
	AllowlistFile string  `json:"allowlist_file"` // optional: persist approved ids here
	PollTimeout   int     `json:"poll_timeout"`   // long-poll seconds
}

// DiscordConfig configures the Discord frontend. Discord is fully optional —
// a config with only a "telegram" block (as today) keeps working unchanged.
// See internal/endpoint/discord/DESIGN.md §5.
type DiscordConfig struct {
	// Enabled gates whether relayd starts the Discord frontend at all. An
	// explicit flag rather than "block present with non-empty admins" —
	// per DESIGN.md §5/§9 open question 3 — because an all-zero-value
	// DiscordConfig{} is otherwise indistinguishable from "block omitted"
	// once JSON-unmarshaled. Fail-closed default (false): no config block
	// (or an empty one) means the frontend goroutine never exists, not
	// "started with an empty allowlist that denies everyone" — see
	// DESIGN.md §8's fail-closed-defaults checklist item.
	Enabled bool `json:"enabled"`

	TokenEnv string `json:"token_env"` // env var holding the bot token

	// Admins/Allowlist hold Discord snowflake ids. They're strings in JSON
	// (not numbers) because a snowflake is a 64-bit value and JSON numbers
	// are conventionally parsed as float64 by generic tooling — an operator
	// hand-editing this file could silently lose precision. Discord's own
	// API returns snowflakes as strings for the same reason. See DESIGN.md
	// §5.
	Admins        []string `json:"admins"`         // ids that may run admin commands (also allowed)
	Allowlist     []string `json:"allowlist"`      // permitted sender user ids
	AllowlistFile string   `json:"allowlist_file"` // optional: persist approved ids here

	// AllowGuildMessages/AllowedGuildIDs/RequireMentionInGuild are
	// Discord-specific (no Telegram concept of a guild). Default is the
	// narrowest posture: DM-only, no guild intents requested at all — see
	// internal/endpoint/discord DESIGN.md §2/§3.
	AllowGuildMessages bool     `json:"allow_guild_messages"`
	AllowedGuildIDs    []string `json:"allowed_guild_ids"`

	// RequireMentionInGuildRaw is a pointer so applyDefaults can tell "field
	// omitted from JSON" (nil) apart from "operator explicitly set false"
	// (non-nil, false) — a plain bool's zero value is indistinguishable from
	// an explicit false, which previously meant wiring
	// WithRequireMentionInGuild(cfg.Discord.RequireMentionInGuild) silently
	// flipped the guild policy fail-open (ambient/unmentioned guild chatter
	// relayable) even though DESIGN.md §5 and config.example.json document
	// the default as true. Use RequireMentionInGuild() to read the resolved
	// value.
	RequireMentionInGuildRaw *bool `json:"require_mention_in_guild"`
}

// RequireMentionInGuild resolves the effective value: the configured value
// if the operator set one, else the documented default (true). Call after
// Load (which runs applyDefaults), or directly — both are safe since this
// resolves the default itself rather than depending on applyDefaults having
// mutated anything.
func (d DiscordConfig) RequireMentionInGuild() bool {
	if d.RequireMentionInGuildRaw == nil {
		return true
	}
	return *d.RequireMentionInGuildRaw
}

// AdminIDs parses Discord.Admins as snowflake ids, returning a clear error on
// a malformed entry rather than silently dropping it.
func (d DiscordConfig) AdminIDs() ([]snowflake.ID, error) {
	return parseSnowflakes("discord.admins", d.Admins)
}

// AllowlistIDs parses Discord.Allowlist as snowflake ids.
func (d DiscordConfig) AllowlistIDs() ([]snowflake.ID, error) {
	return parseSnowflakes("discord.allowlist", d.Allowlist)
}

// AllowedGuildSnowflakes parses Discord.AllowedGuildIDs as snowflake ids.
func (d DiscordConfig) AllowedGuildSnowflakes() ([]snowflake.ID, error) {
	return parseSnowflakes("discord.allowed_guild_ids", d.AllowedGuildIDs)
}

func parseSnowflakes(field string, raw []string) ([]snowflake.ID, error) {
	ids := make([]snowflake.ID, 0, len(raw))
	for _, s := range raw {
		id, err := snowflake.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid snowflake %q: %w", field, s, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ClaudeConfig configures the Claude backend.
type ClaudeConfig struct {
	Socket string `json:"socket"` // unix socket the shim connects to
}

// BudgetConfig configures the rate limit / circuit breaker.
type BudgetConfig struct {
	Tier string `json:"tier"` // free|pro|max5|max20

	// ConversationCaps optionally bounds cumulative estimated tokens for a
	// specific conversation (keyed by chat_id, matching relay.Message's
	// ConversationID/Meta["chat_id"]) - independent of and tighter than the
	// global Tier budget. For a specific untrusted or resource-testing
	// contact (e.g. an allowlisted but non-admin user) rather than the
	// whole relay. Once a conversation's cumulative usage reaches its cap,
	// further inbound messages from it are dropped before ever reaching
	// the backend, so no more inference tokens are spent on it at all -
	// see Broker.conversationCapExceeded's doc comment in relay.go.
	ConversationCaps map[string]int64 `json:"conversation_caps,omitempty"`
}

// Defaults applied when fields are omitted.
const (
	DefaultTokenEnv        = "TELEGRAM_BOT_TOKEN"
	DefaultPollTimeout     = 30
	DefaultTier            = "pro"
	DefaultDiscordTokenEnv = "DISCORD_BOT_TOKEN"
)

// defaultSocket prefers the per-user runtime dir ($XDG_RUNTIME_DIR, mode 0700)
// so the IPC socket isn't in world-accessible /tmp; falls back to /tmp when it's
// unset (the socket itself is chmod 0600 either way).
func defaultSocket() string {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "agent-relay.sock")
	}
	return "/tmp/agent-relay.sock"
}

// Load reads and validates a config file, applying defaults.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &c, nil
}

// validate checks the config after defaults are applied and returns a clear
// error rather than letting a mistake surface as confusing downstream behavior.
func (c *Config) validate() error {
	if len(c.Telegram.Admins) == 0 && len(c.Telegram.Allowlist) == 0 {
		return fmt.Errorf("telegram: no admins or allowlist — the bot would serve nobody; add your Telegram user id to \"admins\"")
	}
	for _, id := range c.Telegram.Admins {
		if id <= 0 {
			return fmt.Errorf("telegram.admins: invalid id %d (must be > 0)", id)
		}
	}
	for _, id := range c.Telegram.Allowlist {
		if id <= 0 {
			return fmt.Errorf("telegram.allowlist: invalid id %d (must be > 0)", id)
		}
	}
	if _, ok := budget.DefaultTiers[c.Budget.Tier]; !ok {
		return fmt.Errorf("budget.tier %q is unknown (want one of: free, pro, max5, max20)", c.Budget.Tier)
	}
	if c.Telegram.PollTimeout < 0 {
		return fmt.Errorf("telegram.poll_timeout must be >= 0")
	}
	if c.Claude.Socket == "" {
		return fmt.Errorf("claude.socket must not be empty")
	}
	if c.Discord.Enabled {
		if len(c.Discord.Admins) == 0 && len(c.Discord.Allowlist) == 0 {
			return fmt.Errorf("discord: enabled but no admins or allowlist — the bot would serve nobody; add your Discord user id to \"admins\"")
		}
		if _, err := c.Discord.AdminIDs(); err != nil {
			return err
		}
		if _, err := c.Discord.AllowlistIDs(); err != nil {
			return err
		}
		if _, err := c.Discord.AllowedGuildSnowflakes(); err != nil {
			return err
		}
		// allow_guild_messages=true with an empty allowed_guild_ids is
		// deliberately NOT rejected here: it's a valid (if inert) config —
		// fail-closed means every guild is denied until at least one is
		// listed, not that the combination itself is invalid. See
		// DESIGN.md §5.
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Telegram.TokenEnv == "" {
		c.Telegram.TokenEnv = DefaultTokenEnv
	}
	if c.Telegram.PollTimeout == 0 {
		c.Telegram.PollTimeout = DefaultPollTimeout
	}
	if c.Claude.Socket == "" {
		c.Claude.Socket = defaultSocket()
	}
	if c.Budget.Tier == "" {
		c.Budget.Tier = DefaultTier
	}
	if c.Scheduler.File == "" {
		c.Scheduler.File = "schedules.json"
	}
	if c.Discord.Enabled && c.Discord.TokenEnv == "" {
		c.Discord.TokenEnv = DefaultDiscordTokenEnv
	}
}

// Token resolves the Telegram bot token from the configured env var.
func (c *Config) Token() (string, error) {
	v := os.Getenv(c.Telegram.TokenEnv)
	if v == "" {
		return "", fmt.Errorf("bot token env %s is not set", c.Telegram.TokenEnv)
	}
	return v, nil
}

// DiscordToken resolves the Discord bot token from the configured env var —
// same convention as Token(), extended for the second frontend rather than
// inventing a new one (DESIGN.md §8).
func (c *Config) DiscordToken() (string, error) {
	v := os.Getenv(c.Discord.TokenEnv)
	if v == "" {
		return "", fmt.Errorf("discord bot token env %s is not set", c.Discord.TokenEnv)
	}
	return v, nil
}
