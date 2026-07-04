// Package config loads the relay daemon's configuration from a JSON file.
// JSON (not YAML) keeps the project dependency-free. Secrets (the bot token)
// are never stored in the file — only the name of the env var that holds them.
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config is the top-level daemon configuration.
type Config struct {
	Telegram TelegramConfig `json:"telegram"`
	Claude   ClaudeConfig   `json:"claude"`
	Budget   BudgetConfig   `json:"budget"`
}

// TelegramConfig configures the Telegram frontend.
type TelegramConfig struct {
	TokenEnv    string  `json:"token_env"`   // env var holding the bot token
	Allowlist   []int64 `json:"allowlist"`   // permitted sender user ids
	PollTimeout int     `json:"poll_timeout"` // long-poll seconds
}

// ClaudeConfig configures the Claude backend.
type ClaudeConfig struct {
	Socket string `json:"socket"` // unix socket the shim connects to
}

// BudgetConfig configures the rate limit / circuit breaker.
type BudgetConfig struct {
	Tier string `json:"tier"` // free|pro|max5|max20
}

// Defaults applied when fields are omitted.
const (
	DefaultTokenEnv    = "TELEGRAM_BOT_TOKEN"
	DefaultPollTimeout = 30
	DefaultTier        = "pro"
	DefaultSocket      = "/tmp/agent-relay.sock"
)

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
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Telegram.TokenEnv == "" {
		c.Telegram.TokenEnv = DefaultTokenEnv
	}
	if c.Telegram.PollTimeout == 0 {
		c.Telegram.PollTimeout = DefaultPollTimeout
	}
	if c.Claude.Socket == "" {
		c.Claude.Socket = DefaultSocket
	}
	if c.Budget.Tier == "" {
		c.Budget.Tier = DefaultTier
	}
}

// Token resolves the bot token from the configured env var.
func (c *Config) Token() (string, error) {
	v := os.Getenv(c.Telegram.TokenEnv)
	if v == "" {
		return "", fmt.Errorf("bot token env %s is not set", c.Telegram.TokenEnv)
	}
	return v, nil
}
