// Package security turns a YAML security profile into the permission
// configuration a Claude Code session launches with.
//
// Two modes:
//
//	mode: full        all tools, no prompts (launch with --dangerously-skip-permissions).
//	                  For a trusted single operator running their own instance.
//	mode: restricted  allow-listed tools run freely; deny-listed tools are hard-blocked;
//	                  anything else prompts — and those prompts are relayed to admins
//	                  (via /allow · /deny). The `reply` tool is always allowed so the
//	                  bot can answer.
//
// The relay's channel MCP server is always enabled so the channel loads without a
// consent prompt.
package security

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Mode is the security posture.
type Mode string

const (
	ModeFull       Mode = "full"
	ModeRestricted Mode = "restricted"
)

const (
	replyTool     = "mcp__relay__reply" // always allowed: the bot must be able to reply
	channelServer = "relay"             // enabled so the channel loads without a prompt
)

// Config is a parsed security.yaml.
type Config struct {
	Mode  Mode     `yaml:"mode"`
	Allow []string `yaml:"allow"` // restricted: auto-approved tools (no prompt)
	Deny  []string `yaml:"deny"`  // restricted: hard-blocked tools (never asked)
}

// Load reads and validates a security profile. A missing/blank mode defaults to
// restricted (fail-safe).
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if c.Mode == "" {
		c.Mode = ModeRestricted
	}
	if c.Mode != ModeFull && c.Mode != ModeRestricted {
		return nil, fmt.Errorf("invalid mode %q (want full|restricted)", c.Mode)
	}
	return &c, nil
}

// SkipPermissions reports whether the session should launch with
// --dangerously-skip-permissions (full mode only).
func (c *Config) SkipPermissions() bool { return c.Mode == ModeFull }

// Settings returns the .claude/settings.json content for the session. Full mode
// only enables the channel server (skip-permissions handles tools). Restricted
// mode also sets allow/deny; unlisted tools prompt (and get relayed to admins).
func (c *Config) Settings() map[string]any {
	s := map[string]any{"enabledMcpjsonServers": []string{channelServer}}
	if c.Mode == ModeFull {
		return s
	}
	seen := map[string]bool{replyTool: true}
	allow := []string{replyTool}
	for _, t := range c.Allow {
		if !seen[t] {
			seen[t] = true
			allow = append(allow, t)
		}
	}
	deny := []string{}
	denied := map[string]bool{}
	for _, t := range c.Deny {
		if !seen[t] && !denied[t] { // never both allow and deny the same tool
			denied[t] = true
			deny = append(deny, t)
		}
	}
	s["permissions"] = map[string]any{"allow": allow, "deny": deny}
	return s
}
