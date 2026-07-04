// Command apply-security reads a security profile (security.yaml) and configures
// the Claude Code session: it writes .claude/settings.json with the resulting
// permissions, and prints any extra launch flags to stdout for the launcher to
// pass to `claude` (e.g. --dangerously-skip-permissions in full mode).
//
//	FLAGS=$(apply-security --config security.yaml)
//	claude $FLAGS --dangerously-load-development-channels server:relay
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jeanhaley32/agent-relay/internal/security"
)

func main() {
	cfg := flag.String("config", "security.yaml", "path to the security profile")
	out := flag.String("settings", ".claude/settings.json", "settings file to generate")
	flag.Parse()

	sc, err := security.Load(*cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "apply-security:", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "apply-security:", err)
		os.Exit(1)
	}
	b, err := json.MarshalIndent(sc.Settings(), "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "apply-security:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "apply-security:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "apply-security: mode=%s → wrote %s\n", sc.Mode, *out)

	// Launch flags for the launcher to consume on stdout.
	if sc.SkipPermissions() {
		fmt.Print("--dangerously-skip-permissions")
	}
}
