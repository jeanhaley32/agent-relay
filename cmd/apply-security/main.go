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
	"strings"

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
	// Merge into the existing settings rather than overwriting the file. This
	// command owns only the keys it emits (enabledMcpjsonServers, permissions);
	// everything else in settings.json belongs to the operator and must
	// survive. Overwriting silently destroyed any registered hooks on EVERY
	// launch, since run.sh calls this just before starting Claude - which is
	// why the reply-drift Stop hook could never stay registered no matter how
	// it was installed (real incident 2026-07-19).
	existing, readErr := os.ReadFile(*out)
	if readErr != nil {
		existing = nil // no file yet - start from empty
	}
	merged, err := mergeSettings(existing, sc.Settings())
	if err != nil {
		// Don't clobber a file we can't parse - the operator's hooks may be in
		// there. Fail loudly instead of silently resetting it.
		fmt.Fprintf(os.Stderr, "apply-security: %s exists but is not valid JSON (%v) - refusing to overwrite\n", *out, err)
		os.Exit(1)
	}
	b, err := json.MarshalIndent(merged, "", "  ")
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
	var flags []string
	if sc.SkipPermissions() {
		flags = append(flags, "--dangerously-skip-permissions")
	}
	// Strip interactive-prompt tools in every mode: a headless relay session has
	// no terminal for a human to answer a modal, so leaving them in freezes it.
	if dt := sc.DisallowedTools(); len(dt) > 0 {
		flags = append(flags, "--disallowedTools")
		flags = append(flags, dt...)
	}
	fmt.Print(strings.Join(flags, " "))
}

// mergeSettings layers this command's owned keys over whatever is already in
// settings.json, instead of replacing the file. Keys it does not emit (hooks
// above all) belong to the operator and must survive: run.sh invokes this
// immediately before launching Claude, so overwriting silently destroyed any
// registered hook on every single launch - which is why the reply-drift Stop
// hook could never stay registered however it was installed. Returns an error
// for unparseable input so the caller can refuse rather than reset the file.
func mergeSettings(existing []byte, own map[string]any) (map[string]any, error) {
	merged := map[string]any{}
	if len(existing) > 0 {
		if err := json.Unmarshal(existing, &merged); err != nil {
			return nil, err
		}
	}
	for k, v := range own {
		merged[k] = v // this command's keys win; operator keys are preserved
	}
	return merged, nil
}
