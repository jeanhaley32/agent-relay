package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Commit b5ab29a made Telegram opt-in, but main resolved the Telegram bot
// token before checking whether Telegram was wanted, so a Discord-, Matrix- or
// web-only deployment died at startup with "bot token env TELEGRAM_BOT_TOKEN
// is not set". The ordering is invisible to any unit test of config or of a
// frontend, so this one runs the real binary.
func TestStartsWithTelegramDisabledAndNoToken(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the relayd binary")
	}

	dir := t.TempDir()
	bin := filepath.Join(dir, "relayd")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	// Matrix is enabled and pointed at an unroutable tailnet address, so the
	// process gets past startup and then sits in its connect retry loop. That
	// is the success condition: reaching the retry loop means it never asked
	// for a Telegram token.
	cfgPath := filepath.Join(dir, "config.json")
	cfg := `{
		"telegram": {"enabled": false},
		"matrix": {"enabled": true, "homeserver_url": "http://100.64.0.99:8008",
		           "admins": ["@someone:example"], "token_env": "TEST_MATRIX_TOKEN"},
		"claude": {"socket": "` + filepath.Join(dir, "relay.sock") + `"},
		"state_dir": "` + dir + `"
	}`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--config", cfgPath)
	cmd.Env = append(envWithout(os.Environ(), "TELEGRAM_BOT_TOKEN"), "TEST_MATRIX_TOKEN=dummy")
	out, _ := cmd.CombinedOutput()

	got := string(out)
	if strings.Contains(got, "bot token env") {
		t.Fatalf("relayd demanded a Telegram token while Telegram was disabled:\n%s", got)
	}
	if !strings.Contains(got, "telegram: disabled") {
		t.Fatalf("expected relayd to report Telegram disabled and carry on, got:\n%s", got)
	}
}

func envWithout(env []string, key string) []string {
	kept := env[:0:0]
	for _, kv := range env {
		if !strings.HasPrefix(kv, key+"=") {
			kept = append(kept, kv)
		}
	}
	return kept
}
