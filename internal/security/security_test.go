package security

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "security.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFullMode(t *testing.T) {
	c, err := Load(write(t, "mode: full\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !c.SkipPermissions() {
		t.Fatal("full mode should skip permissions")
	}
	s := c.Settings()
	if _, ok := s["permissions"]; ok {
		t.Fatal("full mode should not set permission lists")
	}
	if srv := s["enabledMcpjsonServers"].([]string); srv[0] != "relay" {
		t.Fatalf("channel server should be enabled: %v", srv)
	}
}

// Interactive-prompt tools must be stripped in EVERY mode: a headless relay
// session freezes if the model can render a modal no human can answer.
func TestDisallowsInteractivePromptsInAllModes(t *testing.T) {
	for _, mode := range []string{"mode: full\n", "mode: restricted\n"} {
		c, err := Load(write(t, mode))
		if err != nil {
			t.Fatal(err)
		}
		dt := c.DisallowedTools()
		found := false
		for _, tool := range dt {
			if tool == "AskUserQuestion" {
				found = true
			}
		}
		if !found {
			t.Fatalf("%q: AskUserQuestion must be disallowed, got %v", mode, dt)
		}
	}
}

func TestRestrictedMode(t *testing.T) {
	c, err := Load(write(t, "mode: restricted\nallow: [Read, Grep]\ndeny: [Bash, Write]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.SkipPermissions() {
		t.Fatal("restricted mode must not skip permissions")
	}
	perms := c.Settings()["permissions"].(map[string]any)
	allow := perms["allow"].([]string)
	deny := perms["deny"].([]string)

	// reply is always allowed, plus the configured tools.
	if allow[0] != replyTool {
		t.Fatalf("reply must be allowed first: %v", allow)
	}
	if !contains(allow, "Read") || !contains(allow, "Grep") {
		t.Fatalf("configured allows missing: %v", allow)
	}
	if !contains(deny, "Bash") || !contains(deny, "Write") {
		t.Fatalf("configured denies missing: %v", deny)
	}
}

func TestDefaultsToRestricted(t *testing.T) {
	c, err := Load(write(t, "allow: [Read]\n")) // no mode
	if err != nil {
		t.Fatal(err)
	}
	if c.Mode != ModeRestricted || c.SkipPermissions() {
		t.Fatal("blank mode must fail safe to restricted")
	}
}

func TestInvalidMode(t *testing.T) {
	if _, err := Load(write(t, "mode: yolo\n")); err == nil {
		t.Fatal("invalid mode must error")
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
