package main

import (
	"encoding/json"
	"testing"
)

// Guards the 2026-07-19 incident: this command runs immediately before Claude
// launches, so overwriting settings.json silently wiped the operator's
// registered hooks on EVERY launch. Hooks must survive.
func TestMergeSettingsPreservesHooks(t *testing.T) {
	existing := []byte(`{
	  "hooks": {"Stop": [{"hooks": [{"type": "command", "command": "detect-reply-drift.py"}]}]},
	  "enabledMcpjsonServers": ["stale"]
	}`)
	own := map[string]any{"enabledMcpjsonServers": []string{"relay"}}

	merged, err := mergeSettings(existing, own)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if _, ok := merged["hooks"]; !ok {
		t.Fatal("hooks were dropped - registered hooks would be wiped on every launch (regression)")
	}
	// This command's own keys must still win over stale values.
	b, _ := json.Marshal(merged["enabledMcpjsonServers"])
	if string(b) != `["relay"]` {
		t.Fatalf("owned key not applied: %s", b)
	}
}

// A missing/empty file is normal on first run and must not error.
func TestMergeSettingsEmptyStart(t *testing.T) {
	merged, err := mergeSettings(nil, map[string]any{"enabledMcpjsonServers": []string{"relay"}})
	if err != nil {
		t.Fatalf("merge on empty: %v", err)
	}
	if merged["enabledMcpjsonServers"] == nil {
		t.Fatal("owned keys missing when starting from no file")
	}
}

// Unparseable input must error, never silently reset - the operator's hooks
// could be in there.
func TestMergeSettingsRefusesGarbage(t *testing.T) {
	if _, err := mergeSettings([]byte("{not json"), map[string]any{"a": 1}); err == nil {
		t.Fatal("expected an error for invalid JSON so the caller refuses to overwrite")
	}
}
