package deniedlog

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestFileDeniedLoggerWritesPlatformTaggedEntries(t *testing.T) {
	path := t.TempDir() + "/denied.jsonl"
	dl, err := NewFileDeniedLogger(path)
	if err != nil {
		t.Fatalf("NewFileDeniedLogger: %v", err)
	}
	dl.LogDenied("telegram", 111, "Casey", "chatA", "let me in")
	dl.LogDenied("discord", 222, "Riley", "chanB", "me too")
	if err := dl.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 entries, got %d: %q", len(lines), data)
	}
	var e0, e1 Entry
	json.Unmarshal([]byte(lines[0]), &e0)
	json.Unmarshal([]byte(lines[1]), &e1)
	if e0.Platform != "telegram" || e0.ID != 111 || e0.Text != "let me in" {
		t.Errorf("entry 0 wrong: %+v", e0)
	}
	if e1.Platform != "discord" || e1.ID != 222 || e1.ChatID != "chanB" {
		t.Errorf("entry 1 wrong: %+v", e1)
	}
}

func TestNoopDiscards(t *testing.T) {
	// Must not panic and there's nothing to flush — it just drops.
	Noop{}.LogDenied("telegram", 1, "x", "y", "z")
}
