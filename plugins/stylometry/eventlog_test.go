package stylometry

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEventLog_AppendCreatesFileAndDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "events.jsonl")
	l := &EventLog{Path: path}

	exp := Explanation{UserID: "u1", At: time.Now().UTC(), Score: 0.5, Window: "hello"}
	if err := l.Append(exp); err != nil {
		t.Fatalf("Append: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("expected the log file to exist: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		t.Fatal("expected at least one line in the log file")
	}
	var got Explanation
	if err := json.Unmarshal(scanner.Bytes(), &got); err != nil {
		t.Fatalf("line is not valid JSON: %v", err)
	}
	if got.UserID != "u1" || got.Score != 0.5 || got.Window != "hello" {
		t.Fatalf("round-tripped explanation = %+v, want UserID=u1 Score=0.5 Window=hello", got)
	}
}

func TestEventLog_AppendIsCumulative(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	l := &EventLog{Path: path}

	for i := 0; i < 3; i++ {
		if err := l.Append(Explanation{UserID: "u1"}); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	n := 0
	for scanner.Scan() {
		n++
	}
	if n != 3 {
		t.Fatalf("expected 3 lines after 3 Append calls, got %d", n)
	}
}
