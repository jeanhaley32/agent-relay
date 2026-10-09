package eventlog

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestLogAppendsAndTraces(t *testing.T) {
	p := filepath.Join(t.TempDir(), "e.jsonl")
	l, err := Open(p)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	id := NewMsgID()
	// A full lifecycle for one message — this is what grep <msg_id> must show.
	l.Log(Record{MsgID: id, Event: Received, Frontend: "telegram", ChatID: "1", Bytes: 5})
	l.Log(Record{MsgID: id, Event: Injected})
	l.Log(Record{MsgID: id, Event: SendOK, Detail: "message_id=42"})
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 lines, got %d: %q", len(lines), b)
	}
	for _, ln := range lines {
		var r Record
		if err := json.Unmarshal([]byte(ln), &r); err != nil {
			t.Fatalf("line not valid JSON: %v (%s)", err, ln)
		}
		if r.MsgID != id {
			t.Fatalf("msg_id not carried: %q", r.MsgID)
		}
		if r.TS == "" {
			t.Fatal("timestamp not stamped")
		}
	}
}

// Concurrent writers must never interleave partial lines, or the trail becomes
// unparseable exactly when things are going wrong (high load).
func TestConcurrentWritesStayLineAtomic(t *testing.T) {
	p := filepath.Join(t.TempDir(), "e.jsonl")
	l, _ := Open(p)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Log(Record{MsgID: NewMsgID(), Event: Received, Detail: strings.Repeat("x", 200)})
		}()
	}
	wg.Wait()
	_ = l.Close()

	f, _ := os.Open(p)
	defer f.Close()
	n := 0
	s := bufio.NewScanner(f)
	for s.Scan() {
		var r Record
		if err := json.Unmarshal(s.Bytes(), &r); err != nil {
			t.Fatalf("interleaved/corrupt line: %v", err)
		}
		n++
	}
	if n != 50 {
		t.Fatalf("want 50 records, got %d", n)
	}
}

// A nil Logger must be usable so callers never need nil checks in hot paths.
func TestNilLoggerIsSafe(t *testing.T) {
	var l *Logger
	l.Log(Record{Event: Received}) // must not panic
	if err := l.Close(); err != nil {
		t.Fatalf("nil close: %v", err)
	}
}

func TestMsgIDsAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewMsgID()
		if id == "" {
			t.Fatal("empty msg id")
		}
		if seen[id] {
			t.Fatalf("duplicate msg id %q", id)
		}
		seen[id] = true
	}
}
