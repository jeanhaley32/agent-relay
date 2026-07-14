package senderr

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSplitFitsUnchanged(t *testing.T) {
	got := Split("hello", 100)
	if len(got) != 1 || got[0] != "hello" {
		t.Fatalf("Split of a short string = %v, want unchanged single-element slice", got)
	}
}

func TestSplitBreaksOnParagraph(t *testing.T) {
	text := strings.Repeat("a", 50) + "\n\n" + strings.Repeat("b", 50)
	got := Split(text, 60)
	if len(got) != 2 {
		t.Fatalf("Split() = %d chunks, want 2: %v", len(got), got)
	}
	if strings.Contains(got[0], "b") || strings.Contains(got[1], "a") {
		t.Errorf("split crossed the paragraph boundary: %v", got)
	}
}

func TestSplitHardCutsUnbreakableSpan(t *testing.T) {
	// A single "word" with no spaces/newlines longer than the limit must
	// still be split (never silently truncated or left over-limit).
	text := strings.Repeat("x", 250)
	got := Split(text, 100)
	if len(got) != 3 {
		t.Fatalf("Split() = %d chunks, want 3 for a 250-char unbreakable span at limit 100: %v", len(got), got)
	}
	var rejoined strings.Builder
	for _, c := range got {
		if n := utf8.RuneCountInString(c); n > 100 {
			t.Errorf("chunk exceeds limit: %d runes", n)
		}
		rejoined.WriteString(c)
	}
	if rejoined.String() != text {
		t.Errorf("rejoined chunks lost content: got %d chars, want %d", rejoined.Len(), len(text))
	}
}

func TestSplitNoDataLoss(t *testing.T) {
	// Every rune in the input must appear in the output, in order - a
	// splitter that silently drops content on a chunk boundary is worse
	// than the permanent-drop behavior it replaces.
	text := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 200)
	got := Split(text, 500)
	if len(got) < 2 {
		t.Fatalf("expected multiple chunks for a %d-char input at limit 500", len(text))
	}
	rejoined := strings.Join(got, "")
	// Splitting drops at most one separator rune (space/newline) between
	// chunks, so allow for that when comparing lengths.
	if utf8.RuneCountInString(rejoined) < utf8.RuneCountInString(text)-len(got) {
		t.Errorf("rejoined length %d too short vs original %d (%d chunks) - real content was lost",
			utf8.RuneCountInString(rejoined), utf8.RuneCountInString(text), len(got))
	}
}

func TestSplitEveryChunkWithinLimit(t *testing.T) {
	text := strings.Repeat("word ", 2000)
	limit := 2000
	got := Split(text, limit)
	for i, c := range got {
		if n := utf8.RuneCountInString(c); n > limit {
			t.Errorf("chunk %d has %d runes, exceeds limit %d", i, n, limit)
		}
	}
}
