// Package senderr is shared across frontends (Telegram, Discord, ...) so
// their retry classification can't silently drift apart from each other.
package senderr

import "unicode/utf8"

// Permanent marks a Send failure as non-retryable.
type Permanent struct{ Err error }

func (e Permanent) Error() string { return e.Err.Error() }
func (e Permanent) Unwrap() error { return e.Err }

// Split breaks text into chunks of at most limit runes each.
func Split(text string, limit int) []string {
	if limit <= 0 {
		return []string{text}
	}
	if utf8.RuneCountInString(text) <= limit {
		return []string{text}
	}

	var chunks []string
	remaining := text
	for utf8.RuneCountInString(remaining) > limit {
		cut, onSeparator := bestBreak(remaining, limit)
		chunks = append(chunks, remaining[:cut])
		remaining = remaining[cut:]
		// Only trim when the cut landed on a separator: a hard rune-count
		// fallback cut has no such guarantee, and trimming there would
		// silently eat real content.
		if onSeparator {
			remaining = trimOneLeadingSeparator(remaining)
		}
	}
	if remaining != "" {
		chunks = append(chunks, remaining)
	}
	return chunks
}

// bestBreak prefers the latest separator at or before the rune-limit
// boundary so chunks don't split mid-word, falling back to a hard rune-count
// cut (always valid, since it's counted in runes not bytes) when none exists
// in range. The second return value reports whether the cut landed on a
// separator, which Split needs to decide whether trimming the leading
// separator off the remainder is safe.
func bestBreak(s string, limit int) (int, bool) {
	limitByte := runeLimitByteOffset(s, limit)
	window := s[:limitByte]

	if i := lastIndexFrom(window, "\n\n"); i > 0 {
		return i + len("\n\n"), true
	}
	if i := lastIndexFrom(window, "\n"); i > 0 {
		return i + len("\n"), true
	}
	if i := lastIndexFrom(window, " "); i > 0 {
		return i + len(" "), true
	}
	return limitByte, false
}

func runeLimitByteOffset(s string, limit int) int {
	count := 0
	for i := range s {
		if count == limit {
			return i
		}
		count++
	}
	return len(s)
}

func lastIndexFrom(s, sep string) int {
	for i := len(s) - len(sep); i >= 0; i-- {
		if s[i:i+len(sep)] == sep {
			return i
		}
	}
	return -1
}

func trimOneLeadingSeparator(s string) string {
	switch {
	case len(s) >= 1 && s[0] == '\n':
		return s[1:]
	case len(s) >= 1 && s[0] == ' ':
		return s[1:]
	default:
		return s
	}
}
