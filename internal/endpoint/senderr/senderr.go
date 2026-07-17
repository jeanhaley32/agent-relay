// Package senderr provides shared outbound-message plumbing used by every
// frontend (Telegram, Discord, ...): the retry-classification error type and
// a length-aware message splitter. A "permanent" send failure is one where
// retrying is guaranteed to reproduce the same failure (a missing
// destination id, a 4xx that isn't a rate limit) as opposed to a transient
// one (network blip, provider outage) that background retry can legitimately
// fix.
//
// This was extracted out of internal/endpoint/telegram once a second
// frontend (Discord) needed the identical type, to avoid two copies of the
// same retry-classification logic silently drifting apart over time.
package senderr

import "unicode/utf8"

// Permanent marks a Send failure as non-retryable.
type Permanent struct{ Err error }

func (e Permanent) Error() string { return e.Err.Error() }
func (e Permanent) Unwrap() error { return e.Err }

// Split breaks text into chunks no longer than limit runes, so a frontend
// can deliver an oversized reply as multiple messages instead of permanently
// dropping it — a long reply silently failing against a platform's
// per-message length limit leaves the sender with no error and no message.
// Returns a single-element slice unchanged if text already fits.
//
// Prefers breaking on paragraph boundaries ("\n\n"), then single newlines,
// then spaces, so chunks read naturally rather than splitting mid-word; only
// falls back to a hard rune-count cut for a single "word" longer than limit
// on its own (rare, but must never infinite-loop or drop a rune).
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
		cut := bestBreak(remaining, limit)
		chunks = append(chunks, remaining[:cut])
		remaining = remaining[cut:]
		// bestBreak already cuts past the separator it broke on, so this only
		// fires when that separator repeats (e.g. "\n\n\n" or two spaces) and
		// leaves one more of the same rune dangling at the new start; drop it
		// so chunks don't accumulate leading whitespace.
		remaining = trimOneLeadingSeparator(remaining)
	}
	if remaining != "" {
		chunks = append(chunks, remaining)
	}
	return chunks
}

// bestBreak returns a byte offset into s, at or before the rune-limit
// boundary, preferring the latest paragraph/newline/space break so chunks
// don't split mid-word. Falls back to a hard rune-count cut (always valid,
// since it's counted in runes not bytes) if no separator exists in range.
func bestBreak(s string, limit int) int {
	limitByte := runeLimitByteOffset(s, limit)
	window := s[:limitByte]

	if i := lastIndexFrom(window, "\n\n"); i > 0 {
		return i + len("\n\n")
	}
	if i := lastIndexFrom(window, "\n"); i > 0 {
		return i + len("\n")
	}
	if i := lastIndexFrom(window, " "); i > 0 {
		return i + len(" ")
	}
	return limitByte
}

// runeLimitByteOffset returns the byte offset of the limit-th rune in s (or
// len(s) if s has fewer runes than limit).
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
