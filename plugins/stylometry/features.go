// Package stylometry is an optional, opt-in AnomalyDetector implementation
// for agent-relay (see relay.AnomalyDetector) — a separate Go module so
// nobody who doesn't want it pays for its dependencies. It scores how well a
// message matches a sender's historical writing style, backed by Qdrant.
//
// This is soft, probabilistic signal, not proof of identity: function-word
// frequencies and message-shape statistics are the standard stylometry
// features (harder to fake than content words), but style genuinely varies
// by mood/context/device, and anyone with access to a sender's message
// history could deliberately mimic it. Wire this in as an advisory layer on
// top of real controls (session expiry, allowlists), never as the sole gate.
package stylometry

import (
	"strings"
	"unicode"
)

// functionWords are the closed-class English words stylometry research
// treats as the most person-specific and hardest to deliberately fake —
// unlike content words, people don't consciously choose "the" vs "a" the way
// they choose vocabulary, so their relative frequencies are a stable
// fingerprint of habitual style. This is not an exhaustive list; it's a
// fixed, ordered set so the resulting feature vector has a stable dimension
// across every call.
var functionWords = []string{
	"the", "a", "an", "and", "but", "or", "so", "if", "of", "to", "in", "on",
	"at", "for", "with", "that", "this", "it", "is", "was", "are", "were",
	"i", "you", "we", "they", "not", "just",
}

// FeatureDim is the fixed length of every vector ExtractFeatures returns —
// callers (e.g. the Qdrant collection schema) need this to be stable.
var FeatureDim = len(functionWords) + 6

// ExtractFeatures computes a fixed-length stylometric feature vector from a
// single message: function-word frequencies (normalized by word count) plus
// a handful of shape statistics — average word length, average sentence
// length, message length in words, punctuation ratio, uppercase-letter
// ratio, and digit ratio. All frequencies/ratios are normalized to [0,1]-ish
// ranges (relative to word or character count) so no single long message
// dominates the vector purely by being long.
func ExtractFeatures(text string) []float32 {
	words := strings.Fields(text)
	wordCount := float64(len(words))
	if wordCount == 0 {
		return make([]float32, FeatureDim)
	}

	vec := make([]float32, 0, FeatureDim)

	counts := make(map[string]int, len(functionWords))
	var totalWordLen int
	var punct, upper, digit, letters int
	for _, w := range words {
		lower := strings.ToLower(strings.Trim(w, ".,!?;:\"'()[]"))
		counts[lower]++
		totalWordLen += len(w)
	}
	for _, r := range text {
		switch {
		case unicode.IsPunct(r):
			punct++
		case unicode.IsUpper(r):
			upper++
			letters++
		case unicode.IsLower(r):
			letters++
		case unicode.IsDigit(r):
			digit++
		}
	}

	for _, fw := range functionWords {
		vec = append(vec, float32(float64(counts[fw])/wordCount))
	}

	sentences := strings.FieldsFunc(text, func(r rune) bool { return r == '.' || r == '!' || r == '?' })
	avgSentenceLen := wordCount
	if len(sentences) > 0 {
		avgSentenceLen = wordCount / float64(len(sentences))
	}

	charCount := float64(len([]rune(text)))
	upperRatio := float32(0)
	if letters > 0 {
		upperRatio = float32(float64(upper) / float64(letters))
	}

	vec = append(vec,
		float32(float64(totalWordLen)/wordCount/10.0), // avg word length, scaled down
		float32(avgSentenceLen/20.0),                  // avg sentence length in words, scaled down
		float32(wordCount/50.0),                       // message length in words, scaled down
		float32(float64(punct)/charCount),             // punctuation ratio
		upperRatio,                                    // uppercase-letter ratio
		float32(float64(digit)/charCount),             // digit ratio
	)
	return vec
}
