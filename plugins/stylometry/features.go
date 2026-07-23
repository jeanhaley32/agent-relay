// Package stylometry is an optional, opt-in AnomalyDetector implementation
// for agent-relay's relay.AnomalyDetector — a separate Go module so nobody
// who doesn't want it pays for its dependencies. It scores how well a
// message matches a sender's historical writing style, backed by Qdrant.
// This is soft signal, not proof of identity — wire it in as an advisory
// layer on top of real controls, never as the sole gate.
package stylometry

import (
	"strings"
	"unicode"
)

// functionWords are the closed-class words treated as the hardest to
// deliberately fake — their relative frequency is a stable fingerprint of
// habitual style. Fixed and ordered so the feature vector's dimension never
// changes across calls.
var functionWords = []string{
	"the", "a", "an", "and", "but", "or", "so", "if", "of", "to", "in", "on",
	"at", "for", "with", "that", "this", "it", "is", "was", "are", "were",
	"i", "you", "we", "they", "not", "just",
}

// shapeFeatureNames labels the 6 dimensions ExtractFeatures appends after
// functionWords, in that exact order.
var shapeFeatureNames = []string{
	"avg_word_length", "avg_sentence_length", "message_length_words",
	"punctuation_ratio", "uppercase_ratio", "digit_ratio",
}

// FeatureDim is the fixed length of every vector ExtractFeatures returns.
var FeatureDim = len(functionWords) + len(shapeFeatureNames)

// FeatureNames labels each dimension ExtractFeatures returns, in order.
func FeatureNames() []string {
	names := make([]string, 0, FeatureDim)
	for _, fw := range functionWords {
		names = append(names, "fw:"+fw)
	}
	names = append(names, shapeFeatureNames...)
	return names
}

// ExtractFeatures computes a fixed-length stylometric feature vector from
// text: function-word frequencies plus a handful of normalized shape
// statistics.
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
