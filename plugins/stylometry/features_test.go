package stylometry

import (
	"testing"
)

func TestExtractFeatures_StableDimension(t *testing.T) {
	for _, text := range []string{"", "hi", "The quick brown fox jumps over the lazy dog. It was fast!"} {
		vec := ExtractFeatures(text)
		if len(vec) != FeatureDim {
			t.Fatalf("ExtractFeatures(%q) returned %d dims, want %d", text, len(vec), FeatureDim)
		}
	}
}

func TestExtractFeatures_EmptyTextIsZeroVector(t *testing.T) {
	vec := ExtractFeatures("")
	for i, v := range vec {
		if v != 0 {
			t.Fatalf("expected zero vector for empty text, got nonzero at index %d: %f", i, v)
		}
	}
}

// TestExtractFeatures_FunctionWordFrequencyDiffers is the load-bearing
// property: two texts with very different function-word usage must produce
// different vectors, since that's the whole signal the detector scores on.
func TestExtractFeatures_FunctionWordFrequencyDiffers(t *testing.T) {
	a := ExtractFeatures("the cat and the dog and the bird")
	b := ExtractFeatures("cats dogs birds fish snakes lizards")
	same := true
	for i := range a {
		if a[i] != b[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("expected texts with very different function-word usage to produce different vectors")
	}
}

// TestFeatureNames_MatchesVectorDimensionAndOrder guards the invariant
// topDeviations depends on: FeatureNames()[i] must label ExtractFeatures'
// index i, for every i, or explanations would attach the wrong name to the
// wrong value.
func TestFeatureNames_MatchesVectorDimensionAndOrder(t *testing.T) {
	names := FeatureNames()
	if len(names) != FeatureDim {
		t.Fatalf("FeatureNames() returned %d names, want %d (FeatureDim)", len(names), FeatureDim)
	}
	for i, fw := range functionWords {
		want := "fw:" + fw
		if names[i] != want {
			t.Errorf("names[%d] = %q, want %q", i, names[i], want)
		}
	}
	for i, sn := range shapeFeatureNames {
		idx := len(functionWords) + i
		if names[idx] != sn {
			t.Errorf("names[%d] = %q, want %q", idx, names[idx], sn)
		}
	}
}

func TestExtractFeatures_DeterministicForSameInput(t *testing.T) {
	a := ExtractFeatures("Hello there, how are you doing today?")
	b := ExtractFeatures("Hello there, how are you doing today?")
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("expected identical vectors for identical input, differed at index %d: %f vs %f", i, a[i], b[i])
		}
	}
}
