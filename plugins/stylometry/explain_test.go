package stylometry

import "testing"

func TestTopDeviations_MostDeviantFirst(t *testing.T) {
	// dim 0 differs a lot (2.0 vs avg 0.0), dim 1 barely (1.01 vs avg 1.0).
	vec := []float32{2.0, 1.01}
	neighbors := [][]float32{{0.0, 1.0}, {0.0, 1.0}}
	devs := topDeviations(vec, neighbors, 2)
	if len(devs) != 2 {
		t.Fatalf("expected 2 deviations, got %d", len(devs))
	}
	if devs[0].Feature != FeatureNames()[0] {
		t.Fatalf("expected dimension 0 (largest delta) first, got %q", devs[0].Feature)
	}
	if devs[0].Delta != 2.0 {
		t.Fatalf("expected delta 2.0 for the most-deviant dimension, got %v", devs[0].Delta)
	}
}

func TestTopDeviations_EmptyNeighborsReturnsNil(t *testing.T) {
	if got := topDeviations([]float32{1, 2, 3}, nil, 5); got != nil {
		t.Fatalf("expected nil for zero neighbors, got %v", got)
	}
}

func TestTopDeviations_CapsAtRequestedCount(t *testing.T) {
	vec := make([]float32, 10)
	nb := make([][]float32, 1)
	nb[0] = make([]float32, 10)
	devs := topDeviations(vec, nb, 3)
	if len(devs) != 3 {
		t.Fatalf("expected exactly 3 deviations, got %d", len(devs))
	}
}
