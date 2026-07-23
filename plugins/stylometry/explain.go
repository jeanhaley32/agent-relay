package stylometry

import (
	"sort"
	"time"
)

// FeatureDeviation is one dimension's contribution to an anomaly score: how
// far this window's value sits from the average of the sender's own nearest
// historical points on that single dimension.
type FeatureDeviation struct {
	Feature     string  `json:"feature"`
	Value       float32 `json:"value"`
	BaselineAvg float32 `json:"baseline_avg"`
	Delta       float32 `json:"delta"` // Value - BaselineAvg; sign matters, not just magnitude
}

// Explanation is the full, inspectable record of one scored window — not
// just the bare number relay.AnomalyDetector.Score returns. Built so a human
// (or the model, via the JSONL log EventLog writes) can answer "what
// actually caused this alert" instead of trusting a float blind.
type Explanation struct {
	UserID        string             `json:"user_id"`
	At            time.Time          `json:"at"`
	Window        string             `json:"window"` // the concatenated WindowSize-message text that was scored
	Score         float64            `json:"score"`
	NeighborCount int                `json:"neighbor_count"` // how many historical points this was compared against; 0 means "not enough history yet"
	TopDeviations []FeatureDeviation `json:"top_deviations,omitempty"`
}

// topDeviations compares vec against the average of neighbors (both in
// ExtractFeatures' dimension order) and returns the n dimensions with the
// largest absolute delta, most-deviant first — the actual answer to "what
// about this message looked unlike the sender," not just a distance number.
func topDeviations(vec []float32, neighbors [][]float32, n int) []FeatureDeviation {
	if len(neighbors) == 0 {
		return nil
	}
	names := FeatureNames()
	avg := make([]float32, len(vec))
	for _, nb := range neighbors {
		for i, v := range nb {
			if i < len(avg) {
				avg[i] += v
			}
		}
	}
	for i := range avg {
		avg[i] /= float32(len(neighbors))
	}

	devs := make([]FeatureDeviation, len(vec))
	for i, v := range vec {
		delta := v - avg[i]
		devs[i] = FeatureDeviation{Feature: names[i], Value: v, BaselineAvg: avg[i], Delta: delta}
	}
	sort.Slice(devs, func(i, j int) bool { return abs32(devs[i].Delta) > abs32(devs[j].Delta) })
	if n > len(devs) {
		n = len(devs)
	}
	return devs[:n]
}

func abs32(f float32) float32 {
	if f < 0 {
		return -f
	}
	return f
}
