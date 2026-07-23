package stylometry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Detector implements the shape of relay.AnomalyDetector (Score(ctx,
// userID, text) (float64, error)) without importing agent-relay's relay
// package — Go interfaces are structural, so this module stays fully
// decoupled and the main module never needs to know stylometry exists.
type Detector struct {
	// BaseURL is Qdrant's REST endpoint, e.g. "http://10.43.245.226:6333".
	BaseURL string
	// Collection is the Qdrant collection name; created on first use if
	// missing (see EnsureCollection).
	Collection string
	// MinHistory is how many prior points a user needs before a score means
	// anything — below it, Score returns 0 (never anomalous) and just
	// records the point, so a new user's first few messages can't trip the
	// gate purely for lack of a baseline.
	MinHistory int
	// K is how many of the user's own nearest historical points to average
	// distance over. A single-nearest-neighbor score is noisy; averaging a
	// handful is closer to "does this look like their recent style" than
	// "does this exactly match one past message."
	K int

	httpClient *http.Client
}

// NewDetector returns a Detector with sane defaults (MinHistory=10, K=5, a
// 5-second HTTP client) — override the fields directly if you need
// different values.
func NewDetector(baseURL, collection string) *Detector {
	return &Detector{
		BaseURL:    baseURL,
		Collection: collection,
		MinHistory: 10,
		K:          5,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

// EnsureCollection creates the Qdrant collection if it doesn't already
// exist, sized for FeatureDim vectors with cosine distance (the standard
// choice for normalized-ish feature vectors like these). Call once at
// startup before wiring the Detector in; Score does not call this itself so
// a transient collection-creation failure doesn't turn into a startup-time
// dependency on every single scored message.
func (d *Detector) EnsureCollection(ctx context.Context) error {
	body, _ := json.Marshal(map[string]any{
		"vectors": map[string]any{
			"size": FeatureDim,
			// Euclidean, not Cosine: this vector is hand-crafted and mostly
			// near-zero for short messages (most function-word frequencies
			// are 0), and cosine similarity is direction-only — it saturates
			// near 1.0 for any two such sparse vectors regardless of how
			// different their nonzero values actually are. Verified this
			// concretely: Cosine scored an exact-text repeat and a
			// completely unrelated message within 0.01 of each other.
			// Euclidean distance on the raw values discriminates properly.
			"distance": "Euclid",
		},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, d.url("/collections/"+d.Collection), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("stylometry: ensure collection: %w", err)
	}
	defer resp.Body.Close()
	// Qdrant returns 200 on create, but unconditionally 409 if the
	// collection already exists — even with identical params, not just on a
	// mismatch — so 409 here means "already ensured," not an error.
	// Verified directly against a live instance rather than assumed.
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("stylometry: ensure collection: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (d *Detector) url(path string) string {
	return d.BaseURL + path
}

type searchResult struct {
	Result []struct {
		Score float64 `json:"score"`
	} `json:"result"`
}

// Score extracts a feature vector from text, searches Qdrant for userID's K
// nearest historical points, and returns their average Euclidean distance as
// the anomaly score (0 = matches their style closely; larger = less like
// their established pattern — there's no fixed upper bound, tune
// AnomalyThreshold against real traffic rather than assuming a 0-1 range).
// Below MinHistory prior points, it returns 0 unconditionally —
// not enough data to judge yet — but still records the point, so the
// baseline builds up over the user's first several messages. Every call
// (including below-MinHistory ones) upserts the new point, so the baseline
// is always growing/rolling forward.
func (d *Detector) Score(ctx context.Context, userID, text string) (float64, error) {
	vec := ExtractFeatures(text)

	count, err := d.userPointCount(ctx, userID)
	if err != nil {
		return 0, err
	}

	score := 0.0
	if count >= d.MinHistory {
		score, err = d.anomalyScore(ctx, userID, vec)
		if err != nil {
			return 0, err
		}
	}

	if err := d.upsertPoint(ctx, userID, vec); err != nil {
		return 0, err
	}
	return score, nil
}

func (d *Detector) userPointCount(ctx context.Context, userID string) (int, error) {
	body, _ := json.Marshal(map[string]any{
		"filter": map[string]any{
			"must": []map[string]any{{"key": "user_id", "match": map[string]any{"value": userID}}},
		},
		"exact": true,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url("/collections/"+d.Collection+"/points/count"), bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("stylometry: count: %w", err)
	}
	defer resp.Body.Close()
	var out struct {
		Result struct {
			Count int `json:"count"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("stylometry: count: decode: %w", err)
	}
	return out.Result.Count, nil
}

func (d *Detector) anomalyScore(ctx context.Context, userID string, vec []float32) (float64, error) {
	body, _ := json.Marshal(map[string]any{
		"vector": vec,
		"filter": map[string]any{
			"must": []map[string]any{{"key": "user_id", "match": map[string]any{"value": userID}}},
		},
		"limit":        d.K,
		"with_payload": false,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url("/collections/"+d.Collection+"/points/search"), bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("stylometry: search: %w", err)
	}
	defer resp.Body.Close()
	var out searchResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("stylometry: search: decode: %w", err)
	}
	if len(out.Result) == 0 {
		return 0, nil
	}
	// With Euclidean distance, Qdrant's search score IS the raw distance
	// (higher = farther = more anomalous) — there's no 1-minus-similarity
	// conversion to do, unlike Cosine's bounded [0,1] similarity. Verified
	// against a live search response rather than assumed.
	var sum float64
	for _, r := range out.Result {
		sum += r.Score
	}
	return sum / float64(len(out.Result)), nil
}

func (d *Detector) upsertPoint(ctx context.Context, userID string, vec []float32) error {
	body, _ := json.Marshal(map[string]any{
		"points": []map[string]any{{
			"id":      time.Now().UnixNano(),
			"vector":  vec,
			"payload": map[string]any{"user_id": userID, "at": time.Now().UTC().Format(time.RFC3339)},
		}},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, d.url("/collections/"+d.Collection+"/points"), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("stylometry: upsert: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("stylometry: upsert: unexpected status %d", resp.StatusCode)
	}
	return nil
}
