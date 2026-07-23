package stylometry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Detector satisfies relay.AnomalyDetector structurally, without importing
// agent-relay's relay package.
type Detector struct {
	BaseURL    string // Qdrant REST endpoint, e.g. "http://10.43.245.226:6333"
	Collection string
	MinHistory int // prior batches needed before a score is meaningful; below it, Score returns 0
	K          int // nearest historical batches to average distance over

	// WindowSize messages are concatenated into one non-overlapping scoring
	// batch. A sliding window was tried first and discarded: successive
	// overlapping windows made each swap noisy against whatever else was
	// buffered, and it measurably failed to separate in-style from
	// out-of-style text. Non-overlapping batches of 5 fixed that (~23x
	// separation vs ~1.6x at WindowSize=1, measured against live Qdrant).
	WindowSize int

	// Log, if set, records an Explanation for every scored batch.
	Log *EventLog

	httpClient *http.Client

	windowMu sync.Mutex
	windows  map[string][]string
}

// NewDetector returns a Detector with defaults MinHistory=10, K=5,
// WindowSize=5.
func NewDetector(baseURL, collection string) *Detector {
	return &Detector{
		BaseURL:    baseURL,
		Collection: collection,
		MinHistory: 10,
		K:          5,
		WindowSize: 5,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		windows:    make(map[string][]string),
	}
}

// pushWindow buffers text for userID and, once WindowSize messages have
// accumulated, returns the concatenated batch (clearing the buffer) and
// true. Below WindowSize it returns ("", false).
func (d *Detector) pushWindow(userID, text string) (string, bool) {
	d.windowMu.Lock()
	defer d.windowMu.Unlock()
	w := append(d.windows[userID], text)
	if len(w) < d.WindowSize {
		d.windows[userID] = w
		return "", false
	}
	d.windows[userID] = nil
	return strings.Join(w, " "), true
}

// EnsureCollection creates the Qdrant collection if missing. Call once at
// startup; Score does not call this itself.
func (d *Detector) EnsureCollection(ctx context.Context) error {
	body, _ := json.Marshal(map[string]any{
		"vectors": map[string]any{
			"size": FeatureDim,
			// Euclidean, not Cosine: this vector is mostly near-zero for
			// short messages, and cosine similarity is direction-only, so it
			// barely distinguishes sparse vectors regardless of content
			// (verified: Cosine scored an exact repeat and an unrelated
			// message within 0.01 of each other).
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
	// Qdrant returns 409, not 200, for an already-existing collection even
	// with identical params — 409 here means "already ensured."
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
		Score  float64   `json:"score"`
		Vector []float32 `json:"vector"`
	} `json:"result"`
}

// Score satisfies relay.AnomalyDetector. Use ScoreExplain directly for the
// full breakdown.
func (d *Detector) Score(ctx context.Context, userID, text string) (float64, error) {
	exp, err := d.ScoreExplain(ctx, userID, text)
	if err != nil {
		return 0, err
	}
	return exp.Score, nil
}

// ScoreExplain batches text into userID's window (see pushWindow); until a
// batch completes it returns a zero-value Explanation, which means "nothing
// to score yet," not "scored and found normal." Once a batch is ready, it's
// compared against userID's K nearest historical batches (once MinHistory
// exist), upserted as a new point, and — if Log is set — appended there.
func (d *Detector) ScoreExplain(ctx context.Context, userID, text string) (Explanation, error) {
	window, ready := d.pushWindow(userID, text)
	exp := Explanation{UserID: userID, At: time.Now().UTC()}
	if !ready {
		return exp, nil
	}
	exp.Window = window
	vec := ExtractFeatures(window)

	count, err := d.userPointCount(ctx, userID)
	if err != nil {
		return exp, err
	}

	if count >= d.MinHistory {
		neighbors, err := d.searchNeighbors(ctx, userID, vec)
		if err != nil {
			return exp, err
		}
		exp.NeighborCount = len(neighbors)
		var sum float64
		vecs := make([][]float32, len(neighbors))
		for i, n := range neighbors {
			sum += n.dist
			vecs[i] = n.vec
		}
		if len(neighbors) > 0 {
			exp.Score = sum / float64(len(neighbors))
			exp.TopDeviations = topDeviations(vec, vecs, 5)
		}
	}

	if err := d.upsertPoint(ctx, userID, vec); err != nil {
		return exp, err
	}
	if d.Log != nil {
		if err := d.Log.Append(exp); err != nil {
			return exp, err
		}
	}
	return exp, nil
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

type neighbor struct {
	dist float64
	vec  []float32
}

func (d *Detector) searchNeighbors(ctx context.Context, userID string, vec []float32) ([]neighbor, error) {
	body, _ := json.Marshal(map[string]any{
		"vector": vec,
		"filter": map[string]any{
			"must": []map[string]any{{"key": "user_id", "match": map[string]any{"value": userID}}},
		},
		"limit":        d.K,
		"with_payload": false,
		"with_vector":  true,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url("/collections/"+d.Collection+"/points/search"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("stylometry: search: %w", err)
	}
	defer resp.Body.Close()
	var out searchResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("stylometry: search: decode: %w", err)
	}
	// Qdrant's Euclidean search score is the raw distance, not a bounded
	// similarity — higher means farther/more anomalous.
	neighbors := make([]neighbor, len(out.Result))
	for i, r := range out.Result {
		neighbors[i] = neighbor{dist: r.Score, vec: r.Vector}
	}
	return neighbors, nil
}

// SeedHistory backfills userID's baseline from already-trusted past
// messages, chunked into non-overlapping WindowSize batches, skipping the
// search/threshold/log step since seed data isn't being gated.
func (d *Detector) SeedHistory(ctx context.Context, userID string, messages []string) error {
	w := d.WindowSize
	if w < 1 {
		w = 1
	}
	for i := 0; i < len(messages); i += w {
		end := i + w
		if end > len(messages) {
			end = len(messages)
		}
		vec := ExtractFeatures(strings.Join(messages[i:end], " "))
		if err := d.upsertPoint(ctx, userID, vec); err != nil {
			return fmt.Errorf("stylometry: seed history: %w", err)
		}
	}
	return nil
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
