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

	// WindowSize is how many of a sender's messages get batched into one
	// concatenated scoring unit, instead of scoring each message alone —
	// non-overlapping batches (message 1-5, then 6-10, ...), not a sliding
	// window; see pushWindow's doc comment for why sliding was tried and
	// measurably didn't work. Measured directly against live Qdrant before
	// picking this default: per-message scoring (WindowSize=1) separated
	// in-style from out-of-style by only ~1.6x; batches of 5 widened that to
	// ~23x. The tradeoff is latency two ways — a compromised account gets up
	// to WindowSize messages through before a batch is even scored, and a
	// score only ever arrives once every WindowSize messages, not on every
	// one — both are the right side to err on for an advisory signal that
	// fails open anyway.
	WindowSize int

	// Log, if set, records an Explanation for every scored window — the
	// inspectable "what caused this alert" trail. nil ⇒ scoring still
	// works, it's just not persisted anywhere.
	Log *EventLog

	httpClient *http.Client

	windowMu sync.Mutex
	windows  map[string][]string // userID -> most recent WindowSize message texts
}

// NewDetector returns a Detector with sane defaults (MinHistory=10, K=5,
// WindowSize=5, a 5-second HTTP client) — override the fields directly if
// you need different values.
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
// true. Below WindowSize, it returns ("", false) — nothing to score yet.
//
// This is non-overlapping batching, not a sliding window, and that's a
// deliberate correction: an earlier sliding design (append one message,
// keep the last WindowSize, score every call) was measured to NOT reproduce
// the separation improvement windowing is supposed to buy — successive
// scored windows overlapped 4-of-5 messages, so each individual swap was
// noisy relative to whatever else happened to be buffered at that moment,
// not a stable comparison. Clean non-overlapping batches — exactly what was
// validated (WindowSize=1 ~1.6x separation vs WindowSize=5 ~23x) — don't
// have that problem, at the cost of only producing a score once every
// WindowSize messages instead of on every one.
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
		Score  float64   `json:"score"`
		Vector []float32 `json:"vector"`
	} `json:"result"`
}

// Score satisfies relay.AnomalyDetector: rolls text into userID's window,
// scores it, logs the full explanation if Log is set, and returns just the
// number — the bare-float contract the gate in relay.go actually needs. Use
// ScoreExplain directly when you want the full breakdown in-process instead
// of reading it back out of the log.
func (d *Detector) Score(ctx context.Context, userID, text string) (float64, error) {
	exp, err := d.ScoreExplain(ctx, userID, text)
	if err != nil {
		return 0, err
	}
	return exp.Score, nil
}

// ScoreExplain does the real work: buffers text into userID's non-overlapping
// WindowSize-message batch (see pushWindow) — returning a zero-value
// Explanation with NeighborCount 0 until a full batch has accumulated, which
// is not the same as "scored and found normal," just "nothing to score yet"
// — then, once a batch is ready, extracts features from the concatenated
// text, searches Qdrant for userID's K nearest historical points, and
// returns not just the average-Euclidean-distance score but which specific
// feature dimensions drove it (see topDeviations) — 0 score, no deviations
// if the sender has fewer than MinHistory prior batches (not enough data to
// judge yet). Every completed batch upserts as a new point regardless, so
// the baseline keeps growing forward, and — if Log is set — appends the full
// Explanation so "what caused this alert" is answerable later without
// re-deriving it.
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

// neighbor pairs a search hit's distance with its stored vector, so
// ScoreExplain can both average the distance (the score) and compute
// per-dimension deviations against the neighbor vectors (the explanation).
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
	// With Euclidean distance, Qdrant's search score IS the raw distance
	// (higher = farther = more anomalous) — there's no 1-minus-similarity
	// conversion to do, unlike Cosine's bounded [0,1] similarity. Verified
	// against a live search response rather than assumed.
	neighbors := make([]neighbor, len(out.Result))
	for i, r := range out.Result {
		neighbors[i] = neighbor{dist: r.Score, vec: r.Vector}
	}
	return neighbors, nil
}

// SeedHistory backfills userID's baseline directly from already-trusted past
// messages (e.g. an exported chat history) — the manual half of "historical
// vs live capture": messages are chunked into non-overlapping WindowSize
// groups and upserted the same way live scoring does, but with no
// search/threshold/logging step, since seed data isn't being gated, just
// used to build the baseline a new user would otherwise have to accumulate
// live over their first several real messages.
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
