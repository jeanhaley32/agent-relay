package stylometry

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// requireLive skips unless STYLOMETRY_LIVE_TESTS=1, so `go test ./...`
// never silently touches a real Qdrant instance — same gating pattern used
// elsewhere in this project's live-test files.
func requireLive(t *testing.T) string {
	t.Helper()
	if os.Getenv("STYLOMETRY_LIVE_TESTS") != "1" {
		t.Skip("set STYLOMETRY_LIVE_TESTS=1 and QDRANT_URL to run against a real Qdrant instance")
	}
	url := os.Getenv("QDRANT_URL")
	if url == "" {
		t.Fatal("QDRANT_URL must be set when STYLOMETRY_LIVE_TESTS=1")
	}
	return url
}

// TestLive_ExactRepeatScoresLowerThanOutOfStyle asserts a repeat of a
// baseline message scores lower than a deliberately out-of-style one.
// WindowSize=1 here; batching is covered separately by
// TestLive_BatchingWidensSeparation.
func TestLive_ExactRepeatScoresLowerThanOutOfStyle(t *testing.T) {
	baseURL := requireLive(t)
	ctx := context.Background()
	userID := fmt.Sprintf("stylometry-live-test-%d", time.Now().UnixNano())

	d := NewDetector(baseURL, "stylometry_live_test")
	d.MinHistory = 5
	d.WindowSize = 1
	if err := d.EnsureCollection(ctx); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}

	baseline := []string{
		"I think we should look at the numbers again before deciding.",
		"The report is ready and I sent it to the team this morning.",
		"Can you check if the server is still running fine today?",
		"I was thinking about the plan and it makes sense to me now.",
		"We should probably wait until the tests are done before shipping.",
		"The results look good but I want to double check them first.",
	}
	for _, msg := range baseline {
		if _, err := d.Score(ctx, userID, msg); err != nil {
			t.Fatalf("Score(baseline): %v", err)
		}
	}

	repeatScore, err := d.Score(ctx, userID, baseline[3])
	if err != nil {
		t.Fatalf("Score(exact repeat): %v", err)
	}
	t.Logf("exact-repeat-of-baseline score: %.3f", repeatScore)

	outOfStyle := "yo lol nah fr fr bro thats crazy no cap deadass"
	outScore, err := d.Score(ctx, userID, outOfStyle)
	if err != nil {
		t.Fatalf("Score(out-of-style): %v", err)
	}
	t.Logf("out-of-style score: %.3f", outScore)

	if repeatScore >= outScore {
		t.Fatalf("expected a verbatim repeat of a baseline message to score lower than a deliberately out-of-style one (repeat=%.3f, out-of-style=%.3f)", repeatScore, outScore)
	}
}

// TestLive_BelowMinHistoryAlwaysScoresZero confirms a fresh user (no prior
// points) never gets flagged purely for lack of a baseline.
func TestLive_BelowMinHistoryAlwaysScoresZero(t *testing.T) {
	baseURL := requireLive(t)
	ctx := context.Background()
	userID := fmt.Sprintf("stylometry-live-test-fresh-%d", time.Now().UnixNano())

	d := NewDetector(baseURL, "stylometry_live_test")
	d.MinHistory = 10
	if err := d.EnsureCollection(ctx); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}

	score, err := d.Score(ctx, userID, "completely random first ever message")
	if err != nil {
		t.Fatalf("Score: %v", err)
	}
	if score != 0 {
		t.Fatalf("expected score 0 for a user with no history yet, got %.3f", score)
	}
}

// TestLive_BatchingWidensSeparation asserts WindowSize=5 separates in-style
// from out-of-style batches more than WindowSize=1 — the measured result
// behind picking 5 as the default.
func TestLive_BatchingWidensSeparation(t *testing.T) {
	baseURL := requireLive(t)
	ctx := context.Background()

	styleA := []string{
		"I think we should look at the numbers again before deciding.",
		"The report is ready and I sent it to the team this morning.",
		"Can you check if the server is still running fine today?",
		"I was thinking about the plan and it makes sense to me now.",
		"We should probably wait until the tests are done before shipping.",
	}
	styleB := []string{
		"yo lol nah fr fr bro thats crazy no cap deadass",
		"ngl this hits different fr, lowkey obsessed rn tbh",
		"bruh moment fr, cant even rn lmaooo dead",
		"sheesh that slaps ngl, bussin fr fr no cap",
		"lowkey vibing rn ngl, this energy is unmatched fr",
	}

	// sendBatch sends exactly windowSize messages, which always completes
	// exactly one batch given a fresh (or already batch-aligned) buffer.
	sendBatch := func(d *Detector, userID string, msgs []string, windowSize int) float64 {
		t.Helper()
		var last float64
		for i := 0; i < windowSize; i++ {
			s, err := d.Score(ctx, userID, msgs[i%len(msgs)])
			if err != nil {
				t.Fatalf("Score: %v", err)
			}
			last = s
		}
		return last
	}

	gapFor := func(windowSize int) float64 {
		userID := fmt.Sprintf("stylometry-window-test-%d-%d", windowSize, time.Now().UnixNano())
		d := NewDetector(baseURL, "stylometry_live_test")
		d.MinHistory = 2
		d.WindowSize = windowSize
		if err := d.EnsureCollection(ctx); err != nil {
			t.Fatalf("EnsureCollection: %v", err)
		}
		// Two baseline batches to clear MinHistory, then one repeat-of-A
		// batch and one out-of-style (B) batch, each on a clean boundary.
		sendBatch(d, userID, styleA, windowSize)
		sendBatch(d, userID, styleA, windowSize)
		repeat := sendBatch(d, userID, styleA, windowSize)
		out := sendBatch(d, userID, styleB, windowSize)
		return out - repeat
	}

	gap1 := gapFor(1)
	gap5 := gapFor(5)
	t.Logf("gap at WindowSize=1: %.4f, WindowSize=5: %.4f", gap1, gap5)
	if gap5 <= gap1 {
		t.Fatalf("expected WindowSize=5 to separate in-style from out-of-style more than WindowSize=1 (gap1=%.4f, gap5=%.4f)", gap1, gap5)
	}
}

// TestLive_SeedHistoryEnablesImmediateScoring is the real-world proof for
// the manual historical-backfill path: after SeedHistory, a fresh user (who
// has never sent a live message) should already score against that seeded
// baseline instead of needing to accumulate MinHistory live first.
func TestLive_SeedHistoryEnablesImmediateScoring(t *testing.T) {
	baseURL := requireLive(t)
	ctx := context.Background()
	userID := fmt.Sprintf("stylometry-seed-test-%d", time.Now().UnixNano())

	d := NewDetector(baseURL, "stylometry_live_test")
	d.MinHistory = 1
	d.WindowSize = 3
	if err := d.EnsureCollection(ctx); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}

	history := []string{
		"I think we should look at the numbers again.",
		"The report is ready and I sent it this morning.",
		"Can you check if the server is running fine?",
		"I was thinking about the plan, makes sense now.",
		"We should wait until the tests are done first.",
		"The results look good, want to double check.",
	}
	if err := d.SeedHistory(ctx, userID, history); err != nil {
		t.Fatalf("SeedHistory: %v", err)
	}

	count, err := d.userPointCount(ctx, userID)
	if err != nil {
		t.Fatalf("userPointCount: %v", err)
	}
	if count == 0 {
		t.Fatal("expected SeedHistory to have created at least one baseline point")
	}

	// A full WindowSize=3 batch is needed to complete a scored batch; only
	// the last of these 3 calls actually reaches Qdrant with a result.
	postSeed := []string{
		"I think we should check the report once more.",
		"Can you look at this again when you get a chance?",
		"The numbers seem fine but let's verify tomorrow.",
	}
	var exp Explanation
	for _, msg := range postSeed {
		exp, err = d.ScoreExplain(ctx, userID, msg)
		if err != nil {
			t.Fatalf("ScoreExplain after seed: %v", err)
		}
	}
	if exp.NeighborCount == 0 {
		t.Fatal("expected the first completed batch after seeding to already have neighbors to compare against")
	}
}

// TestLive_ScoreExplainLogsToEventLog confirms Log actually gets written to
// when set — the "open interface" for inspecting what caused an alert.
func TestLive_ScoreExplainLogsToEventLog(t *testing.T) {
	baseURL := requireLive(t)
	ctx := context.Background()
	userID := fmt.Sprintf("stylometry-log-test-%d", time.Now().UnixNano())
	logPath := t.TempDir() + "/events.jsonl"

	d := NewDetector(baseURL, "stylometry_live_test")
	d.WindowSize = 1 // one message is one complete batch, simplest case for this test
	d.Log = &EventLog{Path: logPath}
	if err := d.EnsureCollection(ctx); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}

	if _, err := d.ScoreExplain(ctx, userID, "a message that should get logged"); err != nil {
		t.Fatalf("ScoreExplain: %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("expected the log file to have been created: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected at least one logged line")
	}
}
