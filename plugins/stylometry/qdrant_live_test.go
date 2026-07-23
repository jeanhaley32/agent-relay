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

// TestLive_ExactRepeatScoresLowerThanOutOfStyle is the real-world proof,
// scoped to what's actually reliably true of the current feature set: after
// building a baseline of consistent messages, a verbatim repeat of one of
// them must score lower (more like the sender) than a message written in a
// deliberately different register. This is the property that held up
// against the live instance; a stronger "the out-of-style message crosses
// some fixed absolute threshold" claim did not hold reliably with this
// feature set on short messages and is not asserted here — separation is
// real but modest, and AnomalyThreshold needs tuning against real traffic,
// not assumed from a handful of test messages.
func TestLive_ExactRepeatScoresLowerThanOutOfStyle(t *testing.T) {
	baseURL := requireLive(t)
	ctx := context.Background()
	userID := fmt.Sprintf("stylometry-live-test-%d", time.Now().UnixNano())

	d := NewDetector(baseURL, "stylometry_live_test")
	d.MinHistory = 5
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
