package relay

import "context"

// AnomalyDetector scores whether a message plausibly came from userID, given
// whatever history/model the implementation keeps. Higher is more anomalous;
// the scale is the implementation's choice, compared against Broker's
// AnomalyThreshold. This package defines only the seam — no concrete
// implementation lives here, so agent-relay's core has no hard dependency on
// a vector database, an embedding model, or any other detector-specific
// infrastructure. A deployment that wants this wires in its own
// AnomalyDetector; everyone else leaves Broker.Anomaly nil and pays nothing.
type AnomalyDetector interface {
	Score(ctx context.Context, userID, text string) (float64, error)
}
