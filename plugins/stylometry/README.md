# stylometry

An optional, opt-in [`relay.AnomalyDetector`](../../internal/relay/anomaly.go)
implementation for [agent-relay](../..), backed by Qdrant. This is its own Go
module (see `go.mod`) so agent-relay's core has zero dependency on it, and
zero cost for anyone who doesn't wire it in.

## What it does

Scores how well a message matches a sender's own historical writing style:

1. `ExtractFeatures` computes a fixed-length vector from the message —
   function-word frequencies (`the`, `and`, `is`, ...) plus a few shape
   statistics (average word/sentence length, punctuation/uppercase/digit
   ratios). Function words are the standard stylometry signal: people don't
   consciously choose "the" vs "a" the way they choose vocabulary, so their
   relative frequencies are a comparatively stable personal fingerprint.
2. The vector is stored in Qdrant, tagged with the sender's `user_id`.
3. Once a user has `MinHistory` prior points, each new message is compared
   (Euclidean distance) against their `K` nearest historical points; the
   average distance is the anomaly score.

## Wiring it in

```go
d := stylometry.NewDetector("http://<qdrant-host>:6333", "stylometry")
if err := d.EnsureCollection(ctx); err != nil { /* ... */ }

broker.Anomaly = d
broker.AnomalyThreshold = /* tune against real traffic, see below */
broker.AnomalyWarnChatID = "<your admin chat_id>"
```

`d` satisfies `relay.AnomalyDetector` structurally (same `Score` method
signature) — no import of the stylometry package is needed inside
agent-relay itself.

## Honest limitations (found by actually testing against live Qdrant, not assumed)

- **Qdrant's collection-create endpoint returns 409, not 200, when the
  collection already exists** — even with identical parameters, not just on
  a mismatch. `EnsureCollection` treats 409 as success; this was verified
  directly against a live instance after an initial wrong assumption that
  any non-2xx meant a real error.
- **Cosine distance does not work for this feature vector.** Function-word
  frequencies are mostly 0 for short messages, and cosine similarity is
  direction-only — it saturates near 1.0 for any two such sparse vectors
  regardless of how different their nonzero values actually are. Verified
  concretely: with Cosine, an exact repeat of a message and a completely
  unrelated one scored within 0.01 of each other. The collection is created
  with Euclidean distance instead, which discriminates properly.
- **Separation is real but modest with this feature set on short messages.**
  A verbatim repeat of a baseline message reliably scores lower than a
  deliberately different-register message (this is what
  `TestLive_ExactRepeatScoresLowerThanOutOfStyle` actually asserts), but the
  gap was on the order of 0.17 vs 0.21 in testing — not a dramatic split.
  `AnomalyThreshold` needs tuning against real traffic from real users, not
  assumed from a handful of test messages. If separation proves too weak in
  practice, the function-word list or feature weighting is the first place
  to revisit, not the distance metric (already fixed above).
- **This is soft, probabilistic signal, not proof of identity** — see the
  package doc comment in `features.go`. Style genuinely varies by
  mood/context/device, and anyone with access to a sender's message history
  could deliberately mimic it. Use as an advisory layer on top of real
  controls (session expiry, allowlists), never as the sole gate — which is
  exactly how the `relay.AnomalyDetector` hook treats it (fails open on
  error, only revokes an *already-authenticated* admin session, never
  blocks a non-admin outright).

## Testing

- `go test ./...` runs the pure unit tests (feature extraction) with no
  network dependency.
- `STYLOMETRY_LIVE_TESTS=1 QDRANT_URL=http://<host>:6333 go test ./... -run TestLive`
  runs the live tests against a real Qdrant instance (creates and deletes
  its own throwaway collection).
