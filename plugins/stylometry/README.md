# stylometry

An optional, opt-in [`relay.AnomalyDetector`](../../internal/relay/anomaly.go)
implementation for [agent-relay](../..), backed by Qdrant. This is its own Go
module (see `go.mod`) so agent-relay's core has zero dependency on it, and
zero cost for anyone who doesn't wire it in.

## What it does

Scores how well a batch of messages matches a sender's own historical
writing style:

1. Messages are batched: `WindowSize` consecutive messages from the same
   sender are concatenated into one scoring unit before anything is
   compared — not scored one at a time. See "Why batching, not per-message"
   below; this was a measured decision, not a default.
2. `ExtractFeatures` computes a fixed-length vector from the batch —
   function-word frequencies (`the`, `and`, `is`, ...) plus a few shape
   statistics (average word/sentence length, punctuation/uppercase/digit
   ratios). Function words are the standard stylometry signal: people don't
   consciously choose "the" vs "a" the way they choose vocabulary, so their
   relative frequencies are a comparatively stable personal fingerprint.
3. The vector is stored in Qdrant, tagged with the sender's `user_id`.
4. Once a sender has `MinHistory` prior batches, each new batch is compared
   (Euclidean distance) against their `K` nearest historical batches; the
   average distance is the anomaly score, and `ScoreExplain` additionally
   returns which specific feature dimensions drove it (see "Explanations").

## Wiring it in

```go
d := stylometry.NewDetector("http://<qdrant-host>:6333", "stylometry")
d.Log = &stylometry.EventLog{Path: "/var/log/stylometry-events.jsonl"} // optional
if err := d.EnsureCollection(ctx); err != nil { /* ... */ }

broker.Anomaly = d
broker.AnomalyThreshold = /* tune against real traffic, see below */
broker.AnomalyWarnChatID = "<your admin chat_id>"
```

`d` satisfies `relay.AnomalyDetector` structurally (same `Score` method
signature) — no import of the stylometry package is needed inside
agent-relay itself. Because `Score` only returns a value once every
`WindowSize` messages (see below), the gate in `relay.go` won't fire on
every message from a sender — that's expected, not a bug.

## Why batching, not per-message

The first version scored every message individually. Measured against live
Qdrant, per-message scoring separated an in-style message from a
deliberately out-of-style one by only ~1.6x — weak, noisy signal. Batching
`WindowSize` consecutive messages into one scoring unit widened that to
~23x in a controlled comparison (non-overlapping batches, clean corpora) and
~8x in a more realistic end-to-end test (`TestLive_BatchingWidensSeparation`).
This matches the standard authorship-attribution finding that accuracy
scales with sample length — more text per sample means the function-word
frequency signal has more to work with.

**Batches are non-overlapping, not a sliding window** — this is itself a
correction, not the original design. A sliding window (append one message,
score against the last `WindowSize`, repeat every call) was tried and
measurably *didn't* reproduce the separation improvement above: successive
scored windows overlapped `WindowSize-1` messages, so each individual
message swap was noisy relative to whatever else happened to be in the
buffer at that moment, not a stable comparison. Non-overlapping batches
(message 1-5 scored together, then 6-10, ...) don't have that problem. The
tradeoff is real: detection only ever fires once every `WindowSize`
messages, and a compromised account gets up to `WindowSize` messages through
before the first batch is even scored — the right side to err on for signal
that fails open anyway, but worth knowing.

## Explanations (what caused an alert)

`ScoreExplain(ctx, userID, text)` returns a full `Explanation`, not just a
float:

- `Score` — the anomaly score (0 = not enough data yet, or genuinely close
  to baseline)
- `Window` — the actual concatenated text that was scored, once a batch
  completes
- `NeighborCount` — how many historical batches this was compared against
- `TopDeviations` — up to 5 feature dimensions (e.g. `fw:the`,
  `punctuation_ratio`) ranked by how far this batch's value sits from the
  historical average on that dimension, with the raw value/baseline/delta

`Score` (the `relay.AnomalyDetector`-satisfying method) is a thin wrapper
that just returns `.Score` from this.

## The event log (the "open interface" to inspect an alert)

Set `Detector.Log = &EventLog{Path: "..."}` and every scored batch appends
one JSON line — readable directly with `cat`/`grep`/`jq`, no query API
needed:

```sh
tail -f /var/log/stylometry-events.jsonl | jq 'select(.score > 2)'
```

## Historical seeding (backfilling a baseline)

A brand-new sender has to accumulate `MinHistory` live batches before
scoring means anything — `SeedHistory(ctx, userID, messages)` skips that
cold start by backfilling directly from already-trusted past messages (e.g.
an exported chat history), using the same non-overlapping batching and
feature extraction as live scoring, just without the search/threshold/log
step since seed data isn't being gated.

**Real constraint, not solved here:** agent-relay's own event log
deliberately never stores message bodies (see
`internal/eventlog/eventlog.go` — a privacy choice, not an oversight), so
there's no automated corpus inside agent-relay itself to backfill from.
`SeedHistory` takes whatever messages you hand it; where those messages come
from (an export, a different logging path you've deliberately opted into)
is a separate decision this package doesn't make for you.

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
- **A sliding window was tried and measurably didn't work** — see "Why
  batching, not per-message" above. Worth remembering if someone's tempted
  to "simplify" batching back into a sliding window later.
- **Separation is real and now substantial with batching, but
  `AnomalyThreshold` still needs tuning against real traffic**, not assumed
  from test messages — the numbers above (23x / 8x) are from controlled
  comparisons, not a promise about your actual users' message patterns.
- **This is soft, probabilistic signal, not proof of identity** — see the
  package doc comment in `features.go`. Style genuinely varies by
  mood/context/device, and anyone with access to a sender's message history
  could deliberately mimic it. Use as an advisory layer on top of real
  controls (session expiry, allowlists), never as the sole gate — which is
  exactly how the `relay.AnomalyDetector` hook treats it (fails open on
  error, only revokes an *already-authenticated* admin session, never
  blocks a non-admin outright).

## Testing

- `go test ./...` runs the pure unit tests (feature extraction, deviation
  ranking, event log) with no network dependency.
- `STYLOMETRY_LIVE_TESTS=1 QDRANT_URL=http://<host>:6333 go test ./... -run TestLive`
  runs the live tests against a real Qdrant instance (creates and deletes
  its own throwaway collections).
