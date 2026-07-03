# claude-relay — design

A Go broker that connects **message frontends** (Telegram, Discord, webhooks, …) to
**agent backends** (Claude Code, Ollama, OpenAI, …) through one symmetric interface, so
either side is swappable without touching the other.

```
   Telegram ─┐                              ┌── Claude Code  (stdio channel MCP)
   Discord  ─┤     ┌──────────────────┐     │
   Webhook  ─┼────▶│   RELAY DAEMON   │◀────┼── Ollama       (localhost:11434/v1)
   iMessage ─┤     │   (broker, Go)   │     │
   custom   ─┘     └──────────────────┘     └── OpenAI       (api.openai.com/v1)
     Frontend endpoints          ▲ router        Backend endpoints
```

## Core idea: both sides are the same interface

We do **not** model "sources" and "backends" as different abstractions. Everything is an
`Endpoint` — something that emits messages and accepts messages. A `Conversation` binds two
endpoints and pumps between them. Swapping either side is a config change.

```go
type Endpoint interface {
    Name() string
    Recv() <-chan Message                         // messages originating here
    Send(ctx context.Context, m Message) error    // deliver a message here
    Close() error
}

type Message struct {
    ConversationID string
    Role           Role              // User | Assistant | System
    Text           string
    Attachments    []Blob
    Meta           map[string]string // sender id, chat id, severity, model, …
    // Stream is optional incremental delivery (model tokens). Nil = whole message.
    Stream         <-chan Token
}
```

Broker is direction-agnostic:

```go
func Pump(a, b Endpoint) {
    go forward(a.Recv(), b)   // user → brain
    go forward(b.Recv(), a)   // brain → user
}
```

## The two endpoint families

Same interface, very different control flow underneath — deliberately hidden:

| Endpoint            | Who initiates    | Nature                       | Recv / Send mapping                                  |
|---------------------|------------------|------------------------------|------------------------------------------------------|
| Telegram / Discord  | polls platform   | event stream                 | Recv = user msgs · Send = post to chat               |
| Ollama / OpenAI     | **we call it**   | request → response (/stream) | Send(user) triggers API call → reply pushed to Recv  |
| Claude Code channel | **it spawns us** | persistent agent over stdio  | Send = `notifications/claude/channel` · Recv = `reply` tool call |

Two consequences that drive the architecture:

1. **Model backends are Send-driven.** Ollama/OpenAI have no independent event source; a
   reply exists only because we sent a turn. That endpoint owns conversation **history**
   (models are stateless — replay the transcript each turn).

2. **Claude Code inverts control.** It is not an API we call; it *launches* our channel
   MCP process over stdio and pushes events in. So the "Claude Code endpoint" is physically
   a **stdio shim** that Claude spawns, talking back to the long-lived daemon over local
   IPC. The broker sees a normal `Endpoint`; the inversion is contained in the adapter.

```
  [ relay daemon ]  ←— local IPC (unix socket) —→  [ stdio shim ]  ←— stdio —→  [ Claude Code ]
    hosts frontends,                                 tiny; per Claude
    router, history                                  session; spawned by Claude
```

## Claude Code channel protocol (what the shim implements)

A channel is an MCP server Claude Code spawns as a subprocess over **newline-delimited
JSON-RPC on stdio**. Channel-specific bits:

- **Capability:** `capabilities.experimental["claude/channel"] = {}` marks it a channel.
  Add `capabilities.tools = {}` for two-way, and `experimental["claude/channel/permission"]`
  to relay tool-approval prompts.
- **Inbound (→ Claude):** emit notification `notifications/claude/channel` with
  `{ content, meta }`. Claude sees `<channel source="telegram" chat_id="…">body</channel>`.
  Each `meta` key becomes a tag attribute (identifier chars only).
- **Outbound (Claude →):** expose an MCP tool (`reply`) with an input schema; Claude calls
  it, our handler forwards to the platform.
- **Sender gating is mandatory** — allowlist on *sender* id (not room id) before emitting.
  This is the prompt-injection boundary.

## Config expresses the binding

```yaml
conversations:
  - frontend: telegram
    backend:  claude-code       # swap to `ollama` / `openai` with one line
    allowlist: [ "5551212" ]
backends:
  claude-code: { session: "relay-main", channels: ["telegram"] }
  ollama:      { base_url: "http://localhost:11434/v1", model: "qwen3:8b" }
  openai:      { base_url: "https://api.openai.com/v1", model: "gpt-...", key_env: "OPENAI_API_KEY" }
```

This directly enables the token-offload roadmap item: route cheap chatter to local Ollama,
escalate to Claude for real work.

## Economics: how "listening" stays free

Listening costs **zero tokens**. The channel MCP subprocess (our Go program) does
the waiting with ordinary OS I/O — polling Telegram, holding an HTTP port. No model runs
while it waits. A message arriving **pushes** a `notifications/claude/channel` event over
stdio, which triggers exactly one model turn. It is interrupt-driven, not model-polling.

- Events **queue** in the session and **coalesce**: a burst delivered while Claude is busy
  is handled together on the next turn (one turn, not one-per-message).
- Cost is per *trigger*, but each turn replays the **accumulated session context** (LLM APIs
  are stateless), so a long-lived session's per-message cost creeps up as history grows.
  Mitigate with short/fresh sessions, compaction, or routing cheap chatter to Ollama.

## Auth & cost model

Channels require **first-party Anthropic auth** — a claude.ai **Pro/Max subscription** or a
Console API key. They are **not** available on Bedrock/Vertex/Foundry. So the relay runs on
an existing subscription; no metered API needed.

| | Subscription (our case) | Console API key |
|---|---|---|
| Channels work | ✅ | ✅ |
| Cost meter | counts against Pro/Max **usage limits** | per-token billing |
| Idle listening | free | free |

Because the meter is **subscription usage limits**, offloading trivial chatter to local
Ollama isn't about dollars — it's about **not burning Pro/Max quota**. The always-on
`claude --channels …` process inherits this machine's `jeanh` login; if it expires the
channel session needs re-auth (run it under systemd/tmux as `jeanh`).

## Control plane (deterministic, zero-token Go)

Two self-contained modules gate traffic **before** any model turn:

- **`internal/budget`** — account-tier rate limit + circuit breaker. Declare a tier
  (`free|pro|max5|max20`); the `Meter` enforces a rolling-window token budget and trips a
  breaker near the ceiling (`OpenAt`, default 0.9). Recovery for a usage trip is the window
  roll; a cooldown/half-open path exists for future error-based trips. Manual `Pause/Resume`.
  Tier numbers in `DefaultTiers` are **local estimates to tune**, not values from Anthropic.
- **`internal/command`** — slash-command interceptor. Any frontend message starting with
  `/` (`/help`, `/rate`, `/tier`, `/pause`, `/resume`, `/status`) is handled locally and
  replied to directly — it never reaches the model. This is how the operator toggles relay
  behavior from the chat/CLI source itself.

The `Broker` chains them: `frontend msg → command? → budget/circuit gate → backend`. When
the Claude circuit is open, the natural extension is to **fall back to the Ollama backend**
rather than reject — the symmetric `Endpoint` makes that a swap, not a rewrite.

## Constraints (known, design around them)

- **Research preview.** Channels need Claude Code ≥ v2.1.80 (have 2.1.199). `--channels`
  syntax / protocol contract may change. Custom (non-allowlisted) channels require
  `--dangerously-load-development-channels server:<name>`.
- **Auth:** channels require Anthropic auth via claude.ai / Console API key — not on
  Bedrock/Vertex/Foundry.
- **Session must stay open** — events arrive only while a session is live. Run
  `claude --channels …` under tmux/systemd on this always-on box.
- **Go MCP:** we hand-roll JSON-RPC over stdio (no SDK dependency). De-risked by PoC-1.

## Milestones

1. ✅ **PoC-2 — Endpoint/Broker + control plane**: symmetric `Endpoint`, `Message`, `Broker`,
   with `budget` (rate limit + breaker) and `command` (slash commands) wired to a CLI
   frontend + echo backend. Runnable: `cmd/broker-demo`. Budget breaker unit-tested.
2. ⏳ **PoC-1 — Go↔channel dialect** *(next, de-risk)*: prove the reusable `internal/mcp`
   server speaks the channel dialect — declare `claude/channel`, inject an event, expose a
   `reply` tool. HTTP POST in → Claude reacts → `reply` out. `cmd/channel-spike`.
3. **Telegram frontend endpoint** — getUpdates poll, sender allowlist, send.
4. **Claude Code backend endpoint** — shim + daemon IPC (built on PoC-1 + `internal/mcp`).
5. **Ollama backend endpoint** — `/v1/chat/completions`, history mgmt. Proves the swap,
   and doubles as the circuit-breaker fallback when the Claude budget trips.

## Layout

```
claude-relay/
  DESIGN.md
  go.mod
  cmd/broker-demo/        PoC-2: control-plane demo (CLI + echo + budget + commands) ✅
  cmd/channel-spike/      PoC-1: channel MCP server (stdio + HTTP inject)            ⏳
  internal/mcp/           reusable MCP-over-stdio JSON-RPC server (no deps)          ✅
  internal/channel/       Claude Code channel semantics on top of mcp               ⏳
  internal/relay/         Endpoint interface, Message envelope, Broker              ✅
  internal/budget/        account-tier rate limit + circuit breaker (+tests)        ✅
  internal/command/       slash-command control plane                               ✅
  internal/endpoint/cli/  CLI frontend endpoint                                      ✅
  internal/endpoint/echo/ echo backend endpoint                                      ✅
```
