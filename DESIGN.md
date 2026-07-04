# agent-relay — design

## What this is

`agent-relay` routes Telegram messages to a Claude Code session and sends the replies back,
with a control plane in between: allowlist + admin approval, rate limit / circuit breaker,
and tool-approval relay. This document describes how that's built.

The internals are factored around a generic `Endpoint` abstraction, which leaves room for
other frontends or backends; those are sketched under [Design intent](#design-intent-not-built).

```
  Telegram DM ──▶ relayd ──▶ [ command? · access gate · budget/circuit ] ──▶ Claude Code
       ▲                                                                          │
       └──────────────────────────────── reply ──────────────────────────────────┘
```

## The `Endpoint` abstraction

Internally both sides of a conversation implement one interface, so the broker code is the
same regardless of which concrete endpoint sits on each end. Today the only frontend is
Telegram and the only real backend is Claude Code (plus an `echo` backend for tests/demo).

```go
type Endpoint interface {
    Name() string
    Recv() <-chan Message                      // messages originating at this endpoint
    Send(ctx context.Context, m Message) error // deliver a message to this endpoint
    Close() error
}

type Message struct {
    ConversationID string
    Role           Role              // User | Assistant | System
    Text           string
    Meta           map[string]string // chat_id, from_id, model, …
}
```

The `Broker` binds a frontend to a backend and pumps both directions, screening slash
commands and gating model turns through a budget meter:

```
Broker.Run:  frontend.Recv ─▶ command? ─▶ budget/circuit gate ─▶ backend.Send
             backend.Recv  ─▶ (meter the reply) ─▶ frontend.Send
```

## The Claude Code backend (daemon + shim)

Claude Code is not an API we call — it **spawns its channel over stdio**. A long-lived
multi-conversation daemon can't itself be that stdio child, so the backend is split:

```
  [ relayd daemon ]  ◀— unix socket (IPC) —▶  [ relay-shim ]  ◀— stdio —▶  [ Claude Code ]
    hosts Telegram, broker,                     tiny; spawned by
    budget, access, perms                       Claude per session
```

- **`internal/endpoint/claude`** — the daemon-side `Endpoint`. Listens on the socket; `Send`
  emits an `inject` frame; `reply` frames surface on `Recv`; `Permissions()` surfaces tool
  prompts and `Decide()` answers them.
- **`cmd/relay-shim`** — the stdio bridge (built on `internal/channel`). It translates IPC
  frames ↔ the Claude channel, **auto-reconnects** to the daemon, and **buffers outbound
  frames** while disconnected — so restarting `relayd` loses no replies and keeps Claude's
  context.
- **`internal/ipc`** — the newline-JSON frame protocol: `inject`/`reply` and
  `perm_request`/`perm_verdict`.

## Claude Code channel protocol (what the shim implements)

A channel is an MCP server Claude Code spawns as a subprocess over **newline-delimited
JSON-RPC on stdio**. We hand-roll it in Go (`internal/mcp`) — no SDK dependency.

- **Capability:** `capabilities.experimental["claude/channel"] = {}` marks it a channel;
  `capabilities.tools = {}` for two-way; `experimental["claude/channel/permission"]` to
  relay tool-approval prompts.
- **Inbound (→ Claude):** notification `notifications/claude/channel` with `{ content, meta }`
  → Claude sees `<channel source="relay" chat_id="…">body</channel>`.
- **Outbound (Claude →):** an MCP `reply` tool Claude calls; our handler routes it back.
- **Permission relay:** `notifications/claude/channel/permission_request` (in) and
  `…/permission` (verdict out) forward tool approvals to admins and return their answer.

## Access control

`internal/access` — the authorization boundary and the anti-prompt-injection gate:

- Gate on the **sender's user id** (not chat id), fail-closed (empty allowlist denies all).
- **Admins** are auto-allowed and can run admin commands.
- Unauthorized senders are recorded as **pending requests**; an admin runs `/handshake
  approve <id>` to grant access. Approvals persist to a JSON file the relay manages.

## Permission relay

An unattended session must never block on a terminal prompt. When Claude wants a tool that
needs approval, the request is forwarded down the pipeline to admins' Telegram; they reply
`/allow <id>` / `/deny <id>` and the verdict flows back. The complementary control is
bounding the toolset via `.claude/settings.json` (deny dangerous tools, allow safe ones).

## Control plane (deterministic, zero-token Go)

Gates traffic **before** any model turn — so commands, rejections, and rate-limits cost
nothing:

- **`internal/budget`** — account-tier rate limit + circuit breaker. A tier
  (`free|pro|max5|max20`) sets a rolling-window token budget; the breaker trips near the
  ceiling and recovers on the window roll. Manual `/pause` `/resume`. Tier numbers are
  **local estimates to tune**, not values from Anthropic.
- **`internal/command`** — slash-command interceptor. Messages starting with `/` are handled
  locally and never reach the model. Handlers receive a `Context{SenderID, ChatID}` so
  admin-only commands (`/handshake`, `/allow`, `/deny`) can gate on the sender.

## Economics: how "listening" stays free

Listening costs **zero tokens** — the shim waits on OS I/O (Telegram long-poll); no model
runs until a message arrives, which **pushes** an event that triggers exactly one turn.

- Events **queue and coalesce**: a burst delivered while Claude is busy is handled together.
- Each turn replays the **accumulated session context** (LLM APIs are stateless), so a
  long-lived session's per-message cost creeps up as history grows. Default to Sonnet;
  escalate hard tasks to an Opus **subagent** rather than raising the whole session's model.

## Auth & cost model

Channels require **first-party Anthropic auth** — a claude.ai **Pro/Max subscription** or a
Console API key. **Not** available on Bedrock/Vertex/Foundry.

| | Subscription | Console API key |
|---|---|---|
| Channels work | ✅ | ✅ |
| Cost meter | Pro/Max **usage limits** | per-token billing |
| Idle listening | free | free |

Because the meter is subscription usage, keeping the session on **Sonnet** (and using Opus
only via subagents) conserves quota. The always-on `claude` process runs under the operating
user's login; if it expires the session needs re-auth (run under tmux/systemd).

## Constraints

- **Research preview.** Channels need Claude Code ≥ v2.1.80. The `--channels` contract may
  change. Custom (non-allowlisted) channels require `--dangerously-load-development-channels`,
  which prompts for a one-time consent per launch — this can't be scripted away.
- **Session must stay open** — events arrive only while the Claude session is live.
- **Single shared session (today).** All conversations route to one Claude session, so they
  share context — fine for a single user, not isolated for many. See design intent below.

## Config

JSON (dependency-free). The bot token is referenced by env-var **name**, never stored.

```json
{
  "telegram": {
    "token_env": "TELEGRAM_BOT_TOKEN",
    "admins": [123456789],
    "allowlist": [],
    "allowlist_file": "allowlist.json",
    "poll_timeout": 30
  },
  "claude": { "socket": "/tmp/agent-relay.sock" },
  "budget": { "tier": "max5" }
}
```

## Layout

```
agent-relay/
  cmd/relayd/             the daemon: wires Telegram ⇄ broker ⇄ Claude from config
  cmd/relay-shim/         stdio bridge Claude spawns; reconnects + buffers
  cmd/broker-demo/        control-plane demo (CLI + echo, no bot/Claude needed)
  cmd/channel-spike/      standalone channel server used to prove the protocol
  internal/relay/         Endpoint, Message, Broker, StandardCommands
  internal/budget/        account-tier rate limit + circuit breaker (+tests)
  internal/command/       slash-command control plane
  internal/access/        allowlist + admins + pending requests (+tests)
  internal/mcp/           hand-rolled MCP-over-stdio JSON-RPC server
  internal/channel/       Claude Code channel semantics on mcp (+tests)
  internal/ipc/           daemon↔shim frame protocol (+tests)
  internal/config/        JSON config loader (+tests)
  internal/endpoint/telegram/  Telegram frontend (+tests)
  internal/endpoint/claude/    Claude backend: socket listener + shim bridge (+tests)
  internal/endpoint/{cli,echo}/  demo endpoints
  scripts/run.sh          one-command launch (Sonnet default)
```

---

## Design intent (not built)

Directions the `Endpoint` factoring leaves room for. None are built yet.

- **Other frontends** (Discord, iMessage, webhooks) — each would be another `Endpoint`
  implementation with its own sender-gating.
- **Model backends (Ollama, OpenAI)** — unlike Claude Code, these are call-out request/
  response APIs: the endpoint would own conversation history and context-window trimming.
  An Ollama backend could also serve as the **circuit-breaker fallback** (route cheap chatter
  locally when the Claude budget trips) and the token-offload path.
- **Per-user sessions** — spawn/route a separate Claude session per user for context
  isolation. Requires a session supervisor, per-session budgeting, non-interactive spawn
  (pre-seeded consents), and sandboxing. Resolves the single-shared-session limitation.
- **Config-driven binding** — a config that names `frontend`/`backend` per conversation so
  swapping either is a one-line change.
