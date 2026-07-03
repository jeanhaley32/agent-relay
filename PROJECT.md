# agent-relay — project tracker

**Purpose of this file:** the single source of truth for *where the project is* and *what to
do next*. A model or contributor should be able to read this top-to-bottom and continue
development without additional context. It complements — does not duplicate — the other docs:

- [`DESIGN.md`](./DESIGN.md) — architecture, the symmetric-Endpoint model, economics, auth.
- [`README.md`](./README.md) — one-screen overview + how to try it.
- **`PROJECT.md`** (this file) — status, work log, decisions, conventions, and the TODO.

Keep this file current: when you finish work, update **Status**, the **Work log**, and the
**TODO**; when you make a design decision, append to the **Decision log**.

---

## 1. What this is (30-second orientation)

A Go broker that connects **message frontends** (Telegram, CLI, webhooks) to **agent
backends** (Claude Code, Ollama, OpenAI) through **one symmetric `Endpoint` interface**, so
either side is swappable by config. It runs on an always-on, Tailscale-only ThinkPad
(see the machine's `~/CLAUDE.md`). The near-term goal: text a Telegram bot and have it
driven by Claude Code (on a subscription, not metered API), with local Ollama offload for
cheap traffic to conserve subscription quota.

Repo: `github.com/jeanhaley32/agent-relay` (private). Local: `~/agent-relay`. Go 1.24.

## 2. Status snapshot

| Milestone | State | Notes |
|---|---|---|
| Core: Endpoint + Broker + control plane | ✅ done | `internal/relay`, unit-tested control flow via demo |
| Budget: rate limit + circuit breaker | ✅ done | `internal/budget` + tests |
| Command: slash-command control plane | ✅ done | `internal/command` |
| MCP: reusable stdio JSON-RPC server | ✅ done | `internal/mcp` |
| **PoC-1: Go↔Claude Code channel dialect** | ✅ **validated live** | `internal/channel` + `cmd/channel-spike`; see §5 |
| PoC-2: control-plane demo (CLI+echo) | ✅ done | `cmd/broker-demo` |
| Telegram frontend endpoint | ⬜ todo | §8 item T1 |
| Claude Code backend endpoint (daemon+shim) | ⬜ todo | §8 item C1 |
| Ollama backend endpoint (+breaker fallback) | ⬜ todo | §8 item O1 |
| Config loader + daemon wiring | ⬜ todo | §8 item D1 |
| CI (build+test on push) | ⬜ todo | §8 item X1 |

Branch state: PoC-1 on `poc-1-channel` → **PR #1** (open). `main` has the core + control plane.

## 3. Architecture in brief (full detail in DESIGN.md)

Everything is an `Endpoint` (emits + accepts `Message`s). A `Broker` binds two endpoints and
pumps between them, intercepting slash commands and gating model turns through a budget
`Meter`. Frontends and backends are the same interface — that is the whole point.

```
frontend Endpoint ─▶ Broker[ command? → budget/circuit gate ] ─▶ backend Endpoint
        ▲───────────────────────  reply pumped back  ──────────────────────┘
```

Two backend control-flow shapes hidden behind the one interface:
- **Model backends** (Ollama/OpenAI): *we call them*; `Send(user)` triggers an API call and
  the reply is pushed to `Recv`. The endpoint owns conversation history.
- **Claude Code**: *it spawns us* over stdio. The "Claude endpoint" is a long-lived daemon +
  a per-session **stdio shim** (an MCP channel server) talking over local IPC. PoC-1 proved
  the shim half works.

## 4. Module map (what's built)

| Package | Responsibility | Key types / entry points | Reusable? |
|---|---|---|---|
| `internal/mcp` | Generic MCP-over-stdio JSON-RPC 2.0 server. Handshake, tools, notifications. No deps. | `New`, `Server.RegisterTool`, `Server.Notify`, `Server.Serve`, `AddExperimentalCapability` | ✅ any MCP work |
| `internal/channel` | Claude Code channel semantics on top of `mcp`: `claude/channel` capability, inbound event injection, `reply` tool. | `New(name,ver,instructions,onReply)`, `Server.Inject(content,meta)`, `Server.Serve` | Claude-specific |
| `internal/relay` | Transport-neutral core: symmetric `Endpoint`, `Message`, `Broker`. | `Endpoint`, `Message`, `Broker.Run`, `UserMsg`/`AssistantMsg`, `DefaultEstimator` | ✅ core |
| `internal/budget` | Account-tier rolling-window rate limit + circuit breaker. Clock-injectable. | `New(tier,clock)`, `Meter.Allow`, `Meter.Record`, `Meter.Snapshot`, `Pause/Resume/SetTier`, `DefaultTiers` | ✅ any governor |
| `internal/command` | Slash-command interceptor. | `NewRegistry`, `Registry.Register`, `Registry.Dispatch`, `IsCommand` | ✅ any control plane |
| `internal/endpoint/cli` | Frontend endpoint over stdin/stdout (stands in for a chat platform). | `New(conv,in,out)` | demo |
| `internal/endpoint/echo` | Backend endpoint that echoes (stands in for a model). | `New()` | demo |
| `cmd/broker-demo` | Runnable control-plane demo (CLI + echo + budget + commands). | `main` | — |
| `cmd/channel-spike` | Real channel server: Claude spawns over stdio; HTTP inject (POST `/`) + SSE (`GET /events`). | `main`, `--addr` | — |

## 5. Validated results

**PoC-1 live test (2026-07-03, Claude Code v2.1.200):** full bidirectional round-trip through
the Go channel, confirmed against real Claude Code:

1. Claude spawned `./bin/channel-spike` over stdio (via `.mcp.json`), HTTP port came up.
2. `curl -s localhost:8799 -d "what files are in this directory?"` → server emitted
   `notifications/claude/channel`.
3. Claude rendered `← channel-spike: what files…`, ran a tool, formed an answer.
4. Claude called our `reply` tool; approving it broadcast
   `data: reply chat_id=1: Files: DESIGN.md, README.md, …` on the SSE stream.

Also covered by automated tests: `internal/channel/channel_test.go` (in-process handshake
simulation) and `internal/budget/budget_test.go` (breaker trip/recover, pause, tier
fallback). Compiled-binary smoke test verified stdio + HTTP injection produce correct wire
output.

**Live-run learnings:**
- The `reply` tool triggers a **permission prompt** each call. For an unattended relay, use
  the "don't ask again for channel-spike - reply" option or a scoped allow-rule; longer term,
  wire the channel **permission-relay** capability so approvals can come from the frontend.
- Everything is **zero tokens until a message is injected** — confirms the event-driven model.
- tmux servers started from i3 lacked `~/.local/bin` on PATH; fixed by exporting it in
  `~/.xinitrc`. Use absolute `claude` path in scripts to be safe (`scripts/live-test.sh`).

## 6. Decision log (rationale, newest first)

- **D7 Reply permission:** unattended operation needs the reply tool pre-approved (allow-rule
  or permission-relay), discovered in the PoC-1 live run.
- **D6 Hand-roll MCP in Go (no SDK):** removes the "does a Go MCP SDK support experimental
  capabilities + custom notifications" risk; newline-delimited JSON-RPC is small and offline.
  Validated by PoC-1.
- **D5 Control plane in Go, pre-model:** slash commands + budget/circuit gate run in
  deterministic zero-token Go *before* any model turn; this is where offload/limits live.
- **D4 Subscription auth is fine:** channels work on claude.ai Pro/Max (not Bedrock/Vertex/
  Foundry). Cost meter = subscription usage, so Ollama offload conserves quota, not dollars.
- **D3 Daemon + stdio shim split for Claude:** Claude *spawns* the channel over stdio, so a
  long-lived multi-channel relay can't itself be that process; a thin per-session shim talks
  to the daemon over local IPC.
- **D2 Symmetric `Endpoint` for both sides:** frontends and backends share one interface so
  either is swappable; the broker is direction-agnostic.
- **D1 Strong, self-contained modules:** core (`mcp`, `relay`, `budget`, `command`) knows
  nothing about any platform; platforms are `Endpoint` implementations.

## 7. Conventions

- **Modularity:** keep the core platform-agnostic. New platforms/models = new packages under
  `internal/endpoint/…` implementing `relay.Endpoint`. Channel/MCP specifics stay in
  `internal/channel` / `internal/mcp`.
- **stdio hygiene:** an MCP server may write **only** JSON-RPC to stdout; all logs go to stderr.
- **Testing:** every core module gets tests; inject clocks/pipes rather than sleeping. Run
  `go vet ./... && go test ./...` before commit.
- **Git:** work on feature branches, open a PR, don't commit to `main` directly. Commit
  messages end with the `Co-Authored-By: Claude …` trailer; PR bodies end with the Claude Code
  generated-with line.
- **Secrets:** bot tokens / API keys via env or `.env` (git-ignored). Never commit them.

## 8. TODO (with acceptance criteria)

Ordered by the critical path. Each item names files to add and a "done when" bar.

- **T1 — Telegram frontend endpoint** *(next)*
  `internal/endpoint/telegram`. Long-poll `getUpdates`, normalize to `relay.Message`,
  send via `sendMessage`. **Mandatory sender allowlist** (gate on `from.id`, not chat id).
  Token from `TELEGRAM_BOT_TOKEN` env. *Done when:* a broker wires Telegram↔echo and a real
  bot DM round-trips through it with a non-allowlisted sender dropped.

- **C1 — Claude Code backend endpoint** (daemon + shim)
  Wrap PoC-1 into a `relay.Endpoint`. Design the daemon↔shim IPC (unix socket) so the shim
  (built on `internal/channel`) forwards injected events and relays `reply` calls back to the
  daemon. Decide: one long-lived Claude session vs. per-conversation. Pre-approve the reply
  tool (allow-rule) or implement permission-relay. *Done when:* the broker drives a live
  Claude session as a backend and a message injected via the frontend gets a Claude reply.

- **O1 — Ollama backend endpoint**
  `internal/endpoint/ollama`. Call `POST {base_url}/v1/chat/completions`, manage per-
  conversation history, stream or batch the reply onto `Recv`. Wire as the **circuit-breaker
  fallback**: when the Claude budget trips, route to Ollama instead of rejecting. *Done when:*
  `/backend ollama` (or an open breaker) routes a turn to a local model and replies.

- **D1 — Config + daemon wiring**
  `internal/config` (YAML per DESIGN.md) + a `cmd/relayd` that reads config, builds endpoints,
  and runs brokers. *Done when:* a single config file starts Telegram↔(Claude|Ollama) with the
  budget tier and allowlist applied.

- **X1 — CI**: GitHub Actions running `go vet` + `go build` + `go test` on push/PR.

- **Nice-to-haves:** tests for `command`, `relay`, `mcp`; structured logging; `/backend`,
  `/tier`, `/help` commands surfaced through a real frontend; permission-relay (approve tool
  use from the chat app); metrics for usage/turns.

## 9. Constraints & risks

- **Channels are research-preview** (need Claude Code ≥ v2.1.80; have 2.1.200). `--channels`
  syntax/contract may change. Custom channels need `--dangerously-load-development-channels`.
- **Auth:** claude.ai subscription or Console API key only; not Bedrock/Vertex/Foundry. The
  always-on session inherits this box's `jeanh` login; re-auth needed if it expires.
- **Ollama not installed yet** (roadmap item on the box). CPU-only here → 4B–8B models.
- **Budget numbers in `DefaultTiers` are local estimates**, not Anthropic's real limits — tune.

## 10. How to build / test / run

```bash
cd ~/agent-relay
go vet ./... && go test ./...                 # verify
printf '/tier pro\n/rate\nhi\n' | go run ./cmd/broker-demo   # control-plane demo
go build -o bin/channel-spike ./cmd/channel-spike            # build the channel spike
bash scripts/live-test.sh                                     # live test vs Claude Code (tmux)
```

Live test details and the manual 3-terminal flow are in `scripts/live-test.sh` and the PR #1
description.

## 11. Work log (newest first)

- **2026-07-03** — PoC-1 built and **validated live** against Claude Code v2.1.200 (full
  round-trip). Added `internal/channel`, `cmd/channel-spike`, `.mcp.json`,
  `scripts/live-test.sh`, tests. Opened PR #1. Fixed i3/tmux PATH via `~/.xinitrc`.
- **2026-07-03** — Built the core: `internal/{relay,budget,command,mcp,endpoint/cli,endpoint/
  echo}` and `cmd/broker-demo`. Budget breaker unit-tested. Wrote `DESIGN.md`, `README.md`,
  MIT `LICENSE`. Created private GitHub repo, pushed `main`.
