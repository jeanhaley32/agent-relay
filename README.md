# agent-relay

A Go broker that connects **message frontends** (Telegram, CLI, webhooks) to **agent
backends** (Claude Code, Ollama, OpenAI) through one symmetric interface — so either side
is swappable without touching the other.

```
   Telegram ─┐                              ┌── Claude Code  (stdio channel MCP)
   CLI      ─┼────▶  RELAY DAEMON (broker) ─┼── Ollama       (localhost:11434/v1)  [planned]
   Webhook  ─┘         commands + budget    └── OpenAI       (api.openai.com/v1)   [planned]
     frontend Endpoints        gate            backend Endpoints
```

## Idea

Everything is an `Endpoint` — a thing that emits and accepts messages. A `Broker` binds two
endpoints and pumps between them, intercepting slash commands and gating model turns through
an account-tier rate limiter + circuit breaker. Both sides are the same interface, so a
frontend (Telegram) and a backend (Claude) are peers, not special cases.

See [`DESIGN.md`](./DESIGN.md) for architecture and [`PROJECT.md`](./PROJECT.md) for status.

## What works today

A complete **Telegram ⇄ Claude Code** relay: DM a bot and it's answered by a Claude Code
session running on your machine, gated by a rate-limit/circuit-breaker control plane, with
admin-approved access and tool-permission approval from chat. Ollama/OpenAI backends are
designed (symmetric `Endpoint`) but not yet built.

## Requirements

- **Go 1.24+** (to build)
- **Claude Code ≥ v2.1.80**, authenticated with a **first-party Anthropic account**
  (claude.ai Pro/Max subscription or a Console API key). Channels are a *research preview*
  and are **not** available on Bedrock/Vertex/Foundry.
- A **Telegram bot** (via [@BotFather](https://t.me/BotFather)) and your numeric Telegram
  **user id** (e.g. from [@userinfobot](https://t.me/userinfobot)).
- **tmux** (used by the launch script to host the interactive Claude session).

## Setup

```bash
# 1. Build
go build ./...

# 2. Create a Telegram bot with @BotFather, then store the token (never committed)
echo 'TELEGRAM_BOT_TOKEN=123456:your-bot-token' > .env

# 3. Configure — copy the example and set YOUR Telegram user id as admin
cp config.example.json config.json
#    edit config.json: "admins": [ <your-telegram-user-id> ]

# 4. Launch the stack (builds, starts relayd, opens Claude on Sonnet in tmux)
bash scripts/run.sh

# 5. Approve the one-time "development channels" prompt
tmux attach -t relay      # press Enter at the prompt, then Ctrl-b d to detach
```

Now DM your bot — it should reply. For heavier tasks, ask it to *"spawn a subagent on Opus
to …"*. Stop everything with `tmux kill-session -t relay && pkill -x relayd`.

### Notes
- **Access control:** only ids in `admins`/`allowlist` are served; everyone else is queued.
  Approve new users from chat with `/handshake approve <id>` (admin only). Approvals persist
  to `allowlist.json`.
- **Tool approvals:** Claude's tool-use prompts are forwarded to admins' chat — answer with
  `/allow <id>` / `/deny <id>`, so an unattended session never hangs.
- **Restarts are lossless:** the shim auto-reconnects and buffers replies, so you can restart
  `relayd` without losing messages or Claude's context.
- **Custom channels** require `--dangerously-load-development-channels` (research preview);
  `scripts/run.sh` passes it for you.

## Control-plane demo (no bot needed)

```bash
go test ./...
printf '/tier pro\n/rate\nhello\n/status\n' | go run ./cmd/broker-demo
```

## License

[MIT](./LICENSE) © 2026 Jean Haley
