# agent-relay

**Text a Telegram bot, get answered by Claude Code running on your own machine.**

`agent-relay` is a small Go daemon that bridges a chat frontend (Telegram) to an agent
backend (Claude Code), with a control plane in between: an allowlist, per-account rate
limiting, a circuit breaker, and tool-approval prompts you answer from chat. It's built so
either side is swappable — the same core could drive a different chat app or a different
model — but the working, batteries-included path today is **Telegram ⇄ Claude Code**.

```
  Telegram DM ──▶ relayd ──▶ [ commands? · budget/rate gate ] ──▶ Claude Code (Sonnet)
       ▲                                                                │
       └───────────────────────── reply ───────────────────────────────┘
```

- **Runs on your box.** Claude uses your Claude Code session and files; nothing is hosted.
- **Gated.** Only people you allow can use it; strangers queue for approval.
- **Cheap by default.** Runs on Sonnet; ask it to spawn an Opus subagent for hard tasks.
- **Resilient.** Restart/upgrade the daemon without losing messages or context.

> Ollama and OpenAI backends are designed for (the backend is a swappable `Endpoint`) but
> not yet built. See [`DESIGN.md`](./DESIGN.md).

---

## Requirements

| | |
|---|---|
| **Go** | 1.24 or newer (to build) |
| **Claude Code** | v2.1.80+, signed in with a **first-party Anthropic account** — a claude.ai **Pro/Max** subscription or an Anthropic **Console API key**. *Not* supported on Bedrock / Vertex / Foundry. Channels are a research-preview feature. |
| **Telegram** | A bot token from [@BotFather](https://t.me/BotFather) and your numeric user id (get it from [@userinfobot](https://t.me/userinfobot)). |
| **tmux** | Hosts the interactive Claude session that the launcher starts. |

---

## Quickstart

```bash
# 1. Clone and build
git clone https://github.com/jeanhaley32/agent-relay.git
cd agent-relay
go build ./...

# 2. Create a Telegram bot (talk to @BotFather → /newbot) and save the token.
#    .env is gitignored — your token never gets committed.
echo 'TELEGRAM_BOT_TOKEN=123456:paste-your-bot-token-here' > .env

# 3. Configure. Copy the example and set YOUR Telegram user id as an admin.
cp config.example.json config.json
$EDITOR config.json          # set "admins": [ <your-telegram-user-id> ]

# 4. Launch everything (builds, starts the daemon, opens Claude in tmux on Sonnet)
bash scripts/run.sh

# 5. Approve the one-time "development channels" prompt, then detach
tmux attach -t relay         # press Enter at the prompt; then Ctrl-b, d to detach
```

Now open Telegram and message your bot — it should reply within a few seconds. 🎉

To stop everything:

```bash
tmux kill-session -t relay && pkill -x relayd
```

---

## Configuration

`config.json` (copied from `config.example.json`; gitignored so it can hold your ids):

```json
{
  "telegram": {
    "token_env": "TELEGRAM_BOT_TOKEN",   // env var that holds the token (not the token itself)
    "admins": [123456789],               // your user id(s): allowed + can run admin commands
    "allowlist": [],                     // extra allowed user ids (admins are auto-allowed)
    "allowlist_file": "allowlist.json",  // where /handshake approvals are persisted
    "poll_timeout": 30
  },
  "claude": { "socket": "/tmp/agent-relay.sock" },
  "budget": { "tier": "max5" }           // free | pro | max5 | max20 (local rate-limit estimate)
}
```

- The **bot token lives in `.env`**, never in `config.json`.
- `admins`, `allowlist`, and `allowlist.json` are yours — all gitignored.
- Use a different default model: `MODEL=opus bash scripts/run.sh`.

---

## Using the bot

Slash commands are handled by the relay itself (they never reach the model, so they cost
nothing):

| Command | Who | What |
|---|---|---|
| `/help` | anyone allowed | list commands |
| `/rate`, `/status` | anyone allowed | usage vs. your tier's limit + circuit state |
| `/tier <free\|pro\|max5\|max20>` | anyone allowed | switch the rate-limit tier |
| `/pause`, `/resume` | anyone allowed | stop / resume forwarding to the model |
| `/handshake` | admin | list pending access requests |
| `/handshake approve <id>` / `deny <id>` | admin | grant / drop access (persisted) |
| `/allow <id>` / `/deny <id>` | admin | answer a forwarded tool-approval prompt |

**Access requests:** anyone can message the bot, but only allowed ids are served. Others are
queued — run `/handshake` to see and approve them.

**Tool approvals:** when Claude wants to use a tool that needs approval, the prompt is
forwarded to admins' chat; reply `/allow <id>` or `/deny <id>`. The session never hangs.

**Heavier tasks:** the session runs on Sonnet to be light on your quota. Ask it to *"spawn a
subagent on Opus to …"* when you want stronger reasoning.

---

## Try the control plane without a bot

No Telegram or Claude needed — a CLI frontend + echo backend exercises the commands, rate
limiter, and circuit breaker:

```bash
go test ./...
printf '/tier pro\n/rate\nhello\n/status\n' | go run ./cmd/broker-demo
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `claude: command not found` in tmux | The launcher uses an absolute path, but if you run Claude manually, ensure `~/.local/bin` is on `PATH`. |
| Bot never replies | Make sure your user id is in `admins`/`allowlist`, and that you approved the tmux "development channels" prompt (step 5). |
| "blocked by org policy" at startup | On a Team/Enterprise plan an admin must enable channels; personal Pro/Max works out of the box. |
| Claude keeps asking to approve the `reply` tool | Approve with *"don't ask again"* once, or the prompt is forwarded to you via `/allow`. |
| Restarted `relayd` and the bot went quiet | It auto-reconnects within ~1s and buffers replies; give it a moment. Re-launch Claude only if you also killed its tmux session. |

---

## How it works

Everything is an `Endpoint` (emits + accepts messages); a `Broker` binds two of them and
pumps between them, screening slash commands and gating model turns through a budget meter.
The Claude backend is special — Claude Code *spawns* the channel over stdio — so a thin
`relay-shim` bridges the always-on daemon to each Claude session over a unix socket.

Full architecture, the token/turn economics, and the security model are in
[`DESIGN.md`](./DESIGN.md); current status and roadmap in [`PROJECT.md`](./PROJECT.md).

## License

[MIT](./LICENSE) © 2026 Jean Haley
