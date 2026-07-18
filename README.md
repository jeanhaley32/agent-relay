# agent-relay

**Text a Telegram or Discord bot, get answered by Claude Code running on your own machine.**

`agent-relay` is a small Go daemon that routes Telegram and/or Discord messages to a Claude
Code session, with a control plane in between: an allowlist, per-account rate limiting, a
circuit breaker, and tool-approval prompts you answer from chat.

That's the scope today — **a Telegram/Discord router for a Claude Code backend.** No other
model backends are supported. (Internally the pieces are factored so more frontends could be
added later — see [`DESIGN.md`](./DESIGN.md) — but don't pick this up expecting a
general-purpose, provider-agnostic relay.)

> ⚠️ **Experimental (`v0.x`).** This relies on Claude Code **channels**, a *research-preview*
> feature that can change or break with Claude Code updates. It also hands chat users a
> Claude Code session with **tool access to your machine** — read [`SECURITY.md`](./SECURITY.md)
> before exposing it to anyone but yourself. Running an automated bot on your Claude
> subscription is your responsibility under Anthropic's usage policies.

```
  Telegram DM  ──┐                                                       ┌── reply
                 ├─▶ relayd ──▶ [ commands? · budget/rate gate ] ──▶ Claude Code (Sonnet)
  Discord DM   ──┘                                                       └── reply
```

- **Runs on your box.** Claude uses your Claude Code session and files; nothing is hosted.
- **Gated.** Only people you allow can use it; strangers queue for approval.
- **Cheap by default.** Runs on Sonnet; ask it to spawn an Opus subagent for hard tasks.
- **Resilient.** Restart/upgrade the daemon without losing messages or context.

---

## Requirements

| | |
|---|---|
| **Go** | 1.24 or newer (to build) |
| **Claude Code** | v2.1.80+, signed in with a **first-party Anthropic account** — a claude.ai **Pro/Max** subscription or an Anthropic **Console API key**. *Not* supported on Bedrock / Vertex / Foundry. Channels are a research-preview feature. |
| **Telegram** | A bot token from [@BotFather](https://t.me/BotFather) and your numeric user id (get it from [@userinfobot](https://t.me/userinfobot)). Optional — you can run Discord-only. |
| **Discord** | A bot application + token from the [Developer Portal](https://discord.com/developers/applications), and your numeric Discord user id (Settings → Advanced → enable Developer Mode, then right-click yourself → Copy User ID). Optional — you can run Telegram-only. |
| **tmux** | Hosts the interactive Claude session that the launcher starts. |

At least one of Telegram or Discord must be configured; both can run at once.

---

## Quickstart

```bash
# 1. Clone and build
git clone https://github.com/jeanhaley32/agent-relay.git
cd agent-relay
go build ./...

# 2. Create a bot and save its token. .env is gitignored — tokens never get committed.
#    Telegram: talk to @BotFather → /newbot
echo 'TELEGRAM_BOT_TOKEN=123456:paste-your-bot-token-here' > .env
#    Discord (optional, can run alongside or instead of Telegram):
#    discord.com/developers/applications → New Application → Bot → Reset Token
echo 'DISCORD_BOT_TOKEN=paste-your-bot-token-here' >> .env

# 3. Configure. Copy the example and set YOUR user id(s) as admin.
cp config.example.json config.json
$EDITOR config.json          # set "admins": [ <your-telegram-user-id> ]
                              # and/or discord.admins: [ "<your-discord-user-id>" ]
                              # (set discord.enabled: true to turn Discord on)

# 4. Launch everything (builds, starts the daemon, opens Claude in tmux on Sonnet)
bash scripts/run.sh

# 5. Approve the one-time "development channels" prompt, then detach
tmux attach -t relay         # press Enter at the prompt; then Ctrl-b, d to detach
```

Now open Telegram (or DM your Discord bot, once you've shared a server with it) and message it —
it should reply within a few seconds. 🎉

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
  "discord": {
    "enabled": false,                    // set true to turn Discord on (off by default)
    "token_env": "DISCORD_BOT_TOKEN",
    "admins": ["123456789012345678"],    // your Discord user id(s), as strings (snowflake precision)
    "allowlist": [],
    "allowlist_file": "discord_allowlist.json",
    "allow_guild_messages": false,       // false = DM-only, no server/guild access at all
    "allowed_guild_ids": [],             // only used if allow_guild_messages is true
    "require_mention_in_guild": true     // in guild mode, only respond when @mentioned
  },
  "claude": { "socket": "/tmp/agent-relay.sock" },
  "budget": { "tier": "max5" }           // free | pro | max5 | max20 (local rate-limit estimate)
}
```

- **Bot tokens live in `.env`**, never in `config.json`.
- `admins`, `allowlist`, and `allowlist.json`/`discord_allowlist.json` are yours — all gitignored.
- Discord defaults to **DM-only** with zero privileged Gateway intents — the narrowest posture.
  Discord itself won't let a bot DM you until you share a server with it once (invite it via the
  Developer Portal's OAuth2 URL Generator, "bot" scope, no permissions needed for DM-only), then
  DM it — after that it can reply.
- Sender gating is always on the platform's own user id, never on a chat/channel/guild id — an
  ungated channel is a prompt-injection vector. Both frontends share this rule.
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

**Sending a literal command to the model:** prefix with a backslash — `\/help` reaches Claude
as the text `/help` instead of being intercepted by the relay.

**Scheduled reminders & self-wakeups:** the model has three scheduling tools (served by the
relay, so schedules persist and fire even when no session is attached). Just ask in plain
language — *"remind me every morning at 9 to do a 45-minute training session"* or *"ping me in
20 minutes"* — and the model calls `schedule_message` (`cron` for recurring, `in_seconds` for
one-shot; times are in the host's timezone). When a schedule fires, the relay pokes the session
and the model delivers it to you. The model can also schedule a **self-wakeup** to resume a
long-running task (*"check the deploy in 15 minutes and continue"*). `list_schedules` and
`cancel_schedule` manage them; schedules survive restarts (persisted to `schedules.json`).

Configure the store/timezone in `config.json` (optional; defaults shown):

```json
"scheduler": { "file": "schedules.json", "tz": "America/New_York" }
```

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
| Bot never replies | Make sure your user id is in `admins`/`allowlist` (Discord ids go in the `discord` block, Telegram ids in `telegram`), and that you approved the tmux "development channels" prompt (step 5). |
| "blocked by org policy" at startup | On a Team/Enterprise plan an admin must enable channels; personal Pro/Max works out of the box. |
| Claude keeps asking to approve the `reply` tool | Approve with *"don't ask again"* once, or the prompt is forwarded to you via `/allow`. |
| Restarted `relayd` and the bot went quiet | It auto-reconnects within ~1s and buffers replies; give it a moment. Re-launch Claude only if you also killed its tmux session. |
| Discord bot won't DM me: `403 Cannot send messages to this user` | Discord blocks bots from DMing anyone with no mutual server. Invite it to a server you're in (Developer Portal → OAuth2 URL Generator → "bot" scope), then DM it from there. |

---

## How it works

`relayd` long-polls Telegram, screens each message (slash command? allowed sender? within
the rate budget?), and forwards the rest to a Claude Code session. Because Claude Code
*spawns* its channel over stdio, a thin `relay-shim` bridges the always-on daemon to the
Claude session over a unix socket; replies flow back the same way.

Full architecture (including the internal factoring that could support other frontends or
backends in future — currently unused), the token/turn economics, and the security model are
in [`DESIGN.md`](./DESIGN.md); current status and roadmap in [`PROJECT.md`](./PROJECT.md).

## License

[MIT](./LICENSE) © 2026 Jean Haley
