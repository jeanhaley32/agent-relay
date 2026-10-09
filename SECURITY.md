# Security

`agent-relay` lets people message a Telegram bot and be answered by a **Claude Code session
running on your machine**. Claude Code can use tools (read files, run commands) — so an
allowed chat user, or a prompt injection, is driving code on your box. **Treat this as a
remote-code-execution surface and configure it deliberately.** This document explains the
trust model, what's defended, what isn't, and how to run it safely.

> Status: experimental (`v0.x`). It also depends on Claude Code **channels**, a research
> preview that can change or break on Claude Code updates.

## Trust model

Three roles, by Telegram user id:

| Role | Can | Defined by |
|---|---|---|
| **Admin** | Everything a user can, plus admin commands (`/handshake`, `/allow`, `/deny`, `/pause`, `/resume`, `/tier`) | `admins` in `config.json` |
| **Allowed user** | Message the bot; their messages reach Claude | `allowlist` + `/handshake`-approved (`allowlist.json`) |
| **Stranger** | Nothing — messages are dropped and queued for admin approval | everyone else |

The **security boundary is the sender's Telegram user id**, which comes from Telegram's
server-signed update (not user-controllable text). Gating is on the *sender*, never the
chat/room id.

## What is defended

- **Inbound allowlist (fail-closed).** Only admins/allowed ids reach Claude; everyone else
  is dropped (zero model tokens) and queued. An empty allowlist denies everyone.
- **Admin gating.** Mutating/admin commands require an admin **and** a direct message
  (never a group), and fail closed if no admin predicate is wired.
- **Handshake hardening.** Bounded pending queue (anti-DoS), sanitized/truncated names
  (anti-injection into the listing), approve-only-pending/denied ids, reversible deny,
  atomic + logged persistence.
- **Tool restriction (security profiles).** `security.yaml` sets what the session may do:
  `restricted` (default) allow-lists safe tools, hard-denies dangerous ones, and **relays
  anything else to admins** for approval; `full` grants everything (trusted operator).
- **Permission relay.** Tool-approval prompts are forwarded to admins' Telegram (`/allow`,
  `/deny`) so an unattended session never auto-runs a gray-zone tool — or hangs.
- **Outbound gating.** The model can only `reply` to allowlisted chats — it can't message a
  stranger even if told to.
- **Workspace isolation.** Claude runs in a clean workspace (`~/.agent-relay/workspace`)
  with no secrets in its working directory (`ISOLATE=0` opts out).
- **Budget / circuit breaker.** Per-tier rate limiting caps runaway usage.
- **Secrets.** The bot token lives in `.env` (git-ignored), referenced by env-var name;
  `config.json` and `allowlist.json` are git-ignored.

## What is NOT defended (residual risks)

- **Single shared session.** All users share one Claude context, so conversations are not
  isolated between users (fine for a single operator; **do not** treat it as multi-tenant).
- **cwd isolation is not a sandbox.** Claude runs as your OS user, so in `full` mode an
  absolute-path file read is still possible. Restricted mode + workspace isolation mitigate
  this; a real sandbox requires OS isolation (below).
- **Prompt injection.** Content Claude reads (files, tool output) can carry instructions.
  Restricting tools and using an isolated workspace shrinks the blast radius; it doesn't
  eliminate it.
- **Research-preview fragility.** Channel behavior can change with Claude Code releases.
- **Budget tiers are local estimates**, not Anthropic's real limits — tune them.

## Running it safely

**Single trusted operator (just you):** the defaults are reasonable. Keep the token in
`.env`, leave workspace isolation on, and use `full` mode only because you are the only
user.

**Anything with other users — do all of these:**

1. **Restricted profile.** `security.yaml` → `mode: restricted`; grant only the tools the
   bot actually needs. Everything else routes to you for approval.
2. **OS isolation (the real backstop).** Run the Claude session as a **dedicated,
   low-privilege user** that cannot read your `.env`/home, or inside a **container** with
   only the workspace mounted. This is what actually contains a compromised or
   prompt-injected session — application-layer tool rules are not sufficient alone.
3. **Keep isolation on** (`ISOLATE=1`, the default). Never point the workspace at a
   directory containing secrets.
4. **Never** run with `--dangerously-skip-permissions` (i.e. `mode: full`) for untrusted
   users.
5. **Private socket.** The IPC socket defaults to `/tmp` (world-readable directory). On a
   shared host, set `SOCK` to a path under a private directory (e.g.
   `$XDG_RUNTIME_DIR/agent-relay.sock`).
6. **Least privilege on the box.** Restrict the bot's outbound network and filesystem to
   what it needs; the reference deployment keeps all inbound access behind a VPN.
7. **Lock down the bot** in BotFather: disable group joins (`/setjoingroups`), keep privacy
   mode on, and revoke/rotate the token if it ever leaks.
8. **Watch your account terms.** Running an automated bot on your Claude subscription is
   your responsibility under Anthropic's usage policies.

## Reporting a vulnerability

Please report security issues **privately**, not in a public issue:

- Open a **GitHub Security Advisory** on this repository (Security → Report a vulnerability), or
- email the maintainer.

Include reproduction steps and impact. We'll acknowledge and work on a fix before any public
disclosure.
