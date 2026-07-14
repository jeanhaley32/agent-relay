# Discord frontend Endpoint — design

Status: design only, no code yet. Target package: `internal/endpoint/discord`.

This document follows the shape of `internal/endpoint/telegram/telegram.go`
(the reference implementation) and reuses `internal/access.Manager` for
sender-gating. Read those two files plus `internal/relay/relay.go` before
implementing.

## 1. Why this isn't a straight port of Telegram

Telegram's frontend is a stateless long-poll: `GET /getUpdates` on a loop,
each call independently authenticated by the bot token in the URL. There is
no persistent connection to manage.

Discord bots do not work that way. Real-time events (messages, etc.) arrive
over the **Gateway**, a stateful websocket protocol:

- The client opens a websocket to a URL obtained from `GET /gateway/bot`,
  sends an `IDENTIFY` payload (token + intents), and receives a `READY`
  event with a `session_id` and `resume_gateway_url`.
- The server sends periodic `HELLO`/heartbeat-interval info; the client
  **must** send `HEARTBEAT` payloads on that interval or Discord closes the
  connection as zombied.
- The connection *will* drop (deploys, load balancing, network blips,
  Discord-initiated `RECONNECT` opcodes). A well-behaved client tries to
  **RESUME** the previous session (replay missed events) using the stored
  `session_id` + last-seen sequence number; only falls back to a fresh
  `IDENTIFY` if the gateway rejects the resume (`INVALID_SESSION` with
  `d: false`) or the RESUME window has expired.
- Discord also enforces an `IDENTIFY` rate limit (1 per 5s per bot, plus a
  daily max-starts budget) — a reconnect loop without backoff can burn a
  bot's daily identify budget and get it soft-locked out for the day.

This is architecturally the same *shape* as Telegram's polling loop from the
Broker's point of view (a goroutine that feeds `f.recv` until `Close()`), but
the loop's internals are materially different: it's an event-driven
websocket read loop with heartbeat/resume state, not a blocking HTTP call.
**disgo** (`github.com/disgoorg/disgo`) owns the heartbeat/resume/backoff
machinery internally (its `gateway.Gateway` component); the Discord
`Frontend` wraps disgo's event callbacks and republishes them onto
`relay.Message` over the `recv` channel, mirroring how Telegram's `pollLoop`
republishes `tgUpdate` onto the same channel type. We do not hand-roll the
websocket/resume state machine — that's exactly the class of bug (silent
disconnect, missed messages) disgo already exists to get right, and hand-
rolling it here would duplicate work the chosen library review already
selected disgo to avoid.

### Recv() mapping

```
disgo Client (websocket, owns heartbeat + resume)
   -> bot.EventListener OnMessageCreate(event)
        -> gate (bot check, guild/DM policy, sender authorization)
        -> relay.Message{...}
        -> select { case f.recv <- msg; case <-ctx.Done(): return }
```

- `New()` builds the disgo client with the configured intents, registers a
  single `OnMessageCreate` listener, and calls `client.OpenGateway(ctx)`
  (disgo's connect-with-retry entrypoint) in a goroutine, exactly as
  Telegram's `New()` spawns `pollLoop`.
- disgo surfaces connection-state changes (connected/disconnected/resumed)
  via its own logger hook; we forward those into `f.logger` for
  observability (mirrors `getUpdatesFailures`/`lastPollSuccess` — see
  §6 Metrics) but do not need to reimplement backoff: disgo's gateway
  already backs off between reconnect attempts.
- `Close()` cancels the context and calls `client.Close(ctx)`, which sends a
  clean gateway close frame; the event listener goroutine exits and we
  `close(f.recv)` from there (same "poll loop closes recv on exit"
  convention as Telegram).

## 2. Intents — minimum necessary, each justified

Discord gateway intents gate which events the bot receives at all
(unsubscribed-to events are never sent, at the protocol level — this is a
real reduction in attack surface, not just noise reduction). Two intents are
flagged **privileged** by Discord and require explicit enablement in the
Developer Portal (and, above 100 servers, App Review).

Requested intents for this design:

| Intent | Privileged? | Requested? | Why |
|---|---|---|---|
| `GUILDS` | No | Yes | Baseline: needed to receive `GUILD_CREATE`/channel metadata so the bot can resolve channel/guild IDs it's mentioned in. Nearly every bot needs this as a foundation intent. |
| `DIRECT_MESSAGES` | No | Yes | This is a DM-first relay (mirrors Telegram's private-chat-only policy — see §3). Needed to receive `MESSAGE_CREATE` in DM channels at all. |
| `GUILD_MESSAGES` | No | Only if guild/mention mode is enabled (see §3) | Needed to receive `MESSAGE_CREATE` in guild text channels. If the operator restricts this frontend to DMs only (matching Telegram's current "private chats only" stance), this intent is **not requested**, shrinking the bot's guild footprint to zero message visibility. |
| `MESSAGE_CONTENT` | **Yes (privileged)** | Yes, unavoidably | Without it, `MESSAGE_CREATE` events arrive with an **empty `content` field** for any server the bot hasn't been individually granted content access to (DMs are exempt from this restriction, notably). Since guild/mention mode needs the actual text, this is required whenever `GUILD_MESSAGES` is also on. **Not required in DM-only mode** — DM content is delivered regardless of this intent. This is the one privileged intent we cannot avoid if guild mode is used, and it is exactly the intent 2026 best-practice guidance says to scrutinize hardest; documenting the justification here is that scrutiny. |
| `GUILD_MEMBERS` | Yes (privileged) | **No** | Not needed. We gate on the sender's user ID already present on every message event (`author.id`); we don't need the full member/presence list. |
| `GUILD_PRESENCES` | Yes (privileged) | **No** | Not needed. This is a message relay, not a presence dashboard. |

Net: **DM-only deployments request zero privileged intents.** Guild/mention
mode requires exactly one privileged intent (`MESSAGE_CONTENT`), justified
above, and none of the others. This is configurable per §5.

## 3. Sender gating — mirrors Telegram exactly, on Discord user ID

Same reasoning as the Telegram doc comment: **gate on the actual sending
user's ID, never on channel/guild ID.** An ungated channel (e.g. "any
message in this guild channel is trusted") is a prompt-injection vector —
anyone who can post in that channel, including other bots or compromised
accounts, gets to inject text straight into the model.

```go
// Authorizer mirrors telegram.Authorizer exactly (same two methods), so
// access.Manager satisfies both without modification and both frontends
// share one allowlist/admin set.
type Authorizer interface {
    Allowed(id snowflake.ID) bool
    Record(id snowflake.ID, name string)
}
```

`access.Manager` currently takes `int64` ids. Discord user/guild/channel IDs
are **snowflakes**, which disgo types as `snowflake.ID` (an `uint64`
underneath, formatted as a numeric string over the wire — same shape as a
Telegram int64 user id, just unsigned and wider range). Two implementation
options, in order of preference:

1. **Reuse `access.Manager` unmodified**: convert `snowflake.ID` (`uint64`)
   to `int64` at the Discord boundary (`int64(id)`). Values fit as long as
   the id doesn't exceed `math.MaxInt64`, which is true for all real
   Discord snowflakes (they're time-based, current values are ~19 decimal
   digits, well under 2^63). This keeps the **single shared allowlist**
   requirement (item 3 of the task) trivially true: one `access.Manager`
   instance, constructed once in `main`/`relayd`, passed to both the
   Telegram and Discord frontends via `WithAuthorizer`. Add a doc comment on
   `access.Manager` noting the int64-as-snowflake convention if this path is
   taken.
2. Only if (1) proves awkward in practice: add a thin
   `Uint64Authorizer` shim in the discord package that wraps an
   `access.Manager` and does the conversion, still backed by the same
   manager instance. Functionally identical to (1); exists only to keep the
   conversion out of `access` if the maintainer prefers `access` to stay
   platform-agnostic in its literal types.

Either way: **one `access.Manager`, wired into both frontends.** Admins
approved for Telegram are not automatically Discord admins or vice versa —
each id space is distinct — but they are managed through the same
`/handshake`-style admin commands and the same persisted `allowlist.json`
structure (extended with a `discord` section, or a second file — see §5's
open question).

### Channel/guild policy (in addition to sender gating)

Following the Telegram frontend's non-private-chat refusal (`m.Chat.Type !=
"private"` is dropped outright, not just unauthenticated), the Discord
frontend applies an analogous **default-deny on message source**:

- **DMs**: always eligible (subject to sender allowlist).
- **Guild channels**: eligible only if `discord.allowed_guild_ids` (config,
  §5) explicitly lists the guild. Absent that, every guild message is
  dropped and logged, exactly like Telegram's non-private-chat drop — this
  keeps "some rando in a public server my bot got invited to" from ever
  reaching the sender-allowlist check, and it keeps the intent table above
  honest for DM-only operators (§2).
- Within an allowed guild, a message is only forwarded to `Recv()` if it
  **@mentions the bot** or is a **reply to the bot's own message** — a
  guild channel is inherently multi-party, so requiring an explicit address
  (not just channel membership) avoids the bot silently ingesting ambient
  channel chatter it was never asked to look at, on top of the sender
  allowlist. DMs have no such requirement (every message in a DM is
  addressed to the bot by construction, same as Telegram's private chats).
- `chat_id`/`from_id` identity-pair invariant (relay.go's tripwire, line
  224-238): in DM mode `chat_id` (the DM channel id) is **not** equal to
  `from_id` (the user id) the way Telegram's private-chat id happens to
  equal the sender id — Discord DM channel ids are their own snowflake
  namespace. **This means the Broker's existing identity-pair tripwire
  would misfire on every single Discord message and silently drop it.**
  This is a real, load-bearing incompatibility to resolve before wiring
  Discord into the shared Broker, not a footnote:
  - Option A (recommended): loosen the Broker's invariant check to be
    frontend-aware — e.g. only enforce chat_id==from_id for frontends that
    declare that invariant (a `RequiresChatIDEqualsFromID() bool` on a
    frontend, or a Broker field the wiring code sets per-frontend).
  - Option B: don't rely on chat_id for session-gating on Discord — thread
    the user id through as `chat_id` too (set `chat_id = from_id` for DMs,
    since the Broker only ever needs *a* stable per-conversation key and
    the session gate is genuinely keyed on the user, not the physical
    channel). This is simpler and requires zero Broker changes, at the
    cost of "chat_id" not literally meaning "Discord channel id" for DMs —
    acceptable since replies still route correctly via `Meta["channel_id"]`
    (see §4) which is tracked separately from the invariant-checked field.
  - Guild mode makes this worse (channel id, thread id, and user id are all
    distinct), so Option B (or an equivalent synthetic conversation key)
    is the practical path there regardless.
  This must be decided during implementation, not deferred — it's the kind
  of two-packages-away correctness dependency the Broker's own comment
  warns about.

## 4. Message length handling — mirrors `maxMessageLen`/`permanentSendError`

Discord's bot message cap is **2000 characters** per message (not 4096).
Same failure mode as Telegram (`400 message content too long`), same fix:
check length before ever making the HTTP/gateway call, classify as
permanent, never queue for retry.

```go
const maxMessageLen = 2000 // Discord's hard per-message character cap

// permanentSendError — identical type/semantics to telegram's, reused
// verbatim or via a tiny shared internal/senderr package if we want to
// dedupe the two identical definitions. Recommend extracting a shared
// type now that a second frontend needs it, rather than a second copy-paste.
type permanentSendError struct{ error }
func (e permanentSendError) Unwrap() error { return e.error }
```

Recommendation: extract `permanentSendError` into a small shared package
(e.g. `internal/endpoint/senderr`) used by both Telegram and Discord, since
it's now needed twice with identical semantics — avoids drift between two
copies of the same retry-classification logic.

Other permanent-vs-retryable cases to mirror from Telegram's `sendOnce`:

- Missing `channel_id` (Discord's analogue of `chat_id`) → permanent.
- HTTP 4xx other than 429 → permanent (malformed request, missing
  permissions, unknown channel, bot removed from the guild — none of these
  self-heal on retry).
- HTTP 429 (rate limited) → retryable, but **prefer disgo's built-in REST
  rate limiter** over hand-rolled backoff: disgo already parses Discord's
  `X-RateLimit-*` response headers and the global-vs-per-route bucket
  system correctly (this is specifically why disgo was chosen over
  discordgo per the task's prior research — "better rate-limiting
  correctness"). Only the retry **queue/worker loop itself** (persisting a
  failed send across restarts of the attempt, bounded queue, exponential
  backoff on genuine transport errors) needs the Telegram-style
  `retryQueue`/`startRetryWorker` treatment; per-request 429 handling
  should not be duplicated on top of what disgo's REST client already does.
- Discord-specific additional permanent case: **embeds/attachments are out
  of scope (§6)**, but a plain-text send to a channel the bot lacks
  `SEND_MESSAGES` permission in returns `403 Missing Permissions` — treat as
  permanent (retrying won't regrant the permission).

## 5. Config shape

Follows the existing `telegram` block's conventions exactly (same field
names where a Discord equivalent exists, same `_env`/`_file` suffix
conventions).

```jsonc
// config.example.json — new "discord" block, additive, optional. Config.Load
// only requires *some* frontend to have admins/allowlist; Discord is fully
// optional (see validate() changes below).
{
  "discord": {
    "token_env": "DISCORD_BOT_TOKEN",   // env var holding the bot token (never in the file)
    "admins": ["123456789012345678"],   // Discord user ids (snowflakes) — string in JSON,
                                          // since a snowflake can exceed float64's exact-int
                                          // range if ever parsed as a JSON number; see note below
    "allowlist": [],                     // permitted sender user ids, same shape as admins
    "allowlist_file": "discord_allowlist.json", // optional: persist approved ids here
    "allow_guild_messages": false,       // default false ⇒ DM-only, zero privileged intents (§2)
    "allowed_guild_ids": [],             // guild ids eligible when allow_guild_messages=true;
                                          // empty ⇒ no guild is eligible even if the flag is on
                                          // (fail closed, mirrors access.Manager's empty-allowlist stance)
    "require_mention_in_guild": true     // guild messages must @-mention the bot (or reply to it) to be relayed
  }
}
```

Notes / deltas from the Telegram block:

- **IDs as JSON strings, not numbers.** Telegram ids fit safely in a JSON
  number / Go `int64`. Discord snowflakes are 64-bit and, while current
  real values fit under `int64` max, JSON numbers are conventionally
  parsed as `float64` by naive tooling (not Go's own `encoding/json` into
  an `int64` field, which is fine — but any operator hand-editing this file
  with a generic JSON tool could silently lose precision on a snowflake).
  Discord's own API returns snowflakes as **strings** for exactly this
  reason; the config should match that convention. `config.go` parses them
  with `strconv.ParseUint` (or disgo's `snowflake.Parse`) into
  `[]snowflake.ID`, mirroring `TelegramConfig`'s `[]int64` but string-typed
  in JSON.
- **No `poll_timeout` equivalent** — there's no polling; gateway connection
  is push-based. Nothing to configure there.
- `allow_guild_messages` / `allowed_guild_ids` / `require_mention_in_guild`
  are new, Discord-specific — Telegram has no guild concept. Default is the
  safest, narrowest posture (DM-only, no guild intents needed at all).
- `DiscordConfig` gets the same `applyDefaults`/`validate` treatment as
  `TelegramConfig`: `token_env` defaults to `DISCORD_BOT_TOKEN`;
  `validate()` requires admins+allowlist non-empty **only if** the
  `discord` block is actually present/enabled (Discord is opt-in — a config
  with only a `telegram` block, as today, must keep working unchanged).
  Suggested presence check: an explicit `"enabled": true` field, since an
  all-zero-value `DiscordConfig{}` is indistinguishable from "block
  omitted" once JSON-unmarshaled into a struct with no pointer/omitempty
  tracking — same ambiguity doesn't currently exist for Telegram because
  it's the sole frontend and always required.

## 6. Metrics

Mirror Telegram's exposed counters (`SendFailures`, `PermanentDrops`,
`QueueDepth`, `GetUpdatesFailures`, `LastPollSuccess`) with Discord-shaped
analogues:

- `SendFailures` / `PermanentDrops` / `QueueDepth` — identical semantics,
  same names.
- `GetUpdatesFailures` → **`GatewayReconnects`** (count of non-resumable
  reconnects — i.e. had to fresh-IDENTIFY, not RESUME — since those are the
  ones that risk a message gap and burn IDENTIFY budget).
- `LastPollSuccess` → **`LastGatewayEventAt`** (unix seconds of the most
  recently received gateway event of any kind, including heartbeats-acked;
  a stall here is the Discord equivalent of Telegram's "getUpdates stopped
  succeeding" outage signal).

## 7. Out of scope for this pass

Deliberately minimal — a working text-relay frontend, not a kitchen sink:

- **Slash commands** (`/`-prefixed Discord application commands). The relay
  already has its own slash-command layer (`internal/command`) operating on
  message text; duplicating that as native Discord interactions is a
  separate, larger feature (different event type — `INTERACTION_CREATE`,
  different response mechanics with a 3-second ack deadline) and not needed
  for basic relay parity with Telegram.
- **Voice** (voice channel join/audio) — entirely different gateway
  subprotocol (`VOICE_STATE_UPDATE`/UDP audio), not a text relay concern.
- **Reactions** (emoji react as a signal channel) — no Telegram analogue
  exists today for the Broker to consume; would need new `Message`/`Meta`
  semantics upstream first.
- **Embeds / rich formatting / attachments / file uploads** — plain text
  only, matching Telegram's current plain-`sendMessage` behavior. Embeds in
  particular reopen a class of "structured content the model must correctly
  produce" problems (JSON payload construction, embed field-count/length
  limits) that don't exist for a plain string.
- **Threads** as first-class conversation containers — a thread message is
  still just a `MESSAGE_CREATE` with a channel id; treating threads
  specially (auto-follow, thread-scoped conversation IDs) is an enhancement
  once basic relay works, not a blocker for it.
- **OAuth2 Authorization Code flow** — explicitly **not needed**. This bot
  operates purely on a **bot token** via the Gateway + REST API (the same
  trust model as Telegram's bot token), authorized once via the standard
  bot-invite URL (`https://discord.com/oauth2/authorize?client_id=...&
  scope=bot&permissions=...`, itself a *simplified* one-shot OAuth2 grant
  the server admin clicks, not a user-facing login flow this service
  implements or hosts). There is no CSRF-relevant `state` parameter to
  design because **we never initiate or handle an OAuth2 redirect/callback
  ourselves** — that would only become relevant if this project later added
  a "click to invite the bot to your server" web flow, which is out of
  scope here. This is called out explicitly per the task's research context
  so the "OAuth2 in scope?" question has a documented, deliberate answer:
  **no.**
- **Multi-bot / sharding** — irrelevant at this scale (a personal relay
  bot, not a bot in thousands of guilds); disgo supports sharding if it's
  ever needed, but wiring it up now is premature.

## 8. Security checklist

Concrete, mapped to this design (not generic advice):

- [ ] **Token never hardcoded, never logged.** `token_env` config field
      (matches Telegram's `token_env` pattern exactly) — the daemon reads it
      from the environment at startup via `os.Getenv`, same as
      `Config.Token()` does for Telegram today. Extend that method (or add
      `Config.DiscordToken()`) rather than inventing a second convention.
      Store the real value in the KeePassXC vault
      (`~/vessel-log/vault/vessel.kdbx`) per this machine's existing
      credential-vault convention, injected into the service's environment
      at deploy time — never committed, never in `config.json`.
- [ ] **Least-privilege bot invite scope.** Invite URL requests only the
      `bot` scope (no `applications.commands` scope unless/until slash
      commands are implemented, per §7) and only the permission bits
      actually used: `View Channels`, `Send Messages`, `Read Message
      History` (needed to resolve reply-to-bot context for the mention
      check in §3), and `Send Messages in Threads` if thread support is
      later added. Explicitly **not** requested: `Administrator`,
      `Manage Guild`, `Manage Messages`, `Mention Everyone`, or any
      moderation-capable permission — this bot only relays text, it never
      needs to moderate.
- [ ] **Minimum gateway intents**, per §2's table — DM-only mode requests
      zero privileged intents; guild mode requests exactly one
      (`MESSAGE_CONTENT`), justified, with `GUILD_MEMBERS` and
      `GUILD_PRESENCES` explicitly never requested.
- [ ] **Sender-gated on user ID via the shared `access.Manager`**, never on
      channel/guild ID — per §3. No code path treats "message arrived in
      channel X" as suf

ficient authorization on its own; the sender-id
            channel X" as sufficient authorization on its own; the sender-id
      check runs regardless of channel/guild.
- [ ] **Fail-closed defaults**: no config block ⇒ Discord frontend not
      started at all (not "started with an empty allowlist that denies
      everyone" — simpler to reason about: the frontend goroutine doesn't
      exist unless explicitly enabled). Guild messages default OFF
      (`allow_guild_messages: false`). Empty `allowed_guild_ids` denies all
      guilds even if the flag is on. This mirrors `access.Manager`'s
      existing "empty allowlist denies everyone" stance and Telegram's
      `staticAuthorizer{}` fail-closed zero value.
- [ ] **Non-DM, non-allowed-guild traffic dropped before the sender check**,
      logged, never reaches the allowlist/model — per §3's channel/guild
      policy, mirroring Telegram's outright refusal of non-private chats.
- [ ] **No OAuth2 redirect/callback surface** in this service at all — see
      §7. Nothing to CSRF-protect because there's no browser-facing OAuth2
      flow hosted here.
- [ ] **IDENTIFY-budget-aware reconnect**: rely on disgo's built-in backoff
      rather than a naive immediate-retry loop, so a flapping connection
      can't burn the bot's daily IDENTIFY quota and lock it out entirely
      (a real availability risk specific to the gateway protocol, with no
      Telegram equivalent — long-polling has no comparable daily budget).
- [ ] **Rate-limit correctness delegated to disgo**, not hand-rolled — this
      was the explicit reason disgo was chosen over discordgo per the
      task's prior research; the design should not undermine that by
      building a parallel, possibly-wrong 429 handler on top.
- [ ] **`permanentSendError` classification reviewed for Discord-specific
      4xx codes** (403 missing permissions, 404 unknown channel — bot
      kicked/channel deleted) before shipping, so these don't silently burn
      12 retry attempts each the way the original Telegram
      `maxMessageLen` incident did before it was fixed — see §4.
- [ ] **Injectable HTTP/gateway client for tests**, matching Telegram's
      `WithHTTPClient`/`WithBaseURL` options — disgo's REST client accepts a
      custom `http.Client` in its config, and its gateway component accepts
      a custom dialer/URL for tests, so a `WithHTTPClient`/`WithGatewayURL`
      option pair should be exposed on the Discord `Frontend` the same way,
      keeping this endpoint unit-testable without a real bot token, exactly
      like Telegram's `httptest`-based tests do today.

## 9. Open questions to resolve before/during implementation

1. Broker `chat_id == from_id` identity-pair invariant (§3) — must pick
   Option A or B before Discord messages reach the Broker at all, or every
   Discord message gets silently dropped by that tripwire.
2. Single shared `allowlist.json`-style file with a `discord` section
   inside it, vs. a second file (`discord_allowlist.json`, as sketched in
   §5) — either is fine functionally since it's the same `access.Manager`
   instance either way; pick based on whether the operator wants one
   file to read or wants Telegram/Discord approvals visually separated on
   disk. Recommend starting with a second file (simpler `access.Manager`
   persistence code, no format change needed) and revisiting only if
   having two files in practice proves annoying to operate.
3. Whether `DiscordConfig` needs an explicit `enabled` boolean (§5) or
   whether "block present with non-empty admins/allowlist" is a good enough
   presence signal — leaning toward explicit `enabled` for clarity, at the
   cost of one more field.
