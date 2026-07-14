# Matrix frontend Endpoint — design

Status: design only, no code yet. Target package: `internal/endpoint/matrix`.

No real Matrix homeserver, account, access token, or credential of any kind
exists anywhere in this document or in any commit related to it. This is a
from-scratch design against the publicly documented Matrix Client-Server API
and the `maunium.net/go/mautrix` library's own README/godoc, following the
shape of `internal/endpoint/telegram/telegram.go` (the reference
implementation) and `internal/endpoint/discord/discord.go` + its DESIGN.md
(the second frontend, same night). Read all three plus
`internal/relay/relay.go` before implementing.

## 0. Why mautrix-go

`maunium.net/go/mautrix` is the client-server library used by
mautrix-whatsapp, mautrix-signal, and the rest of the mautrix bridge family —
actively maintained, and notably the one Go Matrix library with a real,
production-hardened E2E encryption subsystem (`mautrix.Client.Crypto`, an
Olm/Megolm implementation) rather than only the plaintext HTTP API surface.
That crypto subsystem is the whole reason to reach for this library over
hand-rolling client-server HTTP calls: Matrix's actual differentiator over
Telegram/Discord (both explicitly non-E2E in this project) is real
end-to-end encryption, and Olm/Megolm session management is exactly the kind
of protocol-correctness surface (like Discord's gateway resume/backoff) this
project has already decided not to hand-roll — see discord/DESIGN.md §1's
identical reasoning for choosing disgo over hand-rolled gateway code.

## 1. Homeserver-agnostic by design (Jean's explicit requirement, 2026-07-14)

> "this should be optional for anyone who wants to turn up our relay... pick
> from any of the three options."

Matrix is a federated, open protocol — unlike the Telegram/Discord frontends
(each hardwired to exactly one vendor's API), a Matrix frontend must not
assume a particular homeserver implementation. The three concrete options an
operator should be able to pick from, all satisfied by the same code path:

1. **Self-hosted Conduit** — a lightweight single-binary Rust homeserver, a
   natural fit for this project's own home-lab-on-k3s pattern (see the
   top-level CLAUDE.md's home-lab roadmap) if Jean or another operator wants
   to run their own.
2. **Self-hosted Synapse** — the reference/most-feature-complete
   implementation, heavier (Python, Postgres-backed).
3. **Self-hosted Dendrite** — Matrix.org's second-generation Go
   implementation, a middle ground.
4. **Public matrix.org** (or any other public homeserver an operator already
   has an account on) — zero self-hosting, just create a bot account there.

Concretely, this means:

- **Config takes a `homeserver_url`, full stop** — never a hardcoded
  `https://matrix.org` or any implementation-specific base URL, mirroring
  how `telegram.WithBaseURL`/`discord.WithBaseURL` exist purely as *test*
  seams, not because the frontend assumes one vendor's API. Here the
  homeserver URL is a **required, operator-supplied production config
  value**, not just a test override — the opposite of Telegram/Discord,
  where the base URL is fixed in production and only overridden in tests.
- **No server-specific API extensions used.** Only the standard
  Client-Server API (`/_matrix/client/v3/...`) that every conformant
  homeserver implementation (Conduit, Synapse, Dendrite) and every public
  server implements identically. mautrix-go's `mautrix.Client` talks this
  standard surface; nothing in this design reaches for a Synapse-only admin
  API or a Conduit-only extension.
- **Auth supports both credential shapes** a real operator might have:
  - **Access token** (`access_token` + optional `device_id`) — the simpler
    path: the operator (or their homeserver's admin UI / `register_new_matrix_user`
    CLI for Synapse, or Conduit/Dendrite's equivalent) mints a token for the
    bot account once, out of band, and hands it to this config. Works
    identically regardless of which of the three homeserver options is in
    play, since token-based auth is standard Client-Server API, not a
    server-specific feature.
  - **Username + password login flow** (`user_id`/`username` + `password`)
    — for an operator who only has a bare account and prefers the bot to
    call the standard `POST /_matrix/client/v3/login` itself at startup and
    obtain its own `access_token` + `device_id`, rather than requiring an
    out-of-band token-minting step. Useful for public-server accounts (e.g.
    matrix.org) where the operator may not have shell access to run a
    token-minting CLI at all.
  - Config validation requires **exactly one** of these two auth shapes —
    same "fail with a clear error, not confusing downstream behavior" stance
    as `config.go`'s existing `validate()`.
- **Homeserver reachability is the one thing genuinely out of this
  package's control** (self-hosted homeservers may be behind their own
  Tailscale-only exposure, same posture as every other service on this
  machine per the top-level CLAUDE.md) — the frontend just dials
  `homeserver_url` like any other configured endpoint; it has no opinion on
  whether that URL points at a tailnet-only Conduit instance or the public
  internet.

## 2. E2E encryption: staged as an explicit, opt-in follow-up — MVP ships plaintext-room-only

**Recommendation: MVP is plaintext (unencrypted-room) only, with E2E as a
clearly-scoped, explicitly-flagged follow-up — not shipped day one.**

This mirrors discord/DESIGN.md's own staging call ("DM-only first, guild
mode is the harder path") but the stakes are higher here to get right,
because it's worth being honest about the tension this creates:

**The case for shipping E2E day one:** real end-to-end encryption is
*arguably the entire reason to build a Matrix frontend at all* — Telegram
and Discord are both explicitly non-E2E in this project already (see both
existing DESIGN.md's "why not just use Telegram/Discord" framing implicitly
throughout), so if Matrix support ships without it, a reasonable question is
"then what did Matrix actually buy us over a third stateless-bot-token
frontend?"

**Why MVP ships without it anyway:**

1. **The complexity is not comparable to anything already in this
   codebase.** Telegram's frontend is a stateless long-poll; Discord's adds
   one layer of statefulness (gateway websocket + heartbeat/resume) that
   disgo owns internally. E2E via mautrix-go's crypto subsystem adds a
   *second, orthogonal* layer of statefulness on top of that: a persistent
   Olm/Megolm crypto store (mautrix-go supports SQLite-backed
   `crypto.SQLCryptoStore`), device-list tracking and cross-signing/device
   verification UX, key-backup/session-recovery, and to-device event
   handling for key requests/shares — none of which has any analogue in
   either existing frontend. Shipping it half-right (e.g. a crypto store
   that silently loses keys on a restart, or a bot that never verifies
   devices and so trusts any device claiming to be a given user) is worse
   than not shipping it, because it gives a false impression of
   confidentiality while actually providing none — a user who believes
   their messages are E2E-protected because "it's Matrix" is a strictly
   worse outcome than a user who knows up front the room is plaintext.
2. **A working plaintext relay is still a complete, useful deliverable** —
   the exact same "basic relay parity with Telegram, not a kitchen sink"
   philosophy discord/DESIGN.md §7 stated explicitly for its own MVP scope.
   Sender-gating, the chat_id/from_id invariant, retry/chunking — all of
   §§3-4 below — are real, immediately useful work independent of whether
   the room happens to be encrypted.
3. **Precedent inside this exact codebase**: Discord staged guild mode
   (the harder, broader-attack-surface path) behind DM-only-first. E2E here
   plays the identical role Discord's guild mode played there — the harder,
   higher-stakes mode that deserves its own dedicated design pass once the
   plaintext skeleton (auth, gating, send/recv, retry) is proven, not
   bolted on as an afterthought to a first PR that's already juggling
   homeserver-agnosticism, config shape, and the room-id/user-id invariant
   below.
4. **Concretely staged, not hand-waved**: `MatrixConfig.E2EEnabled` (see §5)
   exists in the config shape *now*, defaulting `false`, so operators and
   the codebase both have a clear, named place this lands rather than a
   TODO comment. When it ships, the design is: `mautrix.Client.Crypto` +
   `crypto.SQLCryptoStore` (SQLite, mirroring this project's existing
   preference for boring embedded stores — no new infra dependency), device
   verification deliberately scoped to **trust-on-first-use (TOFU)** for the
   bot's own single device (see §7's out-of-scope list — cross-signing UX
   is explicitly cut even at that point), and encrypted-room messages
   handled via the standard `OnEvent`/timeline-decrypt callback mautrix-go
   provides. This is scoped as its own DESIGN.md addendum when picked up,
   not retrofitted silently.

Until E2E ships, a message into an encrypted room simply fails to decrypt on
this bot's end — mautrix-go surfaces that as an event it cannot read the
content of. The frontend's stance in that case: **log and drop, never crash,
never treat undecryptable content as empty-string-equals-ignorable text**
(an event whose content mautrix-go can't decrypt must never silently become
`Text: ""` and fall through gate()'s empty-content check as if nothing was
said — see §3's `content == ""` drop, which is for genuinely empty messages,
not failed decryption). A distinct log line and, for a DM, a one-time
"this room is encrypted and I can't read it yet — E2E support isn't enabled"
notice back to the sender (rate-limited to avoid spamming on every message
in a busy encrypted room) is the honest behavior for the plaintext-only MVP.

## 3. Sender gating — on the Matrix user ID, never the room ID

Same non-negotiable rule as Telegram (`from_id`, never `chat_id`) and
Discord (`authorID`, never `channelID`/`guildID`): **gate on the actual
sending user's Matrix ID.** An ungated room is a prompt-injection vector —
same reasoning stated in both existing frontends, restated here because it
is genuinely non-negotiable, not because it differs on Matrix.

A Matrix user ID has the form `@localpart:homeserver.tld` — a string, not a
numeric id like Telegram's `int64` or Discord's `snowflake.ID`. This is
closer in shape to nothing currently in this codebase, so:

```go
// Authorizer mirrors telegram.Authorizer/discord.Authorizer's two methods
// exactly, string-keyed instead of int64/snowflake.ID-keyed, since Matrix
// user ids are not numeric.
type Authorizer interface {
    Allowed(id string) bool // id.MatrixUserID, e.g. "@jean:example.org"
    Record(id string, name string)
}
```

`access.Manager` is currently `int64`-keyed (per discord/DESIGN.md §3, which
converts Discord's `uint64` snowflakes down to `int64` at the package
boundary). That conversion trick does not apply here — a Matrix user ID is
not numeric at all. Two options, in order of preference:

1. **A separate, string-keyed authorizer/allowlist for Matrix**, sharing
   `access.Manager`'s *persistence shape* (same JSON allowlist-file
   convention, same pending-request-record-for-admin-approval workflow) but
   as its own instance — since Matrix identities are not fungible with
   Telegram/Discord ones anyway (different account, different platform,
   same reasoning Discord used for why Telegram admins aren't automatically
   Discord admins). This likely means either genericizing `access.Manager`
   over its id type (`Manager[T comparable]`) so Telegram/Discord/Matrix all
   share one generic implementation, or a small `matrixaccess` package
   mirroring `access.Manager`'s logic string-keyed. Recommend the generic
   route if it's a small diff — three near-identical hand-copies of the
   same allow/record/persist logic is exactly the kind of drift the
   `senderr` package was extracted specifically to avoid (see
   senderr.go's own doc comment on why it was pulled out of telegram.go).
2. Only if generics prove awkward in the existing codebase style: a
   Matrix-local `staticAuthorizer map[string]bool` (mirroring both existing
   packages' own fallback fail-closed zero value) plus a thin persisted
   allowlist file, accepting some duplication rather than forcing a
   same-night refactor of `access.Manager` into this design.

Either way: **fail-closed default** (empty allowlist denies everyone),
identical stance to both existing frontends.

### Room semantics and the chat_id/from_id invariant

This is the one place Matrix's actual data model is a genuinely different
shape from *both* existing frontends, worth working through carefully
rather than copying either Option A/B verbatim:

- **Telegram**: a private chat's `chat_id` numerically *is* the sender's
  `from_id` — one id space, no distinction. The invariant holds by
  construction, no frontend-side work needed.
- **Discord**: `chat_id` (DM channel id) and `from_id` (user id) are
  **different snowflake namespaces entirely** — Discord synthesizes a
  separate DM-channel-id concept distinct from the user id. This is why
  Discord needed "Option B" (`chat_id = from_id` for DMs, a synthetic
  substitution) to satisfy the Broker's invariant tripwire at all.
- **Matrix**: there is no separate "DM channel" concept in the protocol at
  all. A Matrix DM is, structurally, **just a room with (usually) two
  members** — `!opaqueRoomId:homeserver.tld` — no different in kind from
  any other room; "this room is a DM" is a client-side convention
  (`m.direct` account data) rather than a protocol-level distinction the
  way Discord's DM channel type is. In this specific respect Matrix's room
  model is **closer to Telegram's `chat_id`** than to Discord's split
  channel-id/user-id model: **the room already IS the addressable
  conversation unit**, exactly like a Telegram chat.

  The wrinkle: unlike Telegram, where a private chat's `chat_id` and the
  sender's `from_id` are numerically identical, a Matrix room id
  (`!abc123:...`) and a Matrix user id (`@jean:...`) are **never** the same
  string even in a 1:1 DM room — different sigils (`!` vs `@`), different
  values entirely. So the Broker's `chat_id == from_id` tripwire (relay.go,
  the `-2.` check) **would still misfire on every Matrix DM**, the same
  problem Discord hit, just for a different structural reason (Discord: two
  genuinely different id namespaces for the same conceptual thing; Matrix:
  a room id and a user id are never equal by definition, DM or not).

  **Decision: apply Discord's Option B, adapted.** For a 1:1 DM room
  (exactly 2 joined members, one of them this bot), set `chat_id =
  from_id` (the other member's Matrix user ID) rather than the room id —
  same reasoning Discord's DESIGN.md §3 gave: the Broker's session gate and
  identity-pair invariant only need *a* stable per-conversation key
  genuinely keyed on the user, and replies route via a separate
  `Meta["room_id"]` field tracked independently (mirroring Discord's
  `Meta["channel_id"]`), so `chat_id` not literally meaning "the Matrix room
  id" for DMs costs nothing. For a **multi-member room** (a group room the
  bot has been invited into — see §7, likely out of scope for MVP but
  worth deciding the shape now rather than retrofitting later), `chat_id =
  room_id`, and the message carries `Meta["room_id"]` set to the same
  value, mirroring Discord's `guild_id`-present marker that opts a message
  out of the Broker's invariant check for multi-party contexts. A DM-only
  MVP (see §7) makes the multi-member case moot for the first cut, but the
  field-naming decision (`room_id`, not `channel_id`) is made now so a
  later group-room follow-up doesn't need a rename.

- `Meta` shape, mirroring both existing frontends:
  ```go
  Meta: map[string]string{
      "chat_id":   convID,           // = from_id (DM) or room_id (multi-member, follow-up)
      "room_id":   roomID,           // ALWAYS the real Matrix room id — Send() routes on this
      "from_id":   senderMatrixID,   // @user:homeserver.tld
      "from_name": senderDisplayName,
  }
  ```
- Group/multi-member rooms, if/when added, set `Meta["room_id"]` distinctly
  from `chat_id` being genuinely per-user — no separate `guild_id`-style
  marker key is needed the way Discord's is, because for Matrix `chat_id ==
  room_id` *is* how a multi-member message is distinguished (a DM message
  never has `chat_id == room_id`, since DM `chat_id` is always the peer's
  user id, which can never collide with a `!`-sigil room id string) — so
  the Broker's invariant naturally already treats it correctly without a
  bespoke opt-out key, PROVIDED the Broker's identity-pair check special-
  cases Matrix's frontend the same way it does Discord's (i.e. this still
  needs the same kind of frontend-aware carve-out discord/DESIGN.md §3
  flagged as "Option A" — the room-id-as-chat_id path for group rooms
  will still trip the invariant otherwise, exactly like Discord's guild
  case does, and needs the identical `guild_id`-style presence-marker
  treatment when that follow-up lands).

## 4. Message length — no hard per-message cap, but a real event-size ceiling exists

Unlike Telegram (`4096` chars, HTTP 400 on overflow) and Discord (`2000`
chars, HTTP 400 on overflow), **the Matrix Client-Server spec does not
define a hard per-message character limit.** `m.room.message` events are
just JSON event content; there is no `sendMessage`-style length validation
baked into the protocol the way both existing platforms have.

However, this is not "unlimited" in practice:

- Homeservers enforce a **total event size ceiling**, commonly cited at
  **65536 bytes (64 KiB)** for a complete serialized PDU (the whole event —
  headers, signatures, and content JSON together, not just the message
  body) — this is a widely-implemented convention across Synapse/Dendrite/
  Conduit rather than a single normatively-mandated byte count in the spec
  text itself, so treat it as a practical ceiling to stay well clear of,
  not an exact number to bump up against.
- An oversized `PUT /_matrix/client/v3/rooms/{roomId}/send/...` request is
  rejected by the homeserver (HTTP 413 or a 400 with `M_TOO_LARGE`-shaped
  error, depending on implementation) — same "silently guaranteed to fail
  identically on retry" shape as Telegram's 4096/Discord's 2000 overflow,
  just enforced server-side with a much larger and JSON-overhead-inclusive
  ceiling rather than a documented plain client-side constant.

**Decision: adopt a sane, conservative cap well under the practical
ceiling, checked client-side exactly like both existing frontends, and
reuse `senderr.Split` with it.**

```go
// maxMessageLen is a conservative cap chosen well under Matrix's practical
// ~64KiB per-event JSON ceiling (unlike Telegram/Discord, the Matrix spec
// itself defines no fixed per-message character limit — see DESIGN.md §4).
// The gap versus the 64KiB ceiling accounts for: (a) that ceiling bounds
// the WHOLE serialized event (headers/signatures/content together), not
// just the message body text senderr.Split operates on; (b) encrypted
// rooms (once §2's E2E follow-up ships) add Megolm ciphertext overhead on
// top of the plaintext body; (c) staying well clear of a boundary that
// isn't even normatively fixed across homeserver implementations is safer
// than hugging it. 16000 runes is generously larger than either existing
// frontend's cap (so Matrix isn't needlessly chattier than it has to be
// for normal-length replies) while leaving ample headroom under 64KiB even
// accounting for UTF-8 multi-byte runes and JSON escaping overhead.
const maxMessageLen = 16000

// Reused verbatim, same call shape as telegram.go/discord.go's Send:
chunks := senderr.Split(m.Text, maxMessageLen)
```

Permanent-vs-retryable classification mirrors both existing frontends'
`sendOnce`:

- Missing `room_id` (Matrix's analogue of `chat_id`/`channel_id`) →
  permanent.
- HTTP 4xx other than 429 (`M_LIMIT_EXCEEDED`) → permanent — malformed
  event, `M_FORBIDDEN` (bot not joined to / not permitted in the room),
  `M_TOO_LARGE`, room deleted/left — none self-heal on retry.
- HTTP 429 / `M_LIMIT_EXCEEDED` (Matrix's rate-limit response, which also
  carries a `retry_after_ms` hint in the response body unlike Telegram/
  Discord's header-based hints) → retryable, honoring `retry_after_ms` when
  present rather than blind exponential backoff, mirroring how Discord's
  design deferred to disgo's rate-limit-header parsing rather than
  reinventing it — mautrix-go's client should be checked for whether it
  already surfaces/handles this, same "don't duplicate what the library
  already gets right" stance as discord/DESIGN.md §4's disgo call.

## 5. Config shape

Follows `TelegramConfig`/`DiscordConfig`'s conventions: same `_env`/`_file`
suffixes, same `Enabled` presence-flag pattern `DiscordConfig` already
established (config.go's `Enabled bool` + `applyDefaults`/`validate`
treatment) rather than re-deriving that ambiguity a third time.

```go
// MatrixConfig configures the Matrix frontend. Matrix is fully optional —
// a config with only "telegram" (or "telegram" + "discord") blocks keeps
// working unchanged. See internal/endpoint/matrix/DESIGN.md.
type MatrixConfig struct {
    // Enabled gates whether relayd starts the Matrix frontend at all —
    // same rationale as DiscordConfig.Enabled: an all-zero-value
    // MatrixConfig{} must not be mistaken for "frontend wants to start
    // with an empty allowlist." Fail-closed default: false.
    Enabled bool `json:"enabled"`

    // HomeserverURL is REQUIRED whenever Enabled — this frontend is
    // deliberately homeserver-agnostic (DESIGN.md §1): self-hosted
    // Conduit/Synapse/Dendrite or a public server all work identically, as
    // long as this points at that server's standard Client-Server API
    // base, e.g. "https://matrix.example.org" or "https://matrix.org".
    HomeserverURL string `json:"homeserver_url"`

    // --- Auth: exactly one of the two shapes below must be configured. ---

    // AccessTokenEnv, if set, is the env var holding a pre-minted access
    // token (mirrors TelegramConfig.TokenEnv/DiscordConfig.TokenEnv's
    // convention: the secret's NAME lives in the file, never the secret
    // itself). DeviceID should accompany it if known (the device the token
    // was minted for) — required once E2E ships (§2), optional for the
    // plaintext MVP.
    AccessTokenEnv string `json:"access_token_env"`
    DeviceID       string `json:"device_id"`

    // UserID/PasswordEnv, if set instead, make this frontend perform the
    // standard POST /_matrix/client/v3/login flow itself at startup and
    // obtain its own access_token + device_id — for an operator who only
    // has a bare account and no out-of-band token-minting step available
    // (DESIGN.md §1). PasswordEnv follows the same "name of the env var,
    // never the secret" convention.
    UserID      string `json:"user_id"`
    PasswordEnv string `json:"password_env"`

    // Admins/Allowlist hold Matrix user ids as strings ("@user:server.tld")
    // — never numeric, so no snowflake-style JSON-number-precision concern
    // exists here the way DiscordConfig.Admins documents, but they're
    // still plain strings for the same "match the platform's own id
    // representation" reasoning.
    Admins        []string `json:"admins"`
    Allowlist     []string `json:"allowlist"`
    AllowlistFile string   `json:"allowlist_file"`

    // E2EEnabled stages end-to-end encryption support as an explicit,
    // separately-designed follow-up (DESIGN.md §2) — default false. When
    // false, the frontend only ever joins/operates in plaintext rooms and
    // logs-and-drops content it cannot decrypt (§2) rather than crashing
    // or silently treating it as empty. NOT implemented by this design
    // pass; the field exists now so the config shape doesn't need a
    // breaking change when the follow-up lands.
    E2EEnabled bool `json:"e2e_enabled"`
}
```

`validate()` additions, mirroring `DiscordConfig`'s block:

```go
if c.Matrix.Enabled {
    if c.Matrix.HomeserverURL == "" {
        return fmt.Errorf("matrix: enabled but homeserver_url is empty — see DESIGN.md §1")
    }
    hasToken := c.Matrix.AccessTokenEnv != ""
    hasLogin := c.Matrix.UserID != "" && c.Matrix.PasswordEnv != ""
    if hasToken == hasLogin { // both set or neither set
        return fmt.Errorf("matrix: exactly one of access_token_env or (user_id + password_env) must be set")
    }
    if len(c.Matrix.Admins) == 0 && len(c.Matrix.Allowlist) == 0 {
        return fmt.Errorf("matrix: enabled but no admins or allowlist — the bot would serve nobody; add your Matrix user id to \"admins\"")
    }
    if c.Matrix.E2EEnabled {
        return fmt.Errorf("matrix: e2e_enabled is not yet implemented — see DESIGN.md §2")
    }
}
```

That last check is deliberate: rather than silently ignoring
`e2e_enabled: true` in the plaintext-only MVP (which would let an operator
believe they'd turned on encryption support when they hadn't — exactly the
false-confidentiality outcome §2 argues against), the config fails loudly
until the follow-up design actually implements it.

## 6. Testability — `New()`/`Connect()` split, injectable HTTP client

Same requirement discord/DESIGN.md §8 states, and Discord's actual
`New()`/`Connect()` split (discord.go lines 290-401) is the concrete pattern
to follow, not just the principle:

- **`New(homeserverURL string, opts ...Option) (*Frontend, error)`**
  constructs the `Frontend` struct, applies options (including
  `WithHTTPClient`/`WithMatrixClient` test seams), builds a `mautrix.Client`
  via `mautrix.NewClient(homeserverURL, ...)` — but does **not** perform
  login, does **not** start syncing. Mirrors Discord's `New()` building the
  `bot.Client`/REST clients but not calling `OpenGateway`.
- **`Connect(ctx context.Context) error`** performs the actual
  network-touching handshake: either sets the pre-minted access token
  directly on the client (token-auth path) or calls the standard
  `POST /_matrix/client/v3/login` (password path, per §1) to obtain one,
  then starts mautrix-go's sync loop (`client.Sync()`, mautrix-go's
  equivalent of Discord's `OpenGateway` — Matrix's client-server sync is
  itself a long-poll under the hood, `GET /_matrix/client/v3/sync` with a
  `since` token and a `timeout`, structurally closer to Telegram's
  `getUpdates` long-poll than to Discord's websocket, worth noting since it
  means Matrix needs neither a heartbeat/resume state machine nor an
  IDENTIFY budget concern — the sync loop's own `since`-token continuation
  IS the resume mechanism, and it's a plain HTTP long-poll retry on
  failure, not a stateful connection to re-establish).
- **`WithHTTPClient(c *http.Client) Option`** — injected into
  `mautrix.NewClient`'s underlying HTTP transport, mirroring
  `telegram.WithHTTPClient`/`discord.WithHTTPClient` exactly, so
  `httptest`-server-backed unit tests never need a real homeserver.
- **`WithMatrixClient(c MautrixAPI) Option`** (an interface capturing just
  the subset of `*mautrix.Client` methods this package calls — `Login`,
  `Sync`/`SyncWithContext`, `SendMessageEvent` or equivalent, `JoinedMembers`
  for the DM-member-count check in §3) — the most direct test seam,
  mirroring Discord's `WithChannels`/`WithUsers` options that let
  `discord_test.go` exercise `Send()`'s classification logic against a fake
  `rest.Channels` with zero real or fake HTTP transport at all. A hand-
  written fake satisfying `MautrixAPI` lets `matrix_test.go` exercise
  `gate()`/`Send()`'s permanent-vs-retryable logic the same way.
- Event handling (the sync-loop analogue of Discord's `onMessageCreate`)
  should, exactly like Discord's `gate()`, operate on a platform-neutral
  `inboundMessage` struct derived from mautrix-go's timeline event type —
  never take mautrix-go's raw event/client types directly into the gating
  logic — so `gate()` is unit-testable with plain Go structs, no sync loop,
  no real or fake homeserver required, matching discord.go's own stated
  rationale for why `gate()` takes `inboundMessage` rather than
  `*events.MessageCreate` directly (discord.go's package doc comment,
  lines 18-23).
- **No real Matrix homeserver, account, access token, or credential is
  used in any test** — `matrix_test.go` (once written) uses only
  `httptest.Server` and/or the `MautrixAPI` fake, identical in spirit to
  `discord_test.go`'s stated approach.

## 7. Security checklist

Concrete, mapped to this design:

- [ ] **Access token / password never hardcoded, never logged.**
      `access_token_env`/`password_env` config fields (matches
      `token_env`'s pattern exactly) — read via `os.Getenv` at startup.
      Real secret value stored in the KeePassXC vault
      (`~/vessel-log/vault/vessel.kdbx`) per this machine's existing
      credential-vault convention if/when this is actually deployed —
      never committed, never in `config.json`. **No such secret has been
      generated, requested, or referenced anywhere in this design pass.**
- [ ] **Sender-gated on Matrix user ID via a dedicated Authorizer** (§3),
      never on room ID. No code path treats "message arrived in room X" as
      sufficient authorization; the sender-id check runs regardless of
      which room the message came from.
- [ ] **Fail-closed defaults**: no `matrix` config block (or
      `enabled: false`) ⇒ frontend goroutine never starts at all — same
      stance as `DiscordConfig.Enabled`'s doc comment. Empty
      allowlist+admins denies everyone.
- [ ] **Homeserver-agnostic, no implementation-specific trust
      assumptions** (§1) — no hardcoded homeserver URL, no server-specific
      API extension used, so a self-hosted Conduit/Synapse/Dendrite
      instance and a public server are handled by identical code.
- [ ] **E2E scope is explicit, not silently absent** (§2) — `e2e_enabled`
      exists in config now and fails validation loudly (§5) rather than
      being silently ignored, so an operator can never believe encryption
      is active when it isn't. Undecryptable content in an encrypted room
      is logged-and-dropped, never silently treated as empty/ignorable
      text (§2).
- [ ] **Room-join policy**: the bot should not auto-accept arbitrary room
      invites in the plaintext MVP — an unsolicited invite into a room is
      itself an unauthenticated-sender surface (anyone who knows the bot's
      Matrix ID can invite it) closely analogous to Discord's guild-invite
      surface. Recommend: invites auto-accepted only from an
      already-allowlisted admin/user id (checked against the SAME
      Authorizer as message sending — §3), everything else logged and left
      pending for manual admin action, mirroring the
      allowlist-pending-request pattern both existing frontends already
      use for unauthorized senders.
- [ ] **`M_FORBIDDEN`/`M_TOO_LARGE`/other permanent 4xx reviewed** before
      shipping (§4) so these don't silently burn `maxRetryAttempts`
      retries each, the exact class of incident that motivated
      `maxMessageLen`'s original Telegram fix and its Discord repeat.
      Confirm whether mautrix-go already exposes structured Matrix error
      codes (`M_LIMIT_EXCEEDED`, `M_TOO_LARGE`, etc.) rather than only raw
      HTTP status, and classify on those where available for higher
      precision than status-code-alone.
- [ ] **Injectable HTTP client / mautrix client construction split from
      `Connect()`** (§6), matching Telegram's `WithHTTPClient`/`WithBaseURL`
      and Discord's `WithChannels`/`WithUsers`/`Connect()` split — keeps
      this endpoint unit-testable without a real homeserver or real
      credentials.
- [ ] **Rate-limit correctness**: check whether mautrix-go's client already
      parses `M_LIMIT_EXCEEDED`'s `retry_after_ms` and self-throttles
      before shipping a parallel hand-rolled backoff on top of it — same
      "don't duplicate what the library already gets right" stance
      discord/DESIGN.md §4/§8 took for disgo's rate limiter.

## 8. Out of scope for this pass

Deliberately minimal — a working plaintext text-relay frontend, not a
kitchen sink, mirroring discord/DESIGN.md §7's identical stance:

- **End-to-end encryption** (§2) — the single largest deliberate cut, with
  its own staged follow-up rather than a vague TODO.
- **Device verification / cross-signing UX** — even once E2E ships (§2),
  scoped initially to TOFU for the bot's single device; interactive/emoji
  SAS verification flows and cross-signing are a further follow-up beyond
  that, not part of this pass at all.
- **Key backup / session recovery** (`m.megolm_backup.v1` etc.) — relevant
  only once E2E exists; not designed here.
- **Multi-device support** — this bot runs as exactly **one** Matrix
  device. No multi-device session fan-out, no device-list-change handling
  beyond what's needed for TOFU in the eventual E2E follow-up.
- **Group/multi-member rooms** — MVP is DM-only, mirroring Discord's own
  DM-first staging (discord/DESIGN.md §1 top-level framing and §7). The
  `chat_id`/`room_id` field split (§3) is designed to accommodate a later
  group-room follow-up without a breaking rename, but group-room gating
  policy (mention-to-address, analogous to Discord's
  `require_mention_in_guild`) is not designed in this pass.
- **Room-invite auto-accept beyond the admin-only case** (§7) — no general
  "join any public room" or discovery/directory features.
- **Bridging to other Matrix rooms/spaces**, or acting as an actual
  mautrix-style bridge (e.g. bridging Matrix to some third protocol) — this
  is a relay frontend Endpoint like Telegram/Discord, not a bridge; no
  relation to the mautrix *bridge* products beyond sharing the underlying
  client library.
- **Spaces** (Matrix's room-grouping/hierarchy feature) — no interaction
  with spaces at all.
- **Read receipts / typing indicators / presence** — no `Message`/`Meta`
  semantics exist upstream for the Broker to consume these (same reasoning
  discord/DESIGN.md §7 gave for reactions), and they're not relay-relevant.
- **Rich formatting** (`m.text` with `formatted_body`/HTML, per Matrix's
  spec) — plain `m.text` body only, matching both existing frontends'
  plain-text-only stance and avoiding the same "structured content the
  model must correctly produce" scope-creep discord/DESIGN.md §7 flagged
  for embeds.
- **Media/file uploads** (`m.image`, `m.file`, etc., via the Matrix content
  repository `/_matrix/media/v3/upload`) — text only.
- **Application-service (`as_token`) registration mode** — this bot
  authenticates as a normal user account (access token or login), not as a
  registered Application Service with its own namespace-reserved user IDs.
  AS mode is a different, heavier integration model (used by bridges that
  need to mint many puppeted users) and is not needed for a single relay
  bot account.

## 9. Open questions to resolve before/during implementation

1. **`access.Manager` genericization vs. a separate Matrix-local
   authorizer** (§3) — decide based on how invasive genericizing
   `Manager[T comparable]` proves once actually attempted; either is
   functionally fine.
2. **Broker `chat_id == from_id` invariant carve-out for Matrix group
   rooms** (§3) — deferred until the group-room follow-up (§8), but the
   Broker-side "frontend declares its own invariant-exemption" mechanism
   (whichever of Discord's Option A/B-equivalent gets picked) should be
   designed once, generically, rather than growing a third bespoke
   per-frontend carve-out when Matrix group rooms are eventually added.
3. **mautrix-go's exact structured-error surface** for `M_LIMIT_EXCEEDED`/
   `M_TOO_LARGE`/etc. (§4, §7) — needs a quick godoc/source check against
   the actual library version pinned at implementation time, since this
   design was written against publicly documented behavior, not against
   code actually compiled/run in this pass.
4. **Single shared allowlist file convention** — whether Matrix gets its
   own `matrix_allowlist.json` (mirroring Discord's own §9 open-question
   answer of "start with a second file") or folds into a single
   multi-platform allowlist structure now that there would be three
   frontends — recommend following Discord's precedent (separate file,
   simplest persistence code) unless operating three separate files in
   practice proves annoying.
