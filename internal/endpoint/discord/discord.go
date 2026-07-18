// Package discord implements a relay frontend Endpoint backed by the Discord
// Bot API. Unlike Telegram's stateless long-poll, Discord's real-time events
// arrive over the Gateway, a stateful websocket protocol with heartbeats and
// session resume. This package uses github.com/disgoorg/disgo to own that
// websocket/heartbeat/resume/backoff machinery rather than
// hand-rolling it — that class of bug (silent disconnect, missed messages,
// burned IDENTIFY budget) is exactly what disgo already exists to get right.
//
// Inbound messages are gated by a sender allowlist (on the sender's Discord
// user id, never the channel/guild id) — an ungated channel is a
// prompt-injection vector, same reasoning as the Telegram frontend. Guild
// (server) messages are additionally gated: the guild must be explicitly
// allow-listed and, by default, the message must @-mention the bot or reply
// to one of its messages — a guild channel is inherently multi-party, so
// channel membership alone is not "addressed to the bot" the way every
// message in a Telegram private chat or Discord DM is.
//
// The gating logic (gate()) operates on the platform-neutral inboundMessage
// struct rather than disgo's event/bot types directly, so it is unit
// testable without spinning up a real gateway connection or bot token — see
// discord_test.go. The outbound REST client is injectable the same way
// Telegram's HTTP client is (WithHTTPClient/WithBaseURL), so Send() is also
// testable against an httptest server.
package discord

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"

	"github.com/jeanhaley32/agent-relay/internal/endpoint/senderr"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// maxMessageLen is Discord's hard per-message character cap (not 4096 like
// Telegram). Marked senderr.Permanent since retrying an oversized message
// just repeats the same guaranteed failure.
const maxMessageLen = 2000

type permanentSendError = senderr.Permanent

// Authorizer decides whether a sender may use the relay and records requests
// from those who may not (so an admin can approve them later). It mirrors
// telegram.Authorizer's two methods exactly (id type aside) so a single
// access.Manager instance — with the int64/snowflake.ID conversion done at
// this package's boundary — can back both frontends' allowlists without
// access itself knowing about Discord.
type Authorizer interface {
	Allowed(id snowflake.ID) bool
	Record(id snowflake.ID, name string)
}

type staticAuthorizer map[snowflake.ID]bool

func (s staticAuthorizer) Allowed(id snowflake.ID) bool { return s[id] }
func (s staticAuthorizer) Record(snowflake.ID, string)  {}

// managerAdapter wraps an access.Manager-shaped int64 authorizer so it can
// back this package's snowflake.ID-keyed Authorizer. Discord snowflakes are
// uint64 but real values are time-based and well under math.MaxInt64, so
// the conversion is lossless in practice.
type managerAdapter struct {
	m interface {
		Allowed(id int64) bool
		Record(id int64, name string)
	}
}

func (a managerAdapter) Allowed(id snowflake.ID) bool        { return a.m.Allowed(int64(id)) }
func (a managerAdapter) Record(id snowflake.ID, name string) { a.m.Record(int64(id), name) }

// Int64Authorizer adapts an int64-keyed authorizer (e.g. *access.Manager,
// which is intentionally platform-agnostic) into this package's
// snowflake.ID-keyed Authorizer. Use with
// WithAuthorizer to share one allowlist/admin set between the Telegram and
// Discord frontends.
func Int64Authorizer(m interface {
	Allowed(id int64) bool
	Record(id int64, name string)
}) Authorizer {
	return managerAdapter{m: m}
}

// inboundMessage is the platform-neutral shape gate() operates on, derived
// from a disgo events.MessageCreate by the gateway listener. Keeping gate()
// decoupled from disgo's event/bot types is what makes the sender/guild/
// mention policy unit-testable without a real gateway connection or bot
// token.
type inboundMessage struct {
	messageID    snowflake.ID
	channelID    snowflake.ID
	guildID      *snowflake.ID // nil for DMs
	authorID     snowflake.ID
	authorName   string
	authorIsBot  bool
	content      string
	mentionsBot  bool
	isReplyToBot bool
}

// Frontend is a Discord Bot API frontend Endpoint.
type Frontend struct {
	token  string
	auth   Authorizer
	logger *log.Logger

	allowGuildMessages    bool
	allowedGuildIDs       map[snowflake.ID]bool
	requireMentionInGuild bool

	selfID snowflake.ID // this bot's own user id, for mention/reply-to-bot checks

	channels   rest.Channels // outbound REST (Send) — injectable for tests
	users      rest.Users    // outbound REST (DM channel resolution) — injectable for tests
	baseURL    string        // "" ⇒ disgo's default Discord API base
	httpClient *http.Client  // injectable for tests
	client     *bot.Client   // real gateway connection; nil when using a fake transport in tests

	recv     chan relay.Message
	recvOnce sync.Once // guards close(recv) so Close() and any other closer
	// can't double-close or race with sends — see Close's doc comment.
	ctx    context.Context
	cancel context.CancelFunc

	// convChannels remembers, for every ConversationID gate() has ever seen
	// inbound, which physical channel id it maps to. sendOnce falls back to
	// this when a caller-constructed relay.Message (scheduled reminder, admin
	// notice, mcp reply) carries only ConversationID/chat_id and no
	// Meta["channel_id"] at all.
	convChannels sync.Map // ConversationID (string) -> snowflake.ID

	// dmConvs marks which convChannels entries are DMs (chatID == sender user
	// id) rather than guild channels, so KnownConversation can re-check
	// f.auth.Allowed live for DMs without misclassifying guild channel ids
	// (which are never valid auth-manager entries and must stay allowed
	// purely by virtue of having been seen inbound). See KnownConversation's
	// doc comment.
	dmConvs sync.Map // ConversationID (string) -> struct{}

	// dmChannels caches user-id -> resolved DM channel id, so a reply to a
	// user we have never actually seen a convChannels entry for yet (e.g. the
	// very first scheduled reminder to a user, before they've DMed the bot in
	// this process's lifetime) still routes: sendOnce treats an unresolved
	// ConversationID as a Discord user id and resolves/creates the DM channel
	// via POST /users/@me/channels (rest.Users.CreateDMChannel). See
	// sendOnce's doc comment.
	dmChannels sync.Map // user snowflake.ID -> snowflake.ID (DM channel)

	// Same retry-queue rationale as Telegram's Frontend: Send() returns
	// immediately and callers generally discard the error, so a transient
	// failure needs a background path that keeps retrying instead of
	// silently vanishing.
	retryQueue     chan retryItem
	sendFailures   atomic.Int64
	permanentDrops atomic.Int64
	queueDepth     atomic.Int64

	// gatewayReconnects / lastGatewayEventAt are Discord's analogues of
	// Telegram's getUpdatesFailures/lastPollSuccess: the
	// signal that detects a Discord-side outage independent of whether any
	// outbound Send happened to occur during it. gatewayReconnects counts
	// only non-resumable reconnects (fresh IDENTIFY, not RESUME) since those
	// are the ones that risk a message gap and burn IDENTIFY budget.
	gatewayReconnects  atomic.Int64
	sawReady           atomic.Bool  // gates the first Ready (initial connect, not a reconnect) out of gatewayReconnects
	lastGatewayEventAt atomic.Int64 // unix seconds

	// recvDrops counts inbound messages dropped because f.recv was still full
	// after the 5s wait in onMessageCreate. Divergence from Telegram's
	// pollLoop (which blocks indefinitely on send instead of ever dropping)
	// is deliberate here: onMessageCreate runs on disgo's shared event
	// dispatch goroutine, so blocking it indefinitely would stall ALL gateway
	// event processing (heartbeats included) for as long as the backend stays
	// backed up, risking a heartbeat-ack timeout and forced reconnect — worse
	// than dropping one message. Counted (not just logged) so it's visible on
	// /metrics instead of only in logs, matching the send-path drop counters.
	recvDrops atomic.Int64
}

type retryItem struct {
	msg      relay.Message
	attempts int
	nextAt   time.Time
}

const (
	maxRetryAttempts   = 12
	retryQueueCapacity = 200
)

// Option configures a Frontend.
type Option func(*Frontend)

// WithBaseURL overrides the Discord REST API base (for tests).
func WithBaseURL(u string) Option { return func(f *Frontend) { f.baseURL = strings.TrimRight(u, "/") } }

// WithHTTPClient injects an HTTP client for the REST client (for tests /
// custom transport) — mirrors telegram.WithHTTPClient.
func WithHTTPClient(c *http.Client) Option { return func(f *Frontend) { f.httpClient = c } }

// WithChannels injects the rest.Channels implementation Send() uses directly,
// bypassing New()'s normal rest.NewClient/rest.NewChannels construction
// entirely. This is the most direct test seam: a fake rest.Channels lets
// discord_test.go exercise Send()'s permanent-vs-retryable classification
// without any real or fake HTTP transport at all.
func WithChannels(c rest.Channels) Option { return func(f *Frontend) { f.channels = c } }

// WithUsers injects the rest.Users implementation used to resolve/create a
// DM channel for a bare user id when sendOnce has no channel_id to work with
// (see the DM-routing doc comment on sendOnce). Same test-seam rationale as
// WithChannels.
func WithUsers(u rest.Users) Option { return func(f *Frontend) { f.users = u } }

// WithAllowlist sets a fixed set of permitted sender user ids. REQUIRED for
// real use if no Authorizer is supplied: an empty allowlist denies everyone
// (fail closed). For dynamic approval, use WithAuthorizer instead.
func WithAllowlist(ids ...snowflake.ID) Option {
	return func(f *Frontend) {
		s := staticAuthorizer{}
		for _, id := range ids {
			s[id] = true
		}
		f.auth = s
	}
}

// WithAuthorizer supplies a custom authorization backend (e.g. access.Manager
// via Int64Authorizer) that can grant access dynamically and record pending
// requests.
func WithAuthorizer(a Authorizer) Option { return func(f *Frontend) { f.auth = a } }

func WithLogger(l *log.Logger) Option { return func(f *Frontend) { f.logger = l } }

// WithAllowGuildMessages enables guild (server) messages in addition to DMs.
// Default is DM-only, which requests zero privileged gateway intents.
// Guild messages additionally require the guild to be listed
// via WithAllowedGuildIDs — an empty list denies all guilds even with this
// enabled (fail closed, mirrors access.Manager's empty-allowlist stance).
func WithAllowGuildMessages(on bool) Option { return func(f *Frontend) { f.allowGuildMessages = on } }

// WithAllowedGuildIDs sets the guilds eligible for message relay when guild
// messages are enabled.
func WithAllowedGuildIDs(ids ...snowflake.ID) Option {
	return func(f *Frontend) {
		s := map[snowflake.ID]bool{}
		for _, id := range ids {
			s[id] = true
		}
		f.allowedGuildIDs = s
	}
}

// WithRequireMentionInGuild controls whether guild messages must @-mention
// the bot or reply to one of its messages to be relayed. Default true — see
// the package doc comment for why ambient channel membership isn't enough.
func WithRequireMentionInGuild(on bool) Option {
	return func(f *Frontend) { f.requireMentionInGuild = on }
}

// WithSelfID sets the bot's own user id directly, primarily for tests that
// don't go through a real gateway handshake (where selfID is normally
// discovered from client.ID() after connecting).
func WithSelfID(id snowflake.ID) Option { return func(f *Frontend) { f.selfID = id } }

// New builds a Discord frontend and opens the gateway connection. Close stops
// it. token must be a real bot token to connect for real; tests that only
// exercise gate()/Send() against a fake transport can pass a dummy token and
// never call OpenGateway (see discord_test.go).
func New(token string, opts ...Option) (*Frontend, error) {
	f := &Frontend{
		token:                 token,
		auth:                  staticAuthorizer{}, // fail-closed default (denies everyone)
		logger:                log.New(io.Discard, "", 0),
		allowedGuildIDs:       map[snowflake.ID]bool{},
		requireMentionInGuild: true,
		recv:                  make(chan relay.Message, 32),
		retryQueue:            make(chan retryItem, retryQueueCapacity),
	}
	for _, o := range opts {
		o(f)
	}

	if f.channels == nil {
		restOpts := []rest.ClientConfigOpt{}
		if f.httpClient != nil {
			restOpts = append(restOpts, rest.WithHTTPClient(f.httpClient))
		}
		if f.baseURL != "" {
			restOpts = append(restOpts, rest.WithURL(f.baseURL))
		}
		f.channels = rest.NewChannels(rest.NewClient(token, restOpts...), discord.AllowedMentions{})
	}
	if f.users == nil {
		restOpts := []rest.ClientConfigOpt{}
		if f.httpClient != nil {
			restOpts = append(restOpts, rest.WithHTTPClient(f.httpClient))
		}
		if f.baseURL != "" {
			restOpts = append(restOpts, rest.WithURL(f.baseURL))
		}
		f.users = rest.NewUsers(rest.NewClient(token, restOpts...))
	}

	ctx, cancel := context.WithCancel(context.Background())
	f.ctx = ctx
	f.cancel = cancel
	go f.startRetryWorker(ctx)
	return f, nil
}

// Connect builds the real disgo gateway client and opens the websocket
// connection, wiring OnMessageCreate into f.recv via gate(). Split out from
// New() so tests can construct a Frontend (for gate()/Send() unit tests)
// without ever dialing Discord, keeping this endpoint unit-testable without
// a real bot token.
//
// recv must not be closed until the gateway's dispatch loop has stopped,
// or onMessageCreate can send-to-closed-channel; recv is therefore closed
// from Close() only, after client.Close() has blocked until disgo's
// callbacks have all returned — never from a goroutine racing ctx.Done()
// against onMessageCreate's own `f.recv <- msg` sends.
//
// IMPORTANT — Close() does NOT cancel the ctx given here. The ctx passed to
// Connect is only used for the initial client.OpenGateway(ctx) dial; f.cancel
// (called by Close) is a SEPARATE context created in New(), used only to stop
// the retry worker. There is deliberately no context.WithCancel wrapping
// tying the two together. This means: if the caller's ctx is cancelled
// without ever calling Close(), the gateway connection is NOT torn down by
// that alone and f.recv is never closed — a goroutine blocked reading Recv()
// (e.g. the Broker) can wait forever. Close() MUST always be called to
// release recv, regardless of ctx state. Harmless in relayd's current wiring
// (it passes context.Background() to Connect and always calls Close on
// shutdown), but a future caller that expects ctx cancellation alone to be
// sufficient will hang.
func (f *Frontend) Connect(ctx context.Context) error {
	intents := []gateway.Intents{gateway.IntentGuilds, gateway.IntentDirectMessages}
	if f.allowGuildMessages {
		intents = append(intents, gateway.IntentGuildMessages, gateway.IntentMessageContent)
	}

	client, err := disgo.New(f.token,
		bot.WithDefaultGateway(),
		bot.WithGatewayConfigOpts(gateway.WithIntents(intents...)),
		bot.WithEventListenerFunc(f.onMessageCreate),
		// Ready fires on every fresh IDENTIFY (non-resumed reconnect,
		// including the very first connect); Resumed fires on a successful
		// RESUME. Both are "the gateway is alive" signals for
		// lastGatewayEventAt: any gateway event counts, not just
		// inbound messages — a quiet channel with a healthy gateway shouldn't
		// look wedged). Only Ready increments gatewayReconnects, and only
		// from the second one onward: the first Ready is the initial connect,
		// not a reconnect, so counting it would make the metric read 1 after
		// every clean startup with zero actual reconnects. A RESUME doesn't
		// risk a message gap or burn IDENTIFY budget the way a fresh session
		// does, so it's deliberately not counted either (see field doc).
		bot.WithEventListenerFunc(func(e *events.Ready) {
			f.lastGatewayEventAt.Store(time.Now().Unix())
			if f.sawReady.Swap(true) {
				f.gatewayReconnects.Add(1)
			}
		}),
		bot.WithEventListenerFunc(func(e *events.Resumed) {
			f.lastGatewayEventAt.Store(time.Now().Unix())
		}),
	)
	if err != nil {
		return fmt.Errorf("discord: build client: %w", err)
	}
	f.client = client
	// Only fill selfID from the real client if WithSelfID wasn't already
	// supplied — otherwise Connect would silently void that option's
	// contract for any caller that also connects (e.g. a test wiring a fake
	// transport but still exercising Connect). client.ID() is authoritative
	// in production, where selfID starts zero-valued.
	if f.selfID == 0 {
		f.selfID = client.ID()
	}

	if err := client.OpenGateway(ctx); err != nil {
		return fmt.Errorf("discord: open gateway: %w", err)
	}
	go f.watchHeartbeat(client)
	return nil
}

// watchHeartbeat periodically checks the gateway's heartbeat-ack latency and
// treats a nonzero value as proof the connection is alive, refreshing
// lastGatewayEventAt independent of Ready/Resumed/message traffic. Without
// this, a healthy but quiet DM-only bot (no reconnects, no new messages) goes
// "stale" by lastGatewayEventAt after any 5+ minute lull — a false positive,
// since Ready/Resumed only fire on (re)connect, not on an ongoing
// idle-but-healthy session. 30s poll interval, well under the alert's
// 5-minute threshold, so a genuinely dead gateway (Latency() staying 0)
// still goes stale and fires.
func (f *Frontend) watchHeartbeat(client *bot.Client) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			if client.Gateway == nil {
				continue
			}
			if client.Gateway.Latency() > 0 {
				f.lastGatewayEventAt.Store(time.Now().Unix())
			}
		}
	}
}

func (f *Frontend) Name() string               { return "discord" }
func (f *Frontend) Recv() <-chan relay.Message { return f.recv }

// Close stops the frontend: closes the gateway with a clean close frame
// (which blocks until disgo's event-dispatch loop has stopped, so no
// onMessageCreate callback can still be in flight afterward), THEN closes
// f.recv, THEN cancels the retry worker's context. Ordering matters: recv
// must not be closed until the gateway's dispatch loop is guaranteed
// stopped, or onMessageCreate can still send-to-closed-channel. recvOnce
// also guards against a double-close if Close is ever called twice.
func (f *Frontend) Close() error {
	if f.client != nil {
		f.client.Close(context.Background())
	}
	f.recvOnce.Do(func() { close(f.recv) })
	f.cancel()
	return nil
}

// onMessageCreate is disgo's OnMessageCreate callback in real use: it
// extracts the platform-neutral inboundMessage and hands it to gate(), then
// publishes anything gate() allows onto f.recv.
func (f *Frontend) onMessageCreate(e *events.MessageCreate) {
	f.lastGatewayEventAt.Store(time.Now().Unix())
	m := e.Message

	mentionsBot := false
	for _, u := range m.Mentions {
		if u.ID == f.selfID {
			mentionsBot = true
			break
		}
	}
	isReplyToBot := m.ReferencedMessage != nil && m.ReferencedMessage.Author.ID == f.selfID

	in := inboundMessage{
		messageID:    m.ID,
		channelID:    m.ChannelID,
		guildID:      m.GuildID,
		authorID:     m.Author.ID,
		authorName:   m.Author.Username,
		authorIsBot:  m.Author.Bot,
		content:      m.Content,
		mentionsBot:  mentionsBot,
		isReplyToBot: isReplyToBot,
	}
	msg, ok := f.gate(in)
	if !ok {
		return
	}
	select {
	case f.recv <- msg:
	case <-time.After(5 * time.Second):
		f.recvDrops.Add(1)
		f.logger.Printf("discord: recv channel full/blocked, dropped message from channel %s", in.channelID)
	}
}

// gate applies the sender/guild/mention policy and, if the
// message passes, returns the relay.Message to publish. Pure function of its
// input (plus the Frontend's static config/authorizer) so it's directly unit
// testable without any disgo event/gateway machinery.
func (f *Frontend) gate(m inboundMessage) (relay.Message, bool) {
	if m.authorIsBot {
		return relay.Message{}, false // never relay other bots (including ourselves) — avoids bot-loops
	}
	if m.content == "" {
		return relay.Message{}, false
	}

	if m.guildID != nil {
		// Guild message: default-deny on source, mirroring Telegram's
		// outright refusal of non-private chats. This runs
		// BEFORE the sender allowlist check so a rando in some public guild
		// the bot got invited to never even reaches that check.
		if !f.allowGuildMessages || !f.allowedGuildIDs[*m.guildID] {
			f.logger.Printf("discord: dropped message from non-allowed guild id=%s channel=%s sender=%s",
				m.guildID, m.channelID, m.authorID)
			return relay.Message{}, false
		}
		if f.requireMentionInGuild && !m.mentionsBot && !m.isReplyToBot {
			f.logger.Printf("discord: dropped unaddressed guild message channel=%s sender=%s (no mention/reply)",
				m.channelID, m.authorID)
			return relay.Message{}, false
		}
	}

	if !f.auth.Allowed(m.authorID) {
		f.auth.Record(m.authorID, m.authorName) // queue as a pending request
		f.logger.Printf("discord: dropped message from unauthorized sender id=%s (%s) — recorded as pending",
			m.authorID, m.authorName)
		return relay.Message{}, false
	}

	// chat_id/from_id identity-pair invariant. In DMs,
	// Discord's DM channel id is its own snowflake namespace distinct from
	// the sender's user id, unlike Telegram where a private chat's id IS the
	// sender's id. The Broker's session gate only needs *a* stable
	// per-conversation key, genuinely keyed on the user for DMs — so we set
	// conversation/chat_id to the user id for DMs (not the physical channel
	// id), and to the channel id for guild messages (where per-channel
	// conversation scoping is the correct behavior — multiple users share a
	// guild channel). Replies always route via Meta["channel_id"], which is
	// tracked separately from the invariant-checked field either way.
	var convID string
	if m.guildID == nil {
		convID = m.authorID.String()
	} else {
		convID = m.channelID.String()
	}

	// Remember convID -> physical channel id so a later relayd-originated
	// reply (scheduled reminder, admin notice, mcp reply) that only carries
	// ConversationID/chat_id — never Meta["channel_id"] — can still be
	// routed by sendOnce. See the convChannels field doc comment.
	f.convChannels.Store(convID, m.channelID)
	if m.guildID == nil {
		f.dmConvs.Store(convID, struct{}{})
	}

	msg := relay.Message{
		ConversationID: convID,
		Role:           relay.User,
		Text:           m.content,
		Meta: map[string]string{
			"chat_id":    convID,
			"channel_id": m.channelID.String(),
			"from_id":    m.authorID.String(),
			"from_name":  m.authorName,
		},
	}
	if m.guildID != nil {
		// Marks this message as one where chat_id (the channel id) is NOT
		// expected to equal from_id (the sender's user id) — the Broker's
		// identity-pair tripwire (relay.go) treats presence of this key as
		// an explicit per-frontend opt-out of that invariant. DM messages
		// deliberately don't set this:
		// there chat_id IS from_id by construction (see convID above), so
		// the invariant still holds and still guards against a regression.
		msg.Meta["guild_id"] = m.guildID.String()
	}
	return msg, true
}

// discordSnowflakeFloor is a lower bound on any snowflake id minted by the
// real Discord network today. Discord's snowflake epoch is 2015-01-01; the
// timestamp component occupies the high 42 bits (shifted left 22), so any id
// generated since roughly 2020 already exceeds 1e17. Telegram document ids as
// int64, which can in principle occupy up to 52 bits (~4.5e15) - above this
// floor - so this heuristic is not a proof, only a practical disambiguator:
// real Telegram chat/user ids in use here are far smaller (a handful of
// digits, or negative for groups/supergroups), so a collision would require a
// Telegram id in a range Telegram doesn't currently hand out. Used by
// OwnsConversationID to disambiguate a bare numeric ConversationID between
// frontends when relay.MultiFrontend has never seen it delivered inbound —
// see relay.Claimer's doc comment.
const discordSnowflakeFloor = 1_000_000_000_000_000 // 1e15, well below current real Discord ids, well above real-world Telegram ids

// OwnsConversationID implements relay.Claimer: it reports whether id is
// plausibly a Discord conversation id (DM user id or guild channel id) based
// on snowflake syntax and magnitude, without requiring the id to have been
// seen inbound this process lifetime.
func (f *Frontend) OwnsConversationID(id string) bool {
	sf, err := snowflake.Parse(id)
	if err != nil {
		return false
	}
	return int64(sf) >= discordSnowflakeFloor
}

// KnownConversation reports whether chatID is a conversation id this
// Frontend has already seen and gated inbound — i.e. it's a DM user id or a
// guild channel id from an allowed guild that passed gate()'s checks. Used
// by relayd's outbound allowlist (OutboundAllowed in cmd/relayd/main.go) to
// permit replies into guild channels: guild channels are never present in
// the Discord access.Manager's user-id allowlist (that only holds sender
// ids), so without this every model reply into an allowed guild channel
// would be silently dropped even though the inbound message that prompted
// it was itself allowlisted. Fail-closed for anything never seen inbound.
//
// For DMs specifically, chatID IS the sender's user id (see gate()'s convID
// comment), so we additionally re-check f.auth.Allowed(chatID) live rather
// than trusting the cached convChannels entry alone: convChannels is never
// purged, so without this a user denied/removed from the allowlist after an
// earlier authorized DM would stay reachable for outbound replies until
// process restart — an asymmetry with Telegram, whose outbound gate consults
// acc.Allowed live. Guild channel ids are never valid entries in the
// user-id-keyed auth manager, so this check is a no-op (harmlessly false)
// for the guild-channel path and doesn't change that behavior.
func (f *Frontend) KnownConversation(chatID string) bool {
	_, ok := f.convChannels.Load(chatID)
	if !ok {
		return false
	}
	if sf, err := snowflake.Parse(chatID); err == nil && f.auth.Allowed(sf) {
		return true
	}
	// Not currently allowed as a DM sender - only still "known" if this is a
	// guild channel id, tracked separately in dmConvs (never set for guild
	// channels, so this always allows the guild-channel path through).
	_, isDM := f.dmConvs.Load(chatID)
	return !isDM
}

// Send delivers a message to the channel named by m.Meta["channel_id"]
// (falling back to m.ConversationID) via the REST CreateMessage endpoint. A
// message over Discord's real 2000-char limit is split into multiple
// messages (see senderr.Split) so it isn't silently dropped. Every chunk is
// attempted regardless of an earlier chunk's outcome, since a transient
// failure is already queued by sendChunk and skipping the rest would just
// strand them; only the first permanent failure is returned to the caller,
// mirroring telegram.Frontend.Send.
func (f *Frontend) Send(ctx context.Context, m relay.Message) error {
	chunks := senderr.Split(m.Text, maxMessageLen)
	if len(chunks) > 1 {
		var permErr error
		for _, chunk := range chunks {
			cm := m
			cm.Text = chunk
			if err := f.sendChunk(ctx, cm); err != nil {
				var perm permanentSendError
				if errors.As(err, &perm) && permErr == nil {
					permErr = err
				}
			}
		}
		return permErr
	}
	return f.sendChunk(ctx, m)
}

// sendChunk sends one already-within-limit message, classifying the result
// as permanent failure (returned as-is) or transient (queued for background
// retry).
func (f *Frontend) sendChunk(ctx context.Context, m relay.Message) error {
	err := f.sendOnce(ctx, m)
	if err == nil {
		return nil
	}
	f.sendFailures.Add(1)
	var perm permanentSendError
	if errors.As(err, &perm) {
		f.logger.Printf("discord send permanently failed (not retrying): %v", err)
		return err
	}
	f.logger.Printf("discord send failed, queuing for background retry: %v", err)
	f.enqueueRetry(m)
	return err
}

// resolveChannel figures out which physical channel to CreateMessage into
// for m, in priority order:
//
//  1. Meta["channel_id"] — set by gate() for a message that IS a reply
//     within the same inbound turn (the common in-process case).
//  2. convChannels — the channel id last seen for this ConversationID by
//     gate(), covering relayd-originated replies (scheduler/admin/mcp)
//     that only carry ConversationID/chat_id, as long as SOME inbound
//     message from that conversation has been observed since process start.
//  3. dmChannels / CreateDMChannel — last resort for a relayd-originated
//     message whose ConversationID has never been seen inbound this
//     process lifetime at all (e.g. the very first scheduled reminder to a
//     user). For a DM, ConversationID IS the recipient's user id (see
//     gate()'s convID comment), so it's treated as one and resolved via
//     POST /users/@me/channels, which is idempotent — Discord returns the
//     existing DM channel if one already exists. Without this path, every
//     such message would fall through to
//     CreateMessage(<user id>), which 404s (Unknown Channel) and gets
//     misclassified permanent, silently dropping the reply. A guild
//     ConversationID reaching this branch (no prior inbound message AND no
//     channel_id) will also 404 via this path since the id isn't a real
//     user — that failure is still correctly permanent/logged, just not a
//     silent one.
func (f *Frontend) resolveChannel(ctx context.Context, m relay.Message) (snowflake.ID, error) {
	channelIDStr := m.Meta["channel_id"]
	if channelIDStr == "" {
		if v, ok := f.convChannels.Load(m.ConversationID); ok {
			channelIDStr = v.(snowflake.ID).String()
		}
	}
	if channelIDStr != "" {
		id, err := snowflake.Parse(channelIDStr)
		if err != nil {
			return 0, permanentSendError{Err: fmt.Errorf("discord send: invalid channel_id %q: %w", channelIDStr, err)}
		}
		return id, nil
	}

	if m.ConversationID == "" {
		return 0, permanentSendError{Err: fmt.Errorf("discord send: no channel_id and no conversation id")}
	}
	userID, err := snowflake.Parse(m.ConversationID)
	if err != nil {
		return 0, permanentSendError{Err: fmt.Errorf("discord send: no channel_id and conversation id %q is not a valid user id: %w", m.ConversationID, err)}
	}
	if v, ok := f.dmChannels.Load(userID); ok {
		return v.(snowflake.ID), nil
	}
	dm, err := f.users.CreateDMChannel(userID, rest.WithCtx(ctx))
	if err != nil {
		// Transport/HTTP-status errors here get the same permanent-vs-
		// retryable treatment as CreateMessage does below — reuse it by
		// letting sendOnce's caller (Send) classify via errors.As, since
		// this can be a plain *rest.Error too.
		var restErr *rest.Error
		if errors.As(err, &restErr) && restErr.Response != nil {
			status := restErr.Response.StatusCode
			if status/100 == 4 && status != http.StatusTooManyRequests {
				return 0, permanentSendError{Err: fmt.Errorf("discord CreateDMChannel status %d: %s", status, restErr.Message)}
			}
		}
		return 0, fmt.Errorf("discord: resolve DM channel for user %s: %w", userID, err)
	}
	f.dmChannels.Store(userID, dm.ID())
	return dm.ID(), nil
}

// sendOnce is the actual single REST attempt — both Send() and the retry
// worker call this, so there's exactly one code path that talks to Discord.
// Rate-limit (429) handling is intentionally NOT
// duplicated here: disgo's REST client already parses Discord's
// X-RateLimit-* headers and per-route buckets, which is the whole reason
// disgo was chosen over discordgo. This only classifies the resulting error
// as permanent vs. retryable for the outer retry-queue layer.
func (f *Frontend) sendOnce(ctx context.Context, m relay.Message) error {
	channelID, err := f.resolveChannel(ctx, m)
	if err != nil {
		return err
	}
	if n := utf8.RuneCountInString(m.Text); n > maxMessageLen {
		return permanentSendError{Err: fmt.Errorf(
			"message too long (%d chars, Discord's limit is %d) - split it into multiple replies", n, maxMessageLen)}
	}

	create := discord.MessageCreate{Content: m.Text}
	sent, err := f.channels.CreateMessage(channelID, create, rest.WithCtx(ctx))
	if err == nil {
		// Log the real Discord message id Discord itself assigned, so a
		// "the daemon said success" claim can be cross-checked directly
		// against the platform afterward (e.g. via GET .../messages) rather
		// than trusting the ack alone: a reply_ack can report success while
		// the message never actually appears in the channel history.
		f.logger.Printf("discord send ok: channel=%s message=%s", channelID, sent.ID)
		return nil
	}

	var restErr *rest.Error
	if errors.As(err, &restErr) && restErr.Response != nil {
		status := restErr.Response.StatusCode
		// 429 (rate limited) is retryable — disgo's rate limiter should
		// normally absorb these before they ever surface here, but if one
		// does get through, it's still transient. Every other 4xx (400
		// malformed/too-long, 403 missing SEND_MESSAGES permission, 404
		// unknown/deleted channel) fails identically on retry, so it's
		// classified permanent rather than burning maxRetryAttempts
		// retries on a guaranteed-repeat failure.
		if status/100 == 4 && status != http.StatusTooManyRequests {
			return permanentSendError{Err: fmt.Errorf("discord CreateMessage status %d: %s", status, restErr.Message)}
		}
	}
	return err
}

// enqueueRetry adds a failed message to the background retry queue.
// Non-blocking: if the queue is full (a genuinely extreme backlog), the
// oldest-in-flight item is dropped rather than blocking the caller, which
// would stall the whole relay - a bounded, honest failure mode instead of
// unbounded memory growth or a hung sender. The evicted item is counted in
// permanentDrops so it's visible on /metrics rather than a silent drop.
func (f *Frontend) enqueueRetry(m relay.Message) {
	item := retryItem{msg: m, attempts: 0, nextAt: time.Now().Add(retryBackoff(0))}
	select {
	case f.retryQueue <- item:
		f.queueDepth.Add(1)
	default:
		f.logger.Printf("discord retry queue full (%d), dropping oldest to make room", retryQueueCapacity)
		select {
		case <-f.retryQueue:
			f.queueDepth.Add(-1)
			f.permanentDrops.Add(1)
		default:
		}
		select {
		case f.retryQueue <- item:
			f.queueDepth.Add(1)
		default:
		}
	}
}

func retryBackoff(attempts int) time.Duration {
	d := time.Duration(1<<uint(attempts)) * time.Second
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d
}

// startRetryWorker runs until ctx is cancelled, periodically attempting to
// redeliver queued messages. Call once per Frontend.
func (f *Frontend) startRetryWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var pending []retryItem

	for {
		select {
		case <-ctx.Done():
			return
		case item := <-f.retryQueue:
			// Deliberately not decremented here: the item is moving from the
			// channel into the in-worker pending slice, not leaving the
			// backlog. queueDepth is only decremented once an item is fully
			// resolved (sent, dropped permanent, or exhausted), so the
			// exported gauge reflects the whole retry backlog, not just
			// what's still sitting in the channel buffer.
			pending = append(pending, item)
		case <-ticker.C:
			now := time.Now()
			var stillPending []retryItem
			for _, item := range pending {
				if now.Before(item.nextAt) {
					stillPending = append(stillPending, item)
					continue
				}
				sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				err := f.sendOnce(sendCtx, item.msg)
				cancel()
				if err == nil {
					f.queueDepth.Add(-1)
					f.logger.Printf("discord retry succeeded after %d attempt(s) for channel %s",
						item.attempts+1, item.msg.Meta["channel_id"])
					continue
				}
				var perm permanentSendError
				if errors.As(err, &perm) {
					f.queueDepth.Add(-1)
					f.permanentDrops.Add(1)
					f.logger.Printf("discord retry gave up (permanent failure) for channel %s: %v",
						item.msg.Meta["channel_id"], err)
					continue
				}
				item.attempts++
				if item.attempts >= maxRetryAttempts {
					f.queueDepth.Add(-1)
					f.permanentDrops.Add(1)
					f.logger.Printf("discord retry gave up after %d attempts for channel %s: %v",
						item.attempts, item.msg.Meta["channel_id"], err)
					continue
				}
				item.nextAt = now.Add(retryBackoff(item.attempts))
				stillPending = append(stillPending, item)
			}
			pending = stillPending
		}
	}
}

func (f *Frontend) SendFailures() int64   { return f.sendFailures.Load() }
func (f *Frontend) PermanentDrops() int64 { return f.permanentDrops.Load() }
func (f *Frontend) QueueDepth() int64     { return f.queueDepth.Load() }

func (f *Frontend) RecvDrops() int64 { return f.recvDrops.Load() }

func (f *Frontend) GatewayReconnects() int64  { return f.gatewayReconnects.Load() }
func (f *Frontend) LastGatewayEventAt() int64 { return f.lastGatewayEventAt.Load() }
