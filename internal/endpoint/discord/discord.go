// Package discord implements a relay frontend Endpoint backed by the Discord
// Bot API. Unlike Telegram's stateless long-poll, Discord's real-time events
// arrive over the Gateway, a stateful websocket protocol with heartbeats and
// session resume. This package uses github.com/disgoorg/disgo to own that
// websocket/heartbeat/resume/backoff machinery (see DESIGN.md §1) rather than
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
	"sync/atomic"
	"time"

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

// maxMessageLen is Discord's hard per-message character cap for a bot's
// plain-text message (not 4096 like Telegram — see DESIGN.md §4). Checked
// before ever making the REST call; the resulting error is marked permanent
// (senderr.Permanent) so it is never queued for retry, mirroring the
// Telegram maxMessageLen incident this pattern was built to prevent.
const maxMessageLen = 2000

// permanentSendError is the shared retry-classification type — see
// internal/endpoint/senderr. Aliased locally so call sites read naturally.
type permanentSendError = senderr.Permanent

// Authorizer decides whether a sender may use the relay and records requests
// from those who may not (so an admin can approve them later). It mirrors
// telegram.Authorizer's two methods exactly (id type aside) so a single
// access.Manager instance — with the int64/snowflake.ID conversion done at
// this package's boundary, see DESIGN.md §3 option 1 — can back both
// frontends' allowlists without access itself knowing about Discord.
type Authorizer interface {
	Allowed(id snowflake.ID) bool
	Record(id snowflake.ID, name string)
}

// staticAuthorizer is a fixed allowlist (WithAllowlist). It records nothing.
type staticAuthorizer map[snowflake.ID]bool

func (s staticAuthorizer) Allowed(id snowflake.ID) bool { return s[id] }
func (s staticAuthorizer) Record(snowflake.ID, string)  {}

// managerAdapter wraps an access.Manager-shaped int64 authorizer so it can
// back this package's snowflake.ID-keyed Authorizer. Discord snowflakes are
// uint64 but real values are time-based and well under math.MaxInt64 (see
// DESIGN.md §3 option 1), so the conversion is lossless in practice.
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
// snowflake.ID-keyed Authorizer, per DESIGN.md §3 option 1. Use with
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
// mention policy (DESIGN.md §3) unit-testable without a real gateway
// connection or bot token.
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
	baseURL    string        // "" ⇒ disgo's default Discord API base
	httpClient *http.Client  // injectable for tests
	client     *bot.Client   // real gateway connection; nil when using a fake transport in tests

	recv   chan relay.Message
	cancel context.CancelFunc

	// Same retry-queue rationale as Telegram's Frontend: Send() returns
	// immediately and callers generally discard the error, so a transient
	// failure needs a background path that keeps retrying instead of
	// silently vanishing. See telegram.go's retryQueue doc comment.
	retryQueue     chan retryItem
	sendFailures   atomic.Int64
	permanentDrops atomic.Int64
	queueDepth     atomic.Int64

	// gatewayReconnects / lastGatewayEventAt are Discord's analogues of
	// Telegram's getUpdatesFailures/lastPollSuccess (DESIGN.md §6): the
	// signal that detects a Discord-side outage independent of whether any
	// outbound Send happened to occur during it. gatewayReconnects counts
	// only non-resumable reconnects (fresh IDENTIFY, not RESUME) since those
	// are the ones that risk a message gap and burn IDENTIFY budget.
	gatewayReconnects  atomic.Int64
	lastGatewayEventAt atomic.Int64 // unix seconds
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

// WithLogger sets a logger (default: discard).
func WithLogger(l *log.Logger) Option { return func(f *Frontend) { f.logger = l } }

// WithAllowGuildMessages enables guild (server) messages in addition to DMs.
// Default is DM-only, which requests zero privileged gateway intents (see
// DESIGN.md §2). Guild messages additionally require the guild to be listed
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

	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	go f.startRetryWorker(ctx)
	return f, nil
}

// Connect builds the real disgo gateway client and opens the websocket
// connection, wiring OnMessageCreate into f.recv via gate(). Split out from
// New() so tests can construct a Frontend (for gate()/Send() unit tests)
// without ever dialing Discord — mirrors the DESIGN.md §8 checklist item
// requiring this endpoint be unit-testable without a real bot token.
func (f *Frontend) Connect(ctx context.Context) error {
	intents := []gateway.Intents{gateway.IntentGuilds, gateway.IntentDirectMessages}
	if f.allowGuildMessages {
		intents = append(intents, gateway.IntentGuildMessages, gateway.IntentMessageContent)
	}

	client, err := disgo.New(f.token,
		bot.WithDefaultGateway(),
		bot.WithGatewayConfigOpts(gateway.WithIntents(intents...)),
		bot.WithEventListenerFunc(f.onMessageCreate),
	)
	if err != nil {
		return fmt.Errorf("discord: build client: %w", err)
	}
	f.client = client
	f.selfID = client.ID()

	if err := client.OpenGateway(ctx); err != nil {
		return fmt.Errorf("discord: open gateway: %w", err)
	}
	go func() {
		<-ctx.Done()
		close(f.recv)
	}()
	return nil
}

func (f *Frontend) Name() string               { return "discord" }
func (f *Frontend) Recv() <-chan relay.Message { return f.recv }

// Close stops the frontend: cancels the retry worker and, if connected,
// closes the gateway with a clean close frame.
func (f *Frontend) Close() error {
	f.cancel()
	if f.client != nil {
		f.client.Close(context.Background())
	}
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
		f.logger.Printf("discord: recv channel full/blocked, dropped message from channel %s", in.channelID)
	}
}

// gate applies the sender/guild/mention policy from DESIGN.md §3 and, if the
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
		// outright refusal of non-private chats (DESIGN.md §3). This runs
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

	// chat_id/from_id identity-pair invariant: DESIGN.md §3 Option B. In DMs,
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
		msg.Meta["guild_id"] = m.guildID.String()
	}
	return msg, true
}

// Send delivers a message to the channel named by m.Meta["channel_id"]
// (falling back to m.ConversationID) via the REST CreateMessage endpoint. A
// permanent failure (oversized message, missing channel_id, non-429 4xx) is
// returned as-is and NOT queued for retry — see senderr.Permanent. Anything
// else is assumed transient and gets queued for background retry, mirroring
// telegram.Frontend.Send exactly.
func (f *Frontend) Send(ctx context.Context, m relay.Message) error {
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

// sendOnce is the actual single REST attempt — both Send() and the retry
// worker call this, so there's exactly one code path that talks to Discord.
// Per DESIGN.md §4, rate-limit (429) handling is intentionally NOT
// duplicated here: disgo's REST client already parses Discord's
// X-RateLimit-* headers and per-route buckets, which is the whole reason
// disgo was chosen over discordgo. This only classifies the resulting error
// as permanent vs. retryable for the outer retry-queue layer.
func (f *Frontend) sendOnce(ctx context.Context, m relay.Message) error {
	channelIDStr := m.Meta["channel_id"]
	if channelIDStr == "" {
		channelIDStr = m.ConversationID
	}
	if channelIDStr == "" {
		return permanentSendError{Err: fmt.Errorf("discord send: no channel_id")}
	}
	channelID, err := snowflake.Parse(channelIDStr)
	if err != nil {
		return permanentSendError{Err: fmt.Errorf("discord send: invalid channel_id %q: %w", channelIDStr, err)}
	}
	if n := len(m.Text); n > maxMessageLen {
		return permanentSendError{Err: fmt.Errorf(
			"message too long (%d chars, Discord's limit is %d) - split it into multiple replies", n, maxMessageLen)}
	}

	create := discord.MessageCreate{Content: m.Text}
	_, err = f.channels.CreateMessage(channelID, create, rest.WithCtx(ctx))
	if err == nil {
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
		// retries on a guaranteed-repeat failure — see maxMessageLen's
		// sibling incident in telegram.go.
		if status/100 == 4 && status != http.StatusTooManyRequests {
			return permanentSendError{Err: fmt.Errorf("discord CreateMessage status %d: %s", status, restErr.Message)}
		}
	}
	return err
}

// enqueueRetry adds a failed message to the background retry queue.
// Non-blocking: if the queue is full, the oldest-in-flight item is dropped
// rather than blocking the caller — identical policy to telegram.go's
// enqueueRetry, see its doc comment for the reasoning.
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
		default:
		}
		select {
		case f.retryQueue <- item:
			f.queueDepth.Add(1)
		default:
		}
	}
}

// retryBackoff is exponential with a cap — identical policy to telegram.go's
// retryBackoff.
func retryBackoff(attempts int) time.Duration {
	d := time.Duration(1<<uint(attempts)) * time.Second
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d
}

// startRetryWorker runs until ctx is cancelled, periodically attempting to
// redeliver queued messages. Call once per Frontend. Structurally identical
// to telegram.go's startRetryWorker.
func (f *Frontend) startRetryWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var pending []retryItem

	for {
		select {
		case <-ctx.Done():
			return
		case item := <-f.retryQueue:
			f.queueDepth.Add(-1)
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
					f.logger.Printf("discord retry succeeded after %d attempt(s) for channel %s",
						item.attempts+1, item.msg.Meta["channel_id"])
					continue
				}
				item.attempts++
				if item.attempts >= maxRetryAttempts {
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

// SendFailures, PermanentDrops, and QueueDepth expose retry-path counters for
// the Prometheus /metrics endpoint — same semantics as telegram.go.
func (f *Frontend) SendFailures() int64   { return f.sendFailures.Load() }
func (f *Frontend) PermanentDrops() int64 { return f.permanentDrops.Load() }
func (f *Frontend) QueueDepth() int64     { return f.queueDepth.Load() }

// GatewayReconnects and LastGatewayEventAt expose the gateway connection
// health signal — DESIGN.md §6's analogue of Telegram's
// GetUpdatesFailures/LastPollSuccess.
func (f *Frontend) GatewayReconnects() int64  { return f.gatewayReconnects.Load() }
func (f *Frontend) LastGatewayEventAt() int64 { return f.lastGatewayEventAt.Load() }
