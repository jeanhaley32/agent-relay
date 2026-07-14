// Package matrix implements a relay frontend Endpoint backed by the Matrix
// Client-Server API, via maunium.net/go/mautrix. Unlike Telegram/Discord,
// Matrix is a federated, homeserver-agnostic protocol (DESIGN.md §1): this
// package never hardcodes a homeserver URL and never reaches for a
// server-specific API extension, so a self-hosted Conduit/Synapse/Dendrite
// instance or a public server (e.g. matrix.org) all work identically.
//
// The MVP ships plaintext-room-only (DESIGN.md §2): end-to-end encryption
// (mautrix-go's Olm/Megolm crypto subsystem) is deliberately staged as a
// separate follow-up, not shipped here. An encrypted room's message content
// this bot cannot decrypt is logged and dropped, never silently treated as
// empty text.
//
// Inbound messages are gated by a sender allowlist keyed on the sender's
// Matrix user ID (never the room ID) — an ungated room is a
// prompt-injection vector, same reasoning as the Telegram/Discord
// frontends. Matrix has no distinct "DM channel" concept: a 1:1 DM is just
// a two-member room, so (DESIGN.md §3) chat_id is set to the sender's user
// ID for a DM (mirroring Discord's Option B), with the real room id always
// carried separately in Meta["room_id"].
//
// gate() operates on the platform-neutral inboundMessage struct rather than
// mautrix-go's raw event/client types, so it is unit testable without a
// real sync loop or homeserver — see matrix_test.go. The outbound API
// surface used by Send() is captured by the small MautrixAPI interface, so
// Send() is also testable against a fake implementation or an httptest
// server, per DESIGN.md §6.
package matrix

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/jeanhaley32/agent-relay/internal/endpoint/senderr"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// maxMessageLen is a conservative cap chosen well under Matrix's practical
// ~64KiB per-event JSON ceiling — see DESIGN.md §4 for the full reasoning
// (the spec itself defines no fixed per-message character limit, unlike
// Telegram's 4096 or Discord's 2000). senderr.Split counts runes, not
// bytes: a rune can take up to 4 bytes of UTF-8 (more once JSON-escaped,
// e.g. a control character becomes a 6-byte "\u00XX" sequence), so the cap
// must leave enough headroom that even an adversarial/binary-heavy body
// can't cross the 64KiB ceiling. 8000 runes tops out at 32000 bytes of raw
// UTF-8 (48000 worst-case JSON-escaped) — comfortably under the ceiling
// with room for the rest of the event envelope, unlike the previous 16000
// which could reach 64000-96000 bytes on its own.
const maxMessageLen = 8000

// permanentSendError is the shared retry-classification type — see
// internal/endpoint/senderr.
type permanentSendError = senderr.Permanent

// Authorizer decides whether a sender may use the relay and records requests
// from those who may not, mirroring telegram.Authorizer/discord.Authorizer's
// two methods exactly, string-keyed since a Matrix user id
// ("@user:example.org") is not numeric — see DESIGN.md §3.
type Authorizer interface {
	Allowed(id string) bool
	Record(id string, name string)
}

// staticAuthorizer is a fixed allowlist (WithAllowlist). It records nothing.
// Fail-closed default (empty map denies everyone), same stance as
// telegram/discord's own staticAuthorizer.
type staticAuthorizer map[string]bool

func (s staticAuthorizer) Allowed(id string) bool { return s[id] }
func (s staticAuthorizer) Record(string, string)  {}

// inboundMessage is the platform-neutral shape gate() operates on, derived
// from a mautrix-go timeline event by the sync-loop listener. Keeping
// gate() decoupled from mautrix-go's event/client types is what makes the
// sender/room policy (DESIGN.md §3) unit-testable without a real sync loop
// or homeserver.
type inboundMessage struct {
	eventID    string
	roomID     string
	senderID   string
	senderName string
	senderIsMe bool // the bot's own echo (Matrix has no separate "isBot" flag)
	content    string
	// memberCount, when > 0, is the room's currently-known joined member
	// count, used to decide DM (2 members) vs multi-member room semantics
	// (DESIGN.md §3). A caller that doesn't know the count yet should leave
	// this 0, which gate() treats as "assume DM" (the MVP's only supported
	// shape per DESIGN.md §8) rather than blocking on a live lookup.
	memberCount int
}

// Frontend is a Matrix Client-Server API frontend Endpoint.
type Frontend struct {
	homeserverURL string
	auth          Authorizer
	logger        *log.Logger

	// Auth: exactly one of these two shapes is set (validated by config.go).
	accessToken string
	deviceID    string
	userID      string
	password    string

	selfID string // this bot's own Matrix user id, filled after Connect's login/handshake

	api        MautrixAPI      // outbound API surface — injectable for tests
	httpClient *http.Client    // injectable for tests
	client     *mautrix.Client // real client; nil when a fake MautrixAPI is injected

	recv     chan relay.Message
	recvOnce sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
	// syncDone is closed by runSyncLoop's goroutine when it has returned
	// (i.e. after f.ctx is cancelled and no further onMessageEvent/
	// onMemberEvent callback can start) — Close() waits on it before closing
	// f.recv, so a callback can never send on an already-closed channel. Left
	// nil when Connect never wired a real sync loop (WithMatrixClient tests).
	syncDone chan struct{}

	// convRooms remembers, for every ConversationID gate() has seen inbound,
	// which physical room id it maps to — mirrors discord.go's convChannels,
	// so a relayd-originated reply (scheduled reminder, admin notice, mcp
	// reply) that only carries ConversationID/chat_id can still be routed.
	convRooms sync.Map // ConversationID (string) -> room id (string)

	// dmConvs marks which convRooms entries are DMs (chatID == sender user
	// id) rather than group rooms, so KnownConversation can re-check auth
	// only for the DM case and allow group-room chat_ids (room ids) purely
	// by virtue of having been seen inbound — mirrors discord.go's dmConvs.
	// A group room id is never present in the user-id allowlist, so without
	// this split KnownConversation would unconditionally reject every
	// outbound reply into a multi-member room. Never set for group rooms.
	dmConvs sync.Map // ConversationID (string) -> struct{}

	// roomMemberCount caches the joined-member count for a room id, looked up
	// via JoinedMembers the first time a message arrives from a room this
	// process hasn't seen yet — used by onMessageEvent to populate
	// inboundMessage.memberCount so gate() can tell a DM apart from a
	// multi-member room (DESIGN.md §3). Caching avoids a JoinedMembers round
	// trip on every single message.
	roomMemberCount sync.Map // room id (string) -> member count (int)

	// encryptedNoticeSent tracks which rooms have already received the
	// one-time "E2E is not enabled" notice (DESIGN.md §2), so a chatty
	// encrypted room doesn't get spammed with it on every message.
	encryptedNoticeSent sync.Map // room id (string) -> struct{}{}

	retryQueue     chan retryItem
	sendFailures   atomic.Int64
	permanentDrops atomic.Int64
	queueDepth     atomic.Int64
	recvDrops      atomic.Int64

	// lastSyncEventAt is Matrix's analogue of Telegram's lastPollSuccess /
	// Discord's lastGatewayEventAt (DESIGN.md §6): the sync loop is a plain
	// HTTP long-poll (GET /sync), structurally closer to Telegram's
	// getUpdates than to Discord's websocket, so there is no heartbeat/
	// resume state machine here — the since-token continuation IS the
	// resume mechanism.
	lastSyncEventAt atomic.Int64
	syncFailures    atomic.Int64
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

// MautrixAPI captures the subset of *mautrix.Client this package calls,
// per DESIGN.md §6 — the direct test seam that lets matrix_test.go exercise
// gate()/Send()'s classification logic against a hand-written fake with no
// real or fake HTTP transport at all.
type MautrixAPI interface {
	SetCredentials(userID id.UserID, accessToken string)
	Login(ctx context.Context, req *mautrix.ReqLogin) (*mautrix.RespLogin, error)
	SyncWithContext(ctx context.Context) error
	SendMessageEvent(ctx context.Context, roomID id.RoomID, eventType event.Type, content interface{}, extra ...mautrix.ReqSendEvent) (*mautrix.RespSendEvent, error)
	JoinedMembers(ctx context.Context, roomID id.RoomID) (*mautrix.RespJoinedMembers, error)
	JoinRoomByID(ctx context.Context, roomID id.RoomID) (*mautrix.RespJoinRoom, error)
	Whoami(ctx context.Context) (*mautrix.RespWhoami, error)
}

// Option configures a Frontend.
type Option func(*Frontend)

// WithHTTPClient injects an HTTP client for the mautrix client's underlying
// transport (for tests / custom transport) — mirrors
// telegram.WithHTTPClient/discord.WithHTTPClient.
func WithHTTPClient(c *http.Client) Option { return func(f *Frontend) { f.httpClient = c } }

// WithMatrixClient injects the MautrixAPI implementation directly, bypassing
// New()'s normal mautrix.NewClient construction entirely — the most direct
// test seam (DESIGN.md §6), mirroring discord.WithChannels.
func WithMatrixClient(c MautrixAPI) Option { return func(f *Frontend) { f.api = c } }

// WithAccessToken configures token-based auth (DESIGN.md §1): a pre-minted
// access token, optionally with the device id it was minted for.
func WithAccessToken(token, deviceID string) Option {
	return func(f *Frontend) { f.accessToken = token; f.deviceID = deviceID }
}

// WithPasswordLogin configures username+password auth (DESIGN.md §1):
// Connect performs the standard POST /_matrix/client/v3/login flow itself.
func WithPasswordLogin(userID, password string) Option {
	return func(f *Frontend) { f.userID = userID; f.password = password }
}

// WithAllowlist sets a fixed set of permitted sender Matrix user ids.
// REQUIRED for real use if no Authorizer is supplied: an empty allowlist
// denies everyone (fail closed).
func WithAllowlist(ids ...string) Option {
	return func(f *Frontend) {
		s := staticAuthorizer{}
		for _, id := range ids {
			s[id] = true
		}
		f.auth = s
	}
}

// WithAuthorizer supplies a custom authorization backend that can grant
// access dynamically and record pending requests.
func WithAuthorizer(a Authorizer) Option { return func(f *Frontend) { f.auth = a } }

// WithLogger sets a logger (default: discard).
func WithLogger(l *log.Logger) Option { return func(f *Frontend) { f.logger = l } }

// WithSelfID sets the bot's own Matrix user id directly, primarily for
// tests that don't go through a real login/Connect handshake.
func WithSelfID(id string) Option { return func(f *Frontend) { f.selfID = id } }

// New builds a Matrix frontend but does not log in or start syncing — split
// from Connect per DESIGN.md §6, mirroring discord.New/Connect exactly, so
// tests can construct a Frontend (for gate()/Send() unit tests) without
// ever dialing a homeserver. homeserverURL is REQUIRED (DESIGN.md §1):
// Matrix is homeserver-agnostic, so unlike Telegram/Discord this is a
// required production config value, not just a test override.
func New(homeserverURL string, opts ...Option) (*Frontend, error) {
	if homeserverURL == "" {
		return nil, fmt.Errorf("matrix: homeserverURL is required — see DESIGN.md §1")
	}
	f := &Frontend{
		homeserverURL: homeserverURL,
		auth:          staticAuthorizer{}, // fail-closed default (denies everyone)
		logger:        log.New(io.Discard, "", 0),
		recv:          make(chan relay.Message, 32),
		retryQueue:    make(chan retryItem, retryQueueCapacity),
	}
	for _, o := range opts {
		o(f)
	}

	if f.api == nil {
		client, err := mautrix.NewClient(homeserverURL, "", "")
		if err != nil {
			return nil, fmt.Errorf("matrix: build client: %w", err)
		}
		if f.httpClient != nil {
			client.Client = f.httpClient
		}
		f.client = client
		f.api = client
	}

	ctx, cancel := context.WithCancel(context.Background())
	f.ctx = ctx
	f.cancel = cancel
	go f.startRetryWorker(ctx)
	return f, nil
}

// Connect performs the actual network-touching handshake (DESIGN.md §6):
// either sets the pre-minted access token directly (token-auth path) or
// calls the standard login endpoint to obtain one (password path), then
// starts mautrix-go's sync loop. Matrix's sync is itself a long-poll
// (GET /sync with a since token), so — unlike Discord's gateway — there is
// no heartbeat/resume state machine to run separately; the sync loop's own
// retry-on-failure IS the resume mechanism.
//
// Wiring event handlers into the real client's Syncer only happens when
// f.client is non-nil (i.e. New() built a real mautrix.Client) — a Frontend
// constructed with WithMatrixClient for tests has no Syncer to wire, and
// tests exercise gate() directly instead (DESIGN.md §6).
func (f *Frontend) Connect(ctx context.Context) error {
	switch {
	case f.accessToken != "":
		f.api.SetCredentials(id.UserID(f.userID), f.accessToken)
		if f.client != nil {
			f.client.DeviceID = id.DeviceID(f.deviceID)
		}
		if f.selfID == "" {
			f.selfID = f.userID
		}
		if f.selfID == "" {
			// Token-auth with no configured user_id: without knowing our own
			// user id, senderIsMe never matches (self-echo relay-loop risk)
			// and onMemberEvent's admin-invite auto-join never fires (its
			// StateKey match requires a non-empty f.selfID) — DESIGN.md §7.
			// Ask the homeserver who we are rather than silently limping
			// along with echo suppression permanently disabled.
			resp, err := f.api.Whoami(ctx)
			if err != nil {
				return fmt.Errorf("matrix: token auth but no user_id configured and GET /whoami failed: %w — set matrix.user_id alongside access_token_env", err)
			}
			f.selfID = string(resp.UserID)
			// Re-apply credentials now that selfID is known: the earlier
			// SetCredentials call above used f.userID (possibly empty on
			// the token-only auth path), so the underlying mautrix.Client's
			// UserID field would otherwise stay "" for the rest of the
			// process lifetime. It works today (nothing keys off UserID
			// yet), but the SyncStore's next-batch key and any future
			// crypto/Store implementation would key off it, so keep it
			// correct rather than leave a latent footgun.
			f.api.SetCredentials(id.UserID(f.selfID), f.accessToken)
		}
	case f.userID != "" && f.password != "":
		resp, err := f.api.Login(ctx, &mautrix.ReqLogin{
			Type: mautrix.AuthTypePassword,
			Identifier: mautrix.UserIdentifier{
				Type: mautrix.IdentifierTypeUser,
				User: f.userID,
			},
			Password:         f.password,
			StoreCredentials: true,
		})
		if err != nil {
			return fmt.Errorf("matrix: login: %w", err)
		}
		if f.selfID == "" {
			f.selfID = string(resp.UserID)
		}
		f.deviceID = string(resp.DeviceID)
	default:
		return fmt.Errorf("matrix: neither access token nor user_id+password configured")
	}

	if f.client != nil {
		if syncer, ok := f.client.Syncer.(mautrix.ExtensibleSyncer); ok {
			syncer.OnEventType(event.EventMessage, f.onMessageEvent)
			syncer.OnEventType(event.StateMember, f.onMemberEvent)
			syncer.OnEventType(event.EventEncrypted, f.onEncryptedEvent)
			// DESIGN.md §6 gap: mautrix.NewClient uses an in-memory
			// SyncStore, so LoadNextBatch always returns "" on the very
			// first /sync after every relayd restart, and a since=""
			// request returns each joined room's recent timeline backlog.
			// Without this guard, every restart re-publishes recent room
			// history to the Broker as if it were freshly received,
			// causing the agent to re-process (and potentially re-answer)
			// messages it already handled. DontProcessOldEvents detects
			// since=="" and marks every event in that first response
			// suppressed before Dispatch fires the OnEventType callbacks.
			//
			// Trade-off (DESIGN.md §7): this also suppresses any invite-
			// state events already pending in that first response, so an
			// admin invite sent while relayd was down is never auto-joined
			// — onMemberEvent simply never fires for it, and the admin
			// must re-invite after the bot is back up. Logged here (not
			// silently) so this shows up in the journal instead of being a
			// mystery "why didn't it join" report.
			f.logger.Printf("matrix: connecting — note: room invites sent while offline are suppressed by the first replay-guarded /sync and must be resent (DESIGN.md §7)")
			syncer.OnSync(f.client.DontProcessOldEvents)
		}
		f.syncDone = make(chan struct{})
		// Run on f.ctx (cancelled by Close), NOT the ctx passed to Connect —
		// main.go passes context.Background() here, so if the sync loop ran
		// on that ctx instead, Close() calling f.cancel() would never stop
		// it: the loop (and the callbacks it drives, including
		// onMessageEvent's `f.recv <- msg`) would keep running forever,
		// including after Close() closes f.recv, panicking on a
		// send-on-closed-channel. Close() waits on syncDone before closing
		// f.recv so no callback can be mid-flight when the channel closes.
		go func() {
			f.runSyncLoop(f.ctx)
			close(f.syncDone)
		}()
	}
	return nil
}

// runSyncLoop calls SyncWithContext repeatedly until ctx is cancelled — the
// long-poll retry loop that is Matrix's structural analogue of Telegram's
// getUpdates poll loop, per DESIGN.md §6. A failed sync is logged and
// counted (syncFailures, the /metrics analogue of Telegram's
// getUpdatesFailures / Discord's gatewayReconnects) and retried after a
// short backoff rather than crashing the frontend.
func (f *Frontend) runSyncLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		f.lastSyncEventAt.Store(time.Now().Unix())
		if err := f.api.SyncWithContext(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			f.syncFailures.Add(1)
			f.logger.Printf("matrix: sync failed, retrying: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (f *Frontend) Name() string               { return "matrix" }
func (f *Frontend) Recv() <-chan relay.Message { return f.recv }

// Close stops the frontend: cancels f.ctx (stopping the retry worker AND,
// since Connect wires the sync loop onto f.ctx, the sync loop too), waits
// for the sync loop goroutine to actually return via syncDone — so
// onMessageEvent/onMemberEvent are guaranteed to have no callback still in
// flight — and only then closes f.recv (guarded against double-close).
// Mirrors discord.go's Close ordering rationale (stop the event source
// first, close recv second) — see its doc comment.
func (f *Frontend) Close() error {
	f.cancel()
	if f.syncDone != nil {
		<-f.syncDone
	}
	f.recvOnce.Do(func() { close(f.recv) })
	return nil
}

// onMessageEvent is mautrix-go's OnEventType(event.EventMessage) callback in
// real use: it extracts the platform-neutral inboundMessage and hands it to
// gate(), then publishes anything gate() allows onto f.recv.
func (f *Frontend) onMessageEvent(ctx context.Context, evt *event.Event) {
	f.lastSyncEventAt.Store(time.Now().Unix())

	msgContent := evt.Content.AsMessage()
	in := inboundMessage{
		eventID:    string(evt.ID),
		roomID:     string(evt.RoomID),
		senderID:   string(evt.Sender),
		senderName: string(evt.Sender),
		senderIsMe: f.selfID != "" && string(evt.Sender) == f.selfID,
		content:    msgContent.Body,
	}
	if in.content == "" && msgContent.MsgType == "" {
		// A message-typed event mautrix-go could not parse into
		// MessageEventContent at all (e.g. undecryptable content in an
		// encrypted room this MVP can't handle — DESIGN.md §2) must never
		// be silently treated as an ordinary empty message and fall
		// through gate()'s `content == ""` drop as if nothing was said.
		f.logger.Printf("matrix: dropped unparseable/undecryptable event id=%s room=%s sender=%s — E2E is not enabled (DESIGN.md §2)",
			in.eventID, in.roomID, in.senderID)
		return
	}

	// Short-circuit the two cheap gate() checks that need no network lookup
	// (the bot's own echo, and ordinary empty-content events) before paying
	// for the JoinedMembers round trip below: gate() would discard both
	// anyway, so doing the member-count lookup first only adds a homeserver
	// round trip per self-echo in uncached rooms plus a misleading "refusing
	// to guess" drop log for content gate() was always going to drop.
	if in.senderIsMe || in.content == "" {
		return
	}

	// Member count distinguishes a DM (<=2 members) from a multi-member room
	// (DESIGN.md §3). This MUST be a real lookup, not left at the
	// zero-value "assume DM": onMemberEvent auto-joins any room an
	// allowlisted admin invites the bot to, including multi-member rooms: if
	// memberCount stayed 0 here, gate() would treat every group-room message
	// as a DM (convID = sender id) and convRooms.Store would overwrite that
	// sender's real DM room mapping with the group room, so a later
	// relayd-originated reply addressed only by chat_id (a scheduled
	// reminder, an mcp reply) could be delivered into the group room instead
	// of the private DM.
	count, known := f.roomMemberCountFor(ctx, in.roomID)
	if !known {
		// JoinedMembers failed and this room has no cached count yet (a
		// transient homeserver hiccup on the very first message from this
		// room). Defaulting to DM semantics here is exactly the hijack
		// round-1 fix #4 closed for the zero-value case — a group-room
		// message would overwrite the sender's real DM room mapping in
		// convRooms. Hold the message instead: drop it and let the sender
		// retry, rather than guess.
		f.logger.Printf("matrix: dropped message from room %s: member count unknown (JoinedMembers lookup failed and nothing cached), refusing to guess DM-vs-group", in.roomID)
		return
	}
	in.memberCount = count

	msg, ok := f.gate(in)
	if !ok {
		return
	}
	select {
	case f.recv <- msg:
	case <-time.After(5 * time.Second):
		f.recvDrops.Add(1)
		f.logger.Printf("matrix: recv channel full/blocked, dropped message from room %s", in.roomID)
	}
}

// onEncryptedEvent handles m.room.encrypted events (DESIGN.md §2's MVP
// scope: plaintext-room-only, E2E deliberately not implemented). Encrypted
// events are a distinct event type from m.room.message — they never reach
// onMessageEvent/OnEventType(event.EventMessage) at all — so without this
// handler they were silently ignored with no log line, contradicting
// DESIGN.md §2's explicit "log and drop, never silent" requirement and its
// promised one-time "E2E not enabled" notice back to the sender.
func (f *Frontend) onEncryptedEvent(ctx context.Context, evt *event.Event) {
	f.lastSyncEventAt.Store(time.Now().Unix())
	f.logger.Printf("matrix: dropped encrypted event id=%s room=%s sender=%s — E2E is not enabled (DESIGN.md §2)",
		evt.ID, evt.RoomID, evt.Sender)

	// Gate the notice send on the same Authorizer every other outbound path
	// in this frontend uses: without this, any stranger in any encrypted
	// room the bot is in (or gets invited/dragged into) can make the bot
	// emit a message on demand, simply by sending one encrypted event.
	if !f.auth.Allowed(string(evt.Sender)) {
		return
	}

	if _, alreadySent := f.encryptedNoticeSent.Load(string(evt.RoomID)); alreadySent {
		return
	}
	const notice = "This bot does not support end-to-end encrypted rooms yet. Please use an unencrypted room."
	if _, err := f.api.SendMessageEvent(ctx, evt.RoomID, event.EventMessage,
		event.MessageEventContent{MsgType: event.MsgText, Body: notice}); err != nil {
		f.logger.Printf("matrix: failed to send E2E-not-enabled notice to room %s: %v", evt.RoomID, err)
		return
	}
	// Only mark the notice as sent once it actually went out — a failed
	// send (transient network/homeserver error) must not permanently
	// suppress the one-time notice for this room.
	f.encryptedNoticeSent.Store(string(evt.RoomID), struct{}{})
}

// onMemberEvent handles room-invite events (DESIGN.md §7's room-join
// policy): the bot auto-accepts an invite only from an already-allowlisted
// admin/user id (the SAME Authorizer used for message sending), and leaves
// everything else logged and pending for manual admin action — an
// unsolicited invite is itself an unauthenticated-sender surface, the
// Matrix analogue of Discord's guild-invite surface.
func (f *Frontend) onMemberEvent(ctx context.Context, evt *event.Event) {
	// Any membership change in a room we've cached a member count for
	// invalidates that cache — a join/leave by ANY member (not just the
	// bot) changes the count, and a stale low count is exactly what lets
	// gate() misclassify a now-multi-member room as a DM (see
	// roomMemberCountFor's doc comment and the hijack this closes: a
	// 2-member room that later grows to 3+ must stop being routed to
	// convRooms as if it were still the sender's private DM room).
	f.roomMemberCount.Delete(string(evt.RoomID))

	// A DM room that grows a third member must also stop being reachable
	// via the stale convRooms/dmConvs entries recorded while it was still
	// 2-member: otherwise a relayd-originated reply addressed only by
	// chat_id (a scheduled reminder, an mcp reply) resolves via
	// convRooms[sender]->room and lands in the now-multi-member room even
	// though gate() will correctly reclassify the NEXT inbound message from
	// this room as a group. Purge any convRooms/dmConvs entry whose stored
	// room id is this room so resolveRoom can no longer find it that way.
	roomID := string(evt.RoomID)
	f.convRooms.Range(func(k, v any) bool {
		if v.(string) == roomID {
			f.convRooms.Delete(k)
			f.dmConvs.Delete(k)
		}
		return true
	})

	if evt.StateKey == nil || f.selfID == "" || *evt.StateKey != f.selfID {
		return // not an event about this bot's own membership
	}
	member := evt.Content.AsMember()
	if member.Membership != event.MembershipInvite {
		return
	}
	sender := string(evt.Sender)
	if !f.auth.Allowed(sender) {
		f.logger.Printf("matrix: declined to auto-join room %s: inviter %s is not allowlisted", evt.RoomID, sender)
		return
	}
	if _, err := f.api.JoinRoomByID(ctx, evt.RoomID); err != nil {
		f.logger.Printf("matrix: failed to join room %s (invited by allowlisted %s): %v", evt.RoomID, sender, err)
		return
	}
	f.logger.Printf("matrix: auto-joined room %s (invited by allowlisted admin/user %s)", evt.RoomID, sender)
}

// roomMemberCountFor returns the cached joined-member count for roomID,
// looking it up via JoinedMembers on a cache miss, plus whether the count is
// actually known. On lookup failure with nothing cached it logs and returns
// (0, false) rather than caching an error — a subsequent message from the
// same room will simply retry the lookup — and never guesses "2" (DM) or
// otherwise, since an under-count here is what causes the DM/group routing
// hijack described at the roomMemberCountFor call site in onMessageEvent;
// the caller must treat known==false as "hold, don't guess."
func (f *Frontend) roomMemberCountFor(ctx context.Context, roomID string) (int, bool) {
	if v, ok := f.roomMemberCount.Load(roomID); ok {
		return v.(int), true
	}
	resp, err := f.api.JoinedMembers(ctx, id.RoomID(roomID))
	if err != nil {
		f.logger.Printf("matrix: JoinedMembers lookup failed for room %s, treating as unknown: %v", roomID, err)
		return 0, false
	}
	count := len(resp.Joined)
	f.roomMemberCount.Store(roomID, count)
	return count, true
}

// gate applies the sender policy from DESIGN.md §3 and, if the message
// passes, returns the relay.Message to publish. Pure function of its input
// (plus the Frontend's static config/authorizer) so it's directly unit
// testable without any sync loop or homeserver.
func (f *Frontend) gate(m inboundMessage) (relay.Message, bool) {
	if m.senderIsMe {
		return relay.Message{}, false // never relay our own echoes — avoids bot-loops
	}
	if m.content == "" {
		return relay.Message{}, false
	}
	if !f.auth.Allowed(m.senderID) {
		f.auth.Record(m.senderID, m.senderName)
		f.logger.Printf("matrix: dropped message from unauthorized sender id=%s — recorded as pending", m.senderID)
		return relay.Message{}, false
	}

	// chat_id/from_id invariant, DESIGN.md §3: a Matrix room id and a
	// Matrix user id are never the same string (different sigils, `!` vs
	// `@`) even in a 1:1 DM room, unlike Telegram where chat_id IS from_id
	// by construction. For a DM (memberCount <= 2, including 0/unknown —
	// the MVP's only supported shape per §8), set chat_id to the sender's
	// user id (Discord's Option B, adapted), so the Broker's identity-pair
	// invariant still holds; for a multi-member room (memberCount > 2),
	// chat_id is the room id itself, distinguishable from the DM case
	// because chat_id == room_id never happens for a DM (a `!`-room-id
	// string can never equal an `@`-user-id string).
	var convID string
	isGroup := m.memberCount > 2
	if isGroup {
		convID = m.roomID
	} else {
		convID = m.senderID
	}

	f.convRooms.Store(convID, m.roomID)
	if !isGroup {
		f.dmConvs.Store(convID, struct{}{})
	}

	meta := map[string]string{
		"chat_id":   convID,
		"room_id":   m.roomID,
		"from_id":   m.senderID,
		"from_name": m.senderName,
	}
	if isGroup {
		// The Broker's identity-pair invariant (relay.go) drops any message
		// where from_id != chat_id unless Meta["guild_id"] is set — a
		// multi-member room's chat_id (the room id) is never equal to
		// from_id (the sender id) by construction (DESIGN.md §3), so this
		// exemption marker is required for the message to survive the
		// Broker, mirroring discord.go's guild_id exemption for guild
		// channels.
		meta["guild_id"] = m.roomID
	}

	msg := relay.Message{
		ConversationID: convID,
		Role:           relay.User,
		Text:           m.content,
		Meta:           meta,
	}
	return msg, true
}

// KnownConversation reports whether chatID is a conversation id this
// Frontend has already seen and gated inbound — mirrors discord.go's
// KnownConversation. Used by relayd's outbound allowlist to permit replies
// into rooms the model was legitimately talking in.
//
// For a DM, chatID IS the sender's user id (see gate()'s convID comment),
// so we re-check f.auth.Allowed(chatID) live rather than trusting the
// cached convRooms entry alone — a user removed from the allowlist after an
// earlier authorized DM should not stay reachable for outbound replies
// until process restart. A group room id is never a valid entry in the
// user-id-keyed auth manager, so unconditionally requiring
// f.auth.Allowed(chatID) — as this used to — made KnownConversation always
// false for group rooms, blocking every outbound reply into them even
// though the inbound message that prompted the reply was itself
// allowlisted. Group room ids are tracked separately: dmConvs is only ever
// set for the DM case (see gate()), so falling through to "known" for
// anything not in dmConvs correctly allows the group-room path while still
// gating DMs live.
func (f *Frontend) KnownConversation(chatID string) bool {
	_, ok := f.convRooms.Load(chatID)
	if !ok {
		return false
	}
	if _, isDM := f.dmConvs.Load(chatID); isDM {
		return f.auth.Allowed(chatID)
	}
	return true
}

// OwnsConversationID implements relay.Claimer: it reports whether id is
// plausibly a Matrix conversation id — a DM chat_id is a bare Matrix user
// id (`@localpart:homeserver.tld`), the only syntax this package can
// recognize without having seen it delivered inbound this process lifetime
// (a bare room id, `!opaque:homeserver.tld`, looks identical in shape to a
// user id syntactically except for the sigil, so it's included too).
func (f *Frontend) OwnsConversationID(id string) bool {
	if len(id) < 2 {
		return false
	}
	return id[0] == '@' || id[0] == '!'
}

// Send delivers a message to the room named by m.Meta["room_id"] (falling
// back to a room resolved from m.ConversationID via convRooms) via
// SendMessageEvent. A message over maxMessageLen is split via senderr.Split
// and sent as multiple messages in order, mirroring discord.go's Send.
func (f *Frontend) Send(ctx context.Context, m relay.Message) error {
	chunks := senderr.Split(m.Text, maxMessageLen)
	if len(chunks) > 1 {
		// A transient failure on one chunk must not abort the rest: that
		// chunk gets queued for background retry (sendChunk), but if we
		// returned here the remaining chunks would never be sent at all,
		// turning a delayed chunk into a permanently dropped one and
		// garbling the split message's order. Send every chunk regardless,
		// and report the first error (if any) once all have been attempted.
		var firstErr error
		for _, chunk := range chunks {
			cm := m
			cm.Text = chunk
			if err := f.sendChunk(ctx, cm); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
	return f.sendChunk(ctx, m)
}

func (f *Frontend) sendChunk(ctx context.Context, m relay.Message) error {
	err := f.sendOnce(ctx, m)
	if err == nil {
		return nil
	}
	f.sendFailures.Add(1)
	var perm permanentSendError
	if errors.As(err, &perm) {
		f.logger.Printf("matrix send permanently failed (not retrying): %v", err)
		return err
	}
	f.logger.Printf("matrix send failed, queuing for background retry: %v", err)
	f.enqueueRetry(m)
	return err
}

// resolveRoom figures out which physical room to send into for m, in
// priority order: Meta["room_id"], then convRooms (the room last seen for
// this ConversationID by gate()), then ConversationID itself if it's
// already shaped like a room id (`!...`) — mirrors discord.go's
// resolveChannel priority order.
func (f *Frontend) resolveRoom(m relay.Message) (string, error) {
	roomID := m.Meta["room_id"]
	if roomID == "" {
		if v, ok := f.convRooms.Load(m.ConversationID); ok {
			roomID = v.(string)
		}
	}
	if roomID == "" && len(m.ConversationID) > 0 && m.ConversationID[0] == '!' {
		roomID = m.ConversationID
	}
	if roomID == "" {
		return "", permanentSendError{Err: fmt.Errorf("matrix send: no room_id and no known room for conversation %q", m.ConversationID)}
	}
	return roomID, nil
}

// sendOnce is the actual single API attempt — both Send() and the retry
// worker call this. Per DESIGN.md §4/§7, rate-limit (429/M_LIMIT_EXCEEDED)
// handling is intentionally NOT duplicated here: mautrix-go's client
// already sleeps/retries on a 429 by default (Client.IgnoreRateLimit is
// false unless explicitly set), the same "don't duplicate what the library
// already gets right" stance discord/DESIGN.md took for disgo's rate
// limiter. This only classifies the resulting error as permanent vs.
// retryable for the outer retry-queue layer, should one slip through.
func (f *Frontend) sendOnce(ctx context.Context, m relay.Message) error {
	roomID, err := f.resolveRoom(m)
	if err != nil {
		return err
	}
	if n := utf8.RuneCountInString(m.Text); n > maxMessageLen {
		return permanentSendError{Err: fmt.Errorf(
			"message too long (%d chars, cap is %d) - split it into multiple replies", n, maxMessageLen)}
	}

	content := event.MessageEventContent{MsgType: event.MsgText, Body: m.Text}
	resp, err := f.api.SendMessageEvent(ctx, id.RoomID(roomID), event.EventMessage, content)
	if err == nil {
		f.logger.Printf("matrix send ok: room=%s event=%s", roomID, resp.EventID)
		f.convRooms.Store(m.ConversationID, roomID)
		return nil
	}

	var httpErr mautrix.HTTPError
	if errors.As(err, &httpErr) && httpErr.Response != nil {
		status := httpErr.Response.StatusCode
		// Every 4xx other than 429 (M_LIMIT_EXCEEDED) fails identically on
		// retry (malformed event, M_FORBIDDEN — not joined/not permitted,
		// M_TOO_LARGE, room deleted/left) — classify permanent so it
		// doesn't burn maxRetryAttempts retries, per DESIGN.md §4/§7.
		if status/100 == 4 && status != http.StatusTooManyRequests {
			return permanentSendError{Err: fmt.Errorf("matrix send status %d: %s", status, httpErr.Error())}
		}
	}
	return err
}

// enqueueRetry adds a failed message to the background retry queue.
// Non-blocking: if the queue is full, the oldest-in-flight item is dropped
// rather than blocking the caller — identical policy to
// telegram.go/discord.go's enqueueRetry.
func (f *Frontend) enqueueRetry(m relay.Message) {
	item := retryItem{msg: m, attempts: 0, nextAt: time.Now().Add(retryBackoff(0))}
	select {
	case f.retryQueue <- item:
		f.queueDepth.Add(1)
	default:
		f.logger.Printf("matrix retry queue full (%d), dropping oldest to make room", retryQueueCapacity)
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

// retryBackoff is exponential with a cap — identical policy to
// telegram.go/discord.go's retryBackoff.
func retryBackoff(attempts int) time.Duration {
	d := time.Duration(1<<uint(attempts)) * time.Second
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d
}

// startRetryWorker runs until ctx is cancelled, periodically attempting to
// redeliver queued messages. Call once per Frontend. Structurally identical
// to telegram.go/discord.go's startRetryWorker.
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
					f.logger.Printf("matrix retry succeeded after %d attempt(s) for room %s",
						item.attempts+1, item.msg.Meta["room_id"])
					continue
				}
				item.attempts++
				var perm permanentSendError
				if item.attempts >= maxRetryAttempts || errors.As(err, &perm) {
					f.permanentDrops.Add(1)
					f.logger.Printf("matrix retry gave up after %d attempts for room %s: %v",
						item.attempts, item.msg.Meta["room_id"], err)
					continue
				}
				item.nextAt = now.Add(retryBackoff(item.attempts))
				stillPending = append(stillPending, item)
			}
			pending = stillPending
		}
	}
}

// SendFailures, PermanentDrops, and QueueDepth expose retry-path counters
// for the Prometheus /metrics endpoint — same semantics as telegram.go/discord.go.
func (f *Frontend) SendFailures() int64   { return f.sendFailures.Load() }
func (f *Frontend) PermanentDrops() int64 { return f.permanentDrops.Load() }
func (f *Frontend) QueueDepth() int64     { return f.queueDepth.Load() }

// RecvDrops exposes the inbound-drop counter.
func (f *Frontend) RecvDrops() int64 { return f.recvDrops.Load() }

// SyncFailures and LastSyncEventAt expose the sync-loop health signal —
// DESIGN.md §6's analogue of Telegram's GetUpdatesFailures/LastPollSuccess.
func (f *Frontend) SyncFailures() int64    { return f.syncFailures.Load() }
func (f *Frontend) LastSyncEventAt() int64 { return f.lastSyncEventAt.Load() }
