// Package matrix is a relayd frontend Endpoint for a self-hosted, tailnet-only
// Matrix homeserver. It is Jean's private admin channel: because the homeserver
// is reachable ONLY over the Tailscale tunnel, network reachability is itself
// the authentication — a message that arrives came from one of Jean's own
// tailnet devices. That is why this path is exempt from relayd's approval
// (session) gate: the gate exists to re-prove identity on public gateways
// (Telegram/Discord), and there is nothing to re-prove here.
//
// The exemption is narrow and falls out of existing relayd wiring, not from any
// special-casing in the Broker: relayd only session-gates user ids listed in
// SessionGatedUsers, and that set is built solely from the Telegram/Discord
// admin ids. A Matrix message carries a Matrix user id (@admin:example.org) that is
// never in that set, so it skips the gate automatically. Everything else — the
// event log, the Kafka message-persistence pipeline, anomaly scoring, the
// budget/rate accounting — runs unchanged, because these messages enter through
// the same Broker as every other channel.
//
// The transport is the Matrix Client-Server API over plain net/http: a
// long-polling /sync loop for inbound events and a PUT /send for outbound. The
// admin room is unencrypted (tailnet + Tailscale-Serve TLS already protect it),
// so there is deliberately NO E2EE/olm dependency here — keeping the adapter
// lean and readable, in keeping with the house preference for the simplest
// mechanism that meets the threat model.
package matrix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/authz"
	"github.com/jeanhaley32/agent-relay/internal/eventlog"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// syncTimeout is how long a single /sync long-poll blocks server-side waiting
// for new events before returning empty. The homeserver holds the connection
// open for up to this long, so the loop is event-driven, not a busy poll.
const syncTimeout = 30 * time.Second

// httpTimeout bounds a single HTTP call. It must exceed syncTimeout so the sync
// long-poll isn't cut off client-side before the server's own timeout fires.
const httpTimeout = syncTimeout + 15*time.Second

// Frontend is a Matrix homeserver frontend implementing relay.Endpoint (and
// relay.Claimer). It logs in with a pre-provisioned access token, follows the
// /sync stream for messages from authorized admins, and delivers replies back
// into the originating room.
type Frontend struct {
	homeserver string // base URL, e.g. https://host.example.ts.net:8448
	token      string // bot access token
	admins     map[string]bool
	mediaSpool string // dir where inbound media files are saved (empty ⇒ media ignored)
	logger     *log.Logger
	http       *http.Client

	out    chan relay.Message
	closed chan struct{}
	once   sync.Once

	// roomByConv maps a conversation id (the sender's mxid, used DM-style —
	// see deliver) back to the physical room id so Send can route a reply
	// into the right room. Mirrors discord's convChannels map.
	roomByConv sync.Map // convID(string) -> roomID(string)

	// roomsFile persists roomByConv so a restart can still reply into a room
	// before its admin writes again. Empty means memory only.
	roomsFile string
	roomsMu   sync.Mutex

	// txn is a monotonic transaction-id counter for idempotent sends.
	txn atomic.Int64

	// observability counters, exposed for the /metrics endpoint.
	sendFailures   atomic.Int64
	recvDrops      atomic.Int64
	syncFailures   atomic.Int64
	lastSyncAtUnix atomic.Int64
}

// New constructs a Matrix frontend. homeserver is the base URL; token is the
// bot's access token; admins is the set of Matrix user ids (e.g.
// "@admin:example.org") whose messages are relayed — every other sender is
// ignored. mediaSpool is a directory where inbound media (images/audio/etc.)
// are downloaded so the backend can open them; empty disables media handling
// (media messages are then surfaced as a text note without a downloaded file).
func New(homeserver, token string, admins []string, mediaSpool string, logger *log.Logger) *Frontend {
	if logger == nil {
		logger = log.Default()
	}
	adm := make(map[string]bool, len(admins))
	for _, a := range admins {
		adm[a] = true
	}
	return &Frontend{
		homeserver: strings.TrimRight(homeserver, "/"),
		token:      token,
		admins:     adm,
		mediaSpool: mediaSpool,
		logger:     logger,
		http:       &http.Client{Timeout: httpTimeout},
		out:        make(chan relay.Message, 64),
		closed:     make(chan struct{}),
	}
}

func (f *Frontend) Name() string { return "matrix" }

// Assurance is Proved: the homeserver is reachable only over the tailnet, so
// a message arriving here already established who sent it. See the package
// comment. This is why Matrix admins are not liveness-gated.
func (f *Frontend) Assurance() authz.Assurance { return authz.Proved }
func (f *Frontend) Recv() <-chan relay.Message { return f.out }

// OwnsConversationID reports whether id belongs to this frontend: either a
// Matrix room id ('!'-prefixed) or a Matrix user id ('@'-prefixed, the DM-style
// conversation id we actually stamp — see deliver). Telegram (numeric) and
// Discord (numeric snowflake) ids never take these forms, so this is an
// unambiguous claim. It lets relay.MultiFrontend route a relayd-originated
// message (scheduler reminder, admin notice) and satisfies the outbound gate
// for replies into this channel.
func (f *Frontend) OwnsConversationID(id string) bool {
	return strings.HasPrefix(id, "!") || strings.HasPrefix(id, "@")
}

// KnownConversation reports whether an admin has actually spoken in this
// conversation. This is what the outbound gate must ask; OwnsConversationID is
// a routing heuristic on the shape of an id and accepts any room the bot was
// invited into, so using it as the gate left that path open.
//
// roomByConv is written only by deliver, after the admin filter, and by
// LoadRooms, which restores only current admins, so a hit is evidence. Rooms count as well as mxids because a relayd-originated message
// may address either.
//
// A configured admin's own mxid always qualifies, and that is not a weakening:
// it is an identity the operator set in config, not one inferred from traffic.
// Without it the gate starves itself, because a restart without a rooms file
// empties roomByConv — relayd would be unable to send to Matrix at all until
// an admin happened to speak first, silently dropping scheduled deliveries.
// Discord avoids the same trap by falling back to its live allowlist for DMs.
func (f *Frontend) KnownConversation(id string) bool {
	if id == "" {
		return false
	}
	if f.admins[id] {
		return true
	}
	if _, ok := f.roomByConv.Load(id); ok {
		return true
	}
	known := false
	f.roomByConv.Range(func(_, room any) bool {
		if room.(string) == id {
			known = true
			return false
		}
		return true
	})
	return known
}

func (f *Frontend) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

// Metrics accessors for the /metrics endpoint.
func (f *Frontend) SendFailures() int64 { return f.sendFailures.Load() }
func (f *Frontend) RecvDrops() int64    { return f.recvDrops.Load() }
func (f *Frontend) SyncFailures() int64 { return f.syncFailures.Load() }
func (f *Frontend) LastSyncAt() int64   { return f.lastSyncAtUnix.Load() }

// Connect verifies the token by calling /account/whoami and returns the bot's
// own user id. It is called once at startup so a bad token/homeserver fails
// loudly rather than silently producing an endpoint that never receives.
func (f *Frontend) Connect(ctx context.Context) (string, error) {
	var who struct {
		UserID string `json:"user_id"`
	}
	if err := f.get(ctx, "/_matrix/client/v3/account/whoami", &who); err != nil {
		return "", fmt.Errorf("matrix whoami: %w", err)
	}
	return who.UserID, nil
}

// Run drives the inbound /sync loop until Close or ctx cancellation. It should
// be started in its own goroutine after Connect succeeds. selfID is the bot's
// own user id (from Connect); the loop skips the bot's own messages so replies
// don't echo back as new inbound.
// primeSince retries the priming sync until it succeeds, returning false if
// the endpoint is shut down first. It never gives up and continues with an
// empty token: an unprimed sync replays history as live input.
func (f *Frontend) primeSince(ctx context.Context) (string, bool) {
	backoff := time.Second
	for {
		tok, err := f.initialSync(ctx)
		if err == nil {
			return tok, true
		}
		if ctx.Err() != nil {
			return "", false
		}
		f.syncFailures.Add(1)
		f.logger.Printf("matrix: priming sync failed, retry in %s (not syncing until it succeeds): %v", backoff, err)
		select {
		case <-time.After(backoff):
		case <-f.closed:
			return "", false
		case <-ctx.Done():
			return "", false
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (f *Frontend) Run(ctx context.Context, selfID string) {
	defer close(f.out)
	// Prime the sync token so we don't replay the room's history on startup —
	// we only want messages that arrive from now on.
	//
	// This must succeed before the sync loop runs. syncOnce omits `since` when
	// it is empty, which the homeserver reads as a fresh initial sync and
	// answers with each room's recent timeline. Those events pass the message
	// and admin filters, so they are delivered as new commands and re-executed.
	// Matrix is the gate-bypass path, so they would run unchallenged. A
	// homeserver that is still booting after a host restart is enough to
	// trigger it, which is exactly when it is least likely to be noticed.
	since, ok := f.primeSince(ctx)
	if !ok {
		return
	}
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.closed:
			return
		default:
		}
		next, msgs, err := f.syncOnce(ctx, since)
		if err != nil {
			// Cancellation during shutdown is not an error worth logging.
			if ctx.Err() != nil {
				return
			}
			f.syncFailures.Add(1)
			f.logger.Printf("matrix: sync failed, retry in %s: %v", backoff, err)
			select {
			case <-time.After(backoff):
			case <-f.closed:
				return
			case <-ctx.Done():
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		f.lastSyncAtUnix.Store(time.Now().Unix())
		since = next
		for _, m := range msgs {
			if m.sender == selfID || !f.admins[m.sender] {
				continue // ignore our own echoes and non-admin senders
			}
			f.deliver(m)
		}
	}
}

// inbound is a decoded room message event. For media messages (image/audio/
// video/file), mxcURL/filename/mimetype are set and the adapter downloads the
// file before delivering a text pointer to it.
type inbound struct {
	roomID   string
	sender   string
	body     string
	eventID  string
	msgtype  string
	mxcURL   string
	filename string
	mimetype string
}

// LoadRooms restores the conversation-to-room map from path and keeps it there
// as rooms are learned. Without it, a restart drops every reply until the
// admin writes again: the room is otherwise only learned from inbound.
func (f *Frontend) LoadRooms(path string) error {
	f.roomsFile = path
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	rooms := map[string]string{}
	if err := json.Unmarshal(data, &rooms); err != nil {
		return fmt.Errorf("matrix rooms %s: %w", path, err)
	}
	// Only current admins and well-formed room ids come back: KnownConversation
	// treats this map as proof an admin spoke, so a revoked admin must not
	// survive a restart through the file.
	for conv, room := range rooms {
		if !f.admins[conv] || !strings.HasPrefix(room, "!") {
			continue
		}
		f.roomByConv.Store(conv, room)
		f.logger.Printf("matrix: restored room %s for %s", room, conv)
	}
	return nil
}

func (f *Frontend) saveRooms() {
	if f.roomsFile == "" {
		return
	}
	f.roomsMu.Lock()
	defer f.roomsMu.Unlock()
	rooms := map[string]string{}
	f.roomByConv.Range(func(k, v any) bool {
		rooms[k.(string)] = v.(string)
		return true
	})
	data, _ := json.MarshalIndent(rooms, "", "  ")
	tmp := f.roomsFile + ".tmp"
	_ = os.Remove(tmp) // a leftover tmp (or a link planted there) must not keep its mode or target
	if err := writeSynced(tmp, data); err != nil {
		f.logger.Printf("matrix: saving rooms: %v", err)
		return
	}
	if err := os.Rename(tmp, f.roomsFile); err != nil {
		f.logger.Printf("matrix: saving rooms: %v", err)
	}
}

func writeSynced(path string, data []byte) error {
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := fh.Write(data); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	return fh.Close()
}

// mediaMsgTypes are the m.room.message msgtypes that carry a downloadable file.
var mediaMsgTypes = map[string]string{
	"m.image": "image",
	"m.audio": "audio",
	"m.video": "video",
	"m.file":  "file",
}

func (f *Frontend) deliver(m inbound) {
	// DM-style identity, mirroring discord's DM handling: the admin room is
	// effectively 1:1 (Jean + the bot), so we use the sender's mxid as BOTH
	// the conversation id and chat_id. This keeps the Broker's identity-pair
	// invariant (chat_id == from_id for 1:1 chats) satisfied — stamping the
	// room id as chat_id would trip it. The physical room is remembered
	// separately so Send can route the reply back into it.
	convID := m.sender
	if prev, loaded := f.roomByConv.Swap(convID, m.roomID); !loaded || prev.(string) != m.roomID {
		f.saveRooms()
	}

	text := m.body
	meta := map[string]string{
		// msg_id is this message's identity across the whole relay,
		// stamped once at ingress like the other frontends do.
		"msg_id":    eventlog.NewMsgID(),
		"chat_id":   convID,
		"from_id":   m.sender,
		"from_name": m.sender,
		"room_id":   m.roomID,
		"source":    "matrix",
		"platform":  "matrix",
		"event_id":  m.eventID,
	}

	// Media message: download the file and rewrite the text into a pointer the
	// backend can act on (it can open the local path). The raw body of a media
	// message is just the filename, which alone tells the model nothing.
	if kind, isMedia := mediaMsgTypes[m.msgtype]; isMedia {
		meta["media_kind"] = kind
		meta["media_mimetype"] = m.mimetype
		path, err := f.downloadMedia(m.mxcURL, m.filename, m.eventID)
		if err != nil {
			f.logger.Printf("matrix: failed to download %s %q: %v", kind, m.filename, err)
			text = fmt.Sprintf("[%s received from Matrix: %q (%s) — download FAILED: %v]",
				kind, m.filename, m.mimetype, err)
		} else {
			meta["media_path"] = path
			text = fmt.Sprintf("[%s received from Matrix: %q (%s), saved to %s — open it to view]",
				kind, m.filename, m.mimetype, path)
		}
	}

	msg := relay.Message{
		ConversationID: convID,
		Role:           relay.User,
		Text:           text,
		Meta:           meta,
	}
	select {
	case f.out <- msg:
	default:
		// Backpressure: the broker isn't draining. Drop rather than block
		// the sync loop, and count it — the Matrix analogue of a recv drop.
		f.recvDrops.Add(1)
		f.logger.Printf("matrix: recv buffer full, dropped message from %s", m.sender)
	}
}

// downloadMedia fetches an mxc:// URI into the media spool and returns the
// local file path. It uses the authenticated v1 media endpoint (the legacy
// unauthenticated v3 endpoint is removed on current homeservers). The file is
// named with a short event-id prefix so repeated filenames don't collide.
func (f *Frontend) downloadMedia(mxc, filename, eventID string) (string, error) {
	if f.mediaSpool == "" {
		return "", fmt.Errorf("media spool not configured")
	}
	server, mediaID, ok := parseMXC(mxc)
	if !ok {
		return "", fmt.Errorf("invalid mxc uri %q", mxc)
	}
	if err := os.MkdirAll(f.mediaSpool, 0o755); err != nil {
		return "", err
	}
	local := filepath.Join(f.mediaSpool, safeFilename(eventID, filename))
	u := fmt.Sprintf("%s/_matrix/client/v1/media/download/%s/%s",
		f.homeserver, url.PathEscape(server), url.PathEscape(mediaID))
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+f.token)
	resp, err := f.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("download %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	out, err := os.Create(local)
	if err != nil {
		return "", err
	}
	defer out.Close()
	// Bound the write so a hostile/oversized response can't fill the disk. The
	// homeserver's own upload limit is 20 MiB; 64 MiB is comfortable slack.
	if _, err := io.Copy(out, io.LimitReader(resp.Body, 64<<20)); err != nil {
		return "", err
	}
	return local, nil
}

// parseMXC splits an mxc://server/mediaID URI.
func parseMXC(mxc string) (server, mediaID string, ok bool) {
	rest, found := strings.CutPrefix(mxc, "mxc://")
	if !found {
		return "", "", false
	}
	server, mediaID, found = strings.Cut(rest, "/")
	if !found || server == "" || mediaID == "" {
		return "", "", false
	}
	return server, mediaID, true
}

// safeFilename builds a path-safe local name: a short unique tag from the event
// id, an underscore, then the sanitized original filename. The tag prevents two
// files with the same name (e.g. "IMG_0001.jpg" twice) from overwriting.
func safeFilename(eventID, filename string) string {
	base := sanitizeName(filepath.Base(filename))
	if base == "" || base == "." {
		base = "file"
	}
	tag := sanitizeName(strings.TrimPrefix(eventID, "$"))
	if len(tag) > 10 {
		tag = tag[len(tag)-10:]
	}
	if tag == "" {
		tag = "media"
	}
	return tag + "_" + base
}

// sanitizeName maps anything outside [A-Za-z0-9._-] to '_' so the result is a
// safe single path segment.
func sanitizeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// Send delivers m into the Matrix room named by m.ConversationID (falling back
// to m.Meta["chat_id"]). Errors are returned so the broker's retry/ack path can
// handle them like any other frontend.
func (f *Frontend) Send(ctx context.Context, m relay.Message) error {
	// Resolve the physical room. The conversation id is DM-style (the mxid),
	// so map it back to the room we saw it in. A relayd-originated message may
	// instead carry the room id directly (Meta["room_id"] or a '!'-prefixed
	// ConversationID) — honor those too.
	room := ""
	if r, ok := f.roomByConv.Load(m.ConversationID); ok {
		room = r.(string)
	} else if rid := m.Meta["room_id"]; rid != "" {
		room = rid
	} else if strings.HasPrefix(m.ConversationID, "!") {
		room = m.ConversationID
	} else if strings.HasPrefix(m.Meta["chat_id"], "!") {
		room = m.Meta["chat_id"]
	}
	if room == "" {
		return fmt.Errorf("matrix send: cannot resolve room for conversation %q", m.ConversationID)
	}
	txnID := "relayd" + strconv.FormatInt(f.txn.Add(1), 10) + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	path := fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/%s",
		url.PathEscape(room), url.PathEscape(txnID))
	body := map[string]any{"msgtype": "m.text", "body": m.Text}
	// A timeout or 5xx may come after the homeserver stored the event. The
	// send endpoint is idempotent per transaction id, so one retry with the
	// same id settles it without risking a duplicate. If that is ambiguous
	// too, say the outcome is unknown rather than that it failed.
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if err = f.put(ctx, path, body, nil); err == nil {
			return nil
		}
		if !ambiguous(err) || ctx.Err() != nil {
			break
		}
	}
	f.sendFailures.Add(1)
	err = fmt.Errorf("matrix send to %s: %w", room, err)
	if ambiguous(err) {
		return relay.UnconfirmedSendError{Err: err}
	}
	return err
}

// statusError is a non-2xx answer from the homeserver.
type statusError struct {
	code int
	msg  string
}

func (e statusError) Error() string { return e.msg }

// ambiguous reports whether a request error leaves the outcome unknown: a
// timeout or a connection lost after the request went out (the server may have
// acted before the reply was lost), or a 5xx. A 4xx is a definite refusal, and
// so is a failure to connect at all: the request never reached the server.
func ambiguous(err error) bool {
	var se statusError
	if errors.As(err, &se) {
		return se.code >= 500
	}
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return false
	}
	return true
}

// --- Matrix Client-Server API plumbing ---

func (f *Frontend) initialSync(ctx context.Context) (string, error) {
	// filter=0 timeline events, we only want the next_batch token.
	u := f.homeserver + "/_matrix/client/v3/sync?timeout=0&filter=" +
		url.QueryEscape(`{"room":{"timeline":{"limit":0}}}`)
	var resp syncResponse
	if err := f.getURL(ctx, u, &resp); err != nil {
		return "", err
	}
	return resp.NextBatch, nil
}

func (f *Frontend) syncOnce(ctx context.Context, since string) (string, []inbound, error) {
	u := f.homeserver + "/_matrix/client/v3/sync?timeout=" +
		strconv.Itoa(int(syncTimeout/time.Millisecond))
	if since != "" {
		u += "&since=" + url.QueryEscape(since)
	}
	var resp syncResponse
	if err := f.getURL(ctx, u, &resp); err != nil {
		return since, nil, err
	}
	var msgs []inbound
	for roomID, room := range resp.Rooms.Join {
		for _, ev := range room.Timeline.Events {
			if ev.Type != "m.room.message" {
				continue
			}
			isText := ev.Content.MsgType == "m.text" || ev.Content.MsgType == "m.notice"
			_, isMedia := mediaMsgTypes[ev.Content.MsgType]
			if !isText && !isMedia {
				continue
			}
			in := inbound{
				roomID:  roomID,
				sender:  ev.Sender,
				body:    ev.Content.Body,
				eventID: ev.EventID,
				msgtype: ev.Content.MsgType,
			}
			if isMedia {
				in.mxcURL = ev.Content.URL
				in.filename = ev.Content.Body
				in.mimetype = ev.Content.Info.MimeType
			}
			msgs = append(msgs, in)
		}
		// Auto-join is handled below via invite state.
	}
	// Accept any pending room invites (so a new admin room "just works").
	for roomID := range resp.Rooms.Invite {
		if err := f.joinRoom(ctx, roomID); err != nil {
			f.logger.Printf("matrix: failed to join invited room %s: %v", roomID, err)
		} else {
			f.logger.Printf("matrix: joined invited room %s", roomID)
		}
	}
	return resp.NextBatch, msgs, nil
}

func (f *Frontend) joinRoom(ctx context.Context, roomID string) error {
	path := "/_matrix/client/v3/join/" + url.PathEscape(roomID)
	return f.post(ctx, path, map[string]any{}, nil)
}

type syncResponse struct {
	NextBatch string `json:"next_batch"`
	Rooms     struct {
		Join map[string]struct {
			Timeline struct {
				Events []event `json:"events"`
			} `json:"timeline"`
		} `json:"join"`
		Invite map[string]json.RawMessage `json:"invite"`
	} `json:"rooms"`
}

type event struct {
	Type    string `json:"type"`
	Sender  string `json:"sender"`
	EventID string `json:"event_id"`
	Content struct {
		MsgType string `json:"msgtype"`
		Body    string `json:"body"` // text body, or filename for media
		URL     string `json:"url"`  // mxc:// URI for media messages
		Info    struct {
			MimeType string `json:"mimetype"`
			Size     int64  `json:"size"`
		} `json:"info"`
	} `json:"content"`
}

func (f *Frontend) get(ctx context.Context, path string, out any) error {
	return f.getURL(ctx, f.homeserver+path, out)
}

func (f *Frontend) getURL(ctx context.Context, u string, out any) error {
	return f.do(ctx, http.MethodGet, u, nil, out)
}

func (f *Frontend) post(ctx context.Context, path string, body, out any) error {
	return f.do(ctx, http.MethodPost, f.homeserver+path, body, out)
}

func (f *Frontend) put(ctx context.Context, path string, body, out any) error {
	return f.do(ctx, http.MethodPut, f.homeserver+path, body, out)
}

func (f *Frontend) do(ctx context.Context, method, u string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+f.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Matrix errors are JSON {errcode, error}; surface them without
		// leaking the bearer token (which never appears in the body).
		return statusError{code: resp.StatusCode, msg: fmt.Sprintf("matrix %s %d: %s", method, resp.StatusCode, strings.TrimSpace(string(data)))}
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("matrix decode: %w", err)
		}
	}
	return nil
}
