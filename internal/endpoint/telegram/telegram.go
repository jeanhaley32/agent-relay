// Package telegram implements a relay frontend Endpoint backed by the Telegram
// Bot API. It long-polls getUpdates for inbound messages and sends replies via
// sendMessage. Inbound messages are gated by a sender allowlist (on the sender's
// user id, never the chat id) — an ungated channel is a prompt-injection vector.
//
// The HTTP client and base URL are injectable so the endpoint is unit-testable
// against an httptest server with no real bot token.
package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/endpoint/senderr"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

const defaultBaseURL = "https://api.telegram.org"

// maxMessageLen is Telegram's hard per-message character cap (sendMessage
// rejects anything longer with HTTP 400 "message is too long"). Marked
// permanent (see permanentSendError) since retrying an oversized message
// just repeats the same guaranteed failure.
const maxMessageLen = 4096

type permanentSendError = senderr.Permanent

// Authorizer decides whether a sender may use the relay and records requests
// from those who may not (so an admin can approve them later). An access.Manager
// satisfies this; so does the built-in static allowlist.
type Authorizer interface {
	Allowed(id int64) bool
	Record(id int64, name string)
}

// staticAuthorizer is a fixed allowlist (WithAllowlist). It records nothing.
type staticAuthorizer map[int64]bool

func (s staticAuthorizer) Allowed(id int64) bool { return s[id] }
func (s staticAuthorizer) Record(int64, string)  {}

// Frontend is a Telegram Bot API frontend Endpoint.
type Frontend struct {
	token       string
	base        string
	http        *http.Client
	auth        Authorizer
	pollTimeout int // long-poll seconds
	logger      *log.Logger

	recv   chan relay.Message
	cancel context.CancelFunc

	// Send() returns immediately without waiting on delivery, so a transient
	// Telegram outage would otherwise drop a reply forever with no trace of
	// the failure anywhere. retryQueue is a bounded background retry path: a
	// failed send is also queued here for a background worker to keep
	// retrying with backoff, instead of just vanishing.
	retryQueue     chan retryItem
	sendFailures   atomic.Int64 // failed immediate attempts only, not background retries
	permanentDrops atomic.Int64 // count of drops across all give-up paths
	queueDepth     atomic.Int64

	// getUpdatesFailures/lastPollSuccess are the real signal for an outage:
	// polling is how relayd actually finds out whether Telegram is reachable
	// at all, independent of whether anything happened to be sent during
	// the outage.
	getUpdatesFailures atomic.Int64
	lastPollSuccess    atomic.Int64 // unix seconds
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

// WithBaseURL overrides the Telegram API base (for tests).
func WithBaseURL(u string) Option { return func(f *Frontend) { f.base = strings.TrimRight(u, "/") } }

// WithHTTPClient injects an HTTP client (for tests / custom transport).
func WithHTTPClient(c *http.Client) Option { return func(f *Frontend) { f.http = c } }

// WithAllowlist sets a fixed set of permitted sender user ids. REQUIRED for real
// use if no Authorizer is supplied: an empty allowlist denies everyone (fail
// closed). For dynamic approval, use WithAuthorizer instead.
func WithAllowlist(ids ...int64) Option {
	return func(f *Frontend) {
		s := staticAuthorizer{}
		for _, id := range ids {
			s[id] = true
		}
		f.auth = s
	}
}

// WithAuthorizer supplies a custom authorization backend (e.g. access.Manager)
// that can grant access dynamically and record pending requests.
func WithAuthorizer(a Authorizer) Option { return func(f *Frontend) { f.auth = a } }

// WithPollTimeout sets the long-poll timeout in seconds (default 30).
func WithPollTimeout(sec int) Option { return func(f *Frontend) { f.pollTimeout = sec } }

// WithLogger sets a logger (default: discard). A nil logger is treated as
// discard so every other f.logger.Printf call site can assume non-nil.
func WithLogger(l *log.Logger) Option {
	return func(f *Frontend) {
		if l == nil {
			l = log.New(io.Discard, "", 0)
		}
		f.logger = l
	}
}

// New builds a Telegram frontend and starts polling. Close stops it.
func New(token string, opts ...Option) *Frontend {
	f := &Frontend{
		token:       token,
		base:        defaultBaseURL,
		auth:        staticAuthorizer{}, // fail-closed default (denies everyone)
		pollTimeout: 30,
		logger:      log.New(io.Discard, "", 0),
		recv:        make(chan relay.Message, 32),
		retryQueue:  make(chan retryItem, retryQueueCapacity),
	}
	for _, o := range opts {
		o(f)
	}
	if f.http == nil {
		// Give the client headroom over the long-poll timeout.
		f.http = &http.Client{Timeout: time.Duration(f.pollTimeout+15) * time.Second}
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	go f.pollLoop(ctx)
	go f.startRetryWorker(ctx)
	return f
}

func (f *Frontend) Name() string               { return "telegram" }
func (f *Frontend) Recv() <-chan relay.Message { return f.recv }

// Close stops polling. The Recv channel is closed by the poll loop on exit.
func (f *Frontend) Close() error { f.cancel(); return nil }

// --- Telegram wire types (subset we use) ------------------------------------

type tgUser struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name"`
	Username  string `json:"username"`
}

type tgChat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"` // "private" | "group" | "supergroup" | "channel"
}

type tgMessage struct {
	MessageID int64  `json:"message_id"`
	From      tgUser `json:"from"`
	Chat      tgChat `json:"chat"`
	Text      string `json:"text"`
}

type tgUpdate struct {
	UpdateID int64      `json:"update_id"`
	Message  *tgMessage `json:"message"`
}

type tgUpdatesResp struct {
	OK          bool       `json:"ok"`
	Result      []tgUpdate `json:"result"`
	Description string     `json:"description"`
}

// pollLoop long-polls getUpdates and emits allowed messages until ctx is done.
func (f *Frontend) pollLoop(ctx context.Context) {
	defer close(f.recv)
	var offset int64
	for {
		if ctx.Err() != nil {
			return
		}
		updates, err := f.getUpdates(ctx, offset)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			f.getUpdatesFailures.Add(1)
			f.logger.Printf("getUpdates error: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second): // simple backoff
			}
			continue
		}
		f.lastPollSuccess.Store(time.Now().Unix())
		for _, u := range updates {
			offset = u.UpdateID + 1 // ack: never re-fetch this update
			m := u.Message
			if m == nil || m.Text == "" {
				continue
			}
			if m.From.ID <= 0 { // no/invalid sender (e.g. anonymous channel post) — ignore
				continue
			}
			// Groups/supergroups/channels are refused outright, not just
			// unauthenticated: a group chat mixes messages from many
			// senders under one chat_id, so per-sender allowlisting and
			// conversation identity both get muddier than in a private 1:1
			// chat. BotFather is also configured to disallow group invites,
			// but this is the code-level backstop in case that setting is
			// ever changed or reset.
			if m.Chat.Type != "private" {
				f.logger.Printf("dropped message from non-private chat id=%d type=%q sender=%d", m.Chat.ID, m.Chat.Type, m.From.ID)
				continue
			}
			if !f.auth.Allowed(m.From.ID) {
				name := m.From.Username
				if name == "" {
					name = m.From.FirstName
				}
				f.auth.Record(m.From.ID, name) // queue as a pending request
				f.logger.Printf("dropped message from unauthorized sender id=%d (%s) — recorded as pending", m.From.ID, name)
				continue
			}
			msg := relay.Message{
				ConversationID: strconv.FormatInt(m.Chat.ID, 10),
				Role:           relay.User,
				Text:           m.Text,
				Meta: map[string]string{
					"chat_id":   strconv.FormatInt(m.Chat.ID, 10),
					"from_id":   strconv.FormatInt(m.From.ID, 10),
					"from_name": m.From.Username,
				},
			}
			select {
			case f.recv <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (f *Frontend) getUpdates(ctx context.Context, offset int64) ([]tgUpdate, error) {
	q := url.Values{}
	q.Set("timeout", strconv.Itoa(f.pollTimeout))
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	endpoint := fmt.Sprintf("%s/bot%s/getUpdates?%s", f.base, f.token, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out tgUpdatesResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if !out.OK {
		return nil, fmt.Errorf("telegram: %s", out.Description)
	}
	return out.Result, nil
}

// BotInfo identifies the connected bot, returned by the getMe handshake.
type BotInfo struct {
	ID        int64
	Username  string
	FirstName string
}

// Me performs the connection handshake: it calls Telegram's getMe to verify the
// token and identify the bot. A clear error here means a bad token or that
// Telegram is unreachable — call it at startup to fail fast instead of silently
// long-polling a misconfigured bot.
func (f *Frontend) Me(ctx context.Context) (BotInfo, error) {
	endpoint := fmt.Sprintf("%s/bot%s/getMe", f.base, f.token)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return BotInfo{}, err
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return BotInfo{}, err
	}
	defer resp.Body.Close()
	var out struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
		Result      tgUser `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return BotInfo{}, err
	}
	if !out.OK {
		return BotInfo{}, fmt.Errorf("telegram getMe: %s", out.Description)
	}
	return BotInfo{ID: out.Result.ID, Username: out.Result.Username, FirstName: out.Result.FirstName}, nil
}

// Send delivers a message to the chat named by m.Meta["chat_id"] (falling back
// to m.ConversationID), splitting it via senderr.Split if over Telegram's
// limit. Ordering across chunks is best-effort only: once a chunk is queued
// for background retry, it's retried independently, so a later chunk can
// still land before an earlier one.
func (f *Frontend) Send(ctx context.Context, m relay.Message) error {
	chunks := senderr.Split(m.Text, maxMessageLen)
	if len(chunks) > 1 {
		var permErr, firstErr error
		queuing := false
		for _, chunk := range chunks {
			cm := m
			cm.Text = chunk
			if queuing {
				f.enqueueRetry(cm)
				continue
			}
			if err := f.sendChunk(ctx, cm); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				var perm permanentSendError
				if errors.As(err, &perm) {
					if permErr == nil {
						permErr = err
					}
					continue
				}
				// Transient failure: sendChunk already queued this chunk for
				// retry. Stop attempting later chunks immediately and queue
				// them too, in order, so the retry worker delivers the rest
				// of the split reply in the original order instead of racing
				// ahead of the queued chunk.
				queuing = true
			}
		}
		if permErr != nil {
			return permErr
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
		// Never retried, so it's dropped right here - count it alongside the
		// async gave-up-after-retries drops so PermanentDrops() reflects every
		// message that will never be delivered, not just the ones that went
		// through the retry queue first.
		f.permanentDrops.Add(1)
		f.logger.Printf("telegram send permanently failed (not retrying): %v", err)
		return err
	}
	f.logger.Printf("telegram send failed, queuing for background retry: %v", err)
	f.enqueueRetry(m)
	return err
}

func (f *Frontend) sendOnce(ctx context.Context, m relay.Message) error {
	chatID := m.Meta["chat_id"]
	if chatID == "" {
		chatID = m.ConversationID
	}
	if chatID == "" {
		return permanentSendError{Err: fmt.Errorf("telegram send: no chat_id")}
	}
	body, _ := json.Marshal(map[string]any{"chat_id": chatID, "text": m.Text})
	endpoint := fmt.Sprintf("%s/bot%s/sendMessage", f.base, f.token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		sendErr := fmt.Errorf("telegram sendMessage status %d: %s", resp.StatusCode, string(b))
		// 429 (rate limited) is the one 4xx worth retrying - Telegram expects
		// the caller to back off and resend the identical payload. Every
		// other 4xx (400 malformed/too-long, 403 blocked-by-user, etc.) will
		// fail identically on retry, so mark it permanent rather than
		// burning 12 retry attempts on a guaranteed-repeat failure.
		if resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusTooManyRequests {
			return permanentSendError{Err: sendErr}
		}
		return sendErr
	}
	return nil
}

// enqueueRetry adds a failed message to the background retry queue.
// Non-blocking: if the channel is full (a genuinely extreme backlog), the
// oldest-in-flight item is dropped rather than blocking the caller, which
// would stall the whole relay. The actual backlog bound is enforced in
// startRetryWorker (which caps its pending slice too), since draining the
// channel each loop would otherwise let the real in-flight count grow well
// past retryQueueCapacity.
func (f *Frontend) enqueueRetry(m relay.Message) {
	item := retryItem{msg: m, attempts: 0, nextAt: time.Now().Add(retryBackoff(0))}
	select {
	case f.retryQueue <- item:
		f.queueDepth.Add(1)
	default:
		f.logger.Printf("telegram retry queue full (%d), dropping oldest queued item to make room", retryQueueCapacity)
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
			f.logger.Printf("telegram retry queue full again, dropping new item")
			f.permanentDrops.Add(1)
		}
	}
}

// retryBackoff is exponential with a cap, so a sustained outage doesn't
// hammer Telegram's API uselessly, but a brief blip still retries fast
// enough to matter.
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
			// pending is capped separately from the channel: draining the
			// channel each iteration would otherwise make the channel's
			// 200-slot bound illusory, letting the in-flight backlog grow
			// unbounded with failure-rate x retry-exhaustion-time.
			if len(pending) >= retryQueueCapacity {
				f.logger.Printf("telegram retry backlog full (%d), dropping oldest pending item", retryQueueCapacity)
				f.queueDepth.Add(-1)
				f.permanentDrops.Add(1)
				pending = pending[1:]
			}
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
					f.logger.Printf("telegram retry succeeded after %d attempt(s) for chat %s",
						item.attempts+1, item.msg.Meta["chat_id"])
					continue
				}
				var perm permanentSendError
				if errors.As(err, &perm) {
					f.queueDepth.Add(-1)
					f.permanentDrops.Add(1)
					f.logger.Printf("telegram retry gave up (permanent failure) for chat %s: %v",
						item.msg.Meta["chat_id"], err)
					continue
				}
				item.attempts++
				if item.attempts >= maxRetryAttempts {
					f.queueDepth.Add(-1)
					f.permanentDrops.Add(1)
					f.logger.Printf("telegram retry gave up after %d attempts for chat %s: %v",
						item.attempts, item.msg.Meta["chat_id"], err)
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

// GetUpdatesFailures and LastPollSuccess expose the getUpdates polling
// health signal (see the field comments on Frontend for why).
func (f *Frontend) GetUpdatesFailures() int64 { return f.getUpdatesFailures.Load() }
func (f *Frontend) LastPollSuccess() int64    { return f.lastPollSuccess.Load() }
