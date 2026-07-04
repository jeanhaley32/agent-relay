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
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/relay"
)

const defaultBaseURL = "https://api.telegram.org"

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
}

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

// WithLogger sets a logger (default: discard).
func WithLogger(l *log.Logger) Option { return func(f *Frontend) { f.logger = l } }

// New builds a Telegram frontend and starts polling. Close stops it.
func New(token string, opts ...Option) *Frontend {
	f := &Frontend{
		token:       token,
		base:        defaultBaseURL,
		auth:        staticAuthorizer{}, // fail-closed default (denies everyone)
		pollTimeout: 30,
		logger:      log.New(io.Discard, "", 0),
		recv:        make(chan relay.Message, 32),
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
	return f
}

func (f *Frontend) Name() string               { return "telegram" }
func (f *Frontend) Recv() <-chan relay.Message  { return f.recv }

// Close stops polling. The Recv channel is closed by the poll loop on exit.
func (f *Frontend) Close() error { f.cancel(); return nil }

// --- Telegram wire types (subset we use) ------------------------------------

type tgUser struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name"`
	Username  string `json:"username"`
}

type tgChat struct {
	ID int64 `json:"id"`
}

type tgMessage struct {
	MessageID int64   `json:"message_id"`
	From      tgUser  `json:"from"`
	Chat      tgChat  `json:"chat"`
	Text      string  `json:"text"`
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
			f.logger.Printf("getUpdates error: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second): // simple backoff
			}
			continue
		}
		for _, u := range updates {
			offset = u.UpdateID + 1 // ack: never re-fetch this update
			m := u.Message
			if m == nil || m.Text == "" {
				continue
			}
			if m.From.ID <= 0 { // no/invalid sender (e.g. anonymous channel post) — ignore
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

// Send delivers a message to the chat named by m.Meta["chat_id"] (falling back
// to m.ConversationID) via sendMessage.
func (f *Frontend) Send(ctx context.Context, m relay.Message) error {
	chatID := m.Meta["chat_id"]
	if chatID == "" {
		chatID = m.ConversationID
	}
	if chatID == "" {
		return fmt.Errorf("telegram send: no chat_id")
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
		return fmt.Errorf("telegram sendMessage status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}
