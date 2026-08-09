// Package inbound is the shared constructor for the canonical inbound message
// every frontend produces. Telegram, Discord, and any future frontend build the
// same envelope — a User-role relay.Message whose Meta is stamped with a fresh
// msg_id, a receive timestamp, and the sender's identity — so that construction
// lives in exactly one place instead of being copy-pasted (and drifting) per
// frontend. Platform-specific Meta (e.g. Discord's channel_id or guild marker)
// is layered on by the caller after Envelope returns.
package inbound

import (
	"time"

	"github.com/jeanhaley32/agent-relay/internal/eventlog"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

// TimeFormat is the wall-clock format stamped into Meta["ts"] at ingress:
// RFC3339 in UTC, so the model (and the audit trail) can compute the elapsed
// time between messages unambiguously.
const TimeFormat = time.RFC3339

// now is overridable in tests; nil ⇒ time.Now.
var now = time.Now

// Envelope builds the canonical inbound relay.Message. platform names the
// source frontend ("telegram", "discord"); convID is the conversation identity
// (chat/channel/DM); chatID/fromID/fromName are the sender identity as the
// broker's gates and the audit trail expect them. The returned message's Meta
// carries:
//
//	msg_id    - this message's identity for the whole relay (minted here, once)
//	ts        - receive time (RFC3339 UTC), so elapsed time is knowable
//	platform  - originating frontend
//	chat_id   - conversation id used by the allowlist / caps / routing
//	from_id   - sender's permanent account id
//	from_name - sender's display name
//
// The caller may add frontend-specific keys to the returned Meta before
// sending it on (identifier-char keys only — they become <channel> tag
// attributes).
func Envelope(platform, convID, text, chatID, fromID, fromName string) relay.Message {
	return relay.Message{
		ConversationID: convID,
		Role:           relay.User,
		Text:           text,
		Meta: map[string]string{
			"msg_id":    eventlog.NewMsgID(),
			"ts":        now().UTC().Format(TimeFormat),
			"platform":  platform,
			"chat_id":   chatID,
			"from_id":   fromID,
			"from_name": fromName,
		},
	}
}
