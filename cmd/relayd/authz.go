// Adapters between relayd's concrete platform managers and internal/authz,
// which owns the policy itself.
//
// Two warts live here rather than in authz, because they are facts about how
// main is assembled rather than about authorization: Discord's access.Manager
// does not exist until its handshake has run, and conversation ownership is
// not known until the frontends are constructed. Both are late-bound behind a
// func so a single Authorizer can still be built once, early.
//
// See ~/vessel-log/notes/relay-session-authz-analysis.md.
package main

import (
	"github.com/jeanhaley32/agent-relay/internal/access"
	"github.com/jeanhaley32/agent-relay/internal/authz"
)

// lateDirectory reads a directory through a func, so a manager assigned after
// the Authorizer is built is still consulted. A nil manager answers no to
// everything, which is how "this platform is disabled" is expressed.
type lateDirectory struct{ get func() *access.Manager }

func (l lateDirectory) Allowed(id int64) bool {
	d := l.get()
	return d != nil && d.Allowed(id)
}

func (l lateDirectory) IsAdmin(id int64) bool {
	d := l.get()
	return d != nil && d.IsAdmin(id)
}

// newAuthorizer assembles relayd's policy. named holds the non-numeric admin
// ids (Matrix user ids, the web pane's ConvID); acc and discordAcc are the two
// numeric directories, consulted in that order; owns reports whether a
// frontend claims a conversation id.
func newAuthorizer(named []map[string]bool, acc *access.Manager, discordAcc func() *access.Manager, owns func(string) bool) *authz.Authorizer {
	opts := []authz.Option{
		authz.WithNumericDirectory(lateDirectory{get: func() *access.Manager { return acc }}),
		authz.WithNumericDirectory(lateDirectory{get: discordAcc}),
		authz.WithConversationOwnership(owns),
	}
	for _, set := range named {
		for id := range set {
			opts = append(opts, authz.WithNamedAdmins(id))
		}
	}
	return authz.New(opts...)
}

// newIsAdmin builds the admin predicate the command registry gates on.
func newIsAdmin(matrixAdmins, webAdmins map[string]bool, acc *access.Manager, discordAcc func() *access.Manager) func(string) bool {
	a := newAuthorizer([]map[string]bool{matrixAdmins, webAdmins}, acc, discordAcc, nil)
	return func(senderID string) bool { return a.MayAdmin(senderID).Allowed }
}

// outboundAllowed implements the outbound gate: the model can only reply to
// allowlisted chats. The inbound allowlist gates who reaches Claude; this
// stops Claude messaging strangers. chatID is a Telegram chat id (== user id)
// or, for Discord, gate()'s convID (== user id for DMs, == channel id for
// guild messages). Guild channels are inherently multi-party so single-id
// allowlisting doesn't apply there; instead known reports whether a frontend
// has already seen and gated this chatID inbound — a guild channel from an
// allowed guild, a Matrix room, or the web pane. That covers relayd-originated
// replies into a conversation the model was legitimately talking in, while
// still failing closed for anything never seen inbound.
func outboundAllowed(chatID string, acc *access.Manager, discordAcc *access.Manager, known func(string) bool) bool {
	a := newAuthorizer(nil, acc, func() *access.Manager { return discordAcc }, known)
	return a.MayReceive(chatID).Allowed
}

// frontendAdmins pairs a live frontend with the admin ids configured for it.
type frontendAdmins struct {
	front authz.Assured
	ids   []string
}

// livenessGated returns the admin ids that must additionally prove they are
// live — an approved tailnet session, or a present bound device — before their
// admin commands are honored.
//
// The rule is read off the transport: an id a platform merely *claims* is
// spoofable by whoever holds that account, so those admins are gated. An id a
// transport *proved* (a tailnet-only homeserver, a per-request whois) needs no
// second proof, so those admins are absent from the result.
//
// This used to be an inline union of the Telegram and Discord admin lists, and
// the Matrix and web exemptions were expressed as nothing at all — an absence
// with a comment. Now every frontend states its own assurance and the absence
// is a consequence rather than an omission.
func livenessGated(fs []frontendAdmins) map[string]bool {
	gated := map[string]bool{}
	for _, f := range fs {
		if f.front == nil || !authz.NeedsLivenessProof(f.front.Assurance()) {
			continue
		}
		for _, id := range f.ids {
			gated[id] = true
		}
	}
	return gated
}
