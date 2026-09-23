// Package authz owns relayd's authorization decisions: may this sender speak,
// may they run admin commands, and may relayd send to this conversation.
//
// It exists because those decisions were previously anonymous closures in
// cmd/relayd/main.go, reconciling five mechanisms across three id namespaces
// with no single place to read the policy. See
// ~/vessel-log/notes/relay-session-authz-analysis.md.
//
// The concept that makes the special cases disappear is Assurance: how well
// the transport itself established who the sender is. A Telegram or Discord id
// is a claim by a third-party account, spoofable if that account is
// compromised, so it needs a periodic liveness proof. A message from a
// tailnet-only Matrix homeserver, or from the web pane behind a per-request
// whois, is already proved by the fact that it arrived.
package authz

import "strconv"

// Assurance is how strongly the transport established the sender's identity.
type Assurance int

const (
	// Claimed: a third-party platform account asserts this id. The account
	// could be compromised, so the id alone is not proof.
	Claimed Assurance = iota

	// Proved: reaching this transport at all required being who you say you
	// are. A tailnet-only homeserver, or a per-request whois check.
	Proved
)

func (a Assurance) String() string {
	if a == Proved {
		return "proved"
	}
	return "claimed"
}

// NumericDirectory answers questions about ids in one numeric namespace. Both
// access.Manager instances satisfy it, which is how one Authorizer covers
// Telegram and Discord without knowing either exists.
type NumericDirectory interface {
	Allowed(id int64) bool
	IsAdmin(id int64) bool
}

// Decision is an authorization answer with its reason. The reason is for the
// operator reading a log line, not for the caller to branch on.
type Decision struct {
	Allowed bool
	Reason  string
}

func allow(reason string) Decision { return Decision{Allowed: true, Reason: reason} }
func deny(reason string) Decision  { return Decision{Allowed: false, Reason: reason} }

// Authorizer holds the policy. Zero value is useless; use New.
type Authorizer struct {
	// numeric are directories keyed by an int64 id, consulted in order.
	// A nil entry is skipped, which is how "Discord is disabled" is
	// expressed without a special case.
	numeric []NumericDirectory

	// named are ids that are not numbers: Matrix user ids and the web
	// pane's conversation id. They are admins by configuration rather than
	// by a directory lookup.
	namedAdmins map[string]bool

	// owns reports whether any frontend claims a conversation id. Supplied
	// by the caller because ownership is a routing fact the frontends hold.
	owns func(conversationID string) bool
}

// Option configures an Authorizer.
type Option func(*Authorizer)

// WithNumericDirectory adds a directory of int64-keyed ids. Nil is accepted
// and ignored, so a caller can pass a disabled platform's manager without a
// conditional.
func WithNumericDirectory(d NumericDirectory) Option {
	return func(a *Authorizer) {
		if d != nil {
			a.numeric = append(a.numeric, d)
		}
	}
}

// WithNamedAdmins adds non-numeric admin ids.
func WithNamedAdmins(ids ...string) Option {
	return func(a *Authorizer) {
		for _, id := range ids {
			if id != "" {
				a.namedAdmins[id] = true
			}
		}
	}
}

// WithConversationOwnership supplies the routing check used by MayReceive.
func WithConversationOwnership(owns func(string) bool) Option {
	return func(a *Authorizer) { a.owns = owns }
}

func New(opts ...Option) *Authorizer {
	a := &Authorizer{namedAdmins: map[string]bool{}}
	for _, o := range opts {
		o(a)
	}
	return a
}

// MayAdmin reports whether senderID may run admin-flagged commands.
//
// Assurance is a parameter rather than a separate gate elsewhere: the
// liveness requirement is part of this question, not a different one. A
// Proved sender needs no session; a Claimed sender's liveness is checked by
// the caller, which owns the session and device machinery.
func (a *Authorizer) MayAdmin(senderID string) Decision {
	if senderID == "" {
		return deny("empty sender id")
	}
	// Non-numeric ids are checked first and deliberately: strconv.ParseInt
	// fails on them, so a lookup-first order would deny every Matrix and web
	// admin. This ordering is load-bearing.
	if a.namedAdmins[senderID] {
		return allow("configured named admin")
	}
	id, err := strconv.ParseInt(senderID, 10, 64)
	if err != nil {
		return deny("sender id is neither a configured named admin nor numeric")
	}
	for _, d := range a.numeric {
		if d.IsAdmin(id) {
			return allow("admin in a numeric directory")
		}
	}
	return deny("not an admin in any directory")
}

// NeedsLivenessProof reports whether a sender at this assurance level must
// additionally prove they are live — an approved session or a present bound
// device. This is the whole of the rule that SessionGatedUsers plus two
// undocumented absences used to express.
func NeedsLivenessProof(as Assurance) bool { return as != Proved }

// MayReceive reports whether relayd may send to a conversation id. A target is
// legitimate if it is allowlisted in a numeric directory, or if some frontend
// owns it — a Matrix room or the web pane, which relayd only ever learns about
// from an authorized inbound message.
func (a *Authorizer) MayReceive(conversationID string) Decision {
	if conversationID == "" {
		return deny("empty conversation id")
	}
	if id, err := strconv.ParseInt(conversationID, 10, 64); err == nil {
		for _, d := range a.numeric {
			if d.Allowed(id) {
				return allow("allowlisted in a numeric directory")
			}
		}
	}
	if a.owns != nil && a.owns(conversationID) {
		return allow("owned by a frontend")
	}
	return deny("not allowlisted and owned by no frontend")
}
