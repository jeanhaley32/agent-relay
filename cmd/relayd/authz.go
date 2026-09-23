// Admin identity resolution, extracted from main so it can be characterized.
//
// This is the whole of relayd's "is this sender an admin" policy, and it is
// spread over three id namespaces: non-numeric Matrix ids, the non-numeric web
// ConvID, and int64 ids held in one access.Manager per numeric platform. The
// ordering is load-bearing — the non-numeric checks must precede ParseInt,
// which returns false for anything that is not a number.
//
// See ~/vessel-log/notes/relay-session-authz-analysis.md for why this shape
// exists and what is intended to replace it.
package main

import (
	"strconv"

	"github.com/jeanhaley32/agent-relay/internal/access"
)

// newIsAdmin builds the admin predicate the command registry gates on.
// discordAcc is read through a func because main assigns it later, once
// Discord's own handshake has run.
func newIsAdmin(matrixAdmins, webAdmins map[string]bool, acc *access.Manager, discordAcc func() *access.Manager) func(string) bool {
	return func(senderID string) bool {
		if matrixAdmins[senderID] {
			return true // Matrix admin ids are non-numeric (@user:server)
		}
		if webAdmins[senderID] {
			return true // web frontend ConvID (non-numeric), tailnet-whois verified
		}
		id, err := strconv.ParseInt(senderID, 10, 64)
		if err != nil {
			return false
		}
		if acc != nil && acc.IsAdmin(id) {
			return true
		}
		d := discordAcc()
		return d != nil && d.IsAdmin(id)
	}
}
