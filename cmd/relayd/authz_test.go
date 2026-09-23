// Characterization tests for relayd's authorization decisions.
//
// These pin the behaviour of the CURRENT implementation, including its quirks,
// so the planned split into an internal/authz package can be shown to be
// equivalent rather than merely believed to be. They are written against the
// code as it stands and must pass unchanged afterwards.
//
// The thing they are guarding against is specific: this is the control between
// an inbound message and a shell on the host, and the direction a refactor
// drifts is fail-open. A test that only asserts the allow cases would not
// notice that.
package main

import (
	"testing"

	"github.com/jeanhaley32/agent-relay/internal/access"
)

const (
	tgAdmin      = int64(111)
	tgAllowed    = int64(222)
	dcAdmin      = int64(900000000000000001)
	dcAllowed    = int64(900000000000000002)
	matrixAdmin  = "@jean:vessel"
	webConvID    = "web-jean"
	strangerNum  = "999"
	strangerText = "@nobody:example.org"
)

func testManagers(t *testing.T) (tg, dc *access.Manager) {
	t.Helper()
	tg = access.New([]int64{tgAdmin}, []int64{tgAllowed}, "", nil)
	dc = access.New([]int64{dcAdmin}, []int64{dcAllowed}, "", nil)
	return tg, dc
}

func TestIsAdminAcrossEveryIDNamespace(t *testing.T) {
	tg, dc := testManagers(t)
	isAdmin := newIsAdmin(
		map[string]bool{matrixAdmin: true},
		map[string]bool{webConvID: true},
		tg,
		func() *access.Manager { return dc },
	)

	cases := []struct {
		name   string
		sender string
		want   bool
	}{
		{"telegram admin", "111", true},
		{"telegram allowed but not admin", "222", false},
		{"discord admin", "900000000000000001", true},
		{"discord allowed but not admin", "900000000000000002", false},
		{"matrix admin id", matrixAdmin, true},
		{"web conversation id", webConvID, true},
		{"unknown numeric sender", strangerNum, false},
		{"unknown non-numeric sender", strangerText, false},
		{"empty sender", "", false},
		// The non-numeric checks run before ParseInt. If that order were
		// reversed these two would fall through to false.
		{"matrix id would not survive ParseInt", "@jean:vessel", true},
		{"web id would not survive ParseInt", "web-jean", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAdmin(tc.sender); got != tc.want {
				t.Fatalf("isAdmin(%q) = %v, want %v", tc.sender, got, tc.want)
			}
		})
	}
}

// Discord's manager is read through a func because main assigns it after this
// predicate is built. A nil manager must deny rather than panic.
func TestIsAdminWithDiscordDisabled(t *testing.T) {
	tg, _ := testManagers(t)
	isAdmin := newIsAdmin(map[string]bool{}, map[string]bool{}, tg,
		func() *access.Manager { return nil })

	if !isAdmin("111") {
		t.Fatal("telegram admin should still be admin with Discord off")
	}
	if isAdmin("900000000000000001") {
		t.Fatal("a Discord admin id must not be admin when Discord is disabled")
	}
}

func TestOutboundAllowedAcrossEveryNamespace(t *testing.T) {
	tg, dc := testManagers(t)

	// `known` stands in for the frontends that own non-numeric conversation
	// ids: Matrix room ids and the web pane's ConvID.
	known := func(id string) bool { return id == "!room:vessel" || id == webConvID }

	cases := []struct {
		name   string
		chatID string
		known  func(string) bool
		want   bool
	}{
		{"telegram admin is allowlisted", "111", known, true},
		{"telegram allowlisted non-admin", "222", known, true},
		{"discord allowlisted", "900000000000000002", known, true},
		{"matrix room via known()", "!room:vessel", known, true},
		{"web conv id via known()", webConvID, known, true},
		{"unknown numeric target", strangerNum, known, false},
		{"unknown non-numeric target", strangerText, known, false},
		{"empty target", "", known, false},
		// known is nil when no frontend supplies ownership.
		{"matrix room with no known func", "!room:vessel", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := outboundAllowed(tc.chatID, tg, dc, tc.known); got != tc.want {
				t.Fatalf("outboundAllowed(%q) = %v, want %v", tc.chatID, got, tc.want)
			}
		})
	}
}

// With Discord disabled, a Discord id must not become a legal outbound target.
func TestOutboundAllowedWithDiscordDisabled(t *testing.T) {
	tg, _ := testManagers(t)
	if outboundAllowed("900000000000000002", tg, nil, nil) {
		t.Fatal("a Discord chat id was allowed as an outbound target with Discord disabled")
	}
	if !outboundAllowed("222", tg, nil, nil) {
		t.Fatal("a Telegram allowlisted id should remain a legal target")
	}
}
