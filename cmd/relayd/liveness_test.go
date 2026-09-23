package main

import (
	"testing"

	"github.com/jeanhaley32/agent-relay/internal/authz"
)

type fakeFrontend struct{ as authz.Assurance }

func (f fakeFrontend) Assurance() authz.Assurance { return f.as }

// The session gate used to be a hand-kept union of the Telegram and Discord
// admin lists, with Matrix and web exempt by virtue of nobody having added
// them. That exemption is now a consequence of their assurance, and this test
// pins the resulting set to what the old code produced.
func TestLivenessGatedMatchesTheOldUnion(t *testing.T) {
	claimed := fakeFrontend{authz.Claimed}
	proved := fakeFrontend{authz.Proved}

	gated := livenessGated([]frontendAdmins{
		{claimed, []string{"111", "222"}},         // telegram
		{claimed, []string{"900000000000000001"}}, // discord
		{proved, []string{"@admin:example.org"}},  // matrix
		{proved, []string{"web-admin"}},           // web
	})

	for _, id := range []string{"111", "222", "900000000000000001"} {
		if !gated[id] {
			t.Errorf("%q must be liveness-gated: its transport only claims the identity", id)
		}
	}
	for _, id := range []string{"@admin:example.org", "web-admin"} {
		if gated[id] {
			t.Errorf("%q must NOT be liveness-gated: the transport already proved it, and gating it would demand a session that transport cannot satisfy", id)
		}
	}
	if len(gated) != 3 {
		t.Fatalf("gated set has %d entries, want 3: %v", len(gated), gated)
	}
}

// A frontend that is not running contributes nothing, and must not panic.
func TestLivenessGatedSkipsAbsentFrontends(t *testing.T) {
	gated := livenessGated([]frontendAdmins{
		{nil, []string{"111"}},
		{fakeFrontend{authz.Claimed}, []string{"222"}},
	})
	if gated["111"] {
		t.Error("an absent frontend's ids must not be gated")
	}
	if !gated["222"] {
		t.Error("a live Claimed frontend's ids must be gated")
	}
}

// With no Claimed frontend at all the set is empty, which is what main keys on
// to skip installing the session gate entirely. A Proved-only deployment (the
// Matrix-only setup) must not end up with a gate nobody can pass.
func TestLivenessGatedIsEmptyWhenEveryTransportProves(t *testing.T) {
	gated := livenessGated([]frontendAdmins{
		{fakeFrontend{authz.Proved}, []string{"@admin:example.org"}},
		{fakeFrontend{authz.Proved}, []string{"web-admin"}},
	})
	if len(gated) != 0 {
		t.Fatalf("want no gated admins, got %v", gated)
	}
}
