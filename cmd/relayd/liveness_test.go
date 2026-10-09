package main

import (
	"context"
	"testing"

	"github.com/jeanhaley32/agent-relay/internal/authz"
	"github.com/jeanhaley32/agent-relay/internal/relay"
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
		{claimed, nil, []string{"111", "222"}},         // telegram
		{claimed, nil, []string{"900000000000000001"}}, // discord
		{proved, nil, []string{"@admin:example.org"}},  // matrix
		{proved, nil, []string{"web-admin"}},           // web
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
		{nil, nil, []string{"111"}},
		{fakeFrontend{authz.Claimed}, nil, []string{"222"}},
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
		{fakeFrontend{authz.Proved}, nil, []string{"@admin:example.org"}},
		{fakeFrontend{authz.Proved}, nil, []string{"web-admin"}},
	})
	if len(gated) != 0 {
		t.Fatalf("want no gated admins, got %v", gated)
	}
}

// Admin escalation used to be built by a function that took a *telegram.Frontend
// and a *discord.Frontend by name, so Matrix and web admins were never targets.
// In a Matrix-only or web-only deployment that silently disabled tool-approval
// prompts, the tracker's escalation, and both webhooks: the model would wait
// forever for an /allow nobody was ever shown. See #79.
func TestAdminTargetsCoverEveryFrontend(t *testing.T) {
	var sent []string
	send := func(dest string) func(context.Context, relay.Message) error {
		return func(context.Context, relay.Message) error {
			sent = append(sent, dest)
			return nil
		}
	}

	targets := adminTargets([]frontendAdmins{
		{fakeFrontend{authz.Claimed}, send("telegram"), []string{"111"}},
		{fakeFrontend{authz.Proved}, send("matrix"), []string{"@admin:example.org"}},
		{fakeFrontend{authz.Proved}, send("web"), []string{"web-admin"}},
	})

	if len(targets) != 3 {
		t.Fatalf("got %d targets, want 3 — an admin on any running frontend must be reachable", len(targets))
	}
	for _, target := range targets {
		if err := target.send(context.Background(), relay.Message{}); err != nil {
			t.Fatalf("send to %q: %v", target.chatID, err)
		}
	}
	for _, want := range []string{"telegram", "matrix", "web"} {
		found := false
		for _, got := range sent {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("no admin escalation reached the %s frontend", want)
		}
	}
}

// A Matrix-only deployment is the case that used to be silently broken.
func TestAdminTargetsInAMatrixOnlyDeployment(t *testing.T) {
	targets := adminTargets([]frontendAdmins{
		{fakeFrontend{authz.Proved}, func(context.Context, relay.Message) error { return nil },
			[]string{"@admin:example.org"}},
	})
	if len(targets) != 1 || targets[0].chatID != "@admin:example.org" {
		t.Fatalf("Matrix-only deployment has no admin escalation path: %+v", targets)
	}
}

// A frontend with no way to send is skipped rather than producing a target
// that panics when something tries to escalate through it.
func TestAdminTargetsSkipsFrontendsWithNoSend(t *testing.T) {
	targets := adminTargets([]frontendAdmins{
		{fakeFrontend{authz.Claimed}, nil, []string{"111"}},
	})
	if len(targets) != 0 {
		t.Fatalf("want no targets, got %+v", targets)
	}
}
