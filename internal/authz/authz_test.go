package authz

import "testing"

// stubDirectory stands in for an access.Manager.
type stubDirectory struct {
	admins  map[int64]bool
	allowed map[int64]bool
}

func (s stubDirectory) IsAdmin(id int64) bool { return s.admins[id] }
func (s stubDirectory) Allowed(id int64) bool { return s.allowed[id] || s.admins[id] }

func testAuthorizer(owns func(string) bool) *Authorizer {
	telegram := stubDirectory{
		admins:  map[int64]bool{111: true},
		allowed: map[int64]bool{222: true},
	}
	discord := stubDirectory{
		admins:  map[int64]bool{900000000000000001: true},
		allowed: map[int64]bool{900000000000000002: true},
	}
	return New(
		WithNumericDirectory(telegram),
		WithNumericDirectory(discord),
		WithNamedAdmins("@jean:vessel", "web-jean"),
		WithConversationOwnership(owns),
	)
}

// These mirror the characterization tests in cmd/relayd, which pin the
// behaviour this package has to reproduce exactly.
func TestMayAdminMatchesTheBehaviourItReplaces(t *testing.T) {
	a := testAuthorizer(nil)
	cases := []struct {
		name   string
		sender string
		want   bool
	}{
		{"telegram admin", "111", true},
		{"telegram allowed but not admin", "222", false},
		{"discord admin", "900000000000000001", true},
		{"discord allowed but not admin", "900000000000000002", false},
		{"matrix admin id", "@jean:vessel", true},
		{"web conversation id", "web-jean", true},
		{"unknown numeric sender", "999", false},
		{"unknown non-numeric sender", "@nobody:example.org", false},
		{"empty sender", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.MayAdmin(tc.sender); got.Allowed != tc.want {
				t.Fatalf("MayAdmin(%q) = %v (%s), want %v", tc.sender, got.Allowed, got.Reason, tc.want)
			}
		})
	}
}

// The named-admin check must precede the numeric parse. If it did not, every
// Matrix and web admin would be denied, and nothing else would fail.
func TestNamedAdminsAreCheckedBeforeNumericParse(t *testing.T) {
	a := New(WithNamedAdmins("@jean:vessel"))
	if d := a.MayAdmin("@jean:vessel"); !d.Allowed {
		t.Fatalf("a named admin was denied: %s", d.Reason)
	}
}

func TestMayAdminWithNoDirectories(t *testing.T) {
	a := New()
	if d := a.MayAdmin("111"); d.Allowed {
		t.Fatal("no directories configured must deny, not allow")
	}
	// A nil directory is accepted and skipped, which is how a disabled
	// platform is expressed without a conditional at the call site.
	b := New(WithNumericDirectory(nil))
	if d := b.MayAdmin("111"); d.Allowed {
		t.Fatal("a nil directory must not grant admin")
	}
}

func TestMayReceiveMatchesTheBehaviourItReplaces(t *testing.T) {
	owns := func(id string) bool { return id == "!room:vessel" || id == "web-jean" }
	a := testAuthorizer(owns)

	cases := []struct {
		name   string
		chatID string
		want   bool
	}{
		{"telegram admin is allowlisted", "111", true},
		{"telegram allowlisted non-admin", "222", true},
		{"discord allowlisted", "900000000000000002", true},
		{"matrix room owned by a frontend", "!room:vessel", true},
		{"web conv id owned by a frontend", "web-jean", true},
		{"unknown numeric target", "999", false},
		{"unknown non-numeric target", "@nobody:example.org", false},
		{"empty target", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.MayReceive(tc.chatID); got.Allowed != tc.want {
				t.Fatalf("MayReceive(%q) = %v (%s), want %v", tc.chatID, got.Allowed, got.Reason, tc.want)
			}
		})
	}

	// With no ownership func, a room id has nothing to vouch for it.
	noOwner := testAuthorizer(nil)
	if d := noOwner.MayReceive("!room:vessel"); d.Allowed {
		t.Fatal("a room id was allowed with no frontend claiming it")
	}
}

// The rule that SessionGatedUsers plus two undocumented absences used to
// express, as one function.
func TestNeedsLivenessProof(t *testing.T) {
	if !NeedsLivenessProof(Claimed) {
		t.Fatal("a claimed identity must need a liveness proof — this is the whole point of the session gate")
	}
	if NeedsLivenessProof(Proved) {
		t.Fatal("a proved identity must not need one; requiring it would break the Matrix and web paths")
	}
	// An unset Assurance is Claimed, so a frontend that forgets to report
	// one gets the stricter treatment rather than the laxer.
	var unset Assurance
	if !NeedsLivenessProof(unset) {
		t.Fatal("the zero value must fail closed")
	}
}
