package command

import "testing"

func TestAdminGating(t *testing.T) {
	r := NewRegistry()
	r.Register(Command{Name: "danger", Admin: true, Run: func(Context, []string) string { return "did it" }})
	r.IsAdmin = func(sender string) bool { return sender == "admin" }

	// Non-admin in a DM (chat==sender) is refused.
	reply, handled := r.Dispatch(Context{SenderID: "bob", ChatID: "bob"}, "/danger")
	if !handled || reply == "did it" {
		t.Fatalf("non-admin should be refused, got %q", reply)
	}

	// Admin in a DM succeeds.
	reply, _ = r.Dispatch(Context{SenderID: "admin", ChatID: "admin"}, "/danger")
	if reply != "did it" {
		t.Fatalf("admin should be allowed, got %q", reply)
	}

	// Admin but NOT in a DM (group: chat != sender) is refused.
	reply, _ = r.Dispatch(Context{SenderID: "admin", ChatID: "group42"}, "/danger")
	if reply == "did it" {
		t.Fatalf("admin command must be DM-only, got %q", reply)
	}
}

func TestHelpHidesAdminFromNonAdmins(t *testing.T) {
	r := NewRegistry()
	r.Register(Command{Name: "danger", Admin: true, Run: func(Context, []string) string { return "" }})
	r.Register(Command{Name: "safe", Run: func(Context, []string) string { return "" }})
	r.IsAdmin = func(sender string) bool { return sender == "admin" }

	nonAdmin, _ := r.Dispatch(Context{SenderID: "bob", ChatID: "bob"}, "/help")
	if contains(nonAdmin, "danger") {
		t.Fatalf("/help must hide admin commands from non-admins: %q", nonAdmin)
	}
	if !contains(nonAdmin, "safe") {
		t.Fatalf("/help should show non-admin commands: %q", nonAdmin)
	}
	admin, _ := r.Dispatch(Context{SenderID: "admin", ChatID: "admin"}, "/help")
	if !contains(admin, "danger") {
		t.Fatalf("/help should show admin commands to admins: %q", admin)
	}
}

// NilIsAdmin (no admin system, e.g. CLI demo) allows admin commands.
func TestNilAdminAllows(t *testing.T) {
	r := NewRegistry()
	r.Register(Command{Name: "danger", Admin: true, Run: func(Context, []string) string { return "ok" }})
	reply, _ := r.Dispatch(Context{}, "/danger")
	if reply != "ok" {
		t.Fatalf("nil IsAdmin should allow (demo mode), got %q", reply)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
