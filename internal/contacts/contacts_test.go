package contacts

import (
	"os"
	"path/filepath"
	"testing"
)

func TestObserveCreatesAndResolves(t *testing.T) {
	d := New("", nil)
	d.Observe("telegram", "555000111", "", "Casey")
	d.Observe("discord", "555000222", "555000999", "caseyq")

	chatID, ok := d.Resolve("telegram.casey")
	if !ok || chatID != "555000111" {
		t.Fatalf("Resolve(telegram.casey) = %q, %v", chatID, ok)
	}
	chatID, ok = d.Resolve("discord.caseyq")
	if !ok || chatID != "555000222" {
		t.Fatalf("Resolve(discord.caseyq) = %q, %v", chatID, ok)
	}

	// Repeated observation must not create a duplicate or change the alias.
	d.Observe("telegram", "555000111", "", "Casey")
	if len(d.List()) != 2 {
		t.Fatalf("expected 2 identities, got %d", len(d.List()))
	}
	for _, id := range d.List() {
		if id.Platform == "telegram" && id.MessageCount != 2 {
			t.Fatalf("expected message_count 2, got %d", id.MessageCount)
		}
	}
}

func TestResolveUnknownFallsBackToLiteral(t *testing.T) {
	d := New("", nil)
	chatID, ok := d.Resolve("555000111")
	if ok || chatID != "555000111" {
		t.Fatalf("Resolve(raw chat_id) = %q, %v; want unchanged, ok=false", chatID, ok)
	}
}

func TestLinkAndResolvePerson(t *testing.T) {
	d := New("", nil)
	d.Observe("telegram", "555000111", "", "Casey")
	d.Observe("discord", "555000222", "", "caseyq")

	if err := d.Link("casey", "telegram.casey", "telegram.casey", "discord.caseyq"); err != nil {
		t.Fatalf("Link: %v", err)
	}
	chatID, ok := d.Resolve("person:casey")
	if !ok || chatID != "555000111" {
		t.Fatalf("Resolve(person:casey) = %q, %v; want default (telegram)", chatID, ok)
	}

	// Linking an already-linked identity to a different person is refused.
	d.Observe("discord", "999", "", "someoneelse")
	if err := d.Link("bob", "", "discord.caseyq"); err == nil {
		t.Fatal("expected error linking an already-linked identity to a different person")
	}
}

func TestLookupReturnsLinkedIdentity(t *testing.T) {
	d := New("", nil)
	d.Observe("telegram", "555000111", "", "Casey")
	d.Observe("discord", "555000222", "", "caseyq")
	if err := d.Link("casey", "telegram.casey", "telegram.casey", "discord.caseyq"); err != nil {
		t.Fatalf("Link: %v", err)
	}

	id, ok := d.Lookup("discord", "555000222")
	if !ok {
		t.Fatal("Lookup(discord, 555000222) not found")
	}
	if id.LinkedTo != "casey" {
		t.Fatalf("LinkedTo = %q, want %q", id.LinkedTo, "casey")
	}

	id, ok = d.Lookup("telegram", "555000111")
	if !ok || id.LinkedTo != "casey" {
		t.Fatalf("Lookup(telegram, 555000111) = %+v, %v; want LinkedTo=casey", id, ok)
	}

	if _, ok := d.Lookup("discord", "999"); ok {
		t.Fatal("Lookup for unknown chat_id should return ok=false")
	}
}

func TestLinkUnknownMemberFails(t *testing.T) {
	d := New("", nil)
	if err := d.Link("casey", "", "telegram.nobody"); err == nil {
		t.Fatal("expected error linking an unknown identity")
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "contacts.json")

	d1 := New(path, nil)
	d1.Observe("telegram", "111", "", "Alice")
	if err := d1.Link("alice", "telegram.alice", "telegram.alice"); err != nil {
		t.Fatalf("Link: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected persisted file: %v", err)
	}

	d2 := New(path, nil)
	chatID, ok := d2.Resolve("telegram.alice")
	if !ok || chatID != "111" {
		t.Fatalf("after reload, Resolve(telegram.alice) = %q, %v", chatID, ok)
	}
	chatID, ok = d2.Resolve("person:alice")
	if !ok || chatID != "111" {
		t.Fatalf("after reload, Resolve(person:alice) = %q, %v", chatID, ok)
	}
}

func TestUniqueAliasOnCollision(t *testing.T) {
	d := New("", nil)
	d.Observe("telegram", "1", "", "Sam")
	d.Observe("telegram", "2", "", "Sam") // same display name, different chat_id

	list := d.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 identities, got %d", len(list))
	}
	if list[0].Alias == list[1].Alias {
		t.Fatalf("expected distinct aliases on collision, both got %q", list[0].Alias)
	}
}
