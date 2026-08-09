// Package contacts is a relayd-maintained directory of platform identities
// (one record per {platform, chat_id}) and optional person groups linking
// multiple identities together. It exists to replace hardcoded chat_ids in
// schedules and replies with resolvable names like "discord.jeanh32" or
// "person:jean" — see ~/vessel-log/notes/relay-contacts-protocol.md for the
// full design.
//
// Record creation/observation (last_seen, display_name, message_count) is
// fully automatic, driven by inbound traffic. Everything else — new person
// groups, linking an identity into one, renames, deletions — is a deliberate
// API call the daemon only exposes to admin-issued actions; nothing here
// infers that two differently-named identities are the same person.
package contacts

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Identity is one platform-specific record: the base unit of the directory.
type Identity struct {
	Platform     string    `json:"platform"`
	ChatID       string    `json:"chat_id"`
	GuildID      string    `json:"guild_id,omitempty"`
	Alias        string    `json:"alias"`
	DisplayName  string    `json:"display_name"`
	Role         string    `json:"role,omitempty"`      // "admin" / "non-admin", informational only
	LinkedTo     string    `json:"linked_to,omitempty"` // person group name, or ""
	FirstSeen    time.Time `json:"first_seen"`
	LastSeen     time.Time `json:"last_seen"`
	MessageCount int64     `json:"message_count"`
}

func (id Identity) key() string { return id.Platform + "\x00" + id.ChatID }

// aliasKey returns the resolvable "<platform>.<alias>" form.
func (id Identity) aliasKey() string { return id.Platform + "." + id.Alias }

// Person is an explicit grouping of already-existing identities. Never
// inferred — always created/modified by a deliberate Link/Unlink call.
type Person struct {
	Name    string   `json:"person"`
	Default string   `json:"default,omitempty"` // "<platform>.<alias>" of the preferred identity
	Members []string `json:"members"`           // "<platform>.<alias>" keys
}

// Directory is the concurrency-safe, JSON-persisted store of identities and
// person groups.
type Directory struct {
	mu         sync.RWMutex
	identities map[string]*Identity // key: platform\x00chat_id
	byAlias    map[string]*Identity // key: "<platform>.<alias>"
	people     map[string]*Person   // key: person name

	savePath string
	saveMu   sync.Mutex
	logger   *log.Logger
	now      func() time.Time
}

type persisted struct {
	Identities []*Identity `json:"identities"`
	People     []*Person   `json:"people"`
}

// New builds a Directory, loading savePath if non-empty and present.
func New(savePath string, logger *log.Logger) *Directory {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	d := &Directory{
		identities: map[string]*Identity{},
		byAlias:    map[string]*Identity{},
		people:     map[string]*Person{},
		savePath:   savePath,
		logger:     logger,
		now:        time.Now,
	}
	d.load()
	return d
}

// Observe records (or updates) a platform identity from a live inbound
// message. This is the only fully-automatic write path: it never sets
// LinkedTo, never invents a person group, and never overwrites an
// admin-set alias once the identity already exists — new identities get an
// alias derived from displayName (sanitized), existing ones keep theirs.
func (d *Directory) Observe(platform, chatID, guildID, displayName string) {
	if platform == "" || chatID == "" {
		return
	}
	d.mu.Lock()
	k := platform + "\x00" + chatID
	id, ok := d.identities[k]
	now := d.now()
	if !ok {
		alias := d.uniqueAliasLocked(platform, sanitizeAlias(displayName, chatID))
		id = &Identity{
			Platform:    platform,
			ChatID:      chatID,
			GuildID:     guildID,
			Alias:       alias,
			DisplayName: displayName,
			FirstSeen:   now,
		}
		d.identities[k] = id
		d.byAlias[id.aliasKey()] = id
	}
	if guildID != "" {
		id.GuildID = guildID
	}
	if displayName != "" {
		id.DisplayName = displayName
	}
	id.LastSeen = now
	id.MessageCount++
	d.mu.Unlock()
	d.save()
}

// Resolve turns a name into a live chat_id. Accepted forms:
//   - "<platform>.<alias>" — resolves that identity directly
//   - "person:<name>" — resolves via the person's Default, else its most
//     recently-active member
//   - anything else (a raw chat_id, or an unrecognized name) — returned
//     unchanged with ok=false, so callers fall back to treating it as a
//     literal chat_id (backward compatible with pre-directory callers)
func (d *Directory) Resolve(name string) (chatID string, ok bool) {
	if name == "" {
		return name, false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	if strings.HasPrefix(name, "person:") {
		p, ok := d.people[strings.TrimPrefix(name, "person:")]
		if !ok {
			return name, false
		}
		target := p.Default
		if target == "" {
			target = mostRecentMember(p, d.byAlias)
		}
		if id, ok := d.byAlias[target]; ok {
			return id.ChatID, true
		}
		return name, false
	}

	if id, ok := d.byAlias[name]; ok {
		return id.ChatID, true
	}
	return name, false
}

// Lookup returns a copy of the identity record for (platform, chatID), if
// known. Callers that only need LinkedTo (e.g. to resolve a canonical
// person key) should use this rather than Resolve, which goes the other
// direction (name -> chat_id).
func (d *Directory) Lookup(platform, chatID string) (Identity, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	id, ok := d.identities[platform+"\x00"+chatID]
	if !ok {
		return Identity{}, false
	}
	return *id, true
}

func mostRecentMember(p *Person, byAlias map[string]*Identity) string {
	var best string
	var bestSeen time.Time
	for _, m := range p.Members {
		if id, ok := byAlias[m]; ok && id.LastSeen.After(bestSeen) {
			best, bestSeen = m, id.LastSeen
		}
	}
	return best
}

// Link creates or updates a person group so it includes the given
// "<platform>.<alias>" member keys. Deliberate/admin-only by convention —
// the daemon should only expose this through an explicit admin action, never
// call it from the automatic Observe path. Returns an error if a member is
// unknown or already linked to a different person.
func (d *Directory) Link(person string, defaultMember string, members ...string) error {
	d.mu.Lock()
	defer func() { d.mu.Unlock(); d.save() }()

	for _, m := range members {
		id, ok := d.byAlias[m]
		if !ok {
			return fmt.Errorf("contacts: unknown identity %q", m)
		}
		if id.LinkedTo != "" && id.LinkedTo != person {
			return fmt.Errorf("contacts: %q is already linked to person %q", m, id.LinkedTo)
		}
	}
	p, ok := d.people[person]
	if !ok {
		p = &Person{Name: person}
		d.people[person] = p
	}
	if defaultMember != "" {
		p.Default = defaultMember
	}
	seen := map[string]bool{}
	for _, m := range p.Members {
		seen[m] = true
	}
	for _, m := range members {
		if !seen[m] {
			p.Members = append(p.Members, m)
			seen[m] = true
		}
		d.byAlias[m].LinkedTo = person
	}
	return nil
}

// Unlink removes member from its person group (if any), leaving the
// underlying identity record untouched.
func (d *Directory) Unlink(member string) {
	d.mu.Lock()
	defer func() { d.mu.Unlock(); d.save() }()

	id, ok := d.byAlias[member]
	if !ok || id.LinkedTo == "" {
		return
	}
	p, ok := d.people[id.LinkedTo]
	if ok {
		for i, m := range p.Members {
			if m == member {
				p.Members = append(p.Members[:i], p.Members[i+1:]...)
				break
			}
		}
		if p.Default == member {
			p.Default = ""
		}
	}
	id.LinkedTo = ""
}

// List returns all identities, sorted by platform then alias.
func (d *Directory) List() []Identity {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]Identity, 0, len(d.identities))
	for _, id := range d.identities {
		out = append(out, *id)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Platform != out[j].Platform {
			return out[i].Platform < out[j].Platform
		}
		return out[i].Alias < out[j].Alias
	})
	return out
}

// uniqueAliasLocked returns base, or base-2/base-3/... if base is already
// taken on this platform. Caller holds d.mu.
func (d *Directory) uniqueAliasLocked(platform, base string) string {
	candidate := base
	for i := 2; ; i++ {
		if _, taken := d.byAlias[platform+"."+candidate]; !taken {
			return candidate
		}
		candidate = fmt.Sprintf("%s-%d", base, i)
	}
}

func sanitizeAlias(displayName, fallback string) string {
	s := strings.ToLower(strings.TrimSpace(displayName))
	if s == "" {
		s = fallback
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == ' ' || r == '_' || r == '-':
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "user"
	}
	return out
}

// --- persistence (atomic, serialized, logged) -------------------------------

func (d *Directory) load() {
	if d.savePath == "" {
		return
	}
	b, err := os.ReadFile(d.savePath)
	if err != nil {
		if !os.IsNotExist(err) {
			d.logger.Printf("contacts: could not read %s: %v", d.savePath, err)
		}
		return
	}
	_ = os.Chmod(d.savePath, 0o600)

	var p persisted
	if err := json.Unmarshal(b, &p); err != nil {
		d.logger.Printf("contacts: %s is unreadable — directory NOT loaded: %v", d.savePath, err)
		return
	}
	for _, id := range p.Identities {
		cp := *id
		d.identities[cp.key()] = &cp
		d.byAlias[cp.aliasKey()] = &cp
	}
	for _, per := range p.People {
		cp := *per
		d.people[cp.Name] = &cp
	}
	d.logger.Printf("contacts: loaded %d identities, %d person groups from %s", len(d.identities), len(d.people), d.savePath)
}

func (d *Directory) save() {
	if d.savePath == "" {
		return
	}
	d.saveMu.Lock()
	defer d.saveMu.Unlock()

	d.mu.RLock()
	p := persisted{}
	for _, id := range d.identities {
		p.Identities = append(p.Identities, id)
	}
	for _, per := range d.people {
		p.People = append(p.People, per)
	}
	d.mu.RUnlock()

	sort.Slice(p.Identities, func(i, j int) bool { return p.Identities[i].aliasKey() < p.Identities[j].aliasKey() })
	sort.Slice(p.People, func(i, j int) bool { return p.People[i].Name < p.People[j].Name })

	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		d.logger.Printf("contacts: marshal directory: %v", err)
		return
	}
	tmp := d.savePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		d.logger.Printf("contacts: write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, d.savePath); err != nil {
		d.logger.Printf("contacts: rename %s: %v", tmp, err)
	}
}
