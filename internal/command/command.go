// Package command is a self-contained slash-command control plane. The broker
// hands it every inbound frontend message; if the text is a "/command", the
// registry handles it locally and returns a reply, so it never reaches the
// model (zero tokens). Anything not starting with "/" is passed through.
//
// It has no dependency on the rest of the relay — handlers close over whatever
// state they need (a budget.Meter, a backend switch, etc.).
package command

import (
	"sort"
	"strings"
)

// Context carries who/where a command came from, so handlers can gate on the
// sender (e.g. admin-only commands). Fields are strings so the command layer
// stays platform-neutral; the broker fills them from the message Meta.
type Context struct {
	SenderID string // frontend sender id (e.g. Telegram from_id)
	ChatID   string // conversation/chat id
}

// Handler runs a command with the caller context and whitespace-split
// arguments, and returns the reply text to send back over the frontend.
type Handler func(ctx Context, args []string) string

// Command is one registered slash command.
type Command struct {
	Name  string // without the leading slash, e.g. "rate"
	Help  string // one-line description for /help
	Admin bool   // requires the caller to be an admin (and to be in a DM)
	Run   Handler
}

// Registry holds the command set. Not safe for concurrent registration, but
// Dispatch is read-only after setup and safe to call concurrently.
type Registry struct {
	cmds map[string]Command
	// IsAdmin reports whether a sender id (as a string) is an admin. It MUST be
	// set to use Admin-flagged commands: when nil, admin gating fails closed and
	// every Admin command is denied. (The CLI demo opts in with a func returning
	// true.)
	IsAdmin func(senderID string) bool
}

// NewRegistry returns an empty registry with a built-in /help.
func NewRegistry() *Registry {
	r := &Registry{cmds: map[string]Command{}}
	r.Register(Command{Name: "help", Help: "list commands", Run: func(ctx Context, _ []string) string {
		return r.help(ctx)
	}})
	return r
}

// admin reports whether the caller is an admin. Fails closed: a nil predicate
// (no admin system wired) denies all admin commands.
func (r *Registry) admin(senderID string) bool {
	return r.IsAdmin != nil && r.IsAdmin(senderID)
}

// Register adds or replaces a command.
func (r *Registry) Register(c Command) { r.cmds[c.Name] = c }

// IsCommand reports whether text should be intercepted (starts with "/").
func IsCommand(text string) bool {
	return strings.HasPrefix(strings.TrimSpace(text), "/")
}

// Escaped recognizes a backslash-escaped command: a leading `\/` means "send the
// literal /command to the model, don't intercept it here." It returns the
// unescaped text (the leading `\` removed) and true when text is escaped; the
// caller forwards the unescaped text to the backend instead of dispatching.
func Escaped(text string) (unescaped string, ok bool) {
	if strings.HasPrefix(text, `\/`) {
		return text[1:], true // drop the escape → "/..." goes to the model
	}
	return text, false
}

// Dispatch handles a command line. handled is false if text is not a command,
// in which case the caller forwards the message to the backend as normal.
func (r *Registry) Dispatch(ctx Context, text string) (reply string, handled bool) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "/") {
		return "", false
	}
	fields := strings.Fields(text[1:]) // drop the slash
	if len(fields) == 0 {
		return r.help(ctx), true
	}
	name, args := fields[0], fields[1:]
	c, ok := r.cmds[name]
	if !ok {
		return "unknown command: /" + name + "  (try /help)", true
	}
	// Admin commands require the caller to be an admin AND to be in a direct
	// message (chat id == sender id), so admin output never leaks into a group.
	if c.Admin {
		if ctx.ChatID != ctx.SenderID {
			return "admin commands are only available in a direct message to the bot", true
		}
		if !r.admin(ctx.SenderID) {
			return "⛔ not authorized (admins only)", true
		}
	}
	return c.Run(ctx, args), true
}

// help renders the command list, sorted for stable output. Admin commands are
// hidden from non-admins.
func (r *Registry) help(ctx Context) string {
	isAdmin := r.admin(ctx.SenderID)
	names := make([]string, 0, len(r.cmds))
	for n, c := range r.cmds {
		if c.Admin && !isAdmin {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("commands:\n")
	for _, n := range names {
		b.WriteString("  /")
		b.WriteString(n)
		if h := r.cmds[n].Help; h != "" {
			b.WriteString(" — ")
			b.WriteString(h)
		}
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}
