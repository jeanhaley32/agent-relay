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

// Handler runs a command with its whitespace-split arguments and returns the
// reply text to send back over the frontend.
type Handler func(args []string) string

// Command is one registered slash command.
type Command struct {
	Name string // without the leading slash, e.g. "rate"
	Help string // one-line description for /help
	Run  Handler
}

// Registry holds the command set. Not safe for concurrent registration, but
// Dispatch is read-only after setup and safe to call concurrently.
type Registry struct {
	cmds map[string]Command
}

// NewRegistry returns an empty registry with a built-in /help.
func NewRegistry() *Registry {
	r := &Registry{cmds: map[string]Command{}}
	r.Register(Command{Name: "help", Help: "list commands", Run: r.help})
	return r
}

// Register adds or replaces a command.
func (r *Registry) Register(c Command) { r.cmds[c.Name] = c }

// IsCommand reports whether text should be intercepted (starts with "/").
func IsCommand(text string) bool {
	return strings.HasPrefix(strings.TrimSpace(text), "/")
}

// Dispatch handles a command line. handled is false if text is not a command,
// in which case the caller forwards the message to the backend as normal.
func (r *Registry) Dispatch(text string) (reply string, handled bool) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "/") {
		return "", false
	}
	fields := strings.Fields(text[1:]) // drop the slash
	if len(fields) == 0 {
		return r.help(nil), true
	}
	name, args := fields[0], fields[1:]
	c, ok := r.cmds[name]
	if !ok {
		return "unknown command: /" + name + "  (try /help)", true
	}
	return c.Run(args), true
}

// help renders the command list, sorted for stable output.
func (r *Registry) help(_ []string) string {
	names := make([]string, 0, len(r.cmds))
	for n := range r.cmds {
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
