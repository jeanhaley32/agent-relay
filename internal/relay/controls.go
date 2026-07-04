package relay

import (
	"fmt"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
)

// StandardCommands builds the default slash-command control plane wired to a
// budget Meter: /rate, /status, /tier, /pause, /resume (plus the built-in
// /help). Shared by cmd/broker-demo and cmd/relayd so the operator commands are
// identical everywhere.
func StandardCommands(m *budget.Meter) *command.Registry {
	r := command.NewRegistry()
	r.Register(command.Command{Name: "rate", Help: "show usage vs. limit", Run: func([]string) string {
		return RenderStatus(m.Snapshot())
	}})
	r.Register(command.Command{Name: "status", Help: "alias for /rate", Run: func([]string) string {
		return RenderStatus(m.Snapshot())
	}})
	r.Register(command.Command{Name: "tier", Help: "set account tier: /tier max5", Run: func(a []string) string {
		if len(a) == 0 {
			return "usage: /tier free|pro|max5|max20"
		}
		if err := m.SetTier(a[0]); err != nil {
			return err.Error()
		}
		return "tier set to " + a[0] + "\n" + RenderStatus(m.Snapshot())
	}})
	r.Register(command.Command{Name: "pause", Help: "stop forwarding to the model", Run: func([]string) string {
		m.Pause()
		return "⏸ relay paused"
	}})
	r.Register(command.Command{Name: "resume", Help: "resume forwarding", Run: func([]string) string {
		m.Resume()
		return "▶ relay resumed"
	}})
	return r
}

// RenderStatus formats a budget snapshot for /rate and /status replies.
func RenderStatus(s budget.Status) string {
	paused := ""
	if s.Paused {
		paused = "  [PAUSED]"
	}
	return fmt.Sprintf(
		"tier=%s  used=%d/%d tokens (%.1f%%)  window_left=%s  circuit=%s%s",
		s.Tier, s.Used, s.Limit, s.PercentUsed,
		s.WindowLeft.Round(time.Second), s.State, paused,
	)
}
