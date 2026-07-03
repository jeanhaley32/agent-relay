// Command broker-demo is PoC-2: a runnable demonstration of the relay control
// plane with no real model attached. A CLI frontend feeds a Broker wired to an
// echo backend, a budget Meter (rate limit + circuit breaker), and a slash
// command registry. Type messages to see them echoed and metered; use
// /help, /rate, /tier, /pause, /resume, /status to drive the relay.
//
//	go run ./cmd/broker-demo --tier pro
//	printf '/tier pro\n/rate\nhello\n/status\n' | go run ./cmd/broker-demo
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/cli"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/echo"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

func main() {
	tier := flag.String("tier", "pro", "account tier: free|pro|max5|max20")
	flag.Parse()

	meter := budget.New(*tier, nil)

	// Wire the control-plane commands to the live meter.
	cmds := command.NewRegistry()
	cmds.Register(command.Command{Name: "rate", Help: "show usage vs. limit", Run: func([]string) string {
		return renderStatus(meter.Snapshot())
	}})
	cmds.Register(command.Command{Name: "status", Help: "alias for /rate", Run: func([]string) string {
		return renderStatus(meter.Snapshot())
	}})
	cmds.Register(command.Command{Name: "tier", Help: "set account tier: /tier max5", Run: func(a []string) string {
		if len(a) == 0 {
			return "usage: /tier free|pro|max5|max20"
		}
		if err := meter.SetTier(a[0]); err != nil {
			return err.Error()
		}
		return "tier set to " + a[0] + "\n" + renderStatus(meter.Snapshot())
	}})
	cmds.Register(command.Command{Name: "pause", Help: "stop forwarding to the model", Run: func([]string) string {
		meter.Pause()
		return "⏸ relay paused"
	}})
	cmds.Register(command.Command{Name: "resume", Help: "resume forwarding", Run: func([]string) string {
		meter.Resume()
		return "▶ relay resumed"
	}})

	front := cli.New("demo", os.Stdin, os.Stdout)
	back := echo.New()

	b := &relay.Broker{Frontend: front, Backend: back, Commands: cmds, Meter: meter}

	fmt.Printf("broker-demo — tier=%s. type a message, or /help. Ctrl-D to exit.\n", *tier)
	if err := b.Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "broker:", err)
		os.Exit(1)
	}
}

func renderStatus(s budget.Status) string {
	return fmt.Sprintf(
		"tier=%s  used=%d/%d tokens (%.1f%%)  window_left=%s  circuit=%s%s",
		s.Tier, s.Used, s.Limit, s.PercentUsed,
		s.WindowLeft.Round(time.Second), s.State,
		map[bool]string{true: "  [PAUSED]", false: ""}[s.Paused],
	)
}
