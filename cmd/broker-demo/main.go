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

	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/cli"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/echo"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

func main() {
	tier := flag.String("tier", "pro", "account tier: free|pro|max5|max20")
	flag.Parse()

	meter := budget.New(*tier, nil)
	cmds := relay.StandardCommands(meter) // shared control-plane commands
	cmds.IsAdmin = func(string) bool { return true } // demo: the single local operator is admin

	front := cli.New("demo", os.Stdin, os.Stdout)
	back := echo.New()

	b := &relay.Broker{Frontend: front, Backend: back, Commands: cmds, Meter: meter}

	fmt.Printf("broker-demo — tier=%s. type a message, or /help. Ctrl-D to exit.\n", *tier)
	if err := b.Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "broker:", err)
		os.Exit(1)
	}
}
