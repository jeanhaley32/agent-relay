// Command relayd is the MVP relay daemon: it wires the Telegram frontend ⇄ the
// broker (slash commands + budget/circuit gate) ⇄ the Claude Code backend, from
// a JSON config file, into a single long-running process.
//
// It runs alongside a Claude Code session: relayd listens on a unix socket; you
// launch Claude with cmd/relay-shim pointed at that socket (via .mcp.json) so
// the two connect. See the startup banner for the exact command.
//
//	export TELEGRAM_BOT_TOKEN=...           # from BotFather
//	go run ./cmd/relayd --config config.json
//	# then, in the repo, with .mcp.json registering relay-shim:
//	claude --dangerously-load-development-channels server:relay
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeanhaley32/agent-relay/internal/config"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	claudebk "github.com/jeanhaley32/agent-relay/internal/endpoint/claude"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/telegram"
	"github.com/jeanhaley32/agent-relay/internal/relay"
)

func main() {
	cfgPath := flag.String("config", "config.json", "path to JSON config file")
	flag.Parse()

	logger := log.New(os.Stderr, "[relayd] ", log.LstdFlags)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		logger.Fatalf("config: %v", err)
	}
	token, err := cfg.Token()
	if err != nil {
		logger.Fatalf("%v", err)
	}
	if len(cfg.Telegram.Allowlist) == 0 {
		logger.Printf("WARNING: telegram allowlist is empty — all inbound messages will be dropped (fail-closed)")
	}

	// Budget + shared control-plane commands.
	meter := budget.New(cfg.Budget.Tier, nil)
	cmds := relay.StandardCommands(meter)

	// Claude backend: listen on the socket for the shim.
	back, err := claudebk.New(cfg.Claude.Socket)
	if err != nil {
		logger.Fatalf("claude backend: %v", err)
	}
	defer back.Close()

	// Telegram frontend.
	front := telegram.New(token,
		telegram.WithAllowlist(cfg.Telegram.Allowlist...),
		telegram.WithPollTimeout(cfg.Telegram.PollTimeout),
		telegram.WithLogger(logger),
	)

	b := &relay.Broker{Frontend: front, Backend: back, Commands: cmds, Meter: meter}

	logger.Printf("relayd up — tier=%s, socket=%s, allowlist=%d sender(s)",
		cfg.Budget.Tier, cfg.Claude.Socket, len(cfg.Telegram.Allowlist))
	logger.Printf("now start Claude so the shim connects:")
	logger.Printf("    claude --dangerously-load-development-channels server:relay")
	logger.Printf("(with .mcp.json registering: relay-shim --socket %s)", cfg.Claude.Socket)

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logger.Printf("shutting down")
		_ = front.Close() // closes the frontend Recv -> Broker.Run returns
		cancel()
	}()

	if err := b.Run(ctx); err != nil {
		logger.Fatalf("broker: %v", err)
	}
	logger.Printf("stopped")
}
