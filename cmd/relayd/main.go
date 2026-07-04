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
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/jeanhaley32/agent-relay/internal/access"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
	"github.com/jeanhaley32/agent-relay/internal/config"
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
	if len(cfg.Telegram.Admins) == 0 && len(cfg.Telegram.Allowlist) == 0 {
		logger.Printf("WARNING: no admins or allowlist — all inbound messages will be dropped (fail-closed)")
	}

	// Access manager: allowlist + admins + pending-request queue (persisted).
	acc := access.New(cfg.Telegram.Admins, cfg.Telegram.Allowlist, cfg.Telegram.AllowlistFile)

	// Budget + shared control-plane commands + the admin /handshake command.
	meter := budget.New(cfg.Budget.Tier, nil)
	cmds := relay.StandardCommands(meter)
	cmds.Register(command.Command{
		Name: "handshake",
		Help: "admin: list/approve/deny access requests",
		Run:  handshake(acc),
	})

	// Claude backend: listen on the socket for the shim.
	back, err := claudebk.New(cfg.Claude.Socket)
	if err != nil {
		logger.Fatalf("claude backend: %v", err)
	}
	defer back.Close()

	// Telegram frontend, authorized via the access manager.
	front := telegram.New(token,
		telegram.WithAuthorizer(acc),
		telegram.WithPollTimeout(cfg.Telegram.PollTimeout),
		telegram.WithLogger(logger),
	)

	b := &relay.Broker{Frontend: front, Backend: back, Commands: cmds, Meter: meter}

	logger.Printf("relayd up — tier=%s, socket=%s, allowed=%d sender(s), admins=%d",
		cfg.Budget.Tier, cfg.Claude.Socket, len(acc.Allowlist()), len(cfg.Telegram.Admins))
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

// handshake returns the admin-only /handshake command handler:
//
//	/handshake              list pending access requests
//	/handshake approve <id> grant access to a pending id
//	/handshake deny <id>    drop a pending request
func handshake(acc *access.Manager) command.Handler {
	return func(ctx command.Context, args []string) string {
		sender, _ := strconv.ParseInt(ctx.SenderID, 10, 64)
		if !acc.IsAdmin(sender) {
			return "⛔ not authorized (admins only)"
		}
		if len(args) == 0 {
			pend := acc.Pending()
			if len(pend) == 0 {
				return "no pending requests"
			}
			var b strings.Builder
			b.WriteString("pending requests:\n")
			for _, r := range pend {
				fmt.Fprintf(&b, "  %d — %s\n", r.ID, r.Name)
			}
			b.WriteString("approve with: /handshake approve <id>")
			return strings.TrimRight(b.String(), "\n")
		}
		if len(args) < 2 {
			return "usage: /handshake [approve|deny] <id>"
		}
		id, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return "invalid id: " + args[1]
		}
		switch args[0] {
		case "approve":
			if acc.Approve(id) {
				return fmt.Sprintf("✅ approved %d", id)
			}
			return fmt.Sprintf("approved %d (was not pending)", id)
		case "deny":
			if acc.Deny(id) {
				return fmt.Sprintf("denied %d", id)
			}
			return fmt.Sprintf("%d was not pending", id)
		default:
			return "usage: /handshake [approve|deny] <id>"
		}
	}
}
