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
	"time"

	"github.com/jeanhaley32/agent-relay/internal/access"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
	"github.com/jeanhaley32/agent-relay/internal/config"
	claudebk "github.com/jeanhaley32/agent-relay/internal/endpoint/claude"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/telegram"
	"github.com/jeanhaley32/agent-relay/internal/ipc"
	"github.com/jeanhaley32/agent-relay/internal/relay"
	"github.com/jeanhaley32/agent-relay/internal/scheduler"
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
	acc := access.New(cfg.Telegram.Admins, cfg.Telegram.Allowlist, cfg.Telegram.AllowlistFile, logger)

	// Budget + shared control-plane commands + the admin /handshake command.
	meter := budget.New(cfg.Budget.Tier, nil)
	cmds := relay.StandardCommands(meter)
	// Central admin gating for all Admin-flagged commands.
	cmds.IsAdmin = func(senderID string) bool {
		id, err := strconv.ParseInt(senderID, 10, 64)
		return err == nil && acc.IsAdmin(id)
	}
	cmds.Register(command.Command{
		Name:  "handshake",
		Help:  "admin: list/approve/deny access requests",
		Admin: true,
		Run:   handshake(acc),
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

	// Handshake: verify the token and identify the bot before serving. Fails
	// fast with a clear message instead of silently long-polling a bad token.
	hctx, hcancel := context.WithTimeout(context.Background(), 15*time.Second)
	info, err := front.Me(hctx)
	hcancel()
	if err != nil {
		logger.Fatalf("bot connection failed (check the %s env var): %v", cfg.Telegram.TokenEnv, err)
	}
	logger.Printf("connected to Telegram as @%s (bot id %d)", info.Username, info.ID)

	// Permission relay: admins approve tool-use prompts via /allow and /deny.
	cmds.Register(command.Command{Name: "allow", Help: "admin: approve a tool request: /allow <id>", Admin: true, Run: verdict(back, true)})
	cmds.Register(command.Command{Name: "deny", Help: "admin: reject a tool request: /deny <id>", Admin: true, Run: verdict(back, false)})

	// Forward Claude's tool-approval prompts to every admin's chat.
	go func() {
		for req := range back.Permissions() {
			msg := fmt.Sprintf("🔐 Claude wants to use %s\n%s\n\napprove: /allow %s   deny: /deny %s",
				req.Tool, req.Detail, req.ID, req.ID)
			for _, admin := range cfg.Telegram.Admins {
				id := strconv.FormatInt(admin, 10)
				_ = front.Send(context.Background(), relay.Message{
					ConversationID: id, Text: msg, Meta: map[string]string{"chat_id": id},
				})
			}
		}
	}()

	// Scheduler: reminders/self-wakeups the model creates via the schedule tools.
	// Firing injects the text back into the Claude session (through the buffered
	// inject path), so a fire that lands while Claude is briefly down waits and
	// delivers on reconnect. The prompt is intentionally general: the text may be
	// a reminder to relay to the user OR a self-wakeup to resume a long task.
	loc := time.Local
	if cfg.Scheduler.TZ != "" {
		if l, err := time.LoadLocation(cfg.Scheduler.TZ); err != nil {
			logger.Printf("scheduler: bad tz %q, using local: %v", cfg.Scheduler.TZ, err)
		} else {
			loc = l
		}
	}
	sched, err := scheduler.New(cfg.Scheduler.File, loc, func(chatID, text string) {
		prompt := "[scheduled trigger you set earlier fired] " + text +
			"\n\nAct on it now. If it is a reminder for the user, deliver it by calling the " +
			"reply tool with chat_id=\"" + chatID + "\". If it is a self-wakeup to resume work, continue that work."
		_ = back.Send(context.Background(), relay.Message{
			ConversationID: chatID, Role: relay.User, Text: prompt,
			Meta: map[string]string{"chat_id": chatID, "scheduled": "1"},
		})
	}, logger)
	if err != nil {
		logger.Fatalf("scheduler: %v", err)
	}
	defer sched.Close()

	// Service schedule-tool calls coming from the model (via the shim).
	go serveSchedules(back, sched, logger)

	b := &relay.Broker{Frontend: front, Backend: back, Commands: cmds, Meter: meter}
	// Outbound gate: the model can only reply to allowlisted chats. The inbound
	// allowlist gates who reaches Claude; this stops Claude messaging strangers.
	b.OutboundAllowed = func(chatID string) bool {
		id, err := strconv.ParseInt(chatID, 10, 64)
		if err != nil || !acc.Allowed(id) {
			logger.Printf("blocked outbound reply to non-allowlisted chat %q", chatID)
			return false
		}
		return true
	}

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

// serveSchedules answers schedule-tool calls from the model: create/list/cancel
// against the scheduler, replying with a human-readable result the model relays.
func serveSchedules(back *claudebk.Endpoint, sched *scheduler.Scheduler, logger *log.Logger) {
	for req := range back.Schedules() {
		result, errText := handleSchedule(sched, req)
		if err := back.SchedRespond(req.ReqID, result, errText); err != nil {
			logger.Printf("schedule respond (%s): %v", req.Op, err)
		}
	}
}

// handleSchedule performs one schedule op and returns (result, errText).
func handleSchedule(sched *scheduler.Scheduler, req claudebk.SchedRequest) (string, string) {
	switch req.Op {
	case ipc.OpScheduleCreate:
		sc, err := sched.Create(req.Text, req.Cron, time.Duration(req.InSeconds)*time.Second, req.ChatID)
		if err != nil {
			return "", err.Error()
		}
		when := describeSchedule(sched, sc)
		return fmt.Sprintf("scheduled (id %s) — %s", sc.ID, when), ""
	case ipc.OpScheduleList:
		list := sched.List()
		if len(list) == 0 {
			return "no active schedules", ""
		}
		var b strings.Builder
		fmt.Fprintf(&b, "%d active schedule(s):\n", len(list))
		for _, sc := range list {
			fmt.Fprintf(&b, "  %s — %s — %q\n", sc.ID, describeSchedule(sched, sc), sc.Text)
		}
		return strings.TrimRight(b.String(), "\n"), ""
	case ipc.OpScheduleCancel:
		if sched.Cancel(req.SchedID) {
			return "cancelled " + req.SchedID, ""
		}
		return "", "no schedule with id " + req.SchedID
	default:
		return "", "unknown schedule op: " + req.Op
	}
}

// describeSchedule renders when a schedule fires, for the model to relay.
func describeSchedule(sched *scheduler.Scheduler, sc *scheduler.Schedule) string {
	next := sched.Next(sc)
	if sc.Recurring() {
		return fmt.Sprintf("recurring %q, next %s", sc.Cron, next.Format("Mon 2006-01-02 15:04 MST"))
	}
	return "once at " + next.Format("Mon 2006-01-02 15:04 MST")
}

// verdict returns an /allow or /deny handler that answers a pending tool-approval
// request by its id. Admin gating is enforced centrally by the registry.
func verdict(back *claudebk.Endpoint, allow bool) command.Handler {
	return func(_ command.Context, args []string) string {
		if len(args) < 1 {
			return "usage: /" + map[bool]string{true: "allow", false: "deny"}[allow] + " <request_id>"
		}
		if err := back.Decide(args[0], allow); err != nil {
			return "error: " + err.Error()
		}
		if allow {
			return "✅ allowed " + args[0]
		}
		return "⛔ denied " + args[0]
	}
}

// handshake returns the admin-only /handshake command handler:
//
//	/handshake              list pending access requests
//	/handshake approve <id> grant access to a pending id
//	/handshake deny <id>    drop a pending request
func handshake(acc *access.Manager) command.Handler {
	const maxList = 20 // cap the listing so it stays under Telegram's message limit
	return func(_ command.Context, args []string) string {
		if len(args) == 0 {
			pend := acc.Pending()
			if len(pend) == 0 {
				return "no pending requests"
			}
			var b strings.Builder
			b.WriteString("pending requests:\n")
			for i, r := range pend {
				if i >= maxList {
					fmt.Fprintf(&b, "  …and %d more\n", len(pend)-maxList)
					break
				}
				fmt.Fprintf(&b, "  %d — %s\n", r.ID, r.Name)
			}
			b.WriteString("approve with: /handshake approve <id>")
			return strings.TrimRight(b.String(), "\n")
		}
		if len(args) < 2 {
			return "usage: /handshake [approve|deny] <id>"
		}
		id, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || id <= 0 {
			return "invalid id: " + args[1]
		}
		switch args[0] {
		case "approve":
			if acc.Approve(id) {
				return fmt.Sprintf("✅ approved %d", id)
			}
			return fmt.Sprintf("%d is not pending or denied — not approved (use an id from /handshake)", id)
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
