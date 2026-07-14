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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/disgoorg/snowflake/v2"

	"github.com/jeanhaley32/agent-relay/internal/access"
	"github.com/jeanhaley32/agent-relay/internal/approval"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
	"github.com/jeanhaley32/agent-relay/internal/config"
	claudebk "github.com/jeanhaley32/agent-relay/internal/endpoint/claude"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/discord"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/telegram"
	"github.com/jeanhaley32/agent-relay/internal/ipc"
	"github.com/jeanhaley32/agent-relay/internal/relay"
	"github.com/jeanhaley32/agent-relay/internal/scheduler"
	"github.com/jeanhaley32/agent-relay/internal/session"
)

// replyDriftTotal counts turns where the model produced assistant text in
// response to a relay channel event but never called the reply tool - the
// class of bug behind the 2026-07-14 incident (an answer composed entirely
// in the terminal, never sent, invisible to every existing metric since no
// send was ever attempted). Incremented by detect-reply-drift.py, a Claude
// Code Stop hook that scans the session transcript for exactly this pattern;
// see scripts/detect-reply-drift.py.
var replyDriftTotal atomic.Int64

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

	// discordAcc is the Discord frontend's own access manager (own id
	// namespace: Discord snowflakes, not Telegram user ids). It is declared
	// here — ahead of the Discord frontend actually being started below — so
	// the admin gate and /handshake closures below can capture it by
	// reference: mustStartDiscord() assigns into it later, once
	// cfg.Discord.Enabled is known, but both admin commands need to consult
	// whichever access manager(s) are actually live.
	var discordAcc *access.Manager

	// Budget + shared control-plane commands + the admin /handshake command.
	meter := budget.New(cfg.Budget.Tier, nil)
	cmds := relay.StandardCommands(meter)
	// Central admin gating for all Admin-flagged commands. Consults both the
	// Telegram and (if enabled) Discord access managers — each frontend's
	// admin ids live in its own manager, so a Discord admin's id is only
	// ever found in discordAcc, and would otherwise never satisfy IsAdmin.
	cmds.IsAdmin = func(senderID string) bool {
		id, err := strconv.ParseInt(senderID, 10, 64)
		if err != nil {
			return false
		}
		if acc.IsAdmin(id) {
			return true
		}
		return discordAcc != nil && discordAcc.IsAdmin(id)
	}
	cmds.Register(command.Command{
		Name:  "handshake",
		Help:  "admin: list/approve/deny access requests",
		Admin: true,
		Run: handshake(func() []*access.Manager {
			if discordAcc != nil {
				return []*access.Manager{acc, discordAcc}
			}
			return []*access.Manager{acc}
		}),
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

	// Handshake: verify the token and identify the bot before serving. Retries
	// with backoff for up to ~2 minutes before giving up - a bare Fatalf here
	// made relayd fatally fragile against a boot-time DNS race (this process
	// can start before network-online.target is meaningfully ready,
	// especially as a systemd --user unit where that target doesn't gate
	// anything), even though getUpdates() below already retries forever.
	// Still fails fast with a clear message for a genuinely bad token/config -
	// just not on the very first attempt.
	var info telegram.BotInfo
	handshakeDeadline := time.Now().Add(2 * time.Minute)
	backoff := time.Second
	for {
		hctx, hcancel := context.WithTimeout(context.Background(), 15*time.Second)
		info, err = front.Me(hctx)
		hcancel()
		if err == nil {
			break
		}
		if time.Now().After(handshakeDeadline) {
			logger.Fatalf("bot connection failed after retrying for 2m (check the %s env var): %v", cfg.Telegram.TokenEnv, err)
		}
		logger.Printf("bot handshake failed, retrying in %s: %v", backoff, err)
		time.Sleep(backoff)
		if backoff < 15*time.Second {
			backoff *= 2
		}
	}
	logger.Printf("connected to Telegram as @%s (bot id %d)", info.Username, info.ID)

	// Discord frontend (optional, DESIGN.md §9): the endpoint has existed on
	// disk since the previous PR but nothing constructed/started it, so
	// discord.enabled=true validated cleanly and then did nothing — a config
	// that silently ran no Discord frontend at all. Wired here the same way
	// Telegram is: its own access manager (converted to snowflake ids), its
	// own New/Connect handshake, then fanned into the Broker's single
	// Frontend slot via relay.MultiFrontend alongside Telegram.
	frontendEndpoint := relay.Endpoint(front)
	var discordFront *discord.Frontend
	// discordAcc itself is declared earlier (near cmds.IsAdmin) so the admin
	// gate and /handshake closures can capture it by reference; assign into
	// it here rather than redeclaring, or those closures would keep seeing
	// the original nil.
	if cfg.Discord.Enabled {
		discordFront, discordAcc = mustStartDiscord(cfg, logger)
		frontendEndpoint = relay.NewMultiFrontend(front, discordFront)
	}

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
	// adminChatID is the target for direct-to-Jean escalations and ack receipts.
	// (Also used by the Grafana webhook below.)
	adminChatID := ""
	if len(cfg.Telegram.Admins) > 0 {
		adminChatID = strconv.FormatInt(cfg.Telegram.Admins[0], 10)
	}

	// Pending-event tracker: follows every fired trigger until the agent
	// acknowledges it, escalating (re-inject → direct message to Jean) if it is
	// silently buried in a busy session. inject reports whether the frame
	// reached a live shim (vs was buffered while disconnected); fallback and
	// receipt reach Jean directly via the frontend.
	eventsPath := cfg.Scheduler.File + ".events"
	inject := func(chatID, text string) bool {
		_ = back.Send(context.Background(), relay.Message{
			ConversationID: chatID, Role: relay.User, Text: text,
			Meta: map[string]string{"chat_id": chatID, "scheduled": "1"},
		})
		return back.Connected()
	}
	// sendToAdmin returns the send error so the tracker's fallback path can
	// avoid marking a failed last-line-of-defense escalation as delivered.
	sendToAdmin := func(text string) error {
		if adminChatID == "" {
			return nil
		}
		return front.Send(context.Background(), relay.Message{
			ConversationID: adminChatID, Text: text,
			Meta: map[string]string{"chat_id": adminChatID},
		})
	}
	// receipt is fire-and-forget (a best-effort audit ping), so it discards the
	// error; the fallback path uses the error-returning form directly.
	sendReceipt := func(text string) { _ = sendToAdmin(text) }
	tracker, err := scheduler.NewTracker(eventsPath, inject, sendToAdmin, sendReceipt, scheduler.TrackerConfig{}, logger)
	if err != nil {
		logger.Fatalf("event tracker: %v", err)
	}
	defer tracker.Close()

	sched, err := scheduler.New(cfg.Scheduler.File, loc, func(scheduleID, chatID, text string) error {
		// Record a pending event (persisted first) and inject it; the tracker
		// follows it to acknowledgment and escalates if it is ignored. An error
		// here means the event was NOT durably recorded — return it so the
		// scheduler keeps a one-shot schedule for retry instead of deleting it.
		if _, err := tracker.Fire(scheduleID, chatID, text); err != nil {
			logger.Printf("event tracker fire: %v", err)
			return err
		}
		return nil
	}, logger)
	if err != nil {
		logger.Fatalf("scheduler: %v", err)
	}
	defer sched.Close()

	// Tailnet-bound approval flow for high-risk actions: a loopback-only
	// /request+/status API this agent calls (via curl) to ask for a human
	// decision, and a Tailscale-interface-only /approve page the request
	// link points at. Being able to load the approve page at all is proof
	// of tailnet membership - stronger than trusting a Telegram chat_id
	// alone, which is spoofable if that account is ever compromised. See
	// 2026-07-10 relay conversation with Jean for the design rationale.
	appr := approval.NewManager("http://100.99.212.119:9212")
	reqListener, err := net.Listen("tcp", "127.0.0.1:9211")
	if err != nil {
		logger.Fatalf("approval request listener: %v", err)
	}
	go func() {
		if err := http.Serve(reqListener, appr.RequestHandler()); err != nil {
			logger.Printf("approval request server: %v", err)
		}
	}()
	// The Tailscale interface address may not be assigned yet this early at
	// boot (tailscaled racing relayd) - retry with backoff instead of
	// silently running with a broken gate, which would permanently lock the
	// admin out of Telegram control (the re-auth link would 404 forever).
	appListener, err := listenWithRetry("tcp", "100.99.212.119:9212", 30*time.Second, logger)
	if err != nil {
		logger.Fatalf("approval page listener: %v", err)
	}
	go func() {
		if err := http.Serve(appListener, appr.ApproveHandler()); err != nil {
			logger.Printf("approval page server: %v", err)
		}
	}()

	// Service schedule-tool and event-tool calls coming from the model (via the shim).
	go serveSchedules(back, sched, tracker, logger)

	b := &relay.Broker{Frontend: frontendEndpoint, Backend: back, Commands: cmds, Meter: meter,
		ConversationCaps: cfg.Budget.ConversationCaps}

	// Grafana alert webhook: alerts are routed through the model backend
	// (same injection path as scheduled triggers), not straight to Telegram -
	// so alerts get judgment/context applied before they reach the user,
	// instead of Grafana paging directly. Loopback-only: Grafana runs on
	// this same host, no need to expose it beyond localhost. Also hosts
	// /webhook/reply-drift and /webhook/token-usage, hence needing b now
	// that it exists.
	go serveGrafanaWebhook(back, adminChatID, acc, meter, front, discordFront, tracker, logger, b)

	// Reply-inferred acknowledgment: a model reply landing on a chat after a
	// trigger fired there is strong evidence the trigger was handled, so
	// auto-resolve any still-open events for that chat. Supplements ack_event.
	b.OnBackendReply = func(m relay.Message) {
		chatID := m.Meta["chat_id"]
		if chatID == "" {
			chatID = m.ConversationID
		}
		if chatID != "" {
			tracker.NoteReply(chatID, time.Now())
		}
	}
	// Surface the real Send outcome back to the reply tool call that
	// originated it (by RequestID, carried in Meta["reply_id"]), instead of
	// the tool call always returning "sent" regardless of what actually
	// happened - real incident 2026-07-11, see AckBackendReply's doc comment.
	b.AckBackendReply = func(m relay.Message, sendErr error) {
		reqID := m.Meta["reply_id"]
		if reqID == "" {
			return // reply carries no correlation id (e.g. an internal/system reply) - nothing waiting
		}
		errText := ""
		if sendErr != nil {
			errText = sendErr.Error()
		}
		if err := back.ReplyRespond(reqID, errText); err != nil {
			logger.Printf("reply ack for %s: %v", reqID, err)
		}
	}
	// Outbound gate: the model can only reply to allowlisted chats. The inbound
	// allowlist gates who reaches Claude; this stops Claude messaging strangers.
	// Checks both the Telegram (int64) and, when enabled, Discord (snowflake)
	// allowlists — chatID is a Telegram chat id (== user id) or, for Discord,
	// gate()'s convID (== user id for DMs, == channel id for guild messages).
	// Guild channels are inherently multi-party so acc-style single-id
	// allowlisting doesn't apply there; instead we allow any chatID the
	// Discord frontend has already seen and gated inbound (KnownConversation)
	// — i.e. a guild channel from an allowed guild, or a DM user id. That
	// covers scheduled reminders / relayd-originated replies into a channel
	// the model was legitimately talking in, while still failing closed for
	// anything never seen inbound.
	b.OutboundAllowed = func(chatID string) bool {
		if id, err := strconv.ParseInt(chatID, 10, 64); err == nil && acc.Allowed(id) {
			return true
		}
		if discordAcc != nil {
			if id, err := snowflake.Parse(chatID); err == nil && discordAcc.Allowed(int64(id)) {
				return true
			}
		}
		if discordFront != nil && discordFront.KnownConversation(chatID) {
			return true
		}
		logger.Printf("blocked outbound reply to non-allowlisted chat %q", chatID)
		return false
	}

	cmds.Register(command.Command{
		Name:  "lockdown",
		Help:  "admin: /lockdown on|off - block all non-admin senders from reaching the model",
		Admin: true,
		Run: func(_ command.Context, args []string) string {
			if len(args) == 0 {
				if b.Lockdown.Load() {
					return "lockdown is ON"
				}
				return "lockdown is OFF"
			}
			switch args[0] {
			case "on":
				b.Lockdown.Store(true)
				return "🔒 lockdown ON - non-admin senders are now blocked"
			case "off":
				b.Lockdown.Store(false)
				return "🔓 lockdown OFF - non-admin senders can message normally again"
			default:
				return "usage: /lockdown on|off"
			}
		},
	})

	// Admin session gate: every admin user_id (from_id) must re-prove
	// tailnet presence (via the approval page) after 30 min idle, closing
	// the gap where a compromised messenger account alone would otherwise be
	// trusted. Keyed on user_id, not chat_id - see SessionGatedUsers doc
	// comment in internal/relay/relay.go. Tracked independently per admin.
	// Covers both Telegram admins (int64 ids) and, when enabled, Discord
	// admins (snowflake ids) - the gate is the guard against a compromised
	// admin account on EITHER frontend, so both id spaces must feed it or a
	// Discord admin could run /handshake approve, /allow, /lockdown etc
	// indefinitely with zero tailnet proof.
	if len(cfg.Telegram.Admins) > 0 || len(cfg.Discord.Admins) > 0 {
		gated := make(map[string]bool, len(cfg.Telegram.Admins)+len(cfg.Discord.Admins))
		for _, admin := range cfg.Telegram.Admins {
			gated[strconv.FormatInt(admin, 10)] = true
		}
		if discordAdminIDs, err := cfg.Discord.AdminIDs(); err == nil {
			for _, admin := range discordAdminIDs {
				gated[admin.String()] = true
			}
		}
		b.Session = session.NewManager(30 * time.Minute)
		b.Approval = appr
		b.SessionGatedUsers = gated
		b.SessionTTL = 10 * time.Minute

		cmds.Register(command.Command{
			Name:  "reauth",
			Help:  "admin: force every admin session (including yours) to expire, requiring tailnet re-approval",
			Admin: true,
			Run: func(command.Context, []string) string {
				b.Session.ExpireAll()
				return "All admin sessions expired. Your next message (including this reply's delivery) will trigger a tailnet re-auth challenge."
			},
		})
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
		_ = frontendEndpoint.Close() // closes the frontend Recv -> Broker.Run returns
		cancel()
	}()

	if err := b.Run(ctx); err != nil {
		logger.Fatalf("broker: %v", err)
	}
	logger.Printf("stopped")
}

// mustStartDiscord builds the Discord frontend from cfg.Discord, connects
// its gateway, and returns it along with the access.Manager backing its
// allowlist (so the caller can also consult it for outbound gating).
// Fatal on error, mirroring the Telegram handshake's posture: an operator
// who set discord.enabled=true with a bad token/config wants a clear
// startup failure, not a frontend that silently never came up (the exact
// gap this function exists to close - see DESIGN.md §9 / finding 3).
func mustStartDiscord(cfg *config.Config, logger *log.Logger) (*discord.Frontend, *access.Manager) {
	token, err := cfg.DiscordToken()
	if err != nil {
		logger.Fatalf("discord: %v", err)
	}
	adminIDs, err := cfg.Discord.AdminIDs()
	if err != nil {
		logger.Fatalf("discord: %v", err)
	}
	allowIDs, err := cfg.Discord.AllowlistIDs()
	if err != nil {
		logger.Fatalf("discord: %v", err)
	}
	guildIDs, err := cfg.Discord.AllowedGuildSnowflakes()
	if err != nil {
		logger.Fatalf("discord: %v", err)
	}

	// access.Manager is int64-keyed and platform-agnostic by design; Discord
	// snowflakes are uint64 but real values are time-based and well under
	// math.MaxInt64, so the conversion is lossless in practice (same
	// reasoning as discord.Int64Authorizer's doc comment).
	toInt64 := func(ids []snowflake.ID) []int64 {
		out := make([]int64, len(ids))
		for i, id := range ids {
			out[i] = int64(id)
		}
		return out
	}
	discordAcc := access.New(toInt64(adminIDs), toInt64(allowIDs), cfg.Discord.AllowlistFile, logger)

	front, err := discord.New(token,
		discord.WithAuthorizer(discord.Int64Authorizer(discordAcc)),
		discord.WithLogger(logger),
		discord.WithAllowGuildMessages(cfg.Discord.AllowGuildMessages),
		discord.WithAllowedGuildIDs(guildIDs...),
		discord.WithRequireMentionInGuild(cfg.Discord.RequireMentionInGuild()),
	)
	if err != nil {
		logger.Fatalf("discord: %v", err)
	}

	ctx := context.Background()
	if err := front.Connect(ctx); err != nil {
		logger.Fatalf("discord: connect: %v", err)
	}
	logger.Printf("connected to Discord (admins=%d, allowlist=%d, guild_messages=%v)",
		len(adminIDs), len(allowIDs), cfg.Discord.AllowGuildMessages)
	return front, discordAcc
}

// listenWithRetry binds addr, retrying with a fixed 1s backoff until
// deadline elapses. Used for listeners bound to an interface address (like
// the Tailscale IP) that may not exist yet this early at boot.
func listenWithRetry(network, addr string, deadline time.Duration, logger *log.Logger) (net.Listener, error) {
	giveUp := time.Now().Add(deadline)
	var lastErr error
	for {
		l, err := net.Listen(network, addr)
		if err == nil {
			return l, nil
		}
		lastErr = err
		if time.Now().After(giveUp) {
			return nil, lastErr
		}
		logger.Printf("listen %s: %v, retrying...", addr, err)
		time.Sleep(1 * time.Second)
	}
}

// grafanaWebhookPayload is the subset of Grafana's unified-alerting webhook
// contact-point JSON we actually use. See:
// https://grafana.com/docs/grafana/latest/alerting/configure-notifications/manage-contact-points/integrations/webhook-notifier/
type grafanaWebhookPayload struct {
	Status string `json:"status"`
	Alerts []struct {
		Status      string            `json:"status"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"alerts"`
}

// serveGrafanaWebhook listens on loopback-only for Grafana's alert webhook
// and injects each alert into the Claude session via the same buffered-inject
// path the scheduler uses (back.Send), rather than notifying Telegram
// directly - so the model applies judgment/context before anything reaches
// the user, instead of Grafana paging around it.
func serveGrafanaWebhook(back *claudebk.Endpoint, adminChatID string, acc *access.Manager, meter *budget.Meter, front *telegram.Frontend, discordFront *discord.Frontend, tracker *scheduler.Tracker, logger *log.Logger, b *relay.Broker) {
	if adminChatID == "" {
		logger.Printf("grafana webhook: no admin chat configured, not starting listener")
		return
	}
	mux := http.NewServeMux()
	// Real gaps closed 2026-07-09, both requested directly by Jean: (1)
	// unauthorized Telegram senders were already queued internally
	// (access.Manager.Record, surfaced only via the /handshake admin
	// command) but nothing alerted proactively; (2) there was no admin
	// visibility into relayd's own budget/circuit-breaker state at all -
	// both now exposed here for the admin dashboard + alert rule.
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := meter.Snapshot()
		stateNum := 0
		switch snap.State {
		case budget.Open:
			stateNum = 1
		case budget.HalfOpen:
			stateNum = 2
		}
		pausedNum := 0
		if snap.Paused {
			pausedNum = 1
		}
		// Discord frontend is optional (cfg.Discord.Enabled) - report zeros
		// rather than omitting the series when it's off, so alert rules
		// that reference these metrics don't need conditional queries.
		var discordSendFailures, discordPermanentDrops, discordQueueDepth int64
		var discordRecvDrops, discordGatewayReconnects, discordLastGatewayEventAt int64
		if discordFront != nil {
			discordSendFailures = discordFront.SendFailures()
			discordPermanentDrops = discordFront.PermanentDrops()
			discordQueueDepth = discordFront.QueueDepth()
			discordRecvDrops = discordFront.RecvDrops()
			discordGatewayReconnects = discordFront.GatewayReconnects()
			discordLastGatewayEventAt = discordFront.LastGatewayEventAt()
		}
		body := fmt.Sprintf(
			"# HELP relayd_unrecognized_access_attempts_total Distinct non-allowlisted Telegram senders who have messaged the bot since relayd started.\n"+
				"# TYPE relayd_unrecognized_access_attempts_total counter\n"+
				"relayd_unrecognized_access_attempts_total %d\n"+
				"# HELP relayd_pending_access_requests Currently unresolved (not yet approved/denied) access requests.\n"+
				"# TYPE relayd_pending_access_requests gauge\n"+
				"relayd_pending_access_requests %d\n"+
				"# HELP relayd_allowlist_size Number of Telegram user ids currently allowlisted.\n"+
				"# TYPE relayd_allowlist_size gauge\n"+
				"relayd_allowlist_size %d\n"+
				"# HELP relayd_budget_percent_used Percent of the rolling token-budget window used.\n"+
				"# TYPE relayd_budget_percent_used gauge\n"+
				"relayd_budget_percent_used %f\n"+
				"# HELP relayd_budget_used_tokens Tokens used in the current rolling window.\n"+
				"# TYPE relayd_budget_used_tokens gauge\n"+
				"relayd_budget_used_tokens %d\n"+
				"# HELP relayd_budget_limit_tokens Configured token limit for the current rolling window.\n"+
				"# TYPE relayd_budget_limit_tokens gauge\n"+
				"relayd_budget_limit_tokens %d\n"+
				"# HELP relayd_budget_window_seconds_left Seconds remaining in the current rolling budget window.\n"+
				"# TYPE relayd_budget_window_seconds_left gauge\n"+
				"relayd_budget_window_seconds_left %f\n"+
				"# HELP relayd_circuit_breaker_state 0=closed (normal), 1=open (tripped, traffic rejected), 2=half-open (probing).\n"+
				"# TYPE relayd_circuit_breaker_state gauge\n"+
				"relayd_circuit_breaker_state %d\n"+
				"# HELP relayd_circuit_breaker_paused 1 if the breaker is manually paused.\n"+
				"# TYPE relayd_circuit_breaker_paused gauge\n"+
				"relayd_circuit_breaker_paused %d\n"+
				"# HELP relayd_telegram_send_failures_total Failed Telegram sendMessage attempts since relayd started (includes retries).\n"+
				"# TYPE relayd_telegram_send_failures_total counter\n"+
				"relayd_telegram_send_failures_total %d\n"+
				"# HELP relayd_telegram_permanent_drops_total Messages that exhausted all retry attempts and were permanently dropped.\n"+
				"# TYPE relayd_telegram_permanent_drops_total counter\n"+
				"relayd_telegram_permanent_drops_total %d\n"+
				"# HELP relayd_telegram_retry_queue_depth Messages currently queued for background retry.\n"+
				"# TYPE relayd_telegram_retry_queue_depth gauge\n"+
				"relayd_telegram_retry_queue_depth %d\n"+
				"# HELP relayd_telegram_getupdates_failures_total Failed getUpdates poll attempts since relayd started - the real signal of a Telegram-side outage.\n"+
				"# TYPE relayd_telegram_getupdates_failures_total counter\n"+
				"relayd_telegram_getupdates_failures_total %d\n"+
				"# HELP relayd_telegram_last_poll_success_seconds Unix timestamp of the last successful getUpdates poll.\n"+
				"# TYPE relayd_telegram_last_poll_success_seconds gauge\n"+
				"relayd_telegram_last_poll_success_seconds %d\n"+
				"# HELP relayd_pending_events_open Currently-open (unacknowledged) fired triggers being followed to completion.\n"+
				"# TYPE relayd_pending_events_open gauge\n"+
				"relayd_pending_events_open %d\n"+
				"# HELP relayd_pending_events_oldest_age_seconds Age of the oldest still-open pending event in seconds (0 if none open).\n"+
				"# TYPE relayd_pending_events_oldest_age_seconds gauge\n"+
				"relayd_pending_events_oldest_age_seconds %f\n"+
				"# HELP relayd_discord_send_failures_total Failed Discord message-send attempts since relayd started (includes retries). 0 if the Discord frontend is disabled.\n"+
				"# TYPE relayd_discord_send_failures_total counter\n"+
				"relayd_discord_send_failures_total %d\n"+
				"# HELP relayd_discord_permanent_drops_total Discord messages that exhausted all retry attempts and were permanently dropped.\n"+
				"# TYPE relayd_discord_permanent_drops_total counter\n"+
				"relayd_discord_permanent_drops_total %d\n"+
				"# HELP relayd_discord_retry_queue_depth Discord messages currently queued for background retry.\n"+
				"# TYPE relayd_discord_retry_queue_depth gauge\n"+
				"relayd_discord_retry_queue_depth %d\n"+
				"# HELP relayd_discord_recv_drops_total Inbound Discord gateway events dropped without being relayed since relayd started - the Discord analogue of a getUpdates outage.\n"+
				"# TYPE relayd_discord_recv_drops_total counter\n"+
				"relayd_discord_recv_drops_total %d\n"+
				"# HELP relayd_discord_gateway_reconnects_total Discord gateway reconnects since relayd started.\n"+
				"# TYPE relayd_discord_gateway_reconnects_total counter\n"+
				"relayd_discord_gateway_reconnects_total %d\n"+
				"# HELP relayd_discord_last_gateway_event_seconds Unix timestamp of the last event received from the Discord gateway (0 if the Discord frontend is disabled or has seen no events yet).\n"+
				"# TYPE relayd_discord_last_gateway_event_seconds gauge\n"+
				"relayd_discord_last_gateway_event_seconds %d\n"+
				"# HELP relayd_reply_drift_total Turns where the model answered a relay event in plain terminal text without calling the reply tool, so nothing was ever sent - detected by a Stop hook scanning the transcript, see scripts/detect-reply-drift.py.\n"+
				"# TYPE relayd_reply_drift_total counter\n"+
				"relayd_reply_drift_total %d\n",
			acc.TotalRecorded(), len(acc.Pending()), len(acc.Allowlist()),
			snap.PercentUsed, snap.Used, snap.Limit, snap.WindowLeft.Seconds(),
			stateNum, pausedNum,
			front.SendFailures(), front.PermanentDrops(), front.QueueDepth(),
			front.GetUpdatesFailures(), front.LastPollSuccess(),
			tracker.OpenCount(), tracker.OldestOpenAge().Seconds(),
			discordSendFailures, discordPermanentDrops, discordQueueDepth,
			discordRecvDrops, discordGatewayReconnects, discordLastGatewayEventAt,
			replyDriftTotal.Load(),
		)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(body))
	})
	// /webhook/reply-drift: called by scripts/detect-reply-drift.py (a Claude
	// Code Stop hook) when it finds a turn that answered a relay event in
	// plain terminal text without ever calling the reply tool. Loopback-only,
	// same as every other endpoint on this mux - this process's threat model
	// already assumes localhost is trusted. Just increments a counter; see
	// replyDriftTotal's doc comment for why this stays observability-only
	// rather than auto-forwarding the orphaned text (Jean's call, 2026-07-14:
	// auto-forward risks sending unintended draft/mid-thought text).
	mux.HandleFunc("/webhook/reply-drift", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		replyDriftTotal.Add(1)
		logger.Printf("reply drift detected: model answered a relay event in terminal text without calling the reply tool")
		w.WriteHeader(http.StatusOK)
	})
	// /webhook/token-usage: called by scripts/token-usage-hook.py (a Claude
	// Code Stop hook) with real per-conversation token usage computed from
	// the session transcript's own Claude-API usage data - replacing the
	// interim chars/4 text-length estimate the Broker uses live between hook
	// runs. Jean's explicit request, 2026-07-14: the estimate was found to
	// undercount real usage by roughly 2-3x for a capped conversation. Body:
	// {"usage": {"<chat_id>": <int64 tokens>, ...}} - a batch of every
	// currently-capped conversation's real usage in one call, since the hook
	// recomputes attribution for the whole transcript each run anyway.
	// SetConversationUsage is a no-op for any chat_id without a configured
	// cap, so this endpoint can't be used to inject usage for an uncapped
	// conversation.
	mux.HandleFunc("/webhook/token-usage", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			Usage map[string]int64 `json:"usage"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			logger.Printf("token-usage webhook: bad payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for chatID, tokens := range payload.Usage {
			b.SetConversationUsage(chatID, tokens)
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/webhook/grafana", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var payload grafanaWebhookPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			logger.Printf("grafana webhook: bad payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for _, a := range payload.Alerts {
			name := a.Labels["alertname"]
			severity := a.Labels["severity"]
			summary := a.Annotations["summary"]
			prompt := fmt.Sprintf(
				"[Grafana alert, status=%s, severity=%s] %s: %s\n\n"+
					"Use your judgment on how urgently to surface this to the user via the "+
					"reply tool (chat_id=\"%s\") - a firing critical alert probably warrants an "+
					"immediate message, a resolved one may just be worth a brief note, and if "+
					"you're already mid-investigation on the same subsystem you can fold it into "+
					"that instead of sending a separate ping.",
				a.Status, severity, name, summary, adminChatID,
			)
			_ = back.Send(context.Background(), relay.Message{
				ConversationID: adminChatID, Role: relay.User, Text: prompt,
				Meta: map[string]string{"chat_id": adminChatID, "grafana_alert": "1"},
			})
		}
		w.WriteHeader(http.StatusOK)
	})
	addr := "127.0.0.1:9210"
	logger.Printf("grafana webhook listening on %s/webhook/grafana", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Printf("grafana webhook server stopped: %v", err)
	}
}

// serveSchedules answers schedule-tool calls from the model: create/list/cancel
// against the scheduler, replying with a human-readable result the model relays.
func serveSchedules(back *claudebk.Endpoint, sched *scheduler.Scheduler, tracker *scheduler.Tracker, logger *log.Logger) {
	for req := range back.Schedules() {
		var result, errText string
		switch req.Op {
		case ipc.OpEventAck, ipc.OpEventList:
			result, errText = handleEvent(tracker, req)
		default:
			result, errText = handleSchedule(sched, req)
		}
		if err := back.SchedRespond(req.ReqID, result, errText); err != nil {
			logger.Printf("schedule respond (%s): %v", req.Op, err)
		}
	}
}

// handleEvent performs one pending-event op (ack/list) and returns (result, errText).
func handleEvent(tracker *scheduler.Tracker, req claudebk.SchedRequest) (string, string) {
	switch req.Op {
	case ipc.OpEventAck:
		if err := tracker.Ack(req.SchedID, req.Text); err != nil {
			return "", err.Error()
		}
		return "acknowledged " + req.SchedID, ""
	case ipc.OpEventList:
		list := tracker.ListPending()
		if len(list) == 0 {
			return "no open pending events", ""
		}
		var b strings.Builder
		fmt.Fprintf(&b, "%d open pending event(s):\n", len(list))
		for _, ev := range list {
			nudged := "never"
			if !ev.LastNudgeAt.IsZero() {
				nudged = ev.LastNudgeAt.Format("15:04:05 MST")
			}
			fmt.Fprintf(&b, "  %s — fired %s, last nudge %s — %q\n",
				ev.ID, ev.FiredAt.Format("Mon 15:04:05 MST"), nudged, ev.Text)
		}
		return strings.TrimRight(b.String(), "\n"), ""
	default:
		return "", "unknown event op: " + req.Op
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
//
// managers() returns every access manager currently in play (Telegram, plus
// Discord's own if that frontend is enabled) — each frontend has its own id
// namespace and its own pending queue, so a request recorded by one manager
// is otherwise invisible to (and unapprovable from) the other. Listing merges
// all of them; approve/deny try each manager in turn and act on whichever one
// actually has the id pending/denied, so an admin on either frontend can
// resolve any request without knowing which frontend it came from.
func handshake(managers func() []*access.Manager) command.Handler {
	const maxList = 20 // cap the listing so it stays under Telegram's message limit
	return func(_ command.Context, args []string) string {
		if len(args) == 0 {
			var pend []access.Request
			for _, acc := range managers() {
				pend = append(pend, acc.Pending()...)
			}
			if len(pend) == 0 {
				return "no pending requests"
			}
			sort.Slice(pend, func(i, j int) bool { return pend[i].FirstSeen.Before(pend[j].FirstSeen) })
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
			for _, acc := range managers() {
				if acc.Approve(id) {
					return fmt.Sprintf("✅ approved %d", id)
				}
			}
			return fmt.Sprintf("%d is not pending or denied — not approved (use an id from /handshake)", id)
		case "deny":
			for _, acc := range managers() {
				if acc.Deny(id) {
					return fmt.Sprintf("denied %d", id)
				}
			}
			return fmt.Sprintf("%d was not pending", id)
		default:
			return "usage: /handshake [approve|deny] <id>"
		}
	}
}
