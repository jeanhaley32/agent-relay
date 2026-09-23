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
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/disgoorg/snowflake/v2"

	"github.com/jeanhaley32/agent-relay/internal/access"
	"github.com/jeanhaley32/agent-relay/internal/adminbind"
	"github.com/jeanhaley32/agent-relay/internal/approval"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/command"
	"github.com/jeanhaley32/agent-relay/internal/config"
	"github.com/jeanhaley32/agent-relay/internal/contacts"
	"github.com/jeanhaley32/agent-relay/internal/deniedlog"
	claudebk "github.com/jeanhaley32/agent-relay/internal/endpoint/claude"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/discord"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/matrix"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/senderr"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/telegram"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/web"
	"github.com/jeanhaley32/agent-relay/internal/eventlog"
	"github.com/jeanhaley32/agent-relay/internal/ipc"
	"github.com/jeanhaley32/agent-relay/internal/relay"
	"github.com/jeanhaley32/agent-relay/internal/scheduler"
	"github.com/jeanhaley32/agent-relay/internal/session"
	"github.com/jeanhaley32/agent-relay/internal/tailnet"
	"github.com/jeanhaley32/agent-relay/plugins/stylometry"
)

// replyDriftTotal counts turns where the model produced assistant text in
// response to a relay channel event but never called the reply tool - an
// answer composed entirely in the terminal, never sent, invisible to every
// existing metric since no send was ever attempted. Incremented by the
// /webhook/reply-drift handler below, which scripts/detect-reply-drift.py (a
// Claude Code Stop hook) POSTs to after scanning the session transcript for
// exactly this pattern.
var replyDriftTotal atomic.Int64

// adminTarget is one admin's reachable chat on a specific frontend: its chat
// id plus that frontend's Send. Admin escalations (tool-approval prompts,
// scheduler last-line-of-defense DMs, Grafana alerts) fan out to every
// configured target, so a Discord-only-admin deployment is notified exactly
// like a Telegram one instead of being silently dropped.
type adminTarget struct {
	chatID string
	send   func(context.Context, relay.Message) error
}

// errNoAdminConfigured signals that an admin escalation could not be
// delivered because no admin target is configured, not that a send failed.
var errNoAdminConfigured = errors.New("relayd: no admin target configured")

// notifyAdmins sends text to every admin target's own frontend. It returns
// nil if at least one send succeeds, the first error if all fail, and
// errNoAdminConfigured if there are no targets at all.
func notifyAdmins(ctx context.Context, targets []adminTarget, text string) error {
	if len(targets) == 0 {
		return errNoAdminConfigured
	}
	var firstErr error
	ok := false
	for _, t := range targets {
		err := t.send(ctx, relay.Message{
			ConversationID: t.chatID, Text: text, Meta: map[string]string{"chat_id": t.chatID},
		})
		if err == nil {
			ok = true
		} else if firstErr == nil {
			firstErr = err
		}
	}
	if ok {
		return nil
	}
	return firstErr
}

// buildAdminTargets fans out to every Telegram admin (via the Telegram
// frontend) plus, when Discord is enabled, every Discord admin (via the
// Discord frontend). discordFront is nil when Discord is disabled.
func buildAdminTargets(cfg *config.Config, front *telegram.Frontend, discordFront *discord.Frontend, logger *log.Logger) []adminTarget {
	var targets []adminTarget
	if front != nil {
		for _, admin := range cfg.Telegram.Admins {
			id := strconv.FormatInt(admin, 10)
			targets = append(targets, adminTarget{chatID: id, send: front.Send})
		}
	}
	if discordFront != nil {
		ids, err := cfg.Discord.AdminIDs()
		if err != nil {
			logger.Printf("admin targets: discord admin ids: %v", err)
		}
		for _, id := range ids {
			targets = append(targets, adminTarget{chatID: id.String(), send: discordFront.Send})
		}
	}
	return targets
}

func main() {
	cfgPath := flag.String("config", "config.json", "path to JSON config file")
	flag.Parse()

	logger := log.New(os.Stderr, "[relayd] ", log.LstdFlags)

	// Tailscale is a hard dependency (see README Requirements) - relayd binds
	// its admin re-auth/approval flow to the tailnet address. Check that the
	// CLI is present up front, before any other setup work, and fail with a
	// clear "Tailscale isn't installed" message rather than letting the user
	// discover it two minutes into an unrelated retry loop further down. Not
	// having an IP assigned YET is a separate, normal boot-race case, handled
	// later by tailscaleIPWithRetry.
	if _, err := exec.LookPath("tailscale"); err != nil {
		logger.Fatalf("Tailscale is required but the `tailscale` CLI was not found on PATH: %v\n"+
			"Install Tailscale and run `tailscale up` before starting relayd - see README Requirements.", err)
	}

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

	// Admin device binding: ties an admin's sender id to a Tailscale device
	// that must show as present before their admin-flagged commands run.
	// Opt-in and admin-only (see internal/adminbind doc comment) - a sender
	// with no binding is unaffected, so this is fully backward compatible
	// until an admin explicitly binds their own device.
	adminDevices := adminbind.New(cfg.StatePath("adminbind.json"), logger)
	tailnetStatus := tailnet.New(5 * time.Second)

	// matrixAdmins is the set of Matrix user ids (e.g. "@admin:example.org") treated
	// as relay admins. Matrix ids are non-numeric, so they live outside the
	// access.Manager (which keys on int64) and are consulted directly by the
	// IsAdmin closure below. Deliberately NOT added to SessionGatedUsers: the
	// tailnet-only homeserver is the gate-bypass — see internal/endpoint/matrix.
	matrixAdmins := make(map[string]bool, len(cfg.Matrix.Admins))
	if cfg.Matrix.Enabled {
		for _, a := range cfg.Matrix.Admins {
			matrixAdmins[a] = true
		}
	}

	// webAdmins holds the web frontend's single ConvID (non-numeric, e.g.
	// "web-jean"), treated as a relay admin. Like Matrix it lives outside the
	// int64 access.Manager and is consulted by the IsAdmin closure. Deliberately
	// NOT added to SessionGatedUsers: the tailnet whois check on every request
	// is the gate — see internal/endpoint/web.
	webAdmins := make(map[string]bool, 1)
	if cfg.Web.Enabled && cfg.Web.ConvID != "" {
		webAdmins[cfg.Web.ConvID] = true
	}

	// Budget + shared control-plane commands + the admin /handshake command.
	meter := budget.New(cfg.Budget.Tier, nil)
	cmds := relay.StandardCommands(meter)
	// Central admin gating for all Admin-flagged commands. Consults both the
	// Telegram and (if enabled) Discord access managers — each frontend's
	// admin ids live in its own manager, so a Discord admin's id is only
	// ever found in discordAcc, and would otherwise never satisfy IsAdmin.
	cmds.IsAdmin = newIsAdmin(matrixAdmins, webAdmins, acc, func() *access.Manager { return discordAcc })
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

	// Contacts directory: relayd-maintained record of platform identities,
	// built automatically from inbound traffic. Resolves names like
	// "discord.alice" or "person:alice" to a live chat_id, so schedules and
	// replies don't hardcode raw chat_ids that go stale when someone switches
	// which app they're using.
	dir := contacts.New(cfg.StatePath("contacts.json"), logger)

	// Claude backend: listen on the socket for the shim.
	back, err := claudebk.New(cfg.Claude.Socket)
	if err != nil {
		logger.Fatalf("claude backend: %v", err)
	}
	defer back.Close()
	back.Resolve = dir.Resolve

	// One denied-sender log, shared across every frontend, so unauthorized
	// attempts on any platform land in a single audit file. Built before the
	// Telegram block because Discord uses it too and Telegram may be off.
	var deniedLog deniedlog.Logger = deniedlog.Noop{}
	if cfg.Telegram.DeniedLogPath != "" {
		dl, err := deniedlog.NewFileDeniedLogger(cfg.Telegram.DeniedLogPath)
		if err != nil {
			logger.Fatalf("denied-sender log: %v", err)
		}
		defer dl.Close()
		deniedLog = dl
	}

	// Telegram frontend, authorized via the access manager. Optional: nil
	// front means "not configured", and every downstream use is guarded.
	var front *telegram.Frontend
	if cfg.Telegram.Enabled() {
		telegramOpts := []telegram.Option{
			telegram.WithAuthorizer(acc),
			telegram.WithPollTimeout(cfg.Telegram.PollTimeout()),
			telegram.WithOffsetStore(cfg.StatePath("telegram_offset")),
			telegram.WithLogger(logger),
		}
		if cfg.Telegram.DeniedLogPath != "" {
			telegramOpts = append(telegramOpts, telegram.WithDeniedLogger(deniedLog))
		}
		front = telegram.New(token, telegramOpts...)

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
				logger.Fatalf("bot connection failed after retrying for 2m (check the %s env var): %s", cfg.Telegram.TokenEnv, front.SafeErr(err))
			}
			logger.Printf("bot handshake failed, retrying in %s: %s", backoff, front.SafeErr(err))
			time.Sleep(backoff)
			if backoff < 15*time.Second {
				backoff *= 2
			}
		}
		logger.Printf("connected to Telegram as @%s (bot id %d)", info.Username, info.ID)
	} else {
		logger.Printf("telegram: disabled")
	}

	// Discord frontend (optional, per DESIGN.md's wiring/startup design):
	// Discord has its own snowflake id namespace and access manager, so it
	// gets its own New/Connect handshake, then fans into the Broker's single
	// Frontend slot via relay.MultiFrontend alongside Telegram.
	var discordFront *discord.Frontend
	var matrixFront *matrix.Frontend
	var frontends []relay.Endpoint
	if front != nil {
		frontends = append(frontends, front)
	}
	if cfg.Discord.Enabled {
		discordFront, discordAcc = mustStartDiscord(cfg, logger, deniedLog)
		frontends = append(frontends, discordFront)
	}
	if cfg.Matrix.Enabled {
		matrixFront = mustStartMatrix(cfg, logger)
		frontends = append(frontends, matrixFront)
	}
	var webFront *web.Frontend
	if cfg.Web.Enabled {
		webFront = mustStartWeb(cfg, logger)
		frontends = append(frontends, webFront)
	}
	// validate() guarantees at least one frontend is configured, so this is
	// never empty.
	var frontendEndpoint relay.Endpoint
	switch len(frontends) {
	case 0:
		logger.Fatalf("no frontend started — check the telegram/discord/matrix/web config")
	case 1:
		frontendEndpoint = frontends[0]
	default:
		frontendEndpoint = relay.NewMultiFrontend(frontends...)
	}

	// Permission relay: admins approve tool-use prompts via /allow and /deny.
	// The same commands also resolve force_reauth requests the model triggers
	// (see reauthGate) — one approval surface for every gated action.
	reauth := newReauthGate()
	cmds.Register(command.Command{Name: "allow", Help: "admin: approve a tool request: /allow <id>", Admin: true, Run: verdict(back, reauth, true)})
	cmds.Register(command.Command{Name: "deny", Help: "admin: reject a tool request: /deny <id>", Admin: true, Run: verdict(back, reauth, false)})

	// Admin escalation targets: every configured admin on whichever
	// frontend(s) they live on (Telegram and/or Discord). A Discord-only-admin
	// deployment must be reachable exactly like a Telegram one.
	admins := buildAdminTargets(cfg, front, discordFront, logger)

	// Forward Claude's tool-approval prompts to every admin's chat.
	go func() {
		for req := range back.Permissions() {
			msg := fmt.Sprintf("🔐 Claude wants to use %s\n%s\n\napprove: /allow %s   deny: /deny %s",
				req.Tool, req.Detail, req.ID, req.ID)
			_ = notifyAdmins(context.Background(), admins, msg)
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
	// With no admin configured there's nobody to escalate to, but that's
	// still a failure to deliver - report it as one (errNoAdminConfigured)
	// rather than silently claiming success. Fans out to every configured
	// admin target across whichever frontend(s) they live on.
	sendToAdmin := func(text string) error {
		return notifyAdmins(context.Background(), admins, text)
	}
	// receipt is fire-and-forget (a best-effort audit ping), so it discards the
	// error; the fallback path uses the error-returning form directly.
	sendReceipt := func(text string) { _ = sendToAdmin(text) }
	tracker, err := scheduler.NewTracker(eventsPath, inject, sendToAdmin, sendReceipt, scheduler.TrackerConfig{}, logger)
	if err != nil {
		logger.Fatalf("event tracker: %v", err)
	}
	defer tracker.Close()

	// externalSchedule notifies the live session the moment the scheduler's
	// file watcher finds a schedule in schedules.json that didn't arrive via
	// a schedule_message tool call (e.g. a hand-edit while relayd is
	// running). It's informational only — no pending event, no ack_event
	// required, and the schedule stays armed either way; this just keeps
	// provenance visible in real time instead of only via list_schedules.
	externalSchedule := func(sc *scheduler.Schedule, kind string) {
		text := fmt.Sprintf(
			"[schedule %s via file edit — provenance: file] id=%s cron=%q chat_id=%s\ntext: %q\n\n"+
				"This schedule was found in schedules.json without a matching schedule_message tool call. "+
				"It is already armed and will fire normally — no action required unless you want to review or cancel it.",
			kind, sc.ID, sc.Cron, sc.ChatID, sc.Text,
		)
		_ = back.Send(context.Background(), relay.Message{
			ConversationID: sc.ChatID, Role: relay.User, Text: text,
			Meta: map[string]string{"chat_id": sc.ChatID, "external_schedule": "1"},
		})
	}

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
	}, externalSchedule, logger)
	if err != nil {
		logger.Fatalf("scheduler: %v", err)
	}
	defer sched.Close()

	// Tailnet-bound approval flow for high-risk actions: a loopback-only
	// /request+/status API this agent calls (via curl) to ask for a human
	// decision, and a Tailscale-interface-only /approve page the request
	// link points at. Being able to load the approve page at all is proof
	// of tailnet membership - stronger than trusting a Telegram chat_id
	// alone, which is spoofable if that account is ever compromised.
	//
	// The bind address is resolved from the tailscale CLI at startup
	// rather than hardcoded, so this doesn't silently break (wrong IP baked
	// into a public binary/repo) if the tailnet IP ever changes. Retried with
	// the same backoff as the listener below - tailscaled may not have
	// assigned the interface an address yet this early at boot.
	tsIP, err := tailscaleIPWithRetry(2*time.Minute, logger)
	if err != nil {
		logger.Fatalf("resolve tailscale IP: %v", err)
	}
	appr := approval.NewManager(fmt.Sprintf("http://%s:9212", tsIP))
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
	appListener, err := listenWithRetry("tcp", fmt.Sprintf("%s:9212", tsIP), 2*time.Minute, logger)
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

	// Durable audit trail: one JSON line per lifecycle step of every message,
	// keyed by the msg_id stamped at ingress. This is what makes "what happened
	// to the message I sent at 16:31?" answerable in seconds instead of an hour
	// of forensics. Failing to open it is not fatal - observability must never
	// keep the relay from running.
	events, err := eventlog.Open("relay-events.jsonl")
	if err != nil {
		logger.Printf("event log disabled (%v)", err)
	} else {
		defer events.Close()
		logger.Printf("event log: relay-events.jsonl")
	}

	b := &relay.Broker{Frontend: frontendEndpoint, Backend: back, Commands: cmds, Meter: meter,
		OnInboundObserved: func(m relay.Message) {
			dir.Observe(m.Meta["platform"], m.Meta["chat_id"], m.Meta["guild_id"], m.Meta["from_name"])
		},
		Events:                 events,
		ConversationCaps:       cfg.Budget.ConversationCaps,
		DefaultConversationCap: cfg.Budget.DefaultConversationCap,
		// Admins are exempt from DefaultConversationCap - the blanket cap is
		// for arbitrary allowlisted contacts, not the operator. Reuses
		// cmds.IsAdmin directly: chat_id == from_id for any private 1:1
		// conversation (the only kind a per-conversation cap makes sense
		// for), so the same admin check already used for slash commands
		// applies unchanged here.
		ConversationCapExempt: cmds.IsAdmin,
	}

	// Grafana alert webhook: alerts are routed through the model backend
	// (same injection path as scheduled triggers), not straight to Telegram -
	// so alerts get judgment/context applied before they reach the user,
	// instead of Grafana paging directly. Loopback-only: Grafana runs on
	// this same host, no need to expose it beyond localhost. Also hosts
	// /webhook/reply-drift and /webhook/token-usage, hence needing b now
	// that it exists.
	go serveGrafanaWebhook(back, admins, acc, meter, front, discordFront, tracker, sched, logger, b, *cfgPath)

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
	// happened. Classification lives in ackErrText.
	b.AckBackendReply = func(m relay.Message, sendErr error) {
		reqID := m.Meta["reply_id"]
		if reqID == "" {
			return // reply carries no correlation id (e.g. an internal/system reply) - nothing waiting
		}
		if err := back.ReplyRespond(reqID, ackErrText(sendErr)); err != nil {
			logger.Printf("reply ack for %s: %v", reqID, err)
		}
	}
	// Outbound gate: see outboundAllowed's doc comment for the full rationale.
	b.OutboundAllowed = func(chatID string) bool {
		known := func(id string) bool {
			if discordFront != nil && discordFront.KnownConversation(id) {
				return true
			}
			// A Matrix room id (starts with '!') is a legitimate outbound
			// target: the model only ever gets a Matrix conversation from an
			// authorized admin's inbound message, so replying into it is safe.
			if matrixFront != nil && matrixFront.OwnsConversationID(id) {
				return true
			}
			// The web pane's ConvID is a legitimate outbound target for the same
			// reason as Matrix: the model only ever sees it from a whois-verified
			// admin inbound, so replying back into it is safe.
			return webFront != nil && webFront.OwnsConversationID(id)
		}
		if outboundAllowed(chatID, acc, discordAcc, known) {
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

		// Admin device-presence gate (on top of the session gate above): an
		// admin who has bound a Tailscale device to their id must have that
		// device online for their Admin-flagged commands specifically, or a
		// fresh re-auth approval (same Session/Approval machinery) standing
		// in for it - see AdminDevicePresent's doc comment on Broker.
		b.AdminDevicePresent = func(senderID string) (required, online bool) {
			device, bound := adminDevices.Device(senderID)
			if !bound {
				return false, false
			}
			peer, ok := tailnetStatus.Peer(device)
			return true, ok && peer.Online
		}

		// force_reauth tool: the model can trigger a targeted re-auth challenge
		// (the single-admin counterpart of /reauth's ExpireAll), but only for a
		// gated admin and only after a human approves via /allow — so a
		// manipulated model session can't force-revoke an admin unilaterally.
		go serveReauth(back, reauth,
			func(id string) bool { return gated[id] },
			b.Session.Revoke,
			func(text string) error { return notifyAdmins(context.Background(), admins, text) },
			logger)

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

	if cfg.Stylometry.Enabled {
		det := stylometry.NewDetector(cfg.Stylometry.QdrantURL, cfg.Stylometry.Collection)
		if cfg.Stylometry.LogPath != "" {
			det.Log = &stylometry.EventLog{Path: cfg.Stylometry.LogPath}
		}
		if err := det.EnsureCollection(context.Background()); err != nil {
			logger.Printf("stylometry disabled (EnsureCollection: %v)", err)
		} else {
			b.Anomaly = det
			b.AnomalyThreshold = cfg.Stylometry.Threshold
			b.AnomalyWarnChatID = cfg.Stylometry.WarnChatID
			// Resolve linked identities (e.g. the same person's Telegram and
			// Discord accounts) to one canonical key so their style history
			// accumulates as a single profile instead of splitting per
			// platform - see contacts.Directory.Link.
			b.AnomalyIdentity = func(platform, chatID string) string {
				id, ok := dir.Lookup(platform, chatID)
				if !ok || id.LinkedTo == "" {
					return ""
				}
				return "person:" + id.LinkedTo
			}
			logger.Printf("stylometry enabled: collection=%s threshold=%.2f",
				cfg.Stylometry.Collection, cfg.Stylometry.Threshold)
		}
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

// ackErrText classifies a Send outcome for AckBackendReply: only a permanent
// failure (senderr.Permanent) is surfaced as an error string - Frontend.Send
// also returns an error for transient failures that it has already queued
// for background retry, and reporting those to the model would invite a
// resend that duplicates delivery once the retry lands. Returns "" for a nil
// or transient error.
func ackErrText(sendErr error) string {
	var perm senderr.Permanent
	if sendErr != nil && errors.As(sendErr, &perm) {
		return sendErr.Error()
	}
	return ""
}

// mustStartDiscord builds the Discord frontend from cfg.Discord, connects
// its gateway, and returns it along with the access.Manager backing its
// allowlist (so the caller can also consult it for outbound gating).
// Config validation errors are fatal immediately; the gateway connect itself
// retries with a bounded timeout for ~2m (mirroring the Telegram handshake)
// before Fatalf, so a transient boot-time DNS race doesn't kill relayd (and
// the already-connected Telegram frontend with it) - an operator who set
// discord.enabled=true with a genuinely bad token/config still gets a clear
// startup failure, just not on the very first attempt (the exact gap this
// function exists to close - see DESIGN.md's wiring/startup design).
func mustStartDiscord(cfg *config.Config, logger *log.Logger, deniedLog deniedlog.Logger) (*discord.Frontend, *access.Manager) {
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
		discord.WithDeniedLogger(deniedLog),
		discord.WithLogger(logger),
		discord.WithAllowGuildMessages(cfg.Discord.AllowGuildMessages),
		discord.WithAllowedGuildIDs(guildIDs...),
		discord.WithRequireMentionInGuild(cfg.Discord.RequireMentionInGuild()),
	)
	if err != nil {
		logger.Fatalf("discord: %v", err)
	}

	// Bounded retry loop mirroring the Telegram handshake above: a transient
	// boot-time DNS/network race or a wedged OpenGateway shouldn't Fatalf
	// relayd (which would also tear down the already-connected Telegram
	// frontend) before Broker.Run even starts. Still fails fast with a clear
	// message for a genuinely bad token/config - just not on the first attempt.
	connectDeadline := time.Now().Add(2 * time.Minute)
	backoff := time.Second
	for {
		cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
		err = front.Connect(cctx)
		ccancel()
		if err == nil {
			break
		}
		if time.Now().After(connectDeadline) {
			logger.Fatalf("discord: connect failed after retrying for 2m: %v", err)
		}
		logger.Printf("discord connect failed, retrying in %s: %v", backoff, err)
		time.Sleep(backoff)
		if backoff < 15*time.Second {
			backoff *= 2
		}
	}
	logger.Printf("connected to Discord (admins=%d, allowlist=%d, guild_messages=%v)",
		len(adminIDs), len(allowIDs), cfg.Discord.AllowGuildMessages)
	return front, discordAcc
}

// mustStartMatrix constructs the Matrix frontend, verifies its access token
// against the homeserver (whoami), and starts its inbound /sync loop. Fatal on
// misconfiguration (missing token/homeserver) so a half-configured admin
// channel fails loudly rather than silently never receiving. The Connect uses a
// bounded retry loop for the same boot-time-network-race reason as Discord.
func mustStartMatrix(cfg *config.Config, logger *log.Logger) *matrix.Frontend {
	tokenEnv := cfg.Matrix.TokenEnv
	if tokenEnv == "" {
		tokenEnv = "MATRIX_ACCESS_TOKEN"
	}
	token := os.Getenv(tokenEnv)
	if token == "" {
		logger.Fatalf("matrix: no access token in env %s", tokenEnv)
	}
	if cfg.Matrix.HomeserverURL == "" {
		logger.Fatalf("matrix: homeserver_url is required")
	}
	if len(cfg.Matrix.Admins) == 0 {
		logger.Fatalf("matrix: at least one admin user id is required (fail-closed)")
	}
	if err := config.CheckTailnetHomeserver(cfg.Matrix.HomeserverURL, cfg.Matrix.AllowNonTailnetHomeserver); err != nil {
		logger.Fatalf("matrix: %v", err)
	}
	if cfg.Matrix.AllowNonTailnetHomeserver && !config.IsTailnetHost(cfg.Matrix.HomeserverURL) {
		logger.Printf("WARNING: matrix.allow_non_tailnet_homeserver is set and %s is not a tailnet address. "+
			"The Matrix path bypasses the session and approval gates on the assumption that reaching the "+
			"homeserver requires being on the tailnet. That assumption no longer holds.", cfg.Matrix.HomeserverURL)
	}
	mediaSpool := cfg.Matrix.MediaSpool
	if mediaSpool == "" {
		mediaSpool = "matrix-media"
	}
	front := matrix.New(cfg.Matrix.HomeserverURL, token, cfg.Matrix.Admins, mediaSpool, logger)

	backoff := 2 * time.Second
	connectDeadline := time.Now().Add(2 * time.Minute)
	var selfID string
	for {
		cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
		id, err := front.Connect(cctx)
		ccancel()
		if err == nil {
			selfID = id
			break
		}
		if time.Now().After(connectDeadline) {
			logger.Fatalf("matrix: connect failed after retrying for 2m: %v", err)
		}
		logger.Printf("matrix connect failed, retrying in %s: %v", backoff, err)
		time.Sleep(backoff)
		if backoff < 15*time.Second {
			backoff *= 2
		}
	}
	go front.Run(context.Background(), selfID)
	logger.Printf("connected to Matrix as %s (homeserver=%s, admins=%d)",
		selfID, cfg.Matrix.HomeserverURL, len(cfg.Matrix.Admins))
	return front
}

// mustStartWeb constructs and starts the web frontend (SSE+POST chat over the
// tailnet). Fatal on misconfiguration (missing conv_id / tailnet_owner) so a
// half-configured channel fails loudly rather than silently accepting nothing —
// or, worse, accepting everything.
func mustStartWeb(cfg *config.Config, logger *log.Logger) *web.Frontend {
	addr := cfg.Web.ListenAddr
	if addr == "" {
		addr = "127.0.0.1:8792"
	}
	if cfg.Web.ConvID == "" {
		logger.Fatalf("web: conv_id is required (fail-closed)")
	}
	if cfg.Web.TailnetOwner == "" {
		logger.Fatalf("web: tailnet_owner is required (fail-closed)")
	}
	name := cfg.Web.FromName
	if name == "" {
		name = "web"
	}
	front, err := web.New(addr, cfg.Web.ConvID, name, cfg.Web.TailnetOwner, logger)
	if err != nil {
		logger.Fatalf("web: %v", err)
	}
	logger.Printf("web frontend listening on %s (conv=%s owner=%s)", addr, cfg.Web.ConvID, cfg.Web.TailnetOwner)
	return front
}

// tailscaleIP resolves this host's current Tailscale IPv4 address, so it never
// needs to be hardcoded (which previously baked one specific IP into a public
// repo/binary and would silently break if the tailnet IP ever changed).
//
// It asks the CLI rather than reading an interface by name. The interface is
// "tailscale0" on Linux but a "utunN" on macOS, with the number varying per
// boot, so a name lookup is a Linux-only assumption.
func tailscaleIP() (string, error) {
	out, err := exec.Command("tailscale", "ip", "-4").Output()
	if err != nil {
		return "", fmt.Errorf("tailscale ip -4: %w", err)
	}
	// Multiple addresses are possible; the first IPv4 is this node's.
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if ip := net.ParseIP(line); ip != nil && ip.To4() != nil {
			return ip.To4().String(), nil
		}
	}
	return "", fmt.Errorf("tailscale ip -4 returned no IPv4 address")
}

// tailscaleIPWithRetry retries tailscaleIP with a fixed 1s backoff until
// timeout elapses - mirrors listenWithRetry's boot-race handling, since
// resolving the address has the exact same "interface not up yet" problem
// as binding to it.
func tailscaleIPWithRetry(timeout time.Duration, logger *log.Logger) (string, error) {
	giveUp := time.Now().Add(timeout)
	var lastErr error
	for {
		ip, err := tailscaleIP()
		if err == nil {
			return ip, nil
		}
		lastErr = err
		if time.Now().After(giveUp) {
			return "", fmt.Errorf("giving up after %s: %w", timeout, lastErr)
		}
		logger.Printf("tailscale address not ready yet, retrying: %v", err)
		time.Sleep(1 * time.Second)
	}
}

// listenWithRetry binds addr, retrying with a fixed 1s backoff until
// deadline elapses. Used for listeners bound to an interface address (like
// the Tailscale IP) that may not exist yet this early at boot.
func listenWithRetry(network, addr string, timeout time.Duration, logger *log.Logger) (net.Listener, error) {
	giveUp := time.Now().Add(timeout)
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
// annotation is one piece of feedback aimed at a place in a file rather than at
// a conversation. Line is 1-indexed; 0 means "the file as a whole".
type annotation struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Text string `json:"text"`
	// "note" renders as a comment thread and persists; "urgent" also raises a
	// toast, for the cases worth interrupting over.
	Kind string `json:"kind"`
}

// annotationHub fans annotations out to every connected editor. Sends are
// non-blocking: an editor that has stopped reading gets its annotation dropped
// rather than stalling the publisher, because a wedged subscriber must not be
// able to block the model's turn.
type annotationHub struct {
	mu   sync.Mutex
	subs map[chan annotation]struct{}
}

func newAnnotationHub() *annotationHub {
	return &annotationHub{subs: make(map[chan annotation]struct{})}
}

func (h *annotationHub) subscribe() chan annotation {
	ch := make(chan annotation, 16)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs[ch] = struct{}{}
	return ch
}

func (h *annotationHub) unsubscribe(ch chan annotation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
}

func (h *annotationHub) publish(note annotation) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	sent := 0
	for ch := range h.subs {
		select {
		case ch <- note:
			sent++
		default:
		}
	}
	return sent
}

var annotations = newAnnotationHub()

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

// newRelaydMux builds the loopback-only observability/webhook mux served by
// serveGrafanaWebhook. Split out so tests can exercise each handler directly
// via httptest without starting a real listener.

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
			// Provenance only called out when it's not the default (tool) —
			// keeps normal listings uncluttered, still surfaces the exception.
			src := ""
			if sc.EffectiveSource() != scheduler.SourceTool {
				src = fmt.Sprintf(" [source: %s]", sc.EffectiveSource())
			}
			fmt.Fprintf(&b, "  %s — %s%s — %q\n", sc.ID, describeSchedule(sched, sc), src, sc.Text)
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

// outboundAllowed implements the outbound gate: the model can only reply to
// allowlisted chats. The inbound allowlist gates who reaches Claude; this
// stops Claude messaging strangers. Checks both the Telegram (int64) and,
// when enabled, Discord (snowflake) allowlists — chatID is a Telegram chat
// id (== user id) or, for Discord, gate()'s convID (== user id for DMs, ==
// channel id for guild messages). Guild channels are inherently multi-party
// so acc-style single-id allowlisting doesn't apply there; instead known
// reports whether the Discord frontend has already seen and gated this
// chatID inbound — i.e. a guild channel from an allowed guild, or a DM user
// id. That covers scheduled reminders / relayd-originated replies into a
// channel the model was legitimately talking in, while still failing closed
// for anything never seen inbound. discordAcc and known may be nil/absent
// when the Discord frontend is disabled.
func outboundAllowed(chatID string, acc *access.Manager, discordAcc *access.Manager, known func(string) bool) bool {
	if id, err := strconv.ParseInt(chatID, 10, 64); err == nil && acc.Allowed(id) {
		return true
	}
	if discordAcc != nil {
		if id, err := snowflake.Parse(chatID); err == nil && discordAcc.Allowed(int64(id)) {
			return true
		}
	}
	if known != nil && known(chatID) {
		return true
	}
	return false
}

// verdict returns an /allow or /deny handler that answers a pending tool-approval
// request by its id. Admin gating is enforced centrally by the registry. A
// force_reauth request registered by the model uses this same command surface:
// gate.decide claims the id first; anything it doesn't recognize falls through
// to the Claude Code permission path (back.Decide).
func verdict(back *claudebk.Endpoint, gate *reauthGate, allow bool) command.Handler {
	return func(_ command.Context, args []string) string {
		if len(args) < 1 {
			return "usage: /" + map[bool]string{true: "allow", false: "deny"}[allow] + " <request_id>"
		}
		if gate != nil && gate.decide(args[0], allow) {
			if allow {
				return "✅ allowed " + args[0] + " (force_reauth)"
			}
			return "⛔ denied " + args[0] + " (force_reauth)"
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
