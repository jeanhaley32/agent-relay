// HTTP surface for relayd: the loopback-only mux that carries /metrics, the
// Grafana webhook, and the /webhook/* endpoints on-box tools use to reach the
// model and the editor. Split out of main.go, which had grown to 1,697 lines
// with two functions accounting for 60% of it.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jeanhaley32/agent-relay/internal/access"
	"github.com/jeanhaley32/agent-relay/internal/budget"
	"github.com/jeanhaley32/agent-relay/internal/config"
	claudebk "github.com/jeanhaley32/agent-relay/internal/endpoint/claude"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/discord"
	"github.com/jeanhaley32/agent-relay/internal/endpoint/telegram"
	"github.com/jeanhaley32/agent-relay/internal/relay"
	"github.com/jeanhaley32/agent-relay/internal/scheduler"
)

func serveGrafanaWebhook(back *claudebk.Endpoint, admins []adminTarget, acc *access.Manager, meter *budget.Meter, front *telegram.Frontend, discordFront *discord.Frontend, tracker *scheduler.Tracker, sched *scheduler.Scheduler, logger *log.Logger, b *relay.Broker, cfgPath string) {
	mux := newRelaydMux(back, admins, acc, meter, front, discordFront, tracker, sched, logger, b, cfgPath)
	// Shared with scripts/detect-reply-drift.py (and any other hook that calls
	// back into relayd): both read RELAY_WEBHOOK_ADDR and fall back to the same
	// default, so moving the port moves both instead of silently breaking the
	// hooks that hardcoded it.
	addr := os.Getenv("RELAY_WEBHOOK_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9210"
	}
	// Fatalf on bind failure, matching the approval listeners above: a silent
	// failure here would leave /metrics, /webhook/token-usage,
	// /webhook/reload-caps and /webhook/grafana all unreachable with nothing
	// but a single startup log line to show for it.
	l, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatalf("grafana webhook listener: %v", err)
	}
	logger.Printf("grafana webhook listening on %s/webhook/grafana", addr)
	if err := http.Serve(l, mux); err != nil {
		logger.Printf("grafana webhook server stopped: %v", err)
	}
}

func newRelaydMux(back *claudebk.Endpoint, admins []adminTarget, acc *access.Manager, meter *budget.Meter, front *telegram.Frontend, discordFront *discord.Frontend, tracker *scheduler.Tracker, sched *scheduler.Scheduler, logger *log.Logger, b *relay.Broker, cfgPath string) *http.ServeMux {
	mux := http.NewServeMux()
	// Exposes two things the admin dashboard/alert rule needs: unauthorized
	// senders queued internally by access.Manager.Record (otherwise only
	// visible via the /handshake admin command), and relayd's own
	// budget/circuit-breaker state, which had no external visibility before.
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
		// sched is nil in some tests that don't exercise scheduling.
		var outOfBandCount int
		if sched != nil {
			outOfBandCount = sched.OutOfBandCount()
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
		// Telegram frontend is always non-nil in production (unlike the
		// optional Discord one) but newRelaydMux is also used directly by
		// tests with front=nil, so guard the same way rather than assume.
		var telegramSendFailures, telegramPermanentDrops, telegramQueueDepth int64
		var telegramGetUpdatesFailures, telegramLastPollSuccess int64
		if front != nil {
			telegramSendFailures = front.SendFailures()
			telegramPermanentDrops = front.PermanentDrops()
			telegramQueueDepth = front.QueueDepth()
			telegramGetUpdatesFailures = front.GetUpdatesFailures()
			telegramLastPollSuccess = front.LastPollSuccess()
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
				"relayd_reply_drift_total %d\n"+
				"# HELP relayd_schedules_out_of_band Currently-armed schedules last detected as added/modified via a direct schedules.json edit rather than the schedule_message tool (Source=file).\n"+
				"# TYPE relayd_schedules_out_of_band gauge\n"+
				"relayd_schedules_out_of_band %d\n",
			acc.TotalRecorded(), len(acc.Pending()), len(acc.Allowlist()),
			snap.PercentUsed, snap.Used, snap.Limit, snap.WindowLeft.Seconds(),
			stateNum, pausedNum,
			telegramSendFailures, telegramPermanentDrops, telegramQueueDepth,
			telegramGetUpdatesFailures, telegramLastPollSuccess,
			tracker.OpenCount(), tracker.OldestOpenAge().Seconds(),
			discordSendFailures, discordPermanentDrops, discordQueueDepth,
			discordRecvDrops, discordGatewayReconnects, discordLastGatewayEventAt,
			replyDriftTotal.Load(),
			outOfBandCount,
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
	// rather than auto-forwarding the orphaned text: auto-forward risks
	// sending unintended draft/mid-thought text.
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
	// runs, which undercounts real usage. Body:
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
	// /webhook/reload-caps: re-reads config.json's budget.conversation_caps
	// and budget.default_conversation_cap from disk and applies them to the
	// live Broker via SetCaps, without a relayd process restart. Only the
	// budget caps are hot-reloaded, not the rest of config.json (token env
	// vars, allowlists, etc. still require a real restart) - deliberately
	// narrow rather than a general config-reload mechanism, which would be
	// a much bigger surface to get right.
	mux.HandleFunc("/webhook/reload-caps", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		fresh, err := config.Load(cfgPath)
		if err != nil {
			logger.Printf("reload-caps: failed to reload %s: %v", cfgPath, err)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		b.SetCaps(fresh.Budget.ConversationCaps, fresh.Budget.DefaultConversationCap)
		logger.Printf("reload-caps: applied %d explicit cap(s), default=%d",
			len(fresh.Budget.ConversationCaps), fresh.Budget.DefaultConversationCap)
		w.WriteHeader(http.StatusOK)
	})
	// /webhook/annotate and /annotations/stream are the outbound half of the
	// VS Code channel. The model posts an annotation ({file, line, text}); the
	// editor extension holds an SSE connection and renders it as a comment
	// thread at that line, or a toast when it's urgent.
	//
	// This is a frontend like Telegram or the web pane, but it is the first one
	// whose messages are not plain text: rendering at a line needs the position,
	// so the payload is structured. Keeping that structure here rather than
	// parsing it out of prose in the extension is what lets the extension stay a
	// dumb renderer, with every judgment about what's worth saying staying with
	// the model.
	mux.HandleFunc("/webhook/annotate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var note annotation
		if err := json.Unmarshal(body, &note); err != nil {
			logger.Printf("annotate webhook: bad payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(note.File) == "" || strings.TrimSpace(note.Text) == "" {
			http.Error(w, "file and text are required", http.StatusBadRequest)
			return
		}
		if note.Kind == "" {
			note.Kind = "note"
		}
		delivered := annotations.publish(note)
		// Report the subscriber count so a caller can tell the difference
		// between "sent" and "sent into the void with the editor closed".
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"delivered_to":%d}`, delivered)
	})
	mux.HandleFunc("/annotations/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher.Flush()

		sub := annotations.subscribe()
		defer annotations.unsubscribe(sub)

		// A periodic comment frame keeps the connection from being reaped by an
		// idle timeout during the long gaps between annotations.
		ping := time.NewTicker(25 * time.Second)
		defer ping.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case note := <-sub:
				payload, err := json.Marshal(note)
				if err != nil {
					continue
				}
				fmt.Fprintf(w, "data: %s\n\n", payload)
				flusher.Flush()
			case <-ping.C:
				fmt.Fprint(w, ": keepalive\n\n")
				flusher.Flush()
			}
		}
	})
	// /webhook/inject hands arbitrary text from a local tool to the model as a
	// user message, the same buffered-inject path the scheduler and the Grafana
	// webhook use. It exists because loopback tools previously had no way to
	// start a turn: the web frontend's /send authenticates by tailscale whois on
	// the client IP, so a process on this box is 127.0.0.1 and always denied.
	// The text reaches the model, never the user directly - so the model decides
	// whether any of it is worth surfacing, and a chatty watcher can't page
	// anyone on its own.
	mux.HandleFunc("/webhook/inject", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if len(admins) == 0 {
			logger.Printf("inject webhook: no admin target configured, dropping")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var payload struct {
			Text   string `json:"text"`
			Source string `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			logger.Printf("inject webhook: bad payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(payload.Text) == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		source := payload.Source
		if source == "" {
			source = "local tool"
		}
		// One admin, not a fan-out. Every admin target feeds the same model
		// session, so sending to all of them starts the same turn once per
		// frontend - the Grafana path fans out because each copy is a separate
		// alert the user may need on that frontend, but an inject is just work
		// handed to the model.
		adm := admins[0]
		_ = back.Send(context.Background(), relay.Message{
			ConversationID: adm.chatID,
			Role:           relay.User,
			Text:           fmt.Sprintf("[injected by %s on this box]\n\n%s", source, payload.Text),
			Meta:           map[string]string{"chat_id": adm.chatID, "injected": "1"},
		})
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/webhook/grafana", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if len(admins) == 0 {
			logger.Printf("grafana webhook: no admin target configured, dropping alert")
			w.WriteHeader(http.StatusServiceUnavailable)
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
			for _, adm := range admins {
				prompt := fmt.Sprintf(
					"[Grafana alert, status=%s, severity=%s] %s: %s\n\n"+
						"Use your judgment on how urgently to surface this to the user via the "+
						"reply tool (chat_id=\"%s\") - a firing critical alert probably warrants an "+
						"immediate message, a resolved one may just be worth a brief note, and if "+
						"you're already mid-investigation on the same subsystem you can fold it into "+
						"that instead of sending a separate ping.",
					a.Status, severity, name, summary, adm.chatID,
				)
				_ = back.Send(context.Background(), relay.Message{
					ConversationID: adm.chatID, Role: relay.User, Text: prompt,
					Meta: map[string]string{"chat_id": adm.chatID, "grafana_alert": "1"},
				})
			}
		}
		w.WriteHeader(http.StatusOK)
	})

	// /webhook/login-alert delivers an alert STRAIGHT to the admin frontend
	// (Telegram/Discord), bypassing the Claude session entirely. This exists
	// precisely for alerts about the Claude session itself being unavailable -
	// login/auth expiry above all. The ordinary /webhook/grafana path injects
	// alerts via back.Send (into the model), so an alert saying "Claude login is
	// dead" would land in the same dead pane and never reach the user - which is
	// exactly what happened on 2026-09-01 (the alerts fired, showed up in the
	// pane behind a "Login expired" wall, and Jean had to notice manually). This
	// path routes through relayd (a separate always-up service that already holds
	// the frontend Send handles), never through the model, so it survives Claude
	// being down. Loopback-only, same as the other webhooks.
	mux.HandleFunc("/webhook/login-alert", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if len(admins) == 0 {
			logger.Printf("login-alert webhook: no admin target configured, dropping alert")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var payload grafanaWebhookPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			logger.Printf("login-alert webhook: bad payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for _, a := range payload.Alerts {
			name := a.Labels["alertname"]
			severity := a.Labels["severity"]
			summary := a.Annotations["summary"]
			if summary == "" {
				summary = a.Annotations["description"]
			}
			text := fmt.Sprintf("⚠️ [%s/%s] %s: %s\n\n(Delivered out-of-band by relayd - the Claude session may be unreachable. If this is a login/auth expiry, run /login in the relay tmux pane.)",
				a.Status, severity, name, summary)
			for _, adm := range admins {
				// adm.send is the frontend's own Send (telegram/discord) - direct
				// to the user, NOT through the model backend. AssistantMsg marks
				// it as an outbound assistant message to that conversation.
				if err := adm.send(context.Background(), relay.AssistantMsg(adm.chatID, text)); err != nil {
					logger.Printf("login-alert webhook: send to %s failed: %v", adm.chatID, err)
				}
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
