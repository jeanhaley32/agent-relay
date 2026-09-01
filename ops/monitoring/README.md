# Claude relay login/auth-expiry alerting

Detects when the relay Claude session's login expires and alerts Jean
**out-of-band** — via relayd straight to the frontend, NOT through the model
backend — because an alert about Claude being down cannot be delivered through
Claude. (On 2026-09-01 the login expired, ordinary Grafana alerts landed in the
dead pane behind a "Login expired" wall, and it had to be caught by hand.)

## Pieces
- `scripts/check-claude-login.py` — collector. Emits to the node-exporter
  textfile dir (`/var/lib/prometheus/node-exporter/claude_login.prom`):
  - `claude_login_expired` 0/1 (reactive — greps the live relay tmux pane for
    Claude's "Login expired / Please run /login" wall; this is what actually
    fires in practice)
  - `claude_refresh_token_expires_seconds` (proactive — the ~monthly hard wall)
  - `claude_access_token_expires_seconds` (context; auto-refreshes, not alarmed)
- `systemd/claude-login-check.{service,timer}` — user units, run the collector
  every 2 min. Install: copy to ~/.config/systemd/user/, `systemctl --user
  daemon-reload && systemctl --user enable --now claude-login-check.timer`.
- relayd `/webhook/login-alert` (cmd/relayd/main.go) — delivers a POSTed alert
  DIRECTLY to the admin frontend (telegram/discord), never via back.Send.
- Grafana (in /etc/grafana/provisioning/alerting/, copies here for the record):
  - `grafana/claude-login-rules.yaml` — the two alert rules.
  - contactpoints.yaml: contact point `relay-login-alert` → the webhook above.
  - policies.yaml: route matching label `alert_channel=out-of-band` →
    `relay-login-alert`. NOT muted overnight (a dead relay at 2am is worth
    knowing); add `mute_time_intervals: [quiet-hours-overnight]` to change.

## Test
    curl -XPOST http://127.0.0.1:9210/webhook/login-alert -H 'Content-Type: application/json' \
      -d '{"status":"firing","alerts":[{"status":"firing","labels":{"alertname":"TEST","severity":"critical","alert_channel":"out-of-band"},"annotations":{"summary":"test"}}]}'
Expect an HTTP 200 and a direct message on the frontend.
