#!/usr/bin/env bash
# One-command launch of the relay stack:
#   - builds the daemon + shim
#   - starts relayd (loads TELEGRAM_BOT_TOKEN from .env)
#   - launches Claude in a tmux session on the chosen model (default: sonnet),
#     with the relay channel registered
#
#   bash scripts/run.sh              # sonnet default
#   MODEL=opus bash scripts/run.sh   # override the model
#
# After it starts, attach to approve the one-time dev-channel prompt:
#   tmux attach -t relay
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

MODEL="${MODEL:-sonnet}"       # relay session model; strong work → ask for an Opus subagent
SESSION="${SESSION:-relay}"
CONFIG="${CONFIG:-config.json}"
SOCK="${SOCK:-/tmp/agent-relay.sock}"

# Bot token from .env (never committed).
[ -f .env ] && { set -a; . ./.env; set +a; }
: "${TELEGRAM_BOT_TOKEN:?set TELEGRAM_BOT_TOKEN (e.g. in .env)}"

# Resolve claude even if PATH is missing ~/.local/bin.
CLAUDE="$(command -v claude || echo "$HOME/.local/bin/claude")"
[ -x "$CLAUDE" ] || { echo "error: claude not found (looked on PATH and ~/.local/bin)" >&2; exit 1; }

echo "building binaries…"
go build -o bin/relayd ./cmd/relayd
go build -o bin/relay-shim ./cmd/relay-shim

# Start relayd if not already running (the shim auto-reconnects, so an existing
# daemon is fine to leave up).
if pgrep -x relayd >/dev/null; then
  echo "relayd already running — leaving it up (shim will reconnect)"
else
  rm -f "$SOCK"
  nohup ./bin/relayd --config "$CONFIG" > relayd.log 2>&1 &
  echo "relayd started (pid $!) → logs in relayd.log"
fi

# (Re)launch Claude on the chosen model with the relay channel.
tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -c "$REPO"
tmux send-keys -t "$SESSION" \
  "$CLAUDE --model $MODEL --dangerously-load-development-channels server:relay" C-m

cat <<EOF
Claude launching in tmux session '$SESSION' on model=$MODEL.
Next:
  tmux attach -t $SESSION      # approve the one-time "development channels" prompt
Then DM your bot. Strong work: ask it to "spawn a subagent on Opus to …".
Stop everything:
  tmux kill-session -t $SESSION && pkill -x relayd
EOF
