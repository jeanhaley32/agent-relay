#!/usr/bin/env bash
# One-command launch of the relay stack:
#   - builds the daemon + shim
#   - starts relayd (loads TELEGRAM_BOT_TOKEN from .env)
#   - launches Claude in a tmux session on the chosen model (default: sonnet),
#     with the relay channel registered
#
# Safety defaults (all have escape valves — env vars):
#   ISOLATE=1   run Claude in a clean workspace with NO secrets in reach (default).
#               Set ISOLATE=0 to run in the repo instead (full repo access; only
#               for a trusted operator).
#   WORKSPACE=  override the isolated workspace dir (default ~/.agent-relay/workspace).
#   SECURITY=   security profile (default security.yaml → falls back to the example).
#               Set mode: full in that file for unrestricted tools.
#   MODEL=      session model (default sonnet). e.g. MODEL=opus.
#   CONTINUE=1  resume the previous session in the working dir (default). Claude
#               always launches in the same dir so history is found; CONTINUE=0
#               starts fresh (use it on the very first run).
#
#   bash scripts/run.sh                    # safe defaults
#   ISOLATE=0 bash scripts/run.sh          # run in the repo (trusted operator)
#   MODEL=opus bash scripts/run.sh
#
# After it starts, attach to approve the one-time dev-channel prompt:
#   tmux attach -t relay
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

MODEL="${MODEL:-sonnet}"
SESSION="${SESSION:-relay}"
CONFIG="${CONFIG:-config.json}"
SOCK="${SOCK:-${XDG_RUNTIME_DIR:-/tmp}/agent-relay.sock}"  # private per-user dir if available
SECURITY="${SECURITY:-security.yaml}"
ISOLATE="${ISOLATE:-1}"                              # 1 = isolated workspace, 0 = run in repo
WORKSPACE="${WORKSPACE:-$HOME/.agent-relay/workspace}"
CONTINUE="${CONTINUE:-1}"                            # 1 = resume the previous session in this dir

# Bot token from .env (never committed).
[ -f .env ] && { set -a; . ./.env; set +a; }
: "${TELEGRAM_BOT_TOKEN:?set TELEGRAM_BOT_TOKEN (e.g. in .env)}"

# Resolve claude even if PATH is missing ~/.local/bin.
CLAUDE="$(command -v claude || echo "$HOME/.local/bin/claude")"
[ -x "$CLAUDE" ] || { echo "error: claude not found (looked on PATH and ~/.local/bin)" >&2; exit 1; }

echo "building binaries…"
go build -o bin/relayd ./cmd/relayd
go build -o bin/relay-shim ./cmd/relay-shim
go build -o bin/apply-security ./cmd/apply-security

[ -f "$SECURITY" ] || SECURITY="security.example.yaml"

# Choose where Claude runs and generate its config there.
#   Isolated: a clean workspace containing ONLY .mcp.json + .claude/settings.json.
#   Your .env / config.json / allowlist.json / source stay in the repo, out of reach.
if [ "$ISOLATE" = "1" ]; then
  mkdir -p "$WORKSPACE/.claude"
  SEC_FLAGS="$(./bin/apply-security --config "$SECURITY" --settings "$WORKSPACE/.claude/settings.json")"
  cat > "$WORKSPACE/.mcp.json" <<JSON
{
  "mcpServers": {
    "relay": { "command": "$REPO/bin/relay-shim", "args": ["--socket", "$SOCK"] }
  }
}
JSON
  CLAUDE_DIR="$WORKSPACE"
  ISO_NOTE="isolated workspace ($WORKSPACE)"
else
  SEC_FLAGS="$(./bin/apply-security --config "$SECURITY")"   # writes ./.claude/settings.json
  CLAUDE_DIR="$REPO"
  ISO_NOTE="repo (ISOLATE=0 — Claude can see your secrets)"
fi

# Start relayd if not already running (the shim auto-reconnects, so an existing
# daemon is fine to leave up).
if pgrep -x relayd >/dev/null; then
  echo "relayd already running — leaving it up (shim will reconnect)"
else
  rm -f "$SOCK"
  nohup ./bin/relayd --config "$CONFIG" > relayd.log 2>&1 &
  echo "relayd started (pid $!) → logs in relayd.log"
fi

# Resume the previous conversation in this directory, so the bot picks up where
# it left off across restarts. Claude Code keys session history to the working
# directory — which is why we always launch in the same CLAUDE_DIR. Set
# CONTINUE=0 for a fresh session (use it on the very first run, no history yet).
CONT_FLAG=""
CONT_NOTE="fresh session"
if [ "$CONTINUE" = "1" ]; then
  CONT_FLAG="--continue"
  CONT_NOTE="continuing previous session (CONTINUE=0 for fresh)"
fi

# (Re)launch Claude in the (stable) chosen directory with the relay channel.
tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -c "$CLAUDE_DIR"
tmux send-keys -t "$SESSION" \
  "$CLAUDE --model $MODEL $SEC_FLAGS $CONT_FLAG --dangerously-load-development-channels server:relay" C-m

cat <<EOF
Claude launching in tmux session '$SESSION'.
  model:    $MODEL
  security: $SECURITY
  workdir:  $CLAUDE_DIR  ($ISO_NOTE)
  session:  $CONT_NOTE
Next:
  tmux attach -t $SESSION      # approve the one-time "development channels" prompt
Then DM your bot. Strong work: ask it to "spawn a subagent on Opus to …".
Stop everything:
  tmux kill-session -t $SESSION && pkill -x relayd
EOF
