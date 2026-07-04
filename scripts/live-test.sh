#!/usr/bin/env bash
# Live end-to-end test of the channel-spike against real Claude Code, in a
# 3-pane tmux window. Uses an absolute path to claude so it works even when the
# tmux server's PATH is missing ~/.local/bin (e.g. a server started from i3).
#
#   bash scripts/live-test.sh
#
# Then: press Enter in the bottom-right pane to inject a message. Watch the left
# pane (Claude reacts) and the top-right pane (Claude's reply streams back).
set -euo pipefail

SESSION="relay"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${PORT:-8799}"

# Resolve claude robustly: PATH first, then the known install location.
CLAUDE="$(command -v claude || true)"
[ -z "$CLAUDE" ] && CLAUDE="$HOME/.local/bin/claude"
if [ ! -x "$CLAUDE" ]; then
  echo "error: claude binary not found (looked on PATH and $HOME/.local/bin)" >&2
  exit 1
fi

# Build the spike so .mcp.json's ./bin/channel-spike exists and is current.
( cd "$REPO" && go build -o bin/channel-spike ./cmd/channel-spike )

# Fresh session.
tmux kill-session -t "$SESSION" 2>/dev/null || true

# Pane 0 (left): Claude Code with our development channel.
tmux new-session -d -s "$SESSION" -c "$REPO"
tmux send-keys -t "$SESSION" \
  "$CLAUDE --dangerously-load-development-channels server:channel-spike" C-m

# Pane 1 (top-right): stream outbound replies once the port is up.
tmux split-window -h -t "$SESSION" -c "$REPO"
tmux send-keys -t "$SESSION" \
  "until curl -sN localhost:$PORT/events; do sleep 0.5; done" C-m

# Pane 2 (bottom-right): inject command pre-typed — press Enter when ready.
tmux split-window -v -t "$SESSION" -c "$REPO"
tmux send-keys -t "$SESSION" \
  "curl -s localhost:$PORT -d 'what files are in this directory?'"

tmux select-layout -t "$SESSION" tiled
tmux select-pane -t "$SESSION".0
tmux attach -t "$SESSION"
