#!/bin/sh
# Deterministically captures THIS session's exact ID (from the
# CLAUDE_CODE_SESSION_ID env var Claude Code itself sets - no file-mtime
# guessing, no freshness window, no race with some other session
# happening to write a newer transcript). Run this explicitly, in the
# session you actually want reboot-relay-safe.sh to resume - as its own
# deliberate step, separate from and before the reboot itself.
set -eu

STATE_FILE="$HOME/agent-relay/.relay_session_id"

if [ -z "${CLAUDE_CODE_SESSION_ID:-}" ]; then
  echo "ERROR: CLAUDE_CODE_SESSION_ID is not set in this shell - not running inside a Claude Code session?" >&2
  exit 1
fi

echo "$CLAUDE_CODE_SESSION_ID" > "$STATE_FILE"
echo "Captured session ID: $CLAUDE_CODE_SESSION_ID"
echo "Written to: $STATE_FILE"
