#!/bin/sh
# Boot-time relay startup for the claude/tmux side. relayd itself now has
# its own supervised systemd unit (relayd.service, Restart=always, and
# relayd's own Telegram handshake retries with backoff internally) - this
# script no longer needs to wait for or start relayd; systemd ordering
# (agent-relay.service After=relayd.service) plus relayd's own resilience
# covers that independently.
#
# Delegates to the project's own scripts/run.sh rather than reimplementing
# its logic, supplying what a boot-time/unattended launch needs:
#   - SESSION_ID: exact --resume target (captured by capture-session-id.sh),
#     not the fuzzy --continue "most recent" heuristic.
#   - UNATTENDED=1: auto-clears the one-time "development channels" startup
#     modal, since there's no human at boot to press Enter.
set -eu

cd /home/jeanh/agent-relay
session_id=$(cat .relay_session_id)

# CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION raises the per-session subagent spawn
# ceiling from its default of 200. This is an always-on, multi-day session that
# spawns a subagent on nearly every scheduled trigger, so the cumulative
# per-process count reached 200 and hard-blocked all further Task/Agent calls
# ("Subagent spawn limit reached (200 of 200)") - unrelated to any account/usage
# limit. The counter resets on process (re)start; a higher ceiling just delays
# the next wall. 2000 buys roughly 10x the runway before a restart is needed.
exec env SESSION_ID="$session_id" UNATTENDED=1 ISOLATE=0 \
	CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION=2000 bash scripts/run.sh
