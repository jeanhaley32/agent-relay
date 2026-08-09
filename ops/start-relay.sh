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

# Per-session runtime ceilings, both raised from their small defaults. These are
# cumulative per-process counters (NOT account/usage/billing limits): an
# always-on, multi-day session that spawns a subagent and runs web searches on
# nearly every scheduled trigger exhausts them and gets hard-blocked -
#   "Subagent spawn limit reached (200 of 200)"          (default 200)
#   web search refused, falls back to WebFetch            (default is lower still)
# The counters reset on process (re)start; a higher ceiling just delays the next
# wall. Both must also be injected into the tmux pane in run.sh - a pane inherits
# the tmux SERVER's env, not this launcher's, so setting them here alone is not
# enough (verified via /proc/<pid>/environ). See run.sh for that half.
# MODEL=opus runs the relay session on Opus instead of run.sh's sonnet default.
# Unlike the cap vars above, this needs no pane injection: run.sh reads MODEL
# into $MODEL and expands it into the launch command itself, so the launcher's
# env is sufficient.
exec env SESSION_ID="$session_id" UNATTENDED=1 ISOLATE=0 MODEL=opus \
	CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION=2000 \
	CLAUDE_CODE_MAX_WEB_SEARCHES_PER_SESSION=1000 bash scripts/run.sh
