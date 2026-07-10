#!/bin/sh
# Safely reboots the box, verifying (not guessing) that the relay session
# will resume correctly on boot. Capture is a SEPARATE, prior step -
# run capture-session-id.sh first, deliberately, in the exact session you
# want resumed. This script only verifies that capture already happened
# and that the systemd unit is armed to use it - it does no session-ID
# guessing of its own.
#
# Usage: ./capture-session-id.sh   (run first, in the session to resume)
#        ./reboot-relay-safe.sh    (then this, to actually reboot)
set -eu

STATE_FILE="$HOME/agent-relay/.relay_session_id"

if [ ! -s "$STATE_FILE" ]; then
  echo "ERROR: $STATE_FILE is missing or empty." >&2
  echo "Run capture-session-id.sh first, in the session you want resumed." >&2
  exit 1
fi

session_id=$(cat "$STATE_FILE")
echo "Will resume session: $session_id"

echo "Verifying systemd relay service is enabled..."
if ! systemctl --user is-enabled agent-relay.service >/dev/null 2>&1; then
  echo "ERROR: agent-relay.service is not enabled - it would not restart on boot. Aborting." >&2
  exit 1
fi

echo "All checks passed. Rebooting in 5 seconds (Ctrl+C to abort)..."
sleep 5
sudo reboot
