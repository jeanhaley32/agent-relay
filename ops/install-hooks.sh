#!/bin/sh
# Register this repo's Claude Code hooks into .claude/settings.json.
#
# .claude/settings.json is gitignored (it holds local/session state), so hook
# registration does NOT survive a fresh clone - which is exactly how the
# reply-drift detector sat inert and unnoticed: the script and its webhook
# existed, but hooks was {} so it never ran once. Run this after cloning, or
# any time `jq '.hooks' .claude/settings.json` comes back empty/null.
set -eu
cd "$(dirname "$0")/.."
S=.claude/settings.json
[ -f "$S" ] || echo '{}' > "$S"
tmp=$(mktemp)
jq --arg cmd "python3 $(pwd)/scripts/detect-reply-drift.py 2>/dev/null" '
  .hooks //= {} |
  .hooks.Stop //= [] |
  # idempotent: drop any prior registration of this same script first
  .hooks.Stop |= map(select((.hooks // []) | map(.command // "") | any(test("detect-reply-drift")) | not)) |
  .hooks.Stop += [{"hooks":[{"type":"command","command":$cmd,"timeout":15,
                             "statusMessage":"Checking the last answer was actually sent…"}]}]
' "$S" > "$tmp" && mv "$tmp" "$S"
echo "registered reply-drift Stop hook in $S"
jq -e '.hooks.Stop[].hooks[].command' "$S" >/dev/null && echo "verified"
