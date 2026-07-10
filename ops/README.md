# ops/ — operator scripts

These are **not part of relayd itself** — they're scripts Jean (the operator)
runs by hand, or that systemd invokes on his behalf, to manage the running
relay session on this specific box. They live outside `scripts/` (which is
the project's own build/run tooling) to keep "how the project runs" separate
from "how this particular deployment is operated day to day."

Expect this folder to stay small and personal — one script per operational
behavior we've needed, not a general framework.

## Scripts

- **`capture-session-id.sh`** — run manually, in the exact Claude Code
  session you want to survive a reboot. Reads `$CLAUDE_CODE_SESSION_ID` and
  writes it to `../.relay_session_id`. Deliberately a separate, explicit step
  from the reboot itself — no file-mtime guessing, no "most recent session"
  heuristic, no race with some other session's transcript.

- **`reboot-relay-safe.sh`** — the only sanctioned way to reboot this box.
  Verifies (doesn't guess) that `capture-session-id.sh` already ran and that
  the systemd unit is armed to resume the right session, then reboots.
  **Always run `capture-session-id.sh` first**, and always ask Jean's
  explicit permission before running this — it's a real reboot of the box he
  depends on.

- **`start-relay.sh`** — boot-time entry point for the `agent-relay.service`
  systemd unit (see `ExecStart`). Delegates to `../scripts/run.sh`, adding
  what an unattended boot-time launch needs: the exact `--resume` session id
  captured above, and auto-clearing the one-time interactive startup modal
  (`UNATTENDED=1`) since there's no human at boot to dismiss it.

## Usage

```
./ops/capture-session-id.sh    # run first, deliberately, in the session to resume
./ops/reboot-relay-safe.sh     # then this, to actually reboot
```

`start-relay.sh` isn't meant to be run by hand — it's what
`agent-relay.service` (`~/.config/systemd/user/agent-relay.service`) invokes
on boot.
