#!/usr/bin/env python3
"""Emit Prometheus metrics about the relay Claude session's login/auth health.

Written to a node-exporter textfile so Prometheus scrapes it and Grafana can
alert. Two independent signals, because they catch different failure modes:

  claude_login_expired            0/1  - REACTIVE. 1 when the live relay tmux
                                          pane is showing Claude's own
                                          "Login expired / Please run /login"
                                          wall. This is the signal that actually
                                          fired on 2026-09-01: the access token
                                          lapsed and the running process failed
                                          to refresh it in-band, even though the
                                          refresh token was still valid for
                                          weeks. Only the pane state catches
                                          that - the credential file looked fine.

  claude_refresh_token_expires_seconds  - PROACTIVE. Seconds until the refresh
                                          token itself expires (~monthly). When
                                          THIS hits zero, no in-process refresh
                                          can save it and a manual /login is
                                          mandatory. Alert a day ahead to
                                          re-login on your own schedule instead
                                          of being surprised.

  claude_access_token_expires_seconds   - context only (access token, ~5h,
                                          normally auto-refreshes; not alarmed
                                          on directly - it's noisy by design).

Never prints token material. Writes atomically (temp + rename) so Prometheus
never scrapes a half-written file. Emits whatever it can; a failure in one
probe never blocks the others.
"""
import json
import os
import subprocess
import sys
import time

CRED = os.path.expanduser("~/.claude/.credentials.json")
TEXTFILE_DIR = os.environ.get("TEXTFILE_DIR", "/var/lib/prometheus/node-exporter")
OUT = os.path.join(TEXTFILE_DIR, "claude_login.prom")
TMUX_SESSION = os.environ.get("RELAY_TMUX", "relay")
# Claude's own login-wall strings. Matching the pane text is deliberately the
# primary detector: it is exactly what a human sees and needs no guess about
# token internals.
WALL_MARKERS = ("Login expired", "Please run /login", "Invalid API key")


def pane_login_expired():
    """1 if the relay pane is showing the login wall, 0 if not, None if unknown."""
    try:
        out = subprocess.run(
            ["tmux", "capture-pane", "-t", TMUX_SESSION, "-p"],
            capture_output=True, text=True, timeout=10)
    except Exception:
        return None  # tmux/pane not available - can't tell, emit nothing
    if out.returncode != 0:
        return None
    text = out.stdout
    # Only the tail matters: an old "Login expired" scrolled far up but since
    # recovered should not latch the alert on. Look at the last ~40 lines, which
    # is where the current prompt/status lives.
    tail = "\n".join([ln for ln in text.splitlines() if ln.strip()][-40:])
    return 1 if any(m in tail for m in WALL_MARKERS) else 0


def token_expiries():
    """Return (access_secs, refresh_secs) remaining, each None if unavailable."""
    try:
        d = json.load(open(CRED))
    except Exception:
        return None, None
    oauth = d.get("claudeAiOauth", d)
    now_ms = time.time() * 1000

    def secs(key):
        v = oauth.get(key)
        try:
            return (int(v) - now_ms) / 1000.0
        except (TypeError, ValueError):
            return None

    return secs("expiresAt"), secs("refreshTokenExpiresAt")


def main():
    lines = []

    def metric(name, value, help_text, mtype="gauge"):
        if value is None:
            return
        lines.append("# HELP %s %s" % (name, help_text))
        lines.append("# TYPE %s %s" % (name, mtype))
        lines.append("%s %s" % (name, value))

    expired = pane_login_expired()
    metric("claude_login_expired", expired,
           "1 if the relay Claude pane is showing the login/auth wall, else 0")

    access_s, refresh_s = token_expiries()
    metric("claude_access_token_expires_seconds", None if access_s is None else round(access_s),
           "Seconds until the Claude access token expires (auto-refreshes; context only)")
    metric("claude_refresh_token_expires_seconds", None if refresh_s is None else round(refresh_s),
           "Seconds until the Claude refresh token expires (manual /login required at zero)")

    # A heartbeat so a silently-dead collector is itself detectable (stale
    # metric -> the freshness of this timestamp stops advancing).
    metric("claude_login_check_timestamp_seconds", round(time.time()),
           "Unix time this collector last ran", mtype="counter")

    payload = "\n".join(lines) + "\n"
    try:
        os.makedirs(TEXTFILE_DIR, exist_ok=True)
        tmp = OUT + ".$$.tmp".replace("$$", str(os.getpid()))
        with open(tmp, "w") as f:
            f.write(payload)
        os.rename(tmp, OUT)  # atomic: Prometheus never sees a partial file
    except PermissionError:
        # Not writable (e.g. run as the wrong user) - print to stdout so the
        # systemd journal captures it and the failure is visible, not silent.
        sys.stdout.write(payload)
        sys.stderr.write("check-claude-login: cannot write %s (permission)\n" % OUT)
        sys.exit(1)


if __name__ == "__main__":
    main()
