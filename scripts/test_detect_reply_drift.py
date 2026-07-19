#!/usr/bin/env python3
"""Tests for detect-reply-drift.py. Run: python3 scripts/test_detect_reply_drift.py

Each case encodes a bug that actually shipped, so a regression is caught here
rather than by a user silently not receiving replies.
"""
import importlib.util, json, os, subprocess, sys, tempfile

HOOK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "detect-reply-drift.py")

CHANNEL = {"type": "user", "timestamp": "2026-07-19T21:00:00Z", "message": {"content": [
    {"type": "text", "text": '<channel source="relay" chat_id="6369276467">question</channel>'}]}}
TEXT = {"type": "assistant", "timestamp": "2026-07-19T21:00:05Z", "message": {"content": [
    {"type": "text", "text": "A substantive answer that was only written to the terminal."}]}}
REPLY = {"type": "assistant", "timestamp": "2026-07-19T21:00:06Z", "message": {"content": [
    {"type": "tool_use", "name": "mcp__relay__reply",
     "input": {"chat_id": "6369276467", "text": "sent"}}]}}


def run(records, stop_hook_active=False):
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
        path = f.name
    try:
        out = subprocess.run(
            [sys.executable, HOOK],
            input=json.dumps({"transcript_path": path, "stop_hook_active": stop_hook_active}),
            capture_output=True, text=True, timeout=30).stdout.strip()
    finally:
        os.unlink(path)
    return json.loads(out) if out else None


def check(name, got, want_block):
    blocked = got is not None and got.get("decision") == "block"
    ok = blocked == want_block
    print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    return ok


results = []
# Plain drift: answered in terminal text, never sent.
results.append(check("text only -> block", run([CHANNEL, TEXT]), True))
# Answered properly.
results.append(check("reply only -> silent", run([CHANNEL, REPLY]), False))
# Thinking aloud, then actually sending: not drift.
results.append(check("text then reply -> silent", run([CHANNEL, TEXT, REPLY]), False))
# The bug that let 6 live tests pass while the user got nothing: the old code
# broke out of the scan at the first reply, so trailing unsent text was never
# examined. Drift is "the turn ENDED with unsent text", not "no reply existed".
results.append(check("REPLY then text -> block  (regression: 2026-07-19)", run([CHANNEL, REPLY, TEXT]), True))
# Loop guard: never nudge twice, or the model can be trapped block/continue.
results.append(check("stop_hook_active -> silent", run([CHANNEL, TEXT], stop_hook_active=True), False))
# No relay traffic in this session at all: nothing to judge.
results.append(check("no channel event -> silent", run([TEXT]), False))

print(f"\n  {sum(results)}/{len(results)} passed")
sys.exit(0 if all(results) else 1)
