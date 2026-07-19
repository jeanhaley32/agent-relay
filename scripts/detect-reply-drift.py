#!/usr/bin/env python3
"""Claude Code Stop hook: detect a turn that answered a relay <channel> event
in plain terminal text without ever calling the mcp__relay__reply tool.

Background (2026-07-14 incident): the model answered a Telegram question
directly in the terminal instead of via the reply tool. Nothing failed - no
send was ever attempted - so every existing send/failure metric stayed at
zero while the user just never got an answer. This hook makes that failure
mode mechanically detectable instead of relying on the model remembering.

Heuristic: find the last user turn in the transcript that contains a relay
<channel source="relay" ...> tag. If any assistant turn after that point
produced non-trivial text (a real answer, not just tool-call chatter) but no
mcp__relay__reply tool_use appears anywhere after the channel event, this is
drift - report it to relayd, which counts it as relayd_reply_drift_total.

Deliberately observability-only, not auto-forwarding: the orphaned text
could be a draft or mid-thought fragment, not necessarily what the model
would send if it noticed the miss itself (Jean's explicit call, 2026-07-14).

Respects stop_hook_active to avoid ever blocking/looping - this hook never
blocks the stop at all, it only reports after the fact.
"""
import json
import os
import sys
import urllib.request

# Shared port convention: relayd and this hook both read RELAY_WEBHOOK_ADDR and
# both fall back to the same default, so changing the port in one place moves
# both. (Previously each hardcoded 127.0.0.1:9210 independently, which silently
# breaks the hook the moment relayd is moved to another port.)
WEBHOOK_ADDR = os.environ.get("RELAY_WEBHOOK_ADDR", "127.0.0.1:9210")
DRIFT_WEBHOOK = f"http://{WEBHOOK_ADDR}/webhook/reply-drift"

# Only the tail of the transcript is needed: we care about the LAST relay event
# and the turns after it. Reading the whole file cost 1.7s per turn on a 104MB
# session transcript - and this hook runs after EVERY turn, on a file that only
# grows. Read a tail window instead, and fall back to the full file if the
# window happens not to contain a relay event (very long turns).
TAIL_BYTES = 4 * 1024 * 1024
CHANNEL_MARKER = '<channel source="relay"'
REPLY_TOOL_NAME = "mcp__relay__reply"
MIN_TEXT_LEN = 20  # ignore trivial/whitespace-only assistant text blocks


def message_text(content):
    """Extract all plain-text blocks from a message's content (str or list of blocks)."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        print(f"message_text: unexpected content type {type(content)!r}", file=sys.stderr)
        return ""
    parts = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
    return "\n".join(parts)


def has_reply_tool_call(content):
    if not isinstance(content, list):
        return False
    for block in content:
        if isinstance(block, dict) and block.get("type") == "tool_use" and block.get("name") == REPLY_TOOL_NAME:
            return True
    return False


def main():
    try:
        hook_input = json.load(sys.stdin)
    except Exception:
        return  # malformed hook input - nothing to do, never block

    if hook_input.get("stop_hook_active"):
        return  # already in a forced-continuation loop; never re-trigger

    transcript_path = hook_input.get("transcript_path")
    if not transcript_path:
        return

    def load(tail_only):
        """Parse transcript records, optionally only the tail window."""
        try:
            with open(transcript_path, "rb") as f:
                if tail_only:
                    size = os.fstat(f.fileno()).st_size
                    if size > TAIL_BYTES:
                        f.seek(size - TAIL_BYTES)
                        f.readline()  # discard the partial first line
                raw = f.read()
        except OSError:
            return None
        out = []
        for line in raw.split(b"\n"):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        return out

    def last_channel(records):
        idx = None
        for i, rec in enumerate(records):
            if rec.get("type") != "user":
                continue
            if CHANNEL_MARKER in message_text(rec.get("message", {}).get("content")):
                idx = i
        return idx

    records = load(tail_only=True)
    if records is None:
        return
    last_channel_idx = last_channel(records)
    if last_channel_idx is None:
        # No relay event in the tail window - re-read the whole file before
        # concluding this session has never seen one, so correctness never
        # depends on the window size.
        records = load(tail_only=False)
        if records is None:
            return
        last_channel_idx = last_channel(records)

    if last_channel_idx is None:
        return  # this session has never seen a relay event - nothing to check

    reply_sent = False
    produced_text = False
    for rec in records[last_channel_idx + 1:]:
        if rec.get("type") != "assistant":
            continue
        content = rec.get("message", {}).get("content")
        if has_reply_tool_call(content):
            reply_sent = True
            break  # satisfied for the rest of this window, no need to scan further
        text = message_text(content)
        if len(text.strip()) >= MIN_TEXT_LEN:
            produced_text = True

    if produced_text and not reply_sent:
        try:
            req = urllib.request.Request(DRIFT_WEBHOOK, method="POST", data=b"")
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass  # relayd may not be running (e.g. dev/test session) - never block on this


if __name__ == "__main__":
    main()
