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

On detection it does two things:

  1. Reports to relayd (relayd_reply_drift_total) - the observability half.
  2. Returns decision:"block" with a reason, which for a Stop hook means "do
     not end the turn": the reason is fed back to the model, so it learns that
     the answer it just wrote never reached the user and can resend properly.

Returning the miss to the model - rather than auto-forwarding the orphaned
text - is deliberate. The text could be a draft or mid-thought fragment, so
the model, not the relay, decides what is actually fit to send (Jean's call,
2026-07-14). This closes the loop that made drift self-perpetuating: writing
terminal text produces NO tool result, no error, no signal of any kind, so
without this the model has no way to know it failed and reports success in
good faith.

Loop safety: stop_hook_active short-circuits the whole check, so the model is
nudged at most once per drift episode and can never be trapped in a
block/continue cycle.
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
        # Observability half: count it even if the feedback below is ignored.
        try:
            req = urllib.request.Request(DRIFT_WEBHOOK, method="POST", data=b"")
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass  # relayd may not be running (e.g. dev/test session) - never block on this

        # Feedback half: hand the miss back to the model. For a Stop hook,
        # decision:"block" means "don't end the turn" - reason is delivered to
        # the model, which can then send the answer properly.
        print(json.dumps({
            "decision": "block",
            "reason": (
                "Your last answer was written as plain terminal text and was NEVER DELIVERED "
                "to the user - you did not call mcp__relay__reply, so nothing was sent and no "
                "error was raised. The user is still waiting and has seen nothing. "
                "Send that answer now by calling mcp__relay__reply with the chat_id from the "
                "channel tag. Do not describe what you would send - actually call the tool. "
                "If the text was only internal narration and not meant for the user, call the "
                "reply tool with whatever the real answer is instead."
            ),
        }))


if __name__ == "__main__":
    main()
