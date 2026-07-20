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
     Kept deliberately terse - the mechanism ("a plain-text answer stays in the
     terminal and is NOT delivered") is already appended to every injected
     message, so restating it here only burns context on every detection. The
     only genuinely new information is that THIS answer was not delivered.

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
import re
import sys
import time
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


# Every output must declare who it is for: "[to: terminal]" for internal
# reasoning, "[to: <chat_id>]" for a person. Declaring intent is what makes this
# decidable - without it, "narration" and "an answer the user never got" are
# indistinguishable in the transcript, which is what produced both missed drift
# and 73 duplicate resends.
DECLARATION = re.compile(r"^\s*\[to:\s*([^\]]{1,64})\]", re.I)


def declaration(text):
    """Return the declared recipient, or None if the text declares none."""
    m = DECLARATION.match(text.strip())
    return m.group(1).strip().lower() if m else None


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

    # The Stop hook fires before the just-finished assistant turn is flushed to
    # the transcript: at firing time the tail's last record is still the USER
    # message. Reading immediately therefore analyses a stale view and can never
    # see the turn being judged - which is exactly why live drift tests came
    # back clean while manual runs on the settled file detected it correctly.
    # Wait (briefly, bounded) for an assistant record to appear after the last
    # user record before deciding.
    def last_kinds(recs):
        last_user = last_assistant = -1
        for i, r in enumerate(recs):
            if r.get("type") == "user":
                last_user = i
            elif r.get("type") == "assistant":
                last_assistant = i
        return last_user, last_assistant

    records = load(tail_only=True)
    if records is None:
        return
    deadline = time.time() + 5.0
    while time.time() < deadline:
        lu, la = last_kinds(records)
        if la > lu:
            break  # the assistant turn has landed; safe to judge
        time.sleep(0.25)
        fresh = load(tail_only=True)
        if fresh is not None:
            records = fresh
    if os.environ.get("DRIFT_DEBUG"):
        try:
            tail = records[-3:]
            with open("/tmp/drift-debug.log", "a") as dbg:
                dbg.write("--- fired %s size=%d\n" % (
                    __import__("datetime").datetime.now().strftime("%H:%M:%S"),
                    os.path.getsize(transcript_path)))
                for r in tail:
                    c = r.get("message", {}).get("content")
                    kind = "text" if isinstance(c, list) and any(
                        isinstance(x, dict) and x.get("type") == "text" for x in c) else "other"
                    if isinstance(c, list) and any(isinstance(x, dict) and x.get("type") == "tool_use"
                                                   and x.get("name") == REPLY_TOOL_NAME for x in c):
                        kind = "REPLY"
                    dbg.write("    %s %s %s\n" % (r.get("timestamp", "")[11:19], r.get("type"), kind))
        except Exception:
            pass
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

    # Walk the turn and classify each substantial output by its declaration.
    reply_sent = False
    undeclared = False        # output with no recipient at all
    addressed_unsent = False  # addressed to a person but never put through reply
    for rec in records[last_channel_idx + 1:]:
        if rec.get("type") != "assistant":
            continue
        content = rec.get("message", {}).get("content")
        if has_reply_tool_call(content):
            reply_sent = True
            continue
        text = message_text(content).strip()
        if len(text) < MIN_TEXT_LEN:
            continue
        who = declaration(text)
        if who is None:
            undeclared = True
        elif who != "terminal":
            # Addressed to a person. Only real if nothing was ever sent for this
            # event - trailing "[to: terminal]" narration after a genuine reply
            # must never trigger a resend (that was the duplicate bug).
            addressed_unsent = True

    # Pull chat_id out of the channel tag so the feedback can name the exact
    # destination instead of making the model go re-find it.
    chat_id = ""
    m = re.search(r'chat_id="([^"]+)"',
                  message_text(records[last_channel_idx].get("message", {}).get("content")))
    if m:
        chat_id = m.group(1)

    if not (undeclared or (addressed_unsent and not reply_sent)):
        return

    try:
        req = urllib.request.Request(DRIFT_WEBHOOK, method="POST", data=b"")
        urllib.request.urlopen(req, timeout=2)
    except Exception:
        pass  # relayd may not be running (dev/test) - never block on this

    target = ' with chat_id="%s"' % chat_id if chat_id else ""
    if addressed_unsent and not reply_sent:
        reason = ("You addressed output to a person but never called "
                  "mcp__relay__reply, so it was not delivered. Send it now%s." % target)
    else:
        # Undeclared: ask for the declaration, and deliberately do NOT say
        # "resend" - an imperative here made the model re-send messages it had
        # already delivered.
        reason = ("Declare a recipient on every output: \"[to: terminal]\" for internal "
                  "notes, or \"[to: <chat_id>]\" for a person (which must also go through "
                  "mcp__relay__reply). Your last output declared none%s." % target)
    print(json.dumps({"decision": "block", "reason": reason}))


if __name__ == "__main__":
    main()
