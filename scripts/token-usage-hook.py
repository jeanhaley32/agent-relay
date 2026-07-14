#!/usr/bin/env python3
"""Claude Code Stop hook: compute real per-conversation token usage from the
session transcript and report it to relayd, replacing the interim chars/4
text-length estimate the Broker uses live.

Background (2026-07-14): the relay's conversation-cap enforcement started
out estimating tokens from message text length (~4 chars/token), the same
heuristic the global budget meter already used. A real spike proved that
badly undercounts a capped conversation's true cost - real usage includes
reasoning and tool-call tokens that never appear in the visible reply text
at all. This hook pulls the real numbers Claude's API already returns (every
assistant transcript record has a "usage" block with input_tokens,
output_tokens, cache_creation_input_tokens) and reports them instead.

Attribution model - "spans": this session is a single shared process
interleaving many people's conversations (Telegram admin, several Discord
contacts, scheduled triggers, alerts), so "everything since chat_id X's last
message" badly over-counts - it picks up unrelated work done in between.
Instead: every <channel source="relay" chat_id="X"> event starts a new
attribution span; all assistant usage from that point until the NEXT channel
event (any chat_id) belongs to that span's chat_id. Spans outside the
configured rolling cap window are ignored, so this naturally matches the
Broker's own window semantics (see relay.go's ConversationCapWindow).

Only reports usage for chat_ids that actually have a configured cap
(read from config.json) - there's no reason to compute or send anything for
uncapped conversations, and relayd's SetConversationUsage is a no-op for
them anyway.
"""
import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone

TOKEN_USAGE_WEBHOOK = "http://127.0.0.1:9210/webhook/token-usage"
CONFIG_PATH = os.environ.get("RELAY_CONFIG", "/home/jeanh/agent-relay/config.json")
CHANNEL_RE = re.compile(r'<channel source="relay"[^>]*\bchat_id="(\d+)"')
DEFAULT_WINDOW_HOURS = 3


def load_capped_chat_ids_and_window():
    try:
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
    except OSError:
        return {}, DEFAULT_WINDOW_HOURS
    caps = cfg.get("budget", {}).get("conversation_caps", {}) or {}
    window_hours = cfg.get("budget", {}).get("conversation_cap_window_hours", DEFAULT_WINDOW_HOURS)
    return caps, window_hours


def message_text(content):
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
    return "\n".join(parts)


def parse_ts(rec):
    ts = rec.get("timestamp")
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def compute_attribution(records, capped_ids, window_hours):
    """Returns {chat_id: total_fresh_tokens} for every capped chat_id, only
    counting spans whose channel event falls within the rolling window."""
    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - window_hours * 3600

    spans = []  # (chat_id, start_idx)
    for i, rec in enumerate(records):
        if rec.get("type") != "user":
            continue
        text = message_text(rec.get("message", {}).get("content"))
        m = CHANNEL_RE.search(text)
        if m and m.group(1) in capped_ids:
            spans.append((m.group(1), i))

    usage = {chat_id: 0 for chat_id in capped_ids}
    for span_i, (chat_id, start) in enumerate(spans):
        span_ts = parse_ts(records[start])
        if span_ts is not None and span_ts.timestamp() < cutoff:
            continue  # this span is outside the rolling window entirely

        # End of this span is either the next channel event of ANY chat_id
        # (capped or not - someone else's conversation still ends this
        # span) or end of transcript. Re-scan all records (not just spans)
        # for the next channel event after `start`.
        end = len(records)
        for j in range(start + 1, len(records)):
            rec = records[j]
            if rec.get("type") != "user":
                continue
            text = message_text(rec.get("message", {}).get("content"))
            if CHANNEL_RE.search(text):
                end = j
                break

        fresh = 0
        for rec in records[start:end]:
            if rec.get("type") == "assistant":
                u = rec.get("message", {}).get("usage")
                if u:
                    fresh += u.get("input_tokens", 0)
                    fresh += u.get("output_tokens", 0)
                    fresh += u.get("cache_creation_input_tokens", 0)
        usage[chat_id] += fresh

    return usage


def main():
    try:
        hook_input = json.load(sys.stdin)
    except Exception:
        return

    if hook_input.get("stop_hook_active"):
        return

    transcript_path = hook_input.get("transcript_path")
    if not transcript_path:
        return

    capped_ids, window_hours = load_capped_chat_ids_and_window()
    if not capped_ids:
        return  # nothing configured to track, don't bother parsing

    try:
        with open(transcript_path) as f:
            lines = f.readlines()
    except OSError:
        return

    records = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    usage = compute_attribution(records, set(capped_ids.keys()), window_hours)
    if not any(usage.values()):
        return

    try:
        body = json.dumps({"usage": usage}).encode()
        req = urllib.request.Request(
            TOKEN_USAGE_WEBHOOK, method="POST", data=body,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=3)
    except Exception:
        pass  # relayd may not be running - never block on this


if __name__ == "__main__":
    main()
