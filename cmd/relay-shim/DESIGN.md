# relay-shim tools — design

The shim registers the MCP tools the Claude session (and its subagents) can
call. Most (`reply`, `schedule_*`, `ack_event`, `force_reauth`) round-trip to
`relayd` over the unix socket because they need daemon-side state or approval
gating. `delegate_to_hypatia` is the exception: it is a stateless, direct
outbound HTTP call and needs no daemon involvement, so it lives entirely in the
shim (`hypatia.go`).

## delegate_to_hypatia

Delegates a fully-specified, mechanical generation task to **Hypatia** — a
self-hosted [Open WebUI](https://hypatia.byatt.io) instance serving **IBM
Granite 4.0-H-Small** over an OpenAI-compatible `POST /api/chat/completions`
endpoint (same JSON schema as OpenAI: `{model, max_tokens, messages[]}` →
`{choices[].message.content}`).

### Why it exists — the delegation policy

Hypatia's output tokens are **free** (the box is already paid for). The
optimization target is therefore **Claude-side dollar cost at equal accuracy**,
not total tokens across both models. The tool earns its keep only when it moves
a long, mechanical, verifiable artifact off the Claude session for a short
prompt.

**Good fit** — a short prompt reliably yields a long, correct, MECHANICAL
artifact whose content is already fully determined by the input:
- boilerplate / scaffold generation from a clear spec
- docstrings / comments for code pasted in full
- rote refactors following one explicit pattern across many sites
- commit messages from an included diff
- format/data conversion (JSON↔YAML, CSV→struct) where the input holds the content

**Poor fit** — anything where the prompt must encode as much reasoning /
design / derivation as the output itself. That thinking cost stays on the
caller regardless, so delegating just adds a lossy round-trip: precise test
cases, edge-case enumeration, precomputed exact values, open-ended design,
judgement calls.

The fit criteria are duplicated into the tool's MCP `description` on purpose —
that string is the interface the model reads to decide whether to call, so the
policy has to live there, not only here.

### Hard limits

- **64k input context, 8k output tokens.** The request always sends
  `max_tokens: 8192`; a `finish_reason: "length"` response is annotated so the
  caller knows the artifact was truncated.
- Hypatia **cannot** explore, read files, ask clarifying questions, or spawn
  subagents. The task must arrive complete and unambiguous in the single
  `prompt`, with all needed context/content inlined.

### Parameters

- `prompt` (string, required) — the complete, self-contained task.
- `system_prompt` (string, optional) — terse framing/role for the request.

### Config / setup

Following the repo's `token_env` convention, the secret is never inline in
config — it is read from the environment at runtime:

- `HYPATIA_API_KEY` (**required**) — the Open WebUI API key.
- `HYPATIA_URL` (optional) — override the endpoint (default the byatt.io one).
- `HYPATIA_MODEL` (optional) — override the served model id.

One-time setup: add the key to the gitignored `.env` (retrieved from the
KeePass vault entry *"Open WebUI (hypatia.byatt.io)"*):

```
printf 'HYPATIA_API_KEY=%s\n' \
  "$(printf '%s\n' "$(sudo cat /root/.vessel-vault-key)" \
     | keepassxc-cli show -a Password ~/vessel-log/vault/vessel.kdbx \
       'Open WebUI (hypatia.byatt.io)')" >> .env
```

`scripts/run.sh` sources `.env` (`set -a; . ./.env`) before launching Claude,
so the shim inherits the variable. If the key is unset the tool returns a clear
error and makes no network call.

### Testing

`hypatia_test.go` drives `hypatiaClient.generate` through a mock
`http.RoundTripper` (no network): happy path (asserts auth header, model, 8k
cap, message shaping), empty-system omission, truncation flagging, auth-error
mapping, and the no-key short-circuit.
