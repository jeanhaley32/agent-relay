# agent-relay

A Go broker that connects **message frontends** (Telegram, CLI, webhooks) to **agent
backends** (Claude Code, Ollama, OpenAI) through one symmetric interface — so either side
is swappable without touching the other.

```
   Telegram ─┐                              ┌── Claude Code  (stdio channel MCP)
   CLI      ─┼────▶  RELAY DAEMON (broker) ─┼── Ollama       (localhost:11434/v1)
   Webhook  ─┘         commands + budget    └── OpenAI       (api.openai.com/v1)
     frontend Endpoints        gate            backend Endpoints
```

## Idea

Everything is an `Endpoint` — a thing that emits and accepts messages. A `Broker` binds two
endpoints and pumps between them, intercepting slash commands and gating model turns through
an account-tier rate limiter + circuit breaker. Both sides are the same interface, so a
frontend (Telegram) and a backend (Claude/Ollama) are peers, not special cases.

See [`DESIGN.md`](./DESIGN.md) for the full architecture, token/turn economics, auth model,
and milestones.

## Status

Early PoC. Working today:

- `internal/relay` — symmetric `Endpoint`, `Message`, and `Broker`
- `internal/budget` — account-tier rate limit + circuit breaker (unit-tested)
- `internal/command` — slash-command control plane (`/help`, `/rate`, `/tier`, `/pause`…)
- `internal/mcp` — reusable, dependency-free MCP-over-stdio JSON-RPC server
- `internal/endpoint/{cli,echo}` — demo frontend/backend endpoints

Next: prove the Go↔Claude Code channel dialect (`internal/channel` + `cmd/channel-spike`),
then real Telegram and Ollama endpoints.

## Try it

```bash
# control-plane demo: CLI frontend + echo backend + budget + commands
go build ./...
go test ./...
printf '/tier pro\n/rate\nhello\n/status\n' | go run ./cmd/broker-demo
```

## License

[MIT](./LICENSE) © 2026 Jean Haley
