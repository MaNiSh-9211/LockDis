# 0022. Wire contract style: outcome oneofs + anonymized watch events

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Two contract questions: how should expected outcomes (held, timed out, lost) be represented, and what may a watcher see?

## Options considered
1. Oneof result types for expected outcomes; anonymized watch events (chosen)
2. gRPC status codes for everything
3. Watch events carrying holder tokens

## Decision
`LockOutcome`, `UnlockResponse`, and `ExtendResponse` carry explicit `oneof result` variants for *expected* outcomes; gRPC status codes are reserved for *infrastructure* failures (bad config, backend down). `Watch` streams emit only `Acquired`/`Freed` — never tokens.

## Why this is the best option
- **Expected vs exceptional is a type-level distinction**: "lock is held" is not an error; modeling it as `Held` in a oneof makes client code exhaustive (`match`) instead of string-matching status details, and keeps metrics/alerting honest (status-code error rates stay meaningful).
- **Tokens are release authority**: streaming them to every watcher would let any subscriber unlock any key — a privilege-escalation hole disguised as observability. Anonymized events preserve the useful signal (state changed) without leaking capability.
- **Evolvable**: oneofs extend by adding variants (backward-compatible); status-code conventions rot into folklore.

## Why not the alternatives
- **Status codes for outcomes**: forces `NOT_FOUND`-style abuse, per-language detail-parsing conventions, and conflates contention with malfunction in dashboards.
- **Token-bearing events**: rejected on security grounds above; if per-holder visibility is ever needed it belongs behind authorization, not in a public stream.

## Consequences
- Clients must handle all oneof variants; generated types make non-exhaustive matches a compile error — enforced, not hoped for.
- `Lost` responses currently zero the fence value over the wire (the server does not know it); the client-side handle retains its own last-known fence for downstream checks.
- Multi-language stubs inherit these guarantees automatically from the single `.proto`.
