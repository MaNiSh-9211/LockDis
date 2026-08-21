# Architecture Decision Records

Every feature and architectural decision gets an ADR here: **what we picked, why it's the best option, and which alternatives were rejected and why.**

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](0001-language-rust.md) | Implementation language: Rust | Accepted |
| [0002](0002-project-form-library-plus-grpc-service.md) | Project form: client library + gRPC service | Accepted |
| [0003](0003-storage-backend-redis-only.md) | Storage backend: Redis only | Accepted |
| [0004](0004-correctness-strategy-jepsen-style.md) | Correctness strategy: Jepsen-style layered testing | Accepted |
| [0005](0005-fencing-tokens-mandatory.md) | Fencing tokens are mandatory on every grant | Accepted |
| [0006](0006-deterministic-simulation-madsim.md) | Deterministic simulation via madsim | Accepted |
| [0007](0007-async-runtime-tokio.md) | Async runtime: Tokio | Accepted |
| [0008](0008-rpc-framework-tonic.md) | RPC framework: tonic (gRPC) | Accepted |
| [0009](0009-redis-client-crate.md) | Redis client crate: `redis` (tokio-comp) | Accepted |
| [0010](0010-waiting-strategy-polling.md) | Contended waiting: polling with bounded interval | Accepted |
| [0011](0011-release-semantics-idempotent-detached-drop.md) | Release semantics: idempotent + detached drop-release | Accepted |
| [0012](0012-fence-counter-retention-rolling-ttl.md) | Fence counter retention: rolling TTL at 10× lease | Accepted |

## Template

```markdown
# NNNN. Title

- **Status:** Proposed | Accepted | Superseded by NNNN
- **Date:** YYYY-MM-DD

## Context
What problem forces a decision? Constraints?

## Options considered
1. ...
2. ...

## Decision
What we picked.

## Why this is the best option
Concrete reasons tied to our goals (safety, modernity, DX, ops).

## Why not the alternatives
Per alternative: the specific reason it lost.

## Consequences
Positive, negative, and follow-up work.
```
