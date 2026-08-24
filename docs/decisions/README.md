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
| [0013](0013-watchdog-renewal-design.md) | Watchdog auto-renewal: ttl/3 cadence, weak-ref task | Accepted |
| [0014](0014-lost-lease-signaling-watch-channel.md) | Lost-lease signaling: watch channel broadcast | Accepted |
| [0015](0015-reentrant-hash-hold-count.md) | Reentrant lock: hash hold count, caller-supplied owner | Accepted |
| [0016](0016-rw-lock-design.md) | Read-write lock: reader-preferring, no promotions | Accepted |
| [0017](0017-semaphore-zset-server-time.md) | Semaphore: ZSET scored by Redis-side expiry instants | Accepted |
| [0018](0018-fair-queue-heartbeat-handoff.md) | Fair queue: FIFO list, heartbeats, handoff-on-release | Accepted |
| [0019](0019-multi-lock-sorted-rollback.md) | Multi-lock: sorted acquisition + rollback retry | Accepted |
| [0020](0020-redlock-independent-masters-fence-allocator.md) | Redlock: independent masters + dedicated fence allocator | Accepted |
| [0021](0021-wire-pass-through-client-renewal.md) | Wire semantics: stateless pass-through, client-side renewal | Accepted |
| [0022](0022-outcome-oneofs-anonymized-watch.md) | Wire contract style: outcome oneofs + anonymized watch | Accepted |
| [0023](0023-health-drain-lifecycle.md) | Readiness & drain: readiness flag + health protocol | Accepted |
| [0024](0024-mtls-optional-ring-tls.md) | Transport security: optional mTLS, ring-based TLS stack | Accepted |
| [0025](0025-bespoke-deterministic-simulation.md) | Deterministic simulation: bespoke seeded scheduler | Accepted |
| [0026](0026-consensus-core-etcd-backend.md) | Consensus core: first-class etcd backend | Accepted |
| [0027](0027-server-sessions.md) | Server-authoritative sessions | Accepted |
| [0028](0028-authz-quotas-audit.md) | Authorization & multi-tenancy: ACLs, quotas, audit | Accepted |
| [0029](0029-watch-fanout-hub.md) | Watch fan-out: hub with one poller per key | Accepted |
| [0030](0030-admin-introspection-durability.md) | Admin introspection + durability guidance | Accepted |
| [0031](0031-backend-abstraction-grpc.md) | BackendOps abstraction for etcd-over-gRPC | Proposed |

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
