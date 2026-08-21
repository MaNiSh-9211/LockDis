# 0002. Project form: client library + gRPC service

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
A locking system can ship as (a) an importable library, (b) a standalone network service, or (c) both. Consumers may be Rust services or polyglot fleets.

## Options considered
1. Library + service (both)
2. Client library only
3. Standalone service only

## Decision
Both: `palisade-core`/`palisade-redis` as an embeddable Rust library, plus `palisade-server`, a gRPC service exposing the same semantics over protobuf, with generated clients for other languages.

## Why this is the best option
- **Library path** = lowest latency and zero extra infra for Rust users (direct Redis access).
- **Service path** = any language can consume correct locking without reimplementing algorithms; centralizes policy (max TTL, auth, metrics).
- **One semantic core**: both faces share `palisade-core`, so behavior and tests are identical — no drift between "the lib" and "the service".
- Matches the architecture of proven systems (e.g., how etcd serves both a client lib and a server).

## Why not the alternatives
- **Library only**: excludes non-Rust ecosystems; every language team re-implements (and mis-implements) Redlock/fencing — the status quo we're trying to fix.
- **Service only**: forces an extra network hop and deployment on Rust users who could embed directly; worse p99 for hot paths.

## Consequences
- Must define the gRPC contract early (proto drives SDKs).
- Server adds ops surface: auth (mTLS), health probes, graceful drain — planned in Phase 5.
- Versioning discipline needed between proto, server, and library releases.
