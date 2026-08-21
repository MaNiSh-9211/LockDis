# 0003. Storage backend: Redis only

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Locks need a store with atomic compare-and-set, TTL/lease support, and high availability. Candidates: Redis, etcd, Consul, ZooKeeper, Postgres. Scope decision: which backends does v1 support?

## Options considered
1. Redis only
2. Multi-backend (Redis + etcd + Postgres + in-memory)
3. etcd only
4. Postgres only

## Decision
Redis only for v1 — but behind a backend-neutral `DistributedLock` trait in `palisade-core`, so etcd/Postgres adapters can be added later without breaking the API.

## Why this is the best option
- **Focus**: one backend done to Jepsen-grade correctness beats three done superficially; our differentiator is rigor, not breadth.
- **Ubiquity**: Redis is already deployed nearly everywhere locks are needed; zero new infrastructure for most adopters.
- **Feature fit**: `SET NX PX`, Lua scripts for atomic grant+fence+release, keyspace notifications, Sentinel for HA — everything the design needs.
- **Performance headroom**: sub-millisecond local ops support our p99 targets at high op rates.
- **Trait boundary preserves optionality**: multi-backend remains a future crate (`palisade-etcd`), not a redesign.

## Why not the alternatives
- **Multi-backend in v1**: triples testing surface (every algorithm × every backend × every failure mode in simulation); dilutes the correctness budget that is the project's whole point.
- **etcd only**: strongest consistency story (Raft-linearized ops), but heavier operational footprint (etcd cluster to run), lower throughput ceiling, and it would sidestep the interesting Redlock/fencing engineering this project exists to demonstrate.
- **Postgres only**: clever (advisory locks) and zero extra infra if you have PG, but throughput-bound by the database and awkward for fair queues/watch semantics; better as a *fence-check adapter* target than as the lock store.

## Consequences
- We inherit Redis's known caveats and must engineer around them explicitly:
  - async replication ⇒ failover can double-grant → mitigated by mandatory fencing tokens (ADR 0005);
  - keyspace notifications are fire-and-forget ⇒ polling fallback always present;
  - Redlock requires N independent masters, not cluster/replicas → enforced in config validation.
- Docs must state supported topologies and their safety/availability trade-offs plainly.
