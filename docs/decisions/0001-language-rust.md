# 0001. Implementation language: Rust

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
A distributed locking system is safety-critical infrastructure: races, data races, and subtle memory bugs directly translate into mutual-exclusion violations. We need async I/O, high performance, cross-platform single binaries, and maximum confidence in correctness.

## Options considered
1. Rust
2. Go
3. Java/Kotlin (JVM)
4. TypeScript/Node.js

## Decision
Rust.

## Why this is the best option
- **Fearless concurrency**: the borrow checker statically eliminates data races — exactly the bug class that breaks lock implementations.
- **No GC pauses**: a stop-the-world pause in the lock *service* is indistinguishable from a partition; Rust's deterministic destruction removes that entire failure mode from our own code.
- **Modern ecosystem for this exact domain**: Tokio (async), tonic (gRPC), madsim (deterministic simulation), testcontainers, criterion.
- **Distribution story**: static binaries, no runtime/JVM to install for the server component.
- **Signal of rigor**: a correctness-focused project written in the language designed for correctness reinforces the project thesis.

## Why not the alternatives
- **Go**: excellent infra ecosystem (etcd/Consul are Go) and faster iteration, but GC pauses exist, data races are runtime-detected (not prevented), and "most modern" differentiator is weaker.
- **Java/Kotlin**: Redisson already occupies this space; JVM pauses are a core hazard for lock holders; heavy deployment footprint.
- **TypeScript/Node**: fastest prototyping but event-loop blocking, weak typing guarantees for invariants, unsuitable for systems-grade correctness claims.

## Consequences
- Slower initial development; steeper learning curve for contributors.
- Need disciplined use of `unsafe = forbid` in core crates (CI lint).
- Compile times mitigated by workspace splitting (crates/ layout).
