# 0008. RPC framework: tonic (gRPC)

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
`palisade-server` (ADR 0002) needs a wire protocol for polyglot clients, with streaming (Watch), auth (mTLS), health probes, and generated SDKs.

## Options considered
1. gRPC via tonic (+ prost)
2. REST/JSON (axum)
3. Custom TCP protocol
4. Apache Thrift / Cap'n Proto

## Decision
gRPC over HTTP/2 using tonic + prost, with `grpc-health` protocol support and mTLS.

## Why this is the best option
- **Polyglot SDKs for free**: protoc plugins generate Python/Go/TS/Java clients from our single `.proto` — the entire multi-language story falls out of the choice.
- **Streaming is native**: the `Watch` feature (lock acquired/released/expired events) is a server-streaming RPC, not a bolted-on WebSocket layer.
- **Ecosystem fit**: tonic is the standard Rust gRPC stack; integrates cleanly with Tokio, tower middleware (timeouts, load-shedding, auth layers), and tracing.
- **Ops maturity**: health-check protocol for k8s, interceptors for metrics/auth, mTLS standard practice — everything a "modern service" is expected to have.

## Why not the alternatives
- **REST/JSON**: friendliest debugging, but no streaming semantics without hacks, larger payloads on the hot path, and hand-maintained client SDKs in every language — exactly the toil protobuf exists to remove.
- **Custom TCP**: maximum performance and control, but kills polyglot adoption and forces us to own framing/versioning/security — off-thesis work.
- **Thrift/Cap'n Proto**: capable but niche ecosystems; far fewer maintained client generators and ops tooling than gRPC.

## Consequences
- `.proto` becomes a public, versioned contract: backward-compatibility discipline (buf-style lint/breaking-change checks) enters CI.
- Generated code lives in `palisade-proto`; server/client crates depend on it, never on hand-written message types.
- Deadlines/cancellation map onto lock try-lock timeouts — must be tested explicitly so an aborted RPC never leaks a lease without watchdog cleanup.
