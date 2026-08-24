# Architecture

## System Overview

```mermaid
graph TB
    subgraph "Client Layer"
        RS[Rust SDK]
        PY[Python/TS clients]
        WB[Browser demo SPA<br/>vanilla JS · Tailwind CDN]
    end

    subgraph "Edge"
        GW[API Gateway<br/>rate limiting]
        UAM[UAM Service<br/>authentication]
    end

    subgraph "Palisade Service Tier"
        PS[palisade-server<br/>gRPC · mTLS · authz · sessions<br/>watch hub · Prometheus]
        PD[palisade-demo<br/>axum HTTP + WebSocket<br/>embedded single-page frontend]
    end

    subgraph "Storage Backends"
        R[Redis<br/>Lua-guarded CAS<br/>fence counters]
        E[etcd<br/>Raft consensus<br/>MVCC revisions]
    end

    RS --> GW
    PY --> GW
    GW -->|trusted-header| PS
    UAM -->|identity| GW
    WB -->|JSON over HTTP| PD
    PS -->|BackendOps trait| R
    PS -->|BackendOps trait| E
    PD -->|RedisLockManager directly| R
```

## Crate Dependency Graph

```mermaid
graph TD
    CORE[palisade-core<br/>types · traits · fencing]
    REDIS[palisade-redis<br/>all primitives · Redlock]
    ETCD[palisade-etcd<br/>consensus backend]
    PROTO[palisade-proto<br/>wire contract]
    SERVER[palisade-server<br/>gRPC service · demo binary]
    CLIENT[palisade-client<br/>SDK]
    TESTING[palisade-testing<br/>sim · checker]

    REDIS --> CORE
    ETCD --> CORE
    SERVER --> CORE
    SERVER --> REDIS
    SERVER --> PROTO
    CLIENT --> CORE
    CLIENT --> PROTO
    CLIENT --> TESTING
    TESTING --> CORE
```

Two edges worth calling out: `palisade-proto` is generated wire code only — it depends on tonic/prost, **not** on core. And `CLIENT → TESTING` is a real dependency: the SDK runs invariant checks over its own live traffic (see the verification pipeline below).

## Request Flow: Semantic Lock Acquisition

```mermaid
sequenceDiagram
    participant C as Client SDK
    participant S as palisade-server
    participant R as Redis

    C->>S: TryLock(key, predicates)
    S->>R: EVALSHA semantic acquire script
    Note over S,R: one script per acquisition:<br/>predicates are inlined as Lua conditions
    Note over R: ATOMIC: data-hash exists?<br/>+ lock key absent?<br/>+ all predicates true?<br/>+ SET lock token PX ttl<br/>+ INCR fence counter
    R-->>S: {status=1, fence=42}
    S->>C: Granted{token, fence=42}
    Note over C: Handle owns fence #42.<br/>Watchdog renews at ttl/3.
    
    loop Watchdog renewal
        C->>S: Extend(token)
        S->>R: EVALSHA extend.lua
        R-->>S: ok=1
    end
```

## Safety Model: The Fencing Protocol

```mermaid
sequenceDiagram
    participant A as Holder A (fence#7)
    participant Store as Redis/etcd
    participant B as Holder B (fence#8)
    participant DB as Protected Resource

    A->>A: GC pause / SIGSTOP (past TTL)
    Store->>Store: lease expires
    B->>Store: acquire → granted, fence#8
    A->>A: resumes, believes it holds lock
    A->>DB: UPDATE ... WHERE fence > last_fence
    Note over DB: $fence(7) > last_fence(8)? NO<br/>→ REJECTED ✓
    B->>DB: UPDATE ... WHERE fence > last_fence  
    Note over DB: $fence(8) > last_fence(8)? NO... wait<br/>$fence(8) > last_fence(7)? YES<br/>→ ACCEPTED ✓
```

## Session Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Active : RegisterSession(client_id, ttl)
    Active --> Active : Heartbeat(token) [every ttl/3]
    Active --> Sweeping : silence > ttl
    Sweeping --> Released : sweeper releases bound locks
    Active --> Released : CloseSession(token)
    Released --> [*]
```

## Degradation Tiers (Lock Lattice)

```mermaid
graph LR
    N[NORMAL 0-39] -->|store degrades| E[ELEVATED 40-69]
    E -->|worse| C[CRITICAL 70-89]
    C -->|near-total failure| S[SIEGE 90-100]
    S -->|recovery| C
    C -->|recovery| E
    E -->|recovery| N
```

At each tier, consumers adapt:
| Tier | Gateway action | SDK watchdog cadence |
|---|---|---|
| NORMAL | full throughput | ttl/3 (1×) |
| ELEVATED | shed non-critical | ttl/2 (1.5×) |
| CRITICAL | queue new acquires | ttl×1.5 (2×) |
| SIEGE | circuit-break to fallback | ttl×2 (3×) |

## Correctness Verification Pipeline

```mermaid
graph LR
    P[Property Tests<br/>vs live Redis] --> S[Deterministic Sim<br/>200 clean seeds · zero violations]
    S --> LC[Invariant Checker<br/>hash-chained histories]
    LC --> H[Real-Traffic Checking<br/>gRPC e2e through checker]
    H --> CH[Chaos Suite<br/>CLIENT PAUSE blackout<br/>Redlock quorum loss<br/>etcd member stop]
```

## Web Demo Surface

The `palisade-demo` binary (same crate as the gRPC server, separate port)
serves an embedded browser UI and a thin REST/WS façade. It adds **no new
locking semantics**: every mutation runs the identical Lua-guarded scripts
through `RedisLockManager`, so the safety arguments of ADRs 0005/0011 apply
unchanged.

```mermaid
graph LR
    B["Browser SPA<br/>worker grid · fence timeline<br/>contention banner · event log"]
    AX["palisade-demo<br/>axum router · DemoState"]
    HUB["WatchHub<br/>one poller per key"]
    R[("Redis<br/>Lua scripts")]

    B -->|"POST /api/lock · POST /api/unlock"| AX
    B -->|"GET /api/describe/:key<br/>GET /api/locks · GET /api/pressure"| AX
    B -->|"WS /api/watch/:key"| AX
    AX -->|"try_lock_with · unlock_with_token<br/>describe_key · scan_held"| R
    AX -->|subscribe| HUB
    HUB -->|"probe_state every 100 ms"| R
    AX -->|"SPI gauge readout"| B
```

| Demo route | Mirrors gRPC | Notes |
|---|---|---|
| `POST /api/lock` | `TryLock` | returns token + fence; watchdog off so expiry is visible |
| `POST /api/unlock` | `Unlock` | ownership-checked release; stale tokens get `lost` |
| `GET /api/describe/{key}` | `DescribeKey` | held + version + remaining TTL |
| `GET /api/locks?prefix=` | `ListLocks` | admin SCAN introspection |
| `WS /api/watch/{key}` | `Watch` | anonymized events via the shared hub |
| `GET /api/pressure` | — | INV-5 Store Pressure Index readout |

Honest fine print for anyone staring at the demo: watch events are
**level-triggered** — the hub polls each key at 100 ms and broadcasts state
*transitions* (ADR 0029), so a grant released between two probes surfaces
only as a version bump on the next `Freed`. Events never carry holder
tokens (ADR 0022); tabs are just subscribers, and N tabs still cost exactly
one poller per watched key.
