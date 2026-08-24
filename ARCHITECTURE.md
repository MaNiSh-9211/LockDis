# Architecture

## System Overview

```mermaid
graph TB
    subgraph "Client Layer"
        RS[Rust SDK]
        PY[Python/TS clients]
    end

    subgraph "Edge"
        GW[API Gateway<br/>rate limiting]
        UAM[UAM Service<br/>authentication]
    end

    subgraph "Palisade gRPC Tier"
        PS[palisade-server<br/>mTLS · authz · sessions<br/>watch hub · Prometheus]
    end

    subgraph "Storage Backends"
        R[Redis<br/>Lua-guarded CAS<br/>fence counters]
        E[etcd<br/>Raft consensus<br/>MVCC revisions]
    end

    RS --> GW
    PY --> GW
    GW -->|trusted-header| PS
    UAM -->|identity| GW
    PS -->|BackendOps trait| R
    PS -->|BackendOps trait| E
```

## Crate Dependency Graph

```mermaid
graph TD
    CORE[palisade-core<br/>types · traits · fencing]
    REDIS[palisade-redis<br/>all primitives · Redlock]
    ETCD[palisade-etcd<br/>consensus backend]
    PROTO[palisade-proto<br/>wire contract]
    SERVER[palisade-server<br/>gRPC service]
    CLIENT[palisade-client<br/>SDK]
    TESTING[palisade-testing<br/>sim · checker]

    REDIS --> CORE
    ETCD --> CORE
    PROTO --> CORE
    SERVER --> CORE
    SERVER --> REDIS
    SERVER --> PROTO
    CLIENT --> CORE
    CLIENT --> PROTO
    TESTING --> CORE
```

## Request Flow: Semantic Lock Acquisition

```mermaid
sequenceDiagram
    participant C as Client SDK
    participant S as palisade-server
    participant R as Redis

    C->>S: TryLock(key, predicates)
    S->>R: EVALSHA acquire_where.lua
    Note over R: ATOMIC: EXISTS check<br/>+ predicate evaluation<br/>+ SET NX PX<br/>+ INCR fence counter
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
    P[Property Tests<br/>vs live Redis] --> S[Deterministic Sim<br/>200 seeds × fault injection]
    S --> LC[Invariant Checker<br/>hash-chained histories]
    LC --> H[Real-Traffic Checking<br/>gRPC e2e through checker]
    H --> CH[Chaos Suite<br/>toxiproxy + container kills]
```
