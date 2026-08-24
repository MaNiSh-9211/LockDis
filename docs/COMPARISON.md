# Comparison: Palisade vs Existing Solutions

## Feature Matrix

| Feature | Palisade | Redisson | ZooKeeper | etcd | Consul | HashiCorp Vault |
|---|---|---|---|---|---|---|
| **Fencing tokens (mandatory)** | ✅ every grant | ❌ | ✅ zxid | ✅ mod_revision | ⚠️ session-based | ❌ |
| **Semantic predicates in CAS** | ✅ Lua-in-script | ❌ | ❌ | ⚠️ txn compare | ❌ | ❌ |
| **Server-side session death** | ✅ heartbeat sweep | ❌ client-only | ✅ ephemeral nodes | ✅ lease expiry | ✅ session TTL | ❌ |
| **Safety policy knob** | ✅ 3 levels | ❌ hardcoded | ❌ hardcoded | ❌ hardcoded | ❌ hardcoded | N/A |
| **Testament (deathbed transfer)** | ✅ payload to successor | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Black box flight recorder** | ✅ hash-chained | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Contention prediction** | ✅ EWMA forecast | ❌ blind polling | ❌ watches only | ❌ watch only | ⚠️ blocking queries | N/A |
| **Store pressure index** | ✅ composite SPI | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Fair FIFO queue** | ✅ heartbeat+handoff | ✅ | ✅ sequential nodes | ⚠️ revision order | ⚠️ session order | N/A |
| **Read-write lock** | ✅ per-token readers | ✅ | ⚠️ recipe | ❌ DIY | ⚠️ recipe | ❌ |
| **CountDownLatch** | ✅ | ✅ | ✅ recipe | ❌ DIY | ❌ DIY | ❌ |
| **Multi-language gRPC** | ✅ protobuf v1 | ❌ Java-only | ⚠️ bindings | ✅ gRPC native | ✅ HTTP/gRPC | ✅ HTTP |
| **Consensus backend** | ⚠️ via etcd / Redlock | ❌ Redis only | ✅ ZAB | ✅ Raft | ✅ Raft | ❌ |
| **Authz (prefix ACLs)** | ✅ | ⚠️ basic | ✅ per-node | ✅ users/roles | ✅ policies | ✅ rich |

## Honest Assessment

**Where Palisade wins:**
- Fencing is mandatory by default, not opt-in — the #1 correctness gap in every other system
- Semantic Locks eliminate TOCTOU — no other system embeds predicates atomically
- Safety Policy lets you choose your point on the staleness/liveness frontier explicitly
- Testament enables state handoff across crash boundaries — unique capability

**Where competitors win:**
- ZooKeeper has 15+ years of production hardening at massive scale
- etcd has deeper multi-version consistency and a richer transaction model
- Redisson has deep Spring/Java ecosystem integration
- Consul has built-in service discovery alongside locking

**When to choose Palisade:** you need fencing-first correctness, semantic predicates, explicit safety control, and Rust performance — and you're willing to accept a younger project.

## Migration Notes

| From | Key difference |
|---|---|
| Redisson | Palisade adds fencing, sessions, safety policy; loses Java ecosystem |
| ZooKeeper | Palisade uses familiar Redis/etcd ops; loses global zxid ordering |
| etcd direct locks | Palisade adds semantic predicates, testament, contention prediction |
| Raw SET NX PX | Palisade eliminates every known failure mode of naive locking |
