# 0028. Authorization & multi-tenancy: static ACLs, bearer identity, quotas, audit

- **Status:** Accepted
- **Date:** 2026-08-22

## Context
mTLS authenticates the channel but grants every connected client identical power: any key, unlimited watchers, unlimited fence allocation. Production multi-tenant deployments need per-principal scoping, quotas to contain blast radius and DoS, an admin break-glass path that is *safer* than raw Redis access, and attributable audit trails.

## Options considered
1. Static JSON ACLs + bearer-token principals + prefix permissions + quotas + audited `UnlockForce` (chosen)
2. mTLS certificate CN as principal identity only
3. External authorization service (OPA-style) per request
4. No authz; run one server per tenant

## Decision
- **Identity:** `authorization: Bearer <token>` header resolved against configured principals. In open mode (no `--acl-file`) a single `anonymous` principal with all permissions — localhost-friendly, loudly documented as dev-only.
- **Grants:** each principal lists allowed `key_prefixes` plus per-action flags (`can_lock/unlock/extend/watch/admin`). Prefix "" = all keys.
- **Quotas:** `max_keys` (concurrent holds) and `max_watchers` (live streams), enforced with RAII accounting — the watcher slot frees when its stream dies.
- **Break-glass:** new `UnlockForce` RPC deletes without ownership check, gated behind `can_admin`, emitting a structured audit line (`principal/action/key/outcome`).
- **Audit:** denials and admin operations log structured events via `tracing` for SIEM shipping.

## Why this is the best option
- **Ships today, scales later**: static config covers the 90% case (CI vs prod services vs humans). The `Principal` trait surface means swapping in cert-CN extraction or an external authorizer is additive, not a rewrite.
- **Quotas are containment**: `max_keys=1` on a CI principal turns a leaked credential's damage into one stuck lock instead of a namespace-wide outage; `max_watchers` bounds our known watch-scaling cost.
- **Force-unlock inside the system beats outside it**: before this feature, operators ran raw `DEL` against Redis with zero attribution. Now break-glass is permission-gated and every use lands in the audit stream.
- **Prefix model matches real tenancy**: teams own namespaces (`payments/`, `ci/`); prefixes are trivially reviewable in code review of the config file.

## Why not the alternatives
- **Cert-CN-only**: strongest binding but forces mTLS everywhere including local dev, and rotation/testing is heavier; bearer tokens now, CN mapping as a hardening flag later.
- **External OPA call per RPC**: adds a network dependency to every lock grant — latency and availability coupling exactly where we least want them. An embedded policy engine is future work if policies outgrow JSON.
- **One server per tenant**: operationally brutal at dozens of tenants; also doesn't stop tenant-A keys colliding with tenant-B tooling.

## Consequences
- Tokens are static secrets in a file — rotation = config reload (not yet hot; restart for v1).
- Quota accounting is per-server-process; multi-replica deployments enforce per-replica ceilings unless backed by shared state (documented).
- Audit coverage currently spans denials-by-permission and admin actions; plain lock traffic stays unlogged by design (volume) — trace sampling covers it instead.
