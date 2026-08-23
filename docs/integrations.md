# Stack Integrations (Gateway · UAM · Grafana)

Palisade is designed to sit **behind your existing edge stack**, not replace it.

```
clients ──▶ API Gateway ──▶ UAM (authn) ──▶ palisade-server ──▶ Redis / etcd
            distributed          │                ▲
            rate limiting        └── identity ─────┘  (trusted-header mode)
                     │
                     └── Prometheus scrape ◀── :9100 metrics
                     └── Grafana dashboards (deploy/grafana/)
```

## 1. API Gateway (distributed rate limiting)

The gateway owns request-level throttling; Palisade deliberately does not
rate-limit `TryLock` itself. Recommended policies at the gateway:

| Route | Limit | Rationale |
|---|---|---|
| `/palisade.v1.LockService/TryLock*` | per-principal QPS | absorb retry storms |
| `/Watch` | max concurrent streams | hub cost is O(keys), cap anyway |
| all others | modest global ceiling | protect the store |

Forward identity after authentication: set the header Palisade trusts.

## 2. UAM service (identity)

Run the server with gateway/UAM-vouched identities:

```sh
palisade-server --acl-file acl.json --auth-mode trusted-header
```

UAM authenticates the caller, then forwards:

```
x-palisade-principal: ci-runner-7
```

Palisade authorizes by principal NAME against the ACL prefixes/quotas —
no bearer tokens cross the wire, and token rotation lives entirely in UAM.
Keep the hop gateway↔palisade private (mTLS via existing flags or network).

For direct clients without the gateway, keep `--auth-mode file` with
bearer tokens matched in the same ACL.

## 3. Grafana

- Metrics: scrape `:9100/metrics` (Prometheus format).
- Dashboard: import `deploy/grafana/palisade-dashboard.json`
  (grants/sec, releases/sec, active watch keys, session expiries,
  renewal failures, authz denials, audit log panel).
- Audit lines are JSON when `PALISADE_LOG_JSON=1`; ship them with
  Promtail/Loki and point the dashboard's logs panel at the stream.
