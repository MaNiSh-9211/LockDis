# Operations Runbook

## Deployment

### Docker
```sh
docker run -d --name palisade-server \
  -p 50051:50051 -p 9100:9100 \
  -v /path/to/acl.json:/etc/palisade/acl.json \
  -v /path/to/tls:/etc/palisade/tls \
  ghcr.io/manish-9211/palisade-server:0.3 \
  --redis-url=redis://redis:6379 \
  --listen=0.0.0.0:50051 \
  --metrics-addr=0.0.0.0:9100 \
  --acl-file=/etc/palisade/acl.json \
  --auth-mode=file
```

### Kubernetes
See `deploy/helm/palisade/` for the Helm chart.

## Monitoring

| Metric | Alert threshold | Meaning |
|---|---|---|
| `palisade_grants_total` rate drop | <50% of baseline | Store or network issue |
| `palisade_renewal_failures_total` rate | >1/sec | Watchdog struggling; check store |
| `palisade_sessions_expired_total` spike | >5/min | Clients crashing repeatedly |
| `palisade_authz_denials_total` rate spike | >10/sec | Misconfigured ACL or attack |
| `palisade_watch_keys_active` | >expected | Runaway watcher creation |

## Common Issues

| Symptom | Cause | Fix |
|---|---|---|
| `Held` errors spike | Upstream service holding too long | Check holder TTL and critical section duration |
| `Lost` errors in client logs | Watchdog can't renew (store slow/down) | Check Redis health; increase TTL |
| Sessions expiring unexpectedly | Network partition between client and server | Check connectivity; increase session TTL |
| Fair queue handoff timeout | Waiter heartbeats expiring | Increase heartbeat TTL or reduce waiter count |
| Redlock grant failures | Minority of masters unreachable | Restore failed nodes; verify network |

## Scaling

- **Vertical**: Palisade server is stateless — add replicas freely.
- **Sessions**: Use sticky LB routing OR accept per-replica session tables.
- **Redis**: Scale reads with replicas; writes are bounded by single master throughput.
- **etcd**: 3 nodes for dev, 5 for HA, spread across availability zones.

## Emergency Procedures

### Force-release a stuck lock
```sh
# Via gRPC (requires admin principal)
grpcurl -H "authorization: Bearer root-token" \
  -d '{"key": "stuck/key"}' \
  palisade:50051 palisade.v1.LockService/UnlockForce

# Directly against Redis (last resort — bypasses audit)
redis-cli DEL "stuck/key"
```

### Drain a server for maintenance
```sh
kubectl drain <node> --ignore-daemonsets
# Palisade flips to NOT_SERVING, refuses new grants,
# holders keep their leases (they renew directly against Redis).
```

### Recover from total Redis data loss
All locks vanish. Fencing prevents stale-holder corruption.
Re-acquire immediately. See docs/durability.md for per-backend details.
