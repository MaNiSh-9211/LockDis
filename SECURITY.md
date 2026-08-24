# Security Policy

## Supported Versions

| Version | Supported |
|---|---|
| 0.3.x | ✅ |
| < 0.3 | ❌ |

## Reporting a Vulnerability

Email **security@palisade.dev** or open a private GitHub security advisory.

Do NOT open a public issue for security vulnerabilities. We commit to
responding within 48 hours and releasing a patch within 7 days for
critical issues.

## Security Model

### Authentication
- **mTLS**: client certificates verified against CA (production recommended).
- **Bearer tokens**: static tokens from ACL file (`--auth-mode file`).
- **TrustedHeader**: gateway/UAM vouches identity via `x-palisade-principal`
  header. Requires the network hop to be protected (mTLS or private link).

### Authorization
- Per-principal key-prefix grants (e.g., `ci/` namespace isolation).
- Per-action permissions: `can_lock`, `can_unlock`, `can_extend`, `can_watch`, `can_admin`.
- Quotas: `max_keys` (concurrent holds), `max_watchers` (live streams).

### Transport
- TLS via rustls with ring crypto provider.
- mTLS mode requires client certificates signed by the configured CA.
- Plaintext connections are rejected when TLS is enabled.

### Token Safety
- Watch events NEVER carry holder tokens (tokens are release authority).
- Bearer tokens stop at the gateway in TrustedHeader mode.
- Fencing tokens are safe to expose — they are ordering proofs, not secrets.

## Known Limitations

1. Bearer tokens are stored in plaintext in the ACL JSON file.
2. No token rotation without server restart (hot-reload planned).
3. Quotas are per-server-process under multi-replica deployments.
4. The `UnlockForce` admin operation bypasses ownership checks by design;
   it is audited but cannot be undone.

## Fencing Guarantee

Palisade guarantees that fencing tokens are monotonically increasing per key.
It does **NOT** guarantee that downstream resources check them — that is the
integrator's responsibility. See [docs/fencing-guide.md](docs/fencing-guide.md)
for implementation patterns.
