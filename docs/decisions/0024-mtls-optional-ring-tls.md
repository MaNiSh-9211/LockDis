# 0024. Transport security: optional mTLS, ring-based TLS stack

- **Status:** Accepted
- **Date:** 2026-08-21

## Context
Lock tokens are release authority; on an untrusted network the wire must be encrypted and, ideally, client-authenticated. Deployment reality ranges from localhost dev to zero-trust meshes that already provide mTLS.

## Options considered
1. Optional mTLS via server flags; SDK gains `connect_mtls`; e2e test generates certs with rcgen (chosen)
2. mTLS mandatory always
3. Delegate all transport security to a service mesh (linkerd/istio)
4. Application-level token auth (static API keys)

## Decision
Server flags `--tls-cert/--tls-key` enable TLS; adding `--client-ca` upgrades to mutual TLS (client certificates required). The Rust SDK offers `connect_mtls(endpoint, ca, cert, key)` alongside plain `connect`. TLS uses tonic's rustls integration with the `ring` provider for portable Windows/Linux builds. An e2e test generates CA/server/client certs at runtime (rcgen), proves grant/release over mTLS, and asserts plaintext is rejected.

## Why this is the best option
- **Secure-by-configuration without secure-by-default friction**: local dev stays zero-config; production clusters flip three flags (or use cert-manager, whose Certificate example ships in `deploy/k8s/`).
- **mTLS > static tokens**: certificates give per-client identity, rotation without redeploying secrets, and revocation — API keys would be one shared secret away from leak-and-rotate pain.
- **ring provider**: aws-lc-rs has better throughput but adds CMake/NASM build friction on Windows; ring compiles everywhere we ship. Revisit if benchmarks ever show TLS on the critical path.
- **Tested, not assumed**: generating real certs in CI proves the whole handshake path each run.

## Why not the alternatives
- **Mandatory mTLS**: breaks first-run experience and container-local testing; meshes already solve this for users who want enforced encryption everywhere.
- **Mesh-only**: many target deployments (bare VMs, docker-compose labs, homelabs) have no mesh; the server must carry its own seatbelt.
- **Static API keys**: encrypts nothing, rotates badly, and shares one credential across all holders of the key.

## Consequences
- Client cert issuance/rotation is the operator's job (cert-manager recommended); docs point to the example manifest.
- The Watch stream's no-token guarantee (ADR 0022) now sits behind authenticated channels — defense in depth rather than sole defense.
