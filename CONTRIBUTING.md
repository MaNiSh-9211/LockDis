# Contributing

Thank you for contributing to Palisade! This document explains the process.

## Development Setup

```sh
git clone https://github.com/MaNiSh-9211/LockDis.git
cd LockDis
cargo build --workspace
cargo test --workspace   # requires local Redis on :6379
```

For etcd tests: `docker run -d --name palisade-etcd -p 2379:2379 quay.io/coreos/etcd:v3.5.14 etcd --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379`

## Quality Gates

All PRs must pass:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI enforces these against live Redis + etcd containers.

## Architecture Decision Records

Every feature or architectural decision requires an ADR in `docs/decisions/`.
Use the template in `docs/decisions/README.md`. An ADR must document:
1. The problem and constraints
2. All options considered
3. What we chose and why it's best
4. Why each alternative was rejected
5. Consequences (positive and negative)

## Code Style

- `#![forbid(unsafe_code)]` workspace-wide — no exceptions without ADR.
- `#![deny(missing_docs)]` on all public items.
- Every public function has a doc comment with an example where non-obvious.
- Lua scripts live in `crates/redis/src/scripts/*.lua` with safety argument comments.

## Testing Requirements

New features require:
1. Integration tests against a real backend
2. Property tests for invariant preservation
3. Simulation scenarios for new failure modes

Bug fixes require:
1. A test that reproduces the bug before the fix
2. The fix
3. Confirmation that the test passes after

## Commit Messages

Write commit messages as if explaining to a colleague what changed and why.
No AI attribution, no Co-Authored-By trailers. Plain English, present tense.

## License

By contributing you agree that your contributions will be dual-licensed
under MIT OR Apache-2.0.
