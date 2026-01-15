# Repository Guidelines

## Project Structure & Module Organization

- `cmd/`: binary/CLI entry points (e.g. server/worker/ctl).
- `components/`: main Rust workspace crates (engine, server, cloud, native_br, test utilities).
- `src/`: top-level `tikv` crate sources.
- `tests/`: integration test harness crate and suites:
  - `tests/cloud_engine/`: cloud-engine integration tests.
  - `tests/cloud_engine_failpoints/`: cloud-engine failpoint integration tests.
  - `tests/random/`: random tests.
- `metrics/`: metrics docs/config; runtime metrics live in crate code (e.g. `components/native_br/src/metrics.rs`).
- `scripts/`, `etc/`, `doc/`: tooling and documentation.
- `~/.cargo/registry/src`: depedencies source code path.

## Build, Test, and Development Commands

- Build dev target: `make build`
- Build release artifacts: `make release`
- Run all tests (workspace default): `cargo test`
- Run the cloud-engine integration suite: `cargo test -p tests --test cloud_engine`
- Run a single test with nextest:  
  `cargo nextest run -p tests --test cloud_engine -E 'test(native_backup::limiter::...)'`
- Lint: `make clippy`
- Format: `make format`

## Coding Style & Naming Conventions

- Rust style is enforced via `rustfmt.toml`; run `make format` before pushing.
- Prefer minimal, localized changes (avoid drive-by refactors in `components/`).
- Use descriptive, scoped names for tests and changes (e.g. `native_backup::limiter::...`).

## Testing Guidelines

- Integration tests live under `tests/` and are registered in `tests/Cargo.toml` as `[[test]]` targets.
- Some test targets require the `testexport` feature (enabled by default in `tests/Cargo.toml`).
- Name tests by behavior and expectation (e.g. `test_restore_keyspace_with_failed_store`).
- Prefer targeted runs during iteration (use `cargo nextest run ... -E 'test(...)'`).

## Commit & Pull Request Guidelines

- Commit subjects commonly follow `area: summary (#NNNN)` (e.g. `rfengine: ... (#4123)`), or multi-area `a,b: ...`.
- PRs should include: problem statement, approach, and test evidence (exact command used).
- Link the relevant issue/PR number when applicable.

## Configuration Tips

- Many integration tests spin up in-process clusters and an object-store mock; keep files/ports under temp dirs.
- For panic debugging, set `LOG_FILE=/tmp/test.log` to capture logs (panic hook may exit via `_exit(1)`).
