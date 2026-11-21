# RooDB

Rust SQL database: two wire protocols (MySQL-compatible, TDS) over one query engine, LSM storage, Raft log.

Read on demand: `CONTRIBUTING.md` (layout, key types, query flow, tests, debugging), `design.md` (internals), `LOCKING.md`, `admin.md` (flags, deployment), `SQL.md` (supported SQL).

## Before every commit (non-negotiable)

All four must pass with ZERO warnings:

```bash
cargo build --release
cargo clippy
cargo fmt --check
cargo test --release
```

- `#[allow(dead_code)]` is banned.
- No commit may leave a document stale. In the same commit, update every doc the change affects: `SQL.md` whenever supported SQL changes; likewise `design.md`, `admin.md`, `LOCKING.md`, `README.md`, `CONTRIBUTING.md`, `CLAUDE.md`.

## Invariants

- TLS only: no plaintext network path, for clients or Raft.
- Both protocols (`src/protocol/roodb/`, `src/protocol/tds/`) are thin front-ends over `QueryEngine` (`src/server/query/`), differing only in framing, login and result encoding. SQL behaviour belongs in the engine.
- File I/O is direct I/O: buffers must be 4 KiB-aligned (`AlignedBuffer::page()` / `AlignedBuffer::pages(n)`).
- io_uring code is Linux-only (`#[cfg(target_os = "linux")]`). The backend is chosen at run time (`--io-backend auto|uring|posix`, `ROODB_IO_BACKEND`; `auto` falls back to posix), so everything must work on both. CI runs the tests a second time with `ROODB_IO_BACKEND=posix`.

## Tests

- Server-level integration tests all build into one test binary, `suite` (`tests/suite.rs`, `tests/roodb_suite/`). Most share one server process: use unique table names.
- Use `--release` for Raft cluster tests (timing sensitive).

## Debugging

Put debug/diagnostic/test logic in a wrapper script `/tmp/<name>.sh`; update and re-run that same script rather than issuing ad hoc commands, to minimise prompts.

## Benchmarking

- Always run ALL workloads; never filter to one. Run the full A/B comparison every time.
- Command: `bench/run.sh --table-size=100000 --duration=30`
- Afterwards show a comparison table of ALL workloads: before vs after, RooDB vs MySQL.
- Use `bench/compare.sh` against a baseline when one is available.
