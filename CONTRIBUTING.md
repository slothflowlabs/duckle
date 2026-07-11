# Contributing to Duckle

Thanks for your interest in Duckle. This project is in early development; contributions, issues, and design discussion are all welcome.

By participating you agree to abide by the [Code of Conduct](./CODE_OF_CONDUCT.md).

## Prerequisites

- **Rust** stable, installed via [rustup](https://rustup.rs). The repository pins the toolchain in `rust-toolchain.toml`.
- **Node.js 20+** and **npm 10+**.
- **Tauri 2 system prerequisites** - see https://tauri.app/start/prerequisites for your OS. On Windows this means MSVC build tools and WebView2.

## First-time setup

```sh
# install frontend dependencies
npm --prefix frontend install

# build the workspace (compiles every crate)
cargo build --workspace

# run the desktop app
cargo run -p duckle-desktop
```

The desktop app launches Vite's dev server automatically and opens a Tauri window pointing at it.

## Repository layout

- `apps/desktop/` - Tauri 2 shell.
- `crates/` - Rust crates for runtime, connectors, engines, workflow, scheduling, plugins.
- `frontend/` - React + TypeScript UI.

## Submitting a pull request

1. Fork the repository and create a branch off `main`.
2. Make your change, following the style and test guidance below.
3. Run the checks locally: `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and `npm --prefix frontend run lint` if the frontend changed.
4. Push to your fork and open a pull request against `main`. The PR template will prompt for a summary, related issue, and the checklist.
5. CI runs fmt, clippy, and tests on Linux, macOS, and Windows. Keep the PR focused; a green CI run and a clear description make review fast.

There is no required-reviewer gate, so a maintainer will merge once it looks good. Small, self-contained PRs are merged fastest.

## Style and conventions

- **Rust**: `cargo fmt` and `cargo clippy --workspace --all-targets -- -D warnings` must pass.
- **TypeScript**: 2-space indent, single quotes, trailing commas. Run `npm --prefix frontend run lint` before pushing.
- **Commits**: small, atomic, and self-explanatory. Use imperative subject lines (`Add Parquet source connector`, not `Added` or `Adding`).
- **Comments**: only when the *why* is non-obvious. Don't restate what the code already says.

## Tests

- **Unit tests** live alongside the code (`#[cfg(test)] mod tests` in Rust; co-located `*.test.ts` in the frontend).
- **Integration tests** for crates that need them live under `crates/<name>/tests/`.
- Run everything with `cargo test --workspace`.

## Adding a connector or transform

Connectors (sources and sinks) and transforms are implemented in the
`duckle-duckdb-engine` crate - not via a `plugin-sdk` trait. (The `plugin-sdk`,
`crates/connectors`, and `crates/transform-engine` crates are legacy scaffolding
and are not how components ship today.) The steps are the same for a source,
sink, or transform:

1. Read an existing one as a template - e.g. `snk.snowflake` / `snk.clickhouse`
   for a vendor HTTP sink, `src.mongodb` for an async-driver source.
2. Define the spec struct in `crates/duckdb-engine/src/plan/specs.rs` and add a
   `RuntimeSpec` variant in `crates/duckdb-engine/src/plan/mod.rs`.
3. Add a routing OR-arm (parse the node's properties into the spec) in
   `crates/duckdb-engine/src/plan/mod.rs`.
4. Add the executor in `crates/duckdb-engine/src/connectors.rs` and a dispatch
   arm in `crates/duckdb-engine/src/lib.rs`.
5. Add a palette tile in `frontend/src/workflow-ui/palette-data.ts`, and - if the
   node needs a custom property panel - a field manifest in
   `frontend/src/workflow-ui/fields/manifest-synth.ts`. Regenerate the component
   catalog: `node frontend/scripts/build-catalog.mjs` (writes
   `crates/duckle-mcp/catalog.json`).
6. Add an integration test in `crates/duckdb-engine/tests/execution.rs`. Use a
   mock server for HTTP connectors; env-gate any real-network test so it skips
   unless the relevant `DUCKLE_*` env var is set.
7. Update the README capability table and `docs/roadmap.md`.

See `docs/roadmap.md` ("Contributing a connector") for the same checklist
alongside the roadmap of what is and isn't shipped.

## Legal

By contributing, you agree your contribution is dual-licensed under MIT and Apache-2.0, as the rest of the project is.

Do not paste or port code from incompatibly licensed sources. If you draw inspiration from another project, that is fine - but write the implementation from scratch.
