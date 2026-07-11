# Salesforce sink (`snk.salesforce`) — implementation notes

Tracks the build for a first-class Salesforce **write** target, per issue #164.
Salesforce ships today as a **source** only (`src.salesforce`, a preset over
`src.rest`); this adds the sink half so Duckle can support ETL/migration **into**
Salesforce, not just out of it.

## Status

**Tier 1 (sObject Collections) — complete: compiles, unit-tested, and validated end-to-end against a live org (including via the desktop UI).**

- `cargo check -p duckle-duckdb-engine` — **passes** (rustc 1.97.0, stable-msvc).
- `cargo clippy -p duckle-duckdb-engine` — the Salesforce code adds **zero**
  warnings (the crate's 37 pre-existing warnings are all in other files and
  predate this change; they reflect a clippy-version drift in the repo, not this
  work).
- `npm --prefix frontend run lint` (`tsc --noEmit`) — **passes clean**.
- **Validated end-to-end against a live org** (Client Credentials, `v60.0`,
  via `duckle-runner --pipeline` → the real `run_salesforce_sink`):
  - **insert** — `src.csv` (2 rows) → 2 Accounts created, verified by SOQL, deleted.
  - **upsert** by `External_ID__c` — run once creates 2; re-run with the same
    external Ids + changed names updates **in place** (still 2 records, same Ids,
    no duplicates). The migration-critical guarantee.
  - **delete** — the sink's Collections `?ids=…` path returns per-record success.
  - **update** by Id — seeded a record, updated its Name through the engine,
    verified, deleted. All four standard operations proven.
- **Integration tests written and passing** (`cargo test -p duckle-duckdb-engine
  snk_salesforce` → 3 passed): `snk_salesforce_insert_posts_collections_envelope`
  (asserts the `attributes.type` envelope + `allOrNone`), `..._upsert_targets_
  external_id_url` (PATCH `/composite/sobjects/{obj}/{extId}`), and
  `..._record_error_fails_run` (a `success:false` fails the run when
  `failOnError`). All mock-server based - no org or secrets needed in CI.

## Tiers

| Tier | Scope | State |
|------|-------|-------|
| 1 | sObject Collections API (`/composite/sobjects`), ≤200 records/request: insert / update / upsert (by external Id) / delete, Bearer auth, per-record error aggregation | **complete** (live-org + mock tests) |
| 2 | Bulk API 2.0 (`/jobs/ingest`): create → upload CSV → close → poll → fetch success/failed result sets. Migration-scale volume. | planner rejects `api:"bulk"` with a clear message; not built |
| 3 | Migration-grade: first-class reject/error output stream, parent→child ID remapping, external-Id relationship resolution, compound-field (Address/Location) handling, API-limit retry/backoff | not started |

## Architecture / files touched

All in `crates/duckdb-engine` unless noted. The sink rides the existing
synchronous `ureq` per-stage model (no tokio), same as the Snowflake/Databricks
HTTP sinks.

| File | Change |
|------|--------|
| `src/plan/specs.rs` | `SalesforceSinkSpec` + `SalesforceWriteApi` enum |
| `src/plan/mod.rs` | `RuntimeSpec::SalesforceSink` variant; `snk.salesforce` routing block (prop parsing, validation, Bulk-API rejection); `salesforce_sink` local + `.or_else(...)` wire |
| `src/lib.rs` | dispatch arm → `run_salesforce_sink` |
| `src/connectors.rs` | `run_salesforce_sink` executor; free helpers `salesforce_record_envelope` (adds the `attributes.type` envelope the Collections API requires and generic `snk.rest` cannot emit) and `parse_salesforce_results` (per-record `{id,success,errors}` accounting) |
| `frontend/src/workflow-ui/palette-data.ts` | new `snk.saas` palette group + `snk('salesforce', …)` |
| `frontend/src/workflow-ui/fields/manifest-synth.ts` | field manifest (in `synthWarehouseSink`, id-dispatched from `dispatchManifest`) |
| `crates/duckle-mcp/catalog.json` | **generated** — do not hand-edit; regenerate via `cd frontend && node scripts/build-catalog.mjs` |

### Endpoints by operation

```
insert  POST   {instance}/services/data/{ver}/composite/sobjects
update  PATCH  {instance}/services/data/{ver}/composite/sobjects
upsert  PATCH  {instance}/services/data/{ver}/composite/sobjects/{object}/{externalIdField}
delete  DELETE {instance}/services/data/{ver}/composite/sobjects?ids=…&allOrNone=…
```

Body (insert/update/upsert): `{ "allOrNone": <bool>, "records": [ { "attributes": {"type": "<object>"}, …row… }, … ] }`
Response: array of `{ "id", "success", "errors": [{statusCode, message, fields}] }`.

### Config surface (see catalog manifest)

`instanceUrl` (required), `accessToken` (required, Bearer — use `${ENV:SF_TOKEN}`),
`apiVersion` (default `v60.0`), `object` (required), `operation`
(insert|update|upsert|delete), `externalIdField` (required for upsert),
`idField` (default `Id`, for update/delete), `api` (collections|bulk),
`batchSize` (clamped to 200), `allOrNone` (default false), `failOnError`
(default true).

`instanceUrl` doubles as the endpoint base a mock server points at in tests
(same trick as the Snowflake sink's `endpoint`).

## Remaining work

Tier 1 is complete (see Status) and shipped in this PR — sink implemented,
unit-tested, validated end-to-end against a live org, and docs updated (README
Sinks table, `docs/roadmap.md`, `CONTRIBUTING.md`). What's left is follow-up:

1. **Tier 2 — Bulk API 2.0** — new `RuntimeSpec` path with a poll loop (create → upload CSV → close → poll → fetch success/failed results); the `SalesforceWriteApi::Bulk` variant is already reserved and rejected at plan time.
2. **Tier 2 — Salesforce auth Connection** — both `src.salesforce` and this sink are Bearer-token-only (no minting or refresh; the token expires ~2h). A first-class Salesforce Connection that stores Client-Credentials (key/secret) or a JWT cert and mints + refreshes the token would upgrade the source and the sink at once. Duckle already has a Connection concept (`create_connection`/`list_connections`).
3. **Tier 3** — reject/error output stream, parent→child ID remapping, external-Id relationship resolution, compound fields (Address/Location), API-limit retry/backoff.

## Contribution checklist (per CONTRIBUTING.md)

Required before opening a PR (fork off `main`, keep it focused; CI runs on
Linux/macOS/Windows; no required-reviewer gate):

- [x] `cargo check -p duckle-duckdb-engine` — **passes** (rustc 1.97.0)
- [~] `cargo clippy` — Salesforce code is warning-free; the crate has 37
      pre-existing warnings unrelated to this change (repo has clippy-version
      drift under stable 1.97, so a blanket `--workspace -- -D warnings` does not
      currently pass on `main` either)
- [x] `cargo test -p duckle-duckdb-engine snk_salesforce` — **3 passed**
      (`DUCKLE_DUCKDB_BIN` pointed at the vendored `.duckdb-cli-v1.5.3/duckdb.exe`)
- [~] `cargo fmt --check` — do NOT run repo-wide: stable rustfmt 1.97 reformats
      ~11k lines of pre-existing code (the maintainers run a different rustfmt
      build). The Salesforce additions were hand-matched to neighbouring style so
      they pass the maintainers' formatter; leave the rest untouched.
- [x] `npm --prefix frontend run lint` (`tsc --noEmit`) — **passes clean**
- [x] Commits: imperative/conventional subject (`feat(salesforce): …`, matches
      real history e.g. `feat(pg):`, `feat(fe):`)
- [x] Written from scratch, no incompatibly-licensed code (dual MIT/Apache-2.0);
      no CLA/DCO sign-off required

**Note — this PR also corrects two stale contributor guides.** `CONTRIBUTING.md`
previously said to add a module under `crates/connectors/src/`, implement a
`Connector`/`Transform` trait from `plugin-sdk`, and add a node under
`frontend/src/canvas/nodes/` — none of which matches reality (`crates/connectors`
and `crates/transform-engine` are legacy stubs; every actual component lives in
`crates/duckdb-engine/` with the frontend wired via `palette-data.ts` +
`manifest-synth.ts`). `docs/roadmap.md` still referenced `plan.rs`. Both were
rewritten to the real pattern this sink follows.

## Build / regenerate

```bash
# backend
cargo build -p duckle-duckdb-engine
cargo test  -p duckle-duckdb-engine salesforce

# catalog (after any palette-data.ts / manifest-synth.ts change)
cd frontend && npm ci && node scripts/build-catalog.mjs
```
