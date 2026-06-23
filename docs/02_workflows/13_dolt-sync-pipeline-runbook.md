# Dolt Sync Pipeline Runbook

This runbook captures the first working Stitchly v2 Dolt-to-Parquet pipeline built in the local studio.

It is intended for future agents and humans who need to recreate, debug, or extend the workflow without rediscovering the `code.shell`, `code.sql`, and routing details.

Use this with:

- `docs/02_workflows/07_dolt-parquet-workflows.md`
- `docs/01_nodes/05_control-and-code-node-contracts.md`
- `docs/03_runtime/09_http-runtime-bridge.md`

## Tested Workflow

| Item | Value |
|---|---|
| Pipeline name | `dolt_sync_rates_to_parquet` or equivalent |
| Tested repo | `post-no-preference/rates` |
| Branch | `master` |
| Tested table | `us_treasury` |
| Runtime mode | Browser UI + HTTP bridge |
| Durable state | `.stitchly/state/dolt_sync.duckdb` |
| Local clone | `.stitchly/cache/dolt/rates/repo` |
| Published artifact | `artifacts/dolt/rates/master/us_treasury/snapshots/commit=<head_commit>/data.parquet` |

The current workflow performs repo-level idempotency. First run exports and publishes a snapshot. A second run sees the same processed commit and routes to the skip branch.

## Graph

```text
repo_config
  -> sync_repo
  -> parse_sync_result
  -> skip_gate

skip_gate.case_1
  -> fail_sync

skip_gate.case_2
  -> log_skip

skip_gate.default
  -> plan_exports
  -> parse_export_plan
  -> export_tables_to_stage
  -> parse_export_result
  -> validate_exports
  -> publish_and_update_state
  -> log_done
```

## Node Inventory

| Order | Node id | Type | Input | Output / purpose |
|---:|---|---|---|---|
| 1 | `repo_config` | `code.sql` | None | One config row for repo, paths, and branch. |
| 2 | `sync_repo` | `code.shell` | `repo_config` | Clone/pull repo, resolve head commit, compare state. |
| 3 | `parse_sync_result` | `code.sql` | `sync_repo` | Typed sync metadata with `sync_ok` and `sync_status`. |
| 4 | `skip_gate` | `ctl.switch` | `parse_sync_result` | Route failed, unchanged, or changed sync rows. |
| 5 | `fail_sync` | `ctl.die` | `skip_gate.case_1` | Fail only if the failed-sync branch has rows. |
| 6 | `log_skip` | `ctl.log` | `skip_gate.case_2` | Log unchanged commit skip. |
| 7 | `plan_exports` | `code.shell` | `skip_gate.default` | Emit one JSONL export plan row per table. |
| 8 | `parse_export_plan` | `code.sql` | `plan_exports` | Typed export plan rows with `plan_ok`. |
| 9 | `export_tables_to_stage` | `code.shell` | `parse_export_plan` | Export table to staged Parquet. |
| 10 | `parse_export_result` | `code.sql` | `export_tables_to_stage` | Typed export result with file size and row count. |
| 11 | `validate_exports` | `code.sql` | `parse_export_result` | Metadata validation before publish. |
| 12 | `publish_and_update_state` | `code.shell` | `validate_exports` | Publish artifact and update state DB. |
| 13 | `log_done` | `ctl.log` | `publish_and_update_state` | Log successful publish. |

## Node Configs

Canonical copy/paste payloads live in `docs/dolt_scripts/`.

| Node id | Type | Canonical payload |
|---|---|---|
| `repo_config` | `code.sql` | `docs/dolt_scripts/repo_config.sql` |
| `sync_repo` | `code.shell` | `docs/dolt_scripts/sync_repo.sh` |
| `parse_sync_result` | `code.sql` | `docs/dolt_scripts/parse_sync_result.sh` |
| `plan_exports` | `code.shell` | `docs/dolt_scripts/plan_exports.sh` |
| `parse_export_plan` | `code.sql` | `docs/dolt_scripts/parse_export_plan.sql` |
| `export_tables_to_stage` | `code.shell` | `docs/dolt_scripts/export_tables_to_stage.sh` |
| `parse_export_result` | `code.sql` | `docs/dolt_scripts/parse_export_result.sql` |
| `validate_exports` | `code.sql` | `docs/dolt_scripts/validate_exports.sql` |
| `publish_and_update_state` | `code.shell` | `docs/dolt_scripts/publish_and_export_state.sh` |

Note: `parse_sync_result.sh` currently contains SQL despite its `.sh` extension. Use it as the `code.sql` body for `parse_sync_result`.

### `repo_config`

Type: `code.sql`

Canonical payload: `docs/dolt_scripts/repo_config.sql`

Purpose: start the graph with a literal config row. There is no upstream input, so this SQL starts with `select`, not a comma CTE.

```sql
select
  'rates' as repo_key,
  'post-no-preference/rates' as remote_url,
  'master' as branch,
  '.stitchly/cache/dolt' as cache_root,
  '.stitchly/state/dolt_sync.duckdb' as state_db,
  'artifacts/dolt' as artifact_root,
  false as force_snapshot
```

For other repos, change `repo_key` and `remote_url`.

### `sync_repo`

Type: `code.shell`

Canonical payload: `docs/dolt_scripts/sync_repo.sh`

Purpose: receive the config row, maintain a persistent Dolt clone, read the current commit, compare it to `.stitchly/state/dolt_sync.duckdb`, and emit one JSON object.

Required output shape:

| Field | Type | Meaning |
|---|---|---|
| `repo_key` | string | Local repo key. |
| `remote_url` | string | Dolt remote. |
| `branch` | string | Branch. |
| `repo_path` | string | Local clone path. |
| `previous_commit` | string | Last processed commit from state, empty on first run. |
| `head_commit` | string | Current Dolt head commit. |
| `should_skip` | bool | `true` when previous and head commit match. |

Important implementation details:

- `code.shell` receives upstream rows through `DUCKLE_INPUT_PATH`, `DUCKLE_INPUT_TABLE`, and `DUCKLE_DUCKDB_DATABASE`.
- Define `workspace` and `duckdb_bin` before any helper function that uses them.
- Dolt `dolt_log` uses `commit_hash`, not `hash`.
- Use explicit JSON escaping before printing stdout.
- Redirect Dolt pull/status chatter to stderr so stdout remains one JSON object.

Expected first-run result:

```text
should_skip = false
sync_status = changed
```

Expected second-run result after publish:

```text
should_skip = true
sync_status = unchanged
```

### `parse_sync_result`

Type: `code.sql`

Canonical payload: `docs/dolt_scripts/parse_sync_result.sh`

Purpose: parse `sync_repo.stdout` into typed columns.

Because this node has upstream input, the engine compiles it as:

```sql
WITH input AS (SELECT * FROM upstream) <node SQL>
```

Therefore the node SQL must start with a comma CTE, not a standalone `WITH`.

Required output fields:

| Field | Type | Meaning |
|---|---|---|
| `parsed_ok` | bool | `stdout` was valid JSON. |
| `sync_ok` | bool | Shell exit code was zero and JSON parsed. |
| `sync_status` | string | `shell_failed`, `parse_failed`, `unchanged`, or `changed`. |

Working status for the tested repo after first run:

```text
parsed_ok = true
sync_ok = true
sync_status = changed
```

### `skip_gate`

Type: `ctl.switch`

Purpose: split the control row into failure, skip, or export paths.

Branch conditions:

| key | value |
|---|---|
| `sync_failed` | `sync_ok = false` |
| `skip` | `should_skip = true` |

Real output ports:

| Port | Route |
|---|---|
| `case_1` | `fail_sync` |
| `case_2` | `log_skip` |
| `default` / `else` | `plan_exports` |

Notes:

- Branch `key` is only a label.
- Branch `value` is the SQL condition.
- `ctl.switch` is first-match-wins.
- The default branch is reached when no case condition matches.

### `fail_sync`

Type: `ctl.die`

Purpose: fail the workflow if `sync_repo` failed or emitted invalid metadata.

Config:

| Field | Value |
|---|---|
| Message | `Dolt sync failed or produced invalid metadata` |
| Condition | `has-rows` |

Do not use `always` here. Empty switch branches still execute downstream nodes, so `always` fails even when `case_1` has no rows.

### `log_skip`

Type: `ctl.log`

Purpose: record an idempotent skip.

Message:

```text
Dolt sync skipped: repo already processed at current commit ({rows} row)
```

### `plan_exports`

Type: `code.shell`

Canonical payload: `docs/dolt_scripts/plan_exports.sh`

Purpose: read the changed sync row and emit one JSONL export plan row per Dolt table.

Current implementation:

- Uses `dolt ls` to list tables.
- Emits full snapshot plans.
- Uses repo-level idempotency; table-level delta planning is a later improvement.

Required output shape:

| Field | Type | Meaning |
|---|---|---|
| `repo_key` | string | Repo key. |
| `branch` | string | Branch. |
| `repo_path` | string | Local Dolt repo. |
| `table_name` | string | Dolt table. |
| `previous_commit` | string | Previous processed commit. |
| `head_commit` | string | Current head commit. |
| `export_mode` | string | Currently `snapshot`. |
| `reason` | string | `initial_load` or `changed`. |
| `stage_path` | string | Temporary Parquet path. |
| `snapshot_path` | string | Final artifact path. |
| `delta_path` | string | Empty for snapshot mode. |
| `should_export` | bool | `true` for rows to export. |

Tested output for `rates`:

```text
table_name = us_treasury
export_mode = snapshot
reason = initial_load
should_export = true
```

Shell pitfalls discovered:

- Avoid heredocs in UI-pasted shell; closing markers can be indented or clipped.
- Avoid long multiline `printf` continuations; they are easy to break.
- Emit JSON in small `printf '%s'` chunks and finish with one `printf '%s\n'`.
- Assign `safe_branch` before constructing `stage_path` and `snapshot_path`.

### `parse_export_plan`

Type: `code.sql`

Canonical payload: `docs/dolt_scripts/parse_export_plan.sql`

Purpose: parse JSONL from `plan_exports.stdout` into typed rows.

This node splits stdout by newline, parses each line as JSON, and emits:

| Field | Type | Meaning |
|---|---|---|
| `parsed_ok` | bool | JSON line parsed. |
| `plan_ok` | bool | Plan row has required fields. |
| `plan_status` | string | `snapshot`, `skip`, or failure reason. |

Expected tested output:

```text
parsed_ok = true
plan_ok = true
plan_status = snapshot
```

### `export_tables_to_stage`

Type: `code.shell`

Canonical payload: `docs/dolt_scripts/export_tables_to_stage.sh`

Purpose: export planned Dolt table data to staged Parquet.

Current implementation:

- Reads a single plan row from `DUCKLE_INPUT_PATH`.
- Uses `dolt table export --force --file-type parquet`.
- Emits one JSON object.

Required output shape:

| Field | Type | Meaning |
|---|---|---|
| `stage_path` | string | Staged Parquet path. |
| `snapshot_path` | string | Final publish path. |
| `row_count` | int | Count from Dolt before export. |
| `file_size_bytes` | int | Staged Parquet size. |
| `export_ok` | bool | `true` after non-empty file exists. |

Expected tested output:

```text
row_count = 9110
file_size_bytes = 289539
export_ok = true
```

Limit:

The current script handles one plan row. Generalize this before using repos with many tables, or run one table per workflow branch.

### `parse_export_result`

Type: `code.sql`

Canonical payload: `docs/dolt_scripts/parse_export_result.sql`

Purpose: parse `export_tables_to_stage.stdout` into typed columns.

Expected tested output:

```text
parsed_ok = true
export_result_ok = true
export_result_status = ready_to_publish
```

### `validate_exports`

Type: `code.sql`

Canonical payload: `docs/dolt_scripts/validate_exports.sql`

Purpose: validate export metadata before publish.

Checks:

| Check | Meaning |
|---|---|
| `check_export_result_ok` | Prior export stage succeeded. |
| `check_stage_path_present` | Staged path exists in metadata. |
| `check_snapshot_path_present` | Publish path exists in metadata. |
| `check_table_name_present` | Table name exists. |
| `check_head_commit_present` | Commit exists. |
| `check_row_count_positive` | Row count is greater than zero. |
| `check_file_size_positive` | Staged Parquet size is greater than zero. |

Expected tested output:

```text
export_validation_ok = true
export_validation_status = valid
```

Keep this as `code.sql` for now because it validates workflow metadata. Add `qa.*` nodes later when validating the exported table contents.

### `publish_and_update_state`

Type: `code.shell`

Canonical payload: `docs/dolt_scripts/publish_and_export_state.sh`

Purpose: copy the staged Parquet file to the final artifact path and update `.stitchly/state/dolt_sync.duckdb`.

Behavior:

1. Read the validated export row from `DUCKLE_INPUT_PATH`.
2. Verify `export_validation_ok = true`.
3. Copy staged Parquet to `<snapshot_path>.tmp.$$`.
4. Move the temp file to `snapshot_path`.
5. Verify published size matches staged size.
6. Insert a state row into `dolt_sync`.
7. Emit one JSON result.

Expected tested output:

```text
publish_ok = true
state_updated = true
published_size_bytes = 289539
```

State table:

```sql
create table if not exists dolt_sync (
  repo_key text,
  remote_url text,
  branch text,
  table_name text,
  last_processed_commit text,
  last_snapshot_commit text,
  schema_hash text,
  artifact_manifest_path text,
  row_count bigint,
  updated_at timestamp
);
```

### `log_done`

Type: `ctl.log`

Purpose: log successful publish.

Message:

```text
Dolt sync published {rows} artifact row(s)
```

## Idempotency Check

After a successful publish, rerun from the start.

Expected `parse_sync_result`:

```text
should_skip = true
sync_status = unchanged
```

Expected route:

```text
skip_gate.case_2 -> log_skip
```

The export and publish branch should not run.

## Operational Notes

### `code.sql` With Upstream Input

Connected `code.sql` nodes are wrapped as:

```sql
WITH input AS (SELECT * FROM upstream) <node SQL>
```

Therefore:

- Use `select ...` when there is no upstream input.
- Use `, cte_name as (...) select ...` when there is upstream input and you need additional CTEs.
- Do not start upstream-connected SQL with `WITH`.

### `code.shell` Upstream Input

When `code.shell` has a main upstream connection, the runtime sets:

```text
DUCKLE_INPUT_PATH
DUCKLE_INPUT_FORMAT=jsonl
DUCKLE_INPUT_TABLE
DUCKLE_INPUT_ROW_COUNT
DUCKLE_DUCKDB_DATABASE
```

Use `DUCKLE_INPUT_PATH` for simple one-row JSONL control payloads. Use `DUCKLE_DUCKDB_DATABASE` and `DUCKLE_INPUT_TABLE` when querying upstream rows through DuckDB.

### Shell Style

Use POSIX-compatible shell because the default Unix shell is `/bin/sh`.

Prefer:

```sh
set -eu
printf '%s' "$value"
```

Avoid:

```sh
set -euo pipefail
long printf lines with fragile backslash continuation
heredocs pasted into the UI
```

### Stdout vs Stderr

For shell nodes in this workflow:

- stdout is machine-readable JSON or JSONL.
- stderr is human-readable command progress.

Keep Dolt chatter on stderr:

```sh
dolt pull >&2
dolt table export ... >&2
```

## Known Limitations

| Limitation | Current handling | Future improvement |
|---|---|---|
| One table row in `export_tables_to_stage` | Works for `rates` repo because it has `us_treasury` only. | Loop all JSONL plan rows or split per table branch. |
| Snapshot only | Full table Parquet snapshot on changed repo commit. | Add Dolt delta export and apply-delta workflow. |
| No schema hash | State records empty `schema_hash`. | Compute schema fingerprint from staged Parquet. |
| No content QA | Metadata validation only. | Add Parquet read + `qa.contract`/`qa.notnull` checks. |
| No manifest JSON | State points directly at snapshot path. | Publish manifest with commit, table, row count, schema, size. |
| `remote_url` not persisted in publish state | Publish script inserts empty `remote_url`. | Carry `remote_url` through plan/export result rows. |

## Troubleshooting

| Symptom | Likely fix |
|---|---|
| `parse_sync_result` parser error near `with` | Remove leading `WITH`; upstream-connected `code.sql` must start with comma CTE or final `select`. |
| `fail_sync` fires even though `plan_exports` runs | Set `fail_sync` condition to `has-rows`, not `always`. |
| `ctl.switch` routes unexpectedly | Confirm branch row `value` contains SQL condition; `key` is only a label. |
| `plan_exports: no base tables found` | Use `dolt ls` instead of `SHOW FULL TABLES`. |
| `/bin/sh: printf: usage` | Replace long multiline `printf` blocks with chunked `printf '%s'` calls. |
| `end of file unexpected (expecting done)` | Remove heredocs or check loop/heredoc termination in UI-pasted shell. |
| `parameter not set` | `set -u` caught a typo or assignment order issue; check variable names exactly. |
| JSON line splits across preview rows | Ensure shell emits each JSON object as one physical stdout line. |

## Canonical Payload Files

The exact node payloads are stored in:

```text
docs/dolt_scripts/repo_config.sql
docs/dolt_scripts/sync_repo.sh
docs/dolt_scripts/parse_sync_result.sh
docs/dolt_scripts/plan_exports.sh
docs/dolt_scripts/parse_export_plan.sql
docs/dolt_scripts/export_tables_to_stage.sh
docs/dolt_scripts/parse_export_result.sql
docs/dolt_scripts/validate_exports.sql
docs/dolt_scripts/publish_and_export_state.sh
```

Treat those files as the source of truth for copy/paste node scripts and SQL bodies. Keep this runbook focused on graph shape, routing, expected outputs, and troubleshooting.
