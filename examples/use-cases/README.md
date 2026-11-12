# Use case samples

Runnable versions of the use cases on
[duckle.org/use-cases](https://duckle.org/use-cases.html). Every input is a
small committed file or is written by the workspace's own first pipeline, so
nothing external is needed. Run them in order: later pipelines read what
earlier ones write.

1. **make_inputs** - writes `data/orders.parquet` (500 generated orders),
   `data/catalog.duckdb` (products) and `data/regions.sqlite` (regions) from the
   committed CSVs, so the next pipeline has four systems to read.
2. **join_four_systems** - customers in CSV, orders in Parquet, products in a
   DuckDB file and regions in SQLite, joined in one visual Map (`xf.map`) with
   typed per-column expressions (amount, margin, tier) and an inline filter on
   the order status. A group-by writes revenue by region and category to Parquet
   and upserts it into a DuckDB warehouse.
3. **mirror_initial_load** - copies Monday's CRM extract into
   `output/crm_mirror.duckdb`.
4. **mirror_apply_changes** - `xf.cdc.diff` compares Tuesday's extract with
   Monday's and sends only what changed. The DuckDB sink upserts the changes and
   deletes the customers Tuesday no longer has, which the diff tags `deleted`.
5. **mirror_check** - a quality gate: diffs the mirror against Tuesday's
   extract and fails the run (`qa.assert`) if a single row differs.
6. **rollup_locally** - generates a million orders and writes only the daily
   revenue rollup. The website runs this at a billion rows; raise `rowCount` to
   see it at that scale.
7. **rest_to_parquet** - reads a paged REST API (`nextUrl` pagination) with a
   bearer token, adds a column and writes Parquet. The API's base URL comes from
   the **Local** context (`contexts/local.json`), so another environment is one
   more context. The token is `${ENV:SHOP_API_TOKEN}`, read from the environment
   and never written into the pipeline.

The incremental load (watermarks) is `incremental_load` in
[starter-workspace](../starter-workspace).

## Running them

The REST sample reads a demo API served from `api/`:

```sh
python -m http.server 8765 --bind 127.0.0.1 --directory examples/use-cases/api
```

Then, from the repository root:

```sh
export SHOP_API_TOKEN=demo-token   # any value; the demo API does not check it
for p in examples/use-cases/pipelines/*.json; do
    duckle-runner --pipeline "$p" --workspace examples/use-cases
done
```

Or open the folder as a workspace in the app and run them from the project tree.

## Tests

`tests/4_mirror_apply_changes.test.json` holds `duckle test` cases for the
change feed: fixture files stand in for the two extracts, and the cases assert
exactly what the diff sends.

```sh
duckle-runner test examples/use-cases/tests/*.test.json --workspace examples/use-cases
```

CI validates every pipeline here on all platforms; on Linux it runs each one in
order, runs the tests, and checks that the outputs landed (see
`.github/workflows/ci.yml`).
