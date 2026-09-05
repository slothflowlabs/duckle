//! SQL string builders: per-component SELECT/COPY generation, source &
//! sink readers, ATTACH preludes, and the shared prop/identifier helpers.
//! Extracted from plan/mod.rs (build_stage stays there and calls these).

use super::*;

/// The `SELECT * FROM <reader>` SQL for a source format - used by the
/// engine's inspect path to DESCRIBE / sample without materializing.
pub fn source_select_for_format(format: &str, props: &JsonValue) -> Option<String> {
    // Autodetect (inspect) must build the SAME source SELECT as a real run,
    // or the schema preview diverges from what the node actually reads
    // (issue #18: formats missing here returned None -> the UI fell back to a
    // col_1/col_2/col_3 placeholder even though running the node worked).
    Some(match format {
        "csv" => build_csv_source(props, None),
        "tsv" => build_tsv_source(props, None),
        "parquet" => build_parquet_source(props),
        "json" | "jsonl" | "ndjson" => build_json_source(props),
        "sqlite" => build_sqlite_source(props),
        "duckdb" => build_duckdb_source(props),
        "excel" => build_excel_source(props, None),
        "avro" => build_avro_source(props),
        "inline" => build_inline_source(props),
        "filelist" => build_filelist_source(props),
        "iceberg" => build_iceberg_source(props),
        "delta" => build_delta_source(props),
        "spatial" => build_spatial_source(props),
        "gdb" => build_gdb_source(props),
        "huggingface" => build_huggingface_source(props),
        "fixedwidth" => return build_fixedwidth_source(props, None).ok(),
        // DuckLake is DuckDB-backed; the catalog is ATTACHed as duckle_src by
        // the inspect prelude (see source_prelude), so the SELECT is identical
        // to the run path.
        "ducklake" => return build_relational_source("src.ducklake", props).ok(),
        // DuckLake snapshot inspector: list the catalog's snapshots (newest
        // first) so the UI can show a timeline and let the user pick an AS OF
        // version. The catalog is ATTACHed as duckle_src by source_prelude.
        "ducklake_snapshots" => {
            "SELECT snapshot_id, snapshot_time FROM ducklake_snapshots('duckle_src') ORDER BY snapshot_id DESC".to_string()
        }
        "s3" | "gcs" | "azureblob" | "http" | "https" => {
            return build_cloud_source(format, props, None).ok()
        }
        // MinIO / R2 / B2 are S3-compatible; the endpoint lives in the SECRET,
        // so inspect reads them through the s3 scheme exactly as build_view_sql
        // does at run time (they ran but autodetect returned None before).
        "minio" | "r2" | "b2" => return build_cloud_source("s3", props, None).ok(),
        // ATTACH-based relational sources. The catalog is ATTACHed as duckle_src
        // by the inspect prelude (source_prelude), so the SELECT is identical to
        // the run path. Without this arm, autodetect returned None and the UI
        // fell back to a col_1/col_2/col_3 placeholder even though running the
        // node worked (#129: Postgres autodetect showed col_1/col_2/col_3).
        "postgres" | "cockroach" | "mysql" | "mariadb" | "redshift" | "pgvector"
        | "motherduck" | "bigquery" | "quack" => {
            return build_relational_source(&format!("src.{format}"), props).ok()
        }
        _ => return None,
    })
}

/// ATTACH-relational source formats (postgres/mysql wire families plus the
/// extension-ATTACH warehouses) that have a real SELECT builder. When
/// `source_select_for_format` returns None for one of these it means the
/// connection props were incomplete (the SELECT builder errored), NOT that it
/// is a driver source, so inspect reports it as unsupported instead of running
/// a driver probe that would try to connect (issue #148 follow-up). Kept in one
/// place so it cannot drift from the relational arm of source_select_for_format
/// and the ATTACH block of source_prelude.
pub(crate) fn is_attach_relational_format(format: &str) -> bool {
    matches!(
        format,
        "postgres"
            | "cockroach"
            | "mysql"
            | "mariadb"
            | "redshift"
            | "pgvector"
            | "motherduck"
            | "bigquery"
            | "quack"
    )
}

/// Wrap a source query as a derived table with a dialect-appropriate row cap,
/// so autodetect pulls a small sample instead of the whole source (issue #148).
/// The cap affects only how much a driver fetches, never the resulting schema.
pub(crate) fn cap_preview_query(format: &str, query: &str, n: usize) -> String {
    let inner = query.trim().trim_end_matches(';');
    match format {
        // Oracle predates FETCH FIRST on older releases; ROWNUM is universal.
        "oracle" => format!("SELECT * FROM ({}) WHERE ROWNUM <= {}", inner, n),
        "sqlserver" | "synapse" | "teradata" => {
            format!("SELECT TOP {} * FROM ({}) q", n, inner)
        }
        _ => format!("SELECT * FROM ({}) LIMIT {}", inner, n),
    }
}

/// Build a capped preview SELECT for a driver / API source from its props, so
/// autodetect fetches a small sample. Prefers a user-supplied query / sql;
/// otherwise reconstructs the connector's own `SELECT * FROM <table>` (matching
/// its dialect quoting) and caps that. Returns None when there is nothing to cap
/// (no query and no tableName) - the connector then reads unchanged, which is
/// still correct, just uncapped. The connectors all prefer `query` over
/// `tableName`, so setting `query` from this is what applies the cap.
pub(crate) fn preview_source_query(format: &str, props: &JsonValue, n: usize) -> Option<String> {
    for key in ["query", "sql"] {
        if let Some(q) = props.get(key).and_then(|v| v.as_str()) {
            if !q.trim().is_empty() {
                return Some(cap_preview_query(format, q, n));
            }
        }
    }
    let table = props
        .get("tableName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let schema = props
        .get("schema")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    // Match each connector's own qualification (see the src.* arms in compile()).
    let qualified = match format {
        "sqlserver" | "synapse" => format!("[{}].[{}]", schema.unwrap_or("dbo"), table),
        "oracle" => match schema {
            Some(s) => format!("\"{}\".\"{}\"", s, table),
            None => format!("\"{}\"", table),
        },
        _ => match schema {
            Some(s) => format!("{}.{}", s, table),
            None => table.to_string(),
        },
    };
    Some(cap_preview_query(format, &format!("SELECT * FROM {}", qualified), n))
}

pub(crate) fn missing_input(node: &PipelineNode, port: &str) -> EngineError {
    EngineError::Config(format!(
        "{} ({}) is missing its '{}' input",
        node.data.label, node.id, port
    ))
}

// ---- View SQL (sources + transforms) ------------------------------------

pub(crate) fn build_view_sql(
    component_id: &str,
    props: &JsonValue,
    inputs: &NodeInputs,
    declared: Option<&[duckle_metadata::Column]>,
    reject_wired: bool,
) -> Result<String, String> {
    // Editable plan-stage SQL (issue #157): a `sqlOverride` on a stage replaces
    // its generated SELECT with the user's own. Upstreams are exposed as CTEs -
    // `input` (the main upstream) and `input1`, `input2`, ... (each upstream in
    // edge order, main first) - so the edited SQL is robust to the internal
    // upstream table names. A stage with no upstream (a source) runs the
    // override verbatim.
    if let Some(over) = string_prop(props, "sqlOverride")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        let mut ups: Vec<&str> = Vec::new();
        if let Some(m) = inputs.main() {
            ups.push(m);
        }
        for refs in inputs.ports.values() {
            for r in refs {
                let s = r.as_str();
                if !ups.iter().any(|u| *u == s) {
                    ups.push(s);
                }
            }
        }
        if ups.is_empty() {
            return Ok(over);
        }
        let mut ctes = vec![format!("input AS (SELECT * FROM {})", quote_ident(ups[0]))];
        for (i, up) in ups.iter().enumerate() {
            ctes.push(format!("input{} AS (SELECT * FROM {})", i + 1, quote_ident(up)));
        }
        return Ok(format!("WITH {} {}", ctes.join(", "), over));
    }
    match component_id {
        // Sources - declared schema is consulted by CSV / TSV (via `types=`)
        // and Excel (via an all_varchar read + cast/project wrapper, since
        // read_xlsx has no type map; issue #25). Other sources auto-infer and
        // ignore `declared`.
        //
        // When the reject port is wired (issue #15) the CSV/TSV main read
        // switches to a tolerant split: declared columns are read as raw text,
        // cast back to their type, and rows that fail parsing are dropped from
        // main (they flow to the reject relation instead) rather than aborting
        // the read. With the reject port unwired the SQL is unchanged.
        "src.csv" => Ok(if reject_wired {
            build_csv_source_split(props, declared, false)
        } else {
            build_csv_source(props, declared)
        }),
        "src.tsv" => Ok(if reject_wired {
            build_csv_source_split(props, declared, true)
        } else {
            build_tsv_source(props, declared)
        }),
        "src.parquet" => Ok(build_parquet_source(props)),
        "src.json" | "src.jsonl" => Ok(build_json_source(props)),
        "src.sqlite" => Ok(build_sqlite_source(props)),
        "src.duckdb" => Ok(build_duckdb_source(props)),
        "src.ducklake.diff" => Ok(build_ducklake_diff(props)),
        "src.s3" | "src.gcs" | "src.azureblob" | "src.http"
        | "src.minio" | "src.r2" | "src.b2" => {
            // MinIO / R2 / B2 are S3-compatible; the endpoint lives in
            // the SECRET created by the runtime, so the URL itself is
            // just s3://bucket/key.
            let s = component_id.strip_prefix("src.").unwrap_or(component_id);
            let scheme = if matches!(s, "minio" | "r2" | "b2") { "s3" } else { s };
            build_cloud_source(scheme, props, declared).map_err(|e| e.to_string())
        }
        "src.postgres" | "src.cockroach" | "src.mysql" | "src.mariadb"
        | "src.motherduck" | "src.ducklake" | "src.pgvector"
        | "src.redshift" | "src.bigquery" | "src.quack" => build_relational_source(component_id, props),
        "src.avro" => Ok(build_avro_source(props)),
        "src.excel" => Ok(build_excel_source(props, declared)),
        "src.inline" => Ok(build_inline_source(props)),
        "src.filelist" => Ok(build_filelist_source(props)),
        "src.artifact" => Ok(build_artifact_source(props)),
        "src.iceberg" => Ok(build_iceberg_source(props)),
        "src.delta" => Ok(build_delta_source(props)),
        "src.spatial" => Ok(build_spatial_source(props)),
        "src.gdb" => Ok(build_gdb_source(props)),
        "src.huggingface" => Ok(build_huggingface_source(props)),
        "src.fixedwidth" => build_fixedwidth_source(props, declared),
        // Pass-through transforms
        "xf.filter" => build_filter(inputs, props),
        // Log Rows - pass data through unchanged; its rows surface in the
        // Output / Preview so you can inspect mid-pipeline (like tLogRow).
        "xf.log" => build_passthrough_op(inputs, "SELECT *"),
        "xf.diffsummary" => build_diffsummary(inputs, props),
        "xf.project" => build_project(inputs, props),
        "xf.distinct" => build_distinct(inputs, props),
        "xf.limit" => build_limit(inputs, props),
        "xf.sort" => build_sort(inputs, props),
        "xf.agg" | "xf.groupby" => build_aggregate(inputs, props, GroupMode::Plain),
        "xf.approx.quantile" => build_approx_quantile(inputs, props),
        "xf.rollup" => build_aggregate(inputs, props, GroupMode::Rollup),
        "xf.cube" => build_aggregate(inputs, props, GroupMode::Cube),
        "xf.aggwin" => build_window_aggregate(inputs, props),
        "xf.union" => build_union(inputs, true, props),
        "xf.unionall" => build_union(inputs, false, props),
        "xf.intersect" => build_setop(inputs, "INTERSECT", props),
        "xf.except" => build_setop(inputs, "EXCEPT", props),
        "xf.addcol" | "xf.coalesce" => build_addcol(inputs, props),
        "xf.pyexpr" => build_pyexpr(inputs, props),
        "xf.rownum" | "xf.rank" | "xf.denserank" | "xf.lead" | "xf.lag" | "xf.first"
        | "xf.last" | "xf.ntile" => build_window(inputs, props, component_id),
        "xf.pivot" => build_pivot(inputs, props),
        "xf.zip" => build_zip(inputs, props),
        "xf.unpivot" => build_unpivot(inputs, props),
        "xf.denorm" => build_denormalize(inputs, props),
        "xf.norm" => build_normalize(inputs, props),
        "xf.transpose" => build_transpose(inputs),
        "xf.cdc.diff" => build_cdc_diff(inputs, props),
        "xf.cdc.scd2" => build_scd2(inputs, props),
        "xf.cdc.scd1" => build_scd1(inputs, props),
        "xf.cdc.upsert" => build_upsert(inputs, props),
        "xf.ai.vector_search" => build_vector_search(inputs, props),
        // Data-quality validators - the PASS rows. Failures go to the
        // node's __reject table (see build_reject_sql).
        "qa.notnull" | "qa.range" | "qa.regex" | "qa.unique" | "qa.schemavalidate" => {
            build_quality(inputs, props, component_id, false)
        }
        "qa.profile" => build_profile(inputs, props),
        "qa.describe" => build_describe(inputs),
        "qa.histogram" => build_histogram(inputs, props),
        "qa.standardize" => build_standardize(inputs, props),
        "qa.mask" => build_mask(inputs, props),
        "qa.survivor" => build_survivor(inputs, props),
        "qa.matchgroup" => build_matchgroup(inputs, props),
        "qa.expect" => build_expect(inputs, props),
        "qa.contract" => build_contract(inputs, props),
        "qa.freshness" => build_freshness(inputs, props),
        "qa.outlier" => build_outlier(inputs, props, false),
        // Geometry data-quality tools (issue #158): validate / repair / empty
        // checks via the spatial extension's ST_IsValid / ST_MakeValid /
        // ST_IsEmpty. spatial is force-loaded for these ids in attach_prelude.
        "qa.geomvalidate" => build_geom_validate(inputs, props),
        "qa.geomrepair" => build_geom_repair(inputs, props),
        "qa.geomempty" => build_geom_empty(inputs, props),
        "xf.surrogatekey" => build_surrogate_key(inputs, props),
        "xf.sessionize" => build_sessionize(inputs, props),
        "xf.cdc.scd3" => build_scd3(inputs, props),
        "qa.sample.adv" => build_sample_adv(inputs, props),
        "qa.refintegrity" => build_refintegrity(inputs, props, false),
        "qa.profile.adv" => build_profile_adv(inputs, props),
        "qa.link" => build_record_link(inputs, props),
        "qa.block" => build_er_block(inputs, props),
        "src.model" => Ok(build_model_source(props)?),
        "qa.reconcile" => build_reconcile(inputs, props),
        "qa.classify" => build_classify(inputs, props),
        "qa.dedupe" => build_fuzzy_dedupe(inputs, props),
        "qa.match" => build_record_match(inputs, props),
        "xf.reorder" => build_reorder(inputs, props),
        "xf.count" => build_count(inputs),
        "xf.join.cross" => build_cross_join(inputs),
        "xf.join.spatial" => build_spatial_join(inputs, props),
        "xf.regex" | "xf.regex.extract" | "xf.regex.match" | "xf.trim" | "xf.case"
        | "xf.length" | "xf.substring" | "xf.concat" | "xf.split" | "xf.format" => {
            build_string(inputs, props, component_id)
        }
        "xf.url.parse" => build_url_parse(inputs, props),
        "xf.assert" => build_assert(inputs, props),
        "xf.hash" => build_hash(inputs, props),
        "xf.ip.parse" => build_ip_parse(inputs, props),
        "xf.geo.distance" => build_geo_distance(inputs, props),
        "xf.geo.length" => build_geo_length(inputs, props),
        "xf.geo.perimeter" => build_geo_perimeter(inputs, props),
        "xf.geo.area" => build_geo_area(inputs, props),
        "xf.geo.buffer" => build_geo_buffer(inputs, props),
        "xf.geo.flip" => build_geo_flip(inputs, props),
        "xf.geo.intersects" => build_geo_intersects(inputs, props),
        "xf.geo.setcrs" => build_geo_setcrs(inputs, props),
        "xf.geo.reproject" => build_geo_reproject(inputs, props),
        "xf.geo.create" => build_geo_create(inputs, props),
        "xf.geo.clip" => build_geo_clip(inputs, props),
        "xf.geo.erase" => build_geo_erase(inputs, props),
        "xf.num.round" | "xf.num.abs" | "xf.num.mod" | "xf.num.power" | "xf.num.sqrt"
        | "xf.num.log" => build_numeric(inputs, props, component_id),
        "xf.num.bucketize" => build_bucketize(inputs, props),
        "xf.num.zscore" => build_zscore(inputs, props),
        "xf.num.clamp" => build_clamp(inputs, props),
        "xf.num.sign" => build_sign(inputs, props),
        "xf.rank.filter" => build_rank_filter(inputs, props),
        "xf.fill_forward" => build_fill_forward(inputs, props),
        "xf.fill_backward" => build_fill_backward(inputs, props),
        "xf.fill_constant" => build_fill_constant(inputs, props),
        "xf.row_hash" => build_row_hash(inputs, props),
        "xf.audit" => build_audit(inputs, props),
        "xf.cumulative" => build_cumulative(inputs, props),
        "xf.dt.bin" => build_dt_bin(inputs, props),
        "xf.arr.length" => build_arr_length(inputs, props),
        "xf.uuid" => build_uuid(inputs, props),
        "xf.dt.parse" | "xf.dt.format" | "xf.dt.extract" | "xf.dt.trunc" | "xf.dt.tz" => {
            build_datetime(inputs, props, component_id)
        }
        "xf.dt.add" => build_date_add(inputs, props),
        "xf.dt.diff" => build_date_diff(inputs, props),
        "xf.dt.now" => build_dt_now(inputs, props),
        "xf.dt.epoch" => build_dt_epoch(inputs, props),
        "xf.json.parse" | "xf.json.stringify" | "xf.json.path" => {
            build_json(inputs, props, component_id)
        }
        "xf.json.flatten" => build_json_flatten(inputs, props),
        "xf.json.merge" => build_json_merge(inputs, props),
        "xf.json.array_agg" => build_json_array_agg(inputs, props),
        "xf.text.similarity" => build_text_similarity(inputs, props),
        "xf.text.base64" => build_base64(inputs, props),
        "xf.text.padding" => build_padding(inputs, props),
        "xf.text.match" => build_text_match(inputs, props),
        "xf.text.reverse" => build_text_reverse(inputs, props),
        "xf.text.repeat" => build_text_repeat(inputs, props),
        "xf.text.replace" => build_text_replace(inputs, props),
        "xf.text.slug" => build_text_slug(inputs, props),
        "xf.text.strip_html" => build_text_strip_html(inputs, props),
        "xf.text.tocolumns" => build_text_to_columns(inputs, props),
        "xf.compare" => build_compare(inputs, props),
        "xf.arr.element" | "xf.arr.distinct" | "xf.arr.explode" => {
            build_array(inputs, props, component_id)
        }
        "xf.arr.collect" => build_arr_collect(inputs, props),
        "xf.arr.contains" => build_arr_contains(inputs, props),
        "xf.cast" => build_cast(inputs, props),
        "xf.rename" => build_rename(inputs, props),
        "xf.drop" | "xf.dropcol" => build_drop(inputs, props),
        "xf.map" => build_mapper(inputs, props),
        "xf.join.inner" | "xf.join" => build_join(inputs, props, "INNER"),
        "xf.join.left" => build_join(inputs, props, "LEFT"),
        "xf.join.right" => build_join(inputs, props, "RIGHT"),
        "xf.join.full" | "xf.join.outer" => build_join(inputs, props, "FULL OUTER"),
        "xf.lookup" | "xf.lookup.outer" => build_join(inputs, props, "LEFT"),
        "xf.semi" | "xf.semi.join" => build_semi(inputs, props, false),
        "xf.anti" | "xf.anti.join" => build_semi(inputs, props, true),
        "xf.topn" => build_take(inputs, props, TakeKind::Limit),
        "xf.skip" => build_take(inputs, props, TakeKind::Offset),
        "xf.sample" => build_take(inputs, props, TakeKind::Sample),
        // Custom SQL - runs the user's SELECT as a real stage, with the
        // upstream exposed as `input`. Makes SQL routines executable too.
        "code.sql" | "code.sqltemplate" => build_custom_sql(inputs, props),
        // Routing: replicate is a passthrough (the graph already lets
        // multiple downstream edges read the same materialized table);
        // merge concatenates multiple input streams with UNION ALL.
        "ctl.replicate" => {
            let upstream = inputs.main().ok_or_else(|| missing_input_msg("ctl.replicate"))?;
            Ok(format!("SELECT * FROM {}", quote_ident(upstream)))
        }
        "ctl.merge" => build_union(inputs, false, props),
        // Retry wrapper: passthrough view. Retries are read off the
        // form's Advanced tab as retry_attempts/retry_backoff_ms on
        // THIS stage. Useful as an explicit marker in the DAG saying
        // "retry up to this point in the pipeline on transient
        // failure"; semantically equivalent to setting Advanced.retry
        // on the next downstream stage, but more visually obvious.
        "ctl.retry" => {
            let upstream = inputs.main().ok_or_else(|| missing_input_msg("ctl.retry"))?;
            Ok(format!("SELECT * FROM {}", quote_ident(upstream)))
        }
        // Everything else isn't executable yet. Fail loudly rather than
        // silently passing data through unchanged (which would look like
        // success while doing nothing).
        other => Err(format!(
            "'{}' isn't executable on the DuckDB engine yet - it's a preview component.",
            other
        )),
    }
}

/// Geometry column for the geometry DQ tools (issue #158), default "geometry".
fn geom_col(props: &JsonValue) -> String {
    string_prop(props, "geometryColumn")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "geometry".to_string())
}

/// Validate Geometry (issue #158): flags geometries with `ST_IsValid`. Mode
/// `flag` (default) adds an `is_valid` boolean column and keeps all rows;
/// `valid` / `invalid` keep only the matching rows. spatial is loaded via
/// attach_prelude for this component id.
pub(crate) fn build_geom_validate(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.geomvalidate"))?;
    let src = quote_ident(upstream);
    // CAST to GEOMETRY so the tool accepts both a native GEOMETRY column (a
    // no-op cast) and a VARCHAR WKT/GeoJSON column (parsed) - a bare column
    // reference only auto-casts for string literals, not columns.
    let g = format!("CAST({} AS GEOMETRY)", quote_ident(&geom_col(props)));
    Ok(match string_prop(props, "mode").as_deref() {
        Some("valid") => format!("SELECT * FROM {src} WHERE ST_IsValid({g})"),
        Some("invalid") => format!("SELECT * FROM {src} WHERE NOT ST_IsValid({g})"),
        _ => format!("SELECT *, ST_IsValid({g}) AS is_valid FROM {src}"),
    })
}

/// Repair Geometry (issue #158): replaces the geometry column in place with
/// `ST_MakeValid`. Mode `all` (default) repairs every row; `invalid` only
/// repairs rows that fail `ST_IsValid` (valid rows pass through untouched).
/// Uses `SELECT * REPLACE` so the geometry column is replaced, not duplicated.
pub(crate) fn build_geom_repair(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.geomrepair"))?;
    let src = quote_ident(upstream);
    let col = quote_ident(&geom_col(props));
    // CAST accepts a native GEOMETRY column (no-op) or a VARCHAR WKT column
    // (parsed); both CASE branches stay GEOMETRY-typed so REPLACE is consistent.
    let g = format!("CAST({col} AS GEOMETRY)");
    let expr = match string_prop(props, "mode").as_deref() {
        Some("invalid") => format!("CASE WHEN ST_IsValid({g}) THEN {g} ELSE ST_MakeValid({g}) END"),
        _ => format!("ST_MakeValid({g})"),
    };
    Ok(format!("SELECT * REPLACE ({expr} AS {col}) FROM {src}"))
}

/// Check Empty Geometry (issue #158): flags empty geometries with `ST_IsEmpty`.
/// Mode `flag` (default) adds an `is_empty` boolean column and keeps all rows;
/// `empty` / `nonempty` keep only the matching rows.
pub(crate) fn build_geom_empty(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.geomempty"))?;
    let src = quote_ident(upstream);
    let g = format!("CAST({} AS GEOMETRY)", quote_ident(&geom_col(props)));
    Ok(match string_prop(props, "mode").as_deref() {
        Some("empty") => format!("SELECT * FROM {src} WHERE ST_IsEmpty({g})"),
        Some("nonempty") => format!("SELECT * FROM {src} WHERE NOT ST_IsEmpty({g})"),
        _ => format!("SELECT *, ST_IsEmpty({g}) AS is_empty FROM {src}"),
    })
}

pub(crate) fn build_passthrough_op(inputs: &NodeInputs, op: &str) -> Result<String, String> {
    let upstream = inputs
        .main()
        .ok_or_else(|| "missing main input".to_string())?;
    Ok(format!("{} FROM {}", op, quote_ident(upstream)))
}

pub(crate) fn build_filter(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    // The predicate is usually a structured object carrying compiled
    // `sql`; it may also be a raw string (legacy / raw-SQL mode).
    let predicate = filter_predicate_sql_checked(props.get("predicate"))?
        .or_else(|| {
            props
                .get("filterSql")
                .and_then(JsonValue::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default();
    let predicate = predicate.trim();
    let predicate = if predicate.is_empty() { "TRUE" } else { predicate };
    Ok(format!(
        "SELECT * FROM {} WHERE {}",
        quote_ident(upstream),
        predicate
    ))
}

/// Extract the effective SQL from a filter predicate value, which may be
/// a plain string or the structured FilterPredicate object the visual
/// builder writes ({ mode, conditions, rawSql, sql }).
///
/// `mode: "python"` compiles a Python expression through the same compiler
/// `xf.pyexpr` uses. Without it, a Python predicate would be spliced in as raw
/// SQL and only appear to work: `a and b` and `x in (1, 2)` happen to be
/// spelled the same in both languages, but `x is None`, a conditional
/// expression or an f-string would emit invalid SQL, and an unquoted column
/// named after a SQL keyword would break. Returns Err so the failure names the
/// offending construct instead of surfacing as a DuckDB parse error.
pub(crate) fn filter_predicate_sql_checked(
    v: Option<&JsonValue>,
) -> Result<Option<String>, String> {
    if let Some(JsonValue::Object(o)) = v {
        if o.get("mode").and_then(JsonValue::as_str) == Some("python") {
            let src = o
                .get("expr")
                .or_else(|| o.get("rawSql"))
                .and_then(JsonValue::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            if src.is_empty() {
                return Ok(None);
            }
            return crate::pyexpr::compile(&src)
                .map(Some)
                .map_err(|e| format!("filter expression: {}", e));
        }
    }
    Ok(filter_predicate_sql(v))
}

pub(crate) fn filter_predicate_sql(v: Option<&JsonValue>) -> Option<String> {
    match v {
        Some(JsonValue::String(s)) => Some(s.clone()),
        Some(JsonValue::Object(o)) => o
            .get("sql")
            .and_then(JsonValue::as_str)
            .map(str::to_string)
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                if o.get("mode").and_then(JsonValue::as_str) == Some("raw") {
                    o.get("rawSql").and_then(JsonValue::as_str).map(str::to_string)
                } else {
                    None
                }
            }),
        _ => None,
    }
}

pub(crate) fn build_project(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let columns = columns_from_props(props, "columns").or_else(|| columns_from_props(props, "keep"));
    let cols = match columns {
        Some(cs) if !cs.is_empty() => cs
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", "),
        _ => "*".to_string(),
    };
    Ok(format!("SELECT {} FROM {}", cols, quote_ident(upstream)))
}

pub(crate) fn build_drop(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let columns = columns_from_props(props, "columns")
        .or_else(|| columns_from_props(props, "drop"))
        .unwrap_or_default();
    if columns.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    let except_list = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT * EXCLUDE ({}) FROM {}",
        except_list,
        quote_ident(upstream)
    ))
}

/// xf.diffsummary: reduce a change feed (a `change_type` column, e.g. from
/// src.ducklake.diff) to a single summary row - added / removed / updated /
/// total_changes counts plus a ready-made `summary` text. Feed the row into
/// xf.ai.llm for an AI narrative, or into a validator to assert expected counts.
pub(crate) fn build_diffsummary(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let col = string_prop(props, "changeColumn")
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "change_type".into());
    let c = quote_ident(&col);
    Ok(format!(
        "SELECT added, removed, updated, (added + removed + updated) AS total_changes, \
         added::VARCHAR || ' added, ' || removed::VARCHAR || ' removed, ' || updated::VARCHAR || ' updated' AS summary \
         FROM (SELECT \
         COUNT(*) FILTER (WHERE {c} = 'insert') AS added, \
         COUNT(*) FILTER (WHERE {c} = 'delete') AS removed, \
         COUNT(*) FILTER (WHERE {c} = 'update_postimage') AS updated \
         FROM {tbl})",
        c = c,
        tbl = quote_ident(upstream)
    ))
}

pub(crate) fn build_limit(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let limit = props
        .get("limit")
        .and_then(JsonValue::as_u64)
        .or_else(|| props.get("rows").and_then(JsonValue::as_u64))
        .unwrap_or(100);
    Ok(format!(
        "SELECT * FROM {} LIMIT {}",
        quote_ident(upstream),
        limit
    ))
}

pub(crate) enum TakeKind {
    Limit,
    Offset,
    Sample,
}

pub(crate) fn build_take(inputs: &NodeInputs, props: &JsonValue, kind: TakeKind) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let n = props
        .get("count")
        .and_then(JsonValue::as_u64)
        .or_else(|| props.get("limit").and_then(JsonValue::as_u64))
        .unwrap_or(100);
    let from = quote_ident(upstream);
    // Optional `orderBy` (comma-separated columns) makes LIMIT / OFFSET
    // deterministic. A bare LIMIT/OFFSET picks an arbitrary slice under
    // preserve_insertion_order=false whenever an upstream operator
    // reorders rows, so xf.skip/xf.topn/xf.limit could skip or keep a
    // different set run-to-run (audit B4). We do NOT auto-inject an
    // ordering (it would change both which rows survive and their order
    // for every existing node, plus cost a full sort) and do NOT require
    // it (would break existing nodes); it's opt-in.
    let order_by = {
        let cols = columns_list(props, "orderBy");
        if cols.is_empty() {
            String::new()
        } else {
            format!(
                " ORDER BY {}",
                cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
            )
        }
    };
    Ok(match kind {
        TakeKind::Limit => format!("SELECT * FROM {}{} LIMIT {}", from, order_by, n),
        TakeKind::Offset => format!("SELECT * FROM {}{} OFFSET {}", from, order_by, n),
        TakeKind::Sample => format!("SELECT * FROM {} USING SAMPLE {} ROWS", from, n),
    })
}

/// Custom SQL stage. The upstream table is exposed as a CTE named
/// `input`, so a node's SQL like `SELECT * FROM input WHERE x > 1`
/// just works. With no upstream, the SQL stands alone (e.g. a source
/// SELECT). build_stage wraps the result in CREATE OR REPLACE TABLE.
pub(crate) fn build_custom_sql(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let sql = string_prop(props, "sql")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Custom SQL is empty - write a SELECT or pick a SQL routine".to_string())?;
    // Raw mode (#102 item 3): emit the user's SQL verbatim, with no `WITH input
    // AS (...)` wrapper. The wrapper otherwise breaks any query that starts with
    // its own WITH (nested WITH) and blocks multi-CTE / UNION queries. In raw
    // mode the user references each upstream input by its node id (quoted), e.g.
    // `SELECT * FROM "node_id"`. Pure mode (#102 follow-up) is a superset: it
    // also drops the CREATE wrapper in plan/mod.rs, so the body must be verbatim
    // here too.
    if props.get("rawSql").and_then(JsonValue::as_bool).unwrap_or(false)
        || props.get("pureSql").and_then(JsonValue::as_bool).unwrap_or(false)
    {
        return Ok(sql);
    }
    Ok(match inputs.main() {
        Some(upstream) => {
            format!("WITH input AS (SELECT * FROM {}) {}", quote_ident(upstream), sql)
        }
        None => sql,
    })
}

/// Sanitize an inline dbt model name to a safe SQL identifier. The same rule
/// the scaffolder applies when it writes the model file, so the table dbt
/// creates and the name the engine reads back (output_model) always agree -
/// a name like "my-model" becomes "my_model" in both places, not a
/// table-not-found on read-back.
pub(crate) fn sanitize_dbt_model_name(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    let s = s.trim_matches('_').to_string();
    if s.is_empty() { "duckle_model".to_string() } else { s }
}

pub(crate) fn build_distinct(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        // A bare DISTINCT has no per-group survivor to order, so an orderBy
        // here would be silently ignored. Fail loud instead of dropping it.
        if !columns_list(props, "orderBy").is_empty() {
            return Err("distinct: orderBy needs the key columns to dedupe on - set 'columns', or clear orderBy".into());
        }
        Ok(format!("SELECT DISTINCT * FROM {}", quote_ident(upstream)))
    } else {
        let on = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        // DISTINCT ON keeps the first row per group in ORDER BY order; with
        // no ORDER BY the surviving non-key columns are nondeterministic
        // (worse under preserve_insertion_order=false).
        //
        // Default ORDER BY ALL breaks ties across every column, so the kept
        // row is the deterministic per-group minimum - but it forces a full
        // sort on every column (audit B10: ~1.6s vs ~0.01s on 10M rows, a
        // >100x cost). An optional `orderBy` prop sorts only the key columns
        // plus the chosen tiebreak columns, keeping determinism at a
        // fraction of the cost. The default is unchanged (ORDER BY ALL) so
        // existing pipelines keep their exact current survivor + ordering.
        let tiebreak = columns_list(props, "orderBy");
        let order_clause = if tiebreak.is_empty() {
            "ORDER BY ALL".to_string()
        } else {
            // DISTINCT ON requires its keys to lead the ORDER BY; append the
            // tiebreak columns, then a trailing `*` (all remaining columns) so
            // the survivor is fully deterministic even when (keys, tiebreak)
            // is not unique within a group. `ORDER BY cols, *` is valid DuckDB
            // (unlike `ORDER BY cols, ALL`).
            let tb = tiebreak.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
            format!("ORDER BY {}, {}, *", on, tb)
        };
        Ok(format!(
            "SELECT DISTINCT ON ({}) * FROM {} {}",
            on,
            quote_ident(upstream),
            order_clause
        ))
    }
}

/// One ORDER BY key, assembled the same way whichever form it came from.
///
/// `nulls` is deliberately an Option and NOT defaulted: an existing pipeline
/// using `orderBy` emits no NULLS clause today, so inventing one on upgrade
/// would change the row order of a run that never asked. The single-column
/// form keeps its own `unwrap_or(true)` default, which is what it has always
/// emitted.
fn sort_key(col: &str, dir: Option<&str>, nulls: Option<bool>) -> String {
    // Allowlist the direction: an unexpected token spliced raw would make a
    // malformed ORDER BY / parser error (audit B5). Trimmed and lowercased,
    // because "DESC" is what a hand-written pipeline, an import or an SDK
    // writes, and matching "desc" exactly meant it sorted ASCENDING in silence.
    let dir_kw = match dir.unwrap_or("asc").trim().to_ascii_lowercase().as_str() {
        "desc" => "DESC",
        _ => "ASC",
    };
    let nulls_kw = match nulls {
        Some(true) => " NULLS LAST",
        Some(false) => " NULLS FIRST",
        None => "",
    };
    format!("{} {}{}", quote_ident(col.trim()), dir_kw, nulls_kw)
}

/// `"amount DESC NULLS FIRST"` -> `("amount", Some("desc"), Some(false))`.
///
/// A trailing direction inside the string is how multi-column sort was
/// expressed before the editor could express it at all, so it has to keep
/// working - which is why the whole string cannot simply be quoted as a column
/// name. What remains once the trailing keywords are taken off IS the column,
/// and it is then quoted, so a name with a space in it finally sorts instead of
/// producing a parse error.
///
/// A column literally named `amount DESC` cannot be written this way. The
/// object form is the unambiguous one and the editor writes it.
fn parse_sort_string(s: &str) -> Option<(String, Option<String>, Option<bool>)> {
    let mut toks: Vec<&str> = s.split_whitespace().collect();
    let mut nulls = None;
    if toks.len() >= 3 {
        let last = toks[toks.len() - 1].to_ascii_lowercase();
        let prev = toks[toks.len() - 2].to_ascii_lowercase();
        if prev == "nulls" && (last == "first" || last == "last") {
            nulls = Some(last == "last");
            toks.truncate(toks.len() - 2);
        }
    }
    let mut dir = None;
    if toks.len() >= 2 {
        let last = toks[toks.len() - 1].to_ascii_lowercase();
        if last == "asc" || last == "desc" {
            dir = Some(last);
            toks.truncate(toks.len() - 1);
        }
    }
    if toks.is_empty() {
        return None;
    }
    Some((toks.join(" "), dir, nulls))
}

/// Every column an `xf.sort` node will actually order by, in whichever form it
/// is written.
///
/// Shared with the validator (plan/graph.rs) so that a key which COMPILES is a
/// key which VALIDATES. The two halves had drifted: the validator checked each
/// string entry whole, so `orderBy: ["amount DESC"]` - the documented way to
/// express multi-column sort before the editor could - was refused as
/// "column 'amount DESC' not found" before build_sort ever saw it. It also
/// checked neither the bare-string form nor the legacy `sortColumn`, so a typo
/// in the editor's own Column field reached DuckDB and failed there instead.
pub(crate) fn sort_columns(props: &JsonValue) -> Vec<String> {
    let of = |v: &JsonValue| -> Option<String> {
        if let Some(s) = v.as_str() {
            parse_sort_string(s).map(|(col, _, _)| col)
        } else {
            v.get("column").and_then(JsonValue::as_str).map(str::to_string)
        }
    };
    let mut out: Vec<String> = match props.get("orderBy") {
        Some(JsonValue::Array(arr)) => arr.iter().filter_map(of).collect(),
        Some(JsonValue::String(s)) => {
            s.split(',').filter_map(parse_sort_string).map(|(col, _, _)| col).collect()
        }
        _ => Vec::new(),
    };
    if out.is_empty() {
        if let Some(col) = string_prop(props, "sortColumn").filter(|s| !s.is_empty()) {
            out.push(col);
        }
    }
    out
}

pub(crate) fn build_sort(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let one = |v: &JsonValue| -> Option<String> {
        if let Some(s) = v.as_str() {
            let (col, dir, nulls) = parse_sort_string(s)?;
            Some(sort_key(&col, dir.as_deref(), nulls))
        } else if let Some(obj) = v.as_object() {
            let col = obj.get("column").and_then(JsonValue::as_str)?;
            Some(sort_key(
                col,
                obj.get("direction").and_then(JsonValue::as_str),
                obj.get("nullsLast").and_then(JsonValue::as_bool),
            ))
        } else {
            None
        }
    };
    let mut sort_keys: Vec<String> = match props.get("orderBy") {
        Some(JsonValue::Array(arr)) => arr.iter().filter_map(one).collect(),
        // A bare string, read as the caller plainly meant it - the same
        // forgiveness columns_list already extends, and for the same reason:
        // writing "amount" instead of ["amount"] silently produced a node with
        // no ORDER BY at all, and an unordered result is not a visible failure.
        Some(JsonValue::String(s)) => s
            .split(',')
            .filter_map(parse_sort_string)
            .map(|(col, dir, nulls)| sort_key(&col, dir.as_deref(), nulls))
            .collect(),
        _ => Vec::new(),
    };
    // The legacy single-key form. The editor now writes `orderBy`, but a saved
    // pipeline, the desktop assistant's prompt (apps/desktop/src/llama_chat.rs)
    // and the Talend importer all still write these, so the read stays.
    if sort_keys.is_empty() {
        if let Some(col) = string_prop(props, "sortColumn").filter(|s| !s.is_empty()) {
            sort_keys.push(sort_key(
                &col,
                string_prop(props, "direction").as_deref(),
                Some(props.get("nullsLast").and_then(JsonValue::as_bool).unwrap_or(true)),
            ));
        }
    }
    if sort_keys.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    Ok(format!(
        "SELECT * FROM {} ORDER BY {}",
        quote_ident(upstream),
        sort_keys.join(", ")
    ))
}

pub(crate) enum GroupMode {
    Plain,
    Rollup,
    Cube,
}

pub(crate) fn build_aggregate(
    inputs: &NodeInputs,
    props: &JsonValue,
    mode: GroupMode,
) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    // The Group By form writes `groupKeys`; accept `groupBy` too.
    let group_by: Vec<String> = columns_from_props(props, "groupKeys")
        .or_else(|| columns_from_props(props, "groupBy"))
        .unwrap_or_default();
    let aggregations = props
        .get("aggregations")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    let mut select_terms: Vec<String> = group_by.iter().map(|c| quote_ident(c)).collect();
    for agg in &aggregations {
        // Empty means the same as absent: COUNT(*). The panel's aggregation
        // row offers "- column -" as an option and writes `column: ""` for it,
        // which is PRESENT, so `unwrap_or` never fired and the empty string
        // went on to be quoted - emitting COUNT("") and failing the run on a
        // quoted empty identifier. Trimmed as well, because a column picked and
        // then cleared can leave whitespace behind.
        let column = agg
            .get("column")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .unwrap_or("*");
        // The UI's AggregationsField stores { column, func, output };
        // accept the function/alias spellings too for robustness.
        let func = match agg
            .get("function")
            .or_else(|| agg.get("func"))
            .and_then(JsonValue::as_str)
        {
            Some(f) => f.to_uppercase(),
            // count(*) is the sensible default for a bare row count, but
            // silently turning {column: "amount"} into COUNT(amount) yields a
            // wrong number (a row count where a sum/avg was meant). Require an
            // explicit function for a named column instead of defaulting.
            None if column == "*" => "COUNT".to_string(),
            None => {
                return Err(format!(
                    "Aggregation on column '{}' needs a function (sum, avg, min, max, count, count_distinct, ...)",
                    column
                ))
            }
        };
        let alias = agg
            .get("alias")
            .or_else(|| agg.get("output"))
            .and_then(JsonValue::as_str)
            .map(String::from)
            .unwrap_or_else(|| format!("{}_{}", func.to_lowercase(), column.replace('*', "all")));
        let column_expr = if column == "*" {
            "*".to_string()
        } else {
            quote_ident(column)
        };
        let agg_expr = match func.as_str() {
            "COUNT_DISTINCT" => format!("COUNT(DISTINCT {})", column_expr),
            "APPROX_COUNT_DISTINCT" => format!("approx_count_distinct({})", column_expr),
            _ => format!("{}({})", func, column_expr),
        };
        select_terms.push(format!("{} AS {}", agg_expr, quote_ident(&alias)));
    }
    if select_terms.is_empty() {
        select_terms.push("COUNT(*) AS row_count".to_string());
    }
    let group_clause = if group_by.is_empty() {
        String::new()
    } else {
        let cols = group_by
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        match mode {
            GroupMode::Plain => format!(" GROUP BY {}", cols),
            GroupMode::Rollup => format!(" GROUP BY ROLLUP ({})", cols),
            GroupMode::Cube => format!(" GROUP BY CUBE ({})", cols),
        }
    };
    let having = string_prop(props, "havingClause")
        .or_else(|| string_prop(props, "having"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|h| format!(" HAVING {}", h))
        .unwrap_or_default();
    Ok(format!(
        "SELECT {} FROM {}{}{}",
        select_terms.join(", "),
        quote_ident(upstream),
        group_clause,
        having
    ))
}

pub(crate) fn interval_unit(unit: &str) -> &'static str {
    match unit.to_lowercase().as_str() {
        "year" | "years" => "YEAR",
        "quarter" | "quarters" => "QUARTER",
        "month" | "months" => "MONTH",
        "week" | "weeks" => "WEEK",
        "hour" | "hours" => "HOUR",
        "minute" | "minutes" => "MINUTE",
        "second" | "seconds" => "SECOND",
        _ => "DAY",
    }
}

pub(crate) fn build_date_add(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.add"))?;
    let column = require_column(props)?;
    let amount = props.get("amount").and_then(JsonValue::as_i64).unwrap_or(1);
    let unit = string_prop(props, "unit").unwrap_or_else(|| "day".into());
    // amount * INTERVAL 1 unit handles negatives cleanly.
    let expr = format!(
        "{} + ({} * INTERVAL 1 {})",
        quote_ident(&column),
        amount,
        interval_unit(&unit)
    );
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

pub(crate) fn build_date_diff(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.diff"))?;
    let start = string_prop(props, "startColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Date diff needs a start column".to_string())?;
    let end = string_prop(props, "endColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Date diff needs an end column".to_string())?;
    let unit = string_prop(props, "unit").unwrap_or_else(|| "day".into());
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "date_diff".into());
    Ok(format!(
        "SELECT *, date_diff('{}', {}, {}) AS {} FROM {}",
        sql_escape(&unit),
        quote_ident(&start),
        quote_ident(&end),
        quote_ident(&out),
        quote_ident(upstream)
    ))
}

pub(crate) fn build_json_flatten(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.json.flatten"))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    // Expand a STRUCT column's fields to top-level columns.
    Ok(format!(
        "SELECT * EXCLUDE ({}), {}.* FROM {}",
        col,
        col,
        quote_ident(upstream)
    ))
}

pub(crate) fn build_json_merge(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.json.merge"))?;
    let a = require_column(props)?;
    let b = string_prop(props, "secondColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Merge needs a second column".to_string())?;
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "merged".into());
    Ok(format!(
        "SELECT *, json_merge_patch(CAST({} AS JSON), CAST({} AS JSON)) AS {} FROM {}",
        quote_ident(&a),
        quote_ident(&b),
        quote_ident(&out),
        quote_ident(upstream)
    ))
}

pub(crate) fn build_arr_collect(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.arr.collect"))?;
    let value = string_prop(props, "valueColumn")
        .or_else(|| string_prop(props, "column"))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Collect needs a value column".to_string())?;
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "items".into());
    let group = columns_list(props, "groupBy");
    // Order the collected elements by the value so the array is deterministic;
    // without it list() consumes rows in an unspecified order under
    // preserve_insertion_order=false and the array varies run-to-run.
    let v = quote_ident(&value);
    if group.is_empty() {
        Ok(format!(
            "SELECT list({v} ORDER BY {v}) AS {} FROM {}",
            quote_ident(&out),
            quote_ident(upstream),
            v = v,
        ))
    } else {
        let g = group.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        Ok(format!(
            "SELECT {}, list({v} ORDER BY {v}) AS {} FROM {} GROUP BY {}",
            g,
            quote_ident(&out),
            quote_ident(upstream),
            g,
            v = v,
        ))
    }
}

/// xf.zip - "Zip Arrays to Table": turn a row that carries a list of column
/// names and a list of row-arrays (e.g. {headings:[...], rows:[[...],[...]]})
/// into one output row per inner array, with one real column per heading. It
/// explodes the values list, aligns each inner array with the headings by
/// position, then PIVOTs the heading->value pairs into columns. The output
/// column set is data-driven, so this is a dynamic PIVOT (forced to a TABLE,
/// like xf.pivot / xf.transpose).
pub(crate) fn build_zip(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.zip"))?;
    let headings = string_prop(props, "headingsColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Zip needs a headings column (a list of column names)".to_string())?;
    let values = string_prop(props, "valuesColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Zip needs a values column (a list of row arrays)".to_string())?;
    let up = quote_ident(upstream);
    let h = quote_ident(&headings);
    let v = quote_ident(&values);
    // __duckle_rid keeps each exploded row distinct through the PIVOT; range()
    // walks each position so headings[i] pairs with values[i]; EXCLUDE drops the
    // synthetic id from the result.
    Ok(format!(
        "SELECT * EXCLUDE (__duckle_rid) FROM (\
PIVOT (\
SELECT __duckle_ex.__duckle_rid, \
__duckle_ex.__duckle_h[__duckle_i] AS __duckle_key, \
__duckle_ex.__duckle_v[__duckle_i] AS __duckle_val \
FROM (\
SELECT row_number() OVER () AS __duckle_rid, {h} AS __duckle_h, __duckle_rv AS __duckle_v \
FROM {up}, UNNEST({v}) AS __duckle_t(__duckle_rv)\
) __duckle_ex, \
UNNEST(range(1, len(__duckle_ex.__duckle_h) + 1)) AS __duckle_g(__duckle_i)\
) ON __duckle_key USING first(__duckle_val) GROUP BY __duckle_rid ORDER BY __duckle_rid\
)",
        h = h,
        v = v,
        up = up,
    ))
}

pub(crate) fn build_arr_contains(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.arr.contains"))?;
    let column = require_column(props)?;
    let value = string_prop(props, "value").unwrap_or_default();
    // Only emit a bare numeric literal for a FINITE number. Rust's f64
    // parse also accepts "inf"/"nan"/"infinity"/"1e999"(->inf), none of
    // which are valid DuckDB numeric tokens - emitting them bare caused a
    // hard parse/binder error. Treat those as string search values.
    let lit = match value.trim().parse::<f64>() {
        Ok(n) if n.is_finite() => value.trim().to_string(),
        _ => format!("'{}'", sql_escape(&value)),
    };
    // COALESCE wrap: list_contains returns NULL when the array column
    // itself is NULL (not just missing the value). Without this, any
    // downstream `WHERE _contains` would silently drop NULL-array rows -
    // same class of bug as the IN/NOT IN gotcha we fixed in semi/anti.
    // Empty array correctly returns FALSE; only the NULL-array case
    // needs the COALESCE shield.
    let expr = format!(
        "COALESCE(list_contains({}, {}), FALSE)",
        quote_ident(&column),
        lit
    );
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_contains", column));
    Ok(format!(
        "SELECT *, {} AS {} FROM {}",
        expr,
        quote_ident(&out),
        quote_ident(upstream)
    ))
}

/// Whether the form asked for positional column matching.
///
/// The four set operations declare a `matchBy` select and neither builder took
/// props, so "By position" silently produced a by-name result: inputs that are
/// positionally aligned but differently NAMED were padded with NULLs into a
/// wider table instead of stacked. By name stays the default, which is what an
/// untouched node has always done.
fn matches_by_position(props: &JsonValue) -> bool {
    string_prop(props, "matchBy")
        .map(|v| v.trim().eq_ignore_ascii_case("position"))
        .unwrap_or(false)
}

pub(crate) fn build_union(
    inputs: &NodeInputs,
    distinct: bool,
    props: &JsonValue,
) -> Result<String, String> {
    let mains = inputs.all_main_ports();
    if mains.is_empty() {
        return Err("Union needs at least one input".into());
    }
    // Default to `UNION [ALL] BY NAME` - DuckDB-specific syntax that
    // matches columns by name across inputs, padding missing columns
    // with NULL on each side. The standard SQL `UNION [ALL]` matches
    // by POSITION and silently produces garbage if columns are reordered
    // or one input has an extra column. ETL users almost always expect
    // by-name semantics; legacy positional behavior is still reachable
    // by reordering / projecting columns upstream.
    let op = match (distinct, matches_by_position(props)) {
        (true, false) => " UNION BY NAME ",
        (false, false) => " UNION ALL BY NAME ",
        (true, true) => " UNION ",
        (false, true) => " UNION ALL ",
    };
    Ok(mains
        .iter()
        .map(|id| format!("SELECT * FROM {}", quote_ident(id)))
        .collect::<Vec<_>>()
        .join(op))
}

pub(crate) fn build_setop(
    inputs: &NodeInputs,
    op: &str,
    props: &JsonValue,
) -> Result<String, String> {
    let mains = inputs.all_main_ports();
    if mains.len() < 2 {
        return Err(format!("{} needs two inputs", op));
    }
    // By position there is nothing to realign: the legs are compared as they
    // stand, which is what positional set semantics mean.
    if matches_by_position(props) {
        return Ok(mains
            .iter()
            .map(|id| format!("SELECT * FROM {}", quote_ident(id)))
            .collect::<Vec<_>>()
            .join(&format!(" {} ", op)));
    }
    // Match by column NAME, not position - otherwise INTERSECT/EXCEPT silently
    // compare the wrong columns when the inputs have a different column order.
    // DuckDB only accepts `BY NAME` after UNION (not INTERSECT/EXCEPT), so we
    // realign every later leg to the first leg's columns via a 0-row
    // `<first> WHERE false UNION ALL BY NAME <leg>` template, then join the legs
    // with the plain set operator. (Plain `INTERSECT BY NAME` is a parser error.)
    let first = quote_ident(mains[0]);
    let mut parts = vec![format!("SELECT * FROM {}", first)];
    for id in &mains[1..] {
        parts.push(format!(
            "SELECT * FROM (SELECT * FROM {f} WHERE false UNION ALL BY NAME SELECT * FROM {n})",
            f = first,
            n = quote_ident(id)
        ));
    }
    Ok(parts.join(&format!(" {} ", op)))
}

pub(crate) fn build_window(
    inputs: &NodeInputs,
    props: &JsonValue,
    component_id: &str,
) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "window: missing main input".to_string())?;
    let func = string_prop(props, "function")
        .unwrap_or_else(|| component_id.rsplit('.').next().unwrap_or("rownum").to_string());
    let target = string_prop(props, "targetColumn").filter(|s| !s.is_empty());
    let offset = props.get("offset").and_then(JsonValue::as_u64).unwrap_or(1);
    let need_target = |f: &str| -> Result<String, String> {
        target
            .clone()
            .map(|c| quote_ident(&c))
            .ok_or_else(|| format!("Window function '{}' needs a target column", f))
    };
    let call = match func.as_str() {
        "rownum" => "ROW_NUMBER()".to_string(),
        "rank" => "RANK()".to_string(),
        "denserank" => "DENSE_RANK()".to_string(),
        "lead" => format!("LEAD({}, {})", need_target("lead")?, offset),
        "lag" => format!("LAG({}, {})", need_target("lag")?, offset),
        "first" => format!("FIRST_VALUE({})", need_target("first")?),
        "last" => format!("LAST_VALUE({})", need_target("last")?),
        "ntile" => {
            // NTILE needs its own bucket count, not the lead/lag offset (which
            // defaults to 1 -> a single useless bucket).
            let buckets = props
                .get("ntileBuckets")
                .or_else(|| props.get("buckets"))
                .and_then(JsonValue::as_u64)
                .unwrap_or(4);
            if buckets < 1 {
                return Err("NTILE needs a bucket count of at least 1".to_string());
            }
            format!("NTILE({})", buckets)
        }
        other => return Err(format!("Unknown window function '{}'", other)),
    };
    let partition = columns_list(props, "partitionBy");
    let order = columns_list(props, "orderBy");
    // Every function build_window handles is order-sensitive: ROW_NUMBER,
    // RANK, DENSE_RANK, LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTILE all
    // produce nonsense (or DuckDB errors) without ORDER BY. Catch it at
    // compile time with a clear message instead of letting DuckDB raise
    // "OVER clause requires ORDER BY" two stages later.
    if order.is_empty() {
        return Err(format!(
            "Window function '{}' needs at least one Order By column (otherwise the result has no defined order)",
            func
        ));
    }
    let mut over = String::new();
    if !partition.is_empty() {
        over.push_str(&format!(
            "PARTITION BY {}",
            partition.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        ));
    }
    if !over.is_empty() {
        over.push(' ');
    }
    over.push_str(&format!(
        "ORDER BY {}",
        order.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
    ));
    let out_name = string_prop(props, "outputName")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| func.clone());
    // FIRST_VALUE / LAST_VALUE need an explicit full-partition frame. With
    // an ORDER BY present (always, above) the default window frame is RANGE
    // BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, so LAST_VALUE returns the
    // CURRENT row's value, not the partition's last - a silent wrong result.
    // Span the whole partition so "last"/"first" mean what the user picked.
    let frame = match func.as_str() {
        "first" | "last" => " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        _ => "",
    };
    Ok(format!(
        "SELECT *, {} OVER ({}{}) AS {} FROM {}",
        call,
        over,
        frame,
        quote_ident(&out_name),
        quote_ident(upstream)
    ))
}

pub(crate) fn build_pivot(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "pivot: missing main input".to_string())?;
    let pivot_col = string_prop(props, "pivotColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Pivot needs a pivot column".to_string())?;
    let value_col = string_prop(props, "valueColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Pivot needs a value column".to_string())?;
    let agg = string_prop(props, "aggregation").unwrap_or_else(|| "sum".into());
    let mut sql = format!(
        "PIVOT (SELECT * FROM {}) ON {} USING {}({})",
        quote_ident(upstream),
        quote_ident(&pivot_col),
        agg,
        quote_ident(&value_col)
    );
    let group = columns_list(props, "groupBy");
    if !group.is_empty() {
        sql.push_str(&format!(
            " GROUP BY {}",
            group.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(sql)
}

pub(crate) fn missing_input_msg(component: &str) -> String {
    format!("{} is missing its input connection", component)
}

/// Emit a per-row column expression: add it as `output` if given, else
/// replace the source column in place.
pub(crate) fn apply_col_expr(upstream: &str, column: &str, expr: String, output: Option<String>) -> String {
    match output.filter(|s| !s.trim().is_empty()) {
        Some(out) => format!(
            "SELECT *, {} AS {} FROM {}",
            expr,
            quote_ident(out.trim()),
            quote_ident(upstream)
        ),
        None => format!(
            "SELECT * REPLACE ({} AS {}) FROM {}",
            expr,
            quote_ident(column),
            quote_ident(upstream)
        ),
    }
}

pub(crate) fn require_column(props: &JsonValue) -> Result<String, String> {
    string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "This transform needs a column".to_string())
}

/// Escape stray literal `%` in an xf.format pattern so printf does not
/// mis-parse them as conversion specifiers. A bare `%` not beginning a
/// valid spec corrupts the output (audit B5: '100% done' -> '100 5one').
/// Each `%` that does NOT start a valid printf conversion (optional
/// flags/width/precision then a conversion char, or `%%`) is doubled;
/// intended specifiers like %s, %d, %.2f, %% are left untouched.
pub(crate) fn escape_stray_printf_percents(pattern: &str) -> String {
    let bytes = pattern.as_bytes();
    let mut out = String::with_capacity(pattern.len() + 4);
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'%' {
            let ch = pattern[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        let mut j = i + 1;
        if j < bytes.len() && bytes[j] == b'%' {
            out.push_str("%%");
            i = j + 1;
            continue;
        }
        // printf flags, EXCLUDING space: a space after % almost always
        // means a literal percent followed by prose ("50% off"), not the
        // C space-flag. Including it made "% o"/"% d" in ordinary text
        // parse as a spec and skip escaping (audit B5 test).
        while j < bytes.len() && matches!(bytes[j], b'-' | b'+' | b'0' | b'#') {
            j += 1;
        }
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b'.' {
            j += 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
        }
        let is_spec = j < bytes.len()
            && matches!(
                bytes[j],
                b's' | b'd' | b'i' | b'u' | b'f' | b'F' | b'g' | b'G' | b'e' | b'E'
                    | b'x' | b'X' | b'o' | b'c' | b'b'
            );
        if is_spec {
            out.push_str(&pattern[i..=j]);
            i = j + 1;
        } else {
            out.push_str("%%");
            i += 1;
        }
    }
    out
}

pub(crate) fn build_string(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    let pattern = string_prop(props, "pattern").unwrap_or_default();
    let replacement = string_prop(props, "replacement").unwrap_or_default();
    let expr = match component_id {
        "xf.regex" => format!(
            "regexp_replace(CAST({} AS VARCHAR), '{}', '{}', 'g')",
            col,
            sql_escape(&pattern),
            sql_escape(&replacement)
        ),
        "xf.regex.extract" => {
            // #109: when groupNames is set, use DuckDB's name_list form, which
            // returns a STRUCT with one field per name (positional: the Nth name
            // maps to the Nth capture group). Otherwise keep the integer-group
            // scalar path, fully backward compatible.
            let names_raw = string_prop(props, "groupNames").unwrap_or_default();
            let names: Vec<String> = if names_raw.trim_start().starts_with('[') {
                serde_json::from_str::<Vec<String>>(&names_raw).unwrap_or_default()
            } else {
                names_raw
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            };
            if names.is_empty() {
                let group_idx = props
                    .get("groupIndex")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0)
                    .max(0);
                format!(
                    "regexp_extract(CAST({} AS VARCHAR), '{}', {})",
                    col,
                    sql_escape(&pattern),
                    group_idx
                )
            } else {
                let name_list = names
                    .iter()
                    .map(|n| format!("'{}'", sql_escape(n)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "regexp_extract(CAST({} AS VARCHAR), '{}', [{}])",
                    col,
                    sql_escape(&pattern),
                    name_list
                )
            }
        }
        "xf.regex.match" => format!(
            "regexp_matches(CAST({} AS VARCHAR), '{}')",
            col,
            sql_escape(&pattern)
        ),
        "xf.trim" => format!("trim(CAST({} AS VARCHAR))", col),
        "xf.case" => match pattern.to_lowercase().as_str() {
            "lower" => format!("lower(CAST({} AS VARCHAR))", col),
            "title" | "initcap" | "proper" => format!("initcap(CAST({} AS VARCHAR))", col),
            _ => format!("upper(CAST({} AS VARCHAR))", col),
        },
        "xf.length" => format!("length(CAST({} AS VARCHAR))", col),
        "xf.substring" => {
            let start = pattern.trim().parse::<i64>().unwrap_or(1).max(1);
            match replacement.trim().parse::<i64>() {
                Ok(len) => format!("substring(CAST({} AS VARCHAR), {}, {})", col, start, len),
                Err(_) => format!("substring(CAST({} AS VARCHAR), {})", col, start),
            }
        }
        "xf.concat" => format!("concat(CAST({} AS VARCHAR), '{}')", col, sql_escape(&pattern)),
        "xf.split" => format!("string_split(CAST({} AS VARCHAR), '{}')", col, sql_escape(&pattern)),
        "xf.format" => format!("printf('{}', {})", sql_escape(&escape_stray_printf_percents(&pattern)), col),
        other => return Err(format!("String op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

pub(crate) fn build_numeric(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    let arg = num_prop(props, "argument");
    // num_prop accepts any f64-parseable string, including 'inf'/'nan'/
    // 'infinity', which it then emits BARE as an operand. DuckDB parses
    // those tokens as column references, not float literals, so the stage
    // fails with a confusing "column not found" binder error (audit B5,
    // verified). Reject a non-finite numeric argument with a clear planner
    // error. Overflow literals like 1e400 stay allowed - DuckDB accepts
    // them - so only the literal inf/nan spellings are guarded.
    if let Some(a) = arg.as_deref() {
        let low = a.trim().to_ascii_lowercase();
        if matches!(
            low.as_str(),
            "inf" | "-inf" | "+inf" | "infinity" | "-infinity" | "+infinity" | "nan" | "-nan" | "+nan"
        ) {
            return Err(format!(
                "{}: numeric argument must be a finite number (got '{}')",
                component_id, a
            ));
        }
    }
    let expr = match component_id {
        // DuckDB resolves round() against a native FLOAT overload, so a Float32
        // column is rounded in Float32 - and past about seven significant digits
        // that cannot move: 31.453647 and 31.453648 are the same Float32, so
        // round(col, 6) hands back the input bit for bit and the requested
        // precision is silently dropped (#227). Widening to DOUBLE first makes
        // the precision representable.
        //
        // Only FLOAT is widened. Casting every input would change results that
        // are correct today: DECIMAL rounds half-up exactly, so 8.325 -> 8.33,
        // and through binary floating point the same value comes back 8.32.
        // The cost of the guard is that a DECIMAL input leaves as DOUBLE,
        // because the two CASE branches unify; the value is unchanged.
        "xf.num.round" => {
            let decimals = arg.unwrap_or_else(|| "0".into());
            format!(
                "CASE WHEN typeof({c}) = 'FLOAT' THEN round(CAST({c} AS DOUBLE), {d}) \
                 ELSE round({c}, {d}) END",
                c = col,
                d = decimals
            )
        }
        "xf.num.abs" => format!("abs({})", col),
        "xf.num.mod" => format!("{} % {}", col, arg.ok_or("Modulo needs a divisor argument")?),
        "xf.num.power" => format!("power({}, {})", col, arg.unwrap_or_else(|| "2".into())),
        "xf.num.sqrt" => format!("sqrt({})", col),
        "xf.num.log" => match arg {
            Some(base) => format!("log({}, {})", base, col),
            None => format!("ln({})", col),
        },
        other => return Err(format!("Numeric op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

pub(crate) fn build_datetime(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    let fmt = string_prop(props, "format").unwrap_or_else(|| "%Y-%m-%d".into());
    let unit = string_prop(props, "unit").unwrap_or_else(|| "day".into());
    let tz = string_prop(props, "timezone").unwrap_or_default();
    let expr = match component_id {
        // try_strptime returns NULL on a value that doesn't match the
        // format, instead of strptime's hard error that aborts the entire
        // run on the first unparseable row (one bad date killing a whole
        // pipeline). Matches the TRY_CAST philosophy used elsewhere.
        "xf.dt.parse" => format!("try_strptime(CAST({} AS VARCHAR), '{}')", col, sql_escape(&fmt)),
        "xf.dt.format" => format!("strftime({}, '{}')", col, sql_escape(&fmt)),
        "xf.dt.extract" => format!("date_part('{}', {})", sql_escape(&unit), col),
        "xf.dt.trunc" => format!("date_trunc('{}', {})", sql_escape(&unit), col),
        "xf.dt.tz" => {
            if tz.is_empty() {
                return Err("Timezone convert needs a timezone".into());
            }
            format!("{} AT TIME ZONE '{}'", col, sql_escape(&tz))
        }
        other => return Err(format!("Date/time op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

pub(crate) fn build_json(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    let path = string_prop(props, "path").unwrap_or_default();
    let expr = match component_id {
        "xf.json.parse" => format!("CAST({} AS JSON)", col),
        "xf.json.stringify" => format!("CAST({} AS VARCHAR)", col),
        "xf.json.path" => {
            if path.is_empty() {
                return Err("JSONPath extract needs a path".into());
            }
            format!("json_extract({}, '{}')", col, sql_escape(&path))
        }
        other => return Err(format!("JSON op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

pub(crate) fn build_array(inputs: &NodeInputs, props: &JsonValue, component_id: &str) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg(component_id))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);
    if component_id == "xf.arr.explode" {
        // One row per element, keeping the other columns. Outer-style: a
        // NULL or empty array yields one row with a NULL element instead
        // of being silently dropped. Plain unnest() of NULL/[] produces
        // zero rows, which loses the row's other columns entirely - real
        // data loss for sparse arrays. The CASE injects a single NULL
        // element so the row survives; untyped [NULL] unifies with any
        // array element type.
        return Ok(format!(
            "SELECT unnest(CASE WHEN {c} IS NULL OR length({c}) = 0 THEN [NULL] ELSE {c} END) AS {c}, * EXCLUDE ({c}) FROM {up}",
            c = col,
            up = quote_ident(upstream)
        ));
    }
    let expr = match component_id {
        "xf.arr.element" => {
            let idx = props.get("index").and_then(JsonValue::as_i64).unwrap_or(1);
            format!("{}[{}]", col, idx)
        }
        "xf.arr.distinct" => format!("list_distinct({})", col),
        other => return Err(format!("Array op '{}' is not implemented", other)),
    };
    Ok(apply_col_expr(upstream, &column, expr, string_prop(props, "outputColumn")))
}

pub(crate) fn build_reorder(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.reorder"))?;
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    let listed = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    // Listed columns first, everything else after - never drops a column.
    Ok(format!(
        "SELECT {}, * EXCLUDE ({}) FROM {}",
        listed,
        listed,
        quote_ident(upstream)
    ))
}

pub(crate) fn build_count(inputs: &NodeInputs) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.count"))?;
    Ok(format!("SELECT count(*) AS row_count FROM {}", quote_ident(upstream)))
}

/// Approximate Quantile via DuckDB's t-digest. Single-row aggregate
/// (or one row per group, if `groupBy` is set). Picks `quantile` from
/// 0..1 (default 0.5 = median). approx_quantile uses fixed memory
/// regardless of cardinality, so it's the right tool for "what's the
/// p95 latency over 10B rows" instead of an exact quantile() call
/// that would need to sort the whole input.
pub(crate) fn build_approx_quantile(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.approx.quantile"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Approx Quantile needs a column".to_string())?;
    let q = props.get("quantile").and_then(|v| v.as_f64()).unwrap_or(0.5);
    let q = if (0.0..=1.0).contains(&q) { q } else { 0.5 };
    let group_by: Vec<String> = columns_from_props(props, "groupBy").unwrap_or_default();
    let alias = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_q{}", column, (q * 100.0).round() as i64));
    let select_extra = group_by
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let select = if group_by.is_empty() {
        format!("approx_quantile({}, {}) AS {}", quote_ident(&column), q, quote_ident(&alias))
    } else {
        format!(
            "{}, approx_quantile({}, {}) AS {}",
            select_extra,
            quote_ident(&column),
            q,
            quote_ident(&alias)
        )
    };
    let group_clause = if group_by.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", select_extra)
    };
    Ok(format!(
        "SELECT {} FROM {}{}",
        select,
        quote_ident(upstream),
        group_clause
    ))
}

pub(crate) fn build_cross_join(inputs: &NodeInputs) -> Result<String, String> {
    let left = inputs.main().ok_or_else(|| "Cross join needs a main input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "Cross join needs a lookup input".to_string())?;
    Ok(format!(
        "SELECT * FROM {} CROSS JOIN {}",
        quote_ident(left),
        quote_ident(right)
    ))
}

/// Window aggregate: an aggregate computed over a window, keeping every
/// row (unlike Group By, which collapses them).
pub(crate) fn build_window_aggregate(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.aggwin"))?;
    let func = string_prop(props, "function").unwrap_or_else(|| "sum".into()).to_uppercase();
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "*".into());
    let call = if column == "*" {
        format!("{}(*)", func)
    } else {
        format!("{}({})", func, quote_ident(&column))
    };
    let partition = columns_list(props, "partitionBy");
    let order = columns_list(props, "orderBy");
    let mut over = String::new();
    if !partition.is_empty() {
        over.push_str(&format!(
            "PARTITION BY {}",
            partition.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        ));
    }
    if !order.is_empty() {
        if !over.is_empty() {
            over.push(' ');
        }
        over.push_str(&format!(
            "ORDER BY {}",
            order.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        ));
        // An ORDER BY in a window with no explicit frame defaults to a running
        // aggregate (RANGE UNBOUNDED PRECEDING .. CURRENT ROW), silently turning
        // a per-partition total into a cumulative one. xf.aggwin's contract is a
        // whole-partition aggregate kept on every row (xf.cumulative is the
        // running-total node), so pin the full-partition frame - matching the
        // guard build_window applies for FIRST_VALUE/LAST_VALUE.
        over.push_str(" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING");
    }
    let out = string_prop(props, "outputName")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", func.to_lowercase(), column.replace('*', "all")));
    Ok(format!(
        "SELECT *, {} OVER ({}) AS {} FROM {}",
        call,
        over,
        quote_ident(&out),
        quote_ident(upstream)
    ))
}

/// CDC Diff Detect: compare a 'new' input (main) against a 'previous'
/// input (lookup) on a natural key and tag each row inserted / deleted /
/// updated / unchanged. Updates are detected from the compare columns;
/// unchanged rows are dropped unless the user keeps them.
pub(crate) fn build_cdc_diff(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let cur = inputs
        .main()
        .ok_or_else(|| "Diff Detect needs a 'new' input on the main port".to_string())?;
    let prev = inputs.first_lookup().ok_or_else(|| {
        "Diff Detect needs a 'previous' input (connect it to the previous port)".to_string()
    })?;
    let keys = columns_list(props, "naturalKey");
    if keys.is_empty() {
        return Err("Diff Detect needs natural key columns".to_string());
    }
    let compares = columns_list(props, "compareColumns");
    // Require compareColumns: with none, the `updated` CASE arm below is
    // empty, so every matched-key row - changed or not - falls through to
    // 'unchanged' and is dropped by the default rejectUnchanged=true,
    // silently losing all updates (audit B3, HIGH). This guard always
    // fires (unlike the schema-gated check_list path in compile()).
    if compares.is_empty() {
        return Err(
            "Diff Detect needs compare columns (the columns to check for changes); \
             without them every changed row would be dropped as 'unchanged'"
                .to_string(),
        );
    }
    let reject_unchanged = props
        .get("rejectUnchanged")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let coalesced = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("COALESCE(cur.{q}, prev.{q}) AS {q}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let excl = keys
        .iter()
        .map(|k| quote_ident(k))
        .collect::<Vec<_>>()
        .join(", ");
    let join_on = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("cur.{q} = prev.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let first_key = quote_ident(&keys[0]);
    let updated = if compares.is_empty() {
        String::new()
    } else {
        let diff = compares
            .iter()
            .map(|c| {
                let q = quote_ident(c);
                format!("cur.{q} IS DISTINCT FROM prev.{q}")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        format!("WHEN ({diff}) THEN 'updated' ")
    };
    let inner = format!(
        "SELECT {coalesced}, cur.* EXCLUDE ({excl}), \
         CASE WHEN prev.{first_key} IS NULL THEN 'inserted' \
         WHEN cur.{first_key} IS NULL THEN 'deleted' \
         {updated}ELSE 'unchanged' END AS change_type \
         FROM {cur} cur FULL OUTER JOIN {prev} prev ON {join_on}",
        cur = quote_ident(cur),
        prev = quote_ident(prev),
    );
    if reject_unchanged {
        Ok(format!(
            "SELECT * FROM ({inner}) WHERE change_type != 'unchanged'"
        ))
    } else {
        Ok(inner)
    }
}

/// Denormalize: collapse many rows per group into one, joining the
/// chosen columns into a single delimited cell with string_agg.
pub(crate) fn build_denormalize(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.denorm"))?;
    let group_by = columns_list(props, "groupBy");
    if group_by.is_empty() {
        return Err("Denormalize needs group-by columns".to_string());
    }
    let agg_cols = columns_list(props, "aggregateColumns");
    if agg_cols.is_empty() {
        return Err("Denormalize needs columns to aggregate".to_string());
    }
    let sep = string_prop(props, "separator").unwrap_or_else(|| ", ".into());
    let sep_sql = sep.replace('\'', "''");
    let group_list = group_by
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    // A single ORDER BY shared by every string_agg makes the concatenation
    // deterministic (the rows feeding the aggregate are otherwise in an
    // unspecified order under preserve_insertion_order=false) AND keeps the
    // i-th element of each column aligned with the same source row. Ordering
    // each column by itself would break that cross-column alignment, so the
    // key is the full aggregate-column tuple, identical for all of them.
    let order_key = agg_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let aggs = agg_cols
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("string_agg(CAST({q} AS VARCHAR), '{sep_sql}' ORDER BY {order_key}) AS {q}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT {group_list}, {aggs} FROM {} GROUP BY {group_list}",
        quote_ident(upstream)
    ))
}

/// Normalize: explode a delimited string (or array) column into one row
/// per element, keeping the other columns.
pub(crate) fn build_normalize(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.norm"))?;
    let col = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Normalize needs a column to split".to_string())?;
    let q = quote_ident(&col);
    let sep = string_prop(props, "separator").unwrap_or_else(|| ",".into());
    // Outer-style unnest: a NULL (or empty) array/string yields one row
    // with a NULL element rather than being silently dropped (plain
    // unnest of NULL/[] produces zero rows, losing the row's other
    // columns). Matches the xf.arr.explode behavior.
    let value_expr = if sep.is_empty() {
        // Empty separator means the column is already an array.
        format!("unnest(CASE WHEN {q} IS NULL OR length({q}) = 0 THEN [NULL] ELSE {q} END)")
    } else {
        let sep_sql = sep.replace('\'', "''");
        format!(
            "unnest(CASE WHEN {q} IS NULL THEN [NULL] ELSE string_split(CAST({q} AS VARCHAR), '{sep_sql}') END)"
        )
    };
    Ok(format!(
        "SELECT * EXCLUDE ({q}), {value_expr} AS {q} FROM {}",
        quote_ident(upstream)
    ))
}

/// Transpose: swap the input's rows and columns. The output has one row
/// per original column (named `colname`) and one value column per
/// original row, named `r1`, `r2`, ... The "r" prefix keeps the column
/// names valid identifiers and parsable as a CSV header (a pure-numeric
/// header would not auto-detect). Requires the input's columns to share
/// a compatible type (UNPIVOT cannot mix unrelated types).
pub(crate) fn build_transpose(inputs: &NodeInputs) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.transpose"))?;
    Ok(format!(
        "SELECT * FROM (PIVOT (FROM (SELECT *, \
         'r' || CAST(ROW_NUMBER() OVER () AS VARCHAR) AS _row FROM {up}) \
         UNPIVOT INCLUDE NULLS (val FOR colname IN (COLUMNS(* EXCLUDE _row)))) \
         ON _row USING first(val) GROUP BY colname)",
        up = quote_ident(upstream)
    ))
}

/// Switch / Conditional Split. Routes rows to case_1 ... case_N output
/// ports based on the form's `branches` (a key-value of branch name
/// -> boolean SQL expression). First-match-wins: a row that satisfied
/// branch i is excluded from branches i+1..N and from default. Up to
/// 3 cases (matching the fixed port set) plus a default for the
/// remainder. The form's branch object preserves insertion order
/// because the workspace enables serde_json's preserve_order feature.
pub(crate) fn build_switch(
    node_id: &str,
    inputs: &NodeInputs,
    props: &JsonValue,
    consumer_count: &HashMap<String, usize>,
) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("ctl.switch"))?;
    // `branches` is a key-value field. The UI saves it as an ARRAY of
    // {key,value} (which also preserves branch order = case_1, case_2, ...);
    // older docs may have an object. Accept both, mirroring
    // headers_from_props. The value is the branch condition; the key is
    // just the branch label.
    let mut conds: Vec<String> = Vec::new();
    let raw = props.get("branches");
    if let Some(arr) = raw.and_then(|v| v.as_array()) {
        for item in arr {
            if let Some(c) = item
                .get("value")
                .and_then(|x| x.as_str())
                .filter(|s| !s.trim().is_empty())
            {
                conds.push(c.to_string());
            }
        }
    } else if let Some(obj) = raw.and_then(|v| v.as_object()) {
        for (_name, val) in obj {
            if let Some(c) = val.as_str().filter(|s| !s.trim().is_empty()) {
                conds.push(c.to_string());
            }
        }
    }
    conds.truncate(3);
    if conds.is_empty() {
        return Err("Switch needs at least one branch condition".to_string());
    }
    // Each branch/default port picks VIEW vs TABLE by its OWN downstream
    // consumer count, matching the main/reject policy (audit B9): a single
    // consumer -> lazy VIEW (DuckDB inlines it, no row copy), 2+ -> TABLE.
    // A case port with ZERO consumers is skipped entirely - but its
    // condition is STILL pushed into the negation chain (`prior`), or
    // first-match-wins routing would break and later branches/default would
    // wrongly claim its rows. DUCKLE_FORCE_VIEWS forces views as elsewhere.
    let force_views = std::env::var("DUCKLE_FORCE_VIEWS")
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true")
        })
        .unwrap_or(false);
    let kw = |relation: &str| -> &'static str {
        let consumers = consumer_count.get(relation).copied().unwrap_or(0);
        if force_views || consumers <= 1 { "VIEW" } else { "TABLE" }
    };
    let up = quote_ident(upstream);
    let mut stmts: Vec<String> = Vec::new();
    let mut prior: Vec<String> = Vec::new();
    // Guard every condition with COALESCE(..., FALSE): a row whose
    // condition evaluates to NULL (e.g. comparing a NULL column) is
    // neither TRUE for its branch nor caught by the default's NOT(...)
    // chain (NOT NULL = NULL), so without this it falls through every
    // case AND the default and is silently lost. COALESCE makes NULL
    // behave as "did not match", routing the row to the default branch.
    for (i, cond) in conds.iter().enumerate() {
        let case_rel = format!("{}__case_{}", node_id, i + 1);
        let positive = format!("COALESCE(({}), FALSE)", cond);
        let where_clause = if prior.is_empty() {
            positive
        } else {
            let neg = prior
                .iter()
                .map(|p| format!("NOT COALESCE(({}), FALSE)", p))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("{} AND {}", positive, neg)
        };
        // Skip a dead (unwired) branch port, but ALWAYS extend the negation
        // chain below so first-match-wins for later branches stays correct.
        let consumers = consumer_count.get(&case_rel).copied().unwrap_or(0);
        if consumers >= 1 || force_views {
            stmts.push(format!(
                "CREATE OR REPLACE {} {} AS SELECT * FROM {} WHERE {}",
                kw(&case_rel),
                quote_ident(&case_rel),
                up,
                where_clause
            ));
        }
        prior.push(cond.clone());
    }
    // Default: rows that no branch matched (including NULL-condition rows).
    // Always emitted so the stage SQL is never empty even if every case
    // port is unwired. Lazy VIEW unless 2+ consumers.
    let default_rel = format!("{}__default", node_id);
    let default_where = prior
        .iter()
        .map(|p| format!("NOT COALESCE(({}), FALSE)", p))
        .collect::<Vec<_>>()
        .join(" AND ");
    stmts.push(format!(
        "CREATE OR REPLACE {} {} AS SELECT * FROM {} WHERE {}",
        kw(&default_rel),
        quote_ident(&default_rel),
        up,
        default_where
    ));
    Ok(stmts.join("; "))
}

/// SCD Type 1: overwrite-in-place. Output is the resolved current
/// state: every row from `current`, plus rows from `previous` whose
/// key isn't in current (so unrelated history isn't dropped). Both
/// inputs must have the same column schema.
pub(crate) fn build_scd1(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let cur = inputs.main().ok_or_else(|| missing_input_msg("xf.cdc.scd1"))?;
    let prev = inputs.first_lookup().ok_or_else(|| {
        "SCD1 needs a 'previous' input on the lookup port".to_string()
    })?;
    let keys = columns_list(props, "naturalKey");
    if keys.is_empty() {
        return Err("SCD1 needs natural key columns".to_string());
    }
    let key_eq = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("p.{q} = c.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    // UNION ALL BY NAME (not positional): the retained unmatched-previous
    // rows must align to `current` by column NAME. Positional UNION ALL
    // silently swaps values when the two inputs present columns in a
    // different order (audit B3, DuckDB-verified). SCD1's documented
    // precondition is that both inputs share a schema; BY NAME additionally
    // tolerates column-order differences instead of corrupting them.
    Ok(format!(
        "SELECT * FROM {cur} \
         UNION ALL BY NAME \
         SELECT * FROM {prev} p WHERE NOT EXISTS (SELECT 1 FROM {cur} c WHERE {key_eq})",
        cur = quote_ident(cur),
        prev = quote_ident(prev),
    ))
}

/// Merge / Upsert: output the delta to write into a target -  the
/// rows in `current` that are either a new key or a changed value.
/// Unchanged rows are skipped (the target already has them). Deletes
/// are NOT emitted; use Diff Detect when you need them.
pub(crate) fn build_upsert(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let cur = inputs.main().ok_or_else(|| missing_input_msg("xf.cdc.upsert"))?;
    let prev = inputs.first_lookup().ok_or_else(|| {
        "Upsert needs a 'previous' input on the lookup port".to_string()
    })?;
    let keys = columns_list(props, "naturalKey");
    if keys.is_empty() {
        return Err("Upsert needs natural key columns".to_string());
    }
    let compares = columns_list(props, "compareColumns");
    let key_eq = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("cur.{q} = p.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let first_key = quote_ident(&keys[0]);
    let change_clause = if compares.is_empty() {
        // No compare columns means we only flag new keys; everything
        // already in previous (regardless of value) is skipped.
        String::new()
    } else {
        let cmp_diff = compares
            .iter()
            .map(|c| {
                let q = quote_ident(c);
                format!("cur.{q} IS DISTINCT FROM p.{q}")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        format!(" OR ({cmp_diff})")
    };
    Ok(format!(
        "SELECT cur.* FROM {cur} cur LEFT JOIN {prev} p ON {key_eq} \
         WHERE p.{first_key} IS NULL{change_clause}",
        cur = quote_ident(cur),
        prev = quote_ident(prev),
    ))
}

/// SCD Type 2: maintain versioned history. Reads `current` on main and
/// `previous` on the lookup port; the previous input must already carry
/// the SCD columns (valid_from, valid_to, is_current) at the end of its
/// schema. Output is the new history table: closed records get their
/// valid_to + is_current updated, unchanged records pass through, and
/// new / changed keys land as fresh current versions. Compare columns
/// drive the change detection.
pub(crate) fn build_scd2(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let cur = inputs.main().ok_or_else(|| missing_input_msg("xf.cdc.scd2"))?;
    let prev = inputs.first_lookup().ok_or_else(|| {
        "SCD2 needs a 'previous' input on the lookup port (the current history table)".to_string()
    })?;
    let keys = columns_list(props, "naturalKey");
    if keys.is_empty() {
        return Err("SCD2 needs natural key columns".to_string());
    }
    let compares = columns_list(props, "compareColumns");
    if compares.is_empty() {
        return Err("SCD2 needs at least one compare column to detect changes".to_string());
    }
    let valid_from = string_prop(props, "validFromColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "valid_from".into());
    let valid_to = string_prop(props, "validToColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "valid_to".into());
    let is_current = string_prop(props, "isCurrentColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "is_current".into());

    let key_eq = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("p.{q} = c.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let cmp_diff = compares
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("p.{q} IS DISTINCT FROM c.{q}")
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let cmp_same = compares
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("p.{q} IS NOT DISTINCT FROM c.{q}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let first_key = quote_ident(&keys[0]);
    let vf = quote_ident(&valid_from);
    let vt = quote_ident(&valid_to);
    let ic = quote_ident(&is_current);
    let cur_q = quote_ident(cur);
    let prev_q = quote_ident(prev);

    Ok(format!(
        "WITH prev_current AS (SELECT * FROM {prev_q} WHERE {ic}), \
              prev_history AS (SELECT * FROM {prev_q} WHERE NOT {ic}), \
              to_close AS (SELECT p.* FROM prev_current p LEFT JOIN {cur_q} c ON {key_eq} \
                           WHERE c.{first_key} IS NULL OR ({cmp_diff})), \
              to_keep AS (SELECT p.* FROM prev_current p INNER JOIN {cur_q} c ON {key_eq} \
                          WHERE {cmp_same}), \
              to_insert AS (SELECT c.* FROM {cur_q} c LEFT JOIN prev_current p ON {key_eq} \
                            WHERE p.{first_key} IS NULL OR ({cmp_diff})) \
         SELECT * FROM prev_history \
         UNION ALL SELECT * FROM to_keep \
         UNION ALL SELECT * REPLACE (CURRENT_TIMESTAMP AS {vt}, FALSE AS {ic}) FROM to_close \
         UNION ALL SELECT *, CURRENT_TIMESTAMP AS {vf}, NULL::TIMESTAMP AS {vt}, TRUE AS {ic} FROM to_insert"
    ))
}

/// Unpivot: turn a set of columns into name/value rows (wide to long).
pub(crate) fn build_unpivot(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.unpivot"))?;
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        return Err("Unpivot needs the columns to unpivot".to_string());
    }
    let name_col = string_prop(props, "nameColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "name".into());
    let value_col = string_prop(props, "valueColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "value".into());
    let on = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    // INCLUDE NULLS: DuckDB's UNPIVOT defaults to EXCLUDE NULLS, which
    // silently drops every row whose unpivoted value is NULL - on sparse
    // wide data that's real data loss. The SQL-standard form is the only
    // one that accepts INCLUDE NULLS (the parenthesized statement form
    // rejects it), so emit that: `... UNPIVOT INCLUDE NULLS (value FOR
    // name IN (cols))`.
    Ok(format!(
        "SELECT * FROM {} UNPIVOT INCLUDE NULLS ({} FOR {} IN ({}))",
        quote_ident(upstream),
        quote_ident(&value_col),
        quote_ident(&name_col),
        on
    ))
}

/// Column Profile: one summary-stats row per column, via DuckDB
/// SUMMARIZE (count, null %, approx distinct, min/max, quartiles).
pub(crate) fn build_profile(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.profile"))?;
    let cols = columns_list(props, "columns");
    let projection = if cols.is_empty() {
        "*".to_string()
    } else {
        cols.iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Ok(format!(
        "SELECT * FROM (SUMMARIZE SELECT {} FROM {})",
        projection,
        quote_ident(upstream)
    ))
}

/// Describe: the column names and types of the input.
pub(crate) fn build_describe(inputs: &NodeInputs) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.describe"))?;
    Ok(format!(
        "SELECT * FROM (DESCRIBE SELECT * FROM {})",
        quote_ident(upstream)
    ))
}

/// Histogram: value frequencies for one column, most frequent first.
pub(crate) fn build_histogram(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.histogram"))?;
    let col = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Histogram needs a column".to_string())?;
    let q = quote_ident(&col);
    Ok(format!(
        "SELECT {q} AS value, COUNT(*) AS frequency FROM {} GROUP BY {q} ORDER BY frequency DESC, value",
        quote_ident(upstream)
    ))
}

/// Standardize: trim, case-normalize, and collapse internal whitespace in
/// the chosen text columns, in place.
/// The `SELECT * REPLACE` clause masking one column. hash = deterministic
/// pseudonym md5(['salt' ||] value) - same input maps to the same token (with a
/// shared salt, joinable across masked datasets); partial = keep the last N
/// chars and star the rest; null = drop the value; constant = a fixed
/// replacement. NULL inputs stay NULL.
fn mask_replacement(column: &str, mode: &str, salt: Option<&str>, show_last: i64, value: Option<&str>) -> Result<String, String> {
    let q = quote_ident(column);
    let cv = format!("CAST({} AS VARCHAR)", q);
    let expr = match mode {
        "null" => "NULL".to_string(),
        "constant" => format!("'{}'", sql_escape(value.unwrap_or(""))),
        "hash" => match salt.filter(|s| !s.trim().is_empty()) {
            Some(s) => format!("md5('{}' || {})", sql_escape(s), cv),
            None => format!("md5({})", cv),
        },
        "partial" => {
            let n = show_last.max(0);
            format!(
                "CASE WHEN {cv} IS NULL THEN NULL WHEN length({cv}) <= {n} THEN repeat('*', length({cv})) ELSE repeat('*', length({cv}) - {n}) || right({cv}, {n}) END",
                cv = cv,
                n = n
            )
        }
        other => return Err(format!("mask: unknown mode '{}' (use hash | partial | null | constant)", other)),
    };
    Ok(format!("{} AS {}", expr, q))
}

/// qa.mask: irreversibly mask / anonymize selected columns in place via a
/// `SELECT * REPLACE (...)`. Per-column rules (a `masks` array, or the single
/// column/mode form): hash (salted pseudonym), partial (show last N), null,
/// constant. Pure SQL; for GDPR/PCI-style governance without moving data.
pub(crate) fn build_mask(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.mask"))?;
    let mut repl: Vec<String> = Vec::new();
    if let Some(masks) = props.get("masks").and_then(JsonValue::as_array) {
        for m in masks {
            let column = m.get("column").and_then(JsonValue::as_str).unwrap_or("").trim();
            if column.is_empty() {
                continue;
            }
            let mode = m.get("mode").and_then(JsonValue::as_str).unwrap_or("hash");
            let salt = m.get("salt").and_then(JsonValue::as_str);
            let show_last = m.get("showLast").and_then(JsonValue::as_i64).unwrap_or(4);
            let value = m.get("value").and_then(JsonValue::as_str);
            repl.push(mask_replacement(column, mode, salt, show_last, value)?);
        }
    }
    if repl.is_empty() {
        if let Some(column) = string_prop(props, "column").filter(|s| !s.trim().is_empty()) {
            let mode = string_prop(props, "mode").unwrap_or_else(|| "hash".into());
            let salt = string_prop(props, "salt");
            let show_last = props.get("showLast").and_then(JsonValue::as_i64).unwrap_or(4);
            let value = string_prop(props, "value");
            repl.push(mask_replacement(column.trim(), &mode, salt.as_deref(), show_last, value.as_deref())?);
        }
    }
    if repl.is_empty() {
        return Err("mask: select at least one column to mask".to_string());
    }
    Ok(format!("SELECT * REPLACE ({}) FROM {}", repl.join(", "), quote_ident(upstream)))
}

/// qa.survivor: collapse duplicate records that share a group key into one
/// "golden record", choosing each surviving field by a rule. Uses DuckDB's
/// `COLUMNS(* EXCLUDE keys)` so the rule applies to every non-key column at once
/// (names preserved). Rules: most_frequent (mode), most_recent / oldest
/// (arg_max/arg_min by a recency column), max, min. The MDM merge step that
/// pairs with record matching.
pub(crate) fn build_survivor(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.survivor"))?;
    let keys = columns_list(props, "groupBy");
    if keys.is_empty() {
        return Err("survivor: choose the group-by key column(s) that identify one entity".to_string());
    }
    let key_sql = keys.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let cols = format!("COLUMNS(* EXCLUDE ({}))", key_sql);
    let rule = string_prop(props, "rule").unwrap_or_else(|| "most_frequent".into());
    let agg = match rule.as_str() {
        "most_frequent" => format!("mode({})", cols),
        "max" => format!("max({})", cols),
        "min" => format!("min({})", cols),
        "most_recent" | "oldest" => {
            let rc = string_prop(props, "recencyColumn")
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| format!("survivor: rule '{}' needs a recency column to rank by", rule))?;
            let f = if rule == "most_recent" { "arg_max" } else { "arg_min" };
            format!("{}({}, {})", f, cols, quote_ident(rc.trim()))
        }
        other => {
            return Err(format!(
                "survivor: unknown rule '{}' (use most_frequent | most_recent | oldest | max | min)",
                other
            ))
        }
    };
    Ok(format!(
        "SELECT {}, {} FROM {} GROUP BY {}",
        key_sql,
        agg,
        quote_ident(upstream),
        key_sql
    ))
}

/// qa.matchgroup: turn a list of matched record PAIRS into a stable cluster
/// id per record. Reads two id columns (leftKey / rightKey, defaults id_a /
/// id_b). Builds an undirected edge set (both pair directions) plus a self-rep
/// for every record, then walks the transitive closure with a RECURSIVE CTE and
/// assigns each id cluster_id = the MIN id reachable through any chain of
/// matches (the connected-component representative). Output: id, cluster_id.
/// The resolve step that follows Record Match in an MDM flow.
pub(crate) fn build_matchgroup(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.matchgroup"))?;
    let left = string_prop(props, "leftKey")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "id_a".to_string());
    let right = string_prop(props, "rightKey")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "id_b".to_string());
    let from = quote_ident(upstream);
    let l = quote_ident(&left);
    let r = quote_ident(&right);
    // Both pair directions, then every distinct id, then min-propagation over
    // the transitive closure. UNION (not UNION ALL) dedups the frontier so the
    // recursion terminates even on cyclic match graphs. CAST to VARCHAR so id
    // columns of any type share one comparable type and cluster_id is stable.
    Ok(format!(
        "WITH RECURSIVE \
         edges AS (\
         SELECT CAST({l} AS VARCHAR) AS s, CAST({r} AS VARCHAR) AS t FROM {from} WHERE {l} IS NOT NULL AND {r} IS NOT NULL \
         UNION \
         SELECT CAST({r} AS VARCHAR), CAST({l} AS VARCHAR) FROM {from} WHERE {l} IS NOT NULL AND {r} IS NOT NULL), \
         nodes AS (SELECT s AS id FROM edges UNION SELECT t AS id FROM edges), \
         reach(id, rep) AS (\
         SELECT id, id FROM nodes \
         UNION \
         SELECT e.t, r.rep FROM reach r JOIN edges e ON e.s = r.id) \
         SELECT id, MIN(rep) AS cluster_id FROM reach GROUP BY id",
        l = l,
        r = r,
        from = from
    ))
}

/// qa.sample.adv: take a reproducible random sample of the upstream rows via
/// `USING SAMPLE <percent> PERCENT (<method>, <seed>)` - reservoir (default) or
/// bernoulli. A seed makes the draw deterministic (stable golden files / reruns);
/// omit it for a fresh random sample each run. Pure SQL, columns preserved.
pub(crate) fn build_sample_adv(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.sample.adv"))?;
    let percent = num_prop(props, "percent")
        .ok_or_else(|| "sample: set a sampling percent (e.g. 10 for 10%)".to_string())?;
    if let Ok(p) = percent.parse::<f64>() {
        if !(0.0..=100.0).contains(&p) {
            return Err(format!("sample: percent must be between 0 and 100 (got {})", percent));
        }
    }
    let method = string_prop(props, "method").unwrap_or_else(|| "reservoir".into());
    let method = match method.as_str() {
        "reservoir" => "reservoir",
        "bernoulli" => "bernoulli",
        other => return Err(format!("sample: unknown method '{}' (use reservoir | bernoulli)", other)),
    };
    let sample_clause = match num_prop(props, "seed") {
        Some(seed) => format!("{} PERCENT ({}, {})", percent, method, seed),
        None => format!("{} PERCENT ({})", percent, method),
    };
    Ok(format!("SELECT * FROM {} USING SAMPLE {}", quote_ident(upstream), sample_clause))
}

/// qa.expect: a reusable expectation suite + data-quality scorecard - the
/// native, no-Python answer to declarative data contracts. One node holds N
/// rules ({column, check, args}); it emits ONE ROW PER RULE: expectation (text),
/// total, failed, pass_rate, passed. Built as a UNION ALL of one per-rule SELECT
/// that COUNTs total rows and rows failing the rule's predicate. Checks:
/// not_null, unique, in_set, in_range, regex, non_negative.
pub(crate) fn build_expect(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.expect"))?;
    let from = quote_ident(upstream);
    let rules = collect_expect_rules(props)?;
    if rules.is_empty() {
        return Err("Expectations needs at least one rule (column + check)".to_string());
    }
    let mut branches: Vec<String> = Vec::new();
    for (column, check, args) in &rules {
        if column.trim().is_empty() {
            return Err(format!("Expectation '{}' is missing a column", check));
        }
        let col = quote_ident(column.trim());
        let label = expect_label(column.trim(), check, args);
        let label_lit = format!("'{}'", sql_escape(&label));
        let branch = if check == "unique" {
            format!(
                "SELECT {label} AS expectation, \
                 CAST(COUNT(*) AS BIGINT) AS total, \
                 CAST(COUNT(*) FILTER (WHERE NOT __dq_ok) AS BIGINT) AS failed \
                 FROM (SELECT (COUNT(*) OVER (PARTITION BY {col}) = 1) AS __dq_ok FROM {from}) __dq",
                label = label_lit,
                col = col,
                from = from
            )
        } else {
            let pred = expect_predicate(&col, check, args)?;
            format!(
                "SELECT {label} AS expectation, \
                 CAST(COUNT(*) AS BIGINT) AS total, \
                 CAST(COUNT(*) FILTER (WHERE NOT ({pred})) AS BIGINT) AS failed \
                 FROM {from}",
                label = label_lit,
                pred = pred,
                from = from
            )
        };
        branches.push(branch);
    }
    Ok(format!(
        "SELECT expectation, total, failed, \
         CASE WHEN total = 0 THEN 1.0 ELSE CAST(total - failed AS DOUBLE) / total END AS pass_rate, \
         (failed = 0) AS passed FROM ({}) __dq_scorecard",
        branches.join(" UNION ALL ")
    ))
}

/// PASS predicate for a single check (the row passes when this is TRUE).
fn expect_predicate(col: &str, check: &str, args: &JsonValue) -> Result<String, String> {
    match check {
        "not_null" => Ok(format!("{} IS NOT NULL", col)),
        "non_negative" => Ok(format!("{} >= 0", col)),
        "in_range" => {
            let min = num_prop(args, "min");
            let max = num_prop(args, "max");
            match (min, max) {
                (Some(lo), Some(hi)) => Ok(format!("{} BETWEEN {} AND {}", col, lo, hi)),
                (Some(lo), None) => Ok(format!("{} >= {}", col, lo)),
                (None, Some(hi)) => Ok(format!("{} <= {}", col, hi)),
                (None, None) => Err("in_range check needs a numeric min and/or max in args".to_string()),
            }
        }
        "regex" => {
            let pat = args
                .as_str()
                .map(str::to_string)
                .or_else(|| string_prop(args, "pattern"))
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "regex check needs a pattern in args".to_string())?;
            Ok(format!("regexp_full_match(CAST({} AS VARCHAR), '{}')", col, sql_escape(&pat)))
        }
        "in_set" => {
            let arr = args
                .as_array()
                .or_else(|| args.get("values").and_then(JsonValue::as_array))
                .ok_or_else(|| "in_set check needs a list of allowed values in args".to_string())?;
            let mut lits: Vec<String> = Vec::new();
            for v in arr {
                match v {
                    JsonValue::String(s) => lits.push(format!("'{}'", sql_escape(s))),
                    JsonValue::Number(n) => lits.push(n.to_string()),
                    JsonValue::Bool(b) => lits.push(b.to_string()),
                    _ => {}
                }
            }
            if lits.is_empty() {
                return Err("in_set check needs at least one allowed value in args".to_string());
            }
            Ok(format!("{} IN ({})", col, lits.join(", ")))
        }
        other => Err(format!(
            "Unknown check '{}' (use not_null | unique | in_set | in_range | regex | non_negative)",
            other
        )),
    }
}

/// Human-readable expectation label for the scorecard's `expectation` column.
fn expect_label(column: &str, check: &str, args: &JsonValue) -> String {
    match check {
        "in_range" => {
            let lo = num_prop(args, "min").unwrap_or_else(|| "*".into());
            let hi = num_prop(args, "max").unwrap_or_else(|| "*".into());
            format!("in_range({}, {}, {})", column, lo, hi)
        }
        "in_set" => {
            let n = args
                .as_array()
                .or_else(|| args.get("values").and_then(JsonValue::as_array))
                .map(|a| a.len())
                .unwrap_or(0);
            format!("in_set({}, {} values)", column, n)
        }
        _ => format!("{}({})", check, column),
    }
}

/// Read the rule list: the structured `rules` array of {column, check, args}
/// (hand-authored / MCP), else the GUI key-value map (column -> "check" or
/// "check:args"), mirroring build_mask's dual path.
fn collect_expect_rules(props: &JsonValue) -> Result<Vec<(String, String, JsonValue)>, String> {
    let mut out: Vec<(String, String, JsonValue)> = Vec::new();
    if let Some(arr) = props.get("rules").and_then(JsonValue::as_array) {
        for r in arr {
            let column = r.get("column").and_then(JsonValue::as_str).unwrap_or("").to_string();
            let check = r.get("check").and_then(JsonValue::as_str).unwrap_or("").trim().to_string();
            if check.is_empty() {
                continue;
            }
            let args = r.get("args").cloned().unwrap_or(JsonValue::Null);
            out.push((column, check, args));
        }
        if !out.is_empty() {
            return Ok(out);
        }
    }
    for (column, spec) in kv_pairs(props, "rules") {
        let (check, rest) = match spec.split_once(':') {
            Some((c, r)) => (c.trim().to_string(), r.trim().to_string()),
            None => (spec.trim().to_string(), String::new()),
        };
        if check.is_empty() {
            continue;
        }
        let args = match check.as_str() {
            "in_set" => JsonValue::Array(
                rest.split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| JsonValue::String(s.to_string()))
                    .collect(),
            ),
            "in_range" => {
                let mut it = rest.split(',').map(str::trim);
                let lo = it.next().filter(|s| !s.is_empty());
                let hi = it.next().filter(|s| !s.is_empty());
                let mut o = serde_json::Map::new();
                if let Some(lo) = lo.and_then(|s| s.parse::<f64>().ok()) {
                    o.insert("min".into(), serde_json::json!(lo));
                }
                if let Some(hi) = hi.and_then(|s| s.parse::<f64>().ok()) {
                    o.insert("max".into(), serde_json::json!(hi));
                }
                JsonValue::Object(o)
            }
            "regex" => JsonValue::String(rest),
            _ => JsonValue::Null,
        };
        out.push((column, check, args));
    }
    Ok(out)
}

/// qa.refintegrity: referential-integrity / orphan check across TWO inputs.
/// Main rows whose key EXISTS in the reference input (the single `lookup` port)
/// pass through unchanged; rows whose key is missing (orphans) go to the reject
/// port. Pure-SQL semi-join (EXISTS) for the pass side, anti-join (NOT EXISTS)
/// for the reject side - no row fan-out even on duplicate reference keys, and a
/// NULL main key is treated as an orphan. `reject = true` yields the orphan rows.
pub(crate) fn build_refintegrity(
    inputs: &NodeInputs,
    props: &JsonValue,
    reject: bool,
) -> Result<String, String> {
    let main = inputs.main().ok_or_else(|| missing_input_msg("qa.refintegrity"))?;
    let reference = inputs.first_lookup().ok_or_else(|| {
        "Referential Integrity needs a reference input (connect the lookup port to the table that holds the valid keys)".to_string()
    })?;
    let left_key = string_prop(props, "leftKey")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Referential Integrity needs a main key column (leftKey)".to_string())?;
    let right_key = string_prop(props, "rightKey")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Referential Integrity needs a reference key column (rightKey)".to_string())?;
    let m = quote_ident(main);
    let r = quote_ident(reference);
    let exists = format!(
        "EXISTS (SELECT 1 FROM {r} WHERE {r}.{rk} = {m}.{lk})",
        r = r,
        m = m,
        rk = quote_ident(&right_key),
        lk = quote_ident(&left_key),
    );
    Ok(if reject {
        format!("SELECT {m}.* FROM {m} WHERE NOT {exists}", m = m, exists = exists)
    } else {
        format!("SELECT {m}.* FROM {m} WHERE {exists}", m = m, exists = exists)
    })
}

/// qa.profile.adv: a rich single-column data profile (deeper than SUMMARIZE).
/// For one chosen column it emits a long-form metric/value relation: count,
/// null_count, null_pct, distinct (approx_count_distinct), min, max, the
/// fraction of non-null values matching common patterns (email / integer /
/// decimal / date via regexp_full_match), and the top-N most frequent values
/// with their counts. Output columns: metric, value, count, pct. Pure SQL.
pub(crate) fn build_profile_adv(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.profile.adv"))?;
    let col = string_prop(props, "column")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Column Profile (Advanced) needs a column to profile".to_string())?;
    let top_n = num_prop(props, "topN")
        .and_then(|s| s.parse::<f64>().ok())
        .map(|n| n as i64)
        .unwrap_or(10)
        .clamp(1, 1000);
    let q = quote_ident(&col);
    let col_lit = sql_escape(&col);
    let re_email = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$";
    let re_int = r"^-?[0-9]+$";
    let re_dec = r"^-?[0-9]*\.[0-9]+$";
    let re_date = r"^\d{4}-\d{2}-\d{2}$";
    Ok(format!(
        "WITH __src AS (SELECT CAST({q} AS VARCHAR) AS v FROM {up}), \
         __agg AS (SELECT COUNT(*) AS total, \
         COUNT(*) FILTER (WHERE v IS NULL) AS null_n, \
         approx_count_distinct(v) AS distinct_n, MIN(v) AS min_v, MAX(v) AS max_v, \
         COUNT(*) FILTER (WHERE v IS NOT NULL) AS nonnull, \
         COUNT(*) FILTER (WHERE v IS NOT NULL AND regexp_full_match(v, '{re_email}')) AS email_n, \
         COUNT(*) FILTER (WHERE v IS NOT NULL AND regexp_full_match(v, '{re_int}')) AS int_n, \
         COUNT(*) FILTER (WHERE v IS NOT NULL AND regexp_full_match(v, '{re_dec}')) AS dec_n, \
         COUNT(*) FILTER (WHERE v IS NOT NULL AND regexp_full_match(v, '{re_date}')) AS date_n \
         FROM __src), \
         __topn AS (SELECT v AS val, COUNT(*) AS freq FROM __src WHERE v IS NOT NULL \
         GROUP BY v ORDER BY freq DESC, v LIMIT {top_n}) \
         SELECT \"metric\", \"value\", \"count\", \"pct\" FROM (\
         SELECT 'column' AS \"metric\", '{col_lit}' AS \"value\", NULL::BIGINT AS \"count\", NULL::DOUBLE AS \"pct\", 0 AS \"_ord\" \
         UNION ALL SELECT 'count', CAST(total AS VARCHAR), total, 100.0, 1 FROM __agg \
         UNION ALL SELECT 'null_count', CAST(null_n AS VARCHAR), null_n, ROUND(100.0*null_n/NULLIF(total,0),4), 2 FROM __agg \
         UNION ALL SELECT 'null_pct', CAST(ROUND(100.0*null_n/NULLIF(total,0),4) AS VARCHAR)||'%', null_n, ROUND(100.0*null_n/NULLIF(total,0),4), 3 FROM __agg \
         UNION ALL SELECT 'distinct_approx', CAST(distinct_n AS VARCHAR), distinct_n, NULL, 4 FROM __agg \
         UNION ALL SELECT 'min', min_v, NULL, NULL, 5 FROM __agg \
         UNION ALL SELECT 'max', max_v, NULL, NULL, 6 FROM __agg \
         UNION ALL SELECT 'pattern_email', CAST(ROUND(100.0*email_n/NULLIF(nonnull,0),4) AS VARCHAR)||'%', email_n, ROUND(100.0*email_n/NULLIF(nonnull,0),4), 7 FROM __agg \
         UNION ALL SELECT 'pattern_integer', CAST(ROUND(100.0*int_n/NULLIF(nonnull,0),4) AS VARCHAR)||'%', int_n, ROUND(100.0*int_n/NULLIF(nonnull,0),4), 8 FROM __agg \
         UNION ALL SELECT 'pattern_decimal', CAST(ROUND(100.0*dec_n/NULLIF(nonnull,0),4) AS VARCHAR)||'%', dec_n, ROUND(100.0*dec_n/NULLIF(nonnull,0),4), 9 FROM __agg \
         UNION ALL SELECT 'pattern_date', CAST(ROUND(100.0*date_n/NULLIF(nonnull,0),4) AS VARCHAR)||'%', date_n, ROUND(100.0*date_n/NULLIF(nonnull,0),4), 10 FROM __agg \
         UNION ALL SELECT 'top_value', __topn.val, __topn.freq, ROUND(100.0*__topn.freq/NULLIF((SELECT total FROM __agg),0),4), 11 FROM __topn\
         ) ORDER BY \"_ord\", \"count\" DESC NULLS LAST, \"value\"",
        q = q,
        up = quote_ident(upstream),
        re_email = re_email,
        re_int = re_int,
        re_dec = re_dec,
        re_date = re_date,
        top_n = top_n,
        col_lit = col_lit,
    ))
}

/// Comparison-key pair (show expr, lowered match key) for one side of a record
/// linkage join: a multi-column list (multi_key) or single column (single_key).
fn link_key(props: &JsonValue, multi_key: &str, single_key: &str, alias: &str) -> Result<(String, String), String> {
    let mut cols = columns_list(props, multi_key);
    if cols.is_empty() {
        if let Some(c) = string_prop(props, single_key).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()) {
            cols.push(c);
        }
    }
    if cols.is_empty() {
        return Err(format!("Record Linkage needs the {alias} compare column(s) ({multi_key} or {single_key})"));
    }
    let list = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    Ok((format!("concat_ws(' ', {})", list), format!("lower(concat_ws(' ', {}))", list)))
}

/// qa.link: fuzzy record LINKAGE across TWO inputs (main = left, lookup port =
/// right). Builds a key per side, cross-joins, and keeps every candidate pair
/// whose string similarity meets the threshold (jaro-winkler default, or
/// levenshtein). Single output: left_key, right_key, score. Unlike qa.match
/// (self-join), this links two separate datasets.
pub(crate) fn build_record_link(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let main = inputs.main().ok_or_else(|| missing_input_msg("qa.link"))?;
    let reference = inputs.first_lookup().ok_or_else(|| {
        "Record Linkage needs a reference input (connect the lookup port to the table to link against)".to_string()
    })?;
    let (left_show, left_key) = link_key(props, "leftColumns", "leftKey", "left (main)")?;
    let (right_show, right_key) = link_key(props, "rightColumns", "rightKey", "right (reference)")?;
    let (score, threshold) = similarity(props);
    Ok(format!(
        "WITH a AS (SELECT {ls} AS _show, {lk} AS _key FROM {m}), \
         b AS (SELECT {rs} AS _show, {rk} AS _key FROM {r}) \
         SELECT a._show AS left_key, b._show AS right_key, round({score}, 4) AS score \
         FROM a CROSS JOIN b WHERE {score} >= {threshold} ORDER BY score DESC, left_key, right_key",
        ls = left_show,
        lk = left_key,
        rs = right_show,
        rk = right_key,
        m = quote_ident(main),
        r = quote_ident(reference),
        score = score,
        threshold = threshold,
    ))
}

/// One blocking rule: a label, and the columns that must be equal for a pair
/// to be a candidate. Accepts either shape the GUI key-value field can write.
fn blocking_rules(props: &JsonValue) -> Vec<(String, Vec<String>)> {
    let split = |label: &str, spec: &str| -> Option<(String, Vec<String>)> {
        let cols: Vec<String> = spec
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect();
        if cols.is_empty() {
            None
        } else {
            Some((label.trim().to_string(), cols))
        }
    };
    match props.get("rules") {
        Some(JsonValue::Object(obj)) => obj
            .iter()
            .filter_map(|(k, v)| split(k, v.as_str()?))
            .collect(),
        Some(JsonValue::Array(arr)) => arr
            .iter()
            .filter_map(|item| {
                let k = item.get("key").and_then(|x| x.as_str())?;
                let v = item.get("value").and_then(|x| x.as_str())?;
                split(k, v)
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// qa.block: candidate-pair generation (blocking) for entity resolution.
///
/// Fuzzy matching is expensive because it compares every pair: qa.link CROSS
/// JOINs its two inputs, so linking 100k rows against 100k rows is 10^10
/// comparisons. Blocking is the standard answer - only compare records that
/// already agree on something cheap and discriminating, like the same postcode
/// or the same surname initial - and it is the piece the qa.* family did not
/// have, which is what made the rest of it unusable at real sizes.
///
/// Main input alone is dedupe mode: pairs come from the one table and each
/// unordered pair is kept once. Wire the lookup port and it links two tables.
///
/// Emits `id_a`, `id_b`, `blocking_rule`, and `a_<col>` / `b_<col>` for every
/// carried column. `id_a` / `id_b` are exactly the column names qa.matchgroup
/// already defaults to, so pairs feed clustering with nothing to configure,
/// and the carried columns are what an xf.addcol comparison expression reads
/// (`jaro_winkler_similarity(a_name, b_name)`, `abs(a_amount - b_amount)`).
pub(crate) fn build_er_block(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let main = inputs.main().ok_or_else(|| missing_input_msg("qa.block"))?;
    let reference = inputs.first_lookup();
    let self_mode = reference.is_none();
    let right_rel = reference.unwrap_or(main);

    let left_id = string_prop(props, "leftId")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "Blocking needs the id column on the main input (leftId) so each pair can be identified"
                .to_string()
        })?;
    // Linking two tables whose id columns share a name is the common case, so
    // rightId defaults to the same name rather than erroring.
    let right_id = if self_mode {
        left_id.clone()
    } else {
        string_prop(props, "rightId")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| left_id.clone())
    };

    let rules = blocking_rules(props);
    if rules.is_empty() {
        return Err("Blocking needs at least one rule: a label, and the column(s) that must be equal for a pair to be worth comparing (for example postcode_surname = postcode, surname)".to_string());
    }
    let carry = columns_list(props, "carryColumns");

    let lq = quote_ident(&left_id);
    let rq = quote_ident(&right_id);
    let mut selects: Vec<String> = Vec::with_capacity(rules.len());
    for (label, keys) in &rules {
        let mut on: Vec<String> = keys
            .iter()
            .map(|k| {
                let k = quote_ident(k);
                // An equi-join drops NULL keys by itself, which is what we want:
                // blocking on a NULL would put every unfilled record in one
                // block and undo the whole point.
                format!("l.{k} = r.{k}")
            })
            .collect();
        if self_mode {
            // Keep each unordered pair once and drop self-pairs. CAST so id
            // columns of any type share one comparable type, the same way
            // qa.matchgroup normalises its edge ids.
            on.push(format!(
                "CAST(l.{lq} AS VARCHAR) < CAST(r.{rq} AS VARCHAR)"
            ));
        }
        let mut cols = vec![
            format!("l.{lq} AS id_a"),
            format!("r.{rq} AS id_b"),
            format!("'{}' AS blocking_rule", label.replace("'", "''")),
        ];
        for c in &carry {
            let cq = quote_ident(c);
            cols.push(format!("l.{cq} AS {}", quote_ident(&format!("a_{c}"))));
            cols.push(format!("r.{cq} AS {}", quote_ident(&format!("b_{c}"))));
        }
        selects.push(format!(
            "SELECT {cols} FROM {m} l JOIN {r} r ON {on}",
            cols = cols.join(", "),
            m = quote_ident(main),
            r = quote_ident(right_rel),
            on = on.join(" AND "),
        ));
    }

    // Several rules will often propose the same pair. UNION ALL then keep one
    // row per pair, labelled with the first rule that produced it, so a pair is
    // compared once downstream instead of once per rule that caught it.
    Ok(format!(
        "SELECT * FROM ({}) QUALIFY row_number() OVER (PARTITION BY id_a, id_b ORDER BY blocking_rule) = 1",
        selects.join(" UNION ALL ")
    ))
}

/// qa.reconcile: two-source reconciliation report (source-vs-target validation
/// for migrations / CDC QA). Main = source, lookup port = target. Emits one row
/// per metric (metric, value): source_rows, target_rows, rows_only_in_source,
/// rows_only_in_target, keys_matched, plus per measure source_sum/target_sum/
/// difference. FULL OUTER JOIN on the keys (NULL-safe) + independent COUNT/SUM.
pub(crate) fn build_reconcile(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let main = inputs.main().ok_or_else(|| missing_input_msg("qa.reconcile"))?;
    let reference = inputs.first_lookup().ok_or_else(|| {
        "Reconcile needs a target input (connect the lookup port to the target table to compare against)".to_string()
    })?;
    let keys = columns_list(props, "keyColumns");
    if keys.is_empty() {
        return Err("Reconcile needs at least one key column (keyColumns) to join source and target on".to_string());
    }
    let measures = columns_list(props, "measureColumns");
    let m = quote_ident(main);
    let r = quote_ident(reference);
    let on = keys
        .iter()
        .map(|k| {
            let q = quote_ident(k);
            format!("\"__m\".{q} IS NOT DISTINCT FROM \"__r\".{q}", q = q)
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut rows: Vec<String> = Vec::new();
    rows.push("SELECT 'source_rows' AS metric, CAST((SELECT COUNT(*) FROM \"__m\") AS DOUBLE) AS value".to_string());
    rows.push("SELECT 'target_rows', CAST((SELECT COUNT(*) FROM \"__r\") AS DOUBLE)".to_string());
    rows.push("SELECT 'rows_only_in_source', CAST((SELECT COUNT(*) FROM \"__j\" WHERE \"__m_present\" AND \"__r_present\" IS NULL) AS DOUBLE)".to_string());
    rows.push("SELECT 'rows_only_in_target', CAST((SELECT COUNT(*) FROM \"__j\" WHERE \"__r_present\" AND \"__m_present\" IS NULL) AS DOUBLE)".to_string());
    rows.push("SELECT 'keys_matched', CAST((SELECT COUNT(*) FROM \"__j\" WHERE \"__m_present\" AND \"__r_present\") AS DOUBLE)".to_string());
    for col in &measures {
        let q = quote_ident(col);
        let lbl = sql_escape(col);
        rows.push(format!("SELECT '{lbl}_source_sum', CAST((SELECT SUM({q}) FROM \"__m\") AS DOUBLE)", lbl = lbl, q = q));
        rows.push(format!("SELECT '{lbl}_target_sum', CAST((SELECT SUM({q}) FROM \"__r\") AS DOUBLE)", lbl = lbl, q = q));
        rows.push(format!("SELECT '{lbl}_difference', CAST((SELECT SUM({q}) FROM \"__m\") AS DOUBLE) - CAST((SELECT SUM({q}) FROM \"__r\") AS DOUBLE)", lbl = lbl, q = q));
    }
    Ok(format!(
        "WITH \"__m\" AS (SELECT *, TRUE AS \"__present\" FROM {m}), \
         \"__r\" AS (SELECT *, TRUE AS \"__present\" FROM {r}), \
         \"__j\" AS (SELECT \"__m\".\"__present\" AS \"__m_present\", \"__r\".\"__present\" AS \"__r_present\" \
         FROM \"__m\" FULL OUTER JOIN \"__r\" ON {on}) {rows}",
        m = m,
        r = r,
        on = on,
        rows = rows.join(" UNION ALL ")
    ))
}

/// qa.classify: heuristic column classification / PII tagging - NO LLM, pure
/// regex + stats. Per selected column (props columns, or all) it measures the
/// fraction of non-null values matching known shapes (email/ssn/credit_card/
/// ipv4/uuid/url/phone/date), tags the best match above a threshold (else
/// text), and emits a report: column, detected_type, match_rate, sample_count,
/// is_pii. Drives governance auto-masking (pairs with qa.mask).
pub(crate) fn build_classify(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.classify"))?;
    let threshold = props.get("threshold").and_then(JsonValue::as_f64).unwrap_or(0.8).clamp(0.0, 1.0);
    let cols = columns_list(props, "columns");
    let cast_projection = if cols.is_empty() {
        "COLUMNS(*)::VARCHAR".to_string()
    } else {
        cols.iter().map(|c| {
            let q = quote_ident(c);
            format!("CAST({q} AS VARCHAR) AS {q}", q = q)
        }).collect::<Vec<_>>().join(", ")
    };
    let patterns: &[(&str, bool, &str)] = &[
        ("email", true, "regexp_full_match(col_val, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}')"),
        ("ssn", true, "regexp_full_match(col_val, '\\d{3}-\\d{2}-\\d{4}')"),
        ("uuid", true, "regexp_full_match(col_val, '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')"),
        ("ipv4", true, "regexp_full_match(col_val, '(25[0-5]|2[0-4]\\d|1?\\d?\\d)(\\.(25[0-5]|2[0-4]\\d|1?\\d?\\d)){3}')"),
        ("url", false, "regexp_full_match(col_val, 'https?://[^ ]+')"),
        ("date", false, "regexp_full_match(col_val, '\\d{4}-\\d{2}-\\d{2}([T ]\\d{2}:\\d{2}(:\\d{2})?)?')"),
        ("credit_card", true, "regexp_full_match(replace(replace(col_val, ' ', ''), '-', ''), '\\d{13,16}')"),
        ("phone", true, "regexp_full_match(col_val, '[+]?[-0-9 ().]{7,}')"),
    ];
    let n_cols = patterns.iter().map(|(ty, _, expr)| {
        format!("COUNT(*) FILTER (WHERE col_val IS NOT NULL AND {expr})::DOUBLE AS n_{ty}", expr = expr, ty = ty)
    }).collect::<Vec<_>>().join(", ");
    let r_cols = patterns.iter().map(|(ty, _, _)| format!("n_{ty} / NULLIF(sample_count, 0) AS r_{ty}", ty = ty)).collect::<Vec<_>>().join(", ");
    let greatest_args = patterns.iter().map(|(ty, _, _)| format!("COALESCE(r_{ty}, 0)", ty = ty)).collect::<Vec<_>>().join(", ");
    let mut type_case = format!("CASE WHEN best_rate < {threshold} THEN 'text' ", threshold = threshold);
    for (ty, _, _) in patterns {
        type_case.push_str(&format!("WHEN r_{ty} = best_rate THEN '{ty}' ", ty = ty));
    }
    type_case.push_str("ELSE 'text' END");
    let pii_types = patterns.iter().filter(|(_, pii, _)| *pii).map(|(ty, _, _)| format!("'{ty}'", ty = ty)).collect::<Vec<_>>().join(", ");
    Ok(format!(
        "WITH __cls_src AS (SELECT {cast_projection} FROM {up}), \
         __cls_m AS (FROM __cls_src UNPIVOT INCLUDE NULLS (col_val FOR col_name IN (COLUMNS(*)))), \
         __cls_agg AS (SELECT col_name, COUNT(col_val) AS sample_count, {n_cols} FROM __cls_m GROUP BY col_name), \
         __cls_rates AS (SELECT col_name, sample_count, {r_cols} FROM __cls_agg), \
         __cls_best AS (SELECT *, GREATEST({greatest_args}) AS best_rate FROM __cls_rates), \
         __cls_done AS (SELECT col_name, sample_count, best_rate, {type_case} AS detected_type FROM __cls_best) \
         SELECT col_name AS \"column\", detected_type, round(best_rate, 4) AS match_rate, sample_count, \
         detected_type IN ({pii_types}) AS is_pii FROM __cls_done ORDER BY col_name",
        cast_projection = cast_projection,
        up = quote_ident(upstream),
        n_cols = n_cols,
        r_cols = r_cols,
        greatest_args = greatest_args,
        type_case = type_case,
        pii_types = pii_types,
    ))
}

/// SCD Type 3: keep the PREVIOUS value of each tracked attribute in a sibling
/// previous_<col> column. main = current rows; the prior snapshot is on the
/// lookup port (mirrors build_scd2). Joined on keyColumns (NULL previous for new
/// keys). Optional effective-date stamp.
pub(crate) fn build_scd3(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let cur = inputs.main().ok_or_else(|| missing_input_msg("xf.cdc.scd3"))?;
    let prev = inputs.first_lookup().ok_or_else(|| {
        "SCD3 needs a 'previous' input on the lookup port (the prior snapshot)".to_string()
    })?;
    // The form writes `naturalKey` / `compareColumns`, which is also what every
    // other CDC builder reads. This one read `keyColumns` / `trackColumns` and
    // nothing else, so a node configured in the editor failed with "SCD3 needs
    // key columns" while its Natural key field was filled in, and there was no
    // way to fix that from the UI: the field the error asked for is not on the
    // form. The engine's older spelling stays accepted so a hand-written
    // pipeline that used it keeps working.
    let mut keys = columns_list(props, "naturalKey");
    if keys.is_empty() {
        keys = columns_list(props, "keyColumns");
    }
    if keys.is_empty() {
        return Err("SCD3 needs key columns (naturalKey)".to_string());
    }
    let mut tracked = columns_list(props, "compareColumns");
    if tracked.is_empty() {
        tracked = columns_list(props, "trackColumns");
    }
    if tracked.is_empty() {
        return Err(
            "SCD3 needs at least one column to track a previous value for (compareColumns)"
                .to_string(),
        );
    }
    let key_eq = keys.iter().map(|k| { let q = quote_ident(k); format!("p.{q} = c.{q}") }).collect::<Vec<_>>().join(" AND ");
    let prev_cols = tracked.iter()
        .map(|t| format!("p.{src} AS {dst}", src = quote_ident(t), dst = quote_ident(&format!("previous_{t}"))))
        .collect::<Vec<_>>().join(", ");
    let eff_select = string_prop(props, "effectiveDateColumn")
        .filter(|s| !s.trim().is_empty())
        .map(|name| format!(", CURRENT_TIMESTAMP AS {}", quote_ident(name.trim())))
        .unwrap_or_default();
    Ok(format!(
        "SELECT c.*, {prev_cols}{eff_select} FROM {cur} c LEFT JOIN {prev} p ON {key_eq}",
        cur = quote_ident(cur), prev = quote_ident(prev)
    ))
}

/// qa.outlier: statistical outlier detection. Inliers pass; outliers route to
/// the reject port (mirrors build_quality). method=iqr (outside [Q1-k*IQR,
/// Q3+k*IQR], k default 1.5) or zscore (abs((x-mean)/stddev) > threshold,
/// default 3). Stats are window aggregates over the whole input; NULLs pass;
/// zero spread -> nothing is an outlier (also avoids /0). reject=true yields the
/// outlier rows.
pub(crate) fn build_outlier(inputs: &NodeInputs, props: &JsonValue, reject: bool) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.outlier"))?;
    let column = string_prop(props, "column").filter(|s| !s.is_empty())
        .ok_or_else(|| "Outlier detection needs a numeric column".to_string())?;
    let method = string_prop(props, "method").unwrap_or_else(|| "iqr".into());
    let col = quote_ident(&column);
    let val = format!("CAST({} AS DOUBLE)", col);
    let default = if method == "zscore" { 3.0 } else { 1.5 };
    let sensitivity = props.get("sensitivity").and_then(|v| v.as_f64()).unwrap_or(default);
    if !(sensitivity > 0.0) {
        return Err("Outlier sensitivity must be greater than 0".into());
    }
    let (helpers, inlier) = match method.as_str() {
        "zscore" => (
            format!("avg({val}) OVER () AS __dq_mean, stddev_pop({val}) OVER () AS __dq_sd"),
            format!("{col} IS NULL OR __dq_sd = 0 OR abs(({val} - __dq_mean) / __dq_sd) <= {sensitivity}"),
        ),
        _ => (
            format!("quantile_cont({val}, 0.25) OVER () AS __dq_q1, quantile_cont({val}, 0.75) OVER () AS __dq_q3"),
            format!("{col} IS NULL OR {val} BETWEEN (__dq_q1 - {sensitivity} * (__dq_q3 - __dq_q1)) AND (__dq_q3 + {sensitivity} * (__dq_q3 - __dq_q1))"),
        ),
    };
    let exclude = if method == "zscore" { "__dq_mean, __dq_sd" } else { "__dq_q1, __dq_q3" };
    let guard = if reject { "NOT COALESCE" } else { "COALESCE" };
    Ok(format!(
        "SELECT * EXCLUDE ({exclude}) FROM (SELECT *, {helpers} FROM {up}) WHERE {guard}(({inlier}), TRUE)",
        up = quote_ident(upstream)
    ))
}

/// xf.sessionize: assign a session id to event rows by inactivity gap. Within
/// each partition, ordered by the timestamp, a new session starts when the gap
/// from the previous event exceeds the threshold; session_id is a cumulative sum
/// of the new-session flag, session_seq the 1-based event index within a session.
pub(crate) fn build_sessionize(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.sessionize"))?;
    let order_col = string_prop(props, "orderBy").filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Sessionize needs an Order By column (the event timestamp)".to_string())?;
    let gap = props.get("gap").and_then(|v| v.as_f64())
        .or_else(|| num_prop(props, "gap").and_then(|s| s.parse::<f64>().ok()))
        .filter(|g| *g > 0.0)
        .ok_or_else(|| "Sessionize needs a positive inactivity gap".to_string())?;
    let unit = string_prop(props, "gapUnit").unwrap_or_else(|| "minutes".into());
    let seconds_per = match unit.to_lowercase().as_str() {
        "second" | "seconds" => 1.0_f64,
        "minute" | "minutes" => 60.0,
        "hour" | "hours" => 3_600.0,
        other => return Err(format!("Sessionize: unknown gap unit '{}' (use seconds | minutes | hours)", other)),
    };
    let gap_seconds = gap * seconds_per;
    let gap_literal = if gap_seconds.fract() == 0.0 { format!("{}", gap_seconds as i64) } else { format!("{}", gap_seconds) };
    let session_col = string_prop(props, "sessionColumn").filter(|s| !s.trim().is_empty()).unwrap_or_else(|| "session_id".into());
    let partition = columns_list(props, "partitionBy");
    let emit_seq = props.get("emitSeq").and_then(JsonValue::as_bool).unwrap_or(true);
    let seq_col = string_prop(props, "seqColumn").filter(|s| !s.trim().is_empty()).unwrap_or_else(|| "session_seq".into());
    let ord = quote_ident(&order_col);
    let part_list = partition.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let part_clause = if partition.is_empty() { String::new() } else { format!("PARTITION BY {} ", part_list) };
    let sid = quote_ident(&session_col);
    let up = quote_ident(upstream);
    let core = format!(
        "WITH __flag AS (\
           SELECT *, \
             CASE WHEN lag({ord}) OVER w IS NULL \
                    OR (epoch(CAST({ord} AS TIMESTAMP)) - epoch(CAST(lag({ord}) OVER w AS TIMESTAMP))) > {gap} \
                  THEN 1 ELSE 0 END AS __new_sess \
           FROM {up} WINDOW w AS ({part}ORDER BY {ord})\
         ), \
         __sid AS (\
           SELECT * EXCLUDE (__new_sess), \
             SUM(__new_sess) OVER ({part}ORDER BY {ord}) AS {sid} FROM __flag\
         )",
        ord = ord, gap = gap_literal, up = up, part = part_clause, sid = sid
    );
    if !emit_seq {
        return Ok(format!("{core} SELECT * FROM __sid"));
    }
    let seq_part = if partition.is_empty() {
        format!("PARTITION BY {sid} ", sid = sid)
    } else {
        format!("PARTITION BY {part}, {sid} ", part = part_list, sid = sid)
    };
    Ok(format!(
        "{core} SELECT *, ROW_NUMBER() OVER ({seq_part}ORDER BY {ord}) AS {seq} FROM __sid",
        seq_part = seq_part, ord = ord, seq = quote_ident(&seq_col)
    ))
}

/// qa.freshness: data freshness / SLA gate. age = now - max(column) vs maxAge
/// (minutes/hours/days). mode=gate passes rows through when fresh, else FAILS
/// THE RUN (MATERIALIZED CTE + error() read in the outer WHERE, like
/// qa.contract). mode=report emits one row (max_timestamp, age, threshold,
/// is_fresh). Empty / all-null input is a vacuous pass.
pub(crate) fn build_freshness(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.freshness"))?;
    let from = quote_ident(upstream);
    let column = string_prop(props, "column").filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Freshness Check needs a timestamp/date column".to_string())?;
    let col = quote_ident(column.trim());
    let max_age = num_prop(props, "maxAge").ok_or_else(|| "Freshness Check needs a maxAge (a number)".to_string())?;
    let unit = string_prop(props, "maxAgeUnit").unwrap_or_else(|| "hours".into());
    let (diff_unit, suffix) = match unit.as_str() {
        "minutes" => ("minute", "minutes"),
        "hours" => ("hour", "hours"),
        "days" => ("day", "days"),
        other => return Err(format!("Freshness Check: unknown maxAgeUnit '{}' (use minutes | hours | days)", other)),
    };
    let age = format!("date_diff('{unit}', MAX(CAST({col} AS TIMESTAMP)), CURRENT_TIMESTAMP)", unit = diff_unit, col = col);
    let mode = string_prop(props, "mode").unwrap_or_else(|| "gate".into());
    match mode.as_str() {
        "gate" => {
            let msg_prefix = sql_escape("Data is stale: ");
            let msg_suffix = sql_escape(&format!(" {} old, threshold {} {}", suffix, max_age, suffix));
            Ok(format!(
                "WITH _duckle_freshness AS MATERIALIZED (\
                   SELECT CASE \
                     WHEN MAX(CAST({col} AS TIMESTAMP)) IS NULL THEN 'ok' \
                     WHEN {age} <= {max_age} THEN 'ok' \
                     ELSE error('{prefix}' || {age} || '{suffix}') \
                   END AS result FROM {from}) \
                 SELECT u.* FROM {from} u WHERE (SELECT result FROM _duckle_freshness) IS NOT NULL",
                col = col, age = age, max_age = max_age, prefix = msg_prefix, suffix = msg_suffix, from = from
            ))
        }
        "report" => Ok(format!(
            "SELECT MAX(CAST({col} AS TIMESTAMP)) AS max_timestamp, {age} AS age_{suffix}, \
                    {max_age} AS threshold_{suffix}, ({age} <= {max_age}) AS is_fresh FROM {from}",
            col = col, age = age, suffix = suffix, max_age = max_age, from = from
        )),
        other => Err(format!("Freshness Check: unknown mode '{}' (use gate | report)", other)),
    }
}

/// qa.contract: a DATA CONTRACT enforcement gate. Holds the same rule suite as
/// qa.expect ({column, check, args}: not_null / unique / in_set / in_range /
/// regex / non_negative), but instead of a scorecard it passes EVERY upstream
/// row through unchanged when all rules hold and FAILS THE RUN with a message
/// naming the violated rule(s) the moment any rule breaks. Built like xf.assert:
/// a MATERIALIZED CTE of per-rule violation counts, gated by error() in the
/// outer WHERE (the only shape DuckDB will not optimize away). Empty input is a
/// vacuous pass.
pub(crate) fn build_contract(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.contract"))?;
    let from = quote_ident(upstream);
    let rules = collect_expect_rules(props)?;
    if rules.is_empty() {
        return Err("Data Contract needs at least one rule (column + check)".to_string());
    }
    let mut count_cols: Vec<String> = Vec::new();
    let mut msg_parts: Vec<String> = Vec::new();
    for (i, (column, check, args)) in rules.iter().enumerate() {
        if column.trim().is_empty() {
            return Err(format!("Contract rule '{}' is missing a column", check));
        }
        let col = quote_ident(column.trim());
        let alias = format!("f{}", i);
        let count_expr = if check == "unique" {
            format!(
                "(SELECT CAST(COUNT(*) FILTER (WHERE NOT __dq_ok) AS BIGINT) \
                 FROM (SELECT (COUNT(*) OVER (PARTITION BY {col}) = 1) AS __dq_ok FROM {from}) __u{i})",
                col = col, from = from, i = i
            )
        } else {
            let pred = expect_predicate(&col, check, args)?;
            format!("CAST(COUNT(*) FILTER (WHERE NOT ({pred})) AS BIGINT)", pred = pred)
        };
        count_cols.push(format!("{expr} AS {alias}", expr = count_expr, alias = alias));
        let label = sql_escape(&expect_label(column.trim(), check, args));
        msg_parts.push(format!(
            "CASE WHEN {alias} > 0 THEN '{label}: ' || {alias} || ' row(s) failed' END",
            alias = alias, label = label
        ));
    }
    let total = (0..rules.len()).map(|i| format!("f{}", i)).collect::<Vec<_>>().join(" + ");
    Ok(format!(
        "WITH _duckle_contract AS MATERIALIZED (SELECT {counts} FROM {from}) \
         SELECT u.* FROM {from} u \
         WHERE (SELECT CASE WHEN ({total}) > 0 \
           THEN error('Data contract violated: ' || concat_ws('; ', {msg})) \
           ELSE 0 END FROM _duckle_contract) IS NOT NULL",
        counts = count_cols.join(", "), from = from, total = total, msg = msg_parts.join(", ")
    ))
}

/// xf.surrogatekey: add a deterministic warehouse dimension key column. `hash`
/// -> md5(concat_ws(sep, CAST key cols)) so the same business key always yields
/// the same surrogate across runs/systems; `sequence` -> row_number() OVER
/// (ORDER BY key cols) as a 1..N integer. Unlike xf.uuid (random per row), this
/// keys off the business columns. Single input, single output.
pub(crate) fn build_surrogate_key(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.surrogatekey"))?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "surrogate_key".into());
    let keys = columns_list(props, "keyColumns");
    if keys.is_empty() {
        return Err("Surrogate Key needs at least one business key column".to_string());
    }
    let mode = string_prop(props, "mode").unwrap_or_else(|| "hash".into());
    let key_expr = match mode.as_str() {
        "hash" => {
            let separator = string_prop(props, "separator").filter(|s| !s.is_empty()).unwrap_or_else(|| "||".into());
            let parts = keys.iter().map(|c| format!("CAST({} AS VARCHAR)", quote_ident(c))).collect::<Vec<_>>().join(", ");
            format!("md5(concat_ws('{}', {}))", sql_escape(&separator), parts)
        }
        "sequence" => {
            let order = keys.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
            format!("row_number() OVER (ORDER BY {})", order)
        }
        other => return Err(format!("Surrogate Key: unknown mode '{}' (use hash | sequence)", other)),
    };
    Ok(format!(
        "SELECT *, {key} AS {out} FROM {up}",
        key = key_expr, out = quote_ident(&output), up = quote_ident(upstream)
    ))
}

pub(crate) fn build_standardize(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.standardize"))?;
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        return Err("Standardize needs at least one column".to_string());
    }
    let case = string_prop(props, "case").unwrap_or_else(|| "none".into());
    let trim = props.get("trim").and_then(|v| v.as_bool()).unwrap_or(true);
    let collapse = props
        .get("collapseWhitespace")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let replacements = cols
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            let mut expr = format!("CAST({} AS VARCHAR)", q);
            expr = match case.as_str() {
                "upper" => format!("UPPER({})", expr),
                "lower" => format!("LOWER({})", expr),
                "title" => format!("INITCAP({})", expr),
                _ => expr,
            };
            if collapse {
                expr = format!("regexp_replace({}, '\\s+', ' ', 'g')", expr);
            }
            if trim {
                expr = format!("TRIM({})", expr);
            }
            format!("{} AS {}", expr, q)
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT * REPLACE ({}) FROM {}",
        replacements,
        quote_ident(upstream)
    ))
}

/// Lowercased comparison key from the chosen columns, for fuzzy
/// matching. Errors if no columns are given.
pub(crate) fn match_key(props: &JsonValue) -> Result<String, String> {
    let cols = columns_list(props, "columns");
    if cols.is_empty() {
        return Err("needs at least one compare column".to_string());
    }
    Ok(format!(
        "lower(concat_ws(' ', {}))",
        cols.iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// A 0..1 similarity score expression over a._key / b._key, plus the
/// configured threshold. Unknown algorithms fall back to Jaro-Winkler.
pub(crate) fn similarity(props: &JsonValue) -> (String, f64) {
    let algo = string_prop(props, "algorithm").unwrap_or_else(|| "jaro-winkler".into());
    let threshold = props
        .get("threshold")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.85);
    let score = match algo.as_str() {
        "levenshtein" => "(1.0 - levenshtein(a._key, b._key)::DOUBLE \
             / GREATEST(length(a._key), length(b._key), 1))"
            .to_string(),
        _ => "jaro_winkler_similarity(a._key, b._key)".to_string(),
    };
    (score, threshold)
}

/// Fuzzy Deduplicate: keep the first row of each near-duplicate cluster,
/// where rows are duplicates when their key similarity meets the
/// threshold.
pub(crate) fn build_fuzzy_dedupe(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.dedupe"))?;
    let key = match_key(props).map_err(|e| format!("Fuzzy Deduplicate {e}"))?;
    let (score, threshold) = similarity(props);
    Ok(format!(
        "WITH ranked AS MATERIALIZED (SELECT *, {key} AS _key, \
         ROW_NUMBER() OVER (ORDER BY {key}) AS _rn FROM {up}) \
         SELECT a.* EXCLUDE (_key, _rn) FROM ranked a \
         WHERE NOT EXISTS (SELECT 1 FROM ranked b \
         WHERE b._rn < a._rn AND {score} >= {threshold})",
        up = quote_ident(upstream)
    ))
}

/// Record Match: self-join the input and emit each pair of rows whose key
/// similarity meets the threshold, with a match score (record linkage
/// within one dataset).
pub(crate) fn build_record_match(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("qa.match"))?;
    let key = match_key(props).map_err(|e| format!("Record Match {e}"))?;
    let (score, threshold) = similarity(props);
    Ok(format!(
        "WITH k AS MATERIALIZED (SELECT *, {key} AS _key, ROW_NUMBER() OVER () AS _rn FROM {up}) \
         SELECT a.* EXCLUDE (_key, _rn), b._key AS matched_key, round({score}, 4) AS match_score \
         FROM k a JOIN k b ON a._rn < b._rn AND {score} >= {threshold}",
        up = quote_ident(upstream)
    ))
}

/// Data-quality validators. `reject = false` yields the passing rows;
/// `reject = true` yields the failing rows for the node's reject port.
pub(crate) fn build_quality(
    inputs: &NodeInputs,
    props: &JsonValue,
    component_id: &str,
    reject: bool,
) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "validator: missing main input".to_string())?;
    let from = quote_ident(upstream);
    if component_id == "qa.unique" {
        let keys = columns_list(props, "columns");
        if keys.is_empty() {
            return Err("Uniqueness check needs key columns".into());
        }
        let partition = keys.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let cmp = if reject { ">" } else { "=" };
        // ROW_NUMBER() with no ORDER BY picks an arbitrary survivor per
        // duplicate group, which is non-deterministic under
        // preserve_insertion_order=false + multi-threading: the same input
        // can keep a different row run-to-run (audit B4). An optional
        // `tieBreak` prop (comma-separated columns) makes the survivor
        // deterministic. We do NOT impose a default ordering - that would
        // change which row currently survives for every existing qa.unique
        // node, and there's no safe all-column default (breaks on
        // LIST/STRUCT/MAP). Per-port row COUNTS are unchanged regardless;
        // the prop only fixes WHICH row of each group is kept.
        let order = columns_list(props, "tieBreak");
        let window = if order.is_empty() {
            format!("ROW_NUMBER() OVER (PARTITION BY {})", partition)
        } else {
            let ob = order.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
            format!("ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {})", partition, ob)
        };
        return Ok(format!(
            "SELECT * EXCLUDE (__dq_rn) FROM (SELECT *, {} AS __dq_rn FROM {}) WHERE __dq_rn {} 1",
            window, from, cmp
        ));
    }
    let predicate = quality_pass_predicate(component_id, props)?;
    if reject {
        // The reject PORT is what feeds a dead-letter branch. It carries the failing
        // rows whatever the setting says, and raising here would break the branch that
        // exists to catch them.
        return Ok(format!(
            "SELECT * FROM {} WHERE NOT COALESCE(({}), FALSE)",
            from, predicate
        ));
    }
    // "On failure" offers reject / warn / fail, and only reject ever happened: the
    // setting was never read, so a gate configured to STOP a load let it through and
    // dropped the offending rows on the way. A run asked to stop and reporting success
    // is the worst of the three, so `fail` now raises where the rows are counted.
    //
    // reject stays exactly what it was, which is also what an unset value does, so
    // nothing saved changes.
    let on_fail = string_prop(props, "onFail")
        .map(|s| s.trim().to_ascii_lowercase())
        .unwrap_or_default();
    // warn is labelled "keep row" in the editor and kept nothing: it took this
    // same filtered path as reject, so the rows it promised to keep were dropped
    // and the run reported success. Measured at three rows in, two out.
    //
    // The failing rows are still on the reject port, which carries them whatever
    // this setting says, so warn is "everything continues down main, and the
    // failures are also available to branch on".
    if on_fail == "warn" {
        return Ok(format!("SELECT * FROM {}", from));
    }
    let stop = on_fail == "fail";
    if stop {
        let msg = format!("{component_id}: a row failed the check and On failure is set to fail");
        return Ok(format!(
            "SELECT * FROM {from} WHERE COALESCE(({predicate}), FALSE) AND CASE WHEN (SELECT count(*) FROM {from} WHERE NOT COALESCE(({predicate}), FALSE)) > 0 THEN error('{}') ELSE TRUE END",
            sql_escape(&msg)
        ));
    }
    Ok(format!(
        "SELECT * FROM {} WHERE COALESCE(({}), FALSE)",
        from, predicate
    ))
}

pub(crate) fn quality_pass_predicate(component_id: &str, props: &JsonValue) -> Result<String, String> {
    match component_id {
        "qa.notnull" | "qa.schemavalidate" => {
            // Schema Validate reuses the not-null predicate against the
            // form's expectedColumns list (the columns the user said the
            // input must have populated). Any row missing a value in any
            // of those columns is rejected.
            let key = if component_id == "qa.schemavalidate" {
                "expectedColumns"
            } else {
                "columns"
            };
            let cols = columns_list(props, key);
            if cols.is_empty() {
                return Ok("TRUE".into());
            }
            Ok(cols
                .iter()
                .map(|c| format!("{} IS NOT NULL", quote_ident(c)))
                .collect::<Vec<_>>()
                .join(" AND "))
        }
        "qa.range" => {
            let col = string_prop(props, "column")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Range check needs a column".to_string())?;
            let c = quote_ident(&col);
            let inclusive = props.get("inclusive").and_then(JsonValue::as_bool).unwrap_or(true);
            let (ge, le) = if inclusive { (">=", "<=") } else { (">", "<") };
            let mut parts = Vec::new();
            if let Some(min) = num_prop(props, "min") {
                parts.push(format!("{} {} {}", c, ge, min));
            }
            if let Some(max) = num_prop(props, "max") {
                parts.push(format!("{} {} {}", c, le, max));
            }
            Ok(if parts.is_empty() { "TRUE".into() } else { parts.join(" AND ") })
        }
        "qa.regex" => {
            let col = string_prop(props, "column")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Regex check needs a column".to_string())?;
            let pat = string_prop(props, "pattern")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Regex check needs a pattern".to_string())?;
            Ok(format!(
                "regexp_full_match(CAST({} AS VARCHAR), '{}')",
                quote_ident(&col),
                sql_escape(&pat)
            ))
        }
        other => Err(format!("Validator '{}' is not yet implemented", other)),
    }
}

/// Reject-port SQL for components that split rows. None = no reject table.
pub(crate) fn build_reject_sql(
    component_id: &str,
    props: &JsonValue,
    inputs: &NodeInputs,
    declared: Option<&[duckle_metadata::Column]>,
) -> Result<Option<String>, String> {
    match component_id {
        // CSV / TSV sources: rows whose raw text fails to parse into a
        // declared column type, kept as raw text for review (issue #15).
        "src.csv" => Ok(build_csv_reject_sql(props, declared, false)),
        "src.tsv" => Ok(build_csv_reject_sql(props, declared, true)),
        // The join family's unmatched LEFT rows. These components declare a
        // `reject` output port and nothing filled it, so wiring the port failed
        // the whole run with `Table with name <node>__reject does not exist` -
        // an internal name, for a port the editor offers.
        //
        // An inner join and a semi join DROP these rows, and the port is wired
        // precisely to find out which ones went. A lookup is a LEFT join, so it
        // emits them too (padded with NULLs); there the reject stream is the
        // diagnostic "which rows found nothing", which is what the form's
        // `sendUnmatchedToReject` promised by name.
        //
        // NOT EXISTS rather than NOT IN, for the reason build_semi gives: one
        // NULL on the right makes NOT IN return UNKNOWN, which would silently
        // reject every row - the last place to reintroduce that gotcha is the
        // stream someone is using to account for missing data.
        // Unmatched features: the same anti-join, with the spatial predicate
        // in place of key equality. xf.anti and xf.join.cross are deliberately
        // absent - an anti join's MAIN output already IS the unmatched rows,
        // and a cross join has no predicate, so neither can have a meaningful
        // reject stream. Their ports are removed rather than filled.
        "xf.join.spatial" => {
            let left = inputs
                .main()
                .ok_or_else(|| "spatial join reject: missing main input".to_string())?;
            let right = inputs
                .first_lookup()
                .ok_or_else(|| "spatial join reject: missing lookup input".to_string())?;
            let left_col = string_prop(props, "leftGeomColumn")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "spatial join reject: leftGeomColumn required".to_string())?;
            let right_col = string_prop(props, "rightGeomColumn")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "spatial join reject: rightGeomColumn required".to_string())?;
            Ok(Some(format!(
                "SELECT * FROM {l} m WHERE NOT EXISTS (SELECT 1 FROM {r} r WHERE {f}(m.{lc}, r.{rc}))",
                l = quote_ident(left),
                r = quote_ident(right),
                f = spatial_relation_fn(props),
                lc = quote_ident(&left_col),
                rc = quote_ident(&right_col),
            )))
        }
        "xf.join" | "xf.join.inner" | "xf.lookup" | "xf.lookup.outer" | "xf.semi"
        | "xf.semi.join" => {
            let left = inputs
                .main()
                .ok_or_else(|| "join reject: missing main input".to_string())?;
            let right = inputs
                .first_lookup()
                .ok_or_else(|| "join reject: missing lookup input".to_string())?;
            let (left_keys, right_keys) = join_key_pairs(props)?;
            let on = left_keys
                .iter()
                .zip(right_keys.iter())
                .map(|(l, r)| format!("m.{} = r.{}", quote_ident(l), quote_ident(r)))
                .collect::<Vec<_>>()
                .join(" AND ");
            Ok(Some(format!(
                "SELECT * FROM {l} m WHERE NOT EXISTS (SELECT 1 FROM {r} r WHERE {on})",
                l = quote_ident(left),
                r = quote_ident(right),
            )))
        }
        "xf.filter" => {
            let upstream = inputs.main().ok_or_else(|| "filter: missing main input".to_string())?;
            let predicate = filter_predicate_sql(props.get("predicate")).unwrap_or_default();
            let predicate = predicate.trim();
            let predicate = if predicate.is_empty() { "TRUE" } else { predicate };
            Ok(Some(format!(
                "SELECT * FROM {} WHERE NOT COALESCE(({}), FALSE)",
                quote_ident(upstream),
                predicate
            )))
        }
        "qa.notnull" | "qa.range" | "qa.regex" | "qa.unique" | "qa.schemavalidate" => {
            Ok(Some(build_quality(inputs, props, component_id, true)?))
        }
        // Orphan rows (main key absent from the reference) go to the reject port.
        "qa.refintegrity" => Ok(Some(build_refintegrity(inputs, props, true)?)),
        // Statistical outliers go to the reject port; inliers pass.
        "qa.outlier" => Ok(Some(build_outlier(inputs, props, true)?)),
        _ => Ok(None),
    }
}

pub(crate) fn columns_list(props: &JsonValue, key: &str) -> Vec<String> {
    match props.get(key) {
        Some(JsonValue::Array(arr)) => arr
            .iter()
            // Drop empty / whitespace-only entries: a blank column name is
            // never valid and would otherwise pass length-based guards (e.g.
            // upsert conflictColumns=[""]) and emit a zero-length quoted
            // identifier. Non-empty names are kept verbatim (a column may
            // legitimately contain surrounding spaces).
            .filter_map(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .map(String::from)
            .collect(),
        // A bare string is accepted as a one-column list, or a comma-separated
        // one. Writing conflictColumns="id" instead of ["id"] is the obvious
        // mistake to make, and it used to yield an empty list silently: an
        // upsert with no keys fell back to plain inserts and duplicated the
        // whole table on every run. Reading it as the caller plainly meant it
        // is better than a rule they have to learn from the damage.
        Some(JsonValue::String(s)) => s
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(String::from)
            .collect(),
        _ => Vec::new(),
    }
}

/// A numeric property as a SQL literal - only if it's actually numeric,
/// so it can't smuggle arbitrary SQL into a comparison.
pub(crate) fn num_prop(props: &JsonValue, key: &str) -> Option<String> {
    match props.get(key) {
        Some(JsonValue::Number(n)) => Some(n.to_string()),
        Some(JsonValue::String(s)) => {
            let t = s.trim();
            t.parse::<f64>().ok().map(|_| t.to_string())
        }
        _ => None,
    }
}

/// xf.pyexpr: derive columns from Python expressions, compiled to SQL.
///
/// Each entry in `columns` is `{ name, expr }` where `expr` is a Python
/// expression over the upstream columns. The expression is translated to
/// DuckDB SQL here, at plan time, so the run itself is ordinary vectorized
/// SQL with no interpreter in the data path. An expression that cannot be
/// translated is rejected by name rather than silently routed through
/// something slower.
pub(crate) fn build_pyexpr(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.pyexpr"))?;
    let columns = props
        .get("columns")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    if columns.is_empty() {
        return Err("Python Expression needs at least one output column".into());
    }
    let mut parts: Vec<String> = Vec::with_capacity(columns.len());
    let mut names: Vec<String> = Vec::with_capacity(columns.len());
    for col in &columns {
        let name = string_prop(col, "name")
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| "Python Expression: every column needs a name".to_string())?;
        let expr = string_prop(col, "expr")
            .or_else(|| string_prop(col, "expression"))
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| format!("Python Expression: column '{}' has no expression", name))?;
        let sql = crate::pyexpr::compile(&expr)
            .map_err(|e| format!("Python Expression for column '{}': {}", name, e))?;
        names.push(name.clone());
        parts.push(format!("{} AS {}", sql, quote_ident(&name)));
    }
    // Deriving a column that already exists REPLACES it, in place. Appending
    // with a plain `SELECT *, expr AS amount` produced two columns called
    // amount, and readers disambiguate that by renaming the second one, so
    // the name the caller asked for kept the OLD value and the computed one
    // arrived as amount_1. A following .where("amount > 100") then filtered
    // on the stale column and returned nothing, with the run reporting ok.
    //
    // COLUMNS(lambda ...) drops the replaced names only if they are present,
    // so a derive that adds new columns still just appends. The padding
    // column keeps that star from resolving to an empty set when the derive
    // replaces every column of its input, which is a bind error.
    //
    // A replaced column moves to the end rather than holding its position,
    // which `* REPLACE (...)` would preserve but which errors when the name
    // is absent, and absence is the ordinary case here. Nothing working
    // regresses: before this, a redefined column produced the wrong value,
    // so no correct pipeline depended on where it sat. Columns that are only
    // added, not redefined, keep the order they always had.
    let excluded = names
        .iter()
        .map(|n| format!("'{}'", n.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT * EXCLUDE ({pad}) FROM (SELECT COLUMNS(lambda c: c NOT IN ({excluded})), {} \
         FROM (SELECT *, TRUE AS {pad} FROM {}))",
        parts.join(", "),
        quote_ident(upstream),
        pad = PYEXPR_PAD,
        excluded = excluded,
    ))
}

/// Padding column for build_pyexpr's inner star. Named so it cannot collide
/// with a real column, and stripped again by the outer projection.
const PYEXPR_PAD: &str = "__duckle_pyexpr_pad";

/// Split one delimited column into several named columns (#226).
///
/// Split already exists but returns a LIST in a single column, which is the
/// wrong shape when the parts are really separate fields - `"31.2131 30.24324"`
/// wants to become `latitude` and `longitude`, not a two-element list.
///
/// Each part is wrapped in `nullif(..., '')`. `split_part` returns an empty
/// string, not NULL, for a part that isn't there, and an empty string is not
/// castable: a row holding only `31.2131` would give `longitude = ''`, and the
/// first Cast or numeric transform downstream would abort the run. Empty parts
/// become NULL so a ragged row is missing data rather than poison.
pub(crate) fn build_text_to_columns(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.tocolumns"))?;
    let column = require_column(props)?;
    let col = quote_ident(&column);

    let delimiter = string_prop(props, "delimiter").unwrap_or_default();
    if delimiter.is_empty() {
        return Err(
            "Text to Columns needs a delimiter (the character the value is split on, e.g. a space or ,)"
                .to_string(),
        );
    }

    // Comma-separated output names, matching the groupNames convention already
    // used by Regex Extract rather than inventing a new repeating field.
    let raw_names = string_prop(props, "outputColumns")
        .or_else(|| string_prop(props, "columns"))
        .unwrap_or_default();
    let names: Vec<String> = raw_names
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if names.is_empty() {
        return Err(format!(
            "Text to Columns needs output column names; list them comma separated, \
             e.g. latitude, longitude (splitting '{}')",
            column
        ));
    }
    // Duplicates would emit two columns of the same name and DuckDB would carry
    // both, so downstream references silently resolve to whichever came first.
    for (i, name) in names.iter().enumerate() {
        if names[..i].iter().any(|earlier| earlier == name) {
            return Err(format!(
                "Text to Columns lists the output column '{}' twice; names must be unique",
                name
            ));
        }
    }

    let parts: Vec<String> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            format!(
                "nullif(split_part(CAST({} AS VARCHAR), '{}', {}), '') AS {}",
                col,
                sql_escape(&delimiter),
                i + 1,
                quote_ident(name)
            )
        })
        .collect();

    // Keeping the source column is the default: dropping it is not recoverable
    // downstream, and the parts can always be dropped instead.
    let drop_source = props
        .get("dropSource")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    if drop_source {
        Ok(format!(
            "SELECT * EXCLUDE ({}), {} FROM {}",
            col,
            parts.join(", "),
            quote_ident(upstream)
        ))
    } else {
        Ok(format!(
            "SELECT *, {} FROM {}",
            parts.join(", "),
            quote_ident(upstream)
        ))
    }
}

pub(crate) fn build_addcol(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let columns = props
        .get("columns")
        .or_else(|| props.get("additions"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    // Optional declared `type`: when the form picks a type for the new
    // column, wrap the expression in a cast so the column actually has that
    // type. Use TRY_CAST by default (mirrors build_cast): a hard CAST aborts
    // the whole run on the first value the expression can't coerce - one bad
    // row killing the pipeline. TRY_CAST nulls the bad cell instead. The
    // onError prop opts into the strict path (onError=='fail').
    let cast_fn = match string_prop(props, "onError").as_deref() {
        Some("fail") => "CAST",
        _ => "TRY_CAST",
    };
    let typed_expr = |expr: &str, ty: Option<&str>| -> String {
        match ty.map(str::trim).filter(|s| !s.is_empty()) {
            Some(t) => format!("{}(({}) AS {})", cast_fn, expr, duckle_type_to_duckdb(t)),
            None => expr.to_string(),
        }
    };
    let mut additions: Vec<String> = Vec::new();
    for col in &columns {
        let name = col.get("name").and_then(JsonValue::as_str).unwrap_or("col");
        let expr = col
            .get("expression")
            .or_else(|| col.get("expr"))
            .and_then(JsonValue::as_str)
            .unwrap_or("NULL");
        let ty = col.get("type").and_then(JsonValue::as_str);
        additions.push(format!("{} AS {}", typed_expr(expr, ty), quote_ident(name)));
    }
    // The Add-Column / Coalesce form is single: { name, type, expression }.
    if additions.is_empty() {
        let name = string_prop(props, "name").filter(|s| !s.trim().is_empty());
        let expr = string_prop(props, "expression")
            .or_else(|| string_prop(props, "expr"))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        match (name, expr) {
            (Some(name), Some(expr)) => {
                let ty = string_prop(props, "type");
                additions.push(format!(
                    "{} AS {}",
                    typed_expr(&expr, ty.as_deref()),
                    quote_ident(&name)
                ));
            }
            // A column name with no expression would otherwise silently
            // produce no column yet still report success (the form leaves the
            // `amount * 1.08` placeholder visible, so users think it's set).
            // Fail loud instead.
            (Some(name), None) => {
                return Err(format!(
                    "Add Column '{}' has no expression; enter one (e.g. amount * 1.08) or remove the node",
                    name
                ));
            }
            _ => {}
        }
    }
    if additions.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    Ok(format!(
        "SELECT *, {} FROM {}",
        additions.join(", "),
        quote_ident(upstream)
    ))
}

/// Cast a string column to date/timestamp with an explicit strptime format
/// (xf.cast per-entry `format`, #10). cast_fn "CAST" -> strptime (fail on a bad
/// value), otherwise try_strptime (NULL on a bad value), mirroring the
/// TRY_CAST/CAST onError contract. `target_lc` is the lowercased target type.
fn cast_with_format(cast_fn: &str, column: &str, target_lc: &str, fmt: &str) -> String {
    let strp = if cast_fn == "CAST" { "strptime" } else { "try_strptime" };
    let to = if target_lc.starts_with("timestamp") { "TIMESTAMP" } else { "DATE" };
    format!(
        "{}({}, '{}')::{} AS {}",
        strp,
        quote_ident(column),
        sql_escape(fmt),
        to,
        quote_ident(column)
    )
}

pub(crate) fn build_cast(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let casts = props
        .get("casts")
        .or_else(|| props.get("columns"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    let provided_casts = !casts.is_empty();
    let mut skipped_empty = 0_usize;
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    // The Cast form's "On conversion error" control:
    //   null (default) -> TRY_CAST, bad values become NULL
    //   reject         -> TRY_CAST too (row-level rejection isn't wired
    //                     for cast yet; NULL-on-error is the safe,
    //                     non-failing approximation)
    //   fail           -> CAST, a bad value aborts the run
    // Previously this prop was ignored and we always emitted CAST, so a
    // default-configured cast of dirty data crashed the pipeline instead
    // of nulling the bad cells like the UI promised.
    let cast_fn = match string_prop(props, "onError").as_deref() {
        Some("fail") => "CAST",
        _ => "TRY_CAST",
    };
    // Use REPLACE so we keep other columns. e.g.
    //   SELECT * REPLACE (TRY_CAST(amount AS DECIMAL(10,2)) AS amount) FROM x
    let mut replacements: Vec<String> = Vec::new();
    for c in &casts {
        let column = c.get("column").and_then(JsonValue::as_str).unwrap_or("").trim();
        let target = c
            .get("targetType")
            .or_else(|| c.get("type"))
            .and_then(JsonValue::as_str)
            .unwrap_or("VARCHAR");
        if column.is_empty() {
            skipped_empty += 1;
            continue;
        }
        if !seen.insert(column.to_string()) {
            // Duplicate cast for the same column - silently letting the
            // later definition win used to surprise users who'd added
            // two casts for the same field by accident. Loud error.
            return Err(format!(
                "Cast: column '{}' appears in two cast entries; remove one",
                column
            ));
        }
        // Per-entry `format` parses a string column with its OWN strptime
        // format (e.g. one column %d/%m/%Y, another %m-%d-%Y) - TRY_CAST only
        // accepts ISO-ish strings, so without this multi-format date columns
        // silently null (#10). Only applies to date/timestamp targets.
        let fmt = c
            .get("format")
            .and_then(JsonValue::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let target_lc = target.to_ascii_lowercase();
        // #144: per-column error handling. An entry may override the node-level
        // onError with its own; unset inherits the node default computed above.
        // fail -> CAST (a bad value aborts and names the column via the SQL),
        // null/reject -> TRY_CAST (bad cells become NULL).
        let cast_fn = match c.get("onError").and_then(JsonValue::as_str) {
            Some("fail") => "CAST",
            Some("null") | Some("reject") => "TRY_CAST",
            _ => cast_fn,
        };
        if let (Some(fmt), true) = (fmt, target_lc == "date" || target_lc.starts_with("timestamp")) {
            replacements.push(cast_with_format(cast_fn, column, &target_lc, fmt));
        } else {
            replacements.push(format!(
                "{}({} AS {}) AS {}",
                cast_fn,
                quote_ident(column),
                duckle_type_to_duckdb(target),
                quote_ident(column)
            ));
        }
    }
    // The Cast form is single-column: { column, targetType, format }.
    if replacements.is_empty() {
        if let Some(column) = string_prop(props, "column").filter(|s| !s.trim().is_empty()) {
            let column = column.trim();
            let target = string_prop(props, "targetType")
                .or_else(|| string_prop(props, "type"))
                .unwrap_or_else(|| "string".into());
            let target_lc = target.to_ascii_lowercase();
            let fmt = string_prop(props, "format").map(|s| s.trim().to_string());
            let fmt = fmt.as_deref().filter(|s| !s.is_empty());
            if let (Some(fmt), true) =
                (fmt, target_lc == "date" || target_lc.starts_with("timestamp"))
            {
                replacements.push(cast_with_format(cast_fn, column, &target_lc, fmt));
            } else {
                replacements.push(format!(
                    "{}({} AS {}) AS {}",
                    cast_fn,
                    quote_ident(column),
                    duckle_type_to_duckdb(&target),
                    quote_ident(column)
                ));
            }
        }
    }
    // If the user supplied cast entries but every one was empty / blank,
    // the SELECT * REPLACE clause would be empty - the cast becomes a
    // silent no-op and the user wonders why their column type didn't
    // change. Catch it loudly here.
    if replacements.is_empty() {
        if provided_casts && skipped_empty > 0 {
            return Err(format!(
                "Cast: {} cast entr{} had no column name - pick a column or remove the row",
                skipped_empty,
                if skipped_empty == 1 { "y" } else { "ies" }
            ));
        }
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    Ok(format!(
        "SELECT * REPLACE ({}) FROM {}",
        replacements.join(", "),
        quote_ident(upstream)
    ))
}

/// All (old, new) rename pairs a Rename node carries, across every prop
/// shape the UI / older docs use: a `renames` or `columns` array of
/// {from|source, to|target}, OR the current Rename form's `mapping`
/// array of {key=old, value=new}. Shared by build_rename, the schema
/// derivation, and validation so they never disagree about which column
/// names exist downstream (a mismatch made the validator reject the new
/// name and accept the renamed-away old one).
pub(crate) fn rename_pairs(props: &JsonValue) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(arr) = props
        .get("renames")
        .or_else(|| props.get("columns"))
        .and_then(JsonValue::as_array)
    {
        for r in arr {
            let from = r.get("from").or_else(|| r.get("source")).and_then(JsonValue::as_str);
            let to = r.get("to").or_else(|| r.get("target")).and_then(JsonValue::as_str);
            if let (Some(f), Some(t)) = (from, to) {
                if !f.is_empty() && !t.is_empty() {
                    out.push((f.to_string(), t.to_string()));
                }
            }
        }
    }
    // The current Rename form writes `mapping` as key-value pairs
    // (old -> new); only consulted when the array shapes are absent,
    // matching build_rename's precedence.
    if out.is_empty() {
        if let Some(arr) = props.get("mapping").and_then(JsonValue::as_array) {
            for kv in arr {
                let old = kv.get("key").and_then(JsonValue::as_str);
                let new = kv.get("value").and_then(JsonValue::as_str);
                if let (Some(o), Some(n)) = (old, new) {
                    if !o.is_empty() && !n.is_empty() {
                        out.push((o.to_string(), n.to_string()));
                    }
                }
            }
        }
    }
    out
}

/// Parse a bulk column-rename mapping file (#82): old -> new pairs from a JSON
/// object {"old":"new"} or array [{from,to}/{source,target}/{old,new}], a CSV
/// (two columns old,new with an optional header), or simple YAML `old: new`
/// lines. Dispatch by file extension; unknown extensions try JSON then CSV.
fn parse_rename_map_file(path: &str) -> Result<Vec<(String, String)>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Rename: could not read mapping file '{}': {}", path, e))?;
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let from_json = |v: &JsonValue| -> Option<Vec<(String, String)>> {
        match v {
            JsonValue::Object(m) => Some(
                m.iter()
                    .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect(),
            ),
            JsonValue::Array(a) => Some(
                a.iter()
                    .filter_map(|e| {
                        let from = e.get("from").or_else(|| e.get("source")).or_else(|| e.get("old")).and_then(JsonValue::as_str);
                        let to = e.get("to").or_else(|| e.get("target")).or_else(|| e.get("new")).and_then(JsonValue::as_str);
                        match (from, to) {
                            (Some(f), Some(t)) => Some((f.to_string(), t.to_string())),
                            _ => None,
                        }
                    })
                    .collect(),
            ),
            _ => None,
        }
    };
    let parse_csv = |text: &str| -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (i, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let cols: Vec<&str> = line.splitn(2, ',').map(|s| s.trim().trim_matches('"')).collect();
            if cols.len() != 2 || cols[0].is_empty() || cols[1].is_empty() {
                continue;
            }
            // Skip a header row like old,new / from,to / source,target.
            if i == 0 && matches!(cols[0].to_ascii_lowercase().as_str(), "old" | "from" | "source")
            {
                continue;
            }
            out.push((cols[0].to_string(), cols[1].to_string()));
        }
        out
    };
    let pairs = match ext.as_str() {
        "json" => serde_json::from_str::<JsonValue>(&content)
            .ok()
            .and_then(|v| from_json(&v))
            .ok_or_else(|| "Rename: JSON mapping must be an object {old:new} or array of {from,to}".to_string())?,
        "csv" => parse_csv(&content),
        "yaml" | "yml" => content
            .lines()
            .filter_map(|l| {
                let l = l.trim();
                if l.is_empty() || l.starts_with('#') {
                    return None;
                }
                let (k, v) = l.split_once(':')?;
                let (k, v) = (k.trim().trim_matches('"'), v.trim().trim_matches('"'));
                if k.is_empty() || v.is_empty() {
                    None
                } else {
                    Some((k.to_string(), v.to_string()))
                }
            })
            .collect(),
        // Unknown extension: try JSON, fall back to CSV.
        _ => serde_json::from_str::<JsonValue>(&content)
            .ok()
            .and_then(|v| from_json(&v))
            .unwrap_or_else(|| parse_csv(&content)),
    };
    if pairs.is_empty() {
        return Err(format!("Rename: mapping file '{}' yielded no old->new pairs", path));
    }
    Ok(pairs)
}

pub(crate) fn build_rename(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let mut pairs = rename_pairs(props);
    // #82: a bulk mapping file (JSON / CSV / YAML) of old -> new names. File
    // entries extend the inline pairs; the first mapping for a column wins.
    if let Some(map_file) = string_prop(props, "mappingFile").filter(|s| !s.trim().is_empty()) {
        let mut seen: std::collections::HashSet<String> =
            pairs.iter().map(|(f, _)| f.clone()).collect();
        for (f, t) in parse_rename_map_file(map_file.trim())? {
            if seen.insert(f.clone()) {
                pairs.push((f, t));
            }
        }
    }
    let mut excludes = Vec::new();
    let mut aliases = Vec::new();
    for (from, to) in &pairs {
        excludes.push(quote_ident(from));
        aliases.push(format!(
            "{}.{} AS {}",
            quote_ident(upstream),
            quote_ident(from),
            quote_ident(to)
        ));
    }
    if aliases.is_empty() {
        return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
    }
    Ok(format!(
        "SELECT {}.* EXCLUDE ({}), {} FROM {}",
        quote_ident(upstream),
        excludes.join(", "),
        aliases.join(", "),
        quote_ident(upstream)
    ))
}

/// A configured lookup join on a Map node.
pub(crate) struct MapLookup {
    port: String,
    view: String,
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    kind: &'static str,
}

pub(crate) fn build_mapper(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "mapper: missing main input".to_string())?;

    // Collect the output (name, raw expression) pairs. The Map form writes
    // either `expressions` (key-value: out name -> SQL) or a structured
    // `mapper.outputs` array ({name, expression}). Both are accepted.
    let mut outputs: Vec<(String, String)> = Vec::new();
    if let Some(pairs) = props.get("expressions").and_then(JsonValue::as_array) {
        for kv in pairs {
            let name = kv.get("key").and_then(JsonValue::as_str).unwrap_or("").trim();
            let expr = kv.get("value").and_then(JsonValue::as_str).unwrap_or("").trim();
            if !name.is_empty() && !expr.is_empty() {
                outputs.push((name.to_string(), expr.to_string()));
            }
        }
    }
    // The other spelling of the same thing: one object from output name to expression.
    // It was described here but never read, and a mapper whose outputs do not parse
    // passes its input straight through - so this form compiled to SELECT *, silently,
    // while the node still reported rows and still looked like it had run. Key order is
    // insertion order, so the outputs keep the order they were written in.
    if let Some(map) = props.get("expressions").and_then(JsonValue::as_object) {
        for (name, expr) in map {
            let expr = expr.as_str().unwrap_or("").trim();
            if !name.trim().is_empty() && !expr.is_empty() {
                outputs.push((name.trim().to_string(), expr.to_string()));
            }
        }
    }
    if outputs.is_empty() {
        if let Some(outs) = props.get("mapper").and_then(|m| m.get("outputs")).and_then(JsonValue::as_array) {
            for o in outs {
                let name = o.get("name").and_then(JsonValue::as_str).unwrap_or("").trim();
                let expr = o
                    .get("expression")
                    .or_else(|| o.get("expr"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or("")
                    .trim();
                if !name.is_empty() && !expr.is_empty() {
                    outputs.push((name.to_string(), expr.to_string()));
                }
            }
        }
    }

    // Optional output filter (WHERE), from either `filter` or `mapper.filter`.
    let filter = string_prop(props, "filter")
        .or_else(|| props.get("mapper").and_then(|m| m.get("filter")).and_then(JsonValue::as_str).map(String::from))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Parse the lookup join config: props.lookups = [{port, leftKey,
    // rightKey, joinType}]. Each port must be wired as an actual input
    // (read by exact handle name - NodeInputs::lookup(idx) does NOT map to
    // lookup_1/2/3, see plan.rs ~1776).
    let mut lookups: Vec<MapLookup> = Vec::new();
    if let Some(arr) = props.get("lookups").and_then(JsonValue::as_array) {
        for entry in arr {
            let port = entry
                .get("port")
                .and_then(JsonValue::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Map: each lookup needs a 'port' (e.g. lookup_1)".to_string())?;
            let view = inputs
                .ports
                .get(port)
                .and_then(|v| v.first())
                .ok_or_else(|| format!(
                    "Map: lookup config references port '{}' but no input is wired into it",
                    port
                ))?
                .clone();
            let left_keys = parse_key_list(
                entry.get("leftKey").and_then(JsonValue::as_str).unwrap_or(""),
            );
            let right_keys = parse_key_list(
                entry.get("rightKey").and_then(JsonValue::as_str).unwrap_or(""),
            );
            if left_keys.is_empty() || right_keys.is_empty() {
                return Err(format!(
                    "Map: lookup '{}' needs leftKey and rightKey",
                    port
                ));
            }
            if left_keys.len() != right_keys.len() {
                return Err(format!(
                    "Map: lookup '{}' leftKey and rightKey must have the same number of columns (got {} vs {})",
                    port, left_keys.len(), right_keys.len()
                ));
            }
            let kind = match entry.get("joinType").and_then(JsonValue::as_str) {
                Some("inner") => "INNER",
                Some("left") | None => "LEFT",
                Some(other) => {
                    return Err(format!(
                        "Map: lookup '{}' joinType must be 'inner' or 'left' (got '{}')",
                        port, other
                    ))
                }
            };
            lookups.push(MapLookup { port: port.to_string(), view, left_keys, right_keys, kind });
        }
    }

    // Validate every lookup port referenced in an expression / filter is
    // either configured above or at least wired - otherwise the generated
    // SQL would reference an unknown relation. This replaces the old blanket
    // "Map can't join" refusal with a precise, actionable error.
    let configured: std::collections::BTreeSet<&str> =
        lookups.iter().map(|l| l.port.as_str()).collect();
    let mut referenced: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (_, expr) in &outputs {
        referenced.extend(referenced_lookup_ports(expr));
    }
    if let Some(f) = &filter {
        referenced.extend(referenced_lookup_ports(f));
    }
    for port in &referenced {
        if !configured.contains(port.as_str()) {
            return Err(format!(
                "Map: an expression references lookup port '{}', but it is not configured in 'lookups' (add a lookup with join keys for it)",
                port
            ));
        }
    }

    // No lookups configured AND nothing references one: behave exactly like
    // the original single-input mapper (strip the `main.` prefix off
    // expressions). Preserves prior behavior + tests.
    if lookups.is_empty() {
        if outputs.is_empty() {
            return Ok(format!("SELECT * FROM {}", quote_ident(upstream)));
        }
        // A mapper's expressions read its input. Where an output is named after a column
        // the mapper also reads, SQL resolves the name to the output beside it instead:
        // the expression sees the value being computed next to it rather than the one
        // that came in, and where the collision runs the other way it refuses to compile
        // at all. The names are applied outside the query that reads the input, so an
        // output can never stand in for the column it is named after.
        if shadows_an_input(&outputs) {
            let inner: Vec<String> = outputs
                .iter()
                .enumerate()
                .map(|(i, (_, expr))| format!("{} AS \"__m{}\"", strip_port_prefixes(expr), i))
                .collect();
            let outer: Vec<String> = outputs
                .iter()
                .enumerate()
                .map(|(i, (name, _))| format!("\"__m{}\" AS {}", i, quote_ident(name)))
                .collect();
            let mut inner_sql =
                format!("SELECT {} FROM {}", inner.join(", "), quote_ident(upstream));
            if let Some(predicate) = &filter {
                inner_sql.push_str(filter_clause(predicate));
                inner_sql.push_str(&strip_port_prefixes(predicate));
            }
            return Ok(format!("SELECT {} FROM ({}) AS \"__map\"", outer.join(", "), inner_sql));
        }
        let terms: Vec<String> = outputs
            .iter()
            .map(|(name, expr)| format!("{} AS {}", strip_port_prefixes(expr), quote_ident(name)))
            .collect();
        let mut sql = format!("SELECT {} FROM {}", terms.join(", "), quote_ident(upstream));
        if let Some(predicate) = &filter {
            sql.push_str(filter_clause(predicate));
            sql.push_str(&strip_port_prefixes(predicate));
        }
        return Ok(sql);
    }

    // Join path. Alias each input by its (unique) view name, quoted.
    // main -> "<upstream>", lookup_1 -> "<view1>", etc.
    let mut aliases: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    aliases.insert("main".to_string(), quote_ident(upstream));
    for l in &lookups {
        aliases.insert(l.port.clone(), quote_ident(&l.view));
    }

    if outputs.is_empty() {
        return Err("Map: define at least one output expression when using lookups".to_string());
    }
    let terms: Vec<String> = outputs
        .iter()
        .map(|(name, expr)| format!("{} AS {}", qualify_port_refs(expr, &aliases), quote_ident(name)))
        .collect();

    // FROM main JOIN lookup_1 ON main.k = lookup_1.k [AND ...] JOIN ...
    // Left keys qualify against main; right keys against the lookup view.
    let main_alias = aliases.get("main").cloned().unwrap_or_else(|| quote_ident(upstream));
    let mut from = quote_ident(upstream);
    for l in &lookups {
        let look_alias = aliases.get(&l.port).cloned().unwrap_or_else(|| quote_ident(&l.view));
        let on = l
            .left_keys
            .iter()
            .zip(l.right_keys.iter())
            .map(|(lk, rk)| {
                // A key is usually a column, and is qualified and quoted as one. It is
                // sometimes an expression - matching on a trimmed value, say - and then
                // it already says which input it reads and is used as written. Taking
                // the column out of it instead would quietly match on something else.
                let side = |k: &str, alias: &str| match is_plain_column(k) {
                    true => format!("{}.{}", alias, quote_ident(k)),
                    false => qualify_port_refs(k, &aliases),
                };
                format!("{} = {}", side(lk, &main_alias), side(rk, &look_alias))
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        from.push_str(&format!(" {} JOIN {} ON {}", l.kind, look_alias, on));
    }

    let mut sql = format!("SELECT {} FROM {}", terms.join(", "), from);
    if let Some(predicate) = &filter {
        sql.push_str(filter_clause(predicate));
        sql.push_str(&qualify_port_refs(predicate, &aliases));
    }
    Ok(sql)
}

/// Whether any output is named after a column the mapper's own expressions read.
///
/// Only then does the naming have to be moved out of the way; leaving every other mapper
/// as it was keeps the SQL it emits, and the tests that read it, unchanged.
fn shadows_an_input(outputs: &[(String, String)]) -> bool {
    let names: std::collections::BTreeSet<&str> =
        outputs.iter().map(|(n, _)| n.as_str()).collect();
    outputs.iter().any(|(_, expr)| {
        let mut in_string = false;
        let mut word = String::new();
        let mut hit = false;
        for c in expr.chars().chain(std::iter::once(' ')) {
            if c == '\'' {
                in_string = !in_string;
            }
            if !in_string && (c.is_alphanumeric() || c == '_') {
                word.push(c);
                continue;
            }
            if !word.is_empty() {
                // A name written as `<table>.<column>` is already unambiguous.
                if c != '.' && names.contains(word.as_str()) {
                    hit = true;
                }
                word.clear();
            }
        }
        hit
    })
}

pub(crate) fn strip_port_prefixes(expr: &str) -> String {
    // Replace `<word>.<word>` where the leading word is a known port
    // alias the mapper used, leaving the column reference untouched.
    let mut out = String::with_capacity(expr.len());
    for token in expr.split_inclusive(|c: char| !c.is_alphanumeric() && c != '_' && c != '.') {
        // For each token, if it looks like main.col / lookup_N.col,
        // drop the prefix.
        let (alpha, rest) = split_leading_token(token);
        if !alpha.is_empty() && (alpha == "main" || alpha.starts_with("lookup")) {
            if let Some(stripped) = rest.strip_prefix('.') {
                out.push_str(stripped);
                continue;
            }
        }
        out.push_str(token);
    }
    out
}

/// Collect the set of `lookup_N` port names an expression references
/// (e.g. `lookup_1.name + lookup_2.code` -> {lookup_1, lookup_2}). Used to
/// validate that every referenced lookup is actually configured/wired.
/// String literals are skipped so `'lookup_9.x'` inside a quoted string is
/// not treated as a reference.
pub(crate) fn referenced_lookup_ports(expr: &str) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    let bytes = expr.as_bytes();
    let mut i = 0;
    let mut in_str = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            if c == '\'' {
                // '' is an escaped quote, stays in the string.
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        if c == '\'' {
            in_str = true;
            i += 1;
            continue;
        }
        // Start of an identifier (not preceded by an identifier char, so we
        // don't match the tail of `my_lookup_1`).
        let prev_ident = i > 0 && {
            let p = bytes[i - 1] as char;
            p.is_alphanumeric() || p == '_'
        };
        if !prev_ident && (c.is_ascii_alphabetic() || c == '_') {
            let start = i;
            // Consume only ASCII identifier bytes so `i` stays on a char
            // boundary - `bytes[i] as char` treats a multibyte lead byte as
            // alphanumeric and would slice mid-char (panic) below.
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let ident = &expr[start..i];
            if ident.starts_with("lookup") && i < bytes.len() && bytes[i] == b'.' {
                out.insert(ident.to_string());
            }
            continue;
        }
        i += 1;
    }
    out
}

/// Rewrite `main.col` / `lookup_N.col` references in an expression to
/// quoted, aliased column references (e.g. `"orders"."id"`), using the
/// alias map (port -> already-quoted view name). String literals are left
/// untouched, so an expression like `'http://main.x'` is not corrupted -
/// this is the key difference from strip_port_prefixes, which is not
/// string-aware and is only safe on the no-lookup single-input path.
/// Read the double-quoted identifier starting at `i` (which must be the opening
/// `"`), honouring `""` as an escaped quote. Returns the index just past the
/// closing quote plus the unescaped content, or None when the quote is never
/// closed - in which case the caller leaves the text alone rather than guessing.
fn read_quoted_ident(expr: &str, i: usize) -> Option<(usize, String)> {
    let rest = &expr[i + 1..];
    let mut content = String::new();
    let mut it = rest.char_indices();
    while let Some((off, ch)) = it.next() {
        if ch == '"' {
            if rest[off + 1..].starts_with('"') {
                content.push('"');
                it.next();
                continue;
            }
            return Some((i + 1 + off + 1, content));
        }
        content.push(ch);
    }
    None
}

pub(crate) fn qualify_port_refs(
    expr: &str,
    aliases: &std::collections::BTreeMap<String, String>,
) -> String {
    let bytes = expr.as_bytes();
    let mut out = String::with_capacity(expr.len() + 16);
    let mut i = 0;
    let mut in_str = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            out.push(c);
            if c == '\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                in_str = false;
            }
            i += 1;
            continue;
        }
        if c == '\'' {
            in_str = true;
            out.push(c);
            i += 1;
            continue;
        }
        // A double-quoted identifier is ONE token. Without this the walker
        // rewrote inside it: `"main.col one"` became `""s1"."col" one"`, and
        // DuckDB rejects the leading `""` as a zero-length delimited identifier
        // (#214). Only the lookup path reached this; the no-lookup path uses
        // strip_port_prefixes, which never re-quotes.
        //
        // Placed after the single-quote handling above so a string literal
        // containing a double quote is still copied through untouched.
        if c == '"' {
            if let Some((end, content)) = read_quoted_ident(expr, i) {
                match content.split_once('.') {
                    // `"main.col one"` is what the mapper UI emitted for a
                    // spaced column. Resolve it the same way the no-lookup path
                    // does, so both paths agree on the same input.
                    Some((port, col)) if !col.is_empty() && aliases.contains_key(port) => {
                        out.push_str(&aliases[port]);
                        out.push('.');
                        out.push_str(&quote_ident(col));
                    }
                    // Every other quoted identifier is copied verbatim.
                    _ => out.push_str(&expr[i..end]),
                }
                i = end;
                continue;
            }
            out.push(c);
            i += 1;
            continue;
        }
        let prev_ident = i > 0 && {
            let p = bytes[i - 1] as char;
            p.is_alphanumeric() || p == '_'
        };
        if !prev_ident && (c.is_ascii_alphabetic() || c == '_') {
            let start = i;
            // Consume only ASCII identifier bytes so `i` stays on a char
            // boundary - `bytes[i] as char` treats a multibyte lead byte as
            // alphanumeric and would slice mid-char (panic) below.
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let ident = &expr[start..i];
            // A `<port>.<col>` reference: rewrite to alias + quoted column.
            if i < bytes.len() && bytes[i] == b'.' {
                if let Some(alias) = aliases.get(ident) {
                    // `main."col one"`: the user already delimited the column,
                    // so copy their quoting verbatim rather than re-quoting it.
                    // Previously the ASCII scan below found no identifier byte
                    // after the dot, fell through, and left `main."col one"`
                    // unqualified, so DuckDB reported an unknown table `main`.
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        if let Some((end, _)) = read_quoted_ident(expr, i + 1) {
                            out.push_str(alias);
                            out.push('.');
                            out.push_str(&expr[i + 1..end]);
                            i = end;
                            continue;
                        }
                    }
                    // Consume the dot + the following column identifier.
                    let mut j = i + 1;
                    let col_start = j;
                    // ASCII-only so `&expr[col_start..j]` stays char-safe.
                    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                        j += 1;
                    }
                    if j > col_start {
                        let col = &expr[col_start..j];
                        out.push_str(alias);
                        out.push('.');
                        out.push_str(&quote_ident(col));
                        i = j;
                        continue;
                    }
                }
            }
            out.push_str(ident);
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

pub(crate) fn split_leading_token(s: &str) -> (&str, &str) {
    let mut end = 0;
    for (i, c) in s.char_indices() {
        if c.is_alphanumeric() || c == '_' {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    (&s[..end], &s[end..])
}

/// Parse a key string into a list of column names. Accepts a single
/// column (`"id"`) or comma-separated composite keys (`"customer_id,
/// order_date"`). Whitespace around commas is stripped.
/// Whether a join key is a plain column name rather than something computed.
pub(crate) fn is_plain_column(k: &str) -> bool {
    let t = k.trim();
    !t.is_empty() && t.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Where a mapper's filter belongs: `WHERE`, or `QUALIFY` when it ranks rows.
///
/// A legacy job bounds a loop by asking for a running sequence number and keeping the
/// rows below a bound. That reads as a window function, and SQL does not allow one in
/// `WHERE` at all - the step fails to bind and the branch is lost, for a filter that was
/// translated correctly and then put in the wrong clause. `QUALIFY` is the clause that
/// filters on a window result.
fn filter_clause(predicate: &str) -> &'static str {
    match mentions_window_function(predicate) {
        true => " QUALIFY ",
        false => " WHERE ",
    }
}

/// Whether a predicate calls a window function: the word `OVER` followed by its bracket,
/// outside any string literal. A predicate comparing against text that merely contains
/// the word is not one.
fn mentions_window_function(predicate: &str) -> bool {
    let bytes = predicate.as_bytes();
    let mut in_string = false;
    let mut i = 0usize;
    while i < predicate.len() {
        if !predicate.is_char_boundary(i) {
            i += 1;
            continue;
        }
        let rest = &predicate[i..];
        if rest.starts_with('\'') && !(i > 0 && bytes[i - 1] == b'\\') {
            in_string = !in_string;
            i += 1;
            continue;
        }
        if !in_string && rest.len() >= 4 && rest[..4].eq_ignore_ascii_case("over") {
            let before_ok = i == 0 || !(bytes[i - 1] as char).is_alphanumeric() && bytes[i - 1] != b'_';
            let after = rest[4..].trim_start();
            if before_ok && after.starts_with('(') {
                return true;
            }
        }
        i += 1;
    }
    false
}

pub(crate) fn parse_key_list(raw: &str) -> Vec<String> {
    // Only a comma that separates keys splits the list. A key can be an expression
    // rather than a bare column, and an expression carries commas of its own between
    // its arguments and inside its text - counted as separators, a single key becomes
    // several and the join is refused for a count that was never wrong.
    let (mut depth, mut in_string, mut start) = (0i32, false, 0usize);
    let mut out: Vec<String> = Vec::new();
    let bytes = raw.as_bytes();
    for (i, c) in raw.char_indices() {
        match c {
            '\'' if !(i > 0 && bytes[i - 1] == b'\\') => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth -= 1,
            ',' if depth == 0 && !in_string => {
                out.push(raw[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(raw[start..].trim().to_string());
    out.retain(|s| !s.is_empty());
    out
}

/// Collect join key pairs from either the legacy `leftKey`/`rightKey` text
/// fields (single or comma-separated composite) OR the UI's `multipleKeys`
/// table (`[{key: left, value: right}, ...]`), merged and deduped. The
/// `multipleKeys` table was previously written by the frontend but never read
/// here, so multi-column joins silently collapsed to a single key and produced
/// cross-paired rows (issue #152). A blank right side defaults to the left
/// column name so same-named keys take the clean `USING(...)` path.
pub(crate) fn join_key_pairs(props: &JsonValue) -> Result<(Vec<String>, Vec<String>), String> {
    let mut lefts = parse_key_list(props.get("leftKey").and_then(JsonValue::as_str).unwrap_or(""));
    let mut rights =
        parse_key_list(props.get("rightKey").and_then(JsonValue::as_str).unwrap_or(""));
    if lefts.len() != rights.len() {
        return Err(format!(
            "join: leftKey and rightKey must have the same number of columns (got {} vs {})",
            lefts.len(),
            rights.len()
        ));
    }
    if let Some(arr) = props.get("multipleKeys").and_then(JsonValue::as_array) {
        for entry in arr {
            let l = entry.get("key").and_then(JsonValue::as_str).unwrap_or("").trim();
            if l.is_empty() {
                continue;
            }
            let r = entry.get("value").and_then(JsonValue::as_str).unwrap_or("").trim();
            let r = if r.is_empty() { l } else { r };
            // Skip a pair already contributed by leftKey/rightKey or an
            // earlier table row so USING/ON does not list a column twice.
            if lefts.iter().zip(rights.iter()).any(|(a, b)| a == l && b == r) {
                continue;
            }
            lefts.push(l.to_string());
            rights.push(r.to_string());
        }
    }
    if lefts.is_empty() {
        return Err("join: a join key is required (set Left/Right key or the multi-column key table)".into());
    }
    Ok((lefts, rights))
}

pub(crate) fn build_join(inputs: &NodeInputs, props: &JsonValue, kind: &str) -> Result<String, String> {
    let left = inputs.main().ok_or_else(|| "join: missing main input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "join: missing lookup input".to_string())?;
    let (left_keys, right_keys) = join_key_pairs(props)?;
    // The form's joinType, if set, overrides the component-id default so
    // changing it in the UI actually takes effect.
    let kind = match string_prop(props, "joinType").as_deref() {
        Some("inner") => "INNER",
        Some("left") => "LEFT",
        Some("right") => "RIGHT",
        Some("full") | Some("outer") => "FULL OUTER",
        _ => kind,
    };
    // Two-shaped output:
    // - If the keys have the same names on both sides (common with
    //   well-modeled data), USING(...) gives a clean single copy of
    //   the join columns - no "ambiguous reference" downstream.
    // - If the names differ, ON + EXCLUDE the right-side keys still
    //   dedupes the join columns. Other shared columns (e.g., both
    //   tables have `created_at`) still need the user to project
    //   them via xf.rename or xf.project upstream, but at minimum
    //   the join keys themselves no longer collide.
    let same_names = left_keys == right_keys;
    if same_names {
        let key_list = left_keys
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "SELECT * FROM {l} m {k} JOIN {r} r USING ({keys})",
            l = quote_ident(left),
            k = kind,
            r = quote_ident(right),
            keys = key_list
        ))
    } else {
        let on_clause = left_keys
            .iter()
            .zip(right_keys.iter())
            .map(|(l, r)| format!("m.{} = r.{}", quote_ident(l), quote_ident(r)))
            .collect::<Vec<_>>()
            .join(" AND ");
        // Project each key as COALESCE(left, right) under the left key
        // name, and EXCLUDE the key columns from BOTH sides. The previous
        // `m.*, r.* EXCLUDE(right_keys)` kept the LEFT key column and
        // dropped the right one - fine for INNER/LEFT, but for RIGHT/FULL
        // a right-only row has m.* all NULL, so the join key showed up as
        // NULL even though the right side had a value (data corruption +
        // the key effectively lost). COALESCE recovers the key value from
        // whichever side is present.
        let coalesced = left_keys
            .iter()
            .zip(right_keys.iter())
            .map(|(l, r)| {
                format!(
                    "COALESCE(m.{}, r.{}) AS {}",
                    quote_ident(l),
                    quote_ident(r),
                    quote_ident(l)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let left_excl = left_keys
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        let right_excl = right_keys
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "SELECT {coalesced}, m.* EXCLUDE ({lexcl}), r.* EXCLUDE ({rexcl}) FROM {l} m {k} JOIN {r} r ON {on}",
            coalesced = coalesced,
            lexcl = left_excl,
            rexcl = right_excl,
            l = quote_ident(left),
            k = kind,
            r = quote_ident(right),
            on = on_clause
        ))
    }
}

pub(crate) fn build_semi(inputs: &NodeInputs, props: &JsonValue, anti: bool) -> Result<String, String> {
    let left = inputs.main().ok_or_else(|| "semi: missing main input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "semi: missing lookup input".to_string())?;
    let left_keys = parse_key_list(
        props
            .get("leftKey")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "semi: leftKey required".to_string())?,
    );
    let right_keys = parse_key_list(
        props
            .get("rightKey")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "semi: rightKey required".to_string())?,
    );
    if left_keys.is_empty() || right_keys.is_empty() {
        return Err("semi: keys cannot be empty".into());
    }
    if left_keys.len() != right_keys.len() {
        return Err(format!(
            "semi: leftKey and rightKey must have the same number of columns (got {} vs {})",
            left_keys.len(),
            right_keys.len()
        ));
    }
    // EXISTS / NOT EXISTS replaces IN / NOT IN to fix the classic SQL
    // NULL gotcha: `x NOT IN (subquery)` returns UNKNOWN (treated as
    // false) the moment the subquery yields a single NULL, which makes
    // anti-join silently drop every row. EXISTS evaluates the subquery
    // as a correlated boolean - NULL right-side keys simply don't
    // match and don't break the predicate. Composite keys ride the
    // same construction.
    let prefix = if anti { "NOT " } else { "" };
    let correlated = left_keys
        .iter()
        .zip(right_keys.iter())
        .map(|(l, r)| format!("m.{} = r.{}", quote_ident(l), quote_ident(r)))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(format!(
        "SELECT * FROM {l} m WHERE {pre}EXISTS (SELECT 1 FROM {r} r WHERE {on})",
        l = quote_ident(left),
        pre = prefix,
        r = quote_ident(right),
        on = correlated
    ))
}

// ---- Sources ------------------------------------------------------------

/// The read_csv_auto arguments common to the main read and the reject read:
/// path + header / delimiter / quote / null-sentinel / skip / encoding.
/// The typed bits (`dateformat`, `types`, `all_varchar`) are appended by the
/// caller, since the main read types columns and the reject read keeps them
/// raw text.
fn csv_read_args_base(props: &JsonValue) -> Vec<String> {
    let path = string_prop(props, "path").unwrap_or_default();
    let has_header = props
        .get("hasHeader")
        .and_then(JsonValue::as_bool)
        .unwrap_or(true);
    let delim = string_prop(props, "delimiter");
    let quote = string_prop(props, "quoteChar");
    let null_val = string_prop(props, "nullValue");
    let mut args = vec![format!("'{}'", sql_escape(&path))];
    args.push(format!("header={}", has_header));
    if let Some(d) = delim.as_deref().filter(|s| !s.is_empty()) {
        args.push(format!("delim='{}'", sql_escape(d)));
    }
    // Told nothing, the reader keeps its own default, which is the double quote. An
    // empty value is not nothing: it is the "None" choice, saying the file has no
    // quoting at all, and that has to be said out loud - left unsaid, a file whose
    // lines hold a lone quote is read from there to the next one as a single field.
    match quote.as_deref() {
        Some("") => args.push("quote=''".to_string()),
        Some(q) => args.push(format!("quote='{}'", sql_escape(q))),
        None => {}
    }
    if let Some(n) = null_val.as_deref().filter(|s| !s.is_empty()) {
        args.push(format!("nullstr='{}'", sql_escape(n)));
    }
    if let Some(skip) = props.get("skipLines").and_then(JsonValue::as_u64) {
        if skip > 0 {
            args.push(format!("skip={}", skip));
        }
    }
    if let Some(enc) = string_prop(props, "encoding").filter(|s| !s.is_empty()) {
        // DuckDB's CSV reader rejects the spelling "windows-1252" (it expects
        // "CP1252") and aborts the read. The UI/docs offer "Windows-1252", so
        // remap it to the accepted spelling; everything else passes through.
        let enc = match enc.to_ascii_lowercase().as_str() {
            "windows-1252" | "windows1252" | "cp1252" => "CP1252".to_string(),
            _ => enc,
        };
        args.push(format!("encoding='{}'", sql_escape(&enc)));
    }
    // #98: surface common parse-leniency options as first-class toggles so users
    // don't have to discover the readOptions passthrough. `ignore_errors` skips
    // rows DuckDB can't parse (bad encoding, wrong column count, trailing blank
    // lines); `null_padding` pads short rows with NULL instead of erroring.
    if props.get("ignoreErrors").and_then(JsonValue::as_bool).unwrap_or(false) {
        args.push("ignore_errors=true".to_string());
    }
    if props.get("nullPadding").and_then(JsonValue::as_bool).unwrap_or(false) {
        args.push("null_padding=true".to_string());
    }
    // #83: expose DuckDB's read_csv options that the Basic tab doesn't surface.
    // `filename=true` adds a `filename` column (extract data from the path when
    // globbing a folder); `readOptions` is a passthrough key-value list of any
    // other read_csv argument (e.g. union_by_name=true, sample_size=-1) written
    // verbatim, for power users who want full control of the reader.
    if props.get("filename").and_then(JsonValue::as_bool).unwrap_or(false) {
        args.push("filename=true".to_string());
    }
    for (k, v) in kv_pairs(props, "readOptions") {
        let (k, v) = (k.trim(), v.trim());
        if !k.is_empty() && !v.is_empty() {
            args.push(format!("{}={}", k, v));
        }
    }
    args
}

/// Best-effort read of a local CSV file's HEADER column names, used only to
/// reconcile a declared schema against the actual file (#133). Returns Some
/// ONLY when the header can be read confidently: a local, existing, non-glob
/// `path`, `hasHeader` not false, and no non-UTF-8 `encoding` declared. Any
/// other case (glob, missing file, headerless, custom encoding, read error)
/// returns None, which keeps build_csv_source's original behavior verbatim so
/// issue #3 and the SQL-export / MCP-validate paths are never weakened.
fn csv_header_names(props: &JsonValue) -> Option<std::collections::HashSet<String>> {
    let path = string_prop(props, "path").filter(|s| !s.is_empty())?;
    // A glob reads many files; a single header is not authoritative.
    if path.contains('*') || path.contains('?') || path.contains('[') {
        return None;
    }
    // Headerless files expose no names to reconcile against.
    if !props.get("hasHeader").and_then(JsonValue::as_bool).unwrap_or(true) {
        return None;
    }
    // A non-UTF-8 encoding means our byte-level split may misread the names.
    if let Some(enc) = string_prop(props, "encoding").filter(|s| !s.is_empty()) {
        if !matches!(enc.to_ascii_lowercase().as_str(), "utf-8" | "utf8") {
            return None;
        }
    }
    let content = std::fs::read(&path).ok()?;
    // Strip a leading UTF-8 BOM, then decode lossily (headers are ASCII in
    // practice; lossy keeps us from failing on a stray non-UTF-8 byte).
    let bytes = content.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(&content[..]);
    let text = String::from_utf8_lossy(bytes);
    let skip = props.get("skipLines").and_then(JsonValue::as_u64).unwrap_or(0) as usize;
    let header_line = text.lines().nth(skip)?;
    let delim = string_prop(props, "delimiter")
        .filter(|s| !s.is_empty())
        .and_then(|s| s.chars().next())
        .unwrap_or(',');
    let quote = string_prop(props, "quoteChar")
        .filter(|s| !s.is_empty())
        .and_then(|s| s.chars().next())
        .unwrap_or('"');
    let names: std::collections::HashSet<String> = header_line
        .split(delim)
        .map(|f| f.trim().trim_matches(quote).trim().to_string())
        .filter(|f| !f.is_empty())
        .collect();
    if names.is_empty() {
        None
    } else {
        Some(names)
    }
}

pub(crate) fn build_csv_source(props: &JsonValue, declared: Option<&[duckle_metadata::Column]>) -> String {
    let mut args = csv_read_args_base(props);
    // Explicit date / timestamp parsing format. DuckDB's strptime tokens
    // (%d, %m, %Y, etc.) - the most common pain point is dd/mm/yyyy which
    // DuckDB otherwise mis-detects as mm/dd/yyyy. Setting this keeps the
    // column as a proper DATE / TIMESTAMP instead of forcing VARCHAR via
    // the Schema panel (which is the other workaround we added for #3).
    if let Some(df) = string_prop(props, "dateFormat").filter(|s| !s.is_empty()) {
        args.push(format!("dateformat='{}'", sql_escape(&df)));
    }
    if let Some(tf) = string_prop(props, "timestampFormat").filter(|s| !s.is_empty()) {
        args.push(format!("timestampformat='{}'", sql_escape(&tf)));
    }
    // If the user declared a schema (Schema panel in PropertiesPanel),
    // honor it via DuckDB's `types` argument, which overrides the inferred
    // type for the NAMED columns and auto-detects the rest. This is how a
    // user forces a `dd/mm/yy` date column to stay as VARCHAR instead of
    // being misparsed as `yyyy-mm-dd`. See issue #3.
    //
    // `types` (name-match), NOT `columns` (positional full-schema):
    // `columns` requires the declaration to list EVERY column in the file,
    // so a PARTIAL Schema-panel declaration (the common case - declare only
    // the few columns you care about) hard-failed with a cryptic sniffer
    // "Schema mismatch ... expected N columns" error. `types` accepts a
    // partial map, binds by NAME, and errors only when a declared name is
    // genuinely absent from the file (the correct, loud failure).
    // DuckDB 1.5.3 verified: types={'amt':'VARCHAR'} over a 3-col CSV keeps
    // id=BIGINT (auto) + amt=VARCHAR (forced); a bogus name errors clearly.
    //
    // Per-column multi-format workaround (issue #10): DuckDB has only a
    // single global `dateformat`/`timestampformat`, so to parse several
    // DATE/TIMESTAMP columns each with its OWN format on one read, force
    // those columns to VARCHAR in `types=` (raw text) and re-parse each via
    // try_strptime in a `SELECT * REPLACE (...)` wrap. try_strptime yields
    // NULL (not an error) on a value the format can't parse.
    if let Some(cols) = declared.filter(|c| !c.is_empty()) {
        use duckle_metadata::DataType;
        // #133: reconcile the declaration against the file's real header so a
        // stale/seeded schema whose columns aren't in the file doesn't trip
        // read_csv_auto's COLUMN_TYPES binder error. None = header not
        // confidently readable -> keep every declared column (issue #3 intact).
        let header = csv_header_names(props);
        let mut pairs = Vec::with_capacity(cols.len());
        let mut replaces = Vec::new();
        for c in cols {
            if let Some(h) = header.as_ref() {
                if !h.contains(c.name.as_str()) {
                    continue;
                }
            }
            let fmt = c.format.as_deref().filter(|s| !s.is_empty());
            let datey = matches!(c.data_type, DataType::Date | DataType::Timestamp);
            match (fmt, datey) {
                (Some(fmt), true) => {
                    // Read raw, re-parse with the column's own format.
                    pairs.push(format!("'{}': 'VARCHAR'", sql_escape(&c.name)));
                    let ident = quote_ident(&c.name);
                    let cast = match c.data_type {
                        DataType::Date => "DATE",
                        _ => "TIMESTAMP",
                    };
                    replaces.push(format!(
                        "try_strptime({id}, '{f}')::{cast} AS {id}",
                        id = ident,
                        f = sql_escape(fmt),
                        cast = cast
                    ));
                }
                _ => pairs.push(format!(
                    "'{}': '{}'",
                    sql_escape(&c.name),
                    data_type_to_duckdb_sql(&c.data_type)
                )),
            }
        }
        if pairs.is_empty() {
            // Every declared column was absent from the file (stale seeded
            // schema): skip `types=` and auto-detect the whole file (#133),
            // which is exactly what the reporter expects.
            return format!("SELECT * FROM read_csv_auto({})", args.join(", "));
        }
        // `types=` renames nothing: it maps types onto names the file already
        // has. A headerless file has none, so the declared names were dropped
        // and the relation exposed DuckDB's positional column00..columnNN,
        // breaking every downstream expression. With `ignoreErrors` on it did
        // not even raise - the read just quietly produced the wrong names.
        // `columns=` supplies name AND type together, which is what a
        // headerless declared schema means, and it needs the sniffer off.
        let headerless = !props
            .get("hasHeader")
            .and_then(JsonValue::as_bool)
            .unwrap_or(true);
        if headerless {
            args.push(format!("columns = {{{}}}", pairs.join(", ")));
            args.push("auto_detect=false".to_string());
        } else {
            args.push(format!("types = {{{}}}", pairs.join(", ")));
        }
        if !replaces.is_empty() {
            return format!(
                "SELECT * REPLACE ({}) FROM read_csv_auto({})",
                replaces.join(", "),
                args.join(", ")
            );
        }
    }
    format!("SELECT * FROM read_csv_auto({})", args.join(", "))
}

/// Map Duckle's DataType enum to a DuckDB SQL type string suitable for
/// read_csv_auto's `columns = {...}` argument. "string" -> VARCHAR is
/// the key one here: it stops DuckDB from trying (and usually failing)
/// to auto-parse dd/mm/yy and other non-ISO date formats.
pub(crate) fn data_type_to_duckdb_sql(t: &duckle_metadata::DataType) -> &'static str {
    use duckle_metadata::DataType as D;
    match t {
        D::String => "VARCHAR",
        D::Int32 => "INTEGER",
        D::Int64 => "BIGINT",
        D::Float32 => "FLOAT",
        D::Float64 => "DOUBLE",
        D::Bool => "BOOLEAN",
        D::Date => "DATE",
        D::Timestamp => "TIMESTAMP",
        D::Time => "TIME",
        D::Decimal => "DECIMAL",
        D::Json => "JSON",
        D::Binary => "BLOB",
        // A column declared geometry casts to native GEOMETRY so the typed-cast
        // read paths (CSV/Excel all_varchar + cast wrapper) keep it spatial
        // instead of coercing to VARCHAR (issue #151). Relies on the existing
        // spatial extension auto-load (#82/#83).
        D::Geometry => "GEOMETRY",
    }
}

pub(crate) fn build_tsv_source(props: &JsonValue, declared: Option<&[duckle_metadata::Column]>) -> String {
    // TSV is just CSV with delim='\t'. Force it.
    let mut p = props.clone();
    if let Some(obj) = p.as_object_mut() {
        obj.insert(
            "delimiter".into(),
            JsonValue::String("\t".into()),
        );
    }
    build_csv_source(&p, declared)
}

/// For a declared CSV column that is NOT text, derive the two SQL fragments the
/// reject feature (issue #15) needs, reading the column as raw VARCHAR:
///   - a parse-FAIL predicate: the value is present but unparseable into the
///     declared type (a genuine empty / null-sentinel is NOT a failure), and
///   - a cast expression for `SELECT * REPLACE (...)` that turns the raw text
///     back into the declared type (NULL on a bad value).
/// Returns None for text columns (they can never fail to parse).
fn csv_typed_col_exprs(c: &duckle_metadata::Column) -> Option<(String, String)> {
    use duckle_metadata::DataType;
    let ty = data_type_to_duckdb_sql(&c.data_type);
    if ty == "VARCHAR" {
        return None;
    }
    let id = quote_ident(&c.name);
    let fmt = c.format.as_deref().filter(|s| !s.is_empty());
    let datey = matches!(c.data_type, DataType::Date | DataType::Timestamp);
    let (parse_expr, cast_expr) = match (fmt, datey) {
        (Some(fmt), true) => {
            let cast = if matches!(c.data_type, DataType::Date) { "DATE" } else { "TIMESTAMP" };
            (
                format!("try_strptime({id}, '{f}')", id = id, f = sql_escape(fmt)),
                format!("try_strptime({id}, '{f}')::{c} AS {id}", id = id, f = sql_escape(fmt), c = cast),
            )
        }
        _ => (
            format!("try_cast({id} AS {ty})", id = id, ty = ty),
            format!("try_cast({id} AS {ty}) AS {id}", id = id, ty = ty),
        ),
    };
    let fail = format!(
        "({id} IS NOT NULL AND {id} <> '' AND {p} IS NULL)",
        id = id,
        p = parse_expr
    );
    Some((fail, cast_expr))
}

/// read_csv_auto args for the reject / split path: base args + force every
/// declared column to raw VARCHAR so a bad value never aborts the read. TSV
/// forces a tab delimiter, matching build_tsv_source.
fn csv_raw_args(props: &JsonValue, declared: &[duckle_metadata::Column], is_tsv: bool) -> Vec<String> {
    let owned;
    let p: &JsonValue = if is_tsv {
        let mut c = props.clone();
        if let Some(obj) = c.as_object_mut() {
            obj.insert("delimiter".into(), JsonValue::String("\t".into()));
        }
        owned = c;
        &owned
    } else {
        props
    };
    let mut args = csv_read_args_base(p);
    let pairs: Vec<String> = declared
        .iter()
        .map(|c| format!("'{}': 'VARCHAR'", sql_escape(&c.name)))
        .collect();
    args.push(format!("types = {{{}}}", pairs.join(", ")));
    args
}

/// Reject relation for src.csv / src.tsv (issue #15): rows whose raw text
/// cannot be parsed into a declared column type (e.g. an invalid date), emitted
/// as raw text so they can be written straight to a CSV for review without
/// re-triggering the parse that rejected them. Returns None when nothing could
/// be rejected (no declared schema, or every declared column is text), so the
/// planner skips materializing an always-empty reject relation.
pub(crate) fn build_csv_reject_sql(
    props: &JsonValue,
    declared: Option<&[duckle_metadata::Column]>,
    is_tsv: bool,
) -> Option<String> {
    let cols = declared.filter(|c| !c.is_empty())?;
    let fails: Vec<String> = cols.iter().filter_map(|c| csv_typed_col_exprs(c).map(|(f, _)| f)).collect();
    if fails.is_empty() {
        return None;
    }
    let args = csv_raw_args(props, cols, is_tsv);
    Some(format!(
        "SELECT * FROM read_csv_auto({}) WHERE {}",
        args.join(", "),
        fails.join(" OR ")
    ))
}

/// Main body for a CSV / TSV source whose reject port IS wired: read declared
/// columns as raw text, cast them back to their declared types, and keep only
/// the rows that parse cleanly. The rejected rows go to the complementary
/// `build_csv_reject_sql`. Falls back to the normal `build_csv_source` when the
/// declared schema has no typed columns (nothing to split on), so the SQL is
/// identical to today in that case.
pub(crate) fn build_csv_source_split(
    props: &JsonValue,
    declared: Option<&[duckle_metadata::Column]>,
    is_tsv: bool,
) -> String {
    let cols = match declared.filter(|c| !c.is_empty()) {
        Some(c) => c,
        None => return csv_source_for(props, declared, is_tsv),
    };
    let typed: Vec<(String, String)> = cols.iter().filter_map(csv_typed_col_exprs).collect();
    if typed.is_empty() {
        return csv_source_for(props, declared, is_tsv);
    }
    let args = csv_raw_args(props, cols, is_tsv);
    let replaces: Vec<&str> = typed.iter().map(|(_, c)| c.as_str()).collect();
    let fails: Vec<&str> = typed.iter().map(|(f, _)| f.as_str()).collect();
    format!(
        "SELECT * REPLACE ({}) FROM read_csv_auto({}) WHERE NOT ({})",
        replaces.join(", "),
        args.join(", "),
        fails.join(" OR ")
    )
}

/// Dispatch to build_csv_source / build_tsv_source by the TSV flag.
fn csv_source_for(props: &JsonValue, declared: Option<&[duckle_metadata::Column]>, is_tsv: bool) -> String {
    if is_tsv {
        build_tsv_source(props, declared)
    } else {
        build_csv_source(props, declared)
    }
}

pub(crate) fn build_parquet_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    // Optional projection: comma-separated column list pushed into the read.
    let select = string_prop(props, "columns")
        .filter(|s| !s.trim().is_empty())
        .map(|c| {
            c.split(',')
                .map(|s| quote_ident(s.trim()))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|| "*".into());
    format!("SELECT {} FROM read_parquet('{}')", select, sql_escape(&path))
}

/// Extra read_json args from user props: an explicit format (the UI's Format
/// dropdown, which the reader previously ignored) and `ignore_errors` to skip
/// malformed records instead of aborting the whole load (#101). DuckDB 1.5.4
/// accepts ignore_errors for every JSON format.
fn json_read_extra_args(props: &JsonValue) -> String {
    let mut extra = String::new();
    if let Some(fmt) = string_prop(props, "format") {
        let mapped = match fmt.trim().to_ascii_lowercase().as_str() {
            "array" => Some("array"),
            "jsonl" | "ndjson" | "newline_delimited" => Some("newline_delimited"),
            "object" | "unstructured" => Some("unstructured"),
            _ => None, // "auto" / unknown -> leave to DuckDB's auto-detect
        };
        if let Some(m) = mapped {
            extra.push_str(&format!(", format='{}'", m));
        }
    }
    if props
        .get("ignoreErrors")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
    {
        extra.push_str(", ignore_errors=true");
    }
    // How many rows DuckDB reads before it decides what the columns ARE.
    //
    // Its default is 20480, and on a document whose records do not all carry the
    // same keys that silently DROPS every column that first appears later - the
    // read succeeds, the rows look right, and a field is simply missing. There
    // is no error and nothing to notice, which makes it the worst shape of bug
    // this reader can have.
    //
    // So the default here is -1: scan everything, and know the real schema. That
    // costs an extra pass over the file, which is the honest price of not losing
    // columns; a caller who knows their records are uniform can set a number and
    // get the old behaviour back. The engine already made this exact call for
    // its own intermediate NDJSON for the same reason (#141).
    let sample = props
        .get("sampleSize")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
        })
        .filter(|n| *n != 0)
        .unwrap_or(-1);
    extra.push_str(&format!(", sample_size={}", sample));
    extra
}

pub(crate) fn build_json_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let extra = json_read_extra_args(props);
    // recordsPath: a dotted key path to the array of records inside the JSON
    // (e.g. a REST envelope like {"data":[...]} or {"response":{"records":[...]}}).
    // When set, walk to that array and unnest + recursively flatten each record
    // into columns. Without it, read_json_auto handles plain top-level arrays and
    // newline-delimited JSON as before. The 100 MB object cap keeps large API
    // responses from tripping DuckDB's 16 MB default.
    let records_path = string_prop(props, "recordsPath")
        .or_else(|| string_prop(props, "recordPath"))
        .filter(|s| !s.trim().is_empty());
    match records_path {
        Some(rp) => {
            let accessor = rp
                .split('.')
                .filter(|s| !s.trim().is_empty())
                .map(|seg| quote_ident(seg.trim()))
                .collect::<Vec<_>>()
                .join(".");
            // Flattening is a SETTING, and it was not read: the records branch always
            // walked all the way down, so the only way to keep a nested object whole was
            // to override the generated SQL. Unset it still flattens, so nothing that was
            // saved before changes under anyone.
            let flatten = props
                .get("flatten")
                .and_then(JsonValue::as_bool)
                .unwrap_or(true);
            // A document whose objects each carry an `Id` flattens to Id, Id_1, Id_2 -
            // names that say nothing about which object they came from. Keeping the
            // parent gives Id, owner.Id, account.Id instead. Off unless asked, because
            // it changes every nested column name.
            let keep_parents = props
                .get("keepParentNames")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false);
            if !flatten {
                // Records out as rows and their own keys as columns, with anything
                // nested inside them left whole. Unnesting the list on its own would
                // hand back a single struct column named after the expression, which is
                // not a table anyone can use.
                return format!(
                    "SELECT unnest(r) FROM (SELECT unnest({}) AS r FROM read_json_auto('{}', maximum_object_size=104857600{}))",
                    accessor,
                    sql_escape(&path),
                    extra
                );
            }
            let parents = if keep_parents { ", keep_parent_names := true" } else { "" };
            format!(
                "SELECT unnest({}, recursive := true{}) FROM read_json_auto('{}', maximum_object_size=104857600{})",
                accessor,
                parents,
                sql_escape(&path),
                extra
            )
        }
        None => {
            // With no records path there was no flattening step at all, so the setting
            // could not do anything here even once it was read. Asked for, the row itself
            // is flattened. NOT asked for is the default, so the read is unchanged for
            // everything saved before - which is also why the default differs from the
            // records branch, where the unnest is what pulls the records out and has
            // always flattened.
            let flatten = props
                .get("flatten")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false);
            if !flatten {
                return format!(
                    "SELECT * FROM read_json_auto('{}', maximum_object_size=104857600{})",
                    sql_escape(&path),
                    extra
                );
            }
            let keep = props
                .get("keepParentNames")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false);
            let parents = if keep { ", keep_parent_names := true" } else { "" };
            format!(
                "SELECT unnest(t, recursive := true{}) FROM read_json_auto('{}', maximum_object_size=104857600{}) t",
                parents,
                sql_escape(&path),
                extra
            )
        }
    }
}

pub(crate) fn build_sqlite_source(props: &JsonValue) -> String {
    let database = string_prop(props, "database").unwrap_or_default();
    let table = string_prop(props, "tableName").unwrap_or_default();
    let sql = string_prop(props, "sql");
    let from_arg = sql
        .filter(|s| !s.is_empty())
        .unwrap_or(table);
    format!(
        "SELECT * FROM sqlite_scan('{}', '{}')",
        sql_escape(&database),
        sql_escape(&from_arg)
    )
}

pub(crate) fn build_duckdb_source(props: &JsonValue) -> String {
    // The DuckDB file is ATTACHed as `duckle_src` (READ_ONLY) by the
    // stage / inspect prelude; we read from it qualified by that alias.
    if let Some(table) = string_prop(props, "tableName").filter(|s| !s.is_empty()) {
        match string_prop(props, "schema").filter(|s| !s.is_empty()) {
            Some(schema) => format!(
                "SELECT * FROM duckle_src.{}.{}",
                quote_ident(&schema),
                quote_ident(&table)
            ),
            None => format!("SELECT * FROM duckle_src.{}", quote_ident(&table)),
        }
    } else if let Some(sql) = string_prop(props, "sql").filter(|s| !s.trim().is_empty()) {
        // Advanced: a custom query. Reference tables as duckle_src.<table>.
        format!("({})", sql)
    } else {
        "SELECT 1 AS placeholder LIMIT 0".into()
    }
}

/// ATTACH statements for external-database nodes. The aliases are fixed
/// (`duckle_src` / `duckle_dst`) - safe because each stage is its own
/// CLI process.
/// True when the SQL text references a spatial function (an `st_` token not
/// preceded by an identifier char, so `list_`, `first_`, etc. don't false-fire).
/// Parse a user-supplied list of DuckDB extension names (#113). Accepts a JSON
/// array of strings or a single string separated by commas / whitespace /
/// newlines. Each name is lowercased and stripped to `[a-z0-9_]` (DuckDB
/// extension names are simple identifiers), which also means a name can never
/// inject SQL into the INSTALL / LOAD prelude. Empty names and duplicates are
/// dropped; order is preserved.
fn parse_extension_names(v: Option<&JsonValue>) -> Vec<String> {
    let mut raw: Vec<String> = Vec::new();
    match v {
        Some(JsonValue::Array(items)) => {
            for it in items {
                if let Some(s) = it.as_str() {
                    raw.push(s.to_string());
                }
            }
        }
        Some(JsonValue::String(s)) => {
            for part in s.split(|c: char| c == ',' || c.is_whitespace()) {
                raw.push(part.to_string());
            }
        }
        _ => {}
    }
    let mut out: Vec<String> = Vec::new();
    for name in raw {
        let clean: String = name
            .to_ascii_lowercase()
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !clean.is_empty() && !out.contains(&clean) {
            out.push(clean);
        }
    }
    out
}

pub(crate) fn references_spatial(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut i = 0;
    while let Some(pos) = lower[i..].find("st_") {
        let at = i + pos;
        let prev_ok = at == 0
            || !matches!(bytes[at - 1], b'a'..=b'z' | b'0'..=b'9' | b'_');
        if prev_ok {
            return true;
        }
        i = at + 3;
    }
    false
}

pub(crate) fn attach_prelude(component_id: &str, props: &JsonValue) -> String {
    // #84: SQL Template / Custom SQL can use spatial functions (ST_Point etc)
    // over any source (e.g. lon/lat from a CSV). Load the spatial extension when
    // the user opts in (loadSpatial) or the SQL references an ST_ function.
    // #113: also INSTALL + LOAD any extensions the user listed in
    // `loadExtensions` (e.g. h3, a5), so SQL Template can reach the wider DuckDB
    // ecosystem. Both feed one prelude, returned even when empty.
    if matches!(component_id, "code.sql" | "code.sqltemplate") {
        let mut prelude = String::new();
        let opt_in = props.get("loadSpatial").and_then(JsonValue::as_bool).unwrap_or(false);
        let auto = string_prop(props, "sql").map(|s| references_spatial(&s)).unwrap_or(false);
        let want_spatial = opt_in || auto;
        if want_spatial {
            prelude.push_str("INSTALL spatial; LOAD spatial; ");
        }
        for ext in parse_extension_names(props.get("loadExtensions")) {
            // spatial may already be queued above; don't load it twice.
            if ext == "spatial" && want_spatial {
                continue;
            }
            prelude.push_str(&format!("INSTALL {ext}; LOAD {ext}; "));
        }
        return prelude;
    }
    // Network DBs use host/port + libpq-style fields, not the
    // file-style `database` path the file-based ATTACH connectors use.
    // Cockroach speaks PG wire so it rides the postgres extension;
    // MariaDB speaks MySQL wire so it rides the mysql extension.
    // #86: SQL Server / Synapse sink via the DuckDB mssql community extension
    // (pure TDS, bulk COPY/INSERT). Only the bulk path emits this ATTACH; when
    // the user sets bulk=false the tiberius driver handles the write instead
    // (no prelude, see plan/mod.rs), so emit nothing here in that case.
    if matches!(component_id, "snk.sqlserver" | "snk.synapse")
        && props.get("bulk").and_then(JsonValue::as_bool).unwrap_or(true)
    {
        return mssql_attach(props);
    }
    match component_id {
        "src.postgres" | "src.cockroach" | "src.pgvector" | "src.redshift" => {
            // Redshift speaks the Postgres wire protocol with a different
            // default port (5439). The DuckDB postgres extension is happy
            // pointed at any pg-compatible endpoint.
            let default_port = if component_id == "src.redshift" { 5439 } else { 5432 };
            return db_attach(props, "postgres", default_port, true);
        }
        "snk.postgres" | "snk.cockroach" | "snk.pgvector" | "snk.redshift" => {
            let default_port = if component_id == "snk.redshift" { 5439 } else { 5432 };
            return db_attach(props, "postgres", default_port, false);
        }
        "src.mysql" | "src.mariadb" => return db_attach(props, "mysql", 3306, true),
        "snk.mysql" | "snk.mariadb" => return db_attach(props, "mysql", 3306, false),
        "src.motherduck" => return md_attach(props, true),
        "snk.motherduck" => return md_attach(props, false),
        "src.quack" => return quack_attach(props, true),
        "snk.quack" => return quack_attach(props, false),
        "src.ducklake" | "src.ducklake.diff" => return ducklake_attach(props, true),
        "snk.ducklake" => return ducklake_attach(props, false),
        // BigQuery via the duckdb-bigquery community extension. The
        // user's prop 'project' becomes the BigQuery project ID; the
        // ATTACH alias is the standard duckle_src / duckle_dst.
        "src.bigquery" => return bigquery_attach(props, true),
        "snk.bigquery" => return bigquery_attach(props, false),
        // snk.excel COPYs through the DuckDB excel extension; LOAD is
        // enough since the install paths pre-fetched it.
        "snk.excel" => return "LOAD excel; ".into(),
        // Hugging Face dataset read over hf:// (native HF connector). httpfs
        // resolves the URL; public datasets need only the extension. A private
        // or gated dataset needs a HUGGINGFACE secret with a token - emitted
        // only when one is set. The `token` prop is redacted in exported SQL by
        // is_secret_prop_key and resolved from ${ENV:...} at run time, like every
        // other connector secret.
        "src.huggingface" => {
            let mut prelude = String::from("INSTALL httpfs; LOAD httpfs; ");
            if let Some(token) = string_prop(props, "token").filter(|s| !s.trim().is_empty()) {
                prelude.push_str(&format!(
                    "CREATE OR REPLACE SECRET duckle_hf (TYPE HUGGINGFACE, TOKEN '{}'); ",
                    sql_escape(&token)
                ));
            }
            return prelude;
        }
        // Extensions are pre-installed (desktop: the first-launch
        // installer; CI: a dedicated pre-install step). Each fresh
        // DuckDB process still needs LOAD. Concurrent INSTALL would
        // race on the cached extension file and intermittently fail.
        "src.avro" => return "LOAD avro; ".into(),
        "src.excel" => return "LOAD excel; ".into(),
        "src.iceberg" | "snk.iceberg" => return "LOAD iceberg; ".into(),
        "src.delta" => return "LOAD delta; ".into(),
        // Vector Similarity Search uses the vss extension's array_*
        // distance functions; LOAD before the SELECT runs.
        "xf.ai.vector_search" => return "LOAD vss; ".into(),
        // Full-Text Search uses the fts extension's match_bm25.
        "xf.ai.text_search" => return "LOAD fts; ".into(),
        // Spatial is GDAL-backed and ~50 MB; deliberately kept out of
        // the first-launch DUCKDB_EXTENSIONS pre-fetch so the install
        // stays small. INSTALL runs lazily on first use, then LOAD on
        // every subsequent run.
        // CRS-aware spatial measurements (issue #177) use the *_Spheroid
        // functions, which warn (to stderr) about axis order unless it is pinned.
        // SET geometry_always_xy assumes [lon, lat] - the GeoParquet / de-facto
        // standard - so the measurement is deterministic and warning-free.
        "xf.geo.distance"
        | "xf.geo.length"
        | "xf.geo.perimeter"
        | "xf.geo.area"
        // Create Geometry (#190) builds points/geoms and pins lon/lat order so
        // ST_Point(x, y) reads x as longitude, matching the GeoParquet default.
        | "xf.geo.create" => {
            return "INSTALL spatial; LOAD spatial; SET geometry_always_xy = true; ".into();
        }
        "src.spatial"
        | "src.gdb"
        | "snk.spatial"
        | "xf.geo.buffer"
        | "xf.geo.flip"
        | "xf.geo.intersects"
        | "xf.geo.setcrs"
        | "xf.geo.reproject"
        | "xf.join.spatial"
        | "xf.geo.clip"
        | "xf.geo.erase"
        // Geometry DQ tools (issue #158) call ST_IsValid / ST_MakeValid / ST_IsEmpty.
        | "qa.geomvalidate"
        | "qa.geomrepair"
        | "qa.geomempty" => {
            return "INSTALL spatial; LOAD spatial; ".into();
        }
        // inet is a small built-in extension. INSTALL is a no-op once
        // the extension is bundled, but keeping it explicit means a
        // fresh CLI cache still works without the first-launch fetch.
        "xf.ip.parse" => return "INSTALL inet; LOAD inet; ".into(),
        _ => {}
    }
    let db = match string_prop(props, "database").filter(|s| !s.is_empty()) {
        Some(d) => d,
        None => return String::new(),
    };
    match component_id {
        "src.duckdb" => format!("ATTACH '{}' AS duckle_src (READ_ONLY); ", sql_escape(&db)),
        "snk.sqlite" => format!("ATTACH '{}' AS duckle_dst (TYPE SQLITE); ", sql_escape(&db)),
        "snk.duckdb" => format!("ATTACH '{}' AS duckle_dst; ", sql_escape(&db)),
        _ => String::new(),
    }
}

/// ATTACH a network relational database through a DuckDB extension
/// (postgres or mysql). The connection string is built libpq-style from
/// host / port / database / user / password; the extension-specific key
/// for the database name (`dbname` for libpq/Postgres, `database` for
/// the MySQL driver) is handled here. INSTALL+LOAD is prepended so a
/// fresh user without the extension cache still attaches successfully,
/// though the first-launch installer already pre-fetches both.
/// #86: ATTACH a SQL Server (or Synapse) target via the DuckDB `mssql` community
/// extension for high-throughput bulk writes (TDS protocol, no ODBC/JDBC). The
/// connection string is the extension's `key=value;...` form; the alias is the
/// standard `duckle_dst` so build_relational_sink writes through it with plain
/// CREATE/INSERT. Empty when no host (the caller then errors clearly downstream).
fn mssql_attach(props: &JsonValue) -> String {
    let host = string_prop(props, "host").unwrap_or_default();
    if host.is_empty() {
        return String::new();
    }
    let port = props
        .get("port")
        .and_then(|v| v.as_u64())
        .filter(|p| *p > 0)
        .unwrap_or(1433);
    let mut parts = vec![format!("server={},{}", host, port)];
    if let Some(db) = string_prop(props, "database").filter(|s| !s.is_empty()) {
        parts.push(format!("database={}", db));
    }
    if let Some(u) = string_prop(props, "user")
        .or_else(|| string_prop(props, "username"))
        .filter(|s| !s.is_empty())
    {
        parts.push(format!("user={}", u));
    }
    if let Some(p) = string_prop(props, "password").filter(|s| !s.is_empty()) {
        parts.push(format!("password={}", p));
    }
    // #86 follow-up: honour the same "Trust TLS cert" toggle as the legacy
    // driver (default off). When off we omit the key so the extension validates
    // the cert / lets an older non-TLS server negotiate plainly; when on we trust
    // self-signed certs. Previously this was hardcoded true, which forced a TLS
    // handshake that fails against servers without TLS.
    let trust = props.get("trustCert").and_then(|v| v.as_bool()).unwrap_or(false);
    if trust {
        parts.push("TrustServerCertificate=true".to_string());
    }
    let connstr = parts.join(";");
    // #86 follow-up: expose the insert batch size to the bulk path too. SQL
    // Server caps INSERT ... VALUES at 1000 rows (mssql_insert_max_rows_per_statement),
    // so clamp. It is a session setting, applied after LOAD. COPY/BCP (the fast
    // overwrite path) is unaffected and stays on by default.
    let batch = props
        .get("batchSize")
        .and_then(|v| v.as_u64())
        .filter(|n| *n > 0)
        .map(|n| n.min(1000))
        .unwrap_or(1000);
    format!(
        "INSTALL mssql FROM community; LOAD mssql; SET mssql_insert_batch_size = {}; ATTACH '{}' AS duckle_dst (TYPE mssql); ",
        batch,
        sql_escape(&connstr)
    )
}

/// libpq-style value quoting for a `key=value` connection string. A value that
/// is empty or contains whitespace, a single quote, or a backslash must be
/// wrapped in single quotes with `\` and `'` backslash-escaped; otherwise the
/// value parser stops at the first space and mis-reads the rest of the string.
/// This is what makes special-character passwords (spaces, quotes, `?`, `{`,
/// ...) work over the mysql / postgres ATTACH (issue #157). The whole connstr
/// is later wrapped in a SQL string literal by `sql_escape`, which doubles the
/// single quotes we add here - DuckDB decodes that back to the libpq form.
fn conn_kv_quote(v: &str) -> String {
    let needs_quote = v.is_empty()
        || v.chars().any(|c| c.is_whitespace() || c == '\'' || c == '\\');
    if needs_quote {
        let escaped = v.replace('\\', "\\\\").replace('\'', "\\'");
        format!("'{}'", escaped)
    } else {
        v.to_string()
    }
}

/// Append advanced libpq connection parameters (issue #161) to the structured
/// `key=value` connection string. This is what lets Duckle reach PostgreSQL
/// instances that enforce TLS (`sslmode=require` / `verify-full`, standard in
/// regulated environments) plus client-cert auth, a connect timeout, and
/// session `options`. The DuckDB postgres extension hands the DSN to libpq, so
/// these are the exact libpq parameter names.
///
/// The SSL / libpq-named fields are gated on the postgres wire family because
/// the mysql extension uses different key names (`ssl_mode`, `ssl_ca`, ...); a
/// mysql user needing those uses the free-text passthrough below instead. The
/// passthrough (`connParams`) applies to both families and is appended verbatim
/// so any parameter we do not model explicitly still works.
fn push_advanced_conn_opts(parts: &mut Vec<String>, props: &JsonValue, extension: &str) {
    if extension == "postgres" {
        // sslmode: disable | allow | prefer | require | verify-ca | verify-full.
        if let Some(m) = string_prop(props, "sslmode").filter(|s| !s.is_empty()) {
            parts.push(format!("sslmode={}", conn_kv_quote(&m)));
        }
        // Client-cert / root-cert file paths (values may contain spaces).
        for (prop, key) in [("sslrootcert", "sslrootcert"), ("sslcert", "sslcert"), ("sslkey", "sslkey")] {
            if let Some(v) = string_prop(props, prop).filter(|s| !s.is_empty()) {
                parts.push(format!("{}={}", key, conn_kv_quote(&v)));
            }
        }
        // connect_timeout is integer seconds; accept a number or a numeric string.
        if let Some(t) = props
            .get("connectTimeout")
            .and_then(JsonValue::as_u64)
            .or_else(|| string_prop(props, "connectTimeout").and_then(|s| s.trim().parse::<u64>().ok()))
            .filter(|n| *n > 0)
        {
            parts.push(format!("connect_timeout={}", t));
        }
        // Session-level options, e.g. "-c search_path=myschema" (contains spaces,
        // so conn_kv_quote wraps it in libpq single quotes).
        if let Some(o) = string_prop(props, "options").filter(|s| !s.is_empty()) {
            parts.push(format!("options={}", conn_kv_quote(&o)));
        }
    }
    // Free-text passthrough: any additional libpq/driver parameters, appended
    // verbatim. The user writes `key=value ...` form and is responsible for
    // quoting; the whole DSN is later wrapped by sql_escape so no SQL escape is
    // needed here. Works for both postgres and mysql.
    if let Some(extra) = string_prop(props, "connParams")
        .or_else(|| string_prop(props, "extraParams"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        parts.push(extra);
    }
}

pub(crate) fn db_attach(props: &JsonValue, extension: &str, default_port: u64, read_only: bool) -> String {
    // Advanced override: a raw connection string the user supplies directly
    // (issue #157). When present it replaces the host/port/user/password we
    // would otherwise build, so a user can pass their own libpq key-value
    // string or a mysql://... / postgresql://... URL and encode any special
    // characters themselves. The duckle_src / duckle_dst alias is still ours,
    // so downstream plan stages resolve the same way.
    let connstr = if let Some(c) = string_prop(props, "connString")
        .or_else(|| string_prop(props, "connectionString"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        c
    } else {
        let host = string_prop(props, "host").unwrap_or_default();
        if host.is_empty() {
            return String::new();
        }
        let port = props
            .get("port")
            .and_then(|v| v.as_u64())
            .filter(|p| *p > 0)
            .unwrap_or(default_port);
        let db_key = if extension == "postgres" { "dbname" } else { "database" };
        let mut parts = vec![format!("host={}", conn_kv_quote(&host)), format!("port={}", port)];
        if let Some(db) = string_prop(props, "database").filter(|s| !s.is_empty()) {
            parts.push(format!("{}={}", db_key, conn_kv_quote(&db)));
        }
        if let Some(u) = string_prop(props, "user")
            .or_else(|| string_prop(props, "username"))
            .filter(|s| !s.is_empty())
        {
            parts.push(format!("user={}", conn_kv_quote(&u)));
        }
        if let Some(p) = string_prop(props, "password").filter(|s| !s.is_empty()) {
            parts.push(format!("password={}", conn_kv_quote(&p)));
        }
        push_advanced_conn_opts(&mut parts, props, extension);
        parts.join(" ")
    };
    // `read_only` from the call site means "this is a source" (aliased
    // duckle_src) vs "sink" (duckle_dst). The READ_ONLY *attach option* is
    // applied to sources by default, but a user can drop it with readOnly=false
    // when their server / extension version rejects it (issue #157).
    let is_source = read_only;
    let alias = if is_source { "duckle_src" } else { "duckle_dst" };
    let attach_read_only = is_source
        && !matches!(
            props.get("readOnly"),
            Some(JsonValue::Bool(false))
        )
        && !matches!(
            string_prop(props, "readOnly").as_deref().map(|s| s.trim().to_ascii_lowercase()),
            Some(ref s) if s == "false" || s == "0" || s == "no" || s == "off"
        );
    let mode = if attach_read_only { ", READ_ONLY" } else { "" };
    let type_name = extension.to_uppercase();
    format!(
        "LOAD {ext}; ATTACH '{conn}' AS {alias} (TYPE {type_name}{mode}); ",
        ext = extension,
        conn = sql_escape(&connstr),
        alias = alias,
        type_name = type_name,
        mode = mode
    )
}

/// Source for a network relational DB (Postgres / Cockroach via the
/// postgres extension; MySQL / MariaDB via the mysql extension). Reads
/// from `duckle_src` qualified by the right depth: Postgres uses
/// catalog.schema.table (default schema `public`); MySQL uses
/// catalog.table (the database is selected at ATTACH time).
/// DuckLake time-travel clause for a whole-table source read, from the node's
/// `asOfVersion` (snapshot id) or `asOfTimestamp` prop. Returns "" when neither
/// is set; version wins if both are present. Lets a pipeline read a table as of
/// a past snapshot - the foundation for the snapshot inspector / data diff.
fn time_travel_clause(props: &JsonValue) -> String {
    if let Some(v) = props.get("asOfVersion").and_then(|v| v.as_u64()) {
        return format!(" AT (VERSION => {})", v);
    }
    if let Some(s) = string_prop(props, "asOfVersion").filter(|s| !s.trim().is_empty()) {
        if let Ok(n) = s.trim().parse::<u64>() {
            return format!(" AT (VERSION => {})", n);
        }
    }
    if let Some(ts) = string_prop(props, "asOfTimestamp").filter(|s| !s.trim().is_empty()) {
        return format!(" AT (TIMESTAMP => '{}')", sql_escape(ts.trim()));
    }
    String::new()
}

/// DuckLake Data Diff source (src.ducklake.diff): the row-level change feed
/// between two explicit snapshots, via the global
/// `ducklake_table_changes(catalog, schema, table, from, to)` (catalog +
/// schema + table passed separately, which handles non-default schemas, unlike
/// the catalog-method form). Emits a `change_type` column (insert / delete /
/// update_preimage / update_postimage) plus the row, so it doubles as a data
/// diff / CI assertion when wired into a validator. Both versions are literals
/// (the catalog is ATTACHed as duckle_src by attach_prelude); pick them with
/// the Browse button.
pub(crate) fn build_ducklake_diff(props: &JsonValue) -> String {
    let table = string_prop(props, "table").filter(|s| !s.is_empty()).unwrap_or_default();
    let schema = string_prop(props, "schema")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "main".into());
    let ver = |k: &str| -> u64 {
        props
            .get(k)
            .and_then(|v| v.as_u64())
            .or_else(|| string_prop(props, k).and_then(|s| s.trim().parse::<u64>().ok()))
            .unwrap_or(0)
    };
    format!(
        "SELECT * FROM ducklake_table_changes('duckle_src', '{}', '{}', {}, {})",
        sql_escape(&schema),
        sql_escape(&table),
        ver("fromVersion"),
        ver("toVersion")
    )
}

/// The DuckDB scanner-extension passthrough that runs a query verbatim on the
/// remote server and returns its result (#115 pushdown / in-database
/// processing). `postgres_query` / `mysql_query` are the read analogs of the
/// `postgres_execute` / `mysql_execute` writes Duckle already uses. Returns
/// None for families that have no such function (they keep the subquery wrap).
fn pushdown_query_fn(component_id: &str) -> Option<&'static str> {
    match component_id {
        "src.postgres" | "src.cockroach" | "src.pgvector" | "src.redshift" => Some("postgres_query"),
        "src.mysql" | "src.mariadb" => Some("mysql_query"),
        _ => None,
    }
}

/// Whether the `pushdown` toggle is on (bool true or a truthy string).
pub(crate) fn relational_pushdown_on(props: &JsonValue) -> bool {
    props.get("pushdown").and_then(JsonValue::as_bool).unwrap_or(false)
        || matches!(
            string_prop(props, "pushdown")
                .as_deref()
                .map(|s| s.trim().to_ascii_lowercase())
                .as_deref(),
            Some("true") | Some("1") | Some("yes") | Some("on")
        )
}

pub(crate) fn build_relational_source(component_id: &str, props: &JsonValue) -> Result<String, String> {
    let mode = string_prop(props, "mode").unwrap_or_else(|| "table".into());
    if mode == "incremental" {
        return Err(format!(
            "{}: incremental read mode isn't implemented yet",
            component_id
        ));
    }
    // A custom SQL query wins whenever one is provided, the same way
    // build_duckdb_source infers intent from the filled field. So leaving the
    // Read mode dropdown at its default "Whole table" while typing into the SQL
    // box still runs the query instead of demanding a table name - the duck
    // sources (ducklake / motherduck / quack) now match src.duckdb (#77).
    // Accept either `sql` (duck/relational manifests) or `query` (the warehouse /
    // network manifests use that key) so the custom-query pushdown is uniform
    // across families without renaming keys in saved pipelines.
    let sql = string_prop(props, "sql")
        .or_else(|| string_prop(props, "query"))
        .filter(|s| !s.trim().is_empty());
    if mode == "sql" || sql.is_some() {
        let sql = sql.ok_or_else(|| format!("{}: SQL query is empty", component_id))?;
        // #115 in-database processing: when "pushdown" is on and the family has a
        // native passthrough (Postgres/MySQL), emit it so the literal SQL runs
        // verbatim on the remote server (aggregations, joins and vendor SQL
        // execute in-database, only the result is returned), instead of the
        // (<sql>) subquery wrap that DuckDB's scanner re-parses and re-plans.
        // Families without a passthrough, or with pushdown off, keep the wrap.
        if relational_pushdown_on(props) {
            if let Some(func) = pushdown_query_fn(component_id) {
                let inner = sql.trim().trim_end_matches(';').trim();
                return Ok(format!(
                    "SELECT * FROM {}('duckle_src', '{}')",
                    func,
                    sql_escape(inner)
                ));
            }
        }
        return Ok(format!("({})", sql));
    }
    let table = string_prop(props, "tableName")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("{}: table name is required", component_id))?;
    let schema = string_prop(props, "schemaName").filter(|s| !s.is_empty());
    // DuckLake supports point-in-time reads (AT VERSION / AT TIMESTAMP); only
    // it gets the time-travel clause so a stray prop can't produce invalid SQL
    // on a plain relational source.
    let at = if component_id == "src.ducklake" {
        time_travel_clause(props)
    } else {
        String::new()
    };
    Ok(format!(
        "SELECT * FROM {}{}",
        relational_qualified("duckle_src", component_id, schema.as_deref(), &table),
        at
    ))
}

/// Sink for a network relational DB (Postgres / Cockroach / MySQL /
/// MariaDB). Only `overwrite` (DROP + CREATE) is wired today; append /
/// upsert / truncate / error-if-exists error loudly rather than
/// pretending to apply. Writes inside the ATTACHed `duckle_dst` DB.
/// DuckDB-native targets whose attached connection executes DuckDB's
/// `MERGE INTO` (the "merge" write mode, issue #39). The Postgres / MySQL /
/// Redshift / BigQuery families are excluded: they run through DuckDB's
/// scanner extensions, which do not push a MERGE, so they keep the
/// DELETE + INSERT "upsert" mode.
pub(crate) fn supports_merge(component_id: &str) -> bool {
    // snk.quack is deliberately absent: a Quack-attached table is a streaming
    // remote scan, and MERGE INTO against one fails with "Can only merge into
    // base tables!" (verified on the pinned DuckDB 1.5.4 against a live
    // quack_serve). Listing it here advertised a mode that could never run.
    matches!(
        component_id,
        "snk.duckdb" | "snk.sqlite" | "snk.motherduck" | "snk.ducklake"
    )
}

/// Build a DuckDB `MERGE INTO` for the "merge" write mode: a partial-column
/// upsert that UPDATEs only the columns the source actually carries (leaving
/// every other target column untouched) and INSERTs new rows by the source's
/// columns. Unlike "upsert" (DELETE-by-key + re-INSERT, which nulls absent
/// columns), this preserves columns the source does not provide - the use case
/// in issue #39. `target` is the already-qualified+quoted target table;
/// `from_quoted` is the quoted source view; `cols` is the source column list
/// (from the sink's input schema).
fn build_merge_stmt(
    component_id: &str,
    target: &str,
    from_quoted: &str,
    props: &JsonValue,
    cols: &[String],
) -> Result<String, EngineError> {
    let keys = columns_list(props, "conflictColumns");
    if keys.is_empty() {
        return Err(EngineError::Config(format!(
            "{}: merge needs at least one conflict (key) column",
            component_id
        )));
    }
    if cols.is_empty() {
        return Err(EngineError::Config(format!(
            "{}: merge needs to know the input columns - connect the source so its schema is available, or use the 'upsert' mode",
            component_id
        )));
    }
    let del_col = string_prop(props, "deleteColumn").filter(|s| !s.is_empty());
    let del_val = string_prop(props, "deleteValue").unwrap_or_else(|| "delete".into());
    // Data columns = source columns minus the optional delete-flag control column.
    let data_cols: Vec<&str> = cols
        .iter()
        .map(|c| c.as_str())
        .filter(|c| del_col.as_deref() != Some(c))
        .collect();
    for k in &keys {
        if !data_cols.iter().any(|c| *c == k.as_str()) {
            return Err(EngineError::Config(format!(
                "{}: merge key column '{}' is not among the input columns",
                component_id, k
            )));
        }
    }
    let on = keys
        .iter()
        .map(|k| format!("tgt.{k} = src.{k}", k = quote_ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let set_clause = data_cols
        .iter()
        .filter(|c| !keys.iter().any(|k| k.as_str() == **c))
        .map(|c| format!("{c} = src.{c}", c = quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_cols = data_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = data_cols
        .iter()
        .map(|c| format!("src.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    // Optional CDC delete propagation, mirroring the upsert mode: a matched row
    // flagged for deletion is removed; a flagged unmatched row is not inserted.
    let (delete_clause, not_matched_filter) = match &del_col {
        Some(c) => (
            format!(
                "WHEN MATCHED AND src.{c} IS NOT DISTINCT FROM '{v}' THEN DELETE ",
                c = quote_ident(c),
                v = sql_escape(&del_val)
            ),
            format!(
                " AND src.{c} IS DISTINCT FROM '{v}'",
                c = quote_ident(c),
                v = sql_escape(&del_val)
            ),
        ),
        None => (String::new(), String::new()),
    };
    // Omit the UPDATE clause when the source has only key columns (nothing to set).
    let update_clause = if set_clause.is_empty() {
        String::new()
    } else {
        format!("WHEN MATCHED THEN UPDATE SET {set} ", set = set_clause)
    };
    Ok(format!(
        "MERGE INTO {target} AS tgt USING {from} AS src ON ({on}) \
         {delete_clause}{update_clause}WHEN NOT MATCHED{nmf} THEN INSERT ({ic}) VALUES ({iv})",
        target = target,
        from = from_quoted,
        on = on,
        delete_clause = delete_clause,
        update_clause = update_clause,
        nmf = not_matched_filter,
        ic = insert_cols,
        iv = insert_vals,
    ))
}

pub(crate) fn build_relational_sink(
    component_id: &str,
    props: &JsonValue,
    from_view: &str,
    cols: &[String],
) -> Result<String, EngineError> {
    let table = string_prop(props, "tableName")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| EngineError::Config(format!("{}: table name is required", component_id)))?;
    // `schemaName` is the standard prop; the SQL Server sink form uses `schema`,
    // so accept it too (#86 bulk path).
    let schema = string_prop(props, "schemaName")
        .or_else(|| string_prop(props, "schema"))
        .filter(|s| !s.is_empty());
    let mode = string_prop(props, "mode").unwrap_or_else(|| "overwrite".into());
    let qual = relational_qualified("duckle_dst", component_id, schema.as_deref(), &table);
    // A Quack-attached table is a streaming remote scan, not a base table, and
    // DuckDB's binder refuses to rewrite one. Verified against the pinned 1.5.4
    // with a live quack_serve: SELECT / INSERT / CTAS work, but TRUNCATE and
    // DELETE give "Can only delete from base table", UPDATE gives "Can only
    // update base table" and MERGE gives "Can only merge into base tables!".
    // Those errors surface mid-run with no hint of the cause, so refuse the
    // mode while the pipeline is still being compiled and name the alternative.
    if component_id == "snk.quack" && matches!(mode.as_str(), "truncate" | "upsert" | "merge") {
        return Err(EngineError::Config(format!(
            "{}: write mode '{}' is not supported over the Quack protocol - DuckDB cannot \
             UPDATE, DELETE or MERGE a remote Quack table (it is a streaming scan, not a base \
             table). Use 'append' or 'overwrite', or run the pipeline against the remote \
             database directly instead of through Quack.",
            component_id, mode
        )));
    }
    match mode.as_str() {
        "overwrite" => Ok(format!(
            "DROP TABLE IF EXISTS {q}; CREATE TABLE {q} AS (SELECT * FROM {from})",
            q = qual,
            from = quote_ident(from_view)
        )),
        // Append inserts into the target, creating it on first write when it
        // doesn't exist yet. CREATE TABLE AS SELECT ... LIMIT 0 derives the
        // column types from the upstream, so no separate schema inspection is
        // needed - matching the truncate/upsert branches below and build_db_sink.
        "append" => Ok(format!(
            "CREATE TABLE IF NOT EXISTS {q} AS SELECT * FROM {from} LIMIT 0; \
             INSERT INTO {q} SELECT * FROM {from}",
            q = qual,
            from = quote_ident(from_view)
        )),
        // Truncate keeps the table's existing schema (and any indexes /
        // grants on it) and replaces just the rows. Useful when the
        // table is referenced by downstream views or foreign keys.
        "truncate" => Ok(format!(
            "TRUNCATE TABLE {q}; INSERT INTO {q} SELECT * FROM {from}",
            q = qual,
            from = quote_ident(from_view)
        )),
        // Upsert: set-based DELETE-by-key + re-INSERT, run by DuckDB against the
        // ATTACHed target (DuckLake / MotherDuck / Quack and the DuckDB
        // postgres/mysql extensions all execute DELETE + INSERT). No PRIMARY
        // KEY required. An optional delete-flag column (deleteColumn =
        // deleteValue) removes matched rows without re-inserting them, which is
        // how CDC / diff deletes propagate (issue #19).
        "upsert" => {
            let keys = columns_list(props, "conflictColumns");
            if keys.is_empty() {
                return Err(EngineError::Config(format!(
                    "{}: upsert needs at least one conflict column",
                    component_id
                )));
            }
            let del_col = string_prop(props, "deleteColumn").filter(|s| !s.is_empty());
            let del_val = string_prop(props, "deleteValue").unwrap_or_else(|| "delete".into());
            let sel = match &del_col {
                Some(c) => format!("* EXCLUDE ({})", quote_ident(c)),
                None => "*".to_string(),
            };
            let key_tuple = keys
                .iter()
                .map(|k| quote_ident(k))
                .collect::<Vec<_>>()
                .join(", ");
            let insert_filter = match &del_col {
                Some(c) => format!(
                    " WHERE {} IS DISTINCT FROM '{}'",
                    quote_ident(c),
                    sql_escape(&del_val)
                ),
                None => String::new(),
            };
            Ok(format!(
                "CREATE TABLE IF NOT EXISTS {q} AS SELECT {sel} FROM {from} LIMIT 0; \
                 DELETE FROM {q} WHERE ({keys}) IN (SELECT {keys} FROM {from}); \
                 INSERT INTO {q} SELECT {sel} FROM {from}{insert_filter}",
                q = qual,
                sel = sel,
                from = quote_ident(from_view),
                keys = key_tuple,
                insert_filter = insert_filter,
            ))
        }
        // Merge: partial-column upsert via DuckDB MERGE INTO (issue #39).
        // Updates only the columns the source provides, leaving other target
        // columns untouched; inserts new rows by the source's columns. Only the
        // DuckDB-native targets execute MERGE; the rest keep DELETE+INSERT upsert.
        "merge" => {
            if !supports_merge(component_id) {
                return Err(EngineError::Config(format!(
                    "{}: 'merge' is only supported for DuckDB-native targets (duckdb, sqlite, motherduck, ducklake, quack); use 'upsert' here",
                    component_id
                )));
            }
            let del_col = string_prop(props, "deleteColumn").filter(|s| !s.is_empty());
            let sel = match &del_col {
                Some(c) => format!("* EXCLUDE ({})", quote_ident(c)),
                None => "*".to_string(),
            };
            let create = format!(
                "CREATE TABLE IF NOT EXISTS {q} AS SELECT {sel} FROM {from} LIMIT 0; ",
                q = qual,
                sel = sel,
                from = quote_ident(from_view)
            );
            let merge = build_merge_stmt(component_id, &qual, &quote_ident(from_view), props, cols)?;
            Ok(format!("{}{}", create, merge))
        }
        other => Err(EngineError::Config(format!(
            "{}: write mode '{}' isn't implemented yet (use 'overwrite', 'append', 'truncate', 'upsert', or 'merge')",
            component_id, other
        ))),
    }
}

/// Qualify a table reference under the right naming depth for each
/// network DB family. Postgres / Cockroach use catalog.schema.table
/// (default schema `public`); MotherDuck is DuckDB-native and uses
/// catalog.schema.table with default schema `main`; MySQL / MariaDB
/// use catalog.table (the MySQL database is selected at ATTACH time,
/// though we honour an explicit schemaName as a 3-level qualifier).
pub(crate) fn relational_qualified(alias: &str, component_id: &str, schema: Option<&str>, table: &str) -> String {
    let default_schema: Option<&str> = if component_id.ends_with(".postgres")
        || component_id.ends_with(".cockroach")
        || component_id.ends_with(".pgvector")
        || component_id.ends_with(".redshift")
    {
        Some("public")
    } else if component_id.ends_with(".motherduck") || component_id.ends_with(".ducklake") {
        Some("main")
    } else if component_id.ends_with(".sqlserver") || component_id.ends_with(".synapse") {
        // SQL Server / Synapse default schema (#86, mssql extension bulk path).
        Some("dbo")
    } else if component_id.ends_with(".bigquery") {
        // BigQuery's first level is a "dataset" - same shape as schema.
        // Caller can supply dataset via either prop name; we leave the
        // default empty so the ATTACH-time default dataset takes over
        // when unqualified.
        None
    } else {
        None // MySQL / MariaDB: skip the schema layer unless given
    };
    match (schema, default_schema) {
        (Some(s), _) => format!("{}.{}.{}", alias, quote_ident(s), quote_ident(table)),
        (None, Some(d)) => format!("{}.{}.{}", alias, quote_ident(d), quote_ident(table)),
        (None, None) => format!("{}.{}", alias, quote_ident(table)),
    }
}

/// DuckLake ATTACH. DuckLake is DuckDB's own lakehouse format (a
/// catalog stored in a DuckDB file or Postgres pointing at parquet
/// data files). The form's `path` is the catalog path.
pub(crate) fn ducklake_attach(props: &JsonValue, read_only: bool) -> String {
    let path = match string_prop(props, "path").filter(|s| !s.is_empty()) {
        Some(p) => p,
        None => return String::new(),
    };
    let alias = if read_only { "duckle_src" } else { "duckle_dst" };
    let mut opts: Vec<String> = Vec::new();
    if read_only {
        opts.push("READ_ONLY".into());
    }
    // A DuckLake catalog does not have to be a local DuckDB file: the path may
    // be `sqlite:...`, `postgres:dbname=lake host=...` or `mysql:...`. Those
    // forms carry no implied location for the data files, so DuckLake requires
    // DATA_PATH when the lake does not already exist. Without a way to emit it
    // a Postgres-catalogued lake could not be attached at all.
    //
    // Optional on purpose: an existing lake reads its stored data path from its
    // own metadata, and omitting the option reproduces the previous output
    // byte for byte, so saved pipelines are unaffected.
    if let Some(dp) = string_prop(props, "dataPath").filter(|s| !s.trim().is_empty()) {
        opts.push(format!("DATA_PATH '{}'", sql_escape(dp.trim())));
    }
    // A Postgres-catalogued lake usually shares its database with other things, so the
    // catalog tables live in a schema of their own. That schema is named by
    // METADATA_SCHEMA and there was no way to reach it, so such a lake could not be
    // attached at all.
    if let Some(ms) = string_prop(props, "metadataSchema").filter(|s| !s.trim().is_empty()) {
        opts.push(format!("METADATA_SCHEMA '{}'", sql_escape(ms.trim())));
    }
    // Anything else the catalog takes, as written: `META_SECRET` to point at a stored
    // secret instead of spelling a password into the path, `META_*` for the rest. The
    // NAME is checked rather than escaped - an option name is not a place to put SQL,
    // and a name that is not a plain word is left out rather than quoted into one.
    for (k, v) in kv_pairs(props, "attachOptions") {
        let (k, v) = (k.trim(), v.trim());
        let plain = !k.is_empty()
            && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !k.chars().next().is_some_and(|c| c.is_ascii_digit());
        if plain && !v.is_empty() {
            opts.push(format!("{} '{}'", k.to_uppercase(), sql_escape(v)));
        }
    }
    let tail = if opts.is_empty() {
        String::new()
    } else {
        format!(" ({})", opts.join(", "))
    };
    format!(
        "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:{}' AS {}{}; ",
        sql_escape(&path),
        alias,
        tail
    )
}

/// MotherDuck ATTACH. MotherDuck support is built into DuckDB itself
/// An inline token is applied via `SET motherduck_token` (after the extension
/// loads); if the token isn't in the form, MotherDuck falls back to the
/// MOTHERDUCK_TOKEN env var, which lets a user keep credentials out of saved
/// pipelines.
/// BigQuery via the duckdb-bigquery community extension. ATTACHes a
/// project by ID; auth uses the standard GCP credential discovery
/// (GOOGLE_APPLICATION_CREDENTIALS env var, gcloud default, etc).
/// User points the extension at a project via the 'project' prop;
/// optional 'dataset' fills in the default dataset for unqualified
/// table names.
pub(crate) fn bigquery_attach(props: &JsonValue, read_only: bool) -> String {
    let project = match string_prop(props, "project").filter(|s| !s.is_empty()) {
        Some(p) => p,
        None => return String::new(),
    };
    let dataset = string_prop(props, "dataset").filter(|s| !s.is_empty());
    let attach_target = match dataset {
        Some(d) => format!("project={} dataset={}", project, d),
        None => format!("project={}", project),
    };
    let (alias, mode) = if read_only {
        ("duckle_src", " (READ_ONLY)")
    } else {
        ("duckle_dst", "")
    };
    // INSTALL/LOAD the community extension. The community: tag tells
    // DuckDB to fetch from the community-extensions repo.
    format!(
        "INSTALL bigquery FROM community; LOAD bigquery; ATTACH '{}' AS {} (TYPE bigquery{}); ",
        sql_escape(&attach_target), alias, mode
    )
}

pub(crate) fn md_attach(props: &JsonValue, read_only: bool) -> String {
    let db = match string_prop(props, "database").filter(|s| !s.is_empty()) {
        Some(d) => d,
        None => return String::new(),
    };
    let token = string_prop(props, "token").filter(|s| !s.is_empty());
    let (alias, mode) = if read_only {
        ("duckle_src", " (READ_ONLY)")
    } else {
        ("duckle_dst", "")
    };
    // An inline token must be applied via SET motherduck_token AFTER the
    // extension loads, NOT as an `md:` query parameter: `md:db?motherduck_token=`
    // makes MotherDuck treat the whole `db?motherduck_token=...` string as the
    // database name ("no database/share named ..."). With no inline token,
    // MotherDuck falls back to the MOTHERDUCK_TOKEN environment variable.
    match token {
        Some(t) => format!(
            "INSTALL motherduck; LOAD motherduck; SET motherduck_token='{}'; ATTACH 'md:{}' AS {}{}; ",
            sql_escape(&t),
            sql_escape(&db),
            alias,
            mode
        ),
        None => format!("ATTACH 'md:{}' AS {}{}; ", sql_escape(&db), alias, mode),
    }
}

/// Quack remote protocol. The remote DuckDB instance runs `quack_serve(...)`
/// on port 9494 by default and exposes its database to multiple concurrent
/// clients over HTTP using a custom `application/duckdb` MIME type. Client
/// side: a SECRET carries the auth token, then ATTACH names the URL.
///
/// Quack has been a CORE extension since DuckDB 1.5.3, so the pinned 1.5.4
/// autoloads it - no INSTALL step. (This comment previously said "DuckDB
/// 2.0+", which was wrong: v2.0.0 is the planned STABLE date, not when it
/// became available.) It is still officially BETA until then, and breaking
/// protocol changes are expected.
///
/// What it is NOT: a way to distribute one query across machines. DuckDB's
/// own FAQ states Quack "does not support distributed query processing". In
/// ATTACH mode the shipped scan pushes down projection only - the filter
/// block is commented out in quack_scan.cpp - and count(*) binds to an empty
/// virtual column, so every row still crosses the wire and the CLIENT does
/// all the filtering and aggregation. Published measurements put ATTACH at
/// 2.6x/4.7x/9.5x slower than in-process at 100K/1M/10M rows. Treat it as
/// "reach a remote database", never as a scale-out lever.
pub(crate) fn quack_attach(props: &JsonValue, read_only: bool) -> String {
    let host = match string_prop(props, "host").filter(|s| !s.is_empty()) {
        Some(h) => h,
        None => return String::new(),
    };
    let port = props
        .get("port")
        .and_then(|v| v.as_u64())
        .filter(|p| *p > 0)
        .unwrap_or(9494);
    let token = string_prop(props, "token").filter(|s| !s.is_empty());

    // If the host already carries an explicit :port, respect it; otherwise
    // append the default 9494.
    let url = if host.contains(':') && !host.starts_with('[') {
        format!("quack:{}", host)
    } else {
        format!("quack:{}:{}", host, port)
    };

    let (alias, mode) = if read_only {
        ("duckle_src", " (READ_ONLY)")
    } else {
        ("duckle_dst", "")
    };

    let secret = match token {
        Some(t) => format!(
            "CREATE OR REPLACE SECRET duckle_quack_secret (TYPE QUACK, TOKEN '{}'); ",
            sql_escape(&t)
        ),
        None => String::new(),
    };

    format!("{}ATTACH '{}' AS {}{}; ", secret, sql_escape(&url), alias, mode)
}

/// Excel sink: COPY ... TO '<path>' (FORMAT 'xlsx'). The form's
/// `hasHeader` toggle becomes HEADER true/false. v1.2+ ships native
/// xlsx writer in the excel extension.
pub(crate) fn build_excel_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let header = props
        .get("hasHeader")
        .and_then(JsonValue::as_bool)
        .unwrap_or(true);
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT 'xlsx', HEADER {})",
        quote_ident(from_view),
        sql_escape(&path),
        header
    )
}

/// Iceberg sink: COPY ... TO '<path>' (FORMAT 'iceberg'). DuckDB
/// v1.5+ writes a full Iceberg table (data/ + metadata/) at the
/// given path. Read-back via src.iceberg.
pub(crate) fn build_iceberg_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT 'iceberg')",
        quote_ident(from_view),
        sql_escape(&path)
    )
}

/// Geospatial sink via the spatial extension's GDAL writer. The form's
/// `driver` picks the OGR driver (GeoJSON / GeoPackage / Shapefile /
/// KML / GPX). Most drivers expect a geometry column called `geom`.
pub(crate) fn build_spatial_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let driver = string_prop(props, "driver")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "GeoJSON".into());
    // GeoParquet does not go out through GDAL. The bundled spatial extension has no GDAL
    // Parquet driver - st_drivers() lists none, and a COPY naming one writes NO FILE AT
    // ALL without complaining, so routing it there would be a silent no-op.
    //
    // DuckDB's own Parquet writer does write GeoParquet: the footer carries the `geo`
    // key and the geometry keeps its CRS. So the option sits where it is looked for and
    // goes out through the writer that works.
    if driver.eq_ignore_ascii_case("geoparquet") || driver.eq_ignore_ascii_case("parquet") {
        return format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET)",
            quote_ident(from_view),
            sql_escape(&path)
        );
    }
    // #328: a Shapefile's .dbf carries no encoding of its own, so GDAL writes
    // the platform default and a reader has nothing to go on - Arabic place
    // names came back as `?????`. Declaring it makes GDAL write the bytes as
    // asked AND drop a `.cpg` sidecar naming the encoding, which is what makes
    // the file self-describing rather than merely correct on the machine that
    // wrote it.
    //
    // Passed as a layer-creation option because that is where a GDAL driver
    // takes it; a plain `ENCODING` on the COPY is not an option DuckDB knows.
    let encoding = string_prop(props, "encoding").filter(|s| !s.trim().is_empty());
    let layer_options = match &encoding {
        Some(e) => format!(", LAYER_CREATION_OPTIONS 'ENCODING={}'", sql_escape(e)),
        None => String::new(),
    };
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT GDAL, DRIVER '{}'{})",
        quote_ident(from_view),
        sql_escape(&path),
        sql_escape(&driver),
        layer_options
    )
}

/// SQLite / DuckDB sink - write the upstream into a table inside the
/// ATTACHed `duckle_dst` database. DROP+CREATE works for both writers
/// (the SQLite writer doesn't support CREATE OR REPLACE).
pub(crate) fn build_db_sink(
    component_id: &str,
    props: &JsonValue,
    from_view: &str,
    cols: &[String],
) -> Result<String, EngineError> {
    let table = string_prop(props, "tableName")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "output".into());
    let t = quote_ident(&table);
    let up = quote_ident(from_view);
    let mode = string_prop(props, "mode").unwrap_or_else(|| "overwrite".into());
    let keys = columns_list(props, "conflictColumns");

    // Upsert: set-based DELETE-by-key + re-INSERT (no PRIMARY KEY needed, and
    // far faster than per-row writes). DuckDB runs the query, so `* EXCLUDE`
    // and row-value IN work even when the target is an attached SQLite/DuckDB
    // file. An optional delete-flag column (deleteColumn = deleteValue) marks
    // rows to remove: their keys are deleted and they are not re-inserted -
    // this is how DuckLake CDC (change_type='delete') / cdc.diff deletes flow
    // through to the target.
    if mode == "upsert" {
        // Fail loud instead of silently overwriting (GitHub #19): without a
        // key there is nothing to match on, so "upsert" with no conflict
        // columns used to fall through to DROP TABLE + CREATE - matching the
        // relational sinks' behavior, surface a clear error instead.
        if keys.is_empty() {
            return Err(EngineError::Config(format!(
                "{}: upsert needs at least one conflict column",
                component_id
            )));
        }
        let del_col = string_prop(props, "deleteColumn").filter(|s| !s.is_empty());
        let del_val = string_prop(props, "deleteValue").unwrap_or_else(|| "delete".into());
        let sel = match &del_col {
            Some(c) => format!("* EXCLUDE ({})", quote_ident(c)),
            None => "*".to_string(),
        };
        let key_tuple = keys
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_filter = match &del_col {
            Some(c) => format!(
                " WHERE {} IS DISTINCT FROM '{}'",
                quote_ident(c),
                sql_escape(&del_val)
            ),
            None => String::new(),
        };
        return Ok(format!(
            "CREATE TABLE IF NOT EXISTS duckle_dst.{t} AS SELECT {sel} FROM {up} LIMIT 0; \
             DELETE FROM duckle_dst.{t} WHERE ({keys}) IN (SELECT {keys} FROM {up}); \
             INSERT INTO duckle_dst.{t} SELECT {sel} FROM {up}{insert_filter}",
            t = t,
            sel = sel,
            up = up,
            keys = key_tuple,
            insert_filter = insert_filter,
        ));
    }
    if mode == "merge" {
        // Partial-column upsert via DuckDB MERGE INTO (issue #39): UPDATE only
        // the columns the source carries, INSERT new rows by the source's
        // columns. Preserves target columns the source does not provide.
        let del_col = string_prop(props, "deleteColumn").filter(|s| !s.is_empty());
        let sel = match &del_col {
            Some(c) => format!("* EXCLUDE ({})", quote_ident(c)),
            None => "*".to_string(),
        };
        let create = format!(
            "CREATE TABLE IF NOT EXISTS duckle_dst.{t} AS SELECT {sel} FROM {up} LIMIT 0; ",
            t = t,
            sel = sel,
            up = up,
        );
        let merge = build_merge_stmt(
            component_id,
            &format!("duckle_dst.{}", t),
            &up,
            props,
            cols,
        )?;
        return Ok(format!("{}{}", create, merge));
    }
    if mode == "append" {
        return Ok(format!(
            "CREATE TABLE IF NOT EXISTS duckle_dst.{t} AS SELECT * FROM {up} LIMIT 0; \
             INSERT INTO duckle_dst.{t} SELECT * FROM {up}",
            t = t,
            up = up,
        ));
    }
    if mode == "truncate" {
        // Keep the existing table (and its rowids / downstream references),
        // replace just the rows. CREATE IF NOT EXISTS so a first run still
        // works against a fresh target file.
        return Ok(format!(
            "CREATE TABLE IF NOT EXISTS duckle_dst.{t} AS SELECT * FROM {up} LIMIT 0; \
             DELETE FROM duckle_dst.{t}; \
             INSERT INTO duckle_dst.{t} SELECT * FROM {up}",
            t = t,
            up = up,
        ));
    }
    if mode == "overwrite" {
        return Ok(format!(
            "DROP TABLE IF EXISTS duckle_dst.{}; CREATE TABLE duckle_dst.{} AS (SELECT * FROM {})",
            t, t, up
        ));
    }
    // Fail loud on an unrecognized mode instead of falling through to the
    // destructive DROP+CREATE above. A near-miss of a real mode (e.g. "appnd",
    // "Append", "append ") would otherwise silently wipe the target table.
    // Mirrors build_relational_sink's contract.
    Err(EngineError::Config(format!(
        "{}: write mode '{}' isn't supported (use overwrite, append, truncate, upsert, or merge)",
        component_id, mode
    )))
}

/// Avro source. The `avro` DuckDB community extension exposes
/// `read_avro` (read-only); the LOAD is in the stage prelude so the
/// function is available before the SELECT runs.
pub(crate) fn build_avro_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!("SELECT * FROM read_avro('{}')", sql_escape(&path))
}

/// Validate the text-search form and produce the spec the executor
/// uses to run the two CLI calls (stage table -> index + final query).
pub(crate) fn build_text_search_spec(node_id: &str, inputs: &NodeInputs, props: &JsonValue) -> Result<TextSearchSpec, String> {
    let upstream = inputs
        .main()
        .ok_or_else(|| missing_input_msg("xf.ai.text_search"))?;
    let id_col = string_prop(props, "idColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Search needs an id column (unique per row)".to_string())?;
    let text_cols = columns_list(props, "textColumns");
    if text_cols.is_empty() {
        return Err("Text Search needs at least one text column to index".to_string());
    }
    let query = string_prop(props, "query")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Search needs a query string".to_string())?;
    let top_k = props
        .get("topK")
        .and_then(|v| v.as_u64())
        .filter(|k| *k > 0);
    let output_col = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "score".into());
    let suffix: String = node_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let staging_table = format!("_fts_{}", suffix);
    Ok(TextSearchSpec {
        from_view: upstream.to_string(),
        id_col,
        text_cols,
        query,
        top_k,
        output_col,
        staging_table,
    })
}

/// Spatial Distance: add a column with the distance from each row's
/// geometry to a fixed target point (WKT). Uses the spatial extension's
/// CRS-aware spatial measurement (issue #177): pick the planar or the
/// spheroidal DuckDB function based on the geometry's Coordinate Reference
/// System, and stop with an informative error when the geometry has no CRS.
///
/// The CRS lives in the column *type* (`GEOMETRY('EPSG:4326')`), not in the
/// value: `CAST(g AS GEOMETRY)` and `ST_GeomFromText(...)` both strip it, and
/// `ST_CRS(VARCHAR)` is a bind error. So the CRS name is read from `typeof(col)`
/// (bind-safe for GEOMETRY, plain GEOMETRY and VARCHAR alike) and matched
/// against `duckdb_coordinate_systems()` to resolve the unit. `degree` selects
/// the spheroidal function, any other linear unit selects the planar one, and a
/// missing/unresolved CRS raises `error(...)` at the first row (empty input is
/// left untouched). The measurement itself runs over `CAST(col AS GEOMETRY)`,
/// which only needs coordinates, not the CRS.
///
/// `arg_tail` is appended inside the function call after the cast geometry -
/// `""` for the single-argument measurements, `, ST_GeomFromText('...')` for
/// Distance.
fn crs_aware_measure(
    upstream: &str,
    geom_column: &str,
    output: &str,
    planar_fn: &str,
    spheroid_fn: &str,
    arg_tail: &str,
    label: &str,
) -> String {
    let col = quote_ident(geom_column);
    let out = quote_ident(output);
    let up = quote_ident(upstream);
    format!(
        "WITH __geo_cu AS (\
           SELECT lower(json_extract_string(projjson, '$.coordinate_system.axis[0].unit')) AS unit \
           FROM duckdb_coordinate_systems() \
           WHERE crs_name = regexp_extract(typeof((SELECT {col} FROM {up} LIMIT 1)), 'GEOMETRY\\(''([^'']*)''\\)', 1)\
         ) \
         SELECT __g.*, CASE \
           WHEN __u.unit IS NULL THEN error('{label}: input geometry does not have a valid Coordinate Reference System (CRS). Assign a CRS (e.g. load from GeoParquet/Shapefile) before performing spatial measurements.') \
           WHEN __u.unit = 'degree' THEN {sph}(CAST(__g.{col} AS GEOMETRY){tail}) \
           ELSE {pla}(CAST(__g.{col} AS GEOMETRY){tail}) END AS {out} \
         FROM {up} __g LEFT JOIN __geo_cu __u ON TRUE",
        col = col,
        up = up,
        label = label,
        sph = spheroid_fn,
        pla = planar_fn,
        tail = arg_tail,
        out = out,
    )
}

/// CRS-aware Distance to a fixed target geometry (issue #177). Chooses
/// ST_Distance (planar) or ST_Distance_Spheroid (geographic) from the input's
/// CRS unit.
pub(crate) fn build_geo_distance(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.distance"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Geo Distance needs a geometry column".to_string())?;
    let target = string_prop(props, "targetWkt")
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Geo Distance needs a target geometry (WKT, e.g. 'POINT(0 0)')".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "distance".into());
    let tail = format!(", ST_GeomFromText('{}')", target.replace('\'', "''"));
    Ok(crs_aware_measure(
        upstream,
        &column,
        &output,
        "ST_Distance",
        "ST_Distance_Spheroid",
        &tail,
        "xf.geo.distance",
    ))
}

/// CRS-aware Length of each (multi)linestring (issue #177): ST_Length /
/// ST_Length_Spheroid picked from the input's CRS unit.
pub(crate) fn build_geo_length(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.length"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Geo Length needs a geometry column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "length".into());
    Ok(crs_aware_measure(
        upstream,
        &column,
        &output,
        "ST_Length",
        "ST_Length_Spheroid",
        "",
        "xf.geo.length",
    ))
}

/// CRS-aware Perimeter of each (multi)polygon (issue #177): ST_Perimeter /
/// ST_Perimeter_Spheroid picked from the input's CRS unit.
pub(crate) fn build_geo_perimeter(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.perimeter"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Geo Perimeter needs a geometry column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "perimeter".into());
    Ok(crs_aware_measure(
        upstream,
        &column,
        &output,
        "ST_Perimeter",
        "ST_Perimeter_Spheroid",
        "",
        "xf.geo.perimeter",
    ))
}

/// CRS-aware Area of each (multi)polygon (issue #177): ST_Area /
/// ST_Area_Spheroid picked from the input's CRS unit.
pub(crate) fn build_geo_area(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.area"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Geo Area needs a geometry column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "area".into());
    Ok(crs_aware_measure(
        upstream,
        &column,
        &output,
        "ST_Area",
        "ST_Area_Spheroid",
        "",
        "xf.geo.area",
    ))
}

/// Spatial Buffer: add a column with ST_Buffer(geom, distance) - the
/// area within `distance` of each row's geometry.
pub(crate) fn build_geo_buffer(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.buffer"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Geo Buffer needs a geometry column".to_string())?;
    let distance = props
        .get("distance")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Geo Buffer needs a distance".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "buffer".into());
    Ok(format!(
        "SELECT *, ST_Buffer(CAST({col} AS GEOMETRY), {distance}) AS {out} FROM {up}",
        col = quote_ident(&column),
        distance = distance,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Flip Coordinates (issue #178): swap the X/Y of every vertex in the geometry
/// column (fixes lat,lon data stored as lon,lat and vice versa) via
/// `ST_FlipCoordinates`, replacing the geometry in place with `SELECT * REPLACE`
/// so all other attributes pass through untouched. CAST accepts a native
/// GEOMETRY column (no-op) or a VARCHAR WKT column (parsed).
pub(crate) fn build_geo_flip(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.flip"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Flip Coordinates needs a geometry column".to_string())?;
    let col = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (ST_FlipCoordinates(CAST({col} AS GEOMETRY)) AS {col}) FROM {up}",
        col = col,
        up = quote_ident(upstream)
    ))
}

/// Define Projection (issue #188): assign a CRS to a geometry that has missing
/// or unknown CRS metadata, WITHOUT touching the coordinates, via `ST_SetCRS`.
/// Replaces the geometry in place with `SELECT * REPLACE` so every other column
/// passes through. CAST accepts a native GEOMETRY (no-op) or a VARCHAR WKT
/// column (parsed).
pub(crate) fn build_geo_setcrs(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.setcrs"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Define Projection needs a geometry column".to_string())?;
    let crs = string_prop(props, "crs")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Define Projection needs a coordinate reference system".to_string())?;
    let col = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (ST_SetCRS(CAST({col} AS GEOMETRY), '{crs}') AS {col}) FROM {up}",
        col = col,
        crs = sql_escape(&crs),
        up = quote_ident(upstream)
    ))
}

/// Reproject Geometry (issue #189): reproject a geometry column from a source
/// CRS to a target CRS via `ST_Transform`, replacing it in place. The source CRS
/// is pinned with `ST_SetCRS` first so the transform is well-defined even when
/// the input carries no CRS metadata. `always_xy` (default true) forces
/// lon/lat axis order, matching the GeoParquet / de-facto standard. Rejects a
/// no-op reprojection where source and target are identical.
pub(crate) fn build_geo_reproject(
    inputs: &NodeInputs,
    props: &JsonValue,
) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.reproject"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Reproject Geometry needs a geometry column".to_string())?;
    let source_crs = string_prop(props, "sourceCrs")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Reproject Geometry needs a current (source) CRS".to_string())?;
    let target_crs = string_prop(props, "targetCrs")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Reproject Geometry needs a new (target) CRS".to_string())?;
    if source_crs == target_crs {
        return Err(format!(
            "Reproject Geometry: the current and new CRS are both '{}' - nothing to reproject",
            source_crs
        ));
    }
    let always_xy = props
        .get("alwaysXy")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let col = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (ST_Transform(ST_SetCRS(CAST({col} AS GEOMETRY), '{src}'), '{tgt}', always_xy := {xy}) AS {col}) FROM {up}",
        col = col,
        src = sql_escape(&source_crs),
        tgt = sql_escape(&target_crs),
        xy = always_xy,
        up = quote_ident(upstream)
    ))
}

/// Create Geometry (issue #190): build a native GEOMETRY column from X/Y
/// coordinate columns, a WKT text column, or a WKB binary column, so downstream
/// geospatial transforms can use it. `ST_Point` / `ST_GeomFromText` /
/// `ST_GeomFromWKB` produce the geometry; an optional CRS is stamped with
/// `ST_SetCRS`. When `removeSource` is on (default) the input column(s) are
/// dropped with `* EXCLUDE`, otherwise they are kept alongside the new column.
pub(crate) fn build_geo_create(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.create"))?;
    let source = string_prop(props, "source")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "xy".into());
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "geom".into());
    let crs = string_prop(props, "crs").filter(|s| !s.is_empty());
    let remove = props
        .get("removeSource")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    // Geometry expression + the source column(s) it consumes (for EXCLUDE).
    let (geom_expr, source_cols): (String, Vec<String>) = match source.as_str() {
        "xy" => {
            let x = string_prop(props, "xColumn")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Create Geometry (X/Y) needs an X column".to_string())?;
            let y = string_prop(props, "yColumn")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Create Geometry (X/Y) needs a Y column".to_string())?;
            (
                format!(
                    "ST_Point(CAST({} AS DOUBLE), CAST({} AS DOUBLE))",
                    quote_ident(&x),
                    quote_ident(&y)
                ),
                vec![x, y],
            )
        }
        "wkt" => {
            let w = string_prop(props, "wktColumn")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Create Geometry (WKT) needs a WKT column".to_string())?;
            (
                format!("ST_GeomFromText(CAST({} AS VARCHAR))", quote_ident(&w)),
                vec![w],
            )
        }
        "wkb" => {
            let w = string_prop(props, "wkbColumn")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "Create Geometry (WKB) needs a WKB column".to_string())?;
            (format!("ST_GeomFromWKB({})", quote_ident(&w)), vec![w])
        }
        other => {
            return Err(format!(
                "Create Geometry: source must be xy, wkt, or wkb (got '{}')",
                other
            ))
        }
    };
    // Stamp the CRS when the user set one.
    let geom_expr = match &crs {
        Some(c) => format!("ST_SetCRS({}, '{}')", geom_expr, sql_escape(c)),
        None => geom_expr,
    };
    let out = quote_ident(&output);
    let up = quote_ident(upstream);
    if remove {
        let exclude = source_cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "SELECT * EXCLUDE ({exclude}), {geom_expr} AS {out} FROM {up}"
        ))
    } else {
        Ok(format!("SELECT *, {geom_expr} AS {out} FROM {up}"))
    }
}

/// Base64: encode a column to base64 text, or decode a base64 text
/// column back to bytes (returned as VARCHAR for downstream
/// compatibility - the actual underlying type is BLOB).
pub(crate) fn build_base64(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.base64"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Base64 needs a column".to_string())?;
    let mode = string_prop(props, "mode").unwrap_or_else(|| "encode".into());
    let qcol = quote_ident(&column);
    // Use encode()/decode() for the VARCHAR<->BLOB bridge, NOT CAST. CAST
    // VARCHAR->BLOB hard-errors on any non-ASCII byte ("Invalid byte ... All
    // non-ascii characters must be escaped"), crashing the whole run; and
    // CAST BLOB->VARCHAR hex-escapes non-ASCII bytes ("caf\xC3\xA9"),
    // silently corrupting decoded UTF-8. encode() does a clean UTF-8
    // VARCHAR->BLOB and decode() a clean BLOB->VARCHAR.
    let expr = if mode == "decode" {
        format!("decode(from_base64(CAST({} AS VARCHAR)))", qcol)
    } else {
        format!("base64(encode({}))", qcol)
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", column, mode));
    Ok(format!(
        "SELECT *, {expr} AS {out} FROM {up}",
        expr = expr,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Z-Score: per-row standardized value computed against the whole
/// input via window aggregates. (value - mean) / stddev_samp. Useful
/// for outlier detection and feature scaling. Single SQL pass; no
/// extra stage. If stddev is 0 (all values equal), the result is NULL
/// rather than divide-by-zero.
pub(crate) fn build_zscore(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.num.zscore"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Z-Score needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_zscore", column));
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT *, CASE WHEN stddev_samp(CAST({col} AS DOUBLE)) OVER () = 0 THEN NULL ELSE (CAST({col} AS DOUBLE) - avg(CAST({col} AS DOUBLE)) OVER ()) / stddev_samp(CAST({col} AS DOUBLE)) OVER () END AS {out} FROM {up}",
        col = qcol,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Literal Replace: DuckDB replace(string, search, replacement).
/// Different from xf.regex - this is a literal substring swap, no
/// regex metacharacters.
pub(crate) fn build_text_replace(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.replace"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Replace needs a column".to_string())?;
    let search = string_prop(props, "search")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Replace needs a search string".to_string())?;
    let replacement = string_prop(props, "replacement").unwrap_or_default();
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| column.clone());
    let qcol = quote_ident(&column);
    let expr = format!(
        "replace(CAST({} AS VARCHAR), '{}', '{}')",
        qcol,
        sql_escape(&search),
        sql_escape(&replacement)
    );
    if output == column {
        Ok(format!(
            "SELECT * REPLACE ({} AS {}) FROM {}",
            expr,
            qcol,
            quote_ident(upstream)
        ))
    } else {
        Ok(format!(
            "SELECT *, {} AS {} FROM {}",
            expr,
            quote_ident(&output),
            quote_ident(upstream)
        ))
    }
}

/// URL Slug: lowercase + strip non-alphanumerics + collapse runs of
/// whitespace into single hyphens + trim leading/trailing hyphens.
/// "Hello, World!" -> "hello-world".
pub(crate) fn build_text_slug(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.slug"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Slug needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_slug", column));
    let qcol = quote_ident(&column);
    // Lower, replace any run of non-alphanumerics with a single hyphen,
    // then trim leading/trailing hyphens.
    let expr = format!(
        "trim(regexp_replace(lower(CAST({} AS VARCHAR)), '[^a-z0-9]+', '-', 'g'), '-')",
        qcol
    );
    Ok(format!(
        "SELECT *, {} AS {} FROM {}",
        expr,
        quote_ident(&output),
        quote_ident(upstream)
    ))
}

/// Strip HTML: remove all <...> tag spans via regex. Leaves the text
/// content. Standard newsletter / scrape-cleanup helper.
pub(crate) fn build_text_strip_html(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.strip_html"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Strip HTML needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| column.clone());
    let qcol = quote_ident(&column);
    let expr = format!(
        "regexp_replace(CAST({} AS VARCHAR), '<[^>]+>', '', 'g')",
        qcol
    );
    if output == column {
        Ok(format!(
            "SELECT * REPLACE ({} AS {}) FROM {}",
            expr,
            qcol,
            quote_ident(upstream)
        ))
    } else {
        Ok(format!(
            "SELECT *, {} AS {} FROM {}",
            expr,
            quote_ident(&output),
            quote_ident(upstream)
        ))
    }
}

/// Text Reverse: reverse the characters in a string column.
/// DuckDB reverse() function.
pub(crate) fn build_text_reverse(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.reverse"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Reverse needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_reversed", column));
    Ok(format!(
        "SELECT *, reverse(CAST({col} AS VARCHAR)) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Text Repeat: repeat a string column N times via DuckDB repeat().
pub(crate) fn build_text_repeat(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.repeat"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Repeat needs a column".to_string())?;
    let count = props
        .get("count")
        .and_then(|v| v.as_i64())
        .filter(|n| *n >= 0)
        .unwrap_or(2);
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_repeated", column));
    Ok(format!(
        "SELECT *, repeat(CAST({col} AS VARCHAR), {n}) AS {out} FROM {up}",
        col = quote_ident(&column),
        n = count,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Compare: produce a boolean column from a comparison of two
/// upstream columns. op = eq / neq / lt / le / gt / ge. Useful for
/// flagging mismatches between expected/actual columns.
pub(crate) fn build_compare(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.compare"))?;
    let left = string_prop(props, "leftColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Compare needs a left column".to_string())?;
    let right = string_prop(props, "rightColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Compare needs a right column".to_string())?;
    let op = string_prop(props, "op").unwrap_or_else(|| "eq".into());
    let sql_op = match op.as_str() {
        "neq" => "!=",
        "lt" => "<",
        "le" => "<=",
        "gt" => ">",
        "ge" => ">=",
        _ => "=",
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}_{}", left, op, right));
    Ok(format!(
        "SELECT *, ({} {} {}) AS {} FROM {}",
        quote_ident(&left),
        sql_op,
        quote_ident(&right),
        quote_ident(&output),
        quote_ident(upstream)
    ))
}

/// Text Match: boolean substring / prefix / suffix predicate via
/// DuckDB's contains / starts_with / ends_with. Adds a boolean
/// column - pair with Filter Rows downstream to keep only matches.
pub(crate) fn build_text_match(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.match"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Match needs a column".to_string())?;
    let needle = string_prop(props, "needle")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Match needs a search term".to_string())?;
    let mode = string_prop(props, "mode").unwrap_or_else(|| "contains".into());
    let fn_name = match mode.as_str() {
        "starts_with" => "starts_with",
        "ends_with" => "ends_with",
        _ => "contains",
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", column, mode));
    Ok(format!(
        "SELECT *, {fn}(CAST({col} AS VARCHAR), '{n}') AS {out} FROM {up}",
        fn = fn_name,
        col = quote_ident(&column),
        n = sql_escape(&needle),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Sign: -1 for negative, 0 for zero, +1 for positive. DuckDB's
/// sign() function on a DOUBLE input.
pub(crate) fn build_sign(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.num.sign"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Sign needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_sign", column));
    Ok(format!(
        "SELECT *, sign(CAST({col} AS DOUBLE)) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Clamp: clip numeric values to a [low, high] range via LEAST +
/// GREATEST. Values below low become low; above high become high.
/// Useful for capping outliers before downstream stats.
pub(crate) fn build_clamp(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.num.clamp"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Clamp needs a column".to_string())?;
    let low = props
        .get("low")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Clamp needs a low bound".to_string())?;
    let high = props
        .get("high")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Clamp needs a high bound".to_string())?;
    if high < low {
        return Err("Clamp needs high >= low".to_string());
    }
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (LEAST(GREATEST(CAST({col} AS DOUBLE), {low}), {high}) AS {col}) FROM {up}",
        col = qcol,
        low = low,
        high = high,
        up = quote_ident(upstream)
    ))
}

/// String Padding: pad a string column to a fixed length on the left
/// or right with a fill character. Default fills with space, mode
/// 'left' (lpad) is the classic 'zero-pad numeric IDs' pattern.
pub(crate) fn build_padding(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.padding"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Padding needs a column".to_string())?;
    let length = props
        .get("length")
        .and_then(|v| v.as_i64())
        .filter(|n| *n > 0)
        .ok_or_else(|| "Padding needs a positive target length".to_string())?;
    let fill = string_prop(props, "fill")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| " ".into());
    let side = string_prop(props, "side").unwrap_or_else(|| "left".into());
    let fn_name = if side == "right" { "rpad" } else { "lpad" };
    let qcol = quote_ident(&column);
    let fill_escaped = sql_escape(&fill);
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| column.clone());
    if output == column {
        Ok(format!(
            "SELECT * REPLACE ({fn}(CAST({col} AS VARCHAR), {n}, '{f}') AS {col}) FROM {up}",
            fn = fn_name,
            col = qcol,
            n = length,
            f = fill_escaped,
            up = quote_ident(upstream)
        ))
    } else {
        Ok(format!(
            "SELECT *, {fn}(CAST({col} AS VARCHAR), {n}, '{f}') AS {out} FROM {up}",
            fn = fn_name,
            col = qcol,
            n = length,
            f = fill_escaped,
            out = quote_ident(&output),
            up = quote_ident(upstream)
        ))
    }
}

/// Date/Time Epoch: convert a TIMESTAMP column to Unix epoch seconds
/// (mode 'to') or epoch seconds back to TIMESTAMP (mode 'from').
/// Both directions use DuckDB core functions, no extension needed.
pub(crate) fn build_dt_epoch(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.epoch"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Epoch needs a column".to_string())?;
    let mode = string_prop(props, "mode").unwrap_or_else(|| "to".into());
    let qcol = quote_ident(&column);
    let expr = if mode == "from" {
        // Stay in pure TIMESTAMP space - to_timestamp() returns
        // TIMESTAMPTZ which round-trips wrong on non-UTC sessions.
        format!(
            "(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL '1 second' * CAST({} AS BIGINT))",
            qcol
        )
    } else {
        format!("epoch(CAST({} AS TIMESTAMP))", qcol)
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            if mode == "from" {
                format!("{}_timestamp", column)
            } else {
                format!("{}_epoch", column)
            }
        });
    Ok(format!(
        "SELECT *, {expr} AS {out} FROM {up}",
        expr = expr,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Current Timestamp: add a column holding the time at which the
/// pipeline runs - the standard 'loaded_at' / 'processed_at' /
/// 'ingested_at' stamp every ETL output usually carries. Cast to
/// plain TIMESTAMP - current_timestamp returns TIMESTAMPTZ which
/// serializes with a session-timezone offset and confuses
/// downstream readers.
pub(crate) fn build_dt_now(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.now"))?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "loaded_at".into());
    Ok(format!(
        "SELECT *, CAST(current_timestamp AS TIMESTAMP) AS {out} FROM {up}",
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// UUID: add a freshly-generated UUID v4 to every row. Standard
/// 'surrogate row id' pattern, especially handy before upserts into
/// systems that need a non-business primary key.
pub(crate) fn build_uuid(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.uuid"))?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "row_id".into());
    Ok(format!(
        "SELECT *, uuid() AS {out} FROM {up}",
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Cumulative: running aggregate over an ordered window
/// (sum / avg / count / min / max), optionally per-group. Classic
/// reporting pattern - 'running total of sales', 'cumulative count
/// of users per region'. Uses the standard ROWS BETWEEN UNBOUNDED
/// PRECEDING AND CURRENT ROW frame so the value at each row reflects
/// everything seen so far in scan order.
pub(crate) fn build_cumulative(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.cumulative"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Cumulative needs a column".to_string())?;
    let order_col = string_prop(props, "orderBy")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Cumulative needs an orderBy column".to_string())?;
    let func = string_prop(props, "function").unwrap_or_else(|| "sum".into()).to_lowercase();
    let fn_name = match func.as_str() {
        "avg" => "avg",
        "count" => "count",
        "min" => "min",
        "max" => "max",
        _ => "sum",
    };
    let partition: Vec<String> = columns_from_props(props, "partitionBy").unwrap_or_default();
    let partition_clause = if partition.is_empty() {
        String::new()
    } else {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("PARTITION BY {} ", cols)
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_running_{}", column, fn_name));
    Ok(format!(
        "SELECT *, {fn}({col}) OVER ({part}ORDER BY {ord} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {out} FROM {up}",
        fn = fn_name,
        col = quote_ident(&column),
        part = partition_clause,
        ord = quote_ident(&order_col),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Time Bin: round a timestamp column down to the nearest multiple of
/// the chosen interval (e.g. 5-minute, 1-hour, 1-day buckets) for
/// time-series grouping. Done via epoch math so any (unit, count)
/// combination works, not just the standard date_trunc units.
pub(crate) fn build_dt_bin(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.dt.bin"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Time Bin needs a timestamp column".to_string())?;
    let unit = string_prop(props, "unit").unwrap_or_else(|| "minute".into());
    let count = props
        .get("count")
        .and_then(|v| v.as_i64())
        .filter(|n| *n > 0)
        .unwrap_or(5);
    let seconds_per = match unit.to_lowercase().as_str() {
        "second" | "seconds" => 1_i64,
        "minute" | "minutes" => 60,
        "hour" | "hours" => 3_600,
        "day" | "days" => 86_400,
        _ => 60,
    };
    let bucket_seconds = seconds_per * count;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_bin", column));
    let qcol = quote_ident(&column);
    // Subtract the timestamp's remainder seconds past its bucket boundary.
    // Stays inside the TIMESTAMP type the whole way - to_timestamp() would
    // return TIMESTAMPTZ which then serializes with a timezone offset and
    // round-trips wrong on non-UTC session timezones (tests failed on IST).
    Ok(format!(
        "SELECT *, CAST({col} AS TIMESTAMP) - (INTERVAL '1 second' * (((CAST(epoch(CAST({col} AS TIMESTAMP)) AS BIGINT) % {bucket}) + {bucket}) % {bucket})) AS {out} FROM {up}",
        col = qcol,
        bucket = bucket_seconds,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Array Length: scalar length of an array / list column.
pub(crate) fn build_arr_length(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.arr.length"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Array Length needs a column".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_length", column));
    Ok(format!(
        "SELECT *, length({col}) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Rank Filter: keep the top N rows per group, ordered by a column.
/// Common reporting pattern: 'top 3 spenders per region', 'most
/// recent 5 orders per customer'. Computes ROW_NUMBER over the
/// (partitionBy, orderBy DESC|ASC) window in a subquery, then
/// WHERE filters to rank <= N. desc defaults to true (top N).
pub(crate) fn build_rank_filter(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.rank.filter"))?;
    let order_col = string_prop(props, "orderBy")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Rank Filter needs an orderBy column".to_string())?;
    let partition: Vec<String> = columns_from_props(props, "partitionBy").unwrap_or_default();
    let n = props
        .get("n")
        .and_then(|v| v.as_i64())
        .filter(|n| *n > 0)
        .unwrap_or(10);
    // The UI's Direction select stores "true"/"false" as a STRING, so reading
    // only as a JSON bool ignored the user's choice (always DESC). Accept both.
    let desc = props
        .get("desc")
        .and_then(|v| v.as_bool())
        .or_else(|| {
            props
                .get("desc")
                .and_then(|v| v.as_str())
                .map(|s| !s.eq_ignore_ascii_case("false") && s != "0")
        })
        .unwrap_or(true);
    let direction = if desc { "DESC" } else { "ASC" };
    let partition_clause = if partition.is_empty() {
        String::new()
    } else {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("PARTITION BY {} ", cols)
    };
    Ok(format!(
        "SELECT * EXCLUDE (_duckle_rank) FROM (SELECT u.*, row_number() OVER ({part}ORDER BY {ord} {dir}) AS _duckle_rank FROM {up} u) WHERE _duckle_rank <= {n}",
        part = partition_clause,
        ord = quote_ident(&order_col),
        dir = direction,
        n = n,
        up = quote_ident(upstream)
    ))
}

/// Forward-fill: replace NULL values with the most recent non-null
/// value within a group, ordered by a sort column. The classic
/// time-series gap-fill: missing readings get the previous reading.
/// Uses last_value(col IGNORE NULLS) over an unbounded preceding
/// window - DuckDB evaluates this in one pass.
pub(crate) fn build_fill_forward(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.fill_forward"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Forward Fill needs a column".to_string())?;
    let order_col = string_prop(props, "orderBy")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Forward Fill needs an orderBy column".to_string())?;
    let partition: Vec<String> = columns_from_props(props, "partitionBy").unwrap_or_default();
    let partition_clause = if partition.is_empty() {
        String::new()
    } else {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("PARTITION BY {} ", cols)
    };
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (last_value({col} IGNORE NULLS) OVER ({part}ORDER BY {ord} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {col}) FROM {up}",
        col = qcol,
        part = partition_clause,
        ord = quote_ident(&order_col),
        up = quote_ident(upstream)
    ))
}

/// Row hash: append a stable fingerprint column computed over N
/// other columns. The classic CDC primitive - hash a tuple's
/// content so downstream you can answer "did this row's value
/// change?" without comparing every column.
///
/// SQL: SELECT *, {algo}(concat_ws('||', col1::VARCHAR, col2::VARCHAR, ...)) AS _row_hash
///
/// Concat separator is '||' (a pipe sequence that won't appear in
/// typical data and that keeps multi-column distinguishable - "a"
/// + "bc" != "ab" + "c" when the boundary marker is present).
/// NULLs are coerced to the empty string via concat_ws's default
/// NULL-skipping, which means rows with the same non-null values
/// hash equal regardless of which optional fields were missing -
/// usually what you want for change detection.
pub(crate) fn build_row_hash(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.row_hash"))?;
    let cols: Vec<String> = columns_from_props(props, "columns").unwrap_or_default();
    if cols.is_empty() {
        return Err("Row Hash needs at least one column".to_string());
    }
    let algo = string_prop(props, "algorithm")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "md5".into());
    let algo_fn = match algo.as_str() {
        "md5" => "md5",
        "sha1" => "sha1",
        "sha256" => "sha256",
        other => return Err(format!("Row Hash: unknown algorithm '{}'", other)),
    };
    let out = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "_row_hash".into());
    let parts = cols
        .iter()
        .map(|c| format!("CAST({} AS VARCHAR)", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT *, {algo}(concat_ws('||', {parts})) AS {out} FROM {up}",
        algo = algo_fn,
        parts = parts,
        out = quote_ident(&out),
        up = quote_ident(upstream)
    ))
}

/// Audit columns: stamp every row with provenance + load metadata.
/// The classic warehouse pattern - downstream you can answer "when
/// did this row land?", "from which pipeline?", "which batch?"
/// without joining back to a runs table.
///
/// All four columns are independently toggleable. Strings (`source`,
/// `batchId`) are emitted as literals so context variables resolve
/// at compile time. Use Duckle's `{{ context.foo }}` interpolation
/// in the form to wire a per-run batch ID.
pub(crate) fn build_audit(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.audit"))?;
    let mut adds: Vec<String> = Vec::new();
    let loaded_at = props.get("loadedAt").and_then(JsonValue::as_bool).unwrap_or(true);
    if loaded_at {
        adds.push("current_timestamp AS _loaded_at".to_string());
    }
    if props.get("loadedDate").and_then(JsonValue::as_bool).unwrap_or(false) {
        adds.push("current_date AS _loaded_date".to_string());
    }
    if let Some(s) = string_prop(props, "source").filter(|s| !s.is_empty()) {
        adds.push(format!("'{}' AS _source", sql_escape(&s)));
    }
    if let Some(b) = string_prop(props, "batchId").filter(|s| !s.is_empty()) {
        adds.push(format!("'{}' AS _batch_id", sql_escape(&b)));
    }
    if adds.is_empty() {
        return Err("Audit: enable at least one audit column".to_string());
    }
    Ok(format!(
        "SELECT *, {extra} FROM {up}",
        extra = adds.join(", "),
        up = quote_ident(upstream)
    ))
}

/// Constant-fill: replace NULLs in a column with a user-supplied
/// literal. Rounds out the fill family (forward / backward / constant).
/// String literals are auto-quoted so the user types `unknown`, not
/// `'unknown'`. A value that parses as a finite number passes through
/// raw - lets the same prop handle numeric columns without making the
/// user know SQL quoting rules. The COALESCE expression takes the
/// column's type from the column itself, so numeric vs text doesn't
/// need a separate type hint.
pub(crate) fn build_fill_constant(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.fill_constant"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Fill Constant needs a column".to_string())?;
    // Accept either a string `value` (most common) or a number.
    let literal = match props.get("value") {
        Some(JsonValue::String(s)) => {
            let trimmed = s.trim();
            // If the user typed a bare FINITE number (e.g. `0`, `-1.5`),
            // pass it through unquoted so DuckDB sees a numeric literal.
            // Otherwise quote it as a string. The is_finite guard matters:
            // Rust's f64 parse also accepts "inf"/"nan"/"infinity"/"1e999",
            // which are not valid DuckDB numeric tokens and would make the
            // COALESCE fail - those are almost certainly intended as the
            // literal string fill value.
            match trimmed.parse::<f64>() {
                Ok(n) if n.is_finite() => trimmed.to_string(),
                _ => format!("'{}'", sql_escape(trimmed)),
            }
        }
        Some(JsonValue::Number(n)) => n.to_string(),
        Some(JsonValue::Bool(b)) => b.to_string(),
        _ => return Err("Fill Constant needs a value".to_string()),
    };
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (COALESCE({col}, {lit}) AS {col}) FROM {up}",
        col = qcol,
        lit = literal,
        up = quote_ident(upstream)
    ))
}

/// Backward-fill: replace NULL values with the next non-null value
/// within a group, ordered by a sort column. Pandas-style bfill /
/// "fill up" - useful when the first readings of a series are missing
/// and you'd rather impute from the future than leave them null.
/// Uses first_value(col IGNORE NULLS) over an unbounded following
/// window so the current row sees the nearest non-null ahead of it.
pub(crate) fn build_fill_backward(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.fill_backward"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Backward Fill needs a column".to_string())?;
    let order_col = string_prop(props, "orderBy")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Backward Fill needs an orderBy column".to_string())?;
    let partition: Vec<String> = columns_from_props(props, "partitionBy").unwrap_or_default();
    let partition_clause = if partition.is_empty() {
        String::new()
    } else {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!("PARTITION BY {} ", cols)
    };
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT * REPLACE (first_value({col} IGNORE NULLS) OVER ({part}ORDER BY {ord} ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS {col}) FROM {up}",
        col = qcol,
        part = partition_clause,
        ord = quote_ident(&order_col),
        up = quote_ident(upstream)
    ))
}

/// Numeric Bucketize: bin a numeric column into N equal-width
/// buckets between low and high. Output is 1..N for in-range values,
/// 0 for below-low, N+1 for above-high (PostgreSQL width_bucket
/// semantics). DuckDB core doesn't ship width_bucket as a scalar
/// function (only the Postgres extension defines it), so we expand
/// to the explicit floor((v - low) / step) + 1 form, which works on
/// every DuckDB build.
pub(crate) fn build_bucketize(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.num.bucketize"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Bucketize needs a column".to_string())?;
    // Labeled mode: explicit ascending breakpoints (a JSON array or a
    // comma-separated string) make N+1 half-open buckets with human-readable
    // labels ("<b0", "b0-b1", ..., ">=bN-1"). Optional `labels` (N+1 long)
    // override the auto range labels. NULL maps to a NULL bucket. This is the
    // cohorting form; without `bounds` the equal-width numeric mode below runs.
    let bounds: Vec<f64> = match props.get("bounds") {
        Some(JsonValue::Array(a)) => a
            .iter()
            .filter_map(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.trim().parse::<f64>().ok())))
            .collect(),
        Some(JsonValue::String(s)) => s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.parse::<f64>().ok())
            .collect(),
        _ => Vec::new(),
    };
    if !bounds.is_empty() {
        let qcol = quote_ident(&column);
        let output = string_prop(props, "outputColumn")
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("{}_bucket", column));
        let labels: Vec<String> = match props.get("labels") {
            Some(JsonValue::Array(a)) => a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect(),
            Some(JsonValue::String(s)) => s.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            _ => Vec::new(),
        };
        if !labels.is_empty() && labels.len() != bounds.len() + 1 {
            return Err(format!(
                "Bucketize: labels must have exactly {} entries (one more than the {} breakpoints)",
                bounds.len() + 1,
                bounds.len()
            ));
        }
        let g = |n: f64| -> String {
            // Render a breakpoint without a trailing .0 for whole numbers.
            if n.fract() == 0.0 { format!("{}", n as i64) } else { format!("{}", n) }
        };
        let label_for = |i: usize, auto: String| -> String {
            if labels.len() == bounds.len() + 1 {
                format!("'{}'", sql_escape(&labels[i]))
            } else {
                format!("'{}'", sql_escape(&auto))
            }
        };
        let mut whens: Vec<String> = Vec::new();
        whens.push(format!(
            "WHEN CAST({col} AS DOUBLE) < {b} THEN {lbl}",
            col = qcol, b = g(bounds[0]), lbl = label_for(0, format!("<{}", g(bounds[0])))
        ));
        for i in 1..bounds.len() {
            whens.push(format!(
                "WHEN CAST({col} AS DOUBLE) < {b} THEN {lbl}",
                col = qcol, b = g(bounds[i]), lbl = label_for(i, format!("{}-{}", g(bounds[i - 1]), g(bounds[i])))
            ));
        }
        let last = label_for(bounds.len(), format!(">={}", g(bounds[bounds.len() - 1])));
        return Ok(format!(
            "SELECT *, CASE WHEN {col} IS NULL THEN NULL {whens} ELSE {last} END AS {out} FROM {up}",
            col = qcol, whens = whens.join(" "), last = last, out = quote_ident(&output), up = quote_ident(upstream)
        ));
    }
    let low = props
        .get("low")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Bucketize needs a low bound".to_string())?;
    let high = props
        .get("high")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "Bucketize needs a high bound".to_string())?;
    if high <= low {
        return Err("Bucketize needs high > low".to_string());
    }
    let buckets = props
        .get("buckets")
        .and_then(|v| v.as_i64())
        .filter(|n| *n > 0)
        .unwrap_or(10);
    let step = (high - low) / buckets as f64;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_bucket", column));
    let qcol = quote_ident(&column);
    Ok(format!(
        "SELECT *, CASE WHEN CAST({col} AS DOUBLE) < {low} THEN 0 WHEN CAST({col} AS DOUBLE) >= {high} THEN {overflow} ELSE CAST(floor((CAST({col} AS DOUBLE) - {low}) / {step}) AS INTEGER) + 1 END AS {out} FROM {up}",
        col = qcol,
        low = low,
        high = high,
        step = step,
        overflow = buckets + 1,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// JSON Array Agg: collapse multiple rows into a JSON array per group
/// via json_group_array. With no groupBy, produces one row with the
/// whole input as a single array.
pub(crate) fn build_json_array_agg(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.json.array_agg"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "JSON Array Agg needs a column".to_string())?;
    let group_by: Vec<String> = columns_from_props(props, "groupBy").unwrap_or_default();
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_array", column));
    // Order the array elements by the column so the result is deterministic
    // (rows feed the aggregate in an unspecified order under
    // preserve_insertion_order=false, so the array varies run-to-run).
    // json_group_array is a macro and rejects ORDER BY, so build the array via
    // list() (a true aggregate that accepts ORDER BY) + to_json, which yields
    // the same JSON array.
    let agg = format!(
        "to_json(list({c} ORDER BY {c})) AS {}",
        quote_ident(&output),
        c = quote_ident(&column)
    );
    if group_by.is_empty() {
        Ok(format!("SELECT {} FROM {}", agg, quote_ident(upstream)))
    } else {
        let cols = group_by
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "SELECT {cols}, {agg} FROM {up} GROUP BY {cols}",
            cols = cols,
            agg = agg,
            up = quote_ident(upstream)
        ))
    }
}

/// Text Similarity: pairwise string similarity between two columns
/// via levenshtein (edit distance), damerau_levenshtein (also counts
/// transpositions), jaccard (set similarity of trigrams), or
/// jaro_winkler_similarity (0..1, weighted toward shared prefixes).
/// The first two are integer distances (lower = more similar); the
/// last two are normalized similarities (higher = more similar).
pub(crate) fn build_text_similarity(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.text.similarity"))?;
    let left_col = string_prop(props, "leftColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Similarity needs a left column".to_string())?;
    let right_col = string_prop(props, "rightColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Text Similarity needs a right column".to_string())?;
    let algo = string_prop(props, "algorithm").unwrap_or_else(|| "levenshtein".into());
    let fn_name = match algo.as_str() {
        "damerau_levenshtein" => "damerau_levenshtein",
        "jaccard" => "jaccard",
        "jaro_winkler" => "jaro_winkler_similarity",
        _ => "levenshtein",
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}_{}_score", left_col, right_col, fn_name));
    let l = quote_ident(&left_col);
    let r = quote_ident(&right_col);
    // jaccard() raises "argument too short!" on an empty-string input,
    // which aborts the whole run on the first empty row. Guard it: an
    // empty (or NULL) value on either side yields a NULL score instead.
    // The other algorithms handle empty/short strings fine.
    let expr = if fn_name == "jaccard" {
        format!(
            "CASE WHEN CAST({l} AS VARCHAR) = '' OR CAST({r} AS VARCHAR) = '' THEN NULL \
             ELSE jaccard(CAST({l} AS VARCHAR), CAST({r} AS VARCHAR)) END"
        )
    } else {
        format!("{fn_name}(CAST({l} AS VARCHAR), CAST({r} AS VARCHAR))")
    };
    Ok(format!(
        "SELECT *, {expr} AS {out} FROM {up}",
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Spatial Join: a two-input join whose predicate is a spatial
/// relationship between left.geom and right.geom (intersects /
/// contains / within / touches / crosses / overlaps / equals).
/// Different from xf.geo.intersects which is a one-input enrichment
/// against a fixed target. The classic "orders inside delivery zone"
/// example is `left=orders.point JOIN right=zones.polygon ON
/// ST_Within(orders.point, zones.polygon)`.
/// The DuckDB spatial function a `relation` names.
///
/// Shared by the join and by its reject stream, so the two halves cannot
/// disagree about what "matched" meant - including the fallback, which an
/// unrecognised relation lands on.
fn spatial_relation_fn(props: &JsonValue) -> &'static str {
    match string_prop(props, "relation").unwrap_or_default().as_str() {
        "contains" => "ST_Contains",
        "within" => "ST_Within",
        "touches" => "ST_Touches",
        "crosses" => "ST_Crosses",
        "overlaps" => "ST_Overlaps",
        "equals" => "ST_Equals",
        // #220: Covers / CoveredBy differ from Contains / Within at the
        // boundary - a geometry covers another that touches its edge, which
        // Contains rejects. That distinction is why GIS tools expose both.
        "covers" => "ST_Covers",
        "coveredby" => "ST_CoveredBy",
        _ => "ST_Intersects",
    }
}

pub(crate) fn build_spatial_join(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let left = inputs
        .main()
        .ok_or_else(|| "Spatial Join needs a driving input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "Spatial Join needs a lookup input".to_string())?;
    let left_col = string_prop(props, "leftGeomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Spatial Join needs leftGeomColumn".to_string())?;
    let right_col = string_prop(props, "rightGeomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Spatial Join needs rightGeomColumn".to_string())?;
    let fn_name = spatial_relation_fn(props);
    let kind = match string_prop(props, "joinType").as_deref() {
        Some("left") => "LEFT",
        _ => "INNER",
    };
    // #219: a spatial join across mismatched CRS is the worst kind of wrong -
    // every predicate is false, so the run succeeds and returns zero rows with
    // no hint why. Compare the two CRS up front and fail with both names.
    //
    // The CRS lives in the column TYPE (`GEOMETRY('EPSG:4326')`), not the
    // value, so it is read via typeof() exactly as the CRS-aware measurements
    // do (#177). The guard sits in WHERE rather than the select list because a
    // cross-joined column that nothing reads can be optimised away, and a
    // pruned guard never fires. An unresolved CRS on either side stays
    // permissive: only two KNOWN and DIFFERENT systems are an error.
    let (guard_cte, guard_pred) =
        crs_match_guard(left, &left_col, right, &right_col, "Spatial Join");
    Ok(format!(
        "WITH {guard} \
         SELECT m.*, r.* FROM {lv} m {kind} JOIN {rv} r \
         ON {fnm}(CAST(m.{lc} AS GEOMETRY), CAST(r.{rc} AS GEOMETRY)) \
         WHERE {pred}",
        guard = guard_cte,
        pred = guard_pred,
        lv = quote_ident(left),
        rv = quote_ident(right),
        kind = kind,
        fnm = fn_name,
        lc = quote_ident(&left_col),
        rc = quote_ident(&right_col),
    ))
}

/// A CTE plus a filter that fails when two geometry columns carry KNOWN and
/// DIFFERENT coordinate systems (#219).
///
/// Shared by every two-layer geometry operation, because the failure mode is
/// identical in all of them: mismatched CRS silently produces an empty or wrong
/// result rather than an error. The CRS lives in the column TYPE
/// (`GEOMETRY('EPSG:4326')`), not the value, so it is read via typeof() the
/// same way the CRS-aware measurements do (#177).
///
/// Returns `(cte, predicate)`. The predicate belongs in WHERE, not the select
/// list: a cross-joined column that nothing reads can be optimised away, and a
/// pruned guard never fires.
///
/// An unresolved CRS on either side stays permissive. Only two known and
/// different systems are an error, so layers without CRS metadata keep working.
fn crs_match_guard(
    first_view: &str,
    first_col: &str,
    second_view: &str,
    second_col: &str,
    label: &str,
) -> (String, &'static str) {
    let probe = |view: &str, col: &str| {
        format!(
            "regexp_extract(typeof((SELECT {} FROM {} LIMIT 1)), 'GEOMETRY\\(''([^'']*)''\\)', 1)",
            quote_ident(col),
            quote_ident(view)
        )
    };
    let cte = format!(
        "__crs_guard AS (\
           SELECT CASE \
             WHEN __l <> '' AND __r <> '' AND __l <> __r \
               THEN error('{label}: the first layer uses ' || __l || \
                          ' but the second uses ' || __r || \
                          '. Reproject one side (Reproject Geometry) so both layers share a CRS.') \
             ELSE TRUE END AS __ok \
           FROM (SELECT {l} AS __l, {r} AS __r)\
         )",
        label = label,
        l = probe(first_view, first_col),
        r = probe(second_view, second_col),
    );
    (cte, "(SELECT __ok FROM __crs_guard)")
}

/// Clip (#217): keep every attribute of the input layer, replace its geometry
/// with the part falling inside the clip layer, and drop features that do not
/// intersect it at all.
///
/// The clip layer is dissolved with ST_Union_Agg before use. Without that, a
/// plain join emits one output row per overlapping clip polygon, so an input
/// feature spanning three clip tiles would be triplicated - which is not what
/// Clip means in QGIS, ArcGIS or FME.
pub(crate) fn build_geo_clip(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let input = inputs
        .main()
        .ok_or_else(|| "Clip needs an input layer on the main input".to_string())?;
    let clip = inputs
        .first_lookup()
        .ok_or_else(|| "Clip needs a clip layer on the second input".to_string())?;
    let in_col = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Clip needs the input layer's geometry column".to_string())?;
    // Defaults to the same name, which is the common case when both layers come
    // from the same kind of source.
    let clip_col = string_prop(props, "clipGeomColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| in_col.clone());
    // #218 follow-up: the input geometry is deliberately NOT cast to GEOMETRY.
    // DuckDB keeps a geometry's CRS in its logical type - GEOMETRY('EPSG:27700')
    // - and CAST(x AS GEOMETRY) targets the unparameterised type, discarding it,
    // which is why the Shapefile sink had no CRS to write and emitted no .prj.
    // ST_Intersection propagates the CRS from its arguments. The clip layer keeps
    // its cast: its CRS never reaches the output, and the guard below already
    // requires the two layers to agree.
    let (guard_cte, guard_pred) = crs_match_guard(input, &in_col, clip, &clip_col, "Clip");
    Ok(format!(
        "WITH {guard}, \
         __clip AS (SELECT ST_Union_Agg(CAST({cc} AS GEOMETRY)) AS __g FROM {cv}) \
         SELECT m.* REPLACE (ST_Intersection(m.{ic}, __c.__g) AS {ic}) \
         FROM {iv} m CROSS JOIN __clip __c \
         WHERE {pred} AND __c.__g IS NOT NULL \
           AND ST_Intersects(m.{ic}, __c.__g)",
        guard = guard_cte,
        pred = guard_pred,
        cc = quote_ident(&clip_col),
        cv = quote_ident(clip),
        ic = quote_ident(&in_col),
        iv = quote_ident(input),
    ))
}

/// Erase (#218): keep every attribute of the input layer, subtract the erase
/// layer from its geometry, and drop features left with nothing.
///
/// Like Clip, the erase layer is dissolved first: ST_Difference against each
/// erase feature in turn would only remove the last one. The emptiness check
/// runs on the computed geometry in an outer query so the difference is
/// expressed once.
pub(crate) fn build_geo_erase(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let input = inputs
        .main()
        .ok_or_else(|| "Erase needs an input layer on the main input".to_string())?;
    let erase = inputs
        .first_lookup()
        .ok_or_else(|| "Erase needs an erase layer on the second input".to_string())?;
    let in_col = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Erase needs the input layer's geometry column".to_string())?;
    let erase_col = string_prop(props, "eraseGeomColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| in_col.clone());
    // #218 follow-up: input geometry left uncast so its CRS survives into the
    // output type - see the note in build_geo_clip. BOTH CASE branches must stay
    // uncast; if one is cast the branches disagree and the result decays to bare
    // GEOMETRY, losing the CRS again.
    let (guard_cte, guard_pred) = crs_match_guard(input, &in_col, erase, &erase_col, "Erase");
    Ok(format!(
        "SELECT * FROM (\
           WITH {guard}, \
           __erase AS (SELECT ST_Union_Agg(CAST({ec} AS GEOMETRY)) AS __g FROM {ev}) \
           SELECT m.* REPLACE (\
             CASE WHEN __e.__g IS NULL THEN m.{ic} \
                  ELSE ST_Difference(m.{ic}, __e.__g) END AS {ic}) \
           FROM {iv} m CROSS JOIN __erase __e \
           WHERE {pred}\
         ) WHERE NOT ST_IsEmpty({ic})",
        guard = guard_cte,
        pred = guard_pred,
        ec = quote_ident(&erase_col),
        ev = quote_ident(erase),
        ic = quote_ident(&in_col),
        iv = quote_ident(input),
    ))
}

/// Spatial Intersects: add a boolean column with ST_Intersects(geom,
/// target). Pair with xf.filter downstream to keep only the rows that
/// overlap a polygon (e.g. "orders inside a delivery zone"). Two-input
/// spatial joins land later as xf.join.spatial.
pub(crate) fn build_geo_intersects(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.geo.intersects"))?;
    let column = string_prop(props, "geomColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Spatial Intersects needs a geometry column".to_string())?;
    let target = string_prop(props, "targetWkt")
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Spatial Intersects needs a target geometry (WKT)".to_string())?;
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "intersects".into());
    Ok(format!(
        "SELECT *, ST_Intersects(CAST({col} AS GEOMETRY), ST_GeomFromText('{target}')) AS {out} FROM {up}",
        col = quote_ident(&column),
        target = target.replace('\'', "''"),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Hash: add a column with the md5 / sha1 / sha256 digest (or a
/// DuckDB `hash()` int64) of an input column. Useful for deterministic
/// IDs from natural keys, one-way PII masking, and fingerprinting.
pub(crate) fn build_hash(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.hash"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Hash needs a column".to_string())?;
    let algo = string_prop(props, "algorithm").unwrap_or_else(|| "md5".into());
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_hash", column));
    let fn_name = match algo.as_str() {
        "sha1" => "sha1",
        "sha256" => "sha256",
        "hash" => "hash",
        _ => "md5",
    };
    Ok(format!(
        "SELECT *, {fn_name}(CAST({col} AS VARCHAR)) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Assert: hard-fail the pipeline if any row violates the given SQL
/// predicate. Unlike qa.* validators which route bad rows to a reject
/// port, this stops the whole pipeline so a downstream sink never
/// sees a partial result. Rows pass through unchanged. The CASE
/// invokes DuckDB's error() in the ELSE branch; the error surfaces
/// as the stage's failure with the user's message. The outer
/// EXCLUDE strips the temporary marker column so downstream stages
/// see the original schema.
pub(crate) fn build_assert(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.assert"))?;
    let predicate = string_prop(props, "predicate")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Assert needs a SQL predicate (e.g. amount >= 0)".to_string())?;
    let raw_msg = string_prop(props, "message")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("Assertion violated: {}", predicate));
    let msg = sql_escape(&raw_msg);
    // Aggregate the predicate into a single boolean across the whole
    // input via bool_and, then evaluate one CASE in a MATERIALIZED CTE.
    // This pattern (rather than a per-row CASE in the projection) is the
    // only shape DuckDB reliably keeps - the optimizer prunes unused
    // projection columns even when their CASE has error() in the ELSE,
    // which on some platforms (notably Windows release builds in CI)
    // means the assertion silently never fires. The aggregate has no
    // such hiding place; bool_and is forced to scan every row, and the
    // outer SELECT uses the CTE's value in WHERE so the CTE is
    // genuinely materialized. COALESCE(..., TRUE) treats an empty
    // input as a pass (vacuously true).
    Ok(format!(
        "WITH _duckle_assert AS MATERIALIZED (SELECT CASE WHEN COALESCE(bool_and(CAST(({pred}) AS BOOLEAN)), TRUE) THEN 'ok' ELSE error('{msg}') END AS result FROM {up}) SELECT u.* FROM {up} u WHERE (SELECT result FROM _duckle_assert) IS NOT NULL",
        pred = predicate,
        msg = msg,
        up = quote_ident(upstream)
    ))
}

/// URL Parse: pull a single component out of a URL string column via
/// a fixed regex. Picks one of scheme / host / port / path / query /
/// fragment with the `kind` prop, mirrors xf.ip.parse's shape.
pub(crate) fn build_url_parse(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.url.parse"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "URL Parse needs an input column".to_string())?;
    let kind = string_prop(props, "kind").unwrap_or_else(|| "host".into());
    // Single regex with named groups for every URL component. The
    // expression intentionally accepts URLs with and without a scheme.
    let url_re = "^(?:([a-zA-Z][a-zA-Z0-9+.-]*)://)?([^:/?#]*)(?::([0-9]+))?(/[^?#]*)?(?:\\?([^#]*))?(?:#(.*))?$";
    let group_idx: i64 = match kind.as_str() {
        "scheme" => 1,
        "host" => 2,
        "port" => 3,
        "path" => 4,
        "query" => 5,
        "fragment" => 6,
        _ => 2,
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", column, kind));
    Ok(format!(
        "SELECT *, regexp_extract(CAST({col} AS VARCHAR), '{re}', {idx}) AS {out} FROM {up}",
        col = quote_ident(&column),
        re = sql_escape(url_re),
        idx = group_idx,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// IP Parse: CAST a text/IP column to INET and extract a single
/// component via the inet extension. `kind` picks which piece comes
/// out (host / family / broadcast / netmask / hostmask / masklen /
/// network), so one row gives one output column and the upstream
/// schema is untouched. The CAST handles both bare addresses
/// (1.2.3.4 / ::1) and CIDR notation (10.0.0.0/8).
pub(crate) fn build_ip_parse(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| missing_input_msg("xf.ip.parse"))?;
    let column = string_prop(props, "column")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "IP Parse needs an input column".to_string())?;
    let kind = string_prop(props, "kind").unwrap_or_else(|| "host".into());
    let fn_name = match kind.as_str() {
        "family" => "family",
        "broadcast" => "broadcast",
        "netmask" => "netmask",
        "hostmask" => "hostmask",
        "masklen" => "masklen",
        "network" => "network",
        _ => "host",
    };
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("{}_{}", column, fn_name));
    Ok(format!(
        "SELECT *, {fn_name}(CAST({col} AS INET)) AS {out} FROM {up}",
        col = quote_ident(&column),
        out = quote_ident(&output),
        up = quote_ident(upstream)
    ))
}

/// Vector Similarity Search via the DuckDB vss extension. Adds a
/// similarity score column to each upstream row (against a fixed query
/// vector) and optionally returns only the top-K most similar rows.
/// The vector column is CAST to FLOAT[dim] so vss accepts it; the
/// target vector is embedded as an array literal (validated as a JSON
/// array of numbers at plan time).
pub(crate) fn build_vector_search(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs
        .main()
        .ok_or_else(|| missing_input_msg("xf.ai.vector_search"))?;
    let column = string_prop(props, "vectorColumn")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Vector Search needs a vector column".to_string())?;
    let target = string_prop(props, "targetVector")
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Vector Search needs a target vector (JSON array of floats)".to_string())?;
    let dim = props
        .get("dimension")
        .and_then(|v| v.as_u64())
        .filter(|d| *d > 0)
        .ok_or_else(|| "Vector Search needs a positive dimension".to_string())?;
    let metric = string_prop(props, "distanceMetric").unwrap_or_else(|| "cosine".into());
    let top_k = props
        .get("topK")
        .and_then(|v| v.as_u64())
        .filter(|k| *k > 0);
    let output = string_prop(props, "outputColumn")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "similarity_score".into());

    let vec_vals: Vec<f64> = serde_json::from_str(&target)
        .map_err(|e| format!("Vector Search: targetVector must be a JSON array of numbers ({})", e))?;
    if vec_vals.len() as u64 != dim {
        return Err(format!(
            "Vector Search: target vector has {} elements but dimension is {}",
            vec_vals.len(),
            dim
        ));
    }
    let target_literal = format!(
        "[{}]::FLOAT[{}]",
        vec_vals
            .iter()
            .map(|f| format!("{}", f))
            .collect::<Vec<_>>()
            .join(","),
        dim
    );
    let col_cast = format!("CAST({} AS FLOAT[{}])", quote_ident(&column), dim);
    let (fn_name, order_dir) = match metric.as_str() {
        "l2" | "distance" => ("array_distance", "ASC"),
        "inner_product" | "dot" => ("array_inner_product", "DESC"),
        _ => ("array_cosine_similarity", "DESC"),
    };
    let score_expr = format!("{fn_name}({col_cast}, {target_literal})");
    let mut sql = format!(
        "SELECT *, {score} AS {out} FROM {up}",
        score = score_expr,
        out = quote_ident(&output),
        up = quote_ident(upstream)
    );
    if let Some(k) = top_k {
        sql = format!(
            "{sql} ORDER BY {out} {dir} LIMIT {k}",
            out = quote_ident(&output),
            dir = order_dir
        );
    }
    Ok(sql)
}

/// Geospatial source via the DuckDB spatial extension. ST_Read is
/// GDAL-backed, so the same builder handles GeoJSON, Shapefile,
/// GeoPackage, KML, GPX, and many more (format auto-detected by file
/// extension). The geometry column comes through as binary; downstream
/// transforms (e.g. ST_AsText) can convert it.
pub(crate) fn build_spatial_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    // #241: GeoParquet is not something ST_Read can open. ST_Read is
    // GDAL-backed, and the spatial extension DuckDB ships does not carry
    // GDAL's Parquet driver, so a .geoparquet path fails with "Could not open
    // GDAL dataset" - the file is perfectly readable, just not by that
    // function. `read_parquet` reads it natively and returns a real GEOMETRY
    // with its CRS intact, which is what the rest of the geo components
    // expect.
    if is_parquet_path(&path) {
        return format!("SELECT * FROM read_parquet('{}')", sql_escape(&path));
    }
    format!("SELECT * FROM ST_Read('{}')", sql_escape(&path))
}

/// Whether a path names a Parquet file, including a glob or a remote URL.
///
/// By extension rather than by sniffing, because the decision is made while
/// compiling and the file may not be reachable yet - and because a wrong guess
/// here is a confusing failure at run time rather than a wrong answer.
fn is_parquet_path(path: &str) -> bool {
    let p = path.split(['?', '#']).next().unwrap_or(path).trim().to_ascii_lowercase();
    p.ends_with(".parquet") || p.ends_with(".geoparquet") || p.ends_with(".pq")
}

/// Esri File Geodatabase (.gdb) source via the spatial extension (#205).
/// ST_Read is GDAL-backed and opens the OpenFileGDB dataset (a folder); a
/// .gdb usually holds several feature classes, so `layer` names the one to
/// read (omitted = GDAL's first/default layer). That per-layer selection is
/// why this is a distinct source from src.spatial, which reads only the
/// default layer.
pub(crate) fn build_gdb_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    match string_prop(props, "layer").filter(|s| !s.trim().is_empty()) {
        Some(layer) => format!(
            "SELECT * FROM ST_Read('{}', layer='{}')",
            sql_escape(&path),
            sql_escape(&layer)
        ),
        None => format!("SELECT * FROM ST_Read('{}')", sql_escape(&path)),
    }
}

/// Hugging Face dataset source (native HF connector). DuckDB's httpfs reads
/// `hf://datasets/<repo>[@<revision>]/<path>` directly, so this assembles that
/// URL and lets DuckDB auto-detect Parquet / CSV / JSON by extension (a glob
/// like `**/*.parquet` reads every shard). Auth for private / gated datasets
/// comes from the HUGGINGFACE secret emitted by attach_prelude; public datasets
/// need none.
pub(crate) fn build_huggingface_source(props: &JsonValue) -> String {
    // Accept a bare id ("stanfordnlp/imdb"), a "datasets/..." prefix, or a full
    // hf:// URL; normalise to the bare id.
    let repo = string_prop(props, "repo").unwrap_or_default();
    let repo = repo
        .trim()
        .trim_start_matches("hf://")
        .trim_start_matches("datasets/")
        .trim_matches('/')
        .to_string();
    let path = string_prop(props, "path").unwrap_or_default();
    let path = path.trim().trim_start_matches('/');
    let url = match string_prop(props, "revision").filter(|s| !s.trim().is_empty()) {
        Some(rev) => format!("hf://datasets/{}@{}/{}", repo, rev.trim(), path),
        None => format!("hf://datasets/{}/{}", repo, path),
    };
    format!("SELECT * FROM '{}'", sql_escape(&url))
}

/// Fixed-width / positional source. The form gives a `columns` array
/// of `{name, start (1-based), width}` entries; the engine builds a
/// SELECT that walks each line and pulls the substring at the right
/// offset. The whole-file-as-one-column trick uses read_csv with a
/// delimiter that can't appear in plain text (chr(7) - the BEL) so
/// every line becomes a single string the SUBSTR projections can chew.
/// Trims trailing whitespace by default (the standard for fixed-width
/// dumps where every field is padded to its column width).
pub(crate) fn build_fixedwidth_source(
    props: &JsonValue,
    declared: Option<&[duckle_metadata::Column]>,
) -> Result<String, String> {
    let path = string_prop(props, "path")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Fixed-width source: path required".to_string())?;
    // The form offers `columnWidths` ("10,20,8") and this required `columns`
    // as an array of {name,start,width}, so a node configured in the editor
    // failed outright and no field on the form could satisfy it. Widths are
    // cumulative - the Nth column starts after the ones before it - and the
    // names come from the declared schema when there is one, which is the same
    // rule a headerless CSV already follows.
    let widths_form: Option<Vec<JsonValue>> = string_prop(props, "columnWidths")
        .filter(|s| !s.trim().is_empty())
        .map(|spec| {
            let mut at: i64 = 1;
            let mut out = Vec::new();
            for (i, piece) in spec.split(',').enumerate() {
                let w: i64 = match piece.trim().parse() {
                    Ok(w) if w > 0 => w,
                    _ => continue,
                };
                let name = declared
                    .and_then(|d| d.get(i))
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| format!("col{}", i + 1));
                out.push(serde_json::json!({ "name": name, "start": at, "width": w }));
                at += w;
            }
            out
        })
        .filter(|v: &Vec<JsonValue>| !v.is_empty());
    let owned;
    let cols: &Vec<JsonValue> = match props.get("columns").and_then(|v| v.as_array()) {
        Some(c) => c,
        None => match widths_form {
            Some(w) => {
                owned = w;
                &owned
            }
            None => {
                return Err("Fixed-width source: set columnWidths (e.g. 10,20,8), or a columns                             array of {name, start, width} each"
                    .to_string())
            }
        },
    };
    if cols.is_empty() {
        return Err("Fixed-width source: at least one column required".into());
    }
    let trim = props
        .get("trim")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let projections: Vec<String> = cols
        .iter()
        .map(|c| {
            let name = c
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("col")
                .to_string();
            let start = c.get("start").and_then(|v| v.as_i64()).unwrap_or(1);
            let width = c.get("width").and_then(|v| v.as_i64()).unwrap_or(1);
            let raw = format!("substr(line, {}, {})", start, width);
            let expr = if trim {
                format!("rtrim({})", raw)
            } else {
                raw
            };
            format!("{} AS {}", expr, quote_ident(&name))
        })
        .collect();
    // chr(7) (BEL) is virtually never present in real text; using it as
    // the read_csv delimiter forces every line to land as one column.
    // all_varchar=true keeps the line string-typed regardless of what
    // it happens to start with (numbers, dates, etc).
    Ok(format!(
        "WITH _lines AS (SELECT column0 AS line FROM read_csv_auto('{}', delim = chr(7), header = false, all_varchar = true)) SELECT {} FROM _lines",
        sql_escape(&path),
        projections.join(", ")
    ))
}

/// src.inline: rows written into the pipeline rather than read from anywhere.
///
/// Every other source names an external system, so a control row, an audit
/// stamp or a fixed lookup had nowhere to come from: the workaround was a
/// throwaway file, or a custom-SQL node whose required input port stayed
/// unwired. `columns` is a list of {name, value}; `rowCount` repeats the row.
pub(crate) fn build_inline_source(props: &JsonValue) -> String {
    let cols = kv_pairs(props, "columns");
    if cols.is_empty() {
        return "SELECT NULL WHERE false".to_string();
    }
    let n = props
        .get("rowCount")
        .and_then(JsonValue::as_u64)
        .filter(|n| *n > 0)
        .unwrap_or(1);
    // Values are written as literals, not identifiers: an inline row is data
    // the author typed, and quoting it as SQL would let a stray name resolve
    // against a table that happens to exist.
    let projection = cols
        .iter()
        .map(|(k, v)| format!("'{}' AS {}", sql_escape(v.trim()), quote_ident(k.trim())))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {} FROM range({})", projection, n)
}

/// src.filelist: one row per file in a directory, so a pipeline can iterate a
/// folder. `file` is the full path; `filename` is the last segment.
///
/// The nearest thing before this was an FTP listing, so "process every file in
/// this folder" - the most ordinary batch shape there is - had no local answer.
/// src.artifact: one row per file, described the way a pipeline can reason about it.
///
/// #247 asks for artifacts - PDFs, images, model binaries, OCR output - to be first
/// class alongside tables. The issue also says the right thing about how: an artifact is
/// a REFERENCE, not the bytes. And a reference - uri, media type, size, hash - is a ROW.
///
/// So this is a source, not a new kind of edge. It emits the shape the issue proposes,
/// which then flows through every join, filter, foreach and sink that already exists.
/// A node handing back "a table and an artifact" is two output ports, which the engine
/// has had since reject ports.
///
/// The hash is optional and off by default, because computing it reads every byte - the
/// one thing the issue says not to do to a large model file. Ask for it when you want
/// reproducibility and can pay for it.
pub(crate) fn build_artifact_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let pattern = string_prop(props, "glob")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "*".into());
    let recursive = props
        .get("recursive")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    let trimmed = path.trim_end_matches(['/', '\\']).to_string();
    // A path naming one file is that file; a folder gets the pattern applied to it.
    let target = if trimmed.is_empty() {
        pattern.clone()
    } else if std::path::Path::new(&trimmed).is_file() {
        trimmed.clone()
    } else if recursive {
        format!("{trimmed}/**/{pattern}")
    } else {
        format!("{trimmed}/{pattern}")
    };
    let hash = props
        .get("hash")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    // Named from the extension. Enough to route a pipeline - a PDF one way, an image
    // another - without pretending to sniff content.
    let media = r"CASE lower(regexp_extract(filename, '\.([A-Za-z0-9]+)$', 1)) WHEN 'pdf' THEN 'application/pdf' WHEN 'png' THEN 'image/png' WHEN 'jpg' THEN 'image/jpeg' WHEN 'jpeg' THEN 'image/jpeg' WHEN 'tif' THEN 'image/tiff' WHEN 'tiff' THEN 'image/tiff' WHEN 'zip' THEN 'application/zip' WHEN 'json' THEN 'application/json' WHEN 'xml' THEN 'application/xml' WHEN 'csv' THEN 'text/csv' WHEN 'txt' THEN 'text/plain' WHEN 'html' THEN 'text/html' WHEN 'htm' THEN 'text/html' WHEN 'parquet' THEN 'application/vnd.apache.parquet' ELSE 'application/octet-stream' END";
    let sha = if hash {
        "sha256(content) AS sha256"
    } else {
        "CAST(NULL AS VARCHAR) AS sha256"
    };
    format!(
        "SELECT filename AS uri, parse_filename(filename) AS name, {media} AS media_type, size AS size_bytes, {sha}, last_modified AS modified_at FROM read_blob('{}')",
        sql_escape(&target)
    )
}

pub(crate) fn build_filelist_source(props: &JsonValue) -> String {
    // An explicit `path` is used verbatim, which makes the component double as
    // an existence test: pointed at one file it yields one row, or none. That
    // is what a job's file-exists check needs, and it needs no second component.
    if let Some(path) = string_prop(props, "path").filter(|s| !s.trim().is_empty()) {
        return format!(
            "SELECT file, parse_filename(file) AS filename FROM glob('{}')",
            sql_escape(path.trim())
        );
    }
    let dir = string_prop(props, "directory").unwrap_or_default();
    let pattern = string_prop(props, "pattern")
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "*".into());
    let recursive = props
        .get("recursive")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    let dir = dir.trim_end_matches(['/', '\\']);
    let glob = if recursive {
        format!("{}/**/{}", dir, pattern)
    } else {
        format!("{}/{}", dir, pattern)
    };
    // parse_filename rather than a regex: the separator differs per platform,
    // and the glob above may have been written with either.
    format!(
        "SELECT file, parse_filename(file) AS filename FROM glob('{}')",
        sql_escape(&glob)
    )
}

/// Iceberg source via the DuckDB iceberg extension's `iceberg_scan`.
/// The `path` is the iceberg table location (a local directory or an
/// `s3://...` URL backed by a cloud SECRET created elsewhere).
pub(crate) fn build_iceberg_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!("SELECT * FROM iceberg_scan('{}')", sql_escape(&path))
}

/// Delta Lake source via the DuckDB delta extension's `delta_scan`.
pub(crate) fn build_delta_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    format!("SELECT * FROM delta_scan('{}')", sql_escape(&path))
}

/// Excel (.xlsx) source via DuckDB v1.2+ `read_xlsx`. Supports an
/// optional `sheet` form field (omitted defaults to the first sheet)
/// and a `hasHeader` toggle.
pub(crate) fn build_excel_source(
    props: &JsonValue,
    declared: Option<&[duckle_metadata::Column]>,
) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    // read_xlsx has no `types=` / `columns=` (unlike read_csv_auto), so the
    // Schema panel (retype + remove columns) used to be silently ignored -
    // every column came through with the reader's inferred types (issue #25).
    // When a schema is declared, read every cell as text (all_varchar) and
    // cast + project to exactly the declared columns in an outer SELECT. With
    // no declared schema the read is unchanged (auto-infer, all columns).
    let typed = declared.filter(|c| !c.is_empty());

    // Extra read_xlsx options (sheet / header) are shared by every file.
    let mut opts: Vec<String> = Vec::new();
    if let Some(sheet) = string_prop(props, "sheet").filter(|s| !s.is_empty()) {
        opts.push(format!("sheet = '{}'", sql_escape(&sheet)));
    }
    if let Some(has_header) = props.get("hasHeader").and_then(JsonValue::as_bool) {
        opts.push(format!("header = {}", has_header));
    }
    if typed.is_some() {
        opts.push("all_varchar = true".to_string());
    }
    let one = |p: &str| {
        let mut args = vec![format!("'{}'", sql_escape(p))];
        args.extend(opts.iter().cloned());
        format!("SELECT * FROM read_xlsx({})", args.join(", "))
    };

    // DuckDB's excel reader can't glob (duckdb-excel#30): a wildcard or a
    // directory would silently read only the first file. Expand it ourselves
    // and UNION the per-file reads (BY NAME tolerates column-order drift).
    let files = expand_excel_paths(&path);
    let base = match files.len() {
        0 => one(&path), // nothing matched (or no fs access) - let DuckDB report it
        1 => one(&files[0]),
        _ => files
            .iter()
            .map(|f| one(f))
            .collect::<Vec<_>>()
            .join(" UNION ALL BY NAME "),
    };

    let Some(cols) = typed else {
        return base;
    };
    // Project + cast to exactly the declared columns. Mirrors the CSV path:
    // a DATE/TIMESTAMP column with its own format is re-parsed via
    // try_strptime (NULL on a value the format can't parse); everything else
    // is a plain cast from the all_varchar text.
    use duckle_metadata::DataType;
    let proj = cols
        .iter()
        .map(|c| {
            let id = quote_ident(&c.name);
            let fmt = c.format.as_deref().filter(|s| !s.is_empty());
            match (fmt, c.data_type) {
                // #104: Excel-native serial dates store a number = days since
                // 1899-12-30 (the base accounts for Excel's 1900 leap-year bug).
                // Set the column's format to "excel" to convert the serial to a
                // real date/timestamp instead of parsing it as text.
                (Some(f), DataType::Timestamp) if f.eq_ignore_ascii_case("excel") => format!(
                    "(TIMESTAMP '1899-12-30' + try_cast({id} AS DOUBLE) * INTERVAL 1 DAY) AS {id}",
                    id = id
                ),
                (Some(f), DataType::Date) if f.eq_ignore_ascii_case("excel") => format!(
                    "(TIMESTAMP '1899-12-30' + try_cast({id} AS DOUBLE) * INTERVAL 1 DAY)::DATE AS {id}",
                    id = id
                ),
                (Some(fmt), DataType::Date) => {
                    format!("try_strptime({id}, '{f}')::DATE AS {id}", id = id, f = sql_escape(fmt))
                }
                (Some(fmt), DataType::Timestamp) => format!(
                    "try_strptime({id}, '{f}')::TIMESTAMP AS {id}",
                    id = id,
                    f = sql_escape(fmt)
                ),
                // String is already VARCHAR from all_varchar - select as-is.
                _ if matches!(c.data_type, DataType::String) => id.clone(),
                // #225: with no explicit format a DATE/TIMESTAMP column may hold
                // either a text date or an Excel serial number, and a declared
                // schema forces all_varchar, so a serial arrives as the string
                // "46037" and a plain CAST fails with "invalid date field
                // format". Try the text date first, so real date strings are
                // unaffected, then fall back to the serial conversion. #104's
                // format:"excel" is still honoured above and is now only needed
                // to force the serial reading on an ambiguous column.
                (None, DataType::Date) => format!(
                    "COALESCE(try_cast({id} AS DATE), (TIMESTAMP '1899-12-30' +                      try_cast({id} AS DOUBLE) * INTERVAL 1 DAY)::DATE) AS {id}",
                    id = id
                ),
                (None, DataType::Timestamp) => format!(
                    "COALESCE(try_cast({id} AS TIMESTAMP), TIMESTAMP '1899-12-30' +                      try_cast({id} AS DOUBLE) * INTERVAL 1 DAY) AS {id}",
                    id = id
                ),
                _ => format!("CAST({id} AS {ty}) AS {id}", id = id, ty = data_type_to_duckdb_sql(&c.data_type)),
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {} FROM ({})", proj, base)
}

/// Expand an Excel `path` into concrete .xlsx/.xls files. Handles a plain
/// file, a directory (all workbooks inside), and a `*`/`?` wildcard in the
/// final path segment. Returns an empty Vec when nothing matches or the
/// filesystem can't be read, in which case the caller falls back to handing
/// the literal path to DuckDB so it can surface the error.
fn expand_excel_paths(path: &str) -> Vec<String> {
    use std::path::Path;
    let is_excel = |name: &str| {
        let l = name.to_ascii_lowercase();
        l.ends_with(".xlsx") || l.ends_with(".xls")
    };
    let collect_dir = |dir: &Path, pat: Option<&str>| -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                let keep = is_excel(&name) && pat.map(|p| wildcard_match(p, &name)).unwrap_or(true);
                keep.then(|| e.path().to_string_lossy().into_owned())
            })
            .collect();
        out.sort();
        out
    };

    let p = Path::new(path);
    if p.is_file() {
        return vec![path.to_string()];
    }
    if p.is_dir() {
        return collect_dir(p, None);
    }
    // Wildcard in the final segment: match siblings in the parent directory.
    if path.contains('*') || path.contains('?') {
        let parent = p.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));
        if let Some(pat) = p.file_name().and_then(|s| s.to_str()) {
            return collect_dir(parent, Some(pat));
        }
    }
    Vec::new()
}

/// Minimal shell-style wildcard match supporting `*` (any run) and `?`
/// (single char), case-insensitive. Enough for `*.xlsx`, `2026-*.xls`, etc.
fn wildcard_match(pattern: &str, name: &str) -> bool {
    let pat: Vec<char> = pattern.to_ascii_lowercase().chars().collect();
    let txt: Vec<char> = name.to_ascii_lowercase().chars().collect();
    // Classic two-pointer glob match with backtracking on '*'.
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark): (Option<usize>, usize) = (None, 0);
    while ti < txt.len() {
        if pi < pat.len() && (pat[pi] == '?' || pat[pi] == txt[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pat.len() && pat[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }
    pi == pat.len()
}

/// Cloud sources (S3 / GCS / Azure Blob / HTTP). DuckDB's httpfs +
/// azure extensions let us read these directly via the same
/// read_csv_auto / read_parquet / read_json_auto family of functions.
/// Format is inferred from the URL extension unless the user picks one.
pub(crate) fn build_cloud_source(
    scheme: &str,
    props: &JsonValue,
    declared: Option<&[duckle_metadata::Column]>,
) -> Result<String, EngineError> {
    let path = string_prop(props, "path")
        .or_else(|| string_prop(props, "url"))
        .filter(|s| !s.is_empty())
        .or_else(|| {
            // The storage form supplies bucket + key rather than a full
            // URL; assemble one using the connector's scheme.
            let bucket = string_prop(props, "bucket").filter(|s| !s.is_empty())?;
            let key = string_prop(props, "key").unwrap_or_default();
            let prefix = match scheme {
                "s3" => "s3://",
                "gcs" => "gs://",
                "azureblob" => "az://",
                _ => "https://",
            };
            Some(format!("{}{}/{}", prefix, bucket, key.trim_start_matches('/')))
        })
        .unwrap_or_default();
    let override_fmt = string_prop(props, "format");
    let lower = path.to_ascii_lowercase();
    let chosen = override_fmt.filter(|s| !s.is_empty()).unwrap_or_else(|| {
        if lower.ends_with(".parquet") || lower.ends_with(".pq") {
            "parquet".into()
        } else if lower.ends_with(".json")
            || lower.ends_with(".jsonl")
            || lower.ends_with(".ndjson")
        {
            "json".into()
        } else if lower.ends_with(".tsv") {
            "tsv".into()
        } else {
            "csv".into()
        }
    });
    // Delegate to the LOCAL format builders with the resolved cloud path
    // injected into a cloned props, so a cloud (s3/gcs/azure/http) source
    // gets the same treatment as its local counterpart: parquet column
    // projection and CSV declared-schema (`types=`) + delimiter / header /
    // quote / null / date options. Previously this re-derived a minimal
    // read with none of those, silently dropping issue-#3 type enforcement
    // and every CSV option once the file lived in the cloud (audit B1). The
    // local builders read props["path"], so inject the assembled bucket/key
    // path here.
    let mut local = props.clone();
    if let Some(obj) = local.as_object_mut() {
        obj.insert("path".into(), JsonValue::String(path.clone()));
    }
    Ok(match chosen.as_str() {
        "parquet" => build_parquet_source(&local),
        // Delegate JSON too, so a cloud JSON source gets recordsPath unnesting
        // and the 100 MB maximum_object_size that the local builder applies
        // (a bare read_json_auto here ignored both - audit).
        "json" => build_json_source(&local),
        "tsv" => build_tsv_source(&local, declared),
        // The cloud reader has no Avro/ORC path (DuckDB ships no read_orc, and
        // read_avro is only wired for the local src.avro builder). Selecting
        // either used to fall through to the CSV default below and parse the
        // binary container with read_csv_auto -> garbage columns / a cryptic
        // parse error. Fail loud with an actionable message instead (audit).
        "avro" | "orc" => {
            return Err(EngineError::Unsupported(format!(
                "Cloud source format '{}' is not supported (use Parquet, JSON, CSV, or TSV; for Avro use a local src.avro source)",
                chosen
            )))
        }
        _ => build_csv_source(&local, declared),
    })
}

// ---- Sinks --------------------------------------------------------------

/// Validate-before-insert / dead-letter for DB sinks (#101 slice 6). When the
/// sink has `validateBeforeInsert` on AND a typed declared schema, build a
/// prelude that splits the upstream into rows that cleanly cast to the declared
/// types and rows that don't: the bad rows are COPYed to `deadLetterPath` (with
/// a `__rejected_at` stamp) and the sink then reads only the clean rows. Returns
/// (effective_from_view, prelude_sql); when off it returns (from_view, "") so the
/// sink SQL is byte-identical to before. DuckDB runs this against the ATTACHed
/// target in the same session, so no new execution path is needed.
fn dead_letter_prelude(
    props: &JsonValue,
    schema: Option<&[duckle_metadata::Column]>,
    from_view: &str,
) -> Result<(String, String), EngineError> {
    if !props
        .get("validateBeforeInsert")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
    {
        return Ok((from_view.to_string(), String::new()));
    }
    let cols = match schema.filter(|c| !c.is_empty()) {
        Some(c) => c,
        None => return Ok((from_view.to_string(), String::new())),
    };
    // A row is bad if any typed column's raw value won't cast to its declared
    // type (reuses the CSV reject predicate; text columns can never fail).
    let fails: Vec<String> = cols
        .iter()
        .filter_map(|c| csv_typed_col_exprs(c).map(|(f, _)| f))
        .collect();
    if fails.is_empty() {
        return Ok((from_view.to_string(), String::new()));
    }
    let path = string_prop(props, "deadLetterPath")
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            EngineError::Config(
                "validate-before-insert needs a dead-letter path to write rejected rows to".into(),
            )
        })?;
    let fmt = string_prop(props, "deadLetterFormat").unwrap_or_else(|| "parquet".into());
    let opts = match fmt.to_ascii_lowercase().as_str() {
        "csv" => "(FORMAT CSV, HEADER)",
        "json" => "(FORMAT JSON)",
        _ => "(FORMAT PARQUET, COMPRESSION 'ZSTD')",
    };
    let staged = format!("{}__dlq_stg", from_view);
    let valid = format!("{}__dlq_ok", from_view);
    let prelude = format!(
        "CREATE OR REPLACE TEMP VIEW {staged} AS SELECT *, ({fails}) AS __dlq_bad FROM {from}; \
         COPY (SELECT * EXCLUDE (__dlq_bad), CURRENT_TIMESTAMP AS __rejected_at FROM {staged} WHERE __dlq_bad) TO '{path}' {opts}; \
         CREATE OR REPLACE TEMP VIEW {valid} AS SELECT * EXCLUDE (__dlq_bad) FROM {staged} WHERE NOT __dlq_bad; ",
        staged = quote_ident(&staged),
        valid = quote_ident(&valid),
        from = quote_ident(from_view),
        fails = fails.join(" OR "),
        path = sql_escape(&path.replace('\\', "/")),
        opts = opts,
    );
    Ok((valid, prelude))
}

/// A file sink writes with `COPY ... TO`, which only ever REPLACES.
///
/// The forms offered "Append" (snk.parquet) and "Error if exists" (csv, json,
/// jsonl, excel, parquet) and none of these builders reads `mode` at all - an
/// unrecognised mode is not an error in a COPY, it is the default, and the
/// default is replace. Measured before this guard existed: rows 1,2 written,
/// a second run with mode=append writing 3,4, and the file afterwards held ONLY
/// 3,4. Someone asking to add to a dataset destroyed it, with no error.
///
/// The options are gone from the forms. This is for the pipelines already
/// carrying one, which would otherwise keep silently replacing: refuse, and say
/// what would have happened.
fn refuse_unimplemented_file_mode(
    component_id: &str,
    props: &JsonValue,
) -> Result<(), EngineError> {
    let mode = string_prop(props, "mode").unwrap_or_default();
    let mode = mode.trim();
    if mode.is_empty() || mode.eq_ignore_ascii_case("overwrite") {
        return Ok(());
    }
    Err(EngineError::Unsupported(format!(
        "{component_id}: write mode '{mode}' is not implemented for a file sink - it writes with          COPY, which always replaces the file. Running this would have REPLACED the existing data          rather than {}. Remove the mode, or write to a database sink, which does implement it.",
        if mode.eq_ignore_ascii_case("append") {
            "adding to it"
        } else {
            "refusing"
        }
    )))
}

pub(crate) fn build_sink_sql(
    component_id: &str,
    props: &JsonValue,
    from_view: &str,
    cols: &[String],
    schema: Option<&[duckle_metadata::Column]>,
) -> Result<String, EngineError> {
    match component_id {
        "snk.csv" => {
            refuse_unimplemented_file_mode(component_id, props)?;
            Ok(build_csv_sink(props, from_view))
        }
        "snk.tsv" => {
            let mut p = props.clone();
            if let Some(obj) = p.as_object_mut() {
                obj.insert("delimiter".into(), JsonValue::String("\t".into()));
            }
            Ok(build_csv_sink(&p, from_view))
        }
        "snk.parquet" => {
            refuse_unimplemented_file_mode(component_id, props)?;
            Ok(build_parquet_sink(props, from_view))
        }
        "snk.json" | "snk.jsonl" => {
            refuse_unimplemented_file_mode(component_id, props)?;
            Ok(build_json_sink(props, from_view))
        }
        "snk.s3" | "snk.gcs" | "snk.azureblob"
        | "snk.minio" | "snk.r2" | "snk.b2" => {
            // MinIO / R2 / B2 are S3-compatible; the endpoint lives in the
            // SECRET created by the runtime, so the URL is just s3://bucket/key.
            let s = component_id.strip_prefix("snk.").unwrap_or(component_id);
            let scheme = if matches!(s, "minio" | "r2" | "b2") { "s3" } else { s };
            build_cloud_sink(scheme, props, from_view)
        }
        "snk.sqlite" | "snk.duckdb" => {
            let (eff_from, prelude) = dead_letter_prelude(props, schema, from_view)?;
            Ok(format!("{}{}", prelude, build_db_sink(component_id, props, &eff_from, cols)?))
        }
        "snk.postgres" | "snk.cockroach" | "snk.mysql" | "snk.mariadb"
        | "snk.motherduck" | "snk.ducklake" | "snk.pgvector"
        | "snk.redshift" | "snk.bigquery" | "snk.quack"
        // #86: SQL Server / Synapse bulk path via the DuckDB mssql extension
        // (ATTACH + COPY/INSERT). Reached only in the bulk path; bulk=false
        // routes to the tiberius driver instead (see plan/mod.rs).
        | "snk.sqlserver" | "snk.synapse" => {
            let (eff_from, prelude) = dead_letter_prelude(props, schema, from_view)?;
            Ok(format!("{}{}", prelude, build_relational_sink(component_id, props, &eff_from, cols)?))
        }
        "snk.excel" => {
            refuse_unimplemented_file_mode(component_id, props)?;
            Ok(build_excel_sink(props, from_view))
        }
        "snk.spatial" => Ok(build_spatial_sink(props, from_view)),
        "snk.iceberg" => Ok(build_iceberg_sink(props, from_view)),
        other => Err(EngineError::Unsupported(format!(
            "Sink '{}' is not yet implemented",
            other
        ))),
    }
}

/// Cloud sink - COPY a view out to an s3:// / gs:// / az:// URL.
/// DuckDB's httpfs handles the upload; credentials come from the
/// SECRET wired up in execute_pipeline_with_events. Format is inferred
/// from the URL extension unless overridden.
pub(crate) fn build_cloud_sink(
    scheme: &str,
    props: &JsonValue,
    from_view: &str,
) -> Result<String, EngineError> {
    let path = string_prop(props, "path")
        .or_else(|| string_prop(props, "url"))
        .filter(|s| !s.is_empty())
        .or_else(|| {
            // The storage form supplies bucket + key rather than a full URL;
            // assemble one using the connector's scheme. Mirrors
            // build_cloud_source so a bucket/key sink (including snk.minio /
            // snk.r2 / snk.b2) writes to the right object instead of "".
            let bucket = string_prop(props, "bucket").filter(|s| !s.is_empty())?;
            let key = string_prop(props, "key").unwrap_or_default();
            let prefix = match scheme {
                "s3" => "s3://",
                "gcs" => "gs://",
                "azureblob" => "az://",
                _ => "https://",
            };
            Some(format!("{}{}/{}", prefix, bucket, key.trim_start_matches('/')))
        })
        .unwrap_or_default();
    let override_fmt = string_prop(props, "format").filter(|s| !s.is_empty());
    let lower = path.to_ascii_lowercase();
    let chosen = override_fmt.unwrap_or_else(|| {
        if lower.ends_with(".parquet") || lower.ends_with(".pq") {
            "parquet".into()
        } else if lower.ends_with(".json") || lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
            "json".into()
        } else {
            "csv".into()
        }
    });
    // Delegate to the LOCAL sink builders with the resolved cloud path
    // injected, so a cloud sink honors the same compression / delimiter /
    // null-value / header options as its local counterpart (audit B1).
    // Previously it emitted a fixed option set and ignored all of them.
    //
    // partitionBy is intentionally NOT forwarded: a partitioned directory
    // write over httpfs (s3/gs/azure) behaves very differently from a
    // single-object COPY and isn't validated against a live target, so
    // cloud sinks keep writing a single object as before. The `format` prop
    // selects the format family here (not build_json_sink's array toggle),
    // so it's stripped before the JSON delegation to preserve the current
    // NDJSON-always cloud-json behavior.
    let mut local = props.clone();
    if let Some(obj) = local.as_object_mut() {
        obj.insert("path".into(), JsonValue::String(path.clone()));
        obj.remove("partitionBy");
    }
    Ok(match chosen.as_str() {
        "csv" => build_csv_sink(&local, from_view),
        "json" | "jsonl" | "ndjson" => {
            if let Some(obj) = local.as_object_mut() {
                obj.remove("format");
            }
            build_json_sink(&local, from_view)
        }
        // No Avro/ORC writer exists (DuckDB's COPY has neither). Selecting
        // either used to fall through to the Parquet default below, silently
        // writing Parquet bytes to a path the user named .avro/.orc. Fail loud
        // instead of emitting a file whose contents contradict its format (audit).
        "avro" | "orc" => {
            return Err(EngineError::Unsupported(format!(
                "Cloud sink format '{}' is not supported (use Parquet, JSON, JSONL, or CSV)",
                chosen
            )))
        }
        _ => build_parquet_sink(&local, from_view),
    })
}

/// Guard against the partitioned-write foot-gun. A Hive-partitioned COPY writes
/// one file per distinct value-combination of the partition columns; partition
/// by a high-cardinality column (e.g. a country pair) and you silently explode
/// into tens of thousands of tiny files - a 51k-file / ~5-minute write that is
/// almost never intended. When partitioning, this wraps the COPY source with a
/// fail-fast check: if the approximate distinct partition count exceeds the
/// "Max partitions" cap (default 10000; 0 = unlimited), abort immediately via
/// DuckDB's error() with an actionable message instead of grinding out the
/// files. Returns the COPY source SELECT - plain when not partitioned or the
/// cap is 0, guarded otherwise.
fn partition_guarded_source(props: &JsonValue, from_view: &str, partition: &[String]) -> String {
    let view = quote_ident(from_view);
    let plain = format!("SELECT * FROM {}", view);
    if partition.is_empty() {
        return plain;
    }
    let cap = props
        .get("maxPartitions")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
        })
        .unwrap_or(10_000);
    if cap == 0 {
        return plain; // explicitly unlimited
    }
    // Approximate distinct partition combinations (HyperLogLog - cheap, one
    // pass, far cheaper than writing the files). chr(31) (unit separator) joins
    // multi-column keys with a separator that will not occur in normal data.
    let key = if partition.len() == 1 {
        quote_ident(&partition[0])
    } else {
        let parts = partition
            .iter()
            .map(|c| format!("{}::VARCHAR", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("concat_ws(chr(31), {})", parts)
    };
    let msg = format!(
        "Partition guard: partitioning by ({}) would create more than {} files (one per distinct value combination), which is almost always unintended and very slow. Remove the Partition by columns to write a single file, partition by a lower-cardinality column, or set Max partitions to 0 to allow it.",
        partition.join(", "),
        cap
    );
    format!(
        "SELECT * FROM {view} WHERE CASE WHEN (SELECT approx_count_distinct({key}) FROM {view}) > {cap} THEN error('{msg}') ELSE TRUE END",
        view = view,
        key = key,
        cap = cap,
        msg = sql_escape(&msg)
    )
}

pub(crate) fn build_csv_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    // The sink form writes `writeHeader`; the source uses `hasHeader`.
    let header = props
        .get("writeHeader")
        .or_else(|| props.get("hasHeader"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(true);
    let delim = string_prop(props, "delimiter").unwrap_or_else(|| ",".into());
    let null_val = string_prop(props, "nullValue").unwrap_or_default();
    let mut options = vec![
        "FORMAT CSV".to_string(),
        format!("HEADER {}", header),
        format!("DELIM '{}'", sql_escape(&delim)),
    ];
    if !null_val.is_empty() {
        options.push(format!("NULLSTR '{}'", sql_escape(&null_val)));
    }
    let partition = columns_from_props(props, "partitionBy").unwrap_or_default();
    if !partition.is_empty() {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        options.push(format!("PARTITION_BY ({})", cols));
        options.push("OVERWRITE_OR_IGNORE".to_string());
    }
    format!(
        "COPY ({}) TO '{}' ({})",
        partition_guarded_source(props, from_view, &partition),
        sql_escape(&path),
        options.join(", ")
    )
}

pub(crate) fn build_parquet_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    // #174: the UI exposes "None" as a compression option, but DuckDB only
    // accepts UNCOMPRESSED for "no compression" - a literal NONE / None / ""
    // fails with "Expected compression argument to be any of [uncompressed,
    // ...]". Normalize any none-ish value to UNCOMPRESSED.
    let compression = {
        let c = string_prop(props, "compression").unwrap_or_else(|| "ZSTD".into());
        let c = c.trim();
        if c.is_empty() || c.eq_ignore_ascii_case("none") {
            "UNCOMPRESSED".to_string()
        } else {
            c.to_string()
        }
    };
    let partition = columns_from_props(props, "partitionBy").unwrap_or_default();
    let mut options = vec![
        "FORMAT PARQUET".to_string(),
        format!("COMPRESSION '{}'", sql_escape(&compression)),
    ];
    // #175: optional COMPRESSION_LEVEL. DuckDB (1.5.4) accepts a level ONLY for
    // the ZSTD codec ("Compression level is only supported for the ZSTD
    // compression codec"), so emit it only for ZSTD and ignore it otherwise -
    // an unsupported combination would fail the whole write.
    if compression.eq_ignore_ascii_case("ZSTD") {
        let level = props.get("compressionLevel").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
        });
        if let Some(n) = level {
            options.push(format!("COMPRESSION_LEVEL {}", n));
        }
    }
    // #175: optional Parquet format version. V1 (DuckDB's default) preserves
    // maximum downstream compatibility; only emit PARQUET_VERSION when the user
    // opts into V2, so absent / V1 keeps the default untouched.
    if let Some(v) = string_prop(props, "parquetVersion") {
        let v = v.trim();
        if v.eq_ignore_ascii_case("v2") || v == "2" {
            options.push("PARQUET_VERSION V2".to_string());
        }
    }
    // Forward the "Row group size" UI field. Without it DuckDB falls back to
    // its internal default (~122,880 rows); a larger value (e.g. 1,000,000)
    // cuts per-row-group metadata overhead on big writes. Accept a number or a
    // numeric string; ignore absent / zero (keep DuckDB's default).
    let row_group_size = props.get("rowGroupSize").and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
    });
    if let Some(n) = row_group_size.filter(|n| *n > 0) {
        options.push(format!("ROW_GROUP_SIZE {}", n));
    }
    if !partition.is_empty() {
        let cols = partition
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        options.push(format!("PARTITION_BY ({})", cols));
        // DuckDB refuses to write into an existing partition directory
        // unless one of these is set; OVERWRITE_OR_IGNORE matches what
        // most ETL pipelines want (rewrite the slice we just emitted,
        // leave untouched siblings alone).
        options.push("OVERWRITE_OR_IGNORE".to_string());
    }
    let source = partition_guarded_source(props, from_view, &partition);
    // #319: optional Hilbert spatial ordering, so geometries that are close on
    // the ground land close in the file and row-group pruning can skip more.
    match hilbert_order(props, from_view) {
        None => format!("COPY ({}) TO '{}' ({})", source, sql_escape(&path), options.join(", ")),
        Some(order_by) => format!(
            // The spatial extension is loaded here rather than relied upon:
            // geometry usually arrives from a source that already loaded it and
            // taints this stage, but a GEOMETRY read back from a plain Parquet
            // file does not, and ST_Hilbert would then fail at write time -
            // after the whole pipeline had already run.
            "INSTALL spatial; LOAD spatial; COPY (SELECT * FROM ({}) {}) TO '{}' ({})",
            source,
            order_by,
            sql_escape(&path),
            options.join(", ")
        ),
    }
}

/// The `ORDER BY ST_Hilbert(...)` clause for a Parquet write, when one is asked
/// for (#319).
///
/// **Bounds relative to the data**, not the default global extent: the curve is
/// scaled to what is actually being written, which is what makes neighbouring
/// geometries land in the same row group. It costs one extra scan to find the
/// extent, which is the trade the feature exists to make.
///
/// A scalar subquery rather than the `CROSS JOIN bounds` the issue proposes,
/// because `SELECT *` across that join writes the bbox out as a **column of the
/// exported file**. Verified against DuckDB 1.5.4, where the issue's
/// `ST_Extent_Agg(geom)::BOX_2D` is also rejected outright ("Unimplemented type
/// for cast (GEOMETRY -> BOX_2D)"); `ST_Extent(ST_Extent_Agg(geom))` is the
/// form that works.
///
/// One property rather than a checkbox and a column: a checkbox ticked with no
/// column chosen is a state the engine would have to guess at, and guessing
/// which column holds the geometry is how the wrong one gets sorted on.
fn hilbert_order(props: &JsonValue, from_view: &str) -> Option<String> {
    let column = string_prop(props, "hilbertColumn").filter(|c| !c.trim().is_empty())?;
    let col = quote_ident(column.trim());
    Some(format!(
        "ORDER BY ST_Hilbert({col}, (SELECT ST_Extent(ST_Extent_Agg({col})) FROM {}))",
        quote_ident(from_view)
    ))
}

pub(crate) fn build_json_sink(props: &JsonValue, from_view: &str) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
    let array = string_prop(props, "format")
        .map(|f| f.eq_ignore_ascii_case("array"))
        .unwrap_or(false);
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT JSON, ARRAY {})",
        quote_ident(from_view),
        sql_escape(&path),
        if array { "true" } else { "false" }
    )
}

// ---- Helpers ------------------------------------------------------------

pub(crate) fn columns_from_props(props: &JsonValue, key: &str) -> Option<Vec<String>> {
    props
        .get(key)
        .and_then(JsonValue::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
}

/// A port a person typed, whether the GUI stored it as a number or as text.
///
/// The GUI's integer field writes a JSON NUMBER (`PrimitiveFields.tsx`
/// `IntegerField` calls `onChange(n)`), and `string_prop` is `as_str()` only -
/// so reading a port with `string_prop(..).parse()` got `None` for every value
/// the panel produced and fell through to the default. A GizmoSQL port typed
/// into the panel was silently discarded and the connection went to 31337.
///
/// Text is still accepted, because a hand-written pipeline file and an older
/// saved one both spell it that way.
pub(crate) fn port_prop(props: &JsonValue, key: &str) -> Option<u16> {
    let v = props.get(key)?;
    if let Some(n) = v.as_u64() {
        return u16::try_from(n).ok();
    }
    v.as_str()?.trim().parse().ok()
}

pub(crate) fn string_prop(props: &JsonValue, key: &str) -> Option<String> {
    props
        .get(key)
        .and_then(JsonValue::as_str)
        .map(String::from)
}

/// Reads the `headers` key-value pairs from a HTTP connector's props.
/// Forms write them as either an object ({k: v}) or an array of
/// {key, value} entries; accept both shapes.
/// Read a key-value prop (object `{k: v}` or array of `{key, value}`) into
/// ordered pairs. Used for context variables, parameters, etc.
pub(crate) fn kv_pairs(props: &JsonValue, key: &str) -> Vec<(String, String)> {
    let raw = match props.get(key) {
        Some(v) => v,
        None => return Vec::new(),
    };
    if let Some(obj) = raw.as_object() {
        return obj
            .iter()
            .filter_map(|(k, v)| {
                let val = v.as_str().map(String::from).unwrap_or_else(|| v.to_string());
                (!k.is_empty()).then(|| (k.clone(), val))
            })
            .collect();
    }
    if let Some(arr) = raw.as_array() {
        return arr
            .iter()
            .filter_map(|item| {
                let k = item.get("key").and_then(|x| x.as_str())?;
                if k.is_empty() {
                    return None;
                }
                let v = item.get("value");
                let val = v
                    .and_then(|x| x.as_str())
                    .map(String::from)
                    .or_else(|| v.map(|x| x.to_string()))
                    .unwrap_or_default();
                Some((k.to_string(), val))
            })
            .collect();
    }
    Vec::new()
}

/// Normalise a user-supplied `responsePath` into a real JSON Pointer.
///
/// The value goes straight to `serde_json::Value::pointer`, which requires a
/// leading `/` and returns `None` for anything else - and a `None` here is
/// indistinguishable from "the response had no rows", so `data` or `$.data[*]`
/// produced an empty result with no error at all. The sibling `totalCountPath`
/// has always been normalised this way; this one was not.
///
/// Accepted and converted:
///   `/data/items`  -> unchanged (already a pointer)
///   `data.items`   -> `/data/items`
///   `$.data[*]`    -> `/data`     (JSONPath, as the field's own label invites)
///
/// `json` is false for the XML flavour, where the same property is an element
/// path rather than a pointer. XML trims its own slashes so a leading one is
/// harmless, but dots are legitimate in element names, so it is left alone.
pub(crate) fn json_pointer_path(raw: &str, json: bool) -> String {
    let s = raw.trim();
    if s.is_empty() || !json {
        return s.to_string();
    }
    if s.starts_with('/') {
        // Already a pointer. A literal dot is a legal pointer segment, so
        // someone who wrote one means it.
        return s.to_string();
    }
    // `$` / `$.` prefix, and any `[...]` subscript, are JSONPath spelling. The
    // subscript is dropped rather than honoured: pointing at the array itself
    // is what the row locator wants, and `[*]` has no pointer equivalent.
    let body = s.strip_prefix('$').unwrap_or(s);
    let mut out = String::new();
    for seg in body.split('.') {
        let seg = match seg.find('[') {
            Some(i) => &seg[..i],
            None => seg,
        };
        if seg.is_empty() {
            continue;
        }
        out.push('/');
        out.push_str(seg);
    }
    out
}

pub(crate) fn headers_from_props(props: &JsonValue) -> Vec<(String, String)> {
    let raw = match props.get("headers") {
        Some(v) => v,
        None => return Vec::new(),
    };
    if let Some(obj) = raw.as_object() {
        return obj
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect();
    }
    if let Some(arr) = raw.as_array() {
        return arr
            .iter()
            .filter_map(|item| {
                let k = item.get("key").and_then(|x| x.as_str())?;
                let v = item.get("value").and_then(|x| x.as_str())?;
                Some((k.to_string(), v.to_string()))
            })
            .collect();
    }
    Vec::new()
}

/// Append the form's `authType` + `authToken` as request headers. Shared by all
/// REST-shaped sources and sinks (src.rest + vendor aliases, snk.rest/webhook,
/// GraphQL) so auth behaves identically everywhere.
///
/// - `bearer` -> `Authorization: Bearer <token>`
/// - `apikey` -> `<header>: <token>` (header chosen by `api_key_header`)
/// - anything else (incl. `none`) adds nothing.
/// src.model: read one model card back (#253).
///
/// Pure SQL, because a card is just a JSON file: the only work is turning
/// `name@version` into a path. `@latest` (or no version at all) reads the
/// pointer snk.model moves on every successful registration, which is what lets
/// a scoring pipeline stay unedited across retrains.
///
/// The row it produces carries the artifact URI, so a downstream code.python
/// stage joins it to its feature table and loads the model itself. The engine
/// deliberately does not load models: that would mean an ML runtime in Rust for
/// something the workspace's own virtualenv already does.
pub(crate) fn build_model_source(props: &JsonValue) -> Result<String, String> {
    let dir = string_prop(props, "path")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "models".to_string());
    let reference = string_prop(props, "model")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            "Model needs a model to read, as name or name@version (name@latest reads the current one)".to_string()
        })?;
    let (name, version) = match reference.rsplit_once('@') {
        Some((n, v)) if !n.trim().is_empty() && !v.trim().is_empty() => (n.trim(), v.trim()),
        _ => (reference.as_str(), "latest"),
    };
    let safe = |s: &str| -> String {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    // Forward slashes so the path is a valid SQL string literal on Windows too.
    let path = format!(
        "{}/{}/{}.json",
        dir.trim_end_matches(['/', '\\']).replace('\\', "/"),
        safe(name),
        safe(version)
    );
    Ok(format!(
        "SELECT * FROM read_json_auto('{}')",
        path.replace('\'', "''")
    ))
}

/// #256: the transport settings on an HTTP-backed node.
///
/// Flat props rather than a nested object because that is what a saved `http`
/// connection flattens into, the same way a saved REST connection flattens into
/// url / authType / authToken. Returns None when nothing is set, so a node
/// without transport config builds the default shared agent exactly as before.
pub(crate) fn http_transport_from_props(props: &JsonValue) -> Option<crate::tls::HttpTransport> {
    let secs = |key: &str| props.get(key).and_then(|v| v.as_u64()).filter(|n| *n > 0);
    let t = crate::tls::HttpTransport {
        proxy: string_prop(props, "httpProxy").filter(|s| !s.trim().is_empty()),
        read_timeout_secs: secs("httpReadTimeoutSecs"),
        connect_timeout_secs: secs("httpConnectTimeoutSecs"),
        user_agent: string_prop(props, "httpUserAgent").filter(|s| !s.trim().is_empty()),
    };
    if t == crate::tls::HttpTransport::default() {
        None
    } else {
        Some(t)
    }
}

pub(crate) fn push_rest_auth(headers: &mut Vec<(String, String)>, props: &JsonValue) {
    let auth_type = string_prop(props, "authType").unwrap_or_else(|| "none".into());
    let token = string_prop(props, "authToken").unwrap_or_default();
    if token.is_empty() {
        return;
    }
    match auth_type.as_str() {
        "bearer" => headers.push(("Authorization".into(), format!("Bearer {}", token))),
        "apikey" => {
            let (name, value) = api_key_header(props, &token);
            headers.push((name, value));
        }
        // HTTP Basic. Several palette summaries (Jira, Zendesk, Twilio, CouchDB,
        // OData, SAP, DHIS2) already tell users to pick Basic, but until this arm
        // existed the match fell through to `_ => {}` and sent NO auth header at
        // all - a silent 401 rather than a configuration error.
        //
        // The credential field carries `user:password` and is encoded here, so a
        // user never has to run base64 by hand and no pre-encoded blob has to be
        // pasted into pipeline JSON. Anything already base64 would be encoded a
        // second time, which is why the form labels the field explicitly.
        "basic" => {
            use base64::engine::general_purpose::STANDARD as B64;
            use base64::Engine as _;
            headers.push((
                "Authorization".into(),
                format!("Basic {}", B64.encode(token.as_bytes())),
            ));
        }
        _ => {}
    }
}

/// #166: read OAuth 2.0 client-credentials config for the Salesforce connectors.
/// Returns `Some(..)` when the form selects the client-credentials grant (via
/// `authMode` on the sink form, or `authType` on the src.rest-shaped source
/// form), erroring if a required field is missing; `None` keeps the default
/// Bearer-token path so existing `${ENV:SF_TOKEN}` pipelines are unchanged.
/// `loginUrl` falls back to `instanceUrl` (a My Domain org serves both the OAuth
/// token endpoint and the data API from the same host).
/// #195 generalizes this to any REST source. Pass `salesforce = true` to keep
/// the #166 behaviour, where the token endpoint is derived from `loginUrl`;
/// otherwise the form supplies an explicit `tokenUrl` (e.g. Xero's
/// `https://identity.xero.com/connect/token`) and may select HTTP Basic client
/// authentication via `clientAuth`.
pub(crate) fn rest_oauth_from_props(
    props: &JsonValue,
    salesforce: bool,
) -> Result<Option<RestOAuth>, EngineError> {
    let mode = string_prop(props, "authMode")
        .or_else(|| string_prop(props, "authType"))
        .unwrap_or_else(|| "bearer".into());
    let is_client_credentials = matches!(
        mode.as_str(),
        "clientCredentials" | "client_credentials" | "oauth" | "oauth_client_credentials"
    );
    if !is_client_credentials {
        return Ok(None);
    }
    let who = if salesforce { "salesforce" } else { "rest" };
    let client_id = string_prop(props, "clientId")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| EngineError::Config(format!(
            "{}: clientId required for OAuth client-credentials auth", who,
        )))?;
    let client_secret = string_prop(props, "clientSecret")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| EngineError::Config(format!(
            "{}: clientSecret required for OAuth client-credentials auth", who,
        )))?;
    let (token_url, client_auth) = if salesforce {
        // A My Domain org serves both the OAuth token endpoint and the data
        // API from the same host, so instanceUrl is an accepted fallback.
        let login_url = string_prop(props, "loginUrl")
            .filter(|s| !s.is_empty())
            .or_else(|| string_prop(props, "instanceUrl").filter(|s| !s.is_empty()))
            .map(|s| s.trim_end_matches('/').to_string())
            .ok_or_else(|| EngineError::Config(
                "salesforce: loginUrl required for OAuth client-credentials auth \
                 (e.g. https://acme.my.salesforce.com)"
                    .into(),
            ))?;
        (
            format!("{}/services/oauth2/token", login_url),
            OAuthClientAuth::Body,
        )
    } else {
        let token_url = string_prop(props, "tokenUrl")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| EngineError::Config(
                "rest: tokenUrl required for OAuth client-credentials auth \
                 (e.g. https://identity.xero.com/connect/token)"
                    .into(),
            ))?;
        let client_auth = match string_prop(props, "clientAuth").as_deref() {
            Some("basic") => OAuthClientAuth::Basic,
            _ => OAuthClientAuth::Body,
        };
        (token_url, client_auth)
    };
    Ok(Some(RestOAuth {
        token_url,
        client_id,
        client_secret,
        scope: string_prop(props, "scope").filter(|s| !s.is_empty()),
        client_auth,
    }))
}

/// Resolve the `(header-name, value)` to send for API-key auth. Precedence:
/// 1. an explicit `authHeader` prop (e.g. `X-Redmine-API-Key`);
/// 2. a token written as `Header-Name: value` - split it, so a key pasted as
///    `X-Redmine-API-Key: abc` lands in the right header (issue #40) even from
///    pipelines built before the header field existed;
/// 3. the default `X-API-Key`.
fn api_key_header(props: &JsonValue, token: &str) -> (String, String) {
    if let Some(header) = string_prop(props, "authHeader")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        return (header, token.to_string());
    }
    if let Some((name, value)) = split_header_token(token) {
        return (name, value);
    }
    ("X-API-Key".to_string(), token.to_string())
}

/// If `token` is `Header-Name: value` with a syntactically valid, hyphenated
/// HTTP header name before the colon, split it into `(name, value)`. Returns
/// `None` otherwise. The hyphen requirement keeps a real key value that merely
/// contains a colon (e.g. `id:secret`) from being mistaken for a header line;
/// custom API-key headers are conventionally hyphenated (`X-...-Key`, `Api-Key`).
fn split_header_token(token: &str) -> Option<(String, String)> {
    let (name, value) = token.split_once(':')?;
    let name = name.trim();
    let value = value.trim();
    if name.is_empty()
        || value.is_empty()
        || !name.contains('-')
        || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return None;
    }
    Some((name.to_string(), value.to_string()))
}

pub(crate) fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub(crate) fn duckle_type_to_duckdb(t: &str) -> String {
    match t.to_lowercase().as_str() {
        "string" | "varchar" | "text" => "VARCHAR".into(),
        "int32" | "int" | "integer" => "INTEGER".into(),
        "int64" | "bigint" => "BIGINT".into(),
        "float32" | "real" | "float" => "REAL".into(),
        "float64" | "double" => "DOUBLE".into(),
        "bool" | "boolean" => "BOOLEAN".into(),
        "date" => "DATE".into(),
        "timestamp" => "TIMESTAMP".into(),
        "time" => "TIME".into(),
        "decimal" => "DECIMAL(18,4)".into(),
        "json" => "JSON".into(),
        "binary" | "blob" => "BLOB".into(),
        "geometry" => "GEOMETRY".into(),
        other => other.to_uppercase(),
    }
}

#[cfg(test)]
mod rest_auth_tests {
    use super::push_rest_auth;
    use serde_json::json;

    fn auth(props: serde_json::Value) -> Vec<(String, String)> {
        let mut h = Vec::new();
        push_rest_auth(&mut h, &props);
        h
    }

    #[test]
    fn none_or_empty_adds_nothing() {
        assert!(auth(json!({})).is_empty());
        assert!(auth(json!({ "authType": "none", "authToken": "x" })).is_empty());
        assert!(auth(json!({ "authType": "apikey", "authToken": "" })).is_empty());
    }

    #[test]
    fn bearer_sets_authorization() {
        assert_eq!(
            auth(json!({ "authType": "bearer", "authToken": "abc" })),
            vec![("Authorization".to_string(), "Bearer abc".to_string())]
        );
    }

    #[test]
    fn apikey_defaults_to_x_api_key() {
        assert_eq!(
            auth(json!({ "authType": "apikey", "authToken": "abc" })),
            vec![("X-API-Key".to_string(), "abc".to_string())]
        );
    }

    #[test]
    fn apikey_explicit_header_wins() {
        assert_eq!(
            auth(json!({
                "authType": "apikey",
                "authToken": "abc",
                "authHeader": "X-Redmine-API-Key"
            })),
            vec![("X-Redmine-API-Key".to_string(), "abc".to_string())]
        );
    }

    #[test]
    fn apikey_splits_header_value_token() {
        // Issue #40: a token pasted as "Header: value" lands in the right header.
        assert_eq!(
            auth(json!({ "authType": "apikey", "authToken": "X-Redmine-API-Key: secret123" })),
            vec![("X-Redmine-API-Key".to_string(), "secret123".to_string())]
        );
    }

    #[test]
    fn apikey_does_not_split_colon_value_without_hyphen() {
        // A real "id:secret" style key must NOT be mistaken for a header line.
        assert_eq!(
            auth(json!({ "authType": "apikey", "authToken": "id:secret" })),
            vec![("X-API-Key".to_string(), "id:secret".to_string())]
        );
    }

    #[test]
    fn explicit_header_overrides_colon_in_token() {
        // When the header is named explicitly, the token is used verbatim.
        assert_eq!(
            auth(json!({
                "authType": "apikey",
                "authToken": "a:b:c",
                "authHeader": "X-Custom"
            })),
            vec![("X-Custom".to_string(), "a:b:c".to_string())]
        );
    }
}

#[cfg(test)]
mod db_attach_tests {
    use super::db_attach;
    use serde_json::json;

    #[test]
    fn special_char_password_is_quoted() {
        // A password with a space (and other specials like ? and {) must be
        // single-quoted in the libpq key-value form, then SQL-escaped (each
        // single quote doubled). Issue #157.
        let sql = db_attach(
            &json!({ "host": "h", "database": "db", "user": "u", "password": "p@ss w?rd{" }),
            "mysql",
            3306,
            true,
        );
        assert!(sql.contains("password=''p@ss w?rd{''"), "password not quoted: {}", sql);
        assert!(sql.contains("AS duckle_src (TYPE MYSQL, READ_ONLY)"), "wrong attach: {}", sql);
    }

    #[test]
    fn quote_in_password_is_escaped() {
        // libpq value 'pa\'ss'  ->  sql_escape doubles the quotes: ''pa\''ss''
        let sql = db_attach(
            &json!({ "host": "h", "user": "u", "password": "pa'ss" }),
            "mysql",
            3306,
            true,
        );
        assert!(sql.contains(r"password=''pa\''ss''"), "quote not escaped: {}", sql);
    }

    #[test]
    fn simple_password_stays_bare() {
        let sql = db_attach(
            &json!({ "host": "h", "user": "u", "password": "simplepass" }),
            "mysql",
            3306,
            true,
        );
        assert!(sql.contains("password=simplepass"), "should be bare: {}", sql);
    }

    #[test]
    fn read_only_can_be_disabled() {
        // #157: some MySQL / extension versions reject the READ_ONLY attach
        // option. readOnly=false drops it but keeps the duckle_src alias.
        for ro in [json!(false), json!("false"), json!("off")] {
            let sql = db_attach(
                &json!({ "host": "h", "user": "u", "password": "x", "readOnly": ro }),
                "mysql",
                3306,
                true,
            );
            assert!(!sql.contains("READ_ONLY"), "READ_ONLY should be gone: {}", sql);
            assert!(sql.contains("AS duckle_src (TYPE MYSQL)"), "wrong attach: {}", sql);
        }
    }

    #[test]
    fn read_only_kept_by_default() {
        let sql = db_attach(
            &json!({ "host": "h", "user": "u", "password": "x" }),
            "mysql",
            3306,
            true,
        );
        assert!(sql.contains(", READ_ONLY"), "READ_ONLY should stay by default: {}", sql);
    }

    #[test]
    fn conn_string_override_replaces_built_parts() {
        // #157: a user-supplied connection string is used verbatim, overriding
        // host/password, but Duckle keeps its duckle_src alias.
        let sql = db_attach(
            &json!({
                "connString": "mysql://user:p%40ss@dbhost:3306/app",
                "host": "ignored",
                "password": "ignored",
            }),
            "mysql",
            3306,
            true,
        );
        assert!(
            sql.contains("ATTACH 'mysql://user:p%40ss@dbhost:3306/app' AS duckle_src (TYPE MYSQL, READ_ONLY)"),
            "override not used: {}",
            sql
        );
    }

    #[test]
    fn conn_string_override_works_without_host() {
        // Without connString an empty host returns "" (no ATTACH); with an
        // override there is no host requirement.
        let sql = db_attach(&json!({ "connString": "host=h dbname=db" }), "postgres", 5432, true);
        assert!(sql.contains("ATTACH 'host=h dbname=db' AS duckle_src"), "{}", sql);
    }

    #[test]
    fn postgres_ssl_and_advanced_opts_appended() {
        // #161: TLS-enforcing servers need sslmode; regulated setups add
        // root/client certs, a connect timeout, and session options.
        let sql = db_attach(
            &json!({
                "host": "h", "database": "db", "user": "u", "password": "p",
                "sslmode": "verify-full",
                "sslrootcert": "/etc/ssl/root.crt",
                "sslcert": "/etc/ssl/client.crt",
                "sslkey": "/etc/ssl/client.key",
                "connectTimeout": 10,
                "options": "-c search_path=app",
            }),
            "postgres",
            5432,
            true,
        );
        assert!(sql.contains("sslmode=verify-full"), "sslmode missing: {}", sql);
        assert!(sql.contains("sslrootcert=/etc/ssl/root.crt"), "rootcert missing: {}", sql);
        assert!(sql.contains("sslcert=/etc/ssl/client.crt"), "cert missing: {}", sql);
        assert!(sql.contains("sslkey=/etc/ssl/client.key"), "key missing: {}", sql);
        assert!(sql.contains("connect_timeout=10"), "timeout missing: {}", sql);
        // `options` has a space, so conn_kv_quote single-quotes it and sql_escape
        // then doubles those quotes.
        assert!(sql.contains("options=''-c search_path=app''"), "options not quoted: {}", sql);
    }

    #[test]
    fn connect_timeout_accepts_numeric_string() {
        let sql = db_attach(
            &json!({ "host": "h", "connectTimeout": "15" }),
            "postgres",
            5432,
            true,
        );
        assert!(sql.contains("connect_timeout=15"), "string timeout not parsed: {}", sql);
    }

    #[test]
    fn mysql_ignores_postgres_ssl_fields() {
        // sslmode / sslrootcert are libpq names; the mysql extension uses
        // different keys, so they must not leak into a mysql DSN.
        let sql = db_attach(
            &json!({ "host": "h", "sslmode": "require", "sslrootcert": "/x.crt" }),
            "mysql",
            3306,
            true,
        );
        assert!(!sql.contains("sslmode"), "sslmode should be postgres-only: {}", sql);
        assert!(!sql.contains("sslrootcert"), "rootcert should be postgres-only: {}", sql);
    }

    #[test]
    fn free_text_conn_params_passthrough() {
        // connParams is appended verbatim and works for both families (here a
        // mysql-specific ssl_mode the structured fields do not model).
        let sql = db_attach(
            &json!({ "host": "h", "connParams": "ssl_mode=REQUIRED ssl_ca=/ca.pem" }),
            "mysql",
            3306,
            true,
        );
        assert!(sql.contains("ssl_mode=REQUIRED ssl_ca=/ca.pem"), "passthrough missing: {}", sql);
    }

    #[test]
    fn conn_string_override_ignores_advanced_opts() {
        // A raw connString is used verbatim; the structured advanced fields
        // (which the user would instead encode inside their own string) are
        // not appended.
        let sql = db_attach(
            &json!({ "connString": "host=h dbname=db", "sslmode": "require", "connParams": "x=1" }),
            "postgres",
            5432,
            true,
        );
        assert!(sql.contains("ATTACH 'host=h dbname=db' AS duckle_src"), "{}", sql);
        assert!(!sql.contains("sslmode"), "advanced fields must not append to override: {}", sql);
        assert!(!sql.contains("x=1"), "passthrough must not append to override: {}", sql);
    }
}

#[cfg(test)]
mod pushdown_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pushdown_emits_postgres_query() {
        let sql = build_relational_source(
            "src.postgres",
            &json!({ "mode": "sql", "sql": "SELECT count(*) FROM orders", "pushdown": true }),
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM postgres_query('duckle_src', 'SELECT count(*) FROM orders')"
        );
    }

    #[test]
    fn pushdown_uses_mysql_query_for_mysql_family() {
        // sql prop alone (mode left at default) still triggers the custom-SQL path.
        let sql = build_relational_source(
            "src.mariadb",
            &json!({ "sql": "SELECT 1", "pushdown": true }),
        )
        .unwrap();
        assert_eq!(sql, "SELECT * FROM mysql_query('duckle_src', 'SELECT 1')");
    }

    #[test]
    fn pushdown_escapes_single_quotes_and_trims_semicolon() {
        let sql = build_relational_source(
            "src.postgres",
            &json!({ "sql": "SELECT 'x';", "pushdown": true }),
        )
        .unwrap();
        // The query becomes a string-literal argument, so single quotes double
        // and the trailing semicolon is trimmed.
        assert_eq!(
            sql,
            "SELECT * FROM postgres_query('duckle_src', 'SELECT ''x''')"
        );
    }

    #[test]
    fn pushdown_off_keeps_subquery_wrap() {
        let sql =
            build_relational_source("src.postgres", &json!({ "sql": "SELECT 1" })).unwrap();
        assert_eq!(sql, "(SELECT 1)");
    }

    #[test]
    fn pushdown_ignored_for_family_without_passthrough() {
        // BigQuery has no postgres_query/mysql_query; falls back to the wrap even
        // with pushdown requested, so it can never emit invalid SQL.
        let sql = build_relational_source(
            "src.bigquery",
            &json!({ "sql": "SELECT 1", "pushdown": true }),
        )
        .unwrap();
        assert_eq!(sql, "(SELECT 1)");
    }

    #[test]
    fn pushdown_does_not_affect_whole_table_reads() {
        let sql = build_relational_source(
            "src.postgres",
            &json!({ "tableName": "orders", "pushdown": true }),
        )
        .unwrap();
        assert_eq!(sql, "SELECT * FROM duckle_src.\"public\".\"orders\"");
    }
}

#[cfg(test)]
mod sql_override_tests {
    use super::*;
    use serde_json::json;

    fn inputs_with(ports: &[(&str, &str)]) -> NodeInputs {
        let mut ni = NodeInputs::default();
        for (port, up) in ports {
            ni.ports.entry(port.to_string()).or_default().push(up.to_string());
        }
        ni
    }

    #[test]
    fn override_wraps_single_input_as_cte() {
        let ni = inputs_with(&[("main", "up_a")]);
        let sql = build_view_sql(
            "xf.filter",
            &json!({ "sqlOverride": "SELECT * FROM input WHERE x > 0" }),
            &ni,
            None,
            false,
        )
        .unwrap();
        assert!(sql.starts_with("WITH input AS (SELECT * FROM \"up_a\")"), "{}", sql);
        assert!(sql.contains("input1 AS (SELECT * FROM \"up_a\")"), "{}", sql);
        assert!(sql.ends_with("SELECT * FROM input WHERE x > 0"), "{}", sql);
    }

    #[test]
    fn override_exposes_multiple_inputs_main_first() {
        let ni = inputs_with(&[("main", "up_main"), ("lookup", "up_look")]);
        let sql = build_view_sql(
            "xf.join",
            &json!({ "sqlOverride": "SELECT * FROM input1 JOIN input2 USING (id)" }),
            &ni,
            None,
            false,
        )
        .unwrap();
        assert!(sql.contains("input AS (SELECT * FROM \"up_main\")"), "{}", sql);
        assert!(sql.contains("input1 AS (SELECT * FROM \"up_main\")"), "{}", sql);
        assert!(sql.contains("input2 AS (SELECT * FROM \"up_look\")"), "{}", sql);
    }

    #[test]
    fn override_without_input_is_verbatim() {
        let ni = NodeInputs::default();
        let sql = build_view_sql("src.csv", &json!({ "sqlOverride": "SELECT 1 AS x" }), &ni, None, false).unwrap();
        assert_eq!(sql, "SELECT 1 AS x");
    }

    #[test]
    fn blank_override_falls_through_to_generated() {
        // A whitespace-only override is ignored: the normal builder runs. A
        // ctl.replicate stage just re-selects its upstream, no WITH-input wrapper.
        let ni = inputs_with(&[("main", "up_a")]);
        let sql = build_view_sql("ctl.replicate", &json!({ "sqlOverride": "   " }), &ni, None, false).unwrap();
        assert!(!sql.contains("input AS (SELECT"), "override should have been ignored: {}", sql);
        assert!(sql.contains("SELECT * FROM \"up_a\""), "{}", sql);
    }
}

#[cfg(test)]
mod geom_dq_tests {
    use super::*;
    use serde_json::json;

    fn one_input() -> NodeInputs {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        ni
    }

    #[test]
    fn validate_flag_adds_is_valid_column() {
        let sql = build_geom_validate(&one_input(), &json!({})).unwrap();
        assert_eq!(sql, "SELECT *, ST_IsValid(CAST(\"geometry\" AS GEOMETRY)) AS is_valid FROM \"up\"");
    }

    #[test]
    fn validate_filters_and_honors_column() {
        let valid = build_geom_validate(&one_input(), &json!({"mode":"valid","geometryColumn":"geom"})).unwrap();
        assert_eq!(valid, "SELECT * FROM \"up\" WHERE ST_IsValid(CAST(\"geom\" AS GEOMETRY))");
        let invalid = build_geom_validate(&one_input(), &json!({"mode":"invalid"})).unwrap();
        assert_eq!(invalid, "SELECT * FROM \"up\" WHERE NOT ST_IsValid(CAST(\"geometry\" AS GEOMETRY))");
    }

    #[test]
    fn repair_all_replaces_in_place() {
        let sql = build_geom_repair(&one_input(), &json!({})).unwrap();
        assert_eq!(sql, "SELECT * REPLACE (ST_MakeValid(CAST(\"geometry\" AS GEOMETRY)) AS \"geometry\") FROM \"up\"");
    }

    #[test]
    fn repair_invalid_only_uses_case() {
        let sql = build_geom_repair(&one_input(), &json!({"mode":"invalid"})).unwrap();
        assert_eq!(
            sql,
            "SELECT * REPLACE (CASE WHEN ST_IsValid(CAST(\"geometry\" AS GEOMETRY)) THEN CAST(\"geometry\" AS GEOMETRY) ELSE ST_MakeValid(CAST(\"geometry\" AS GEOMETRY)) END AS \"geometry\") FROM \"up\""
        );
    }

    #[test]
    fn empty_flag_and_filters() {
        assert_eq!(
            build_geom_empty(&one_input(), &json!({})).unwrap(),
            "SELECT *, ST_IsEmpty(CAST(\"geometry\" AS GEOMETRY)) AS is_empty FROM \"up\""
        );
        assert_eq!(
            build_geom_empty(&one_input(), &json!({"mode":"empty"})).unwrap(),
            "SELECT * FROM \"up\" WHERE ST_IsEmpty(CAST(\"geometry\" AS GEOMETRY))"
        );
        assert_eq!(
            build_geom_empty(&one_input(), &json!({"mode":"nonempty"})).unwrap(),
            "SELECT * FROM \"up\" WHERE NOT ST_IsEmpty(CAST(\"geometry\" AS GEOMETRY))"
        );
    }

    #[test]
    fn spatial_extension_is_force_loaded() {
        for id in ["qa.geomvalidate", "qa.geomrepair", "qa.geomempty"] {
            assert!(attach_prelude(id, &json!({})).contains("LOAD spatial"), "{} missing spatial prelude", id);
        }
    }

    #[test]
    fn crs_aware_measurements_pick_planar_or_spheroid() {
        // Issue #177: each measurement resolves the CRS unit from typeof(col),
        // errors when there is none, and picks the planar/spheroidal function.
        let cases = [
            ("xf.geo.length", build_geo_length as fn(&NodeInputs, &JsonValue) -> Result<String, String>, "ST_Length", "ST_Length_Spheroid", "length"),
            ("xf.geo.perimeter", build_geo_perimeter, "ST_Perimeter", "ST_Perimeter_Spheroid", "perimeter"),
            ("xf.geo.area", build_geo_area, "ST_Area", "ST_Area_Spheroid", "area"),
        ];
        for (id, f, planar, spheroid, out) in cases {
            let sql = f(&one_input(), &json!({"geomColumn": "geom"})).unwrap();
            assert!(sql.contains("duckdb_coordinate_systems()"), "{id}: no CRS lookup");
            assert!(sql.contains("regexp_extract(typeof("), "{id}: no typeof CRS probe");
            assert!(sql.contains("does not have a valid Coordinate Reference System"), "{id}: no no-CRS error");
            assert!(sql.contains(&format!("{spheroid}(CAST(")), "{id}: missing {spheroid}");
            // Planar arm must be the bare function, not a prefix of the spheroid one.
            assert!(sql.contains(&format!("ELSE {planar}(CAST(")), "{id}: missing planar {planar}");
            assert!(sql.contains(&format!("AS \"{out}\"")), "{id}: wrong output column");
        }
        // Distance carries a second (target) geometry argument.
        let dist = build_geo_distance(
            &one_input(),
            &json!({"geomColumn": "geom", "targetWkt": "POINT(0 0)"}),
        )
        .unwrap();
        assert!(dist.contains("ST_Distance_Spheroid(CAST(__g.\"geom\" AS GEOMETRY), ST_GeomFromText('POINT(0 0)'))"));
        assert!(dist.contains("ELSE ST_Distance(CAST(__g.\"geom\" AS GEOMETRY), ST_GeomFromText('POINT(0 0)'))"));
        // The measurements pin axis order so the *_Spheroid functions do not warn.
        for id in ["xf.geo.distance", "xf.geo.length", "xf.geo.perimeter", "xf.geo.area"] {
            let p = attach_prelude(id, &json!({}));
            assert!(p.contains("LOAD spatial"), "{id} missing spatial prelude");
            assert!(p.contains("geometry_always_xy"), "{id} missing axis-order pin");
        }
    }
}

#[cfg(test)]
mod geo_projection_tests {
    use super::*;
    use serde_json::json;

    fn one_input() -> NodeInputs {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        ni
    }

    #[test]
    fn setcrs_replaces_geometry_in_place() {
        // #188 Define Projection: assign the CRS, coordinates untouched.
        let sql = build_geo_setcrs(
            &one_input(),
            &json!({"geomColumn": "geom", "crs": "EPSG:4326"}),
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * REPLACE (ST_SetCRS(CAST(\"geom\" AS GEOMETRY), 'EPSG:4326') AS \"geom\") FROM \"up\""
        );
    }

    #[test]
    fn setcrs_needs_column_and_crs() {
        assert!(build_geo_setcrs(&one_input(), &json!({"crs": "EPSG:4326"})).is_err());
        assert!(build_geo_setcrs(&one_input(), &json!({"geomColumn": "geom"})).is_err());
    }

    #[test]
    fn reproject_wraps_setcrs_in_transform_with_always_xy() {
        // #189 Reproject: ST_Transform(ST_SetCRS(...), target, always_xy).
        let sql = build_geo_reproject(
            &one_input(),
            &json!({"geomColumn": "geom", "sourceCrs": "EPSG:4326", "targetCrs": "EPSG:3857"}),
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * REPLACE (ST_Transform(ST_SetCRS(CAST(\"geom\" AS GEOMETRY), 'EPSG:4326'), 'EPSG:3857', always_xy := true) AS \"geom\") FROM \"up\""
        );
        // always_xy is overridable.
        let off = build_geo_reproject(
            &one_input(),
            &json!({"geomColumn": "geom", "sourceCrs": "EPSG:4326", "targetCrs": "EPSG:3857", "alwaysXy": false}),
        )
        .unwrap();
        assert!(off.contains("always_xy := false"));
    }

    #[test]
    fn reproject_rejects_identical_source_and_target() {
        let err = build_geo_reproject(
            &one_input(),
            &json!({"geomColumn": "geom", "sourceCrs": "EPSG:4326", "targetCrs": "EPSG:4326"}),
        )
        .unwrap_err();
        assert!(err.contains("nothing to reproject"), "{err}");
    }

    #[test]
    fn projection_tools_force_load_spatial() {
        for id in ["xf.geo.setcrs", "xf.geo.reproject", "xf.geo.create"] {
            assert!(
                attach_prelude(id, &json!({})).contains("LOAD spatial"),
                "{id} missing spatial prelude"
            );
        }
    }

    #[test]
    fn create_geometry_from_xy_wkt_wkb() {
        // #190. X/Y: ST_Point with a CRS, source columns dropped by default.
        let xy = build_geo_create(
            &one_input(),
            &json!({"source": "xy", "xColumn": "lon", "yColumn": "lat", "crs": "EPSG:4326"}),
        )
        .unwrap();
        assert_eq!(
            xy,
            "SELECT * EXCLUDE (\"lon\", \"lat\"), ST_SetCRS(ST_Point(CAST(\"lon\" AS DOUBLE), CAST(\"lat\" AS DOUBLE)), 'EPSG:4326') AS \"geom\" FROM \"up\""
        );
        // WKT, keep source column, custom output name.
        let wkt = build_geo_create(
            &one_input(),
            &json!({"source": "wkt", "wktColumn": "shape", "crs": "EPSG:4326", "outputColumn": "g", "removeSource": false}),
        )
        .unwrap();
        assert_eq!(
            wkt,
            "SELECT *, ST_SetCRS(ST_GeomFromText(CAST(\"shape\" AS VARCHAR)), 'EPSG:4326') AS \"g\" FROM \"up\""
        );
        // WKB, no CRS -> no ST_SetCRS wrapper.
        let wkb = build_geo_create(
            &one_input(),
            &json!({"source": "wkb", "wkbColumn": "raw"}),
        )
        .unwrap();
        assert_eq!(
            wkb,
            "SELECT * EXCLUDE (\"raw\"), ST_GeomFromWKB(\"raw\") AS \"geom\" FROM \"up\""
        );
    }

    #[test]
    fn create_geometry_validates_source_and_columns() {
        assert!(build_geo_create(&one_input(), &json!({"source": "xy", "yColumn": "lat"})).is_err());
        assert!(build_geo_create(&one_input(), &json!({"source": "wkt"})).is_err());
        assert!(build_geo_create(&one_input(), &json!({"source": "nope"})).is_err());
    }
}

#[cfg(test)]
mod rest_oauth_tests {
    use super::rest_oauth_from_props;
    use crate::plan::OAuthClientAuth;
    use serde_json::json;

    /// #166 contract: Salesforce derives its token endpoint from loginUrl and
    /// sends credentials in the POST body. This must not drift when the OAuth
    /// path was generalized in #195 - it is a shipped, live-verified connector.
    #[test]
    fn salesforce_derives_token_url_from_login_url() {
        let p = json!({
            "authMode": "clientCredentials",
            "clientId": "cid",
            "clientSecret": "secret",
            "loginUrl": "https://acme.my.salesforce.com",
        });
        let o = rest_oauth_from_props(&p, true).unwrap().expect("oauth built");
        assert_eq!(o.token_url, "https://acme.my.salesforce.com/services/oauth2/token");
        assert_eq!(o.client_auth, OAuthClientAuth::Body);
        assert_eq!(o.scope, None);
        assert_eq!(o.client_id, "cid");
        assert_eq!(o.client_secret, "secret");
    }

    #[test]
    fn salesforce_trailing_slash_does_not_double_up() {
        let p = json!({
            "authMode": "clientCredentials",
            "clientId": "cid", "clientSecret": "s",
            "loginUrl": "https://acme.my.salesforce.com/",
        });
        let o = rest_oauth_from_props(&p, true).unwrap().unwrap();
        assert_eq!(o.token_url, "https://acme.my.salesforce.com/services/oauth2/token");
    }

    #[test]
    fn salesforce_falls_back_to_instance_url() {
        let p = json!({
            "authMode": "clientCredentials",
            "clientId": "cid", "clientSecret": "s",
            "instanceUrl": "https://acme.my.salesforce.com",
        });
        let o = rest_oauth_from_props(&p, true).unwrap().unwrap();
        assert_eq!(o.token_url, "https://acme.my.salesforce.com/services/oauth2/token");
    }

    /// #195: a non-Salesforce REST source supplies its own token endpoint.
    #[test]
    fn generic_rest_source_uses_explicit_token_url() {
        let p = json!({
            "authType": "clientCredentials",
            "clientId": "cid", "clientSecret": "s",
            "tokenUrl": "https://identity.xero.com/connect/token",
            "clientAuth": "basic",
            "scope": "accounting.transactions",
        });
        let o = rest_oauth_from_props(&p, false).unwrap().unwrap();
        assert_eq!(o.token_url, "https://identity.xero.com/connect/token");
        assert_eq!(o.client_auth, OAuthClientAuth::Basic);
        assert_eq!(o.scope.as_deref(), Some("accounting.transactions"));
    }

    #[test]
    fn generic_defaults_to_body_client_auth() {
        let p = json!({
            "authType": "oauth",
            "clientId": "cid", "clientSecret": "s",
            "tokenUrl": "https://example.com/token",
        });
        let o = rest_oauth_from_props(&p, false).unwrap().unwrap();
        assert_eq!(o.client_auth, OAuthClientAuth::Body);
    }

    #[test]
    fn generic_without_token_url_is_a_config_error() {
        let p = json!({
            "authType": "clientCredentials",
            "clientId": "cid", "clientSecret": "s",
        });
        let err = rest_oauth_from_props(&p, false).unwrap_err().to_string();
        assert!(err.contains("tokenUrl"), "expected tokenUrl error, got: {}", err);
    }

    /// The default Bearer path must stay untouched so existing ${ENV:TOKEN}
    /// pipelines keep working on every REST alias.
    #[test]
    fn bearer_mode_resolves_to_none_for_both_shapes() {
        let p = json!({ "authType": "bearer", "authToken": "tok" });
        assert!(rest_oauth_from_props(&p, true).unwrap().is_none());
        assert!(rest_oauth_from_props(&p, false).unwrap().is_none());
        let empty = json!({});
        assert!(rest_oauth_from_props(&empty, false).unwrap().is_none());
    }
}
