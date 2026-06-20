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
        "iceberg" => build_iceberg_source(props),
        "delta" => build_delta_source(props),
        "spatial" => build_spatial_source(props),
        "fixedwidth" => return build_fixedwidth_source(props).ok(),
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
        _ => return None,
    })
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
        "src.iceberg" => Ok(build_iceberg_source(props)),
        "src.delta" => Ok(build_delta_source(props)),
        "src.spatial" => Ok(build_spatial_source(props)),
        "src.fixedwidth" => build_fixedwidth_source(props),
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
        "xf.union" => build_union(inputs, true),
        "xf.unionall" => build_union(inputs, false),
        "xf.intersect" => build_setop(inputs, "INTERSECT"),
        "xf.except" => build_setop(inputs, "EXCEPT"),
        "xf.addcol" | "xf.coalesce" => build_addcol(inputs, props),
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
        "xf.geo.buffer" => build_geo_buffer(inputs, props),
        "xf.geo.intersects" => build_geo_intersects(inputs, props),
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
        "ctl.merge" => build_union(inputs, false),
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
    let predicate = filter_predicate_sql(props.get("predicate"))
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

pub(crate) fn build_sort(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let sort_keys: Vec<String> = props
        .get("orderBy")
        .and_then(JsonValue::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    if let Some(s) = v.as_str() {
                        Some(s.to_string())
                    } else if let Some(obj) = v.as_object() {
                        let col = obj.get("column").and_then(JsonValue::as_str)?;
                        let dir = obj
                            .get("direction")
                            .and_then(JsonValue::as_str)
                            .unwrap_or("asc");
                        // Allowlist the direction: an unexpected token spliced
                        // raw would make a malformed ORDER BY / parser error
                        // (audit B5). Map asc/desc explicitly; anything else
                        // falls back to ASC, matching the single-column branch.
                        let dir_kw = match dir.trim().to_ascii_lowercase().as_str() {
                            "desc" => "DESC",
                            _ => "ASC",
                        };
                        Some(format!("{} {}", quote_ident(col), dir_kw))
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let mut sort_keys = sort_keys;
    // The Sort form writes a single sortColumn + direction + nullsLast.
    if sort_keys.is_empty() {
        if let Some(col) = string_prop(props, "sortColumn").filter(|s| !s.is_empty()) {
            let dir = if string_prop(props, "direction").as_deref() == Some("desc") {
                "DESC"
            } else {
                "ASC"
            };
            let nulls = if props.get("nullsLast").and_then(JsonValue::as_bool).unwrap_or(true) {
                " NULLS LAST"
            } else {
                " NULLS FIRST"
            };
            sort_keys.push(format!("{} {}{}", quote_ident(&col), dir, nulls));
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
        let column = agg.get("column").and_then(JsonValue::as_str).unwrap_or("*");
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

pub(crate) fn build_union(inputs: &NodeInputs, distinct: bool) -> Result<String, String> {
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
    let op = if distinct {
        " UNION BY NAME "
    } else {
        " UNION ALL BY NAME "
    };
    Ok(mains
        .iter()
        .map(|id| format!("SELECT * FROM {}", quote_ident(id)))
        .collect::<Vec<_>>()
        .join(op))
}

pub(crate) fn build_setop(inputs: &NodeInputs, op: &str) -> Result<String, String> {
    let mains = inputs.all_main_ports();
    if mains.len() < 2 {
        return Err(format!("{} needs two inputs", op));
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
        "xf.num.round" => format!("round({}, {})", col, arg.unwrap_or_else(|| "0".into())),
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
    Ok(if reject {
        format!("SELECT * FROM {} WHERE NOT COALESCE(({}), FALSE)", from, predicate)
    } else {
        format!("SELECT * FROM {} WHERE COALESCE(({}), FALSE)", from, predicate)
    })
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
        _ => Ok(None),
    }
}

pub(crate) fn columns_list(props: &JsonValue, key: &str) -> Vec<String> {
    props
        .get(key)
        .and_then(JsonValue::as_array)
        .map(|arr| {
            arr.iter()
                // Drop empty / whitespace-only entries: a blank column name is
                // never valid and would otherwise pass length-based guards (e.g.
                // upsert conflictColumns=[""]) and emit a zero-length quoted
                // identifier. Non-empty names are kept verbatim (a column may
                // legitimately contain surrounding spaces).
                .filter_map(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
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

pub(crate) fn build_rename(inputs: &NodeInputs, props: &JsonValue) -> Result<String, String> {
    let upstream = inputs.main().ok_or_else(|| "missing main input".to_string())?;
    let pairs = rename_pairs(props);
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
        let terms: Vec<String> = outputs
            .iter()
            .map(|(name, expr)| format!("{} AS {}", strip_port_prefixes(expr), quote_ident(name)))
            .collect();
        let mut sql = format!("SELECT {} FROM {}", terms.join(", "), quote_ident(upstream));
        if let Some(predicate) = &filter {
            sql.push_str(" WHERE ");
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
                format!("{}.{} = {}.{}", main_alias, quote_ident(lk), look_alias, quote_ident(rk))
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        from.push_str(&format!(" {} JOIN {} ON {}", l.kind, look_alias, on));
    }

    let mut sql = format!("SELECT {} FROM {}", terms.join(", "), from);
    if let Some(predicate) = &filter {
        sql.push_str(" WHERE ");
        sql.push_str(&qualify_port_refs(predicate, &aliases));
    }
    Ok(sql)
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
pub(crate) fn parse_key_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

pub(crate) fn build_join(inputs: &NodeInputs, props: &JsonValue, kind: &str) -> Result<String, String> {
    let left = inputs.main().ok_or_else(|| "join: missing main input".to_string())?;
    let right = inputs
        .first_lookup()
        .ok_or_else(|| "join: missing lookup input".to_string())?;
    let left_keys = parse_key_list(
        props
            .get("leftKey")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "join: leftKey property required".to_string())?,
    );
    let right_keys = parse_key_list(
        props
            .get("rightKey")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "join: rightKey property required".to_string())?,
    );
    if left_keys.is_empty() || right_keys.is_empty() {
        return Err("join: leftKey and rightKey cannot be empty".into());
    }
    if left_keys.len() != right_keys.len() {
        return Err(format!(
            "join: leftKey and rightKey must have the same number of columns (got {} vs {})",
            left_keys.len(),
            right_keys.len()
        ));
    }
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
    if let Some(q) = quote.as_deref().filter(|s| !s.is_empty()) {
        args.push(format!("quote='{}'", sql_escape(q)));
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
    args
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
        let mut pairs = Vec::with_capacity(cols.len());
        let mut replaces = Vec::new();
        for c in cols {
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
        args.push(format!("types = {{{}}}", pairs.join(", ")));
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

pub(crate) fn build_json_source(props: &JsonValue) -> String {
    let path = string_prop(props, "path").unwrap_or_default();
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
            format!(
                "SELECT unnest({}, recursive := true) FROM read_json_auto('{}', maximum_object_size=104857600)",
                accessor,
                sql_escape(&path)
            )
        }
        None => format!(
            "SELECT * FROM read_json_auto('{}', maximum_object_size=104857600)",
            sql_escape(&path)
        ),
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
pub(crate) fn attach_prelude(component_id: &str, props: &JsonValue) -> String {
    // Network DBs use host/port + libpq-style fields, not the
    // file-style `database` path the file-based ATTACH connectors use.
    // Cockroach speaks PG wire so it rides the postgres extension;
    // MariaDB speaks MySQL wire so it rides the mysql extension.
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
        "src.spatial"
        | "snk.spatial"
        | "xf.geo.distance"
        | "xf.geo.buffer"
        | "xf.geo.intersects"
        | "xf.join.spatial" => {
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
pub(crate) fn db_attach(props: &JsonValue, extension: &str, default_port: u64, read_only: bool) -> String {
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
    let mut parts = vec![format!("host={}", host), format!("port={}", port)];
    if let Some(db) = string_prop(props, "database").filter(|s| !s.is_empty()) {
        parts.push(format!("{}={}", db_key, db));
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
    let connstr = parts.join(" ");
    let (alias, mode) = if read_only {
        ("duckle_src", ", READ_ONLY")
    } else {
        ("duckle_dst", "")
    };
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
    let sql = string_prop(props, "sql").filter(|s| !s.trim().is_empty());
    if mode == "sql" || sql.is_some() {
        let sql = sql.ok_or_else(|| format!("{}: SQL query is empty", component_id))?;
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
    matches!(
        component_id,
        "snk.duckdb" | "snk.sqlite" | "snk.motherduck" | "snk.ducklake" | "snk.quack"
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
    let schema = string_prop(props, "schemaName").filter(|s| !s.is_empty());
    let mode = string_prop(props, "mode").unwrap_or_else(|| "overwrite".into());
    let qual = relational_qualified("duckle_dst", component_id, schema.as_deref(), &table);
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
    let (alias, mode) = if read_only {
        ("duckle_src", " (READ_ONLY)")
    } else {
        ("duckle_dst", "")
    };
    format!(
        "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:{}' AS {}{}; ",
        sql_escape(&path),
        alias,
        mode
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

/// Quack remote protocol (DuckDB 2.0+, May 2026). The remote DuckDB
/// instance runs `quack_serve(...)` on port 9494 by default and exposes
/// its database to multiple concurrent clients over HTTP using a
/// custom `application/duckdb` MIME type. Client side: a SECRET
/// carries the auth token, then ATTACH names the URL.
///
/// Requires DuckDB built with quack support; older builds will surface
/// a clear error at runtime ("Unknown ATTACH option 'TYPE'" or
/// similar) without any Duckle-side breakage.
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
    format!(
        "COPY (SELECT * FROM {}) TO '{}' (FORMAT GDAL, DRIVER '{}')",
        quote_ident(from_view),
        sql_escape(&path),
        sql_escape(&driver)
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
/// ST_Distance over CAST geometries. Units come from the SRS of the
/// input geometry (degrees for plain WGS84, metres for projected SRS).
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
    Ok(format!(
        "SELECT *, ST_Distance(CAST({col} AS GEOMETRY), ST_GeomFromText('{target}')) AS {out} FROM {up}",
        col = quote_ident(&column),
        target = target.replace('\'', "''"),
        out = quote_ident(&output),
        up = quote_ident(upstream)
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
    let relation = string_prop(props, "relation").unwrap_or_else(|| "intersects".into());
    let fn_name = match relation.as_str() {
        "contains" => "ST_Contains",
        "within" => "ST_Within",
        "touches" => "ST_Touches",
        "crosses" => "ST_Crosses",
        "overlaps" => "ST_Overlaps",
        "equals" => "ST_Equals",
        _ => "ST_Intersects",
    };
    let kind = match string_prop(props, "joinType").as_deref() {
        Some("left") => "LEFT",
        _ => "INNER",
    };
    Ok(format!(
        "SELECT m.*, r.* FROM {} m {} JOIN {} r ON {}(CAST(m.{} AS GEOMETRY), CAST(r.{} AS GEOMETRY))",
        quote_ident(left),
        kind,
        quote_ident(right),
        fn_name,
        quote_ident(&left_col),
        quote_ident(&right_col)
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
    format!("SELECT * FROM ST_Read('{}')", sql_escape(&path))
}

/// Fixed-width / positional source. The form gives a `columns` array
/// of `{name, start (1-based), width}` entries; the engine builds a
/// SELECT that walks each line and pulls the substring at the right
/// offset. The whole-file-as-one-column trick uses read_csv with a
/// delimiter that can't appear in plain text (chr(7) - the BEL) so
/// every line becomes a single string the SUBSTR projections can chew.
/// Trims trailing whitespace by default (the standard for fixed-width
/// dumps where every field is padded to its column width).
pub(crate) fn build_fixedwidth_source(props: &JsonValue) -> Result<String, String> {
    let path = string_prop(props, "path")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Fixed-width source: path required".to_string())?;
    let cols = props
        .get("columns")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            "Fixed-width source: columns array required ({name, start, width} each)".to_string()
        })?;
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

pub(crate) fn build_sink_sql(
    component_id: &str,
    props: &JsonValue,
    from_view: &str,
    cols: &[String],
) -> Result<String, EngineError> {
    match component_id {
        "snk.csv" => Ok(build_csv_sink(props, from_view)),
        "snk.tsv" => {
            let mut p = props.clone();
            if let Some(obj) = p.as_object_mut() {
                obj.insert("delimiter".into(), JsonValue::String("\t".into()));
            }
            Ok(build_csv_sink(&p, from_view))
        }
        "snk.parquet" => Ok(build_parquet_sink(props, from_view)),
        "snk.json" | "snk.jsonl" => Ok(build_json_sink(props, from_view)),
        "snk.s3" | "snk.gcs" | "snk.azureblob" => build_cloud_sink(props, from_view),
        "snk.sqlite" | "snk.duckdb" => build_db_sink(component_id, props, from_view, cols),
        "snk.postgres" | "snk.cockroach" | "snk.mysql" | "snk.mariadb"
        | "snk.motherduck" | "snk.ducklake" | "snk.pgvector"
        | "snk.redshift" | "snk.bigquery" | "snk.quack" => build_relational_sink(component_id, props, from_view, cols),
        "snk.excel" => Ok(build_excel_sink(props, from_view)),
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
pub(crate) fn build_cloud_sink(props: &JsonValue, from_view: &str) -> Result<String, EngineError> {
    let path = string_prop(props, "path")
        .or_else(|| string_prop(props, "url"))
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
    let compression = string_prop(props, "compression").unwrap_or_else(|| "ZSTD".into());
    let partition = columns_from_props(props, "partitionBy").unwrap_or_default();
    let mut options = vec![
        "FORMAT PARQUET".to_string(),
        format!("COMPRESSION '{}'", sql_escape(&compression)),
    ];
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
    format!(
        "COPY ({}) TO '{}' ({})",
        partition_guarded_source(props, from_view, &partition),
        sql_escape(&path),
        options.join(", ")
    )
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
        _ => {}
    }
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
