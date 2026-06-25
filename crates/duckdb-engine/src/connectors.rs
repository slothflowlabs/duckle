//! Connector + transform runtime runners (impl DuckdbEngine).
//!
//! Every run_* method that executes a non-SQL source/sink/transform spec, the
//! ctl.* sub-pipeline helpers, and a couple of driver cell-to-JSON converters.
//! Extracted from lib.rs; the core engine (run/run_rows/execute_pipeline/
//! materialize helpers) stays there. self.run / self.bin etc. are reachable
//! because this is a child module of the crate root.

use crate::*;

impl DuckdbEngine {
    /// Relational-DB upsert. DuckDB's ATTACH doesn't propagate the
    /// target's UNIQUE / PRIMARY KEY constraints, so a native DuckDB
    /// INSERT ... ON CONFLICT fails to bind. Instead we stage the
    /// upstream into the target DB via ATTACH and then run the real
    /// ON CONFLICT (Postgres) / ON DUPLICATE KEY UPDATE (MySQL) INSERT
    /// directly on the underlying connection through the extension's
    /// passthrough function (postgres_execute / mysql_execute).
    pub(crate) fn run_upsert(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &plan::UpsertSpec,
    ) -> Result<String, EngineError> {
        let desc_sql = format!("DESCRIBE {};", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &desc_sql)?;
        let all_cols: Vec<String> = rows
            .iter()
            .filter_map(|r| {
                r.get("column_name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        if all_cols.is_empty() {
            return Err(EngineError::Query(format!(
                "Upsert: couldn't read columns from '{}'",
                spec.from_view
            )));
        }
        let key_set: std::collections::HashSet<&str> =
            spec.conflict_cols.iter().map(|s| s.as_str()).collect();
        // Delete-propagation control column (if configured) is a control
        // column: excluded from both the SET clause and the explicit INSERT
        // column list, but it stays in the staging table so the DELETE filter
        // and the insert WHERE-guard can read it.
        let delete_col = spec.delete_column.as_deref();
        let data_cols: Vec<&String> = all_cols
            .iter()
            .filter(|c| Some(c.as_str()) != delete_col)
            .collect();
        let set_cols: Vec<&String> = data_cols
            .iter()
            .filter(|c| !key_set.contains(c.as_str()))
            .copied()
            .collect();

        // Sanitized staging table name (suffix from upstream node id).
        let suffix: String = spec
            .from_view
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let target_native = spec
            .target
            .strip_prefix("duckle_dst.")
            .unwrap_or(&spec.target)
            .to_string();
        let staging_unqualified = format!("duckle_upsert_staging_{}", suffix);

        // Step 1: stage the rows in the target DB (via ATTACH).
        // Default schema differs per family (public for PG/Cockroach;
        // for MySQL the database is selected at ATTACH, no schema layer).
        let staging_native = match spec.family {
            plan::UpsertFamily::Postgres => format!("public.{}", staging_unqualified),
            plan::UpsertFamily::MySql => staging_unqualified.clone(),
        };
        let staging_duckle = format!("duckle_dst.{}", staging_native);
        let stage_sql = format!(
            "{secret}{attach}DROP TABLE IF EXISTS {sd}; \
             CREATE TABLE {sd} AS SELECT * FROM {from} WHERE 1=0; \
             INSERT INTO {sd} SELECT * FROM {from};",
            secret = secret_prefix,
            attach = spec.attach,
            sd = staging_duckle,
            from = plan::quote_ident(&spec.from_view)
        );
        self.run(Some(db), &stage_sql, false)?;

        // Step 2: assemble the real upsert SQL, run it on the native
        // connection so the constraint check sees the real schema.
        let native_stmts =
            build_native_upsert_sql(spec, &set_cols, &data_cols, &target_native, &staging_native);
        let exec_fn = match spec.family {
            plan::UpsertFamily::Postgres => "postgres_execute",
            plan::UpsertFamily::MySql => "mysql_execute",
        };
        // Run each statement as its own passthrough CALL. Postgres returns a
        // single (multi-statement) string here so this is one call; MySQL
        // returns its statements separately because its extension rejects a
        // multi-statement batch ("Commands out of sync").
        let mut last = String::new();
        for stmt in &native_stmts {
            let exec_sql = format!(
                "{secret}{attach}CALL {fn_name}('duckle_dst', '{sql}');",
                secret = secret_prefix,
                attach = spec.attach,
                fn_name = exec_fn,
                sql = stmt.replace('\'', "''")
            );
            last = self.run(Some(db), &exec_sql, false)?;
        }
        Ok(last)
    }

    /// HTTP sink (snk.webhook / snk.rest). Materializes the upstream
    /// view via DuckDB's -json output, then either
    ///   - row mode: one ureq request per row, body = row JSON
    ///   - batch mode: a single request with body = entire array JSON
    ///
    /// Returns a synthetic 'sent N rows' report on success; aggregates
    /// per-row HTTP errors into a single Err for the run feedback layer.
    pub(crate) fn run_webhook(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &WebhookSpec,
    ) -> Result<String, EngineError> {
        let select = format!(
            "{}SELECT * FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let rows = self.run_rows(Some(db), &select)?;
        let method = if spec.method.is_empty() {
            "POST".to_string()
        } else {
            spec.method.to_uppercase()
        };
        // Reuse one Agent across all dispatches; in row mode this loops once
        // per row against the same host, so connection pooling avoids a fresh
        // handshake per row.
        let agent = crate::tls::http_agent();
        let dispatch = |body: String, default_ct: &str| -> Result<(), EngineError> {
            let mut req = agent.request(&method, &spec.url);
            let has_ct = spec
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("content-type"));
            for (k, v) in &spec.headers {
                req = req.set(k, v);
            }
            if !has_ct {
                req = req.set("content-type", default_ct);
            }
            match req.send_string(&body) {
                Ok(_) => Ok(()),
                Err(ureq::Error::Status(code, response)) => {
                    let body = response.into_string().unwrap_or_default();
                    Err(EngineError::Query(format!(
                        "HTTP {} from {}: {}",
                        code,
                        spec.url,
                        body.chars().take(200).collect::<String>()
                    )))
                }
                Err(e) => Err(EngineError::Query(format!(
                    "HTTP transport error to {}: {}",
                    spec.url, e
                ))),
            }
        };
        match spec.body_shape.as_str() {
            "batch" => {
                // Wrap the rows array in {body_wrap: [...]} when set,
                // and merge any body_extras (e.g. Milvus's collectionName).
                let body = if spec.body_wrap.is_some() || !spec.body_extras.is_empty() {
                    let mut obj = serde_json::Map::new();
                    if let Some(wrap_key) = &spec.body_wrap {
                        obj.insert(
                            wrap_key.clone(),
                            serde_json::Value::Array(rows.clone()),
                        );
                    }
                    for (k, v) in &spec.body_extras {
                        obj.insert(k.clone(), v.clone());
                    }
                    serde_json::to_string(&serde_json::Value::Object(obj))
                        .unwrap_or_else(|_| "{}".into())
                } else {
                    serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into())
                };
                dispatch(body, "application/json")?;
                Ok(format!("sent 1 batch ({} rows) to {}", rows.len(), spec.url))
            }
            "ndjson_bulk" => {
                // Each row produces TWO lines: an action then the doc.
                // The action template lives in spec.bulk_action (set by
                // snk.elastic / snk.opensearch with the index name baked in).
                let action = spec
                    .bulk_action
                    .as_deref()
                    .unwrap_or("{\"index\":{}}");
                let mut body = String::new();
                for row in &rows {
                    body.push_str(action);
                    body.push('\n');
                    let doc = serde_json::to_string(row).unwrap_or_else(|_| "{}".into());
                    body.push_str(&doc);
                    body.push('\n');
                }
                dispatch(body, "application/x-ndjson")?;
                Ok(format!("bulk-indexed {} docs to {}", rows.len(), spec.url))
            }
            _ => {
                let mut sent = 0_usize;
                for row in &rows {
                    let body = serde_json::to_string(row).unwrap_or_else(|_| "{}".into());
                    dispatch(body, "application/json")?;
                    sent += 1;
                }
                Ok(format!("sent {} rows to {}", sent, spec.url))
            }
        }
    }

    /// Snowflake SQL API sink. Reads the upstream view as JSON,
    /// chunks rows into spec.batch_size groups, builds one multi-row
    /// INSERT per chunk, and POSTs to /api/v2/statements with Bearer
    /// PAT auth. Failures surface as a single Err for the run feedback.
    pub(crate) fn run_snowflake_sink(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &SnowflakeSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!(
            "{}SELECT * FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("snowflake: 0 rows to insert into {}", spec.table));
        }
        // Take column order from the first row (DuckDB CLI -json output
        // preserves the SELECT order, which is the upstream view's order).
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => return Err(EngineError::Query("snowflake: upstream rows aren't JSON objects".into())),
        };
        let schema_name = spec.schema.as_deref().unwrap_or("PUBLIC");
        let qualified = format!(
            "{}.{}.{}",
            sf_quote_ident(&spec.database),
            sf_quote_ident(schema_name),
            sf_quote_ident(&spec.table)
        );
        // Upsert (MERGE) clauses when key columns are configured. Each batch is
        // one MERGE whose source is an inline VALUES table - stateless, so it
        // works against the per-request Snowflake SQL API (no temp table).
        let is_upsert = !spec.upsert_keys.is_empty();
        // Delete-propagation control column (upsert only): excluded from the
        // target's data columns, kept in the source projection for the
        // predicate (see the SQL Server sink for the rationale).
        let delete_col: Option<&str> = if is_upsert {
            spec.delete_column.as_deref()
        } else {
            None
        };
        let data_cols: Vec<&String> = cols
            .iter()
            .filter(|c| Some(c.as_str()) != delete_col)
            .collect();
        let cols_list = data_cols
            .iter()
            .map(|c| sf_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let on_clause = spec
            .upsert_keys
            .iter()
            .map(|k| format!("t.{q} = s.{q}", q = sf_quote_ident(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sf_key_set: std::collections::HashSet<&str> =
            spec.upsert_keys.iter().map(|s| s.as_str()).collect();
        // Target columns in MERGE ... UPDATE SET are unqualified (Snowflake
        // and the emulator reject a `t.` prefix on the SET target); the source
        // side keeps its `s.` alias.
        let update_set = data_cols
            .iter()
            .filter(|c| !sf_key_set.contains(c.as_str()))
            .map(|c| format!("{q} = s.{q}", q = sf_quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_vals = data_cols
            .iter()
            .map(|c| format!("s.{}", sf_quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let (delete_clause, not_matched_guard) = match delete_col {
            Some(dc) => {
                let q = sf_quote_ident(dc);
                let v = jsonnative_quote_inner(&spec.delete_value);
                (
                    format!(" WHEN MATCHED AND s.{q} = '{v}' THEN DELETE", q = q, v = v),
                    format!(" AND (s.{q} IS NULL OR s.{q} <> '{v}')", q = q, v = v),
                )
            }
            None => (String::new(), String::new()),
        };
        let url = spec.endpoint.clone().unwrap_or_else(|| {
            format!(
                "https://{}.snowflakecomputing.com/api/v2/statements",
                spec.account
            )
        });
        // Compute the Authorization header once per stage. JWT lifetime
        // is 1 hour; PAT is the token verbatim. Either way it gets
        // reused across every chunk's POST.
        let auth_header = build_snowflake_auth_header(&spec.account, &spec.auth)?;
        let is_jwt = matches!(spec.auth, SnowflakeAuth::Jwt { .. });
        // POST one statement, failing on HTTP errors AND body-level SQL errors
        // (the SQL API / emulator can return HTTP 200 with an error payload, so
        // checking only the status code would silently drop data).
        let post_stmt = |stmt: String| -> Result<(), EngineError> {
            let mut body_obj = serde_json::Map::new();
            body_obj.insert("statement".into(), JsonValue::String(stmt));
            body_obj.insert("timeout".into(), JsonValue::Number(60.into()));
            body_obj.insert("database".into(), JsonValue::String(spec.database.clone()));
            body_obj.insert("schema".into(), JsonValue::String(schema_name.into()));
            if let Some(wh) = &spec.warehouse {
                body_obj.insert("warehouse".into(), JsonValue::String(wh.clone()));
            }
            if let Some(role) = &spec.role {
                body_obj.insert("role".into(), JsonValue::String(role.clone()));
            }
            let body = serde_json::to_string(&JsonValue::Object(body_obj))
                .unwrap_or_else(|_| "{}".into());
            let mut req = crate::tls::http_agent().post(&url)
                .set("Authorization", &auth_header)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            if is_jwt {
                req = req.set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT");
            }
            match req.send_string(&body) {
                Ok(resp) => {
                    let txt = resp.into_string().unwrap_or_default();
                    if let Some(err) = snowflake_body_error(&txt) {
                        return Err(EngineError::Query(format!(
                            "Snowflake statement failed: {}",
                            err
                        )));
                    }
                    // A statement that exceeds the inline timeout escalates to
                    // async: the body carries a statementHandle and no `data`.
                    // Poll it to completion so a still-running (or later failed)
                    // write isn't counted as a successful insert.
                    let parsed: JsonValue =
                        serde_json::from_str(&txt).unwrap_or(JsonValue::Null);
                    if parsed.get("data").is_none() {
                        if let Some(handle) =
                            parsed.get("statementHandle").and_then(|v| v.as_str())
                        {
                            poll_snowflake_until_done(&url, &auth_header, is_jwt, handle)?;
                        }
                    }
                    Ok(())
                }
                Err(ureq::Error::Status(code, response)) => {
                    let b = response.into_string().unwrap_or_default();
                    Err(EngineError::Query(format!(
                        "Snowflake HTTP {} from {}: {}",
                        code,
                        url,
                        b.chars().take(300).collect::<String>()
                    )))
                }
                Err(e) => Err(EngineError::Query(format!(
                    "Snowflake HTTP transport to {}: {}",
                    url, e
                ))),
            }
        };

        // Auto-create the target if absent (consistent with the SQL Server /
        // Oracle sinks), inferring types from the upstream view. A no-op when
        // the table already exists.
        let col_types: std::collections::HashMap<String, String> =
            describe_columns(self, db, &spec.from_view).into_iter().collect();
        let col_defs = data_cols
            .iter()
            .map(|c| {
                let ty = duckdb_type_to_snowflake(
                    col_types.get(c.as_str()).map(|s| s.as_str()).unwrap_or("VARCHAR"),
                );
                format!("{} {}", sf_quote_ident(c), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        post_stmt(format!("CREATE TABLE IF NOT EXISTS {} ({})", qualified, col_defs))?;

        let mut total_inserted = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            let values: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let row_obj = row.as_object();
                    let vals: Vec<String> = cols
                        .iter()
                        .map(|c| {
                            let v = row_obj
                                .and_then(|o| o.get(c))
                                .unwrap_or(&JsonValue::Null);
                            sql_literal(v, None, Dialect::JsonNative)
                        })
                        .collect();
                    format!("({})", vals.join(", "))
                })
                .collect();
            let stmt = if is_upsert {
                let matched = if update_set.is_empty() {
                    String::new()
                } else {
                    format!(" WHEN MATCHED THEN UPDATE SET {}", update_set)
                };
                // Source as `SELECT lit AS "col", ... UNION ALL ...`: portable
                // across Snowflake and the DuckDB-backed emulator (whose MERGE
                // parser doesn't accept a VALUES table source).
                let src_selects: Vec<String> = chunk
                    .iter()
                    .map(|row| {
                        let obj = row.as_object();
                        let items: Vec<String> = cols
                            .iter()
                            .map(|c| {
                                let v = obj.and_then(|o| o.get(c)).unwrap_or(&JsonValue::Null);
                                format!(
                                    "{} AS {}",
                                    sql_literal(v, None, Dialect::JsonNative),
                                    sf_quote_ident(c)
                                )
                            })
                            .collect();
                        format!("SELECT {}", items.join(", "))
                    })
                    .collect();
                format!(
                    "MERGE INTO {tgt} t USING ({src}) s ON {on}{del}{matched} WHEN NOT MATCHED{guard} THEN INSERT ({cols}) VALUES ({ins})",
                    tgt = qualified,
                    src = src_selects.join(" UNION ALL "),
                    cols = cols_list,
                    on = on_clause,
                    del = delete_clause,
                    matched = matched,
                    guard = not_matched_guard,
                    ins = insert_vals,
                )
            } else {
                format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    qualified,
                    cols_list,
                    values.join(", ")
                )
            };
            post_stmt(stmt)?;
            total_inserted += chunk.len();
        }
        Ok(format!(
            "snowflake: {} {} rows into {}",
            if is_upsert { "merged" } else { "inserted" },
            total_inserted, spec.table
        ))
    }

    /// Oracle sink behind the `oracle` Cargo feature. Without the
    /// feature this returns a clear error so the user knows what to
    /// rebuild with. With the feature, builds multi-row INSERT ALL ...
    /// SELECT * FROM dual statements (Oracle's idiom for multi-row
    /// insert) in batches.
    #[cfg(feature = "oracle")]
    pub(crate) fn run_oracle_sink(
        &self,
        db: &Path,
        spec: &OracleSinkSpec,
    ) -> Result<String, EngineError> {
        // Column names + DuckDB types in view order, used to auto-create the
        // target, decide the fast bind path, and (fallback) render literals.
        let describe = describe_columns(self, db, &spec.from_view);
        if describe.is_empty() {
            return Ok(format!("oracle: 0 columns to insert into {}", spec.table));
        }
        let cols: Vec<String> = describe.iter().map(|(n, _)| n.clone()).collect();
        let col_types: std::collections::HashMap<String, String> =
            describe.iter().cloned().collect();
        // Oracle limits a table to 1000 columns; reject up front with a clear
        // message rather than failing deep in CREATE TABLE / INSERT.
        if cols.len() >= 1000 {
            return Err(EngineError::Query(format!(
                "oracle: {} columns exceeds Oracle's 1000-column table limit",
                cols.len()
            )));
        }
        let oq = |id: &str| format!("\"{}\"", id.replace('"', "\"\""));
        let qualified = match &spec.schema {
            Some(s) => format!("{}.{}", oq(s), oq(&spec.table)),
            None => oq(&spec.table),
        };
        let cols_list = cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");

        // Decide whether every column can take the fast array-bind path. Bind
        // values are sent as strings and converted by Oracle: numbers / text
        // implicitly, DATE / TIMESTAMP via an explicit TO_DATE / TO_TIMESTAMP
        // fed a canonical strftime string. Time-zone, BLOB and nested types
        // are not handled this way, so any of them drops the whole sink to the
        // per-literal INSERT ALL fallback below (no behavior change for them).
        let mut bindable = true;
        let mut placeholders: Vec<String> = Vec::with_capacity(cols.len());
        let mut select_items: Vec<String> = Vec::with_capacity(cols.len());
        for (idx, (name, duck)) in describe.iter().enumerate() {
            let up = duck.trim().to_ascii_uppercase();
            let n = idx + 1;
            let qn = plan::quote_ident(name);
            if up.contains("TIME ZONE")
                || up.starts_with("BLOB")
                || up.starts_with("BYTEA")
                || up.starts_with("BINARY")
                || up.starts_with("VARBINARY")
                || up.ends_with("[]")
                || up.starts_with("STRUCT")
                || up.starts_with("MAP")
                || up.starts_with("LIST")
                || up.starts_with("UNION")
            {
                bindable = false;
                break;
            } else if up == "DATE" {
                placeholders.push(format!("TO_DATE(:{}, 'YYYY-MM-DD')", n));
                select_items.push(format!("strftime({}, '%Y-%m-%d') AS {}", qn, qn));
            } else if up.starts_with("TIMESTAMP") || up == "DATETIME" {
                placeholders.push(format!("TO_TIMESTAMP(:{}, 'YYYY-MM-DD HH24:MI:SS.FF6')", n));
                select_items.push(format!("strftime({}, '%Y-%m-%d %H:%M:%S.%f') AS {}", qn, qn));
            } else {
                placeholders.push(format!(":{}", n));
                select_items.push(qn);
            }
        }

        let conn = oracle::Connection::connect(&spec.user, &spec.password, &spec.connect)
            .map_err(|e| EngineError::Query(format!("oracle connect: {}", e)))?;
        // Pin the decimal separator so string-bound numbers parse with '.'
        // regardless of the server locale (NLS_NUMERIC_CHARACTERS).
        let _ = conn.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'", &[]);

        // Auto-create the target table if absent, inferring column types from
        // the upstream DuckDB view (issue #8). Oracle has no CREATE TABLE IF
        // NOT EXISTS, so swallow ORA-00955 (name already used) in PL/SQL.
        {
            let col_defs = cols
                .iter()
                .map(|c| {
                    let ty = duckdb_type_to_oracle(
                        col_types.get(c).map(|s| s.as_str()).unwrap_or("VARCHAR"),
                    );
                    format!("\"{}\" {}", c.replace('"', "\"\""), ty)
                })
                .collect::<Vec<_>>()
                .join(", ");
            let create_inner =
                format!("CREATE TABLE {} ({})", qualified, col_defs).replace('\'', "''");
            let create_plsql = format!(
                "BEGIN EXECUTE IMMEDIATE '{}'; EXCEPTION WHEN OTHERS THEN \
                 IF SQLCODE != -955 THEN RAISE; END IF; END;",
                create_inner
            );
            conn.execute(&create_plsql, &[])
                .map_err(|e| EngineError::Query(format!("oracle create table: {}", e)))?;
        }

        // Commit periodically, not after every statement: a commit forces a
        // redo-log flush, so per-batch commits dominated large-load wall-clock.
        const COMMIT_EVERY: usize = 200_000;

        // Upsert (MERGE) path: each batch is one MERGE whose source is an
        // inline `SELECT ... FROM dual UNION ALL ...` (Oracle has no multi-row
        // VALUES). Reuses the literal renderer; correct insert-or-update by the
        // configured key columns. Runs before the plain-insert fast/fallback
        // paths and returns when done.
        if !spec.upsert_keys.is_empty() {
            let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
            let rows = self.run_rows(Some(db), &select)?;
            if rows.is_empty() {
                return Ok(format!("oracle: 0 rows to merge into {}", qualified));
            }
            let key_set: std::collections::HashSet<&str> =
                spec.upsert_keys.iter().map(|s| s.as_str()).collect();
            let oq = |c: &str| format!("\"{}\"", c.replace('"', "\"\""));
            // Delete-propagation control column (excluded from target data
            // columns, kept in the source projection for the predicate).
            let delete_col: Option<&str> = spec.delete_column.as_deref();
            let data_cols: Vec<&String> = cols
                .iter()
                .filter(|c| Some(c.as_str()) != delete_col)
                .collect();
            let cols_list_data = data_cols
                .iter()
                .map(|c| oq(c))
                .collect::<Vec<_>>()
                .join(", ");
            let on_clause = spec
                .upsert_keys
                .iter()
                .map(|k| format!("t.{0} = s.{0}", oq(k)))
                .collect::<Vec<_>>()
                .join(" AND ");
            let update_set = data_cols
                .iter()
                .filter(|c| !key_set.contains(c.as_str()))
                .map(|c| format!("t.{0} = s.{0}", oq(c)))
                .collect::<Vec<_>>()
                .join(", ");
            let insert_vals = data_cols
                .iter()
                .map(|c| format!("s.{}", oq(c)))
                .collect::<Vec<_>>()
                .join(", ");
            // Oracle's MERGE deletes via `UPDATE SET ... DELETE WHERE (cond)`
            // (it has no standalone `WHEN MATCHED ... THEN DELETE`): the row is
            // updated first, then removed if the source flag marks a delete.
            // The INSERT clause carries an optional WHERE so a flagged row with
            // no target match is skipped. delete_part needs the UPDATE clause,
            // so it only applies when there are non-key columns to set.
            let (delete_part, insert_where) = match delete_col {
                Some(dc) => {
                    let q = oq(dc);
                    let v = spec.delete_value.replace('\'', "''");
                    let dp = if update_set.is_empty() {
                        String::new()
                    } else {
                        format!(" DELETE WHERE (s.{q} = '{v}')", q = q, v = v)
                    };
                    (
                        dp,
                        format!(" WHERE (s.{q} IS NULL OR s.{q} <> '{v}')", q = q, v = v),
                    )
                }
                None => (String::new(), String::new()),
            };
            let matched = if update_set.is_empty() {
                String::new()
            } else {
                format!(" WHEN MATCHED THEN UPDATE SET {}", update_set)
            };
            // Oracle caps a SELECT at 1000 expressions and statements at 64K;
            // keep each MERGE source small so wide tables stay within limits.
            let rows_per_stmt = (50_000 / cols.len().max(1)).clamp(1, 200);
            let mut total = 0_usize;
            let mut uncommitted = 0_usize;
            for chunk in rows.chunks(rows_per_stmt) {
                self.check_cancelled()?;
                let selects: Vec<String> = chunk
                    .iter()
                    .map(|row| {
                        let obj = row.as_object();
                        let items: Vec<String> = cols
                            .iter()
                            .map(|c| {
                                let v =
                                    obj.and_then(|o| o.get(c)).unwrap_or(&JsonValue::Null);
                                let lit = sql_literal(
                                    v,
                                    col_types.get(c).map(|s| s.as_str()),
                                    Dialect::Oracle,
                                );
                                format!("{} AS {}", lit, oq(c))
                            })
                            .collect();
                        format!("SELECT {} FROM dual", items.join(", "))
                    })
                    .collect();
                let merge = format!(
                    "MERGE INTO {tgt} t USING ({src}) s ON ({on}){matched}{del} WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({ins}){ins_where}",
                    tgt = qualified,
                    src = selects.join(" UNION ALL "),
                    on = on_clause,
                    matched = matched,
                    del = delete_part,
                    cols = cols_list_data,
                    ins = insert_vals,
                    ins_where = insert_where,
                );
                conn.execute(&merge, &[])
                    .map_err(|e| EngineError::Query(format!("oracle merge: {}", e)))?;
                total += chunk.len();
                uncommitted += chunk.len();
                if uncommitted >= COMMIT_EVERY {
                    conn.commit()
                        .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
                    uncommitted = 0;
                }
            }
            conn.commit()
                .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
            return Ok(format!("oracle: merged {} rows into {}", total, qualified));
        }

        // Fast path: one prepared INSERT, array-bound and array-executed
        // (dpiStmt_executeMany). Replaces the old per-99-row INSERT ALL, each
        // a unique literal statement Oracle had to hard-parse.
        if bindable {
            let select = format!(
                "SELECT {} FROM {}",
                select_items.join(", "),
                plan::quote_ident(&spec.from_view)
            );
            let rows = self.run_rows(Some(db), &select)?;
            if rows.is_empty() {
                return Ok(format!("oracle: 0 rows to insert into {}", spec.table));
            }
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                qualified,
                cols_list,
                placeholders.join(", ")
            );
            const BIND_BATCH: usize = 5000;
            let mut batch = conn
                .batch(&insert_sql, BIND_BATCH)
                .build()
                .map_err(|e| EngineError::Query(format!("oracle batch prepare: {}", e)))?;
            let mut total = 0_usize;
            let mut uncommitted = 0_usize;
            for row in &rows {
                if total % BIND_BATCH == 0 {
                    self.check_cancelled()?;
                }
                let obj = row.as_object();
                // Bind every value as a string; the SQL placeholders and
                // Oracle implicit conversion turn it back into the column type.
                let binds: Vec<Option<String>> = cols
                    .iter()
                    .map(|c| match obj.and_then(|o| o.get(c)) {
                        None | Some(JsonValue::Null) => None,
                        Some(JsonValue::String(s)) => Some(s.clone()),
                        Some(JsonValue::Bool(b)) => {
                            Some(if *b { "1".to_string() } else { "0".to_string() })
                        }
                        Some(JsonValue::Number(num)) => Some(num.to_string()),
                        Some(other) => Some(other.to_string()),
                    })
                    .collect();
                let refs: Vec<&dyn oracle::sql_type::ToSql> =
                    binds.iter().map(|b| b as &dyn oracle::sql_type::ToSql).collect();
                batch
                    .append_row(&refs)
                    .map_err(|e| EngineError::Query(format!("oracle insert: {}", e)))?;
                total += 1;
                uncommitted += 1;
                if uncommitted >= COMMIT_EVERY {
                    batch
                        .execute()
                        .map_err(|e| EngineError::Query(format!("oracle insert: {}", e)))?;
                    conn.commit()
                        .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
                    uncommitted = 0;
                }
            }
            batch
                .execute()
                .map_err(|e| EngineError::Query(format!("oracle insert: {}", e)))?;
            conn.commit()
                .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
            return Ok(format!("oracle: inserted {} rows into {}", total, qualified));
        }

        // Fallback path (time-zone / BLOB / nested types): per-literal INSERT
        // ALL, capped under Oracle's 999 cumulative-value limit (issue #11).
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("oracle: 0 rows to insert into {}", spec.table));
        }
        let mut total = 0_usize;
        let mut uncommitted = 0_usize;
        let rows_per_stmt = oracle_insert_all_rows_per_stmt(cols.len(), spec.batch_size);
        for chunk in rows.chunks(rows_per_stmt) {
            self.check_cancelled()?;
            let mut sql = String::from("INSERT ALL");
            for row in chunk {
                let row_obj = row.as_object();
                let vals: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        let v = row_obj.and_then(|o| o.get(c)).unwrap_or(&JsonValue::Null);
                        sql_literal(v, col_types.get(c).map(|s| s.as_str()), Dialect::Oracle)
                    })
                    .collect();
                sql.push_str(&format!(
                    " INTO {} ({}) VALUES ({})",
                    qualified,
                    cols_list,
                    vals.join(", ")
                ));
            }
            sql.push_str(" SELECT 1 FROM dual");
            conn.execute(&sql, &[])
                .map_err(|e| EngineError::Query(format!("oracle insert: {}", e)))?;
            total += chunk.len();
            uncommitted += chunk.len();
            if uncommitted >= COMMIT_EVERY {
                conn.commit()
                    .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
                uncommitted = 0;
            }
        }
        if uncommitted > 0 {
            conn.commit()
                .map_err(|e| EngineError::Query(format!("oracle commit: {}", e)))?;
        }
        Ok(format!("oracle: inserted {} rows into {}", total, qualified))
    }

    #[cfg(not(feature = "oracle"))]
    pub(crate) fn run_oracle_sink(
        &self,
        _db: &Path,
        _spec: &OracleSinkSpec,
    ) -> Result<String, EngineError> {
        Err(EngineError::Config(
            "snk.oracle: this Duckle binary was built without the default \
             `oracle` feature. Default builds include Oracle support; if \
             you're seeing this, rebuild with `cargo build --release` (no \
             --no-default-features). At runtime users still need Oracle \
             Instant Client (libclntsh.so / OCI.dll / libclntsh.dylib) on \
             the library path."
                .into(),
        ))
    }

    /// Oracle source behind the `oracle` Cargo feature. Same gating
    /// model as the sink.
    #[cfg(feature = "oracle")]
    pub(crate) fn run_oracle_source(
        &self,
        db: &Path,
        spec: &OracleSourceSpec,
    ) -> Result<String, EngineError> {
        // Liveness trace (issue #4): each phase plus periodic row progress
        // is timestamped to a temp file so a stuck pull can be located from
        // the log even when the desktop shows no console. Truncated per run.
        let trace_path = std::env::temp_dir().join("duckle-oracle-trace.log");
        let _ = std::fs::remove_file(&trace_path);
        let t0 = std::time::Instant::now();
        let mark = |msg: &str| {
            use std::io::Write;
            let line = format!(
                "[+{:>7}ms] [{}] {}",
                t0.elapsed().as_millis(),
                spec.node_id,
                msg
            );
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&trace_path)
            {
                let _ = writeln!(f, "{}", line);
            }
            eprintln!("[duckle:oracle] {}", line);
        };
        mark(&format!("connecting to {} as {}", spec.connect, spec.user));

        let conn = oracle::Connection::connect(&spec.user, &spec.password, &spec.connect)
            .map_err(|e| EngineError::Query(format!("oracle connect: {}", e)))?;
        mark("connected; normalizing NLS session formats");

        // Issue #4 robustness (not a confirmed fix): pin the session NLS
        // formats to a stable ISO-ish shape so serialized DATE/TIMESTAMP
        // strings do not vary with the server locale. A format that forces
        // read_json_auto to re-sniff every row is the leading remaining
        // hypothesis for the wide-table slowdown. Best-effort: a server
        // that rejects any of these still proceeds with its defaults.
        for nls in [
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'",
            "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'",
        ] {
            if let Err(e) = conn.execute(nls, &[]) {
                mark(&format!("NLS set skipped: {}", e));
            }
        }
        mark("preparing query");

        // Issue #4: the default Oracle prefetch is tiny (often 1-2 rows
        // per round trip). Two knobs matter for a bulk pull and BOTH must be
        // raised: prefetch_rows is OCI's server prefetch, and fetch_array_size
        // (ODPI default 100) is how many rows the client buffers per fetch.
        // Left at 100, a 2M-row pull is ~20 000 client fetches and the OCI
        // fetch dominated wall-clock (profiled at ~12s). Matching both at
        // 5 000 cuts that to ~400 fetches.
        let mut stmt = conn
            .statement(&spec.query)
            .prefetch_rows(5000)
            .fetch_array_size(5000)
            .build()
            .map_err(|e| EngineError::Query(format!("oracle prepare: {}", e)))?;
        let rs = stmt
            .query(&[])
            .map_err(|e| EngineError::Query(format!("oracle query: {}", e)))?;
        let cols: Vec<String> = rs
            .column_info()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        mark(&format!("query open; {} columns; streaming rows", cols.len()));

        // Stream rows straight to the NDJSON temp file. The previous
        // Vec<JsonValue> collector held the entire result set in RAM
        // before handing it to DuckDB - on a million-row x 37-col pull
        // that peaked at ~30 GB resident. Now the writer keeps a 64 KiB
        // buffer regardless of row count.
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let mut count = 0_usize;
        for row_res in rs {
            let row = row_res.map_err(|e| EngineError::Query(format!("oracle row: {}", e)))?;
            let mut obj = serde_json::Map::new();
            for (i, name) in cols.iter().enumerate() {
                obj.insert(name.clone(), Self::oracle_cell_to_json(&row, i));
            }
            writer.write_row(&JsonValue::Object(obj))?;
            count += 1;
            if count % 25_000 == 0 {
                mark(&format!("fetched {} rows", count));
            }
        }
        mark(&format!(
            "fetch complete: {} rows; materializing into DuckDB",
            count
        ));
        writer.finalize_into_table(&self.bin, db, &spec.node_id)?;
        mark(&format!(
            "materialize complete: {} into {}",
            count, spec.node_id
        ));
        Ok(format!(
            "oracle: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// Convert one cell of an Oracle row to JSON without silently
    /// losing data. The old approach was a try-String-then-i64-then-
    /// f64 cascade, which fell through to NULL for DATE / TIMESTAMP /
    /// BLOB / RAW / NUMBER-that-overflows-i64 columns - whole
    /// columns vanished in downstream Parquet (issue #4).
    ///
    /// Strategy: dispatch by Oracle column type. NUMBER with a
    /// non-zero scale is parsed as f64 if it fits, otherwise kept as
    /// a string to avoid the precision trap with high-precision
    /// decimals. DATE / TIMESTAMP becomes an ISO-shaped string.
    /// BLOB / RAW gets base64-encoded. Unknown types fall through to
    /// the String accessor so the cell is at worst visible as text
    /// rather than NULL.
    #[cfg(feature = "oracle")]
    pub(crate) fn oracle_cell_to_json(row: &oracle::Row, i: usize) -> JsonValue {
        use oracle::sql_type::OracleType;
        let infos = row.column_info();
        let oty = infos
            .get(i)
            .map(|c| c.oracle_type().clone())
            .unwrap_or(OracleType::Varchar2(0));

        match oty {
            OracleType::Number(_, scale) if scale == 0 => {
                if let Ok(Some(n)) = row.get::<usize, Option<i64>>(i) {
                    return JsonValue::from(n);
                }
                if let Ok(Some(s)) = row.get::<usize, Option<String>>(i) {
                    return JsonValue::String(s);
                }
                JsonValue::Null
            }
            // Decimal NUMBER / ANSI FLOAT carry up to 38 significant
            // digits, but f64 only round-trips ~15. Reading a
            // high-precision value through f64 silently drops the extra
            // digits (e.g. NUMBER(38,12) 123456.123456789012 -> ...789),
            // so keep the exact text when it would not survive f64.
            OracleType::Number(_, _) | OracleType::Float(_) => {
                // Significant digits = digits with the sign, decimal point
                // and leading/trailing zeros removed.
                fn significant_digits(s: &str) -> usize {
                    let d: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
                    d.trim_start_matches('0').trim_end_matches('0').len()
                }
                if let Ok(Some(s)) = row.get::<usize, Option<String>>(i) {
                    if significant_digits(&s) <= 15 {
                        if let Ok(n) = s.parse::<f64>() {
                            if let Some(num) = serde_json::Number::from_f64(n) {
                                return JsonValue::Number(num);
                            }
                        }
                    }
                    return JsonValue::String(s);
                }
                JsonValue::Null
            }
            // BINARY_DOUBLE / BINARY_FLOAT are true IEEE floats; f64
            // represents them exactly, so emit a JSON number.
            OracleType::BinaryDouble | OracleType::BinaryFloat => {
                if let Ok(Some(s)) = row.get::<usize, Option<String>>(i) {
                    if let Ok(n) = s.parse::<f64>() {
                        if let Some(num) = serde_json::Number::from_f64(n) {
                            return JsonValue::Number(num);
                        }
                    }
                    return JsonValue::String(s);
                }
                JsonValue::Null
            }
            OracleType::Date
            | OracleType::Timestamp(_)
            | OracleType::TimestampTZ(_)
            | OracleType::TimestampLTZ(_) => row
                .get::<usize, Option<String>>(i)
                .ok()
                .flatten()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
            OracleType::BLOB | OracleType::Raw(_) | OracleType::LongRaw => {
                use base64::engine::general_purpose::STANDARD as B64;
                use base64::Engine as _;
                row.get::<usize, Option<Vec<u8>>>(i)
                    .ok()
                    .flatten()
                    .map(|b| JsonValue::String(B64.encode(&b)))
                    .unwrap_or(JsonValue::Null)
            }
            _ => row
                .get::<usize, Option<String>>(i)
                .ok()
                .flatten()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
        }
    }

    #[cfg(not(feature = "oracle"))]
    pub(crate) fn run_oracle_source(
        &self,
        _db: &Path,
        _spec: &OracleSourceSpec,
    ) -> Result<String, EngineError> {
        Err(EngineError::Config(
            "src.oracle: this Duckle binary was built without the default \
             `oracle` feature. Default builds include Oracle support."
                .into(),
        ))
    }

    /// src.adbc: load a prebuilt ADBC driver at runtime, run the query, and
    /// stream the Arrow result to a Parquet temp file, then materialize it
    /// into the node's DuckDB table via read_parquet (no in-process DuckDB).
    /// Not feature-gated: adbc_core links unconditionally; a missing or
    /// incompatible driver surfaces as a clear engine error at load time.
    pub(crate) fn run_adbc_source(
        &self,
        db: &Path,
        spec: &plan::AdbcSourceSpec,
    ) -> Result<String, EngineError> {
        use adbc_core::{
            driver_manager::ManagedDriver,
            options::{AdbcVersion, OptionDatabase, OptionValue},
            Connection, Database, Driver, Statement,
        };
        use arrow_array::RecordBatchReader;
        use parquet::arrow::ArrowWriter;

        // Prepend the driver's own directory to PATH so a self-contained
        // bundled driver folder (driver lib + its dependent libs, e.g.
        // sqlite3.dll) loads without extra setup.
        let driver_path = Path::new(&spec.driver);
        if let Some(parent) = driver_path.parent() {
            if !parent.as_os_str().is_empty() {
                let cur = std::env::var("PATH").unwrap_or_default();
                let sep = if cfg!(windows) { ';' } else { ':' };
                std::env::set_var("PATH", format!("{}{}{}", parent.display(), sep, cur));
            }
        }

        let entry: Option<&[u8]> = spec.entrypoint.as_deref().map(|s| s.as_bytes());
        let looks_like_path = spec.driver.contains('/')
            || spec.driver.contains('\\')
            || spec.driver.ends_with(".dll")
            || spec.driver.ends_with(".so")
            || spec.driver.ends_with(".dylib");
        let mut driver = if looks_like_path {
            ManagedDriver::load_dynamic_from_filename(&spec.driver, entry, AdbcVersion::V110)
        } else {
            ManagedDriver::load_dynamic_from_name(&spec.driver, entry, AdbcVersion::V110)
        }
        .map_err(|e| EngineError::Query(format!("adbc: load driver '{}': {}", spec.driver, e)))?;

        let opts = spec
            .options
            .iter()
            .map(|(k, v)| (OptionDatabase::from(k.as_str()), OptionValue::String(v.clone())));
        let mut database = driver
            .new_database_with_opts(opts)
            .map_err(|e| EngineError::Query(format!("adbc: open database: {}", e)))?;
        let mut conn = database
            .new_connection()
            .map_err(|e| EngineError::Query(format!("adbc: connect: {}", e)))?;
        let mut stmt = conn
            .new_statement()
            .map_err(|e| EngineError::Query(format!("adbc: statement: {}", e)))?;
        stmt.set_sql_query(&spec.query)
            .map_err(|e| EngineError::Query(format!("adbc: set query: {}", e)))?;
        let reader = stmt
            .execute()
            .map_err(|e| EngineError::Query(format!("adbc: execute: {}", e)))?;

        let schema = reader.schema();
        // Key the temp parquet off the run's unique db path (not just the node
        // id) so concurrent runs of the same pipeline never collide on the
        // file, and so the run's TempDbGuard can sweep it. A single-consumer
        // source exposes this file as a lazy VIEW, so it must outlive this
        // stage; the guard removes all sibling *.adbc-*.parquet at run end.
        let safe_node: String = spec
            .node_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let db_name = db
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parquet_path = db.with_file_name(format!("{}.adbc-{}.parquet", db_name, safe_node));
        let file = std::fs::File::create(&parquet_path)
            .map_err(|e| EngineError::Query(format!("adbc: temp parquet: {}", e)))?;

        // Encode the Arrow batches to the temp parquet on a dedicated thread
        // so the parquet encode overlaps the *next* ADBC driver fetch rather
        // than running strictly after it. The driver pull is the dominant cost
        // (measured ~2x the encode for a 2M-row source), so the encode hides
        // behind it almost entirely. Tuning: statistics are disabled (no
        // downstream stage reads parquet stats here) and the row group is
        // enlarged - one big group reads back faster than the default
        // many-small-groups layout. Compression stays the parquet-crate
        // default (uncompressed): a local temp file optimizes for round-trip
        // speed, not disk size.
        use parquet::file::properties::{EnabledStatistics, WriterProperties};
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_size(1_000_000)
            .build();
        let writer_schema = schema.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel::<arrow_array::RecordBatch>(8);
        let writer = std::thread::spawn(move || -> Result<usize, String> {
            let mut w = ArrowWriter::try_new(file, writer_schema, Some(props))
                .map_err(|e| e.to_string())?;
            let mut n = 0usize;
            for batch in rx {
                n += batch.num_rows();
                w.write(&batch).map_err(|e| e.to_string())?;
            }
            w.close().map_err(|e| e.to_string())?;
            Ok(n)
        });

        // The main thread drives the ADBC reader (its FFI stream is not Send,
        // so it stays here) and ships each batch to the writer thread. A send
        // failure means the writer thread already errored; we stop pulling and
        // surface that error from the join below.
        for batch in reader {
            self.check_cancelled()?;
            let batch = batch.map_err(|e| EngineError::Query(format!("adbc: read batch: {}", e)))?;
            if tx.send(batch).is_err() {
                break;
            }
        }
        drop(tx); // close the channel so the writer loop terminates
        let count = writer
            .join()
            .map_err(|_| EngineError::Query("adbc: parquet writer thread panicked".into()))?
            .map_err(|e| EngineError::Query(format!("adbc: write parquet: {}", e)))?;

        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        // Single consumer: hand DuckDB a lazy read_parquet VIEW (no table copy;
        // the consumer pushes projection / predicate into the parquet scan).
        // The file must survive past this stage, so keep it - the run's
        // TempDbGuard sweeps all sibling *.adbc-*.parquet at run end. 2+
        // consumers: materialize a TABLE so the parquet is decoded once, then
        // drop the temp file right away.
        let kw = if spec.single_consumer { "VIEW" } else { "TABLE" };
        let create = format!(
            "CREATE OR REPLACE {} {} AS SELECT * FROM read_parquet('{}')",
            kw,
            plan::quote_ident(&spec.node_id),
            ppath
        );
        self.run(Some(db), &create, false)?;
        if !spec.single_consumer {
            let _ = std::fs::remove_file(&parquet_path);
        }
        Ok(format!("adbc: materialized {} rows into {}", count, spec.node_id))
    }

    /// Single-consumer network-DB source (postgres / mysql / ...): COPY the
    /// already-typed ATTACH result to a temp parquet, then expose a lazy
    /// read_parquet VIEW. The parquet write is cheaper than an on-disk table
    /// insert and the consumer gets projection / predicate pushdown; typed
    /// parquet is lossless. The ATTACH prelude + COPY + VIEW run in one CLI
    /// call (the duckle_src alias is live for the COPY; the VIEW references the
    /// parquet file, so downstream stages read it with no re-attach). The
    /// parquet is keyed off the run db and swept by the run's TempDbGuard.
    pub(crate) fn run_attach_parquet_source(
        &self,
        db: &Path,
        spec: &plan::AttachParquetSourceSpec,
    ) -> Result<String, EngineError> {
        let safe_node: String = spec
            .node_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let db_name = db
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parquet_path = db.with_file_name(format!("{}.attsrc-{}.parquet", db_name, safe_node));
        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let sql = format!(
            "{}COPY ({}) TO '{}' (FORMAT PARQUET); \
             CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')",
            spec.attach,
            spec.body,
            ppath,
            plan::quote_ident(&spec.node_id),
            ppath
        );
        self.run(Some(db), &sql, false)?;
        Ok(format!("attach-parquet: materialized {}", spec.node_id))
    }

    /// materialize = "duckdb" / "duckdbfile": write this stage into a DuckDB
    /// database file (a real table) and ALSO expose it as a normal table in the
    /// run db so downstream stages read it without re-attaching. With an
    /// `output_path` the file is the user's persistent `.duckdb` (kept for later
    /// analytics); without one it is a run-scoped temp file swept at run end.
    pub(crate) fn run_materialize_duckdb(
        &self,
        db: &Path,
        spec: &plan::MaterializeDuckDbSpec,
    ) -> Result<String, EngineError> {
        let safe_node: String = spec
            .node_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let db_name = db
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let (target, persistent) = match &spec.output_path {
            Some(p) => (p.clone(), true),
            // Temp file shares the run-db name prefix so the temp-db sweep
            // collects it at run end, like the attach-parquet temp files.
            None => (
                db.with_file_name(format!("{}.matddb-{}.duckdb", db_name, safe_node))
                    .to_string_lossy()
                    .into_owned(),
                false,
            ),
        };
        let dbpath = target.replace('\\', "/").replace('\'', "''");
        // Per-stage alias avoids the batched "alias already exists" collision;
        // DETACH at the end so a later stage in the same connection is clean.
        let alias = format!("duckle_mat_{}", safe_node);
        let node = plan::quote_ident(&spec.node_id);
        let sql = format!(
            "{attach}ATTACH '{dbpath}' AS {alias}; \
             CREATE OR REPLACE TABLE {alias}.{node} AS ({body}); \
             CREATE OR REPLACE TABLE {node} AS SELECT * FROM {alias}.{node}; \
             DETACH {alias}",
            attach = spec.attach,
            dbpath = dbpath,
            alias = alias,
            node = node,
            body = spec.body,
        );
        self.run(Some(db), &sql, false)?;
        Ok(format!(
            "materialize-duckdb: {} -> {} ({})",
            spec.node_id,
            target,
            if persistent { "persistent" } else { "temp" }
        ))
    }

    /// Convert one cell of a SQL Server row to JSON without silently
    /// losing data. Same issue as Oracle: the old cascade
    /// try-`&str`-then-`i64`-then-`i32`-then-`f64`-then-`bool` failed
    /// for the common Microsoft SQL Server types (DATETIME / DATE /
    /// DATETIMEOFFSET / DECIMAL / NUMERIC / UNIQUEIDENTIFIER /
    /// VARBINARY), silently emitting NULL and dropping whole columns
    /// from the downstream Parquet / DuckDB table.
    ///
    /// Tiberius exposes a `ColumnData` enum reachable via
    /// `Row::try_get_by_index`; we dispatch on it so every SQL Server
    /// scalar gets a faithful JSON representation.
    pub(crate) fn sqlserver_cell_to_json(
        row: &tiberius::Row,
        col: &tiberius::Column,
        i: usize,
    ) -> JsonValue {
        use tiberius::ColumnType;
        // First, the easy path: the most common scalar types map cleanly
        // through Tiberius' generic try_get<T>. We dispatch by the column
        // type the server reported so we don't blindly probe every type.
        match col.column_type() {
            ColumnType::Bit | ColumnType::Bitn => row
                .try_get::<bool, _>(i)
                .ok()
                .flatten()
                .map(JsonValue::Bool)
                .unwrap_or(JsonValue::Null),
            ColumnType::Int1
            | ColumnType::Int2
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Intn => {
                // Try the widest signed int the server might have packed in.
                if let Ok(Some(n)) = row.try_get::<i64, _>(i) {
                    return JsonValue::from(n);
                }
                if let Ok(Some(n)) = row.try_get::<i32, _>(i) {
                    return JsonValue::from(n);
                }
                if let Ok(Some(n)) = row.try_get::<i16, _>(i) {
                    return JsonValue::from(n);
                }
                if let Ok(Some(n)) = row.try_get::<u8, _>(i) {
                    return JsonValue::from(n);
                }
                JsonValue::Null
            }
            // Float8 / FLOAT and MONEY / SMALLMONEY all decode to f64 in
            // tiberius (money is the scaled integer / 1e4); REAL /
            // FLOAT(24) decodes to f32, which try_get::<f64> rejects - so
            // fall back to f32 before giving up. The previous code read
            // floats as f64 only (REAL -> NULL) and routed MONEY through
            // the Numeric path (which money is NOT -> NULL).
            ColumnType::Float4
            | ColumnType::Float8
            | ColumnType::Floatn
            | ColumnType::Money
            | ColumnType::Money4 => {
                let v = row.try_get::<f64, _>(i).ok().flatten().or_else(|| {
                    row.try_get::<f32, _>(i).ok().flatten().map(|x| x as f64)
                });
                v.and_then(|x| serde_json::Number::from_f64(x).map(JsonValue::Number))
                    .unwrap_or(JsonValue::Null)
            }
            // DECIMAL / NUMERIC arrive as tiberius::numeric::Numeric.
            // Stringify (JSON has no fixed-point; f64 would lose the
            // precision that's the point of DECIMAL) - but format it
            // ourselves from the unscaled value + scale. Numeric's own
            // Display signs both the integer and fractional parts, so a
            // negative like -1.2500 renders as the malformed "-1.-2500".
            ColumnType::Decimaln | ColumnType::Numericn => row
                .try_get::<tiberius::numeric::Numeric, _>(i)
                .ok()
                .flatten()
                .map(|n| JsonValue::String(mssql_numeric_to_string(n.value(), n.scale())))
                .unwrap_or(JsonValue::Null),
            // Date / time / datetime / datetimeoffset all expose a
            // chrono::NaiveDate/NaiveDateTime/DateTime<Utc> via tiberius'
            // optional `time`/`chrono` features. The crate's default
            // path on try_get::<&str>` doesn't work for them, but
            // ToString does - drop to that and emit ISO-shaped strings.
            // DATETIMEOFFSET is offset-aware: tiberius decodes it to
            // chrono::DateTime<FixedOffset> (or Utc), NOT a Naive* type, so
            // the naive probes below would all miss and it became NULL.
            // Emit an RFC3339 string preserving the original offset.
            ColumnType::DatetimeOffsetn => {
                if let Ok(Some(dt)) = row.try_get::<chrono::DateTime<chrono::FixedOffset>, _>(i) {
                    return JsonValue::String(dt.to_rfc3339());
                }
                if let Ok(Some(dt)) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(i) {
                    return JsonValue::String(dt.to_rfc3339());
                }
                return row
                    .try_get::<&str, _>(i)
                    .ok()
                    .flatten()
                    .map(|s| JsonValue::String(s.to_string()))
                    .unwrap_or(JsonValue::Null);
            }
            ColumnType::Datetime
            | ColumnType::Datetime2
            | ColumnType::Datetime4
            | ColumnType::Datetimen
            | ColumnType::Daten
            | ColumnType::Timen => {
                // Tiberius with its `chrono` feature exposes try_get<T>
                // for NaiveDateTime / NaiveDate / NaiveTime / DateTime<Utc>.
                // Without these, DATETIME columns silently return None and
                // become NULL downstream - the cascade-style bug we're
                // hunting. ISO-formatted strings travel cleanly to
                // DuckDB's read_json_auto which re-parses them as
                // TIMESTAMP / DATE / TIME.
                if let Ok(Some(dt)) = row.try_get::<chrono::NaiveDateTime, _>(i) {
                    return JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string());
                }
                if let Ok(Some(d)) = row.try_get::<chrono::NaiveDate, _>(i) {
                    return JsonValue::String(d.format("%Y-%m-%d").to_string());
                }
                if let Ok(Some(t)) = row.try_get::<chrono::NaiveTime, _>(i) {
                    return JsonValue::String(t.format("%H:%M:%S%.f").to_string());
                }
                row.try_get::<&str, _>(i)
                    .ok()
                    .flatten()
                    .map(|s| JsonValue::String(s.to_string()))
                    .unwrap_or(JsonValue::Null)
            }
            // VARBINARY / BINARY / IMAGE: base64. JSON can't carry raw bytes.
            ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => {
                use base64::engine::general_purpose::STANDARD as B64;
                use base64::Engine as _;
                row.try_get::<&[u8], _>(i)
                    .ok()
                    .flatten()
                    .map(|b| JsonValue::String(B64.encode(b)))
                    .unwrap_or(JsonValue::Null)
            }
            // GUID -> tiberius re-exposes its own Uuid type. Convert to
            // standard 8-4-4-4-12 hex form via its Display impl. If the
            // re-export changes name across versions, fall through to
            // the &str path which Tiberius supports for Guid columns.
            // GUID: tiberius only provides FromSql for its re-exported
            // Uuid type (the &str accessor doesn't match a Guid column, so
            // the old code always returned NULL). Emit the standard
            // 8-4-4-4-12 hex form.
            ColumnType::Guid => row
                .try_get::<tiberius::Uuid, _>(i)
                .ok()
                .flatten()
                .map(|u| JsonValue::String(u.to_string()))
                .unwrap_or(JsonValue::Null),
            // Everything else (NVarchar / Char / NText / SsVariant / etc):
            // string path. Tiberius' &str accessor handles N* types via
            // UTF-16 -> UTF-8 internally.
            _ => row
                .try_get::<&str, _>(i)
                .ok()
                .flatten()
                .map(|s| JsonValue::String(s.to_string()))
                .unwrap_or(JsonValue::Null),
        }
    }

    /// Cassandra / ScyllaDB sink via the scylla CQL driver. Each row
    /// becomes one INSERT statement (CQL doesn't support multi-row
    /// VALUES). Values are interpolated as literals; bind parameters
    /// would need per-column type detection which the scylla 0.13
    /// generic API makes painful.
    pub(crate) fn run_cassandra_sink(
        &self,
        db: &Path,
        spec: &CassandraSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!(
                "cassandra: 0 rows to insert into {}.{}",
                spec.keyspace, spec.table
            ));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => {
                return Err(EngineError::Query(
                    "cassandra: upstream rows aren't JSON objects".into(),
                ))
            }
        };
        let cols_list = cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let qualified = format!(
            "\"{}\".\"{}\"",
            spec.keyspace.replace('"', "\"\""),
            spec.table.replace('"', "\"\"")
        );
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("cassandra: tokio rt: {}", e)))?;
        let total = rt
            .block_on(async {
                let mut builder = scylla::SessionBuilder::new();
                for cp in spec.contact_points.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    builder = builder.known_node(cp);
                }
                if let (Some(u), Some(p)) = (&spec.user, &spec.password) {
                    builder = builder.user(u, p);
                }
                let session = builder
                    .build()
                    .await
                    .map_err(|e| format!("connect: {}", e))?;
                let mut total = 0_usize;
                for row in &rows {
                    if cancel.load(Ordering::Relaxed) {
                        return Err("cancelled".to_string());
                    }
                    let row_obj = row.as_object();
                    let vals: Vec<String> = cols
                        .iter()
                        .map(|c| {
                            let v = row_obj
                                .and_then(|o| o.get(c))
                                .unwrap_or(&JsonValue::Null);
                            sql_literal(v, None, Dialect::Cassandra)
                        })
                        .collect();
                    let stmt = format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        qualified,
                        cols_list,
                        vals.join(", ")
                    );
                    session
                        .query(stmt, &[])
                        .await
                        .map_err(|e| format!("insert: {}", e))?;
                    total += 1;
                }
                Ok::<usize, String>(total)
            })
            .map_err(|e| if e == "cancelled" {
                EngineError::Cancelled
            } else {
                EngineError::Query(format!("cassandra sink: {}", e))
            })?;
        Ok(format!(
            "cassandra: inserted {} rows into {}.{}",
            total, spec.keyspace, spec.table
        ))
    }

    /// Cassandra / ScyllaDB source via scylla. Best-effort CqlValue ->
    /// JsonValue conversion for the common types (numbers, text, bool,
    /// uuid, blob-as-base64).
    pub(crate) fn run_cassandra_source(
        &self,
        db: &Path,
        spec: &CassandraSourceSpec,
    ) -> Result<String, EngineError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("cassandra: tokio rt: {}", e)))?;
        let rows: Vec<JsonValue> = rt
            .block_on(async {
                let mut builder = scylla::SessionBuilder::new();
                for cp in spec.contact_points.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    builder = builder.known_node(cp);
                }
                if let (Some(u), Some(p)) = (&spec.user, &spec.password) {
                    builder = builder.user(u, p);
                }
                if let Some(ks) = &spec.keyspace {
                    builder = builder.use_keyspace(ks, false);
                }
                let session = builder
                    .build()
                    .await
                    .map_err(|e| format!("connect: {}", e))?;
                let result = session
                    .query(spec.query.clone(), &[])
                    .await
                    .map_err(|e| format!("query: {}", e))?;
                let cols: Vec<String> = result
                    .col_specs
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
                let rows = result.rows.unwrap_or_default();
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut obj = serde_json::Map::new();
                    for (i, name) in cols.iter().enumerate() {
                        let v = row
                            .columns
                            .get(i)
                            .and_then(|cv| cv.as_ref())
                            .map(cql_value_to_json)
                            .unwrap_or(JsonValue::Null);
                        obj.insert(name.clone(), v);
                    }
                    out.push(JsonValue::Object(obj));
                }
                Ok::<Vec<JsonValue>, String>(out)
            })
            .map_err(|e| EngineError::Query(format!("cassandra source: {}", e)))?;
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "cassandra: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// Redis SET sink via the sync redis client. For each upstream row,
    /// SET <keyColumn> <valueColumn|json(row)> [EX <ttl>]. Pipelined in
    /// chunks of batch_size to amortize the round-trip cost.
    pub(crate) fn run_redis_sink(
        &self,
        db: &Path,
        spec: &RedisSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("redis: 0 rows to SET (from {})", spec.from_view));
        }
        let client = redis::Client::open(spec.url.as_str())
            .map_err(|e| EngineError::Query(format!("redis: client open: {}", e)))?;
        let mut conn = client
            .get_connection()
            .map_err(|e| EngineError::Query(format!("redis: connect: {}", e)))?;
        let mut total = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            let mut pipe = redis::pipe();
            for row in chunk {
                let Some(obj) = row.as_object() else {
                    return Err(EngineError::Query(
                        "redis: upstream rows aren't JSON objects".into(),
                    ));
                };
                let key = obj
                    .get(&spec.key_column)
                    .map(|v| match v {
                        JsonValue::String(s) => s.clone(),
                        _ => v.to_string(),
                    })
                    .ok_or_else(|| {
                        EngineError::Query(format!(
                            "redis: keyColumn '{}' not in row",
                            spec.key_column
                        ))
                    })?;
                let value = if spec.value_column.is_empty() {
                    serde_json::to_string(row).unwrap_or_default()
                } else {
                    obj.get(&spec.value_column)
                        .map(|v| match v {
                            JsonValue::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                        .unwrap_or_default()
                };
                if spec.ttl_seconds > 0 {
                    pipe.cmd("SETEX")
                        .arg(&key)
                        .arg(spec.ttl_seconds)
                        .arg(&value)
                        .ignore();
                } else {
                    pipe.cmd("SET").arg(&key).arg(&value).ignore();
                }
            }
            redis::Pipeline::query::<()>(&pipe, &mut conn)
                .map_err(|e| EngineError::Query(format!("redis: SET batch: {}", e)))?;
            total += chunk.len();
        }
        Ok(format!("redis: SET {} key(s)", total))
    }

    /// Redis SCAN+GET source. Walks keys matching key_pattern via SCAN
    /// (cursor-based; safe for large keyspaces - never blocks like
    /// KEYS), then GETs each in pipelined batches of 500 and emits
    /// {key, value} rows. Limit caps the walk so a million-key DB
    /// doesn't take forever; defaults to 10_000.
    pub(crate) fn run_redis_source(
        &self,
        db: &Path,
        spec: &RedisSourceSpec,
    ) -> Result<String, EngineError> {
        let client = redis::Client::open(spec.url.as_str())
            .map_err(|e| EngineError::Query(format!("redis: client open: {}", e)))?;
        let mut conn = client
            .get_connection()
            .map_err(|e| EngineError::Query(format!("redis: connect: {}", e)))?;
        let mut keys: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            self.check_cancelled()?;
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&spec.key_pattern)
                .arg("COUNT")
                .arg(500_u32)
                .query(&mut conn)
                .map_err(|e| EngineError::Query(format!("redis: SCAN: {}", e)))?;
            keys.extend(batch);
            if keys.len() as u64 >= spec.limit {
                keys.truncate(spec.limit as usize);
                break;
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        let mut rows: Vec<JsonValue> = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(500) {
            self.check_cancelled()?;
            let mut pipe = redis::pipe();
            for k in chunk {
                pipe.cmd("GET").arg(k);
            }
            let values: Vec<Option<String>> = redis::Pipeline::query(&pipe, &mut conn)
                .map_err(|e| EngineError::Query(format!("redis: GET batch: {}", e)))?;
            for (k, v) in chunk.iter().zip(values) {
                let mut obj = serde_json::Map::new();
                obj.insert("key".into(), JsonValue::String(k.clone()));
                obj.insert(
                    "value".into(),
                    v.map(JsonValue::String).unwrap_or(JsonValue::Null),
                );
                rows.push(JsonValue::Object(obj));
            }
        }
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "redis: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// Qdrant scroll source. POSTs to /collections/{id}/points/scroll
    /// with {limit, offset, with_payload, with_vector}. The response
    /// puts the points in result.points[] and the next cursor in
    /// result.next_page_offset (null when done). Engine walks pages
    /// until max_pages or the cursor is null, then flattens each
    /// point into {id, ...payload[, vector]}.
    pub(crate) fn run_qdrant_source(
        &self,
        db: &Path,
        spec: &QdrantSourceSpec,
    ) -> Result<String, EngineError> {
        let base = spec.cluster_url.trim_end_matches('/');
        let url = format!("{}/collections/{}/points/scroll", base, spec.collection);
        let mut all_points: Vec<JsonValue> = Vec::new();
        let mut next_offset: Option<JsonValue> = None;
        for _ in 0..spec.max_pages {
            self.check_cancelled()?;
            let mut body = serde_json::Map::new();
            body.insert("limit".into(), JsonValue::from(spec.page_size));
            body.insert("with_payload".into(), JsonValue::Bool(true));
            body.insert("with_vector".into(), JsonValue::Bool(spec.with_vector));
            if let Some(off) = &next_offset {
                body.insert("offset".into(), off.clone());
            }
            let mut req = crate::tls::http_agent().post(&url)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            if !spec.api_key.is_empty() {
                req = req.set("api-key", &spec.api_key);
            }
            let resp = match req.send_string(&serde_json::to_string(&body).unwrap_or_default()) {
                Ok(r) => r.into_json::<JsonValue>().map_err(|e| {
                    EngineError::Query(format!("qdrant: response not JSON: {}", e))
                })?,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "qdrant HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "qdrant transport to {}: {}",
                        url, e
                    )));
                }
            };
            let result = resp.get("result").cloned().unwrap_or(JsonValue::Null);
            if let Some(points) = result.get("points").and_then(|v| v.as_array()) {
                for p in points {
                    let mut obj = serde_json::Map::new();
                    if let Some(id) = p.get("id") {
                        obj.insert("id".into(), id.clone());
                    }
                    if let Some(payload) = p.get("payload").and_then(|v| v.as_object()) {
                        for (k, v) in payload {
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                    if spec.with_vector {
                        if let Some(v) = p.get("vector") {
                            obj.insert("vector".into(), v.clone());
                        }
                    }
                    all_points.push(JsonValue::Object(obj));
                }
            }
            match result.get("next_page_offset") {
                Some(off) if !off.is_null() => next_offset = Some(off.clone()),
                _ => {
                    next_offset = None;
                    break;
                }
            }
        }
        // A non-null cursor surviving the loop means we stopped on the
        // page cap, not because the scroll was exhausted: more points
        // remain. Fail loud rather than materialize a silent subset.
        if next_offset.is_some() {
            return Err(pagination_capped_err(
                "qdrant",
                all_points.len(),
                spec.max_pages,
            ));
        }
        let count = all_points.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &all_points)?;
        Ok(format!(
            "qdrant: materialized {} points into {}",
            count, spec.node_id
        ))
    }

    /// Weaviate object-list source. GET /v1/objects?class=&limit=&after=
    /// returns {objects: [{id, class, properties, vector?}]}; cursor
    /// is the last object's id, passed as `after` on the next request.
    /// Loop terminates on a short page or max_pages.
    pub(crate) fn run_weaviate_source(
        &self,
        db: &Path,
        spec: &WeaviateSourceSpec,
    ) -> Result<String, EngineError> {
        let base = spec.endpoint.trim_end_matches('/');
        let mut all_objects: Vec<JsonValue> = Vec::new();
        let mut after: Option<String> = None;
        let mut more_pending = false;
        for _ in 0..spec.max_pages {
            self.check_cancelled()?;
            let mut url = format!(
                "{}/v1/objects?class={}&limit={}",
                base,
                urlencode_simple(&spec.class),
                spec.page_size
            );
            if spec.with_vector {
                url.push_str("&include=vector");
            }
            if let Some(a) = &after {
                url.push_str(&format!("&after={}", urlencode_simple(a)));
            }
            let mut req = crate::tls::http_agent().get(&url).set("Accept", "application/json");
            if !spec.api_key.is_empty() {
                req = req.set("Authorization", &format!("Bearer {}", spec.api_key));
            }
            let resp = match req.call() {
                Ok(r) => r.into_json::<JsonValue>().map_err(|e| {
                    EngineError::Query(format!("weaviate: response not JSON: {}", e))
                })?,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "weaviate HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "weaviate transport to {}: {}",
                        url, e
                    )));
                }
            };
            let Some(objs) = resp.get("objects").and_then(|v| v.as_array()) else {
                more_pending = false;
                break;
            };
            let page_len = objs.len();
            let mut last_id: Option<String> = None;
            for o in objs {
                let mut obj = serde_json::Map::new();
                if let Some(id) = o.get("id").and_then(|v| v.as_str()) {
                    obj.insert("id".into(), JsonValue::String(id.to_string()));
                    last_id = Some(id.to_string());
                }
                if let Some(props) = o.get("properties").and_then(|v| v.as_object()) {
                    for (k, v) in props {
                        obj.insert(k.clone(), v.clone());
                    }
                }
                if spec.with_vector {
                    if let Some(v) = o.get("vector") {
                        obj.insert("vector".into(), v.clone());
                    }
                }
                all_objects.push(JsonValue::Object(obj));
            }
            if page_len < spec.page_size as usize {
                more_pending = false;
                break;
            }
            match last_id {
                Some(id) => {
                    after = Some(id);
                    more_pending = true;
                }
                None => {
                    more_pending = false;
                    break;
                }
            }
        }
        if more_pending {
            return Err(pagination_capped_err(
                "weaviate",
                all_objects.len(),
                spec.max_pages,
            ));
        }
        let count = all_objects.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &all_objects)?;
        Ok(format!(
            "weaviate: materialized {} objects into {}",
            count, spec.node_id
        ))
    }

    /// Milvus query source. POST /v1/vector/query with {collectionName,
    /// filter, outputFields, limit, offset}. Response: {data: [...]}.
    /// Walks offset += page_size until a short page or max_pages.
    pub(crate) fn run_milvus_source(
        &self,
        db: &Path,
        spec: &MilvusSourceSpec,
    ) -> Result<String, EngineError> {
        let base = spec.endpoint.trim_end_matches('/');
        let url = format!("{}/v1/vector/query", base);
        let mut all_rows: Vec<JsonValue> = Vec::new();
        let mut offset: u64 = 0;
        let mut more_pending = false;
        for _ in 0..spec.max_pages {
            self.check_cancelled()?;
            let mut body = serde_json::Map::new();
            body.insert(
                "collectionName".into(),
                JsonValue::String(spec.collection.clone()),
            );
            body.insert("filter".into(), JsonValue::String(spec.filter.clone()));
            if !spec.output_fields.is_empty() {
                body.insert(
                    "outputFields".into(),
                    JsonValue::Array(
                        spec.output_fields
                            .iter()
                            .map(|f| JsonValue::String(f.clone()))
                            .collect(),
                    ),
                );
            }
            body.insert("limit".into(), JsonValue::from(spec.page_size));
            body.insert("offset".into(), JsonValue::from(offset));
            let mut req = crate::tls::http_agent().post(&url)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            if !spec.api_key.is_empty() {
                req = req.set("Authorization", &format!("Bearer {}", spec.api_key));
            }
            let resp = match req.send_string(&serde_json::to_string(&body).unwrap_or_default()) {
                Ok(r) => r.into_json::<JsonValue>().map_err(|e| {
                    EngineError::Query(format!("milvus: response not JSON: {}", e))
                })?,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "milvus HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "milvus transport to {}: {}",
                        url, e
                    )));
                }
            };
            let Some(arr) = resp.get("data").and_then(|v| v.as_array()) else {
                more_pending = false;
                break;
            };
            let page_len = arr.len();
            for v in arr {
                all_rows.push(v.clone());
            }
            if page_len < spec.page_size as usize {
                more_pending = false;
                break;
            }
            offset += spec.page_size;
            more_pending = true;
        }
        if more_pending {
            return Err(pagination_capped_err(
                "milvus",
                all_rows.len(),
                spec.max_pages,
            ));
        }
        let count = all_rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &all_rows)?;
        Ok(format!(
            "milvus: materialized {} points into {}",
            count, spec.node_id
        ))
    }

    /// YAML / TOML config-format reader. Parses the whole file with
    /// the relevant serde crate, normalizes the value into a Vec of
    /// row objects (top-level array becomes one row per element;
    /// anything else becomes a single row), and materializes via the
    /// shared json-table helper. Aimed at config-data ETL (Helm
    /// values, GitHub Actions matrices, Cargo deps audits), not at
    /// streaming gigabyte logs.
    pub(crate) fn run_format_source(
        &self,
        db: &Path,
        spec: &FormatFileSourceSpec,
    ) -> Result<String, EngineError> {
        let raw = std::fs::read_to_string(&spec.path).map_err(|e| {
            EngineError::Query(format!("{:?} source: read {}: {}", spec.format, spec.path, e))
        })?;
        let val: JsonValue = match spec.format {
            FormatKind::Yaml => serde_yaml::from_str(&raw).map_err(|e| {
                EngineError::Query(format!("yaml parse {}: {}", spec.path, e))
            })?,
            FormatKind::Toml => {
                let t: toml::Value = toml::from_str(&raw).map_err(|e| {
                    EngineError::Query(format!("toml parse {}: {}", spec.path, e))
                })?;
                serde_json::to_value(t).map_err(|e| {
                    EngineError::Query(format!("toml -> json {}: {}", spec.path, e))
                })?
            }
        };
        let rows: Vec<JsonValue> = match val {
            JsonValue::Array(a) => a,
            other => vec![other],
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "{:?}: materialized {} rows into {}",
            spec.format, count, spec.node_id
        ))
    }

    /// YAML / TOML config-format writer. Pulls every row from the
    /// upstream view, serializes the whole batch as a single doc.
    /// YAML emits a top-level `- key: value` array. TOML wraps in a
    /// `rows` key since TOML's top-level grammar disallows a bare
    /// array (you can't write `[ { ... }, { ... } ]` at the root).
    pub(crate) fn run_format_sink(
        &self,
        db: &Path,
        spec: &FormatFileSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        let payload = JsonValue::Array(rows.clone());
        let text = match spec.format {
            FormatKind::Yaml => serde_yaml::to_string(&payload).map_err(|e| {
                EngineError::Query(format!("yaml serialize: {}", e))
            })?,
            FormatKind::Toml => {
                // TOML doesn't allow a top-level array; wrap.
                let mut wrap = serde_json::Map::new();
                wrap.insert("rows".into(), payload);
                let t = serde_json::to_value(JsonValue::Object(wrap)).unwrap_or(JsonValue::Null);
                toml::to_string(&t).map_err(|e| {
                    EngineError::Query(format!("toml serialize: {}", e))
                })?
            }
        };
        std::fs::write(&spec.path, text).map_err(|e| {
            EngineError::Query(format!("{:?} sink: write {}: {}", spec.format, spec.path, e))
        })?;
        Ok(format!(
            "{:?}: wrote {} rows to {}",
            spec.format,
            rows.len(),
            spec.path
        ))
    }

    /// Apache Avro container-file reader via the pure-Rust apache-avro
    /// crate. The .avro file header carries its own schema, so the
    /// engine doesn't take any schema config - it iterates records,
    /// deserializes each Value into JSON, and materializes via the
    /// shared json-table helper. Works on every OS without depending
    /// on the DuckDB community avro extension.
    pub(crate) fn run_avro_source(
        &self,
        db: &Path,
        spec: &AvroSourceSpec,
    ) -> Result<String, EngineError> {
        let file = std::fs::File::open(&spec.path)
            .map_err(|e| EngineError::Query(format!("avro: open {}: {}", spec.path, e)))?;
        let reader = apache_avro::Reader::new(file)
            .map_err(|e| EngineError::Query(format!("avro: open container {}: {}", spec.path, e)))?;
        let mut rows: Vec<JsonValue> = Vec::new();
        for value in reader {
            self.check_cancelled()?;
            let v = value
                .map_err(|e| EngineError::Query(format!("avro: read record: {}", e)))?;
            let j: JsonValue = apache_avro::from_value(&v)
                .map_err(|e| EngineError::Query(format!("avro: value -> json: {}", e)))?;
            rows.push(j);
        }
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "avro: materialized {} records into {}",
            count, spec.node_id
        ))
    }

    /// src.qvd (#88): decode a Qlik QVD file with the clean-room crate::qvd
    /// reader and materialize its records as a table, like src.avro.
    pub(crate) fn run_qvd_source(
        &self,
        db: &Path,
        spec: &QvdSourceSpec,
    ) -> Result<String, EngineError> {
        let rows = crate::qvd::read_file(std::path::Path::new(&spec.path))
            .map_err(|e| EngineError::Query(format!("qvd: {}", e)))?;
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!("qvd: materialized {} records into {}", count, spec.node_id))
    }

    /// XML row-path source. Walks the document, builds a serde_json
    /// tree per element, and emits every element matching the
    /// trailing components of rowPath. Attributes become "@name"
    /// keys, text content goes to "_text" (or the value directly if
    /// the element has no children), nested elements nest naturally
    /// and convert to arrays when the same tag repeats.
    pub(crate) fn run_xml_source(
        &self,
        db: &Path,
        spec: &XmlSourceSpec,
    ) -> Result<String, EngineError> {
        let content = std::fs::read_to_string(&spec.path)
            .map_err(|e| EngineError::Query(format!("xml: read {}: {}", spec.path, e)))?;
        let rows = walk_xml_to_rows(&content, &spec.row_path, &self.cancel)?;
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "xml: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// XML wrapper-element writer. Emits
    ///   <root><row><col>val</col>...</row>...</root>
    /// Values are XML-escaped via quick-xml's writer; complex types
    /// (objects, arrays) get JSON-encoded inside CDATA so the file
    /// round-trips back through src.xml losslessly.
    pub(crate) fn run_xml_sink(
        &self,
        db: &Path,
        spec: &XmlSinkSpec,
    ) -> Result<String, EngineError> {
        use quick_xml::events::{BytesCData, BytesEnd, BytesStart, BytesText, Event};
        use quick_xml::writer::Writer;

        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        let mut writer = Writer::new_with_indent(&mut buf, b' ', 2);
        writer
            .write_event(Event::Decl(quick_xml::events::BytesDecl::new(
                "1.0", Some("UTF-8"), None,
            )))
            .map_err(|e| EngineError::Query(format!("xml: write decl: {}", e)))?;
        writer
            .write_event(Event::Start(BytesStart::new(spec.root_element.as_str())))
            .map_err(|e| EngineError::Query(format!("xml: write root: {}", e)))?;
        for row in &rows {
            self.check_cancelled()?;
            writer
                .write_event(Event::Start(BytesStart::new(spec.row_element.as_str())))
                .map_err(|e| EngineError::Query(format!("xml: write row: {}", e)))?;
            if let Some(obj) = row.as_object() {
                for (k, v) in obj {
                    // A DuckDB column name need not be a legal XML element name
                    // (e.g. "count(*)", a leading digit). Sanitize it and carry
                    // the original verbatim as a `name` attribute so the output
                    // is well-formed and round-trippable.
                    let elem = xml_safe_element_name(k);
                    let mut start = BytesStart::new(elem.as_str());
                    if elem != *k {
                        start.push_attribute(("name", k.as_str()));
                    }
                    writer
                        .write_event(Event::Start(start))
                        .map_err(|e| EngineError::Query(format!("xml: write col {}: {}", k, e)))?;
                    match v {
                        JsonValue::String(s) => {
                            writer
                                .write_event(Event::Text(BytesText::new(s)))
                                .map_err(|e| EngineError::Query(format!("xml: write text: {}", e)))?;
                        }
                        JsonValue::Null => {}
                        JsonValue::Bool(b) => {
                            writer
                                .write_event(Event::Text(BytesText::new(if *b {
                                    "true"
                                } else {
                                    "false"
                                })))
                                .map_err(|e| EngineError::Query(format!("xml: write bool: {}", e)))?;
                        }
                        JsonValue::Number(n) => {
                            writer
                                .write_event(Event::Text(BytesText::new(&n.to_string())))
                                .map_err(|e| EngineError::Query(format!("xml: write num: {}", e)))?;
                        }
                        JsonValue::Array(_) | JsonValue::Object(_) => {
                            // Round-trip complex shapes via JSON-in-CDATA. A
                            // CDATA section can't contain a literal "]]>", so
                            // split any occurrence across two sections; the
                            // reader concatenates them back to the original.
                            let json = serde_json::to_string(v).unwrap_or_default();
                            let safe = json.replace("]]>", "]]]]><![CDATA[>");
                            writer
                                .write_event(Event::CData(BytesCData::new(safe)))
                                .map_err(|e| EngineError::Query(format!("xml: write cdata: {}", e)))?;
                        }
                    }
                    writer
                        .write_event(Event::End(BytesEnd::new(elem.as_str())))
                        .map_err(|e| EngineError::Query(format!("xml: close col: {}", e)))?;
                }
            }
            writer
                .write_event(Event::End(BytesEnd::new(spec.row_element.as_str())))
                .map_err(|e| EngineError::Query(format!("xml: close row: {}", e)))?;
        }
        writer
            .write_event(Event::End(BytesEnd::new(spec.root_element.as_str())))
            .map_err(|e| EngineError::Query(format!("xml: close root: {}", e)))?;
        std::fs::write(&spec.path, buf)
            .map_err(|e| EngineError::Query(format!("xml: write {}: {}", spec.path, e)))?;
        Ok(format!("xml: wrote {} rows to {}", rows.len(), spec.path))
    }

    /// Avro container-file writer. Schema is inferred from the first
    /// row's column values (long / double / string / boolean / bytes /
    /// nullable-union for nulls), unless schemaJson is provided in
    /// which case it's parsed and used verbatim. Each row is written
    /// as one Avro record; the OCF format embeds the schema in the
    /// header so the file is self-describing.
    /// snk.qvd (#88): write upstream rows to a Qlik QVD file via crate::qvd.
    pub(crate) fn run_qvd_sink(
        &self,
        db: &Path,
        spec: &QvdSinkSpec,
    ) -> Result<String, EngineError> {
        let view = plan::quote_ident(&spec.from_view);
        // DESCRIBE for column order + types, so we (a) keep the schema even for a
        // 0-row table and (b) cast HUGEINT/UHUGEINT to BIGINT: DuckDB's CLI -json
        // prints HUGEINT as a quoted string (read_json_auto infers HUGEINT), which
        // would otherwise land integer columns in the QVD as text.
        let desc = self
            .run_rows(Some(db), &format!("DESCRIBE SELECT * FROM {}", view))?;
        let mut columns: Vec<String> = Vec::new();
        let mut replaces: Vec<String> = Vec::new();
        for r in &desc {
            let Some(o) = r.as_object() else { continue };
            let name = o
                .get("column_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            let ty = o
                .get("column_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_uppercase();
            if ty.contains("HUGEINT") {
                let q = plan::quote_ident(&name);
                replaces.push(format!("CAST({q} AS BIGINT) AS {q}"));
            }
            columns.push(name);
        }
        let select = if replaces.is_empty() {
            format!("SELECT * FROM {}", view)
        } else {
            format!("SELECT * REPLACE ({}) FROM {}", replaces.join(", "), view)
        };
        let rows = self.run_rows(Some(db), &select)?;
        crate::qvd::write_file(std::path::Path::new(&spec.path), &columns, &rows)
            .map_err(|e| EngineError::Query(format!("qvd: {}", e)))?;
        Ok(format!("qvd: wrote {} records to {}", rows.len(), spec.path))
    }

    /// src.gizmosql: query a GizmoSQL (Arrow Flight SQL) server, stream the
    /// result to a temp Parquet, then materialize it as a table.
    pub(crate) fn run_gizmosql_source(
        &self,
        db: &Path,
        spec: &GizmoSqlSourceSpec,
    ) -> Result<String, EngineError> {
        let conn = crate::gizmosql::GizmoConn {
            host: spec.host.clone(),
            port: spec.port,
            username: spec.username.clone(),
            password: spec.password.clone(),
            tls: spec.tls,
            tls_skip_verify: spec.tls_skip_verify,
        };
        let safe_node: String = spec
            .node_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let db_name = db
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parquet_path = db.with_file_name(format!("{}.gizmosql-{}.parquet", db_name, safe_node));
        let count = crate::gizmosql::query_to_parquet(&conn, &spec.query, &parquet_path)
            .map_err(EngineError::Query)?;
        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let create = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}')",
            plan::quote_ident(&spec.node_id),
            ppath
        );
        self.run(Some(db), &create, false)?;
        let _ = std::fs::remove_file(&parquet_path);
        Ok(format!(
            "gizmosql: materialized {} records into {}",
            count, spec.node_id
        ))
    }

    /// snk.gizmosql: CREATE the target table (DuckDB types from the upstream
    /// DESCRIBE) then batched INSERT, all over Flight SQL.
    pub(crate) fn run_gizmosql_sink(
        &self,
        db: &Path,
        spec: &GizmoSqlSinkSpec,
    ) -> Result<String, EngineError> {
        let view = plan::quote_ident(&spec.from_view);
        let desc = self.run_rows(Some(db), &format!("DESCRIBE SELECT * FROM {}", view))?;
        let mut cols: Vec<(String, String)> = Vec::new();
        for r in &desc {
            let Some(o) = r.as_object() else { continue };
            let name = o
                .get("column_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            let ty = o
                .get("column_type")
                .and_then(|v| v.as_str())
                .unwrap_or("VARCHAR")
                .to_string();
            cols.push((name, ty));
        }
        if cols.is_empty() {
            return Err(EngineError::Query("gizmosql: upstream has no columns".into()));
        }
        let rows = self.run_rows(Some(db), &format!("SELECT * FROM {}", view))?;

        let tbl = plan::quote_ident(&spec.table);
        let coldefs = cols
            .iter()
            .map(|(n, t)| format!("{} {}", plan::quote_ident(n), t))
            .collect::<Vec<_>>()
            .join(", ");
        let mut stmts: Vec<String> = Vec::new();
        match spec.mode.as_str() {
            "overwrite" | "create" => {
                stmts.push(format!("CREATE OR REPLACE TABLE {} ({})", tbl, coldefs))
            }
            _ => stmts.push(format!("CREATE TABLE IF NOT EXISTS {} ({})", tbl, coldefs)),
        }
        let colnames = cols
            .iter()
            .map(|(n, _)| plan::quote_ident(n))
            .collect::<Vec<_>>()
            .join(", ");
        for chunk in rows.chunks(500) {
            let mut tuples: Vec<String> = Vec::with_capacity(chunk.len());
            for r in chunk {
                let o = r.as_object();
                let tuple = cols
                    .iter()
                    .map(|(n, _)| gizmo_sql_literal(o.and_then(|o| o.get(n)).unwrap_or(&JsonValue::Null)))
                    .collect::<Vec<_>>()
                    .join(", ");
                tuples.push(format!("({})", tuple));
            }
            if !tuples.is_empty() {
                stmts.push(format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    tbl,
                    colnames,
                    tuples.join(", ")
                ));
            }
        }

        let conn = crate::gizmosql::GizmoConn {
            host: spec.host.clone(),
            port: spec.port,
            username: spec.username.clone(),
            password: spec.password.clone(),
            tls: spec.tls,
            tls_skip_verify: spec.tls_skip_verify,
        };
        crate::gizmosql::execute_updates(&conn, &stmts).map_err(EngineError::Query)?;
        Ok(format!("gizmosql: wrote {} rows to {}", rows.len(), spec.table))
    }

    pub(crate) fn run_avro_sink(
        &self,
        db: &Path,
        spec: &AvroSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            // Nothing to write - leave the file untouched rather than
            // creating an empty OCF with an arbitrary schema.
            return Ok(format!("avro: 0 rows to write to {}", spec.path));
        }
        let schema = if !spec.schema_json.is_empty() {
            apache_avro::Schema::parse_str(&spec.schema_json).map_err(|e| {
                EngineError::Query(format!("avro: parse schemaJson: {}", e))
            })?
        } else {
            let Some(first) = rows[0].as_object() else {
                return Err(EngineError::Query(
                    "avro: upstream rows aren't JSON objects".into(),
                ));
            };
            // Infer each field as a ["null", T] union by scanning all rows for
            // the first non-null value, so a null anywhere in a column (or in
            // row 0) doesn't abort the writer with a type mismatch.
            let fields: Vec<serde_json::Value> = first
                .keys()
                .map(|name| {
                    serde_json::json!({
                        "name": name,
                        "type": infer_avro_nullable_field(&rows, name),
                    })
                })
                .collect();
            let schema_json = serde_json::json!({
                "type": "record",
                "name": spec.record_name,
                "fields": fields,
            });
            apache_avro::Schema::parse_str(&schema_json.to_string()).map_err(|e| {
                EngineError::Query(format!("avro: parse inferred schema: {}", e))
            })?
        };
        let file = std::fs::File::create(&spec.path)
            .map_err(|e| EngineError::Query(format!("avro: create {}: {}", spec.path, e)))?;
        let mut writer = apache_avro::Writer::new(&schema, file);
        let mut total = 0_usize;
        for row in &rows {
            self.check_cancelled()?;
            // Build an Avro Record explicitly - apache_avro::to_value
            // on a JSON object returns Value::Map which the Record-
            // typed schema rejects. Record::new + put per field uses
            // the schema's known field list to coerce types.
            let Some(obj) = row.as_object() else {
                return Err(EngineError::Query(
                    "avro: upstream rows aren't JSON objects".into(),
                ));
            };
            let mut record = apache_avro::types::Record::new(&schema).ok_or_else(|| {
                EngineError::Query(
                    "avro: failed to build Record (schema is not a record type)".into(),
                )
            })?;
            for (k, v) in obj {
                record.put(k, json_to_avro_value(v));
            }
            // The inferred schema types every field as a ["null", T] union;
            // apache_avro won't encode a bare value against a union, so resolve
            // the record first to wrap each value into its matching branch
            // (also a no-op for a user-supplied non-union schema).
            let value = apache_avro::types::Value::from(record)
                .resolve(&schema)
                .map_err(|e| EngineError::Query(format!("avro: encode row: {}", e)))?;
            writer
                .append(value)
                .map_err(|e| EngineError::Query(format!("avro: append: {}", e)))?;
            total += 1;
        }
        writer
            .flush()
            .map_err(|e| EngineError::Query(format!("avro: flush: {}", e)))?;
        Ok(format!("avro: wrote {} records to {}", total, spec.path))
    }

    /// RabbitMQ / AMQP 0.9.1 publisher via lapin. Each upstream row
    /// becomes one persistent-delivery-mode message on (exchange,
    /// routingKey). Payload is JSON-stringified row.
    pub(crate) fn run_rabbit_sink(
        &self,
        db: &Path,
        spec: &RabbitSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("rabbit: 0 rows to publish to {}", spec.routing_key));
        }
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("rabbit: tokio rt: {}", e)))?;
        let total: Result<usize, String> = rt.block_on(async {
            use lapin::options::BasicPublishOptions;
            use lapin::{BasicProperties, Connection, ConnectionProperties};
            let conn = Connection::connect(&spec.url, ConnectionProperties::default())
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let channel = conn
                .create_channel()
                .await
                .map_err(|e| format!("channel: {}", e))?;
            // Enable publisher confirms so the awaited confirmation reflects a
            // real broker ack/nack; without confirm_select the publish "confirm"
            // is a no-op and a dropped/rejected message would be reported as
            // published.
            channel
                .confirm_select(lapin::options::ConfirmSelectOptions::default())
                .await
                .map_err(|e| format!("enable publisher confirms: {}", e))?;
            let props = BasicProperties::default().with_delivery_mode(2); // persistent
            let mut total = 0_usize;
            for chunk in rows.chunks(spec.batch_size) {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                for row in chunk {
                    let payload = serde_json::to_vec(row).unwrap_or_default();
                    let confirm = channel
                        .basic_publish(
                            &spec.exchange,
                            &spec.routing_key,
                            BasicPublishOptions::default(),
                            &payload,
                            props.clone(),
                        )
                        .await
                        .map_err(|e| format!("publish: {}", e))?
                        .await
                        .map_err(|e| format!("publish confirm: {}", e))?;
                    if confirm.is_nack() {
                        return Err("broker nacked a published message".into());
                    }
                }
                total += chunk.len();
            }
            Ok(total)
        });
        match total {
            Ok(n) => Ok(format!("rabbit: published {} message(s) to {}", n, spec.routing_key)),
            Err(e) if e == "cancelled" => Err(EngineError::Cancelled),
            Err(e) => Err(EngineError::Query(format!("rabbit sink: {}", e))),
        }
    }

    /// RabbitMQ / AMQP 0.9.1 consumer via lapin. basic_get-polls
    /// the queue (one message per call) until max_messages is
    /// reached or timeout_ms total wall-clock elapses. Auto-acks
    /// each pulled message; emits {payload, routing_key, exchange,
    /// delivery_tag} rows.
    pub(crate) fn run_rabbit_source(
        &self,
        db: &Path,
        spec: &RabbitSourceSpec,
    ) -> Result<String, EngineError> {
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("rabbit: tokio rt: {}", e)))?;
        let result: Result<usize, String> = rt.block_on(async {
            use lapin::options::{BasicAckOptions, BasicGetOptions};
            use lapin::{Connection, ConnectionProperties};
            let conn = Connection::connect(&spec.url, ConnectionProperties::default())
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let channel = conn
                .create_channel()
                .await
                .map_err(|e| format!("channel: {}", e))?;
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_millis(spec.timeout_ms);
            let mut out: Vec<JsonValue> = Vec::new();
            let mut tags: Vec<u64> = Vec::new();
            while (out.len() as u64) < spec.max_messages {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                let got = channel
                    .basic_get(&spec.queue, BasicGetOptions::default())
                    .await
                    .map_err(|e| format!("basic_get: {}", e))?;
                let Some(delivery) = got else {
                    // Empty queue - wait a tick and re-poll until the
                    // deadline; an explicit zero-wait poll would
                    // spin-CPU.
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                };
                let payload = String::from_utf8_lossy(&delivery.data).to_string();
                let mut obj = serde_json::Map::new();
                obj.insert("payload".into(), JsonValue::String(payload));
                obj.insert(
                    "routing_key".into(),
                    JsonValue::String(delivery.routing_key.to_string()),
                );
                obj.insert(
                    "exchange".into(),
                    JsonValue::String(delivery.exchange.to_string()),
                );
                obj.insert(
                    "delivery_tag".into(),
                    JsonValue::from(delivery.delivery_tag),
                );
                out.push(JsonValue::Object(obj));
                // Defer the ack: collect the tag and ack only after the batch
                // is durably materialized below, so a materialize failure
                // leaves the messages queued for redelivery instead of
                // acked-then-lost (mirrors run_pubsub_source).
                tags.push(delivery.delivery_tag);
            }
            // Persist BEFORE acknowledging.
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)
                .map_err(|e| format!("materialize: {}", e))?;
            // Now that the rows are written, ack each message. Ack failure is
            // non-fatal - an un-acked message simply redelivers next run.
            for tag in &tags {
                let _ = channel
                    .basic_ack(*tag, BasicAckOptions::default())
                    .await;
            }
            Ok(out.len())
        });
        let count = match result {
            Ok(c) => c,
            Err(e) if e == "cancelled" => return Err(EngineError::Cancelled),
            Err(e) => return Err(EngineError::Query(format!("rabbit source: {}", e))),
        };
        Ok(format!(
            "rabbit: materialized {} message(s) into {}",
            count, spec.node_id
        ))
    }

    /// Local git repo reader. Shells out to the system `git` CLI -
    /// no libgit2 dependency, no extra Rust crate. mode=log captures
    /// commit history as one row per commit; mode=files captures the
    /// tracked-file tree at a revision as one row per file. NUL-record
    /// + TAB-field framing avoids the usual `|` / newline pitfalls in
    /// commit subjects.
    pub(crate) fn run_git_source(&self, db: &Path, spec: &GitSourceSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let mode = spec.mode.as_str();
        let max = spec.max_rows.to_string();
        let rows: Vec<JsonValue> = match mode {
            "log" => {
                let mut cmd = std::process::Command::new("git");
                #[cfg(windows)]
                {
                    use std::os::windows::process::CommandExt;
                    cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
                }
                cmd.arg("-C")
                    .arg(&spec.repo)
                    .arg("log")
                    .arg("-z")
                    .arg("--max-count")
                    .arg(&max)
                    .arg("--date=iso-strict")
                    .arg("--pretty=format:%H%x09%h%x09%an%x09%ae%x09%ad%x09%s")
                    .arg(&spec.revision);
                if let Some(p) = &spec.path_filter {
                    cmd.arg("--").arg(p);
                }
                let out = cmd
                    .output()
                    .map_err(|e| EngineError::Query(format!("git log: spawn: {}", e)))?;
                if !out.status.success() {
                    return Err(EngineError::Query(format!(
                        "git log exited {}: {}",
                        out.status,
                        String::from_utf8_lossy(&out.stderr)
                    )));
                }
                parse_git_log(&out.stdout)
            }
            "files" => {
                let mut cmd = std::process::Command::new("git");
                #[cfg(windows)]
                {
                    use std::os::windows::process::CommandExt;
                    cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
                }
                cmd.arg("-C")
                    .arg(&spec.repo)
                    .arg("ls-tree")
                    .arg("-r")
                    .arg("-z")
                    .arg("--long")
                    .arg(&spec.revision);
                if let Some(p) = &spec.path_filter {
                    cmd.arg("--").arg(p);
                }
                let out = cmd
                    .output()
                    .map_err(|e| EngineError::Query(format!("git ls-tree: spawn: {}", e)))?;
                if !out.status.success() {
                    return Err(EngineError::Query(format!(
                        "git ls-tree exited {}: {}",
                        out.status,
                        String::from_utf8_lossy(&out.stderr)
                    )));
                }
                parse_git_ls_tree(&out.stdout, spec.max_rows as usize)
            }
            other => {
                return Err(EngineError::Config(format!(
                    "src.git: mode '{}' not supported (use 'log' or 'files')",
                    other
                )))
            }
        };
        self.check_cancelled()?;
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "git ({}): materialized {} row(s) into {}",
            mode, count, spec.node_id
        ))
    }

    /// code.shell: run a single command and emit one row with the
    /// captured stdout/stderr/exit_code/duration_ms. Shell defaults to
    /// cmd.exe on Windows and /bin/sh on Unix; override per stage with
    /// `shell`. Polls a kill-on-cancel loop every 100ms while the child
    /// runs so a long-running command doesn't pin a cancelled pipeline.
    pub(crate) fn run_shell(&self, db: &Path, spec: &ShellSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let started = std::time::Instant::now();
        // Pick shell + argument form.
        let (shell_cmd, flag) = match spec.shell.as_deref() {
            Some(custom) => (custom.to_string(), "-c".to_string()),
            None => {
                if cfg!(windows) {
                    ("cmd.exe".to_string(), "/C".to_string())
                } else {
                    ("/bin/sh".to_string(), "-c".to_string())
                }
            }
        };
        let mut cmd = std::process::Command::new(&shell_cmd);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        cmd.arg(&flag).arg(&spec.command);
        if let Some(dir) = &spec.working_dir {
            cmd.current_dir(dir);
        }
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        let mut child = cmd
            .spawn()
            .map_err(|e| EngineError::Query(format!("shell spawn: {}", e)))?;
        // Drain stdout AND stderr on dedicated threads, the same way run()
        // does, so the child can never deadlock against a full OS pipe
        // buffer (~64 KiB on Windows). The previous code polled try_wait()
        // to exit and only read via wait_with_output() afterwards - a
        // user command emitting more than the buffer (a verbose build log,
        // a recursive listing, `type`/`cat` of a file) blocked writing
        // stdout/stderr while we blocked waiting for exit. With no timeout
        // that hung forever; with one it was killed and misreported as a
        // timeout, discarding output. Concurrent readers keep both pipes
        // drained regardless of size.
        use std::io::Read;
        let mut stdout_pipe = child
            .stdout
            .take()
            .ok_or_else(|| EngineError::Query("shell: stdout not captured".into()))?;
        let mut stderr_pipe = child
            .stderr
            .take()
            .ok_or_else(|| EngineError::Query("shell: stderr not captured".into()))?;
        let stdout_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stdout_pipe.read_to_end(&mut buf);
            buf
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stderr_pipe.read_to_end(&mut buf);
            buf
        });
        // Poll: cancel kills the child; timeout kills the child; else
        // wait for natural exit.
        //
        // On the abort paths (cancel / timeout / wait error) we DON'T join
        // the reader threads: a shell spawns the real command as a
        // grandchild that inherits the pipe write ends, and killing the
        // shell does not kill the grandchild. read_to_end would then block
        // until the grandchild exits on its own - which for a `sleep 30`
        // is exactly the hang the timeout is meant to escape. We discard
        // the output when aborting anyway, so the reader threads are left
        // to finish on their own (they exit once the grandchild releases
        // the pipe). Only the natural-exit path joins to collect output.
        let deadline = spec
            .timeout_ms
            .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break s,
                Ok(None) => {}
                Err(e) => {
                    let _ = child.kill();
                    return Err(EngineError::Query(format!("shell wait: {}", e)));
                }
            }
            if self.cancel.load(Ordering::Relaxed) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(EngineError::Cancelled);
            }
            if let Some(d) = deadline {
                if std::time::Instant::now() >= d {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(EngineError::Query(format!(
                        "shell: timeout after {}ms",
                        spec.timeout_ms.unwrap_or(0)
                    )));
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        };
        let stdout_bytes = stdout_reader.join().unwrap_or_default();
        let stderr_bytes = stderr_reader.join().unwrap_or_default();
        let duration_ms = started.elapsed().as_millis() as i64;
        let exit_code = status.code().unwrap_or(-1);
        let mut row = serde_json::Map::new();
        row.insert(
            "stdout".into(),
            JsonValue::String(String::from_utf8_lossy(&stdout_bytes).into_owned()),
        );
        row.insert(
            "stderr".into(),
            JsonValue::String(String::from_utf8_lossy(&stderr_bytes).into_owned()),
        );
        row.insert("exit_code".into(), JsonValue::from(exit_code));
        row.insert("duration_ms".into(), JsonValue::from(duration_ms));
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &[JsonValue::Object(row)])?;
        Ok(format!(
            "shell: exit {} in {}ms -> {}",
            exit_code, duration_ms, spec.node_id
        ))
    }

    /// xf.dbt: run a dbt Core project (dbt-duckdb adapter) against the run's
    /// working database. The per-stage CLI spawn model means no process holds
    /// the database open between stages, so dbt gets exclusive access during
    /// this stage: its models read upstream node tables directly and the
    /// tables it builds are readable by downstream stages. profiles.yml is
    /// generated per run into a temp dir, named after the project's declared
    /// profile, so the user's project runs unmodified. The upstream table
    /// name (when wired) is passed as var("duckle_input").
    pub(crate) fn run_dbt(&self, db: &Path, spec: &DbtSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let started = std::time::Instant::now();
        // Resolve the project directory. Inline mode (no project_dir) scaffolds
        // an ephemeral one-model project from spec.inline_model into a temp dir.
        let scaffolded;
        let project_dir: &Path = match &spec.project_dir {
            Some(dir) => Path::new(dir),
            None => {
                let model = spec.inline_model.as_deref().ok_or_else(|| {
                    EngineError::Config(
                        "xf.dbt: inline mode needs model SQL (or set projectDir)".into(),
                    )
                })?;
                scaffolded = scaffold_inline_dbt_project(&spec.node_id, &spec.inline_model_name, model)
                    .map_err(|e| EngineError::Query(format!("xf.dbt: scaffold inline project: {e}")))?;
                scaffolded.as_path()
            }
        };
        let project_dir_str = project_dir.to_string_lossy().into_owned();
        let project_file = project_dir.join("dbt_project.yml");
        let project_text = std::fs::read_to_string(&project_file).map_err(|_| {
            EngineError::Config(format!(
                "xf.dbt: '{}' does not look like a dbt project (dbt_project.yml not found)",
                project_dir_str
            ))
        })?;
        // Name the generated profile after the project's `profile:` so the
        // project runs unmodified; fall back to "duckle" + --profile flag.
        let declared_profile = serde_yaml::from_str::<serde_yaml::Value>(&project_text)
            .ok()
            .and_then(|v| v.get("profile").and_then(|p| p.as_str().map(String::from)));
        let (profile_name, force_profile_flag) = match declared_profile {
            Some(p) if !p.trim().is_empty() => (p, false),
            _ => ("duckle".to_string(), true),
        };

        // Target database: the run db by default, so dbt composes with the
        // rest of the canvas. YAML wants forward slashes on Windows.
        let target_db = spec
            .database
            .clone()
            .unwrap_or_else(|| db.to_string_lossy().into_owned());
        let target_db_yaml = target_db.replace('\\', "/");

        let profiles_dir = std::env::temp_dir().join(format!(
            "duckle_dbt_{}_{}",
            std::process::id(),
            spec.node_id.replace(|c: char| !c.is_alphanumeric(), "_")
        ));
        std::fs::create_dir_all(&profiles_dir)
            .map_err(|e| EngineError::Query(format!("xf.dbt: profiles dir: {}", e)))?;
        let profiles_yaml = format!(
            "{}:\n  target: duckle\n  outputs:\n    duckle:\n      type: duckdb\n      path: \"{}\"\n      schema: {}\n      threads: 1\n",
            profile_name, target_db_yaml, spec.schema
        );
        std::fs::write(profiles_dir.join("profiles.yml"), profiles_yaml)
            .map_err(|e| EngineError::Query(format!("xf.dbt: write profiles.yml: {}", e)))?;

        // Assemble: dbt <user command tokens> --project-dir .. --profiles-dir ..
        // The command is split on whitespace (documented; no shell quoting),
        // which avoids cmd.exe/sh quoting pitfalls entirely.
        let mut args: Vec<String> =
            spec.command.split_whitespace().map(|s| s.to_string()).collect();
        if args.is_empty() {
            args.push("run".into());
        }
        args.push("--project-dir".into());
        args.push(project_dir_str.clone());
        args.push("--profiles-dir".into());
        args.push(profiles_dir.to_string_lossy().into_owned());
        if force_profile_flag {
            args.push("--profile".into());
            args.push(profile_name.clone());
        }
        // Expose the upstream tables to dbt: the first as var('duckle_input')
        // (back-compat / single-source) and ALL of them as the list
        // var('duckle_inputs') for multi-source inline models.
        if !spec.from_views.is_empty() {
            args.push("--vars".into());
            args.push(
                serde_json::json!({
                    "duckle_input": spec.from_views.first(),
                    "duckle_inputs": spec.from_views,
                })
                .to_string(),
            );
        } else if let Some(fv) = &spec.from_view {
            args.push("--vars".into());
            args.push(serde_json::json!({ "duckle_input": fv }).to_string());
        }

        let dbt_bin = resolve_dbt_bin(spec.dbt_bin.as_deref());
        let mut cmd = std::process::Command::new(&dbt_bin);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        cmd.args(&args);
        cmd.current_dir(project_dir);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                EngineError::Config(format!(
                    "xf.dbt: dbt was not found (tried '{}'). Duckle ships a bundled dbt \
                     engine; if you are running a bare build, install dbt with the DuckDB \
                     adapter (pipx install dbt-duckdb) or set the 'dbtBin' property to the \
                     dbt executable path.",
                    dbt_bin
                ))
            } else {
                EngineError::Query(format!("xf.dbt: spawn {}: {}", dbt_bin, e))
            }
        })?;

        // Same pipe-drain + cancel/timeout discipline as run_shell: reader
        // threads keep both pipes drained (dbt logs are chatty), the poll
        // loop kills the child on cancel or deadline.
        use std::io::Read;
        let mut stdout_pipe = child
            .stdout
            .take()
            .ok_or_else(|| EngineError::Query("xf.dbt: stdout not captured".into()))?;
        let mut stderr_pipe = child
            .stderr
            .take()
            .ok_or_else(|| EngineError::Query("xf.dbt: stderr not captured".into()))?;
        let stdout_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stdout_pipe.read_to_end(&mut buf);
            buf
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stderr_pipe.read_to_end(&mut buf);
            buf
        });
        let deadline = spec
            .timeout_ms
            .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
        let status = loop {
            match child.try_wait() {
                Ok(Some(s)) => break s,
                Ok(None) => {}
                Err(e) => {
                    let _ = child.kill();
                    return Err(EngineError::Query(format!("xf.dbt: wait: {}", e)));
                }
            }
            if self.cancel.load(Ordering::Relaxed) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(EngineError::Cancelled);
            }
            if let Some(d) = deadline {
                if std::time::Instant::now() >= d {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(EngineError::Query(format!(
                        "xf.dbt: timeout after {}ms",
                        spec.timeout_ms.unwrap_or(0)
                    )));
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        };
        let stdout_text =
            String::from_utf8_lossy(&stdout_reader.join().unwrap_or_default()).into_owned();
        let stderr_text =
            String::from_utf8_lossy(&stderr_reader.join().unwrap_or_default()).into_owned();
        let duration_ms = started.elapsed().as_millis() as i64;

        if !status.success() {
            // dbt reports model errors on stdout; keep the tail of both
            // streams so the failure names the model and the SQL error.
            let mut detail = String::new();
            if !stdout_text.trim().is_empty() {
                detail.push_str(tail_chars(stdout_text.trim(), 2000));
            }
            if !stderr_text.trim().is_empty() {
                if !detail.is_empty() {
                    detail.push('\n');
                }
                detail.push_str(tail_chars(stderr_text.trim(), 1000));
            }
            return Err(EngineError::Query(format!(
                "xf.dbt: dbt exited with code {} after {}ms\n{}",
                status.code().unwrap_or(-1),
                duration_ms,
                detail
            )));
        }

        // Per-model summary from target/run_results.json (written by run /
        // build / test / seed / snapshot). Commands that build nothing
        // (deps, parse) produce a single status row instead.
        let results_path = project_dir.join("target").join("run_results.json");
        let model_rows: Vec<JsonValue> = std::fs::read_to_string(&results_path)
            .ok()
            .and_then(|t| serde_json::from_str::<JsonValue>(&t).ok())
            .and_then(|v| v.get("results").and_then(|r| r.as_array()).cloned())
            .map(|results| {
                results
                    .iter()
                    .map(|r| {
                        let mut row = serde_json::Map::new();
                        let model = r
                            .get("unique_id")
                            .and_then(|u| u.as_str())
                            .map(|u| u.rsplit('.').next().unwrap_or(u).to_string())
                            .unwrap_or_default();
                        row.insert("model".into(), JsonValue::String(model));
                        row.insert(
                            "status".into(),
                            r.get("status").cloned().unwrap_or(JsonValue::Null),
                        );
                        row.insert(
                            "execution_time_s".into(),
                            r.get("execution_time").cloned().unwrap_or(JsonValue::Null),
                        );
                        row.insert(
                            "message".into(),
                            r.get("message").cloned().unwrap_or(JsonValue::Null),
                        );
                        JsonValue::Object(row)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let model_count = model_rows.len();

        match &spec.output_model {
            Some(model) => {
                // The node's output is the built model itself, read back
                // from the target database into the run db when they differ.
                let select = if spec.database.is_some() {
                    let attach_path = target_db.replace('\'', "''");
                    format!(
                        "ATTACH '{}' AS __dbt_out (READ_ONLY); \
                         CREATE OR REPLACE TABLE {} AS SELECT * FROM __dbt_out.{}.{};",
                        attach_path,
                        plan::quote_ident(&spec.node_id),
                        plan::quote_ident(&spec.schema),
                        plan::quote_ident(model)
                    )
                } else {
                    format!(
                        "CREATE OR REPLACE TABLE {} AS SELECT * FROM {};",
                        plan::quote_ident(&spec.node_id),
                        plan::quote_ident(model)
                    )
                };
                self.run(Some(db), &select, false).map_err(|e| {
                    EngineError::Query(format!(
                        "xf.dbt: dbt succeeded but reading outputModel '{}' back failed: {}",
                        model, e
                    ))
                })?;
            }
            None => {
                let rows = if model_rows.is_empty() {
                    let mut row = serde_json::Map::new();
                    row.insert("model".into(), JsonValue::Null);
                    row.insert("status".into(), JsonValue::String("success".into()));
                    row.insert("execution_time_s".into(), JsonValue::Null);
                    row.insert(
                        "message".into(),
                        JsonValue::String(
                            "dbt exited 0; no run_results.json (command builds no models)"
                                .into(),
                        ),
                    );
                    vec![JsonValue::Object(row)]
                } else {
                    model_rows
                };
                materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
            }
        }

        Ok(format!(
            "dbt: exit 0 in {}ms, {} model result(s) -> {}",
            duration_ms, model_count, spec.node_id
        ))
    }

    /// src.ftp: connect, login, list `directory`, filter by optional
    /// glob `pattern`, download up to `max_files`. Each file becomes a
    /// row {filename, size, content_b64, modified}. Content is base64-
    /// encoded so the row stays JSON-clean for downstream stages /
    /// CSV sinks; downstream can use `from_base64()` in DuckDB if it
    /// needs raw bytes back.
    pub(crate) fn run_ftp_source(&self, db: &Path, spec: &FtpSourceSpec) -> Result<String, EngineError> {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine as _;
        use suppaftp::FtpStream;
        self.check_cancelled()?;
        // SFTP (SSH File Transfer Protocol) is a completely different protocol
        // from FTP / FTPS and is not supported yet (issue #16; on the roadmap,
        // it needs an SSH stack). Catch the common mistake of pointing this
        // component at an SFTP server - port 22, or an sftp:// / ssh:// host -
        // and fail with a clear message instead of suppaftp's cryptic
        // "Response contains an invalid syntax" (which is what you get when an
        // FTP client reads an SSH banner).
        if is_sftp_target(&spec.host, spec.port) {
            return Err(EngineError::Config(
                "src.ftp speaks FTP / FTPS, not SFTP (SSH File Transfer). SFTP is a different protocol and is not supported yet (it is on the roadmap). If this is an FTP/FTPS server, use its FTP port (commonly 21); if it is genuinely SFTP, it cannot be read through this component."
                    .into(),
            ));
        }
        // Accept an ftp:// / ftps:// scheme on the host by stripping it; the
        // connect address is host:port.
        let host_l = spec.host.trim().to_ascii_lowercase();
        let host = host_l
            .strip_prefix("ftps://")
            .or_else(|| host_l.strip_prefix("ftp://"))
            .map(|h| h.trim_end_matches('/'))
            .unwrap_or_else(|| spec.host.trim());
        let addr = format!("{}:{}", host, spec.port);
        let mut ftp = FtpStream::connect(&addr)
            .map_err(|e| EngineError::Query(format!("ftp connect {}: {}", addr, e)))?;
        if spec.secure {
            return Err(EngineError::Config(
                "src.ftp: secure=true (FTPS) requires the rustls TLS wrapper which isn't wired up yet. Use secure=false (plain FTP) or wait for the FTPS-explicit feature.".into(),
            ));
        }
        ftp.login(&spec.user, &spec.password)
            .map_err(|e| EngineError::Query(format!("ftp login: {}", e)))?;
        if !spec.directory.is_empty() && spec.directory != "/" {
            ftp.cwd(&spec.directory)
                .map_err(|e| EngineError::Query(format!("ftp cwd {}: {}", spec.directory, e)))?;
        }
        let names = ftp
            .nlst(None)
            .map_err(|e| EngineError::Query(format!("ftp nlst: {}", e)))?;
        let mut rows: Vec<JsonValue> = Vec::new();
        for name in names.iter() {
            self.check_cancelled()?;
            if rows.len() as u64 >= spec.max_files {
                break;
            }
            if let Some(p) = &spec.pattern {
                if !glob_match(p, name) {
                    continue;
                }
            }
            let size = ftp.size(name).ok().map(|n| n as i64);
            // mdtm returns NaiveDateTime in UTC by the FTP spec.
            let modified = ftp
                .mdtm(name)
                .ok()
                .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string());
            let bytes = match ftp.retr_as_buffer(name) {
                Ok(cur) => cur.into_inner(),
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "ftp retr {}: {}",
                        name, e
                    )))
                }
            };
            let mut row = serde_json::Map::new();
            row.insert("filename".into(), JsonValue::String(name.clone()));
            row.insert(
                "size".into(),
                size.map(JsonValue::from).unwrap_or(JsonValue::Null),
            );
            row.insert(
                "modified".into(),
                modified.map(JsonValue::String).unwrap_or(JsonValue::Null),
            );
            row.insert(
                "content_b64".into(),
                JsonValue::String(B64.encode(&bytes)),
            );
            rows.push(JsonValue::Object(row));
        }
        let _ = ftp.quit();
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "ftp: materialized {} file(s) from {}:{} into {}",
            count, spec.host, spec.port, spec.node_id
        ))
    }

    /// src.sftp: connect over SSH, verify the host key against an optional
    /// SHA256 fingerprint pin, authenticate (private key or password), list
    /// `directory`, filter by optional glob `pattern`, download up to
    /// `max_files`. Each file becomes a row {filename, size, content_b64,
    /// modified}. russh / russh-sftp are async (ring backend); we drive them
    /// on a private current-thread tokio runtime so the stage stays blocking
    /// like every other source.
    pub(crate) fn run_sftp_source(&self, db: &Path, spec: &SftpSourceSpec) -> Result<String, EngineError> {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine as _;
        self.check_cancelled()?;

        // Host-key verification. With a pinned fingerprint, refuse any other
        // server key; without one, accept on trust (trust-on-first-use).
        struct Verifier {
            expected: Option<String>,
        }
        impl russh::client::Handler for Verifier {
            type Error = russh::Error;
            async fn check_server_key(
                &mut self,
                server_public_key: &russh::keys::ssh_key::PublicKey,
            ) -> Result<bool, Self::Error> {
                match &self.expected {
                    None => Ok(true),
                    Some(want) => {
                        let got = server_public_key
                            .fingerprint(russh::keys::HashAlg::Sha256)
                            .to_string();
                        // Compare case-sensitively but tolerant of the
                        // "SHA256:" prefix on either side.
                        let norm = |s: &str| s.trim().trim_start_matches("SHA256:").to_string();
                        Ok(norm(&got) == norm(want))
                    }
                }
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("sftp: tokio rt: {}", e)))?;

        let result: Result<Vec<JsonValue>, String> = rt.block_on(async {
            use russh_sftp::client::SftpSession;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let config = std::sync::Arc::new(russh::client::Config::default());
            let handler = Verifier {
                expected: spec.host_fingerprint.clone(),
            };
            let mut session =
                russh::client::connect(config, (spec.host.as_str(), spec.port), handler)
                    .await
                    .map_err(|e| format!("connect {}:{}: {}", spec.host, spec.port, e))?;

            // Auth: a private key wins over a password if both are present.
            let authed = if let Some(pem) = &spec.private_key {
                let key = russh::keys::decode_secret_key(pem, spec.key_passphrase.as_deref())
                    .map_err(|e| format!("private key: {}", e))?;
                let with_alg = russh::keys::PrivateKeyWithHashAlg::new(
                    std::sync::Arc::new(key),
                    Some(russh::keys::HashAlg::Sha256),
                );
                session
                    .authenticate_publickey(spec.user.as_str(), with_alg)
                    .await
                    .map_err(|e| format!("publickey auth: {}", e))?
                    .success()
            } else if let Some(pw) = &spec.password {
                session
                    .authenticate_password(spec.user.as_str(), pw)
                    .await
                    .map_err(|e| format!("password auth: {}", e))?
                    .success()
            } else {
                return Err("no credentials: set a password or a private key".into());
            };
            if !authed {
                return Err(format!(
                    "authentication failed for user '{}' (check credentials / host fingerprint)",
                    spec.user
                ));
            }

            let channel = session
                .channel_open_session()
                .await
                .map_err(|e| format!("open channel: {}", e))?;
            channel
                .request_subsystem(true, "sftp")
                .await
                .map_err(|e| format!("request sftp subsystem: {}", e))?;
            let sftp = SftpSession::new(channel.into_stream())
                .await
                .map_err(|e| format!("sftp session: {}", e))?;

            let entries = sftp
                .read_dir(spec.directory.clone())
                .await
                .map_err(|e| format!("read_dir {}: {}", spec.directory, e))?;

            let mut rows: Vec<JsonValue> = Vec::new();
            for entry in entries {
                if rows.len() as u64 >= spec.max_files {
                    break;
                }
                if entry.file_type().is_dir() {
                    continue;
                }
                let name = entry.file_name();
                if let Some(p) = &spec.pattern {
                    if !glob_match(p, &name) {
                        continue;
                    }
                }
                let meta = entry.metadata();
                let size = meta.size.map(|n| n as i64);
                let modified = meta.mtime.and_then(|t| {
                    chrono::DateTime::<chrono::Utc>::from_timestamp(t as i64, 0)
                        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                });
                let full = entry.path();
                let mut file = sftp
                    .open(full.clone())
                    .await
                    .map_err(|e| format!("open {}: {}", full, e))?;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)
                    .await
                    .map_err(|e| format!("read {}: {}", full, e))?;
                let _ = file.shutdown().await;

                let mut row = serde_json::Map::new();
                row.insert("filename".into(), JsonValue::String(name));
                row.insert(
                    "size".into(),
                    size.map(JsonValue::from).unwrap_or(JsonValue::Null),
                );
                row.insert(
                    "modified".into(),
                    modified.map(JsonValue::String).unwrap_or(JsonValue::Null),
                );
                row.insert("content_b64".into(), JsonValue::String(B64.encode(&bytes)));
                rows.push(JsonValue::Object(row));
            }
            Ok(rows)
        });

        let rows = result.map_err(EngineError::Query)?;
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "sftp: materialized {} file(s) from {}:{} into {}",
            count, spec.host, spec.port, spec.node_id
        ))
    }

    /// COPY the upstream view to a local temp file in `format`
    /// (csv | parquet | json | jsonl; default csv) and return the temp path.
    /// The caller uploads the file then removes it. Mirrors the file-sink COPY
    /// syntax (build_csv_sink / build_parquet_sink / build_json_sink): JSON
    /// "array=true" gives a single JSON array; jsonl gives newline-delimited.
    fn ftp_copy_view_to_temp(
        &self,
        db: &Path,
        from_view: &str,
        format: &str,
    ) -> Result<std::path::PathBuf, EngineError> {
        let ext = match format {
            "parquet" => "parquet",
            "json" => "json",
            "jsonl" => "jsonl",
            _ => "csv",
        };
        let name = format!("duckle-ftp-{}.{}", std::process::id(), ext);
        let path = std::env::temp_dir().join(name);
        // Best-effort clear of any stale temp from a prior run with the same pid.
        let _ = std::fs::remove_file(&path);
        let view = plan::quote_ident(from_view);
        let target = sql_escape(&path.display().to_string());
        let copy = match format {
            "parquet" => format!(
                "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET)",
                view, target
            ),
            "json" => format!(
                "COPY (SELECT * FROM {}) TO '{}' (FORMAT JSON, ARRAY true)",
                view, target
            ),
            "jsonl" => format!(
                "COPY (SELECT * FROM {}) TO '{}' (FORMAT JSON, ARRAY false)",
                view, target
            ),
            _ => format!(
                "COPY (SELECT * FROM {}) TO '{}' (FORMAT CSV, HEADER true)",
                view, target
            ),
        };
        self.run(Some(db), &copy, false)?;
        Ok(path)
    }

    /// snk.ftp (FTP / FTPS): COPY the upstream view to a local temp file in
    /// `format`, connect + login with suppaftp, upload the file to
    /// `remote_path` via put_file, then remove the temp file. SFTP targets are
    /// rejected (a different protocol - use the SFTP option); FTPS is guarded
    /// the same way as the source until the TLS wrapper is wired.
    pub(crate) fn run_ftp_sink(&self, db: &Path, spec: &FtpSinkSpec) -> Result<String, EngineError> {
        use suppaftp::FtpStream;
        self.check_cancelled()?;
        if is_sftp_target(&spec.host, spec.port) {
            return Err(EngineError::Config(
                "snk.ftp (FTP / FTPS) cannot upload to an SFTP (SSH File Transfer) server - it is a different protocol. Choose the SFTP protocol option, or point this at an FTP/FTPS port (commonly 21)."
                    .into(),
            ));
        }
        let host_l = spec.host.trim().to_ascii_lowercase();
        let host = host_l
            .strip_prefix("ftps://")
            .or_else(|| host_l.strip_prefix("ftp://"))
            .map(|h| h.trim_end_matches('/'))
            .unwrap_or_else(|| spec.host.trim());
        let addr = format!("{}:{}", host, spec.port);

        let temp = self.ftp_copy_view_to_temp(db, &spec.from_view, &spec.format)?;
        let upload = (|| -> Result<u64, EngineError> {
            let bytes = std::fs::read(&temp)
                .map_err(|e| EngineError::Query(format!("ftp: read temp {}: {}", temp.display(), e)))?;
            let total = bytes.len() as u64;
            let mut ftp = FtpStream::connect(&addr)
                .map_err(|e| EngineError::Query(format!("ftp connect {}: {}", addr, e)))?;
            if spec.secure {
                return Err(EngineError::Config(
                    "snk.ftp: secure=true (FTPS) requires the rustls TLS wrapper which isn't wired up yet. Use plain FTP or wait for the FTPS-explicit feature.".into(),
                ));
            }
            ftp.login(&spec.user, &spec.password)
                .map_err(|e| EngineError::Query(format!("ftp login: {}", e)))?;
            let mut reader = std::io::Cursor::new(bytes);
            ftp.put_file(&spec.remote_path, &mut reader)
                .map_err(|e| EngineError::Query(format!("ftp put {}: {}", spec.remote_path, e)))?;
            let _ = ftp.quit();
            Ok(total)
        })();
        let _ = std::fs::remove_file(&temp);
        let total = upload?;
        Ok(format!(
            "ftp: uploaded {} bytes to {}:{}/{}",
            total, spec.host, spec.port, spec.remote_path
        ))
    }

    /// snk.ftp (SFTP): COPY the upstream view to a local temp file in `format`,
    /// connect over SSH (host-key verified against an optional SHA256
    /// fingerprint pin), authenticate (private key or password), then upload
    /// the file to `remote_path` via SftpSession::create + write_all. Removes
    /// the temp file afterwards. Connect/auth mirror run_sftp_source.
    pub(crate) fn run_sftp_sink(&self, db: &Path, spec: &SftpSinkSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;

        // Host-key verification. With a pinned fingerprint, refuse any other
        // server key; without one, accept on trust (trust-on-first-use).
        struct Verifier {
            expected: Option<String>,
        }
        impl russh::client::Handler for Verifier {
            type Error = russh::Error;
            async fn check_server_key(
                &mut self,
                server_public_key: &russh::keys::ssh_key::PublicKey,
            ) -> Result<bool, Self::Error> {
                match &self.expected {
                    None => Ok(true),
                    Some(want) => {
                        let got = server_public_key
                            .fingerprint(russh::keys::HashAlg::Sha256)
                            .to_string();
                        let norm = |s: &str| s.trim().trim_start_matches("SHA256:").to_string();
                        Ok(norm(&got) == norm(want))
                    }
                }
            }
        }

        let temp = self.ftp_copy_view_to_temp(db, &spec.from_view, &spec.format)?;
        let result: Result<u64, EngineError> = (|| {
            let bytes = std::fs::read(&temp).map_err(|e| {
                EngineError::Query(format!("sftp: read temp {}: {}", temp.display(), e))
            })?;
            let total = bytes.len() as u64;

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| EngineError::Query(format!("sftp: tokio rt: {}", e)))?;

            let uploaded: Result<(), String> = rt.block_on(async {
                use russh_sftp::client::SftpSession;
                use tokio::io::AsyncWriteExt;

                let config = std::sync::Arc::new(russh::client::Config::default());
                let handler = Verifier {
                    expected: spec.host_fingerprint.clone(),
                };
                let mut session =
                    russh::client::connect(config, (spec.host.as_str(), spec.port), handler)
                        .await
                        .map_err(|e| format!("connect {}:{}: {}", spec.host, spec.port, e))?;

                let authed = if let Some(pem) = &spec.private_key {
                    let key = russh::keys::decode_secret_key(pem, spec.key_passphrase.as_deref())
                        .map_err(|e| format!("private key: {}", e))?;
                    let with_alg = russh::keys::PrivateKeyWithHashAlg::new(
                        std::sync::Arc::new(key),
                        Some(russh::keys::HashAlg::Sha256),
                    );
                    session
                        .authenticate_publickey(spec.user.as_str(), with_alg)
                        .await
                        .map_err(|e| format!("publickey auth: {}", e))?
                        .success()
                } else if let Some(pw) = &spec.password {
                    session
                        .authenticate_password(spec.user.as_str(), pw)
                        .await
                        .map_err(|e| format!("password auth: {}", e))?
                        .success()
                } else {
                    return Err("no credentials: set a password or a private key".into());
                };
                if !authed {
                    return Err(format!(
                        "authentication failed for user '{}' (check credentials / host fingerprint)",
                        spec.user
                    ));
                }

                let channel = session
                    .channel_open_session()
                    .await
                    .map_err(|e| format!("open channel: {}", e))?;
                channel
                    .request_subsystem(true, "sftp")
                    .await
                    .map_err(|e| format!("request sftp subsystem: {}", e))?;
                let sftp = SftpSession::new(channel.into_stream())
                    .await
                    .map_err(|e| format!("sftp session: {}", e))?;

                let mut remote = sftp
                    .create(spec.remote_path.clone())
                    .await
                    .map_err(|e| format!("create {}: {}", spec.remote_path, e))?;
                remote
                    .write_all(&bytes)
                    .await
                    .map_err(|e| format!("write {}: {}", spec.remote_path, e))?;
                remote
                    .shutdown()
                    .await
                    .map_err(|e| format!("close {}: {}", spec.remote_path, e))?;
                Ok(())
            });
            uploaded.map_err(EngineError::Query)?;
            Ok(total)
        })();
        let _ = std::fs::remove_file(&temp);
        let total = result?;
        Ok(format!(
            "sftp: uploaded {} bytes to {}:{}/{}",
            total, spec.host, spec.port, spec.remote_path
        ))
    }

    /// xf.ai.embed: per-row embedding via an OpenAI-compatible API.
    /// Reads the upstream view, batches rows into groups of
    /// batch_size, sends the input_column text array to /v1/embeddings,
    /// zips the returned vectors back into the rows under
    /// output_column. Works with OpenAI, Cohere (via baseUrl override),
    /// Voyage, llama.cpp's embedding server, or any other
    /// OpenAI-shaped endpoint.
    ///
    /// Establishes the AI credential pattern the other xf.ai.* tiles
    /// will follow: apiKey lives in stage props for now (revisable
    /// later if we add a secure keystore - just rewires this one read).
    pub(crate) fn run_ai_embed(&self, db: &Path, spec: &AiEmbedSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &[])?;
            return Ok(format!(
                "ai.embed: 0 upstream rows -> {}",
                spec.node_id
            ));
        }
        let endpoint = format!("{}/v1/embeddings", spec.base_url.trim_end_matches('/'));
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            // Pull the text from each row; missing / non-string values
            // become empty strings so the API call doesn't fail on a
            // single bad row.
            let inputs: Vec<String> = chunk
                .iter()
                .map(|row| {
                    row.get(&spec.input_column)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                })
                .collect();
            let body = serde_json::json!({
                "model": spec.model,
                "input": inputs,
            });
            let resp = crate::tls::http_agent().post(&endpoint)
                .set("Authorization", &format!("Bearer {}", spec.api_key))
                .set("Content-Type", "application/json")
                .send_string(&body.to_string());
            let response: JsonValue = match resp {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("ai.embed parse: {}", e)))?,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "ai.embed HTTP {}: {}",
                        code, body
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "ai.embed transport: {}",
                        e
                    )))
                }
            };
            // OpenAI shape: response.data is an array of {index, embedding: [...]}.
            // Order is guaranteed to match the input order per the API contract.
            let data = response
                .get("data")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if data.len() != chunk.len() {
                return Err(EngineError::Query(format!(
                    "ai.embed: expected {} embeddings, got {}",
                    chunk.len(),
                    data.len()
                )));
            }
            for (row, item) in chunk.iter().zip(data.iter()) {
                let embedding = item.get("embedding").cloned().unwrap_or(JsonValue::Null);
                let mut obj = match row {
                    JsonValue::Object(m) => m.clone(),
                    _ => serde_json::Map::new(),
                };
                obj.insert(spec.output_column.clone(), embedding);
                out.push(JsonValue::Object(obj));
            }
        }
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.embed ({}): embedded {} row(s) into {}",
            spec.model, count, spec.node_id
        ))
    }

    /// src.kinesis: single-shard read via direct HTTP + AWS SigV4
    /// (reuses the helper shipped with src.dynamodb). 3-step protocol
    /// per AWS Kinesis API:
    ///   1. ListShards -> get shard IDs
    ///   2. GetShardIterator -> get a starting iterator
    ///   3. GetRecords loop -> consume up to max_records
    /// Each record's Data field is base64-encoded; if the decoded
    /// payload is a JSON object the object is the row, otherwise we
    /// fall back to {partition_key, sequence_number, data}.
    pub(crate) fn run_kinesis_source(
        &self,
        db: &Path,
        spec: &KinesisSourceSpec,
    ) -> Result<String, EngineError> {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine as _;
        self.check_cancelled()?;
        let host = format!("kinesis.{}.amazonaws.com", spec.region);
        let endpoint = format!("https://{}/", host);
        // Helper: sign + post a Kinesis JSON request, return parsed response.
        let call = |target: &str, body: &serde_json::Value| -> Result<JsonValue, EngineError> {
            let body_str = body.to_string();
            let now = chrono::Utc::now();
            let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();
            let date = now.format("%Y%m%d").to_string();
            let signed = aws_sigv4_sign(
                "POST",
                "/",
                "",
                &host,
                &datetime,
                &date,
                "kinesis",
                &spec.region,
                target,
                &body_str,
                &spec.access_key_id,
                &spec.secret_access_key,
                spec.session_token.as_deref(),
            );
            let mut req = crate::tls::http_agent().post(&endpoint)
                .set("Host", &host)
                .set("Content-Type", "application/x-amz-json-1.0")
                .set("X-Amz-Date", &datetime)
                .set("X-Amz-Target", target)
                .set("Authorization", &signed.authorization);
            if let Some(tok) = &spec.session_token {
                req = req.set("X-Amz-Security-Token", tok);
            }
            match req.send_string(&body_str) {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("kinesis parse: {}", e))),
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    Err(EngineError::Query(format!(
                        "kinesis HTTP {} {}: {}",
                        code, target, b
                    )))
                }
                Err(e) => Err(EngineError::Query(format!("kinesis transport: {}", e))),
            }
        };
        // 1. ListShards
        let shards_resp = call(
            "Kinesis_20131202.ListShards",
            &serde_json::json!({"StreamName": spec.stream_name}),
        )?;
        let shards = shards_resp
            .get("Shards")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let shard_id = shards
            .get(spec.shard_index)
            .and_then(|s| s.get("ShardId"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                EngineError::Query(format!(
                    "kinesis: no shard at index {} (got {} shards)",
                    spec.shard_index,
                    shards.len()
                ))
            })?;
        // 2. GetShardIterator
        let iter_resp = call(
            "Kinesis_20131202.GetShardIterator",
            &serde_json::json!({
                "StreamName": spec.stream_name,
                "ShardId": shard_id,
                "ShardIteratorType": spec.iterator_type,
            }),
        )?;
        let mut shard_iter = iter_resp
            .get("ShardIterator")
            .and_then(|v| v.as_str())
            .ok_or_else(|| EngineError::Query("kinesis: no ShardIterator returned".into()))?
            .to_string();
        // 3. GetRecords loop.
        let mut out: Vec<JsonValue> = Vec::new();
        let mut polls = 0;
        while (out.len() as u64) < spec.max_records && polls < 100 {
            self.check_cancelled()?;
            let remaining = (spec.max_records - out.len() as u64).min(10000);
            let rec_resp = call(
                "Kinesis_20131202.GetRecords",
                &serde_json::json!({
                    "ShardIterator": shard_iter,
                    "Limit": remaining,
                }),
            )?;
            let records = rec_resp
                .get("Records")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let got = records.len();
            for r in records {
                if (out.len() as u64) >= spec.max_records {
                    break;
                }
                let data_b64 = r.get("Data").and_then(|v| v.as_str()).unwrap_or("");
                let partition_key = r
                    .get("PartitionKey")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let sequence_number = r
                    .get("SequenceNumber")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let decoded = B64.decode(data_b64).unwrap_or_default();
                let decoded_str = String::from_utf8_lossy(&decoded).into_owned();
                // If JSON object, that IS the row; otherwise fallback row.
                match serde_json::from_str::<JsonValue>(&decoded_str) {
                    Ok(JsonValue::Object(o)) => out.push(JsonValue::Object(o)),
                    _ => {
                        let mut row = serde_json::Map::new();
                        row.insert("partition_key".into(), JsonValue::String(partition_key));
                        row.insert(
                            "sequence_number".into(),
                            JsonValue::String(sequence_number),
                        );
                        row.insert("data".into(), JsonValue::String(decoded_str));
                        out.push(JsonValue::Object(row));
                    }
                }
            }
            // Advance iterator. If response gives a NextShardIterator,
            // we follow it; otherwise we're done.
            match rec_resp.get("NextShardIterator").and_then(|v| v.as_str()) {
                Some(next) => shard_iter = next.to_string(),
                None => break,
            }
            // If this poll returned nothing and we're at the tip,
            // stop - don't busy-loop on an empty stream.
            if got == 0 {
                break;
            }
            polls += 1;
        }
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        Ok(format!(
            "kinesis: read {} record(s) from {}/shard[{}] -> {}",
            count, spec.stream_name, spec.shard_index, spec.node_id
        ))
    }

    /// src.dynamodb: scan a DynamoDB table via direct HTTP + AWS
    /// SigV4 signing. Pure-Rust dependency (avoids the 300-service
    /// aws-sdk-rust tree). DynamoDB's typed-attribute response shape
    /// ({"S": "x"}, {"N": "5"}, {"BOOL": true}, ...) gets unwrapped
    /// into plain JSON before each row is emitted. Pagination
    /// follows LastEvaluatedKey across up to max_pages requests.
    pub(crate) fn run_dynamodb_source(
        &self,
        db: &Path,
        spec: &DynamoDbSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let host = format!("dynamodb.{}.amazonaws.com", spec.region);
        let endpoint = format!("https://{}/", host);
        let mut all_rows: Vec<JsonValue> = Vec::new();
        let mut last_key: Option<JsonValue> = None;
        let mut pages = 0u64;
        loop {
            self.check_cancelled()?;
            if pages >= spec.max_pages {
                break;
            }
            // Build request body.
            let mut body = serde_json::Map::new();
            body.insert(
                "TableName".into(),
                JsonValue::String(spec.table_name.clone()),
            );
            body.insert("Limit".into(), JsonValue::from(spec.limit_per_page as i64));
            if let Some(lk) = &last_key {
                body.insert("ExclusiveStartKey".into(), lk.clone());
            }
            let body_str = serde_json::Value::Object(body).to_string();
            // Sign with SigV4 + send.
            let now = chrono::Utc::now();
            let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();
            let date = now.format("%Y%m%d").to_string();
            let signed_headers = aws_sigv4_sign(
                "POST",
                "/",
                "",
                &host,
                &datetime,
                &date,
                "dynamodb",
                &spec.region,
                "DynamoDB_20120810.Scan",
                &body_str,
                &spec.access_key_id,
                &spec.secret_access_key,
                spec.session_token.as_deref(),
            );
            let mut req = crate::tls::http_agent().post(&endpoint)
                .set("Host", &host)
                .set("Content-Type", "application/x-amz-json-1.0")
                .set("X-Amz-Date", &datetime)
                .set("X-Amz-Target", "DynamoDB_20120810.Scan")
                .set("Authorization", &signed_headers.authorization);
            if let Some(tok) = &spec.session_token {
                req = req.set("X-Amz-Security-Token", tok);
            }
            let resp = req.send_string(&body_str);
            let response: JsonValue = match resp {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("dynamodb parse: {}", e)))?,
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "dynamodb HTTP {}: {}",
                        code, b
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("dynamodb transport: {}", e)))
                }
            };
            // Items: array of {col: {S: "x"}, col2: {N: "5"}, ...}
            let items = response
                .get("Items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            for item in items {
                all_rows.push(unwrap_dynamodb_attrs(&item));
            }
            // Pagination: stop when no LastEvaluatedKey returned.
            last_key = response.get("LastEvaluatedKey").cloned();
            pages += 1;
            if last_key.is_none() {
                break;
            }
        }
        // A surviving LastEvaluatedKey means the scan stopped on the page
        // cap with more rows still to read - fail loud, don't silently
        // materialize a partial scan.
        if last_key.is_some() {
            return Err(pagination_capped_err(
                "dynamodb",
                all_rows.len(),
                spec.max_pages,
            ));
        }
        let count = all_rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &all_rows)?;
        Ok(format!(
            "dynamodb: scanned {} row(s) from {} ({} page(s)) -> {}",
            count, spec.table_name, pages, spec.node_id
        ))
    }

    /// snk.email: per-row SMTP send via lettre. For each upstream
    /// row, build an email from {to_column, subject_column,
    /// body_column}, send via SMTPS on `port` to `host`. Optional
    /// credentials (host doesn't always require auth for relay).
    pub(crate) fn run_email_sink(&self, db: &Path, spec: &EmailSinkSpec) -> Result<String, EngineError> {
        use lettre::message::{header, Message};
        use lettre::transport::smtp::authentication::Credentials;
        use lettre::{SmtpTransport, Transport};
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            return Ok(format!("email sink: 0 upstream rows"));
        }
        // Build the SMTP transport once per stage.
        let mut builder = SmtpTransport::relay(&spec.host)
            .map_err(|e| EngineError::Query(format!("smtp relay setup: {}", e)))?
            .port(spec.port);
        if !spec.user.is_empty() {
            builder = builder.credentials(Credentials::new(
                spec.user.clone(),
                spec.password.clone(),
            ));
        }
        let mailer = builder.build();
        let from_parsed: lettre::message::Mailbox = spec
            .from_address
            .parse()
            .map_err(|e| EngineError::Query(format!("from address: {}", e)))?;
        let mut sent = 0usize;
        for row in rows.iter() {
            self.check_cancelled()?;
            let to_str = row
                .get(&spec.to_column)
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    EngineError::Query(format!(
                        "snk.email: row missing `{}` column",
                        spec.to_column
                    ))
                })?;
            let subject_str = row
                .get(&spec.subject_column)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let body_str = row
                .get(&spec.body_column)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let to_parsed: lettre::message::Mailbox = to_str
                .parse()
                .map_err(|e| EngineError::Query(format!("to address `{}`: {}", to_str, e)))?;
            let msg = Message::builder()
                .from(from_parsed.clone())
                .to(to_parsed)
                .subject(subject_str)
                .header(header::ContentType::TEXT_PLAIN)
                .body(body_str.to_string())
                .map_err(|e| EngineError::Query(format!("snk.email build: {}", e)))?;
            mailer
                .send(&msg)
                .map_err(|e| EngineError::Query(format!("snk.email send: {}", e)))?;
            sent += 1;
        }
        Ok(format!(
            "email sink: sent {} message(s) via {}:{}",
            sent, spec.host, spec.port
        ))
    }

    /// src.webhook: bind 127.0.0.1:port, collect up to max_requests
    /// inbound HTTP requests with a global timeout deadline, close
    /// the listener. Each request body becomes a row: if the body
    /// parses as JSON object, the object is the row; if it parses
    /// as a JSON array, each element becomes a row; otherwise a
    /// fallback row {method, path, body} captures the raw request.
    pub(crate) fn run_webhook_source(
        &self,
        db: &Path,
        spec: &WebhookSourceSpec,
    ) -> Result<String, EngineError> {
        use std::io::Write;
        use std::net::TcpListener;
        use std::time::{Duration, Instant};
        self.check_cancelled()?;
        let addr = format!("127.0.0.1:{}", spec.port);
        let listener = TcpListener::bind(&addr)
            .map_err(|e| EngineError::Query(format!("webhook bind {}: {}", addr, e)))?;
        // Non-blocking so we can poll cancel + global deadline.
        listener
            .set_nonblocking(true)
            .map_err(|e| EngineError::Query(format!("webhook set_nonblocking: {}", e)))?;
        let deadline = Instant::now() + Duration::from_millis(spec.timeout_ms);
        let mut rows: Vec<JsonValue> = Vec::new();
        // Accepted connections whose 200 is deferred until the batch is
        // durably written (persist-then-ack), so a materialize failure can't
        // leave senders thinking a never-stored event was delivered.
        let mut pending: Vec<std::net::TcpStream> = Vec::new();
        while (rows.len() as u64) < spec.max_requests {
            self.check_cancelled()?;
            if Instant::now() >= deadline {
                break;
            }
            let (mut stream, _addr) = match listener.accept() {
                Ok(s) => s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("webhook accept: {}", e)));
                }
            };
            // The listener is non-blocking so we can poll cancel/deadline, but
            // on macOS/BSD the accepted socket inherits O_NONBLOCK. A read could
            // then hit WouldBlock before the request bytes arrive and the
            // request would be dropped as malformed. Put the accepted stream
            // back into blocking mode so the read timeout below governs it.
            stream.set_nonblocking(false).ok();
            stream
                .set_read_timeout(Some(Duration::from_millis(1000)))
                .ok();
            // Read request bytes until headers parse + body fully consumed.
            let (method, path, headers, body) = match read_http_request(&mut stream) {
                Ok(req) => req,
                Err(e) => {
                    let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    let _ = stream.flush();
                    eprintln!("webhook: skipping malformed request: {}", e);
                    continue;
                }
            };
            // Path filter: 404 anything that doesn't match.
            if let Some(prefix) = &spec.path_filter {
                if !path.starts_with(prefix) {
                    let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    let _ = stream.flush();
                    continue;
                }
            }
            // Parse the body: prefer JSON shape, fall back to raw.
            let body_str = String::from_utf8_lossy(&body).into_owned();
            match serde_json::from_str::<JsonValue>(&body_str) {
                Ok(JsonValue::Object(o)) => rows.push(JsonValue::Object(o)),
                Ok(JsonValue::Array(arr)) => {
                    for v in arr {
                        // Every materialized line must be an object; wrap a
                        // bare scalar/array element so it round-trips as a row
                        // instead of a malformed bare value.
                        if v.is_object() {
                            rows.push(v);
                        } else {
                            let mut m = serde_json::Map::new();
                            m.insert("value".into(), v);
                            rows.push(JsonValue::Object(m));
                        }
                    }
                }
                _ => {
                    let mut row = serde_json::Map::new();
                    row.insert("method".into(), JsonValue::String(method));
                    row.insert("path".into(), JsonValue::String(path));
                    row.insert("body".into(), JsonValue::String(body_str));
                    let mut hdrs = serde_json::Map::new();
                    for (k, v) in headers {
                        hdrs.insert(k, JsonValue::String(v));
                    }
                    row.insert("headers".into(), JsonValue::Object(hdrs));
                    rows.push(JsonValue::Object(row));
                }
            }
            // Hold the connection open; answer it after the batch is persisted.
            pending.push(stream);
        }
        let count = rows.len();
        let materialized = materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows);
        // Persist-then-ack: 200 once the rows are durably written; 503 on
        // failure so a well-behaved sender retries instead of dropping the
        // event. A sender that already timed out waiting will also retry,
        // which is the safe (at-least-once) direction.
        let response: &[u8] = if materialized.is_ok() {
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
        } else {
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 5\r\nConnection: close\r\n\r\nretry"
        };
        for mut s in pending {
            let _ = s.write_all(response);
            let _ = s.flush();
        }
        materialized?;
        Ok(format!(
            "webhook: collected {} request(s) on :{} -> {}",
            count, spec.port, spec.node_id
        ))
    }

    /// src.email: connect to an IMAP server via rustls, select a
    /// mailbox, fetch up to max_messages most recent messages by
    /// reverse-UID order, parse with mail-parser, emit one row per
    /// message with {uid, from, to, subject, date, body_text}.
    ///
    /// Basic auth only - OAuth (gmail / o365) is a follow-up that
    /// needs the same model-API-credential pattern xf.ai.embed
    /// established, plus a token-refresh worker.
    pub(crate) fn run_email_source(
        &self,
        db: &Path,
        spec: &EmailSourceSpec,
    ) -> Result<String, EngineError> {
        use imap::ClientBuilder;
        use mail_parser::MessageParser;
        self.check_cancelled()?;
        let client = ClientBuilder::new(&spec.host, spec.port)
            .connect()
            .map_err(|e| EngineError::Query(format!("imap connect: {}", e)))?;
        let mut session = client
            .login(&spec.user, &spec.password)
            .map_err(|(e, _)| EngineError::Query(format!("imap login: {}", e)))?;
        let mailbox = session
            .select(&spec.mailbox)
            .map_err(|e| EngineError::Query(format!("imap select {}: {}", spec.mailbox, e)))?;
        let total = mailbox.exists as u64;
        if total == 0 {
            let _ = session.logout();
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &[])?;
            return Ok(format!(
                "email: 0 messages in {} -> {}",
                spec.mailbox, spec.node_id
            ));
        }
        // Fetch the last N messages (by sequence). seqset is 1-based.
        let from = total.saturating_sub(spec.max_messages.saturating_sub(1)).max(1);
        let seqset = format!("{}:{}", from, total);
        let messages = session
            .fetch(&seqset, "(UID BODY[])")
            .map_err(|e| EngineError::Query(format!("imap fetch: {}", e)))?;
        let parser = MessageParser::default();
        let mut rows: Vec<JsonValue> = Vec::new();
        for fetch in messages.iter() {
            self.check_cancelled()?;
            let uid = fetch.uid.map(|u| u as i64).unwrap_or(0);
            let body = fetch.body().unwrap_or_default();
            let parsed = parser
                .parse(body)
                .ok_or_else(|| EngineError::Query("email parse failed".into()))?;
            let from = parsed
                .from()
                .map(|addrs| {
                    addrs
                        .iter()
                        .filter_map(|a| a.address())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let to = parsed
                .to()
                .map(|addrs| {
                    addrs
                        .iter()
                        .filter_map(|a| a.address())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            let subject = parsed.subject().unwrap_or("").to_string();
            let date = parsed.date().map(|d| d.to_rfc3339()).unwrap_or_default();
            let body_text = parsed.body_text(0).map(|s| s.into_owned()).unwrap_or_default();
            let mut row = serde_json::Map::new();
            row.insert("uid".into(), JsonValue::from(uid));
            row.insert("from".into(), JsonValue::String(from));
            row.insert("to".into(), JsonValue::String(to));
            row.insert("subject".into(), JsonValue::String(subject));
            row.insert("date".into(), JsonValue::String(date));
            row.insert("body_text".into(), JsonValue::String(body_text));
            rows.push(JsonValue::Object(row));
        }
        let _ = session.logout();
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "email: materialized {} message(s) from {}@{}:{}/{} into {}",
            count, spec.user, spec.host, spec.port, spec.mailbox, spec.node_id
        ))
    }

    /// code.javascript: per-row JS transform via boa_engine. The
    /// user's script is evaluated once to define a `transform`
    /// function, then transform(row) runs per row. Row goes in as a
    /// JS object (marshalled from JSON), transformed row comes back
    /// as a JS object and is converted back. Boa is sandboxed - no
    /// fs, no fetch, no DOM, no setTimeout.
    pub(crate) fn run_javascript(
        &self,
        db: &Path,
        spec: &JavaScriptSpec,
    ) -> Result<String, EngineError> {
        use boa_engine::{js_string, Context, Source};
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &[])?;
            return Ok(format!(
                "code.javascript: 0 upstream rows -> {}",
                spec.node_id
            ));
        }
        // One context per stage - state is intentionally not shared
        // across stages, but IS shared across rows within a stage so
        // the user can declare helpers once at the top of the script.
        let mut ctx = Context::default();
        ctx.eval(Source::from_bytes(spec.script.as_bytes()))
            .map_err(|e| EngineError::Query(format!("js: script eval: {}", e)))?;
        let transform = ctx
            .global_object()
            .get(js_string!("transform"), &mut ctx)
            .map_err(|e| EngineError::Query(format!("js: lookup transform: {}", e)))?;
        if !transform.is_callable() {
            return Err(EngineError::Query(
                "js: script must define a global `transform` function".into(),
            ));
        }
        // BigInt-preserving marshalling. boa's JsValue::from_json/to_json clamp
        // integers to i32 and demote the rest to f64, so a 64-bit id (e.g. a
        // Snowflake key) is silently corrupted even by an identity `return row`.
        // Instead we marshal through JS's own JSON.parse/stringify with a marker:
        // integers outside i32 range are tagged so JS parses them as BigInt and
        // serializes them back exactly; the rest is ordinary JSON.
        const BI_MARK: &str = "\u{0}BI\u{0}";
        fn mark_bigints(v: &JsonValue) -> JsonValue {
            match v {
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        if !(i32::MIN as i64..=i32::MAX as i64).contains(&i) {
                            return JsonValue::String(format!("{}{}", BI_MARK, i));
                        }
                    } else if let Some(u) = n.as_u64() {
                        return JsonValue::String(format!("{}{}", BI_MARK, u));
                    }
                    v.clone()
                }
                JsonValue::Array(a) => JsonValue::Array(a.iter().map(mark_bigints).collect()),
                JsonValue::Object(m) => {
                    JsonValue::Object(m.iter().map(|(k, val)| (k.clone(), mark_bigints(val))).collect())
                }
                _ => v.clone(),
            }
        }
        fn unmark_bigints(v: JsonValue) -> JsonValue {
            match v {
                JsonValue::String(s) if s.starts_with(BI_MARK) => s[BI_MARK.len()..]
                    .parse::<serde_json::Number>()
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::String(s)),
                JsonValue::Array(a) => JsonValue::Array(a.into_iter().map(unmark_bigints).collect()),
                JsonValue::Object(m) => {
                    JsonValue::Object(m.into_iter().map(|(k, val)| (k, unmark_bigints(val))).collect())
                }
                other => other,
            }
        }
        ctx.eval(Source::from_bytes(
            "globalThis.__duckle_M='\\u0000BI\\u0000';\
             globalThis.__duckle_parse=function(s){return JSON.parse(s,function(k,v){return (typeof v==='string'&&v.indexOf(globalThis.__duckle_M)===0)?BigInt(v.slice(globalThis.__duckle_M.length)):v;});};\
             globalThis.__duckle_ser=function(v){return JSON.stringify(v,function(k,val){return (typeof val==='bigint')?(globalThis.__duckle_M+val.toString()):val;});};",
        ))
        .map_err(|e| EngineError::Query(format!("js: marshaller setup: {}", e)))?;
        let parse_fn = ctx
            .global_object()
            .get(js_string!("__duckle_parse"), &mut ctx)
            .map_err(|e| EngineError::Query(format!("js: parse fn: {}", e)))?;
        let ser_fn = ctx
            .global_object()
            .get(js_string!("__duckle_ser"), &mut ctx)
            .map_err(|e| EngineError::Query(format!("js: ser fn: {}", e)))?;

        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            // JSON -> JsValue: mark large ints, let JS parse them as BigInt.
            let s = serde_json::to_string(&mark_bigints(row)).unwrap_or_else(|_| "null".to_string());
            let js_in = parse_fn
                .as_callable()
                .ok_or_else(|| EngineError::Query("js: marshaller missing".into()))?
                .call(
                    &boa_engine::JsValue::Undefined,
                    &[boa_engine::JsValue::from(js_string!(s.as_str()))],
                    &mut ctx,
                )
                .map_err(|e| EngineError::Query(format!("js: row -> JsValue: {}", e)))?;
            let result = transform
                .as_callable()
                .ok_or_else(|| EngineError::Query("js: transform not callable".into()))?
                .call(&boa_engine::JsValue::Undefined, &[js_in], &mut ctx)
                .map_err(|e| EngineError::Query(format!("js: transform call: {}", e)))?;
            // Guard the value's shape BEFORE serializing: a transform that
            // returns nothing (undefined) or null is a programming error.
            if result.is_undefined() || result.is_null() {
                return Err(EngineError::Query(format!(
                    "js: transform must return an object, got {} (did the function return a value?)",
                    if result.is_undefined() { "undefined" } else { "null" }
                )));
            }
            // JsValue -> JSON: stringify in JS (BigInt -> marker), un-mark here.
            let ser = ser_fn
                .as_callable()
                .ok_or_else(|| EngineError::Query("js: marshaller missing".into()))?
                .call(&boa_engine::JsValue::Undefined, &[result], &mut ctx)
                .map_err(|e| EngineError::Query(format!("js: result -> JSON: {}", e)))?;
            let json_out = match ser.as_string() {
                Some(js) => {
                    let text = js.to_std_string_escaped();
                    let parsed: JsonValue = serde_json::from_str(&text)
                        .map_err(|e| EngineError::Query(format!("js: result -> JSON: {}", e)))?;
                    unmark_bigints(parsed)
                }
                None => {
                    return Err(EngineError::Query(
                        "js: transform must return an object".into(),
                    ))
                }
            };
            if !json_out.is_object() {
                return Err(EngineError::Query(format!(
                    "js: transform must return an object, got: {}",
                    json_out
                )));
            }
            out.push(json_out);
        }
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        Ok(format!(
            "code.javascript: transformed {} row(s) into {}",
            count, spec.node_id
        ))
    }

    /// xf.ai.dedupe: drop rows whose embedding is within `threshold`
    /// cosine similarity of a previously-kept row. Reads the
    /// embedding column as a list of floats from each row. No API
    /// call - pure local math. O(N^2) per stage, so the input is
    /// capped at AI_DEDUPE_MAX_ROWS and exceeding it fails loud.
    pub(crate) fn run_ai_dedupe(&self, db: &Path, spec: &AiDedupeSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.len() > AI_DEDUPE_MAX_ROWS {
            return Err(EngineError::Config(format!(
                "ai.dedupe compares every row against all kept rows (O(N^2)); {} input rows \
                 exceeds the {} row limit. Pre-filter or aggregate upstream, or split the \
                 input before semantic dedupe.",
                rows.len(),
                AI_DEDUPE_MAX_ROWS
            )));
        }
        let mut kept: Vec<JsonValue> = Vec::new();
        let mut kept_embeddings: Vec<Vec<f64>> = Vec::new();
        for row in rows.iter() {
            self.check_cancelled()?;
            let raw = row.get(&spec.embedding_column);
            // Accept either a JSON array directly (when read via
            // read_json_auto) OR a stringified JSON array (when the
            // upstream came through a CSV round-trip - DuckDB keeps
            // list literals as strings in CSV).
            let emb: Option<Vec<f64>> = raw.and_then(|v| match v {
                JsonValue::Array(arr) => Some(
                    arr.iter().filter_map(|x| x.as_f64()).collect::<Vec<_>>(),
                ),
                JsonValue::String(s) => serde_json::from_str::<JsonValue>(s)
                    .ok()
                    .and_then(|j| j.as_array().cloned())
                    .map(|arr| arr.iter().filter_map(|x| x.as_f64()).collect::<Vec<_>>()),
                _ => None,
            });
            let Some(e) = emb else {
                // Missing/invalid embedding - keep the row (don't
                // silently drop data the user might want).
                kept.push(row.clone());
                kept_embeddings.push(Vec::new());
                continue;
            };
            // Drop if any previously-kept embedding is within threshold.
            let is_dup = kept_embeddings
                .iter()
                .filter(|p| !p.is_empty())
                .any(|p| cosine_similarity(p, &e) >= spec.threshold);
            if !is_dup {
                kept.push(row.clone());
                kept_embeddings.push(e);
            }
        }
        let count = kept.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &kept)?;
        Ok(format!(
            "ai.dedupe: {} -> {} row(s) (threshold {}) into {}",
            rows.len(),
            count,
            spec.threshold,
            spec.node_id
        ))
    }

    /// xf.ai.classify: per-row LLM-backed classifier. Builds a
    /// constrained prompt asking the model to choose exactly one of
    /// the user-supplied categories. Result that's not in the list
    /// gets normalized to "UNKNOWN" so downstream filters don't break.
    pub(crate) fn run_ai_classify(
        &self,
        db: &Path,
        spec: &AiClassifySpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &[])?;
            return Ok(format!("ai.classify: 0 upstream rows -> {}", spec.node_id));
        }
        let endpoint = format!("{}/v1/chat/completions", spec.base_url.trim_end_matches('/'));
        let cat_list = spec.categories.join(", ");
        let system_prompt = format!(
            "You are a strict classifier. Pick exactly one of these categories: {}. \
             Reply with only the category name and nothing else.",
            cat_list
        );
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            let text = row
                .get(&spec.input_column)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let body = serde_json::json!({
                "model": spec.model,
                "temperature": 0.0,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text},
                ],
            });
            let resp = crate::tls::http_agent().post(&endpoint)
                .set("Authorization", &format!("Bearer {}", spec.api_key))
                .set("Content-Type", "application/json")
                .send_string(&body.to_string());
            let response: JsonValue = match resp {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("ai.classify parse: {}", e)))?,
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!("ai.classify HTTP {}: {}", code, b)));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("ai.classify transport: {}", e)))
                }
            };
            let raw = response
                .pointer("/choices/0/message/content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();
            // Constrain to the supplied category list; anything not
            // in it becomes UNKNOWN so downstream pipelines don't
            // see surprise values.
            let chosen = spec
                .categories
                .iter()
                .find(|c| c.eq_ignore_ascii_case(&raw))
                .cloned()
                .unwrap_or_else(|| "UNKNOWN".into());
            let mut obj = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            obj.insert(spec.output_column.clone(), JsonValue::String(chosen));
            out.push(JsonValue::Object(obj));
        }
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.classify ({}): {} row(s) -> {}",
            spec.model, count, spec.node_id
        ))
    }

    /// xf.ai.llm: per-row LLM call via OpenAI-compatible chat
    /// completions API. Renders prompt_template with {col} subst
    /// from each row; if template is empty, sends the input column
    /// text as-is. Optional system prompt + temperature. Result text
    /// lands in output_column.
    ///
    /// Unlike xf.ai.embed which batches inputs in a single request,
    /// chat completions are one prompt per call - N rows = N HTTP
    /// requests. Users should keep dataset sizes manageable or chain
    /// with xf.rows.head to sample.
    pub(crate) fn run_ai_llm(&self, db: &Path, spec: &AiLlmSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &[])?;
            return Ok(format!("ai.llm: 0 upstream rows -> {}", spec.node_id));
        }
        let endpoint = format!("{}/v1/chat/completions", spec.base_url.trim_end_matches('/'));
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            let user_text = if spec.prompt_template.is_empty() {
                row.get(&spec.input_column)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            } else {
                render_prompt_template(&spec.prompt_template, row)
            };
            let mut messages: Vec<serde_json::Value> = Vec::new();
            if let Some(sys) = &spec.system_prompt {
                messages.push(serde_json::json!({"role": "system", "content": sys}));
            }
            messages.push(serde_json::json!({"role": "user", "content": user_text}));
            let body = serde_json::json!({
                "model": spec.model,
                "messages": messages,
                "temperature": spec.temperature,
            });
            let resp = crate::tls::http_agent().post(&endpoint)
                .set("Authorization", &format!("Bearer {}", spec.api_key))
                .set("Content-Type", "application/json")
                .send_string(&body.to_string());
            let response: JsonValue = match resp {
                Ok(r) => r
                    .into_json()
                    .map_err(|e| EngineError::Query(format!("ai.llm parse: {}", e)))?,
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!("ai.llm HTTP {}: {}", code, b)));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("ai.llm transport: {}", e)))
                }
            };
            let content = response
                .pointer("/choices/0/message/content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mut obj = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            obj.insert(spec.output_column.clone(), JsonValue::String(content));
            out.push(JsonValue::Object(obj));
        }
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.llm ({}): {} row(s) -> {}",
            spec.model, count, spec.node_id
        ))
    }

    /// xf.ai.pii: regex-based PII redaction. For each upstream row,
    /// detect emails / phones / SSNs / credit-card numbers in the
    /// input column and replace each match with `[REDACTED-TYPE]`.
    /// Pure local regex - no API call, no model. LLM-backed redaction
    /// is a follow-up that would share the xf.ai.embed pattern.
    ///
    /// The regex set is intentionally conservative (favor false-
    /// negatives over false-positives) - users with stricter PII
    /// needs should follow up with an LLM-backed pass or NER model.
    pub(crate) fn run_ai_pii(&self, db: &Path, spec: &AiPiiSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        // Compile regex set once per stage (not once per row).
        let patterns = pii_patterns(&spec.types);
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            let text = row
                .get(&spec.input_column)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let redacted = patterns.iter().fold(text, |acc, (re, label)| {
                re.replace_all(&acc, *label).into_owned()
            });
            let mut obj = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            obj.insert(spec.output_column.clone(), JsonValue::String(redacted));
            out.push(JsonValue::Object(obj));
        }
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.pii: redacted {} row(s) into {}",
            count, spec.node_id
        ))
    }

    /// xf.ai.chunk: text splitter for RAG / embedding pipelines.
    /// Splits the `input_column` of each upstream row into chunks of
    /// at most `chunk_size` characters with `chunk_overlap` between
    /// successive chunks. mode="explode" emits one row per chunk
    /// (with chunk_index + chunk_count + the rest of the source row);
    /// mode="array" emits one row per source row with the chunks as
    /// a JSON array in `output_column`.
    pub(crate) fn run_ai_chunk(&self, db: &Path, spec: &AiChunkSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        let mut out: Vec<JsonValue> = Vec::new();
        for row in rows.iter() {
            self.check_cancelled()?;
            let text = row
                .get(&spec.input_column)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let chunks = chunk_text(text, spec.chunk_size, spec.chunk_overlap);
            let base = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            if spec.mode == "array" {
                let mut obj = base;
                obj.insert(
                    spec.output_column.clone(),
                    JsonValue::Array(
                        chunks.into_iter().map(JsonValue::String).collect(),
                    ),
                );
                out.push(JsonValue::Object(obj));
            } else {
                // explode (default)
                let count = chunks.len() as i64;
                for (idx, chunk) in chunks.into_iter().enumerate() {
                    let mut obj = base.clone();
                    obj.insert(
                        spec.output_column.clone(),
                        JsonValue::String(chunk),
                    );
                    obj.insert("chunk_index".into(), JsonValue::from(idx as i64));
                    obj.insert("chunk_count".into(), JsonValue::from(count));
                    out.push(JsonValue::Object(obj));
                }
            }
        }
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        Ok(format!(
            "ai.chunk: split {} upstream row(s) into {} chunk(s) -> {}",
            rows.len(),
            count,
            spec.node_id
        ))
    }

    /// code.wasm: per-row WebAssembly transform via wasmi (interpreter).
    /// For each upstream row, the engine writes the input column text
    /// into the module's linear memory, calls the exported transform
    /// function (i32, i32) -> i64, then reads the (out_ptr, out_len)
    /// pair back from the returned i64 to recover the result string.
    ///
    /// By default each row gets a fresh module instance so state
    /// doesn't leak between rows - safer for user-supplied modules. When
    /// spec.reuse_instance is set the stage instantiates once and reuses
    /// that instance across every row (faster, but linear memory persists
    /// between rows). wasmi is an interpreter so each call has
    /// interpretation overhead; for ETL (rows in the thousands, not
    /// millions per second) it's fine.
    ///
    /// Modules run sandboxed: no host imports, no fs, no network. If
    /// the module's exports don't match the contract we return a
    /// clear EngineError rather than panicking.
    pub(crate) fn run_wasm(&self, db: &Path, spec: &WasmSpec) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &[])?;
            return Ok(format!("wasm: 0 upstream rows -> {}", spec.node_id));
        }
        let engine = wasmi::Engine::default();
        let module = wasmi::Module::new(&engine, &spec.wasm_bytes[..])
            .map_err(|e| EngineError::Query(format!("wasm: parse module: {}", e)))?;
        // Per-stage mode: build one instance up front and reuse it.
        let mut shared = if spec.reuse_instance {
            Some(Self::wasm_new_instance(&engine, &module, &spec.function)?)
        } else {
            None
        };
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            let input_text = row
                .get(&spec.input_column)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let result_text = match shared.as_mut() {
                Some((store, memory, transform)) => {
                    Self::wasm_run_one(store, *memory, *transform, &input_text)?
                }
                None => {
                    let (mut store, memory, transform) =
                        Self::wasm_new_instance(&engine, &module, &spec.function)?;
                    Self::wasm_run_one(&mut store, memory, transform, &input_text)?
                }
            };
            let mut obj = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            obj.insert(
                spec.output_column.clone(),
                JsonValue::String(result_text),
            );
            out.push(JsonValue::Object(obj));
        }
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        Ok(format!(
            "wasm ({}): processed {} row(s) into {}",
            spec.function, count, spec.node_id
        ))
    }

    /// Instantiate the module and resolve its `memory` export plus the
    /// transform function. Memory/TypedFunc are lightweight store-independent
    /// handles (Copy), so the caller can hold them and drive many calls
    /// against the returned store.
    #[allow(clippy::type_complexity)]
    pub(crate) fn wasm_new_instance(
        engine: &wasmi::Engine,
        module: &wasmi::Module,
        function: &str,
    ) -> Result<
        (
            wasmi::Store<()>,
            wasmi::Memory,
            wasmi::TypedFunc<(i32, i32), i64>,
        ),
        EngineError,
    > {
        let mut store = wasmi::Store::new(engine, ());
        let linker = wasmi::Linker::new(engine);
        let instance = linker
            .instantiate(&mut store, module)
            .and_then(|p| p.start(&mut store))
            .map_err(|e| EngineError::Query(format!("wasm: instantiate: {}", e)))?;
        let memory = instance
            .get_memory(&store, "memory")
            .ok_or_else(|| EngineError::Query("wasm: module has no exported `memory`".into()))?;
        let transform = instance
            .get_typed_func::<(i32, i32), i64>(&store, function)
            .map_err(|e| {
                EngineError::Query(format!(
                    "wasm: export `{}(i32, i32) -> i64` not found: {}",
                    function, e
                ))
            })?;
        Ok((store, memory, transform))
    }

    /// Run a single transform invocation against an existing instance.
    /// Returns the output string read back from module memory.
    pub(crate) fn wasm_run_one(
        store: &mut wasmi::Store<()>,
        memory: wasmi::Memory,
        transform: wasmi::TypedFunc<(i32, i32), i64>,
        input: &str,
    ) -> Result<String, EngineError> {
        // Write input at a fixed offset (1024). Modules that want
        // dynamic alloc can ignore this offset and use their own
        // allocator - we still pass our offset as in_ptr.
        let in_ptr: u32 = 1024;
        let in_len: u32 = input.len() as u32;
        memory
            .data_mut(&mut *store)
            .get_mut(in_ptr as usize..(in_ptr as usize + in_len as usize))
            .ok_or_else(|| EngineError::Query("wasm: input doesn't fit in memory".into()))?
            .copy_from_slice(input.as_bytes());
        let packed = transform
            .call(&mut *store, (in_ptr as i32, in_len as i32))
            .map_err(|e| EngineError::Query(format!("wasm: call: {}", e)))?;
        let out_ptr = ((packed >> 32) & 0xFFFFFFFF) as u32;
        let out_len = (packed & 0xFFFFFFFF) as u32;
        let mem_data = memory.data(&*store);
        // Widen to usize before adding: out_ptr/out_len are module-controlled,
        // so `out_ptr + out_len` as u32 would overflow-panic in debug builds.
        let out_end = (out_ptr as usize)
            .checked_add(out_len as usize)
            .ok_or_else(|| EngineError::Query("wasm: out ptr+len overflow".into()))?;
        let out_slice = mem_data
            .get(out_ptr as usize..out_end)
            .ok_or_else(|| {
                EngineError::Query(format!(
                    "wasm: out (ptr={}, len={}) out of memory bounds (mem_size={})",
                    out_ptr,
                    out_len,
                    mem_data.len()
                ))
            })?;
        String::from_utf8(out_slice.to_vec())
            .map_err(|e| EngineError::Query(format!("wasm: output not utf-8: {}", e)))
    }

    /// src.clipboard: read the system clipboard as text. If it parses
    /// as a JSON array-of-objects the array becomes rows directly; if
    /// it parses as a single JSON object that single object becomes
    /// one row; otherwise we emit one row {text, length}. Fails with
    /// a clear EngineError when the display server isn't reachable
    /// (e.g. headless Linux CI) - arboard's Clipboard::new returns
    /// the underlying platform error.
    pub(crate) fn run_clipboard_source(
        &self,
        db: &Path,
        spec: &ClipboardSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let mut cb = arboard::Clipboard::new()
            .map_err(|e| EngineError::Query(format!("clipboard unavailable: {}", e)))?;
        let text = cb
            .get_text()
            .map_err(|e| EngineError::Query(format!("clipboard get_text: {}", e)))?;
        let rows: Vec<JsonValue> = match serde_json::from_str::<JsonValue>(&text) {
            Ok(JsonValue::Array(arr)) if arr.iter().all(|v| v.is_object()) => arr,
            Ok(JsonValue::Object(o)) => vec![JsonValue::Object(o)],
            _ => {
                let mut row = serde_json::Map::new();
                row.insert("text".into(), JsonValue::String(text.clone()));
                row.insert("length".into(), JsonValue::from(text.chars().count() as i64));
                vec![JsonValue::Object(row)]
            }
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "clipboard: materialized {} row(s) into {}",
            count, spec.node_id
        ))
    }

    /// NATS publisher via async-nats. Each upstream row becomes one
    /// NATS message published to `subject` (or to subject + "." +
    /// row[subjectSuffixColumn] for per-row routing). Payload is the
    /// JSON-stringified row.
    pub(crate) fn run_nats_sink(
        &self,
        db: &Path,
        spec: &NatsSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("nats: 0 rows to publish to {}", spec.subject));
        }
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("nats: tokio rt: {}", e)))?;
        let total: Result<usize, String> = rt.block_on(async {
            let client = async_nats::connect(&spec.urls)
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let mut total = 0_usize;
            for chunk in rows.chunks(spec.batch_size) {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                for row in chunk {
                    let payload = serde_json::to_vec(row).unwrap_or_default();
                    let subject = if spec.subject_suffix_column.is_empty() {
                        spec.subject.clone()
                    } else {
                        let suffix = row
                            .get(&spec.subject_suffix_column)
                            .map(|v| match v {
                                JsonValue::String(s) => s.clone(),
                                _ => v.to_string(),
                            })
                            .unwrap_or_default();
                        if suffix.is_empty() {
                            spec.subject.clone()
                        } else {
                            format!("{}.{}", spec.subject, suffix)
                        }
                    };
                    client
                        .publish(subject, payload.into())
                        .await
                        .map_err(|e| format!("publish: {}", e))?;
                }
                total += chunk.len();
            }
            client.flush().await.map_err(|e| format!("flush: {}", e))?;
            Ok(total)
        });
        match total {
            Ok(n) => Ok(format!("nats: published {} message(s) to {}", n, spec.subject)),
            Err(e) if e == "cancelled" => Err(EngineError::Cancelled),
            Err(e) => Err(EngineError::Query(format!("nats sink: {}", e))),
        }
    }

    /// NATS subscribe-with-timeout collector. Drains messages from
    /// `subject` until either max_records is reached or timeout_ms
    /// elapses (wall clock). Emits {subject, payload, headers (json)}
    /// rows. Best-fit for "snapshot a queue" and "drain a topic"
    /// batch patterns; true streaming is a separate engine workstream.
    pub(crate) fn run_nats_source(
        &self,
        db: &Path,
        spec: &NatsSourceSpec,
    ) -> Result<String, EngineError> {
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("nats: tokio rt: {}", e)))?;
        let result: Result<Vec<JsonValue>, String> = rt.block_on(async {
            use futures_util::StreamExt;
            let client = async_nats::connect(&spec.urls)
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let mut sub = client
                .subscribe(spec.subject.clone())
                .await
                .map_err(|e| format!("subscribe: {}", e))?;
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_millis(spec.timeout_ms);
            let mut out: Vec<JsonValue> = Vec::new();
            while (out.len() as u64) < spec.max_records {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let next = tokio::time::timeout(remaining, sub.next()).await;
                match next {
                    Ok(Some(msg)) => {
                        let mut obj = serde_json::Map::new();
                        obj.insert(
                            "subject".into(),
                            JsonValue::String(msg.subject.to_string()),
                        );
                        obj.insert(
                            "payload".into(),
                            JsonValue::String(
                                String::from_utf8_lossy(&msg.payload).to_string(),
                            ),
                        );
                        out.push(JsonValue::Object(obj));
                    }
                    _ => break,
                }
            }
            Ok(out)
        });
        let rows = match result {
            Ok(r) => r,
            Err(e) if e == "cancelled" => return Err(EngineError::Cancelled),
            Err(e) => return Err(EngineError::Query(format!("nats source: {}", e))),
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "nats: materialized {} message(s) into {}",
            count, spec.node_id
        ))
    }

    /// GCP Pub/Sub publish via REST. POST to
    ///   /v1/projects/{project}/topics/{topic}:publish
    /// Body: {messages: [{data: base64, attributes: {}}]}.
    /// Auth: Bearer OAuth2 access token.
    pub(crate) fn run_pubsub_sink(
        &self,
        db: &Path,
        spec: &PubSubSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("pubsub: 0 rows to publish to {}", spec.topic));
        }
        let url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/topics/{}:publish",
            spec.project, spec.topic
        );
        let mut total = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            use base64::Engine as _;
            let messages: Vec<JsonValue> = chunk
                .iter()
                .map(|row| {
                    let json = serde_json::to_vec(row).unwrap_or_default();
                    let data = base64::engine::general_purpose::STANDARD.encode(&json);
                    serde_json::json!({ "data": data })
                })
                .collect();
            let body = serde_json::json!({ "messages": messages });
            let resp = crate::tls::http_agent().post(&url)
                .set("Content-Type", "application/json")
                .set("Authorization", &format!("Bearer {}", spec.access_token))
                .send_string(&serde_json::to_string(&body).unwrap_or_default());
            match resp {
                Ok(_) => total += chunk.len(),
                Err(ureq::Error::Status(code, r)) => {
                    let b = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "pubsub HTTP {} on publish: {}",
                        code,
                        b.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "pubsub transport: {}",
                        e
                    )));
                }
            }
        }
        Ok(format!(
            "pubsub: published {} message(s) to {}",
            total, spec.topic
        ))
    }

    /// GCP Pub/Sub pull + ack via REST. POST to
    ///   /v1/projects/{project}/subscriptions/{sub}:pull
    /// with {maxMessages: N}. Auto-acks the batch via
    ///   /v1/projects/{project}/subscriptions/{sub}:acknowledge
    /// Emits {message_id, publish_time, data} rows where data is
    /// the UTF-8-decoded message payload.
    pub(crate) fn run_pubsub_source(
        &self,
        db: &Path,
        spec: &PubSubSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let pull_url = format!(
            "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}:pull",
            spec.project, spec.subscription
        );
        let body = serde_json::json!({ "maxMessages": spec.max_messages });
        let resp = crate::tls::http_agent().post(&pull_url)
            .set("Content-Type", "application/json")
            .set("Authorization", &format!("Bearer {}", spec.access_token))
            .send_string(&serde_json::to_string(&body).unwrap_or_default());
        let response: JsonValue = match resp {
            Ok(r) => r
                .into_json()
                .map_err(|e| EngineError::Query(format!("pubsub: response not JSON: {}", e)))?,
            Err(ureq::Error::Status(code, r)) => {
                let b = r.into_string().unwrap_or_default();
                return Err(EngineError::Query(format!(
                    "pubsub HTTP {} on pull: {}",
                    code,
                    b.chars().take(300).collect::<String>()
                )));
            }
            Err(e) => return Err(EngineError::Query(format!("pubsub transport: {}", e))),
        };
        let received = response
            .get("receivedMessages")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut rows: Vec<JsonValue> = Vec::with_capacity(received.len());
        let mut ack_ids: Vec<String> = Vec::with_capacity(received.len());
        for item in received {
            if let Some(ack) = item.get("ackId").and_then(|v| v.as_str()) {
                ack_ids.push(ack.to_string());
            }
            let message = item.get("message").cloned().unwrap_or(JsonValue::Null);
            let mut obj = serde_json::Map::new();
            obj.insert(
                "message_id".into(),
                message.get("messageId").cloned().unwrap_or(JsonValue::Null),
            );
            obj.insert(
                "publish_time".into(),
                message.get("publishTime").cloned().unwrap_or(JsonValue::Null),
            );
            // The data field is base64-encoded - decode best-effort.
            use base64::Engine as _;
            let data_raw = message.get("data").and_then(|v| v.as_str()).unwrap_or("");
            let decoded: Option<String> = base64::engine::general_purpose::STANDARD
                .decode(data_raw)
                .ok()
                .map(|b: Vec<u8>| String::from_utf8_lossy(&b).to_string());
            obj.insert(
                "data".into(),
                decoded.map(JsonValue::String).unwrap_or(JsonValue::Null),
            );
            rows.push(JsonValue::Object(obj));
        }
        let count = rows.len();
        // Persist BEFORE acknowledging: if materialize fails, the messages
        // stay queued and redeliver on their visibility timeout rather than
        // being acked-then-lost.
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        // Acknowledge the batch so messages don't redeliver. Failure
        // is non-fatal - the messages stay queued and re-deliver on
        // their visibility timeout.
        if !ack_ids.is_empty() {
            let ack_url = format!(
                "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}:acknowledge",
                spec.project, spec.subscription
            );
            let ack_body = serde_json::json!({ "ackIds": ack_ids });
            let _ = crate::tls::http_agent().post(&ack_url)
                .set("Content-Type", "application/json")
                .set("Authorization", &format!("Bearer {}", spec.access_token))
                .send_string(&serde_json::to_string(&ack_body).unwrap_or_default());
        }
        Ok(format!(
            "pubsub: materialized {} message(s) into {}",
            count, spec.node_id
        ))
    }

    /// Kafka / Redpanda producer via rskafka. Each upstream row
    /// becomes one Kafka record: key = optional keyColumn value,
    /// value = JSON-stringified row. Records go into a single
    /// partition (multi-partition fan-out is a follow-up). Async
    /// underneath; wrapped in tokio block_on like mongo / tiberius.
    pub(crate) fn run_kafka_sink(
        &self,
        db: &Path,
        spec: &KafkaSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("kafka: 0 rows to produce to {}", spec.topic));
        }
        let cancel = self.cancel.clone();
        let bootstrap: Vec<String> = spec
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("kafka: tokio rt: {}", e)))?;
        let total: Result<usize, String> = rt.block_on(async {
            use rskafka::client::partition::{Compression, UnknownTopicHandling};
            use rskafka::client::ClientBuilder;
            use rskafka::record::Record;
            let client = ClientBuilder::new(bootstrap)
                .build()
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let pc = client
                .partition_client(&spec.topic, spec.partition_id, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| format!("partition client: {}", e))?;
            let mut total = 0_usize;
            let now = chrono::Utc::now();
            for chunk in rows.chunks(spec.batch_size) {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                let records: Vec<Record> = chunk
                    .iter()
                    .map(|row| {
                        let key = if spec.key_column.is_empty() {
                            None
                        } else {
                            row.get(&spec.key_column).and_then(|v| match v {
                                JsonValue::String(s) => Some(s.as_bytes().to_vec()),
                                JsonValue::Null => None,
                                other => Some(other.to_string().into_bytes()),
                            })
                        };
                        let value = serde_json::to_string(row)
                            .unwrap_or_default()
                            .into_bytes();
                        Record {
                            key,
                            value: Some(value),
                            headers: std::collections::BTreeMap::new(),
                            timestamp: now,
                        }
                    })
                    .collect();
                pc.produce(records, Compression::default())
                    .await
                    .map_err(|e| format!("produce batch: {}", e))?;
                total += chunk.len();
            }
            Ok(total)
        });
        match total {
            Ok(n) => Ok(format!("kafka: produced {} record(s) to {}", n, spec.topic)),
            Err(e) if e == "cancelled" => Err(EngineError::Cancelled),
            Err(e) => Err(EngineError::Query(format!("kafka sink: {}", e))),
        }
    }

    /// Kafka / Redpanda consumer via rskafka. Batch-fetches up to
    /// max_records messages from a single partition starting at
    /// start_offset (negative = earliest available). Emits rows of
    /// {offset, key, value, timestamp_ms}. Value is the raw bytes
    /// decoded as UTF-8 (best-effort) - schema-aware decoding (Avro,
    /// Protobuf) is on the roadmap.
    pub(crate) fn run_kafka_source(
        &self,
        db: &Path,
        spec: &KafkaSourceSpec,
    ) -> Result<String, EngineError> {
        let cancel = self.cancel.clone();
        let bootstrap: Vec<String> = spec
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("kafka: tokio rt: {}", e)))?;
        let rows: Result<Vec<JsonValue>, String> = rt.block_on(async {
            use rskafka::client::partition::UnknownTopicHandling;
            use rskafka::client::ClientBuilder;
            let client = ClientBuilder::new(bootstrap)
                .build()
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let pc = client
                .partition_client(&spec.topic, spec.partition_id, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| format!("partition client: {}", e))?;
            // start_offset sentinels: -2 = latest tip (only messages produced
            // after this read starts), any other negative = earliest available,
            // >= 0 = that literal offset.
            let mut next_offset = if spec.start_offset == -2 {
                pc.get_offset(rskafka::client::partition::OffsetAt::Latest)
                    .await
                    .map_err(|e| format!("latest offset: {}", e))?
            } else if spec.start_offset < 0 {
                pc.get_offset(rskafka::client::partition::OffsetAt::Earliest)
                    .await
                    .map_err(|e| format!("earliest offset: {}", e))?
            } else {
                spec.start_offset
            };
            let mut out: Vec<JsonValue> = Vec::new();
            while (out.len() as u64) < spec.max_records {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                let (records, _hw) = pc
                    .fetch_records(next_offset, 1..1_000_000, 1_000)
                    .await
                    .map_err(|e| format!("fetch: {}", e))?;
                if records.is_empty() {
                    break;
                }
                for r in records {
                    let mut obj = serde_json::Map::new();
                    obj.insert("offset".into(), JsonValue::from(r.offset));
                    obj.insert(
                        "timestamp_ms".into(),
                        JsonValue::from(r.record.timestamp.timestamp_millis()),
                    );
                    obj.insert(
                        "key".into(),
                        r.record
                            .key
                            .as_ref()
                            .map(|b| JsonValue::String(String::from_utf8_lossy(b).to_string()))
                            .unwrap_or(JsonValue::Null),
                    );
                    obj.insert(
                        "value".into(),
                        r.record
                            .value
                            .as_ref()
                            .map(|b| JsonValue::String(String::from_utf8_lossy(b).to_string()))
                            .unwrap_or(JsonValue::Null),
                    );
                    out.push(JsonValue::Object(obj));
                    next_offset = r.offset + 1;
                    if out.len() as u64 >= spec.max_records {
                        break;
                    }
                }
            }
            Ok(out)
        });
        let rows = match rows {
            Ok(r) => r,
            Err(e) if e == "cancelled" => return Err(EngineError::Cancelled),
            Err(e) => return Err(EngineError::Query(format!("kafka source: {}", e))),
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "kafka: materialized {} record(s) into {}",
            count, spec.node_id
        ))
    }

    /// SQL Server / Synapse sink via tiberius. Builds multi-row INSERT
    /// VALUES statements batched at spec.batch_size (default 1000 -
    /// SQL Server's per-INSERT VALUES cap). Values are interpolated as
    /// SQL literals via the shared json_to_sql_literal helper - not
    /// parameterized; safe for pipeline-produced data but document
    /// users not to wire untrusted upstream into SQL Server directly.
    pub(crate) fn run_sqlserver_sink(
        &self,
        db: &Path,
        spec: &SqlServerSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!(
                "sqlserver: 0 rows to insert into [{}].[{}]",
                spec.schema, spec.table
            ));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => {
                return Err(EngineError::Query(
                    "sqlserver: upstream rows aren't JSON objects".into(),
                ));
            }
        };
        let qualified = format!(
            "{}.{}.{}",
            ss_quote_ident(&spec.database),
            ss_quote_ident(&spec.schema),
            ss_quote_ident(&spec.table),
        );
        // Upsert (MERGE) clauses, when key columns are configured. Each batch
        // becomes a single MERGE whose source is an inline VALUES table -
        // stateless and correct against real SQL Server (no #temp needed).
        let is_upsert = !spec.upsert_keys.is_empty();
        // Delete-propagation control column (upsert only): flagged rows are
        // DELETEd from the target by key, not written. It is a control column,
        // so it is excluded from the target's data columns (auto-create,
        // INSERT, UPDATE) while still projected in the source so the predicate
        // can read it.
        let delete_col: Option<&str> = if is_upsert {
            spec.delete_column.as_deref()
        } else {
            None
        };
        let data_cols: Vec<&String> = cols
            .iter()
            .filter(|c| Some(c.as_str()) != delete_col)
            .collect();
        // Source column list (all cols incl. the delete flag) names the
        // `AS s (...)` aliases; the data column list drives writes.
        let src_cols_list = cols
            .iter()
            .map(|c| ss_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let cols_list = data_cols
            .iter()
            .map(|c| ss_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        // Auto-create the target table when it doesn't exist, inferring
        // column types from the upstream DuckDB view. The sink otherwise
        // only INSERTs, so loading into a not-yet-created table failed with
        // "Invalid object name" (issue #8: "newly created tables"). Wrapped
        // in IF OBJECT_ID(...) IS NULL so an existing table is untouched.
        let col_types: std::collections::HashMap<String, String> =
            describe_columns(self, db, &spec.from_view).into_iter().collect();
        let col_defs = data_cols
            .iter()
            .map(|c| {
                let ty = duckdb_type_to_sqlserver(
                    col_types.get(c.as_str()).map(|s| s.as_str()).unwrap_or("VARCHAR"),
                );
                format!("{} {}", ss_quote_ident(c), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let create_sql = format!(
            "IF OBJECT_ID('{}', 'U') IS NULL CREATE TABLE {} ({})",
            qualified.replace('\'', "''"),
            qualified,
            col_defs
        );
        let on_clause = spec
            .upsert_keys
            .iter()
            .map(|k| format!("t.{q} = s.{q}", q = ss_quote_ident(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let key_set: std::collections::HashSet<&str> =
            spec.upsert_keys.iter().map(|s| s.as_str()).collect();
        let update_set = data_cols
            .iter()
            .filter(|c| !key_set.contains(c.as_str()))
            .map(|c| format!("t.{q} = s.{q}", q = ss_quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_vals = data_cols
            .iter()
            .map(|c| format!("s.{}", ss_quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        // DELETE-by-flag clause + a NULL-safe NOT-MATCHED guard so a flagged
        // row that has no target match is skipped rather than inserted.
        let (delete_clause, not_matched_guard) = match delete_col {
            Some(dc) => {
                let q = ss_quote_ident(dc);
                let v = spec.delete_value.replace('\'', "''");
                (
                    format!(" WHEN MATCHED AND s.{q} = '{v}' THEN DELETE", q = q, v = v),
                    format!(" AND (s.{q} IS NULL OR s.{q} <> '{v}')", q = q, v = v),
                )
            }
            None => (String::new(), String::new()),
        };
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("sqlserver: tokio rt: {}", e)))?;
        let total = rt
            .block_on(async {
                use tokio_util::compat::TokioAsyncWriteCompatExt;
                let mut config = tiberius::Config::new();
                config.host(&spec.host);
                config.port(spec.port);
                config.authentication(tiberius::AuthMethod::sql_server(
                    &spec.user,
                    &spec.password,
                ));
                config.database(&spec.database);
                if spec.trust_cert {
                    config.trust_cert();
                }
                let tcp = tokio::net::TcpStream::connect(config.get_addr())
                    .await
                    .map_err(|e| format!("connect: {}", e))?;
                tcp.set_nodelay(true).ok();
                let mut client = tiberius::Client::connect(config, tcp.compat_write())
                    .await
                    .map_err(|e| format!("tds handshake: {}", e))?;
                // Create the table if it isn't there yet (no-op otherwise).
                client
                    .execute(create_sql.as_str(), &[])
                    .await
                    .map_err(|e| format!("create table: {}", e))?;
                let mut total = 0_usize;
                for chunk in rows.chunks(spec.batch_size) {
                    if cancel.load(Ordering::Relaxed) {
                        return Err("cancelled".to_string());
                    }
                    let values: Vec<String> = chunk
                        .iter()
                        .map(|row| {
                            let row_obj = row.as_object();
                            let vals: Vec<String> = cols
                                .iter()
                                .map(|c| {
                                    let v = row_obj
                                        .and_then(|o| o.get(c))
                                        .unwrap_or(&JsonValue::Null);
                                    sql_literal(
                                        v,
                                        col_types.get(c).map(|s| s.as_str()),
                                        Dialect::SqlServer,
                                    )
                                })
                                .collect();
                            format!("({})", vals.join(", "))
                        })
                        .collect();
                    let stmt = if is_upsert {
                        let matched = if update_set.is_empty() {
                            String::new()
                        } else {
                            format!(" WHEN MATCHED THEN UPDATE SET {}", update_set)
                        };
                        format!(
                            "MERGE INTO {tgt} AS t USING (VALUES {vals}) AS s ({src_cols}) ON {on}{del}{matched} WHEN NOT MATCHED{guard} THEN INSERT ({cols}) VALUES ({ins});",
                            tgt = qualified,
                            vals = values.join(", "),
                            src_cols = src_cols_list,
                            cols = cols_list,
                            on = on_clause,
                            del = delete_clause,
                            matched = matched,
                            guard = not_matched_guard,
                            ins = insert_vals,
                        )
                    } else {
                        format!(
                            "INSERT INTO {} ({}) VALUES {}",
                            qualified,
                            cols_list,
                            values.join(", ")
                        )
                    };
                    client
                        .execute(stmt, &[])
                        .await
                        .map_err(|e| format!("execute: {}", e))?;
                    total += chunk.len();
                }
                Ok::<usize, String>(total)
            })
            .map_err(|e| if e == "cancelled" {
                EngineError::Cancelled
            } else {
                EngineError::Query(format!("sqlserver sink: {}", e))
            })?;
        Ok(format!(
            "sqlserver: {} {} rows into [{}].[{}].[{}]",
            if is_upsert { "merged" } else { "inserted" },
            total, spec.database, spec.schema, spec.table
        ))
    }

    /// SQL Server / Synapse source via tiberius. Runs the query,
    /// iterates the result stream, converts each row's ColumnData
    /// to JSON, and materializes via the jsonobjects helper.
    pub(crate) fn run_sqlserver_source(
        &self,
        db: &Path,
        spec: &SqlServerSourceSpec,
    ) -> Result<String, EngineError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("sqlserver: tokio rt: {}", e)))?;
        // Open the NDJSON file BEFORE the async block so we own the
        // writer on the executor thread; pass it in by move so the
        // streaming row loop can write each row as it arrives.
        // tiberius's old into_first_result() collected the full row
        // set into a Vec<tiberius::Row> in driver memory, doubled
        // again when we converted to Vec<JsonValue>. For a 1 M-row
        // pull that's two large allocations alive at once; now neither
        // exists - rows pass through tiberius -> writer immediately.
        let writer = JsonLinesWriter::open(&spec.node_id)?;
        // &Path is Copy; capture it for the async block (block_on is scoped,
        // so this never outlives &self).
        let bin = self.binary();
        let count: usize = rt
            .block_on(async move {
                use futures_util::TryStreamExt;
                use tiberius::QueryItem;
                use tokio_util::compat::TokioAsyncWriteCompatExt;
                let mut writer = writer;
                let mut config = tiberius::Config::new();
                config.host(&spec.host);
                config.port(spec.port);
                config.authentication(tiberius::AuthMethod::sql_server(
                    &spec.user,
                    &spec.password,
                ));
                config.database(&spec.database);
                if spec.trust_cert {
                    config.trust_cert();
                }
                let tcp = tokio::net::TcpStream::connect(config.get_addr())
                    .await
                    .map_err(|e| format!("connect: {}", e))?;
                tcp.set_nodelay(true).ok();
                let mut client = tiberius::Client::connect(config, tcp.compat_write())
                    .await
                    .map_err(|e| format!("tds handshake: {}", e))?;
                let mut stream = client
                    .query(&spec.query, &[])
                    .await
                    .map_err(|e| format!("query: {}", e))?;
                let mut count = 0_usize;
                while let Some(item) = stream
                    .try_next()
                    .await
                    .map_err(|e| format!("row stream: {}", e))?
                {
                    let row = match item {
                        QueryItem::Row(r) => r,
                        QueryItem::Metadata(_) => continue,
                    };
                    let mut obj = serde_json::Map::new();
                    for (i, col) in row.columns().iter().enumerate() {
                        let name = col.name().to_string();
                        obj.insert(name, Self::sqlserver_cell_to_json(&row, col, i));
                    }
                    writer
                        .write_row(&JsonValue::Object(obj))
                        .map_err(|e| format!("write row: {}", e))?;
                    count += 1;
                }
                writer
                    .finalize_into_table(bin, db, &spec.node_id)
                    .map_err(|e| format!("finalize: {}", e))?;
                Ok::<usize, String>(count)
            })
            .map_err(|e| EngineError::Query(format!("sqlserver source: {}", e)))?;
        Ok(format!(
            "sqlserver: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// ClickHouse sink: HTTP POST to `?query=INSERT INTO db.table FORMAT
    /// JSONEachRow` with NDJSON body. Batched at spec.batch_size rows.
    pub(crate) fn run_clickhouse_sink(
        &self,
        db: &Path,
        spec: &ClickHouseSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!(
                "clickhouse: 0 rows to insert into {}",
                spec.table
            ));
        }
        let qualified = match &spec.database {
            Some(d) => format!("{}.{}", db_quote_ident(d), db_quote_ident(&spec.table)),
            None => db_quote_ident(&spec.table),
        };
        let base = format!(
            "{}/?query={}",
            spec.endpoint.trim_end_matches('/'),
            urlencode_simple(&format!(
                "INSERT INTO {} FORMAT JSONEachRow",
                qualified
            ))
        );
        let mut total = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            // NDJSON body: one row per line.
            let mut body = String::new();
            for row in chunk {
                let line = serde_json::to_string(row).unwrap_or_else(|_| "{}".into());
                body.push_str(&line);
                body.push('\n');
            }
            let mut req = crate::tls::http_agent().post(&base)
                .set("Content-Type", "application/x-ndjson");
            if let Some(u) = &spec.user {
                req = req.set("X-ClickHouse-User", u);
            }
            if let Some(p) = &spec.password {
                req = req.set("X-ClickHouse-Key", p);
            }
            match req.send_string(&body) {
                Ok(_) => total += chunk.len(),
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "ClickHouse HTTP {} on insert into {}: {}",
                        code,
                        qualified,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "ClickHouse HTTP transport: {}",
                        e
                    )));
                }
            }
        }
        Ok(format!(
            "clickhouse: inserted {} rows into {}",
            total, qualified
        ))
    }

    /// ClickHouse source: POST the SELECT with FORMAT JSON appended; the
    /// response has a top-level `data: [{...}]` array of row objects.
    /// Materialize via the existing jsonobjects helper.
    pub(crate) fn run_clickhouse_source(
        &self,
        db: &Path,
        spec: &ClickHouseSourceSpec,
    ) -> Result<String, EngineError> {
        let url = format!("{}/", spec.endpoint.trim_end_matches('/'));
        let q = if spec
            .query
            .to_uppercase()
            .contains("FORMAT JSON")
        {
            spec.query.clone()
        } else {
            format!("{} FORMAT JSON", spec.query.trim())
        };
        let mut req = crate::tls::http_agent().post(&url).set("Content-Type", "text/plain");
        if let Some(u) = &spec.user {
            req = req.set("X-ClickHouse-User", u);
        }
        if let Some(p) = &spec.password {
            req = req.set("X-ClickHouse-Key", p);
        }
        if let Some(d) = &spec.database {
            req = req.set("X-ClickHouse-Database", d);
        }
        let resp = match req.send_string(&q) {
            Ok(r) => r,
            Err(ureq::Error::Status(code, r)) => {
                let body = r.into_string().unwrap_or_default();
                return Err(EngineError::Query(format!(
                    "ClickHouse HTTP {} on query: {}",
                    code,
                    body.chars().take(300).collect::<String>()
                )));
            }
            Err(e) => {
                return Err(EngineError::Query(format!(
                    "ClickHouse HTTP transport: {}",
                    e
                )));
            }
        };
        let response: JsonValue = resp
            .into_json()
            .map_err(|e| EngineError::Query(format!("ClickHouse response not JSON: {}", e)))?;
        let rows = response
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "clickhouse: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// MongoDB sink: insert_many into the collection in batches. The
    /// async mongodb driver is wrapped in a per-stage tokio runtime
    /// (block_on) so it fits the synchronous executor model the rest
    /// of the engine uses.
    pub(crate) fn run_mongo_sink(
        &self,
        db: &Path,
        spec: &MongoSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        let cancel = self.cancel.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("mongo: tokio runtime: {}", e)))?;
        let result: Result<String, String> = rt.block_on(async {
            let client = mongodb::Client::with_uri_str(&spec.uri)
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let collection = client
                .database(&spec.database)
                .collection::<mongodb::bson::Document>(&spec.collection);
            if spec.mode == "replace" {
                if let Err(e) = collection.drop().await {
                    // Dropping a missing collection is not an error
                    // we should surface; log + continue.
                    eprintln!("mongo: drop before replace failed: {}", e);
                }
            }
            // Upsert mode: replace_one(upsert=true) keyed on `upsert_keys`,
            // which is the idiomatic, index-backed MongoDB upsert (one round
            // trip per doc, no full-collection rewrite). Delete propagation:
            // a doc whose `delete_column` equals `delete_value` is delete_one'd
            // by the same key filter instead of being written; the control
            // column is stripped from the stored document either way.
            if !spec.upsert_keys.is_empty() {
                let mut upserted = 0_usize;
                let mut deleted = 0_usize;
                for chunk in rows.chunks(spec.batch_size) {
                    if cancel.load(Ordering::Relaxed) {
                        return Err("cancelled".into());
                    }
                    for v in chunk {
                        let mut doc = match mongodb::bson::to_document(v) {
                            Ok(d) => d,
                            Err(_) => continue,
                        };
                        let mut filter = mongodb::bson::Document::new();
                        for k in &spec.upsert_keys {
                            if let Some(val) = doc.get(k) {
                                filter.insert(k.clone(), val.clone());
                            }
                        }
                        // No key value on this row -> nothing to match on; skip
                        // rather than upsert an unkeyed document.
                        if filter.is_empty() {
                            continue;
                        }
                        let is_delete = spec
                            .delete_column
                            .as_deref()
                            .map(|dc| bson_flag_matches(doc.get(dc), &spec.delete_value))
                            .unwrap_or(false);
                        if let Some(dc) = &spec.delete_column {
                            doc.remove(dc);
                        }
                        if is_delete {
                            collection
                                .delete_one(filter)
                                .await
                                .map_err(|e| format!("delete_one: {}", e))?;
                            deleted += 1;
                        } else {
                            collection
                                .replace_one(filter, doc)
                                .upsert(true)
                                .await
                                .map_err(|e| format!("replace_one: {}", e))?;
                            upserted += 1;
                        }
                    }
                }
                return Ok(format!(
                    "mongodb: upserted {} / deleted {} docs in {}.{}",
                    upserted, deleted, spec.database, spec.collection
                ));
            }
            let mut total = 0_usize;
            for chunk in rows.chunks(spec.batch_size) {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                let docs: Vec<mongodb::bson::Document> = chunk
                    .iter()
                    .filter_map(|v| mongodb::bson::to_document(v).ok())
                    .collect();
                if docs.is_empty() {
                    continue;
                }
                let inserted = docs.len();
                collection
                    .insert_many(docs)
                    .await
                    .map_err(|e| format!("insert_many: {}", e))?;
                total += inserted;
            }
            Ok(format!(
                "mongodb: inserted {} docs into {}.{}",
                total, spec.database, spec.collection
            ))
        });
        result.map_err(|e| if e == "cancelled" {
            EngineError::Cancelled
        } else {
            EngineError::Query(format!("mongodb sink: {}", e))
        })
    }

    /// MongoDB source: find() with optional filter + projection +
    /// limit. The cursor is drained eagerly and the resulting BSON
    /// documents are converted to JsonValue for materialization.
    pub(crate) fn run_mongo_source(
        &self,
        db: &Path,
        spec: &MongoSourceSpec,
    ) -> Result<String, EngineError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("mongo: tokio runtime: {}", e)))?;
        let docs: Result<Vec<mongodb::bson::Document>, String> = rt.block_on(async {
            let client = mongodb::Client::with_uri_str(&spec.uri)
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let collection = client
                .database(&spec.database)
                .collection::<mongodb::bson::Document>(&spec.collection);
            let mut out = Vec::new();
            if let Some(pl) = &spec.pipeline {
                // #106: aggregation pipeline mode ($match / $lookup / $group ...).
                let v: serde_json::Value = serde_json::from_str(pl)
                    .map_err(|e| format!("bad pipeline JSON: {}", e))?;
                let arr = v
                    .as_array()
                    .ok_or_else(|| "pipeline must be a JSON array of stages".to_string())?;
                let stages = arr
                    .iter()
                    .map(|s| {
                        mongodb::bson::to_document(s)
                            .map_err(|e| format!("pipeline stage to bson: {}", e))
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let mut cursor = collection
                    .aggregate(stages)
                    .await
                    .map_err(|e| format!("aggregate: {}", e))?;
                while cursor.advance().await.map_err(|e| format!("cursor: {}", e))? {
                    out.push(
                        cursor
                            .deserialize_current()
                            .map_err(|e| format!("deserialize: {}", e))?,
                    );
                }
            } else {
                let filter: mongodb::bson::Document = match &spec.filter {
                    Some(f) => {
                        let v: serde_json::Value = serde_json::from_str(f)
                            .map_err(|e| format!("bad filter JSON: {}", e))?;
                        mongodb::bson::to_document(&v)
                            .map_err(|e| format!("filter to bson: {}", e))?
                    }
                    None => mongodb::bson::Document::new(),
                };
                let mut find = collection.find(filter);
                if let Some(limit) = spec.limit {
                    find = find.limit(limit);
                }
                if let Some(p) = &spec.projection {
                    let pv: serde_json::Value = serde_json::from_str(p)
                        .map_err(|e| format!("bad projection JSON: {}", e))?;
                    let pdoc = mongodb::bson::to_document(&pv)
                        .map_err(|e| format!("projection to bson: {}", e))?;
                    find = find.projection(pdoc);
                }
                let mut cursor = find.await.map_err(|e| format!("find: {}", e))?;
                while cursor.advance().await.map_err(|e| format!("cursor: {}", e))? {
                    out.push(
                        cursor
                            .deserialize_current()
                            .map_err(|e| format!("deserialize: {}", e))?,
                    );
                }
            }
            Ok(out)
        });
        let docs = docs.map_err(|e| EngineError::Query(format!("mongodb source: {}", e)))?;
        // BSON Document -> JsonValue. Some BSON types (ObjectId, Date)
        // serialize as objects with {$oid: ...} / {$date: ...} - good
        // enough for downstream DuckDB to ingest as strings/json.
        // Fail loud on a BSON->JSON conversion error rather than silently
        // dropping the document (which would under-count the read).
        let json_docs: Vec<JsonValue> = docs
            .iter()
            .map(|d| {
                serde_json::to_value(d)
                    .map_err(|e| EngineError::Query(format!("mongodb: BSON to JSON: {}", e)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let count = json_docs.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &json_docs)?;
        Ok(format!(
            "mongodb: materialized {} docs into {}",
            count, spec.node_id
        ))
    }

    /// Elasticsearch / OpenSearch _search source. POSTs the query DSL
    /// to {endpoint}/{index}/_search and follows the configured
    /// pagination mode (from+size or search_after). Extracts
    /// hits.hits[]._source per page and materializes.
    pub(crate) fn run_elastic_source(
        &self,
        db: &Path,
        spec: &ElasticSourceSpec,
    ) -> Result<String, EngineError> {
        use plan::ElasticPagination;
        let url = format!(
            "{}/{}/_search",
            spec.endpoint.trim_end_matches('/'),
            spec.index
        );
        let query_dsl: JsonValue = match &spec.query {
            Some(q) => serde_json::from_str(q).map_err(|e| {
                EngineError::Config(format!("elastic: invalid query JSON: {}", e))
            })?,
            None => serde_json::json!({ "match_all": {} }),
        };
        let post = |body: &JsonValue| -> Result<JsonValue, EngineError> {
            let body_str = serde_json::to_string(body).unwrap_or_else(|_| "{}".into());
            let mut req = crate::tls::http_agent().post(&url)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            if let Some(key) = &spec.api_key {
                req = req.set("Authorization", &format!("ApiKey {}", key));
            }
            match req.send_string(&body_str) {
                Ok(r) => r.into_json().map_err(|e| {
                    EngineError::Query(format!("Elastic response not JSON: {}", e))
                }),
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    Err(EngineError::Query(format!(
                        "Elastic HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )))
                }
                Err(e) => Err(EngineError::Query(format!(
                    "Elastic HTTP transport to {}: {}",
                    url, e
                ))),
            }
        };
        let mut all_rows: Vec<JsonValue> = Vec::new();
        let mut pages = 0_u64;
        let mut truncated = false;
        match &spec.pagination {
            ElasticPagination::FromSize => {
                let mut from = 0_u64;
                loop {
                    self.check_cancelled()?;
                    let body = serde_json::json!({
                        "query": query_dsl,
                        "size": spec.size,
                        "from": from,
                    });
                    let response = post(&body)?;
                    let hits = response
                        .pointer("/hits/hits")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let hit_count = hits.len();
                    for h in hits {
                        let source = h
                            .get("_source")
                            .cloned()
                            .unwrap_or(JsonValue::Object(Default::default()));
                        all_rows.push(source);
                    }
                    pages += 1;
                    if (hit_count as u64) < spec.size {
                        break;
                    }
                    if pages >= spec.max_pages {
                        truncated = true;
                        break;
                    }
                    from = from.saturating_add(spec.size);
                }
            }
            ElasticPagination::SearchAfter { sort } => {
                // search_after walks via the last hit's `sort` array.
                // Lifts the 10k max_result_window cap entirely.
                let mut last_sort: Option<JsonValue> = None;
                loop {
                    self.check_cancelled()?;
                    let mut body = serde_json::json!({
                        "query": query_dsl,
                        "size": spec.size,
                        "sort": sort,
                    });
                    if let Some(sa) = &last_sort {
                        body["search_after"] = sa.clone();
                    }
                    let response = post(&body)?;
                    let hits = response
                        .pointer("/hits/hits")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let hit_count = hits.len();
                    // Grab the last hit's sort before we move `hits`.
                    let next_after = hits
                        .last()
                        .and_then(|h| h.get("sort"))
                        .cloned();
                    for h in hits {
                        let source = h
                            .get("_source")
                            .cloned()
                            .unwrap_or(JsonValue::Object(Default::default()));
                        all_rows.push(source);
                    }
                    pages += 1;
                    if hit_count == 0 {
                        break;
                    }
                    if (hit_count as u64) < spec.size {
                        // Last page didn't fill - we're done even with
                        // search_after.
                        break;
                    }
                    if pages >= spec.max_pages {
                        truncated = true;
                        break;
                    }
                    last_sort = match next_after {
                        Some(s) => Some(s),
                        None => break, // server returned no sort; can't continue.
                    };
                }
            }
        }
        if truncated {
            return Err(pagination_capped_err(
                "elastic",
                all_rows.len(),
                spec.max_pages,
            ));
        }
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &all_rows)?;
        Ok(format!(
            "elastic: materialized {} rows ({} page(s), {}) into {}",
            all_rows.len(),
            pages,
            match &spec.pagination {
                ElasticPagination::FromSize => "from+size",
                ElasticPagination::SearchAfter { .. } => "search_after",
            },
            spec.node_id
        ))
    }

    /// Generic HTTP REST source. Fetches the URL (optionally with a
    /// JSON body for POST APIs), parses the response, walks the
    /// configured JSON pointer to find the row array, and follows
    /// cursor pagination by extracting a cursor token + appending it
    /// as a query string parameter to the next request. Stops when
    /// no cursor token is present or max_pages is hit.
    pub(crate) fn run_rest_source(
        &self,
        db: &Path,
        spec: &RestSourceSpec,
    ) -> Result<String, EngineError> {
        let mut url = spec.url.clone();
        let mut all_rows: Vec<JsonValue> = Vec::new();
        let mut pages = 0_u64;
        let mut truncated = false;
        // Mutable state for offset / page strategies; cursor uses
        // per-response extraction inside the loop.
        let mut offset = 0_u64;
        let mut page_no = match &spec.pagination {
            RestPagination::Page { start_page, .. } => *start_page,
            _ => 1,
        };
        // Seed the FIRST request with the start page; the loop only appends the
        // page param on subsequent requests, so without this the first call hit
        // the server's default page and a non-default start_page was skipped.
        if let RestPagination::Page { page_param, start_page } = &spec.pagination {
            let sep = if url.contains('?') { '&' } else { '?' };
            url = format!("{}{}{}={}", url, sep, page_param, start_page);
        }
        // One Agent for the whole pagination walk so keep-alive connections
        // are reused across pages instead of a fresh TCP+TLS handshake each
        // request (ureq::request uses a throwaway agent per call).
        let agent = crate::tls::http_agent();
        loop {
            self.check_cancelled()?;
            // Build request
            let mut req = agent.request(&spec.method, &url);
            let has_ct = spec
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("content-type"));
            for (k, v) in &spec.headers {
                req = req.set(k, v);
            }
            if spec.body.is_some() && !has_ct {
                req = req.set("content-type", "application/json");
            }
            let resp_result = match &spec.body {
                Some(b) => req.send_string(b),
                None => req.call(),
            };
            let response_raw = match resp_result {
                Ok(r) => r,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "REST HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "REST HTTP transport to {}: {}",
                        url, e
                    )));
                }
            };
            // Capture Link header before consuming the response body.
            let link_header = response_raw.header("link").map(String::from);
            // For XML, parse as text + walk row_path; pagination is
            // not meaningful (SOAP has no cross-envelope convention)
            // so we treat the JSON-pointer/cursor variants as no-ops
            // by returning a Null response from this branch.
            let (rows, response): (Vec<JsonValue>, JsonValue) = match spec.response_format {
                RestResponseFormat::Json => {
                    let response: JsonValue = response_raw.into_json().map_err(|e| {
                        EngineError::Query(format!("REST response not JSON: {}", e))
                    })?;
                    // Locate the rows: the whole response when no responsePath
                    // is set, else the JSON pointer target. A located ARRAY is
                    // the row set; a single OBJECT is one row (issue #13: APIs
                    // like open-meteo return one JSON object, which previously
                    // yielded zero rows + an empty file with no error). Scalars
                    // / null / missing pointer are genuinely empty.
                    let rows = {
                        let located = if spec.response_path.is_empty() {
                            Some(&response)
                        } else {
                            response.pointer(&spec.response_path)
                        };
                        match located {
                            Some(JsonValue::Array(a)) => a.clone(),
                            // An empty object means "no data" (like []), not a
                            // single empty row.
                            Some(JsonValue::Object(o)) if o.is_empty() => Vec::new(),
                            Some(v @ JsonValue::Object(_)) => vec![v.clone()],
                            _ => Vec::new(),
                        }
                    };
                    (rows, response)
                }
                RestResponseFormat::Xml => {
                    let body = response_raw.into_string().map_err(|e| {
                        EngineError::Query(format!("REST XML response read: {}", e))
                    })?;
                    let rows = walk_xml_to_rows(&body, &spec.response_path, &self.cancel)?;
                    (rows, JsonValue::Null)
                }
            };
            let row_count = rows.len();
            all_rows.extend(rows);
            pages += 1;
            // Determine whether another page exists (and set up the next
            // request URL as a side effect). Done BEFORE the page-cap
            // check so we can tell "genuinely exhausted" (advanced=false)
            // from "stopped at the cap with more to fetch" (advanced=true
            // while pages >= max_pages).
            let advanced = match &spec.pagination {
                RestPagination::None => false,
                RestPagination::Cursor { next_path, param } => {
                    let next = response
                        .pointer(next_path)
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(String::from);
                    match next {
                        Some(token) => {
                            let sep = if spec.url.contains('?') { '&' } else { '?' };
                            url = format!(
                                "{}{}{}={}",
                                spec.url,
                                sep,
                                param,
                                urlencode_simple(&token)
                            );
                            true
                        }
                        None => false,
                    }
                }
                RestPagination::Offset { offset_param, page_size, total_path } => {
                    // A short page means we have reached the end.
                    if (row_count as u64) < *page_size {
                        false
                    } else {
                        let next_offset = offset.saturating_add(*page_size);
                        // Body-driven stop (issue #41): an API that reports a
                        // total row count (e.g. Redmine `total_count`) returns
                        // HTTP 200 + an empty array past the end, so the status
                        // code cannot signal the end. Stop once the next offset
                        // would be at or past the total.
                        let reached_total = total_path
                            .as_deref()
                            .and_then(|p| response.pointer(p))
                            .and_then(|v| {
                                v.as_u64()
                                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
                            })
                            .map(|total| next_offset >= total)
                            .unwrap_or(false);
                        if reached_total {
                            false
                        } else {
                            offset = next_offset;
                            let sep = if spec.url.contains('?') { '&' } else { '?' };
                            url = format!("{}{}{}={}", spec.url, sep, offset_param, offset);
                            true
                        }
                    }
                }
                RestPagination::Page { page_param, .. } => {
                    if row_count == 0 {
                        false
                    } else {
                        page_no = page_no.saturating_add(1);
                        let sep = if spec.url.contains('?') { '&' } else { '?' };
                        url = format!("{}{}{}={}", spec.url, sep, page_param, page_no);
                        true
                    }
                }
                RestPagination::Link => {
                    match link_header.as_deref().and_then(parse_link_next) {
                        Some(next_url) => {
                            url = next_url;
                            true
                        }
                        None => false,
                    }
                }
                RestPagination::NextUrl { next_path } => {
                    let next = response
                        .pointer(next_path)
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .map(String::from);
                    match next {
                        Some(next_url) => {
                            url = next_url;
                            true
                        }
                        None => false,
                    }
                }
            };
            if !advanced {
                break;
            }
            if pages >= spec.max_pages {
                truncated = true;
                break;
            }
        }
        if truncated {
            return Err(pagination_capped_err(
                "rest",
                all_rows.len(),
                spec.max_pages,
            ));
        }
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &all_rows)?;
        Ok(format!(
            "rest: materialized {} rows ({} page(s)) into {}",
            all_rows.len(),
            pages,
            spec.node_id
        ))
    }

    /// Read a pipeline file, parse it as a PipelineDoc, and run it
    /// inline via the engine's normal execute_pipeline. Failures
    /// surface as Err(EngineError::Query) with the sub-pipeline's
    /// error message. Used by ctl.runpipeline / ctl.trigger.
    pub(crate) fn run_subpipeline(&self, path: &str) -> Result<(), EngineError> {
        self.run_subpipeline_with_subs(path, &std::collections::HashMap::new())
    }

    /// ctl.parallelize: run each branch sub-pipeline doc (JSON, carrying a
    /// `${__PSNAP__}` snapshot placeholder) concurrently. Each branch parses +
    /// executes in its own temp DB on a worker thread; branches read the shared
    /// snapshot Parquet read-only, so there is no write contention. Runs in
    /// waves of `max_concurrency` (0 = all at once) and fails on the first
    /// branch error.
    pub(crate) fn run_parallel_branches(
        &self,
        branches: &[String],
        snapshot: &Path,
        max_concurrency: usize,
    ) -> Result<Vec<crate::RunResult>, EngineError> {
        // Forward slashes + no quotes -> safe to splice into the branch JSON.
        let snap = snapshot.display().to_string().replace('\\', "/");
        // max_concurrency 0 = auto: run one branch per available CPU core
        // (capped to the branch count) so many branches don't oversubscribe
        // the machine. A non-zero value is an explicit cap.
        let wave = if max_concurrency == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .min(branches.len().max(1))
        } else {
            max_concurrency
        };
        // Collect each branch's RunResult so the caller can fold the branch
        // nodes (and their sink row counts) back into the parent run report -
        // otherwise a parallelize-terminated pipeline shows "0 rows written".
        let mut results: Vec<crate::RunResult> = Vec::new();
        for chunk in branches.chunks(wave) {
            let mut handles = Vec::with_capacity(chunk.len());
            for doc_json in chunk {
                let engine = self.clone();
                let content = doc_json.replace("${__PSNAP__}", &snap);
                handles.push(std::thread::spawn(move || -> Result<crate::RunResult, String> {
                    let doc: plan::PipelineDoc = serde_json::from_str(&content)
                        .map_err(|e| format!("branch parse: {}", e))?;
                    let r = engine.execute_pipeline(&doc);
                    if r.status == "ok" {
                        Ok(r)
                    } else {
                        Err(r.error.unwrap_or_else(|| "branch failed".into()))
                    }
                }));
            }
            for h in handles {
                match h.join() {
                    Ok(Ok(r)) => results.push(r),
                    Ok(Err(e)) => return Err(EngineError::Query(e)),
                    Err(_) => return Err(EngineError::Query("branch thread panicked".into())),
                }
            }
        }
        Ok(results)
    }

    /// Read a pipeline file, perform `${KEY}` text substitution from
    /// the supplied map, parse the result as a PipelineDoc, and run
    /// it inline. Used by ctl.iterate (${ITER_INDEX}) and ctl.foreach
    /// (${ITER_ITEM_<field>}). String substitution happens on the raw
    /// JSON text so any prop value can carry templated content; safe
    /// because we substitute INSIDE JSON strings only when the
    /// placeholder is in a string literal already.
    pub(crate) fn run_subpipeline_with_subs(
        &self,
        path: &str,
        subs: &std::collections::HashMap<String, String>,
    ) -> Result<(), EngineError> {
        let resolved = resolve_subpipeline_ref(path);
        let mut content = std::fs::read_to_string(&resolved).map_err(|e| {
            EngineError::Config(format!("sub-pipeline: read '{}': {}", resolved, e))
        })?;
        // Resolve the workspace's context variables (e.g. ${MOTHERDUCK_TOKEN})
        // in the child too. The parent pipeline is resolved by the caller before
        // it reaches the engine, but a child read raw from disk here is not, so
        // its context placeholders would otherwise pass through literally. Per-
        // row ITER substitutions win on any key collision.
        let mut merged = workspace_context_vars();
        for (k, v) in subs {
            merged.insert(k.clone(), v.clone());
        }
        for (key, val) in &merged {
            let placeholder = format!("${{{}}}", key);
            if content.contains(&placeholder) {
                // JSON-escape the value before substitution so embedded
                // quotes / backslashes don't break parsing.
                let escaped: String = val
                    .chars()
                    .flat_map(|c| match c {
                        '"' => vec!['\\', '"'],
                        '\\' => vec!['\\', '\\'],
                        '\n' => vec!['\\', 'n'],
                        '\r' => vec!['\\', 'r'],
                        '\t' => vec!['\\', 't'],
                        c => vec![c],
                    })
                    .collect();
                content = content.replace(&placeholder, &escaped);
            }
        }
        let sub_doc: plan::PipelineDoc = serde_json::from_str(&content).map_err(|e| {
            EngineError::Config(format!("sub-pipeline: parse '{}': {}", path, e))
        })?;
        let result = self.execute_pipeline(&sub_doc);
        if result.status == "ok" {
            Ok(())
        } else {
            Err(EngineError::Query(
                result
                    .error
                    .unwrap_or_else(|| "sub-pipeline failed (no error message)".into()),
            ))
        }
    }

    /// xf.incremental: materialize only the rows whose watermark column is
    /// past the last successful run's mark, and queue the new mark to be
    /// persisted iff the whole run succeeds (the executor writes
    /// `pending` after the final stage). The mark lives in
    /// `$DUCKLE_WORKSPACE/state/<pipeline>/<node>.json` as {column, value,
    /// type}; the type lets the next run cast the stored string back to the
    /// column's real type for a correct comparison.
    pub(crate) fn run_incremental(
        &self,
        db: &Path,
        spec: &plan::IncrementalSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<(std::path::PathBuf, JsonValue)>,
    ) -> Result<String, EngineError> {
        let col_q = plan::quote_ident(&spec.column);
        let up_q = plan::quote_ident(&spec.from_view);
        let node_q = plan::quote_ident(&spec.node_id);

        let state_path = incremental_state_path(pipeline_name, &spec.node_id);
        let saved = state_path.as_ref().and_then(read_incremental_state);

        // Build the WHERE filter from saved state, else the configured
        // initial value (typed by probing the column), else no filter.
        let predicate = if let Some((value, ty)) = &saved {
            Some(format!(
                "{} > CAST('{}' AS {})",
                col_q,
                value.replace('\'', "''"),
                sanitize_sql_type(ty)
            ))
        } else if let Some(initial) = &spec.initial {
            match self.probe_column_type(db, &up_q, &col_q) {
                Some(ty) => Some(format!(
                    "{} > CAST('{}' AS {})",
                    col_q,
                    initial.replace('\'', "''"),
                    sanitize_sql_type(&ty)
                )),
                // No rows to probe a type from -> nothing to load anyway.
                None => Some(format!("{} > '{}'", col_q, initial.replace('\'', "''"))),
            }
        } else {
            None
        };
        let where_clause = predicate
            .map(|p| format!(" WHERE {}", p))
            .unwrap_or_default();

        let materialize = format!(
            "CREATE OR REPLACE TABLE {node} AS SELECT * FROM {up}{where_clause};",
            node = node_q,
            up = up_q,
            where_clause = where_clause,
        );
        self.run(Some(db), &materialize, false)?;

        // New high-water mark = MAX over the rows we just loaded. NULL means
        // nothing new this run, so we leave the saved mark untouched.
        let max_sql = format!(
            "SELECT CAST(MAX({col}) AS VARCHAR) AS v, typeof(MAX({col})) AS t FROM {node};",
            col = col_q,
            node = node_q,
        );
        if let Some(row) = self.run_rows(Some(db), &max_sql)?.into_iter().next() {
            let new_val = row.get("v").and_then(|v| v.as_str()).map(String::from);
            let new_ty = row
                .get("t")
                .and_then(|v| v.as_str())
                .unwrap_or("VARCHAR")
                .to_string();
            if let (Some(value), Some(path)) = (new_val, state_path) {
                pending.push((
                    path,
                    serde_json::json!({
                        "column": spec.column,
                        "value": value,
                        "type": new_ty,
                    }),
                ));
            }
        }
        Ok(format!(
            "incremental: loaded rows past the saved {} watermark",
            spec.column
        ))
    }

    /// src.ducklake.changes: DuckLake change-data-feed (CDC) source. ATTACHes
    /// the catalog, reads the current snapshot id and the last consumed one
    /// (workspace state), materializes `table_changes(table, last, current)`
    /// (rows with snapshot_id > last, so the boundary snapshot isn't re-read),
    /// and queues the new snapshot id to persist on run success.
    pub(crate) fn run_ducklake_cdc(
        &self,
        db: &Path,
        spec: &plan::DuckLakeCdcSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<(std::path::PathBuf, JsonValue)>,
    ) -> Result<String, EngineError> {
        let path = spec.path.replace('\\', "/").replace('\'', "''");
        let attach = format!(
            "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:{}' AS duckle_src (READ_ONLY); ",
            path
        );
        let node_q = plan::quote_ident(&spec.node_id);
        // Table arg for table_changes() is a string; qualify with the schema
        // when one is configured (DuckLake defaults to `main`).
        let table_arg = match &spec.schema {
            Some(s) if !s.is_empty() => format!("{}.{}", s, spec.table),
            _ => spec.table.clone(),
        }
        .replace('\'', "''");

        // Current snapshot id from the catalog.
        let cur_rows = self.run_rows(
            Some(db),
            &format!("{}SELECT max(snapshot_id) AS cur FROM duckle_src.snapshots();", attach),
        )?;
        let current = cur_rows
            .into_iter()
            .next()
            .and_then(|r| r.get("cur").cloned())
            .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok())))
            .unwrap_or(0);

        let state_path = incremental_state_path(pipeline_name, &spec.node_id);
        let last = state_path
            .as_ref()
            .and_then(read_snapshot_state)
            .unwrap_or(spec.initial_snapshot);

        let type_filter = if spec.inserts_only {
            " AND change_type = 'insert'"
        } else {
            ""
        };

        if current == 0 || last >= current {
            // No snapshots yet, or nothing new: emit an empty result that still
            // carries the change-feed schema when the catalog has snapshots.
            let empty_sql = if current == 0 {
                format!("CREATE OR REPLACE TABLE {node} AS SELECT NULL::BIGINT AS snapshot_id, NULL::VARCHAR AS change_type LIMIT 0;", node = node_q)
            } else {
                format!(
                    "{attach}CREATE OR REPLACE TABLE {node} AS SELECT * FROM duckle_src.table_changes('{tbl}', {cur}, {cur}) WHERE 1=0;",
                    attach = attach, node = node_q, tbl = table_arg, cur = current,
                )
            };
            self.run(Some(db), &empty_sql, false)?;
            return Ok(format!(
                "ducklake-cdc: no new changes (snapshot {} -> {})",
                last, current
            ));
        }

        let materialize = format!(
            "{attach}CREATE OR REPLACE TABLE {node} AS SELECT * FROM duckle_src.table_changes('{tbl}', {last}, {cur}) WHERE snapshot_id > {last}{type_filter};",
            attach = attach,
            node = node_q,
            tbl = table_arg,
            last = last,
            cur = current,
            type_filter = type_filter,
        );
        self.run(Some(db), &materialize, false)?;

        let rows = self
            .run_rows(
                Some(db),
                &format!("SELECT count(*) AS c FROM {};", node_q),
            )?
            .into_iter()
            .next()
            .and_then(|r| r.get("c").cloned())
            .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok())))
            .unwrap_or(0);

        if let Some(path) = state_path {
            pending.push((path, serde_json::json!({ "snapshot_id": current })));
        }
        Ok(format!(
            "ducklake-cdc: {} change row(s) from snapshot {} to {}",
            rows, last, current
        ))
    }

    /// Best-effort type of a column from a sample non-null row, e.g.
    /// "BIGINT" / "TIMESTAMP". None when the upstream has no rows to probe.
    fn probe_column_type(&self, db: &Path, up_q: &str, col_q: &str) -> Option<String> {
        let sql = format!(
            "SELECT typeof({col}) AS t FROM {up} WHERE {col} IS NOT NULL LIMIT 1;",
            col = col_q,
            up = up_q,
        );
        self.run_rows(Some(db), &sql)
            .ok()
            .and_then(|rows| rows.into_iter().next())
            .and_then(|r| r.get("t").and_then(|v| v.as_str()).map(String::from))
    }

    /// Snowflake SQL API source. POSTs the SELECT, polls the
    /// statementHandle if the server returned async, then walks
    /// resultSetMetaData.partitionInfo[] fetching partitions 1..N
    /// (partition 0 ships inline in the initial response). Each
    /// partition's `data` array is concatenated and materialized
    /// into node_id via read_json_auto.
    pub(crate) fn run_snowflake_source(
        &self,
        db: &Path,
        spec: &SnowflakeSourceSpec,
    ) -> Result<String, EngineError> {
        let base_url = spec.endpoint.clone().unwrap_or_else(|| {
            format!(
                "https://{}.snowflakecomputing.com/api/v2/statements",
                spec.account
            )
        });
        let auth_header = build_snowflake_auth_header(&spec.account, &spec.auth)?;
        let is_jwt = matches!(spec.auth, SnowflakeAuth::Jwt { .. });
        let mut body_obj = serde_json::Map::new();
        body_obj.insert("statement".into(), JsonValue::String(spec.query.clone()));
        body_obj.insert("timeout".into(), JsonValue::Number(60.into()));
        if let Some(db) = &spec.database {
            body_obj.insert("database".into(), JsonValue::String(db.clone()));
        }
        if let Some(s) = &spec.schema {
            body_obj.insert("schema".into(), JsonValue::String(s.clone()));
        }
        if let Some(wh) = &spec.warehouse {
            body_obj.insert("warehouse".into(), JsonValue::String(wh.clone()));
        }
        if let Some(role) = &spec.role {
            body_obj.insert("role".into(), JsonValue::String(role.clone()));
        }
        let body = serde_json::to_string(&JsonValue::Object(body_obj))
            .unwrap_or_else(|_| "{}".into());
        let initial = sf_request(&base_url, "POST", &auth_header, is_jwt, Some(&body))?;
        // If the server handed us a statementHandle without data
        // (async path: 202 in HTTP terms, but ureq returns 200/202
        // both as Ok), poll until we see data.
        let mut response = if initial.get("data").is_some() {
            initial
        } else {
            let handle = initial
                .get("statementHandle")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    EngineError::Query(
                        "Snowflake response has neither data nor statementHandle".into(),
                    )
                })?
                .to_string();
            poll_snowflake_until_done(&base_url, &auth_header, is_jwt, &handle)?
        };
        // resultSetMetaData.rowType carries each column's name + type (+
        // scale/precision). Snowflake encodes EVERY cell as a JSON string, so
        // we read each column as VARCHAR and cast it to its real type from
        // rowType - timestamps are float epoch-seconds strings, dates are day
        // counts, numbers are decimal strings; read_json_auto would otherwise
        // infer them as VARCHAR/DOUBLE (GitHub #24, column-type inference).
        let row_type = response
            .pointer("/resultSetMetaData/rowType")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                EngineError::Query("Snowflake response missing resultSetMetaData.rowType".into())
            })?;
        let mut cols: Vec<String> = Vec::with_capacity(row_type.len());
        let mut columns_spec_parts: Vec<String> = Vec::with_capacity(row_type.len());
        let mut select_parts: Vec<String> = Vec::with_capacity(row_type.len());
        for c in row_type {
            // Bail rather than `continue` on a nameless column: the row data is
            // an array of cells positioned by the ORIGINAL column index, so
            // silently dropping one name would shift every later column name
            // onto the wrong cell. (Snowflake always names columns; this just
            // guarantees the name list stays index-aligned with the cells.)
            let Some(name) = c.get("name").and_then(|n| n.as_str()) else {
                return Err(EngineError::Query(
                    "Snowflake rowType has a column with no name; cannot align result columns"
                        .into(),
                ));
            };
            let sf_type = c
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("text")
                .to_ascii_lowercase();
            let scale = c.get("scale").and_then(|s| s.as_i64()).unwrap_or(0);
            let precision = c.get("precision").and_then(|p| p.as_i64()).unwrap_or(38);
            let ident = plan::quote_ident(name);
            cols.push(name.to_string());
            columns_spec_parts.push(format!("'{}': 'VARCHAR'", name.replace('\'', "''")));
            select_parts.push(format!(
                "{} AS {}",
                snowflake_cast_expr(&ident, &sf_type, scale, precision),
                ident
            ));
        }
        let columns_spec = columns_spec_parts.join(", ");
        let select_list = select_parts.join(", ");
        let mut all_data = response
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        // Multi-partition: partitionInfo[0] shipped inline (the `data` above);
        // fetch partitions 1..N. Each `?partition=N` body is gzip-compressed
        // (decoded transparently by ureq's gzip feature) and carries NO
        // metadata - it is the row payload only, which Snowflake may serialize
        // as a bare array of rows OR as a {"data": [...]} object, so accept
        // both. statementHandle is present even in the inline case (GitHub #24).
        let partition_count = response
            .pointer("/resultSetMetaData/partitionInfo")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(1);
        if partition_count > 1 {
            let handle = response
                .get("statementHandle")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    EngineError::Query(
                        "Snowflake paged response missing statementHandle".into(),
                    )
                })?
                .to_string();
            for i in 1..partition_count {
                self.check_cancelled()?;
                let part_url = format!("{}/{}?partition={}", base_url, handle, i);
                let part = sf_request(&part_url, "GET", &auth_header, is_jwt, None)?;
                let part_rows = match &part {
                    JsonValue::Array(a) => Some(a.clone()),
                    _ => part.get("data").and_then(|v| v.as_array()).cloned(),
                };
                match part_rows {
                    Some(rows) => all_data.extend(rows),
                    None => {
                        return Err(EngineError::Query(format!(
                            "Snowflake partition {} returned no row data (unexpected response shape)",
                            i
                        )))
                    }
                }
            }
        }
        // Pretend warning to silence "response variable unused after
        // reassignment" if all_data didn't grow.
        let _ = &mut response;
        materialize_typed_arrayrows(
            &self.bin,
            db,
            &spec.node_id,
            &cols,
            &columns_spec,
            &select_list,
            &all_data,
        )?;
        Ok(format!(
            "snowflake: materialized {} rows ({} partition(s)) into {}",
            all_data.len(),
            partition_count,
            spec.node_id
        ))
    }

    /// Databricks SQL source. POSTs the SELECT, polls for SUCCEEDED
    /// if the server returned PENDING/RUNNING after wait_timeout, then
    /// follows result.next_chunk_internal_link until exhausted. Each
    /// chunk's data_array is concatenated and materialized.
    pub(crate) fn run_databricks_source(
        &self,
        db: &Path,
        spec: &DatabricksSourceSpec,
    ) -> Result<String, EngineError> {
        let base_url = spec.endpoint.clone().unwrap_or_else(|| {
            format!("https://{}/api/2.0/sql/statements/", spec.workspace)
        });
        let auth = format!("Bearer {}", spec.pat);
        let mut body_obj = serde_json::Map::new();
        body_obj.insert("statement".into(), JsonValue::String(spec.query.clone()));
        body_obj.insert(
            "warehouse_id".into(),
            JsonValue::String(spec.warehouse_id.clone()),
        );
        if let Some(c) = &spec.catalog {
            body_obj.insert("catalog".into(), JsonValue::String(c.clone()));
        }
        if let Some(s) = &spec.schema {
            body_obj.insert("schema".into(), JsonValue::String(s.clone()));
        }
        body_obj.insert(
            "wait_timeout".into(),
            JsonValue::String(format!("{}s", spec.wait_timeout_seconds)),
        );
        body_obj.insert(
            "on_wait_timeout".into(),
            JsonValue::String("CONTINUE".into()),
        );
        let body = serde_json::to_string(&JsonValue::Object(body_obj))
            .unwrap_or_else(|_| "{}".into());
        let initial = dbr_request(&base_url, "POST", &auth, Some(&body))?;
        // Poll until SUCCEEDED if we got PENDING/RUNNING back.
        let response = match initial
            .pointer("/status/state")
            .and_then(|v| v.as_str())
            .unwrap_or("SUCCEEDED")
        {
            "SUCCEEDED" => initial,
            "PENDING" | "RUNNING" => {
                let statement_id = initial
                    .get("statement_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        EngineError::Query(
                            "Databricks async response missing statement_id".into(),
                        )
                    })?
                    .to_string();
                let poll_url = format!("{}{}", base_url, statement_id);
                poll_databricks_until_done(&poll_url, &auth)?
            }
            other => {
                let err = initial
                    .pointer("/status/error/message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("(no message)");
                return Err(EngineError::Query(format!(
                    "Databricks statement state {}: {}",
                    other, err
                )));
            }
        };
        let cols = response
            .pointer("/manifest/schema/columns")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                EngineError::Query(
                    "Databricks response missing manifest.schema.columns".into(),
                )
            })?
            .iter()
            .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(String::from))
            .collect::<Vec<_>>();
        let mut all_data = response
            .pointer("/result/data_array")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        // Follow next_chunk_internal_link until None. The link is a
        // path under the workspace; prepend https://workspace.
        let mut next_link: Option<String> = response
            .pointer("/result/next_chunk_internal_link")
            .and_then(|v| v.as_str())
            .map(String::from);
        let mut chunks = 1_usize;
        while let Some(link) = next_link {
            self.check_cancelled()?;
            // If endpoint override is in play (tests), prepend the
            // override's scheme+host; otherwise use the workspace host.
            let chunk_url = if let Some(ep) = &spec.endpoint {
                // Extract "scheme://host[:port]" from ep so we can
                // append the relative chunk link as-is.
                let prefix_end = ep
                    .find("://")
                    .map(|i| {
                        let after = &ep[i + 3..];
                        i + 3 + after.find('/').unwrap_or(after.len())
                    })
                    .unwrap_or(ep.len());
                format!("{}{}", &ep[..prefix_end], link)
            } else {
                format!("https://{}{}", spec.workspace, link)
            };
            let chunk = dbr_request(&chunk_url, "GET", &auth, None)?;
            match chunk.get("data_array").and_then(|v| v.as_array()) {
                Some(d) => {
                    all_data.extend(d.iter().cloned());
                    chunks += 1;
                }
                None => {
                    return Err(EngineError::Query(
                        "databricks chunk follower: response has no data_array".into(),
                    ))
                }
            }
            next_link = chunk
                .get("next_chunk_internal_link")
                .and_then(|v| v.as_str())
                .map(String::from);
        }
        materialize_arrayrows_as_table(&self.bin, db, &spec.node_id, &cols, &all_data)?;
        Ok(format!(
            "databricks: materialized {} rows ({} chunk(s)) into {}",
            all_data.len(),
            chunks,
            spec.node_id
        ))
    }

    /// Databricks SQL sink. Same multi-row INSERT batching as Snowflake;
    /// difference is the URL shape, the body field names (warehouse_id,
    /// catalog/schema, wait_timeout, on_wait_timeout), and identifier
    /// quoting uses backticks instead of double quotes.
    pub(crate) fn run_databricks_sink(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &DatabricksSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!(
            "{}SELECT * FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("databricks: 0 rows to insert into {}", spec.table));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => return Err(EngineError::Query("databricks: upstream rows aren't JSON objects".into())),
        };
        // Build the qualified target. Catalog/schema both optional;
        // Databricks accepts 2-part (schema.table) or 3-part naming
        // (catalog.schema.table) when ambient catalog/schema is set in
        // the request body.
        let qualified = match (&spec.catalog, &spec.schema) {
            (Some(c), Some(s)) => format!(
                "{}.{}.{}",
                db_quote_ident(c),
                db_quote_ident(s),
                db_quote_ident(&spec.table)
            ),
            (None, Some(s)) => format!(
                "{}.{}",
                db_quote_ident(s),
                db_quote_ident(&spec.table)
            ),
            _ => db_quote_ident(&spec.table),
        };
        // Upsert (MERGE) clauses when key columns are configured. Databricks
        // (Spark SQL) accepts a subquery source and qualified UPDATE SET.
        let is_upsert = !spec.upsert_keys.is_empty();
        // Delete-propagation control column (upsert only): excluded from the
        // target's data columns, kept in the source projection (see SQL Server).
        let delete_col: Option<&str> = if is_upsert {
            spec.delete_column.as_deref()
        } else {
            None
        };
        let data_cols: Vec<&String> = cols
            .iter()
            .filter(|c| Some(c.as_str()) != delete_col)
            .collect();
        let cols_list = data_cols
            .iter()
            .map(|c| db_quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let on_clause = spec
            .upsert_keys
            .iter()
            .map(|k| format!("t.{q} = s.{q}", q = db_quote_ident(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let dk_key_set: std::collections::HashSet<&str> =
            spec.upsert_keys.iter().map(|s| s.as_str()).collect();
        let update_set = data_cols
            .iter()
            .filter(|c| !dk_key_set.contains(c.as_str()))
            .map(|c| format!("t.{q} = s.{q}", q = db_quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_vals = data_cols
            .iter()
            .map(|c| format!("s.{}", db_quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");
        let (delete_clause, not_matched_guard) = match delete_col {
            Some(dc) => {
                let q = db_quote_ident(dc);
                let v = jsonnative_quote_inner(&spec.delete_value);
                (
                    format!(" WHEN MATCHED AND s.{q} = '{v}' THEN DELETE", q = q, v = v),
                    format!(" AND (s.{q} IS NULL OR s.{q} <> '{v}')", q = q, v = v),
                )
            }
            None => (String::new(), String::new()),
        };
        let url = spec.endpoint.clone().unwrap_or_else(|| {
            format!("https://{}/api/2.0/sql/statements/", spec.workspace)
        });
        let mut total_inserted = 0_usize;
        for chunk in rows.chunks(spec.batch_size) {
            self.check_cancelled()?;
            let values: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let row_obj = row.as_object();
                    let vals: Vec<String> = cols
                        .iter()
                        .map(|c| {
                            let v = row_obj
                                .and_then(|o| o.get(c))
                                .unwrap_or(&JsonValue::Null);
                            sql_literal(v, None, Dialect::JsonNative)
                        })
                        .collect();
                    format!("({})", vals.join(", "))
                })
                .collect();
            let stmt = if is_upsert {
                let src_selects: Vec<String> = chunk
                    .iter()
                    .map(|row| {
                        let obj = row.as_object();
                        let items: Vec<String> = cols
                            .iter()
                            .map(|c| {
                                let v = obj.and_then(|o| o.get(c)).unwrap_or(&JsonValue::Null);
                                format!(
                                    "{} AS {}",
                                    sql_literal(v, None, Dialect::JsonNative),
                                    db_quote_ident(c)
                                )
                            })
                            .collect();
                        format!("SELECT {}", items.join(", "))
                    })
                    .collect();
                let matched = if update_set.is_empty() {
                    String::new()
                } else {
                    format!(" WHEN MATCHED THEN UPDATE SET {}", update_set)
                };
                format!(
                    "MERGE INTO {tgt} t USING ({src}) s ON {on}{del}{matched} WHEN NOT MATCHED{guard} THEN INSERT ({cols}) VALUES ({ins})",
                    tgt = qualified,
                    src = src_selects.join(" UNION ALL "),
                    cols = cols_list,
                    on = on_clause,
                    del = delete_clause,
                    matched = matched,
                    guard = not_matched_guard,
                    ins = insert_vals,
                )
            } else {
                format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    qualified,
                    cols_list,
                    values.join(", ")
                )
            };
            let mut body_obj = serde_json::Map::new();
            body_obj.insert("statement".into(), JsonValue::String(stmt));
            body_obj.insert(
                "warehouse_id".into(),
                JsonValue::String(spec.warehouse_id.clone()),
            );
            if let Some(c) = &spec.catalog {
                body_obj.insert("catalog".into(), JsonValue::String(c.clone()));
            }
            if let Some(s) = &spec.schema {
                body_obj.insert("schema".into(), JsonValue::String(s.clone()));
            }
            body_obj.insert(
                "wait_timeout".into(),
                JsonValue::String(format!("{}s", spec.wait_timeout_seconds)),
            );
            body_obj.insert(
                "on_wait_timeout".into(),
                JsonValue::String("CONTINUE".into()),
            );
            let body = serde_json::to_string(&JsonValue::Object(body_obj))
                .unwrap_or_else(|_| "{}".into());
            let req = crate::tls::http_agent().post(&url)
                .set("Authorization", &format!("Bearer {}", spec.pat))
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            match req.send_string(&body) {
                Ok(r) => {
                    // An HTTP 200 does NOT mean the statement finished: with
                    // on_wait_timeout=CONTINUE, Databricks returns the envelope
                    // with status.state = PENDING/RUNNING (poll required) or
                    // even FAILED. Inspect the state before counting the batch,
                    // mirroring run_databricks_source, so we don't report a
                    // still-running or failed write as inserted.
                    let env: JsonValue = r
                        .into_string()
                        .ok()
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or(JsonValue::Null);
                    let state = env
                        .pointer("/status/state")
                        .and_then(|v| v.as_str())
                        .unwrap_or("SUCCEEDED");
                    match state {
                        "SUCCEEDED" => {}
                        "PENDING" | "RUNNING" => {
                            let statement_id = env
                                .get("statement_id")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    EngineError::Query(
                                        "Databricks async write response missing statement_id"
                                            .into(),
                                    )
                                })?;
                            let poll_url = format!("{}{}", url, statement_id);
                            poll_databricks_until_done(
                                &poll_url,
                                &format!("Bearer {}", spec.pat),
                            )?;
                        }
                        other => {
                            let err = env
                                .pointer("/status/error/message")
                                .and_then(|v| v.as_str())
                                .unwrap_or("(no message)");
                            return Err(EngineError::Query(format!(
                                "Databricks write statement state {}: {}",
                                other, err
                            )));
                        }
                    }
                    total_inserted += chunk.len();
                }
                Err(ureq::Error::Status(code, response)) => {
                    let body = response.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "Databricks HTTP {} from {}: {}",
                        code,
                        url,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "Databricks HTTP transport to {}: {}",
                        url, e
                    )));
                }
            }
        }
        Ok(format!(
            "databricks: inserted {} rows into {}",
            total_inserted, spec.table
        ))
    }

    /// Full-Text Search runs in two CLI invocations sharing the same
    /// temp DB file. The first stages the upstream into a permanent
    /// table; the second builds the BM25 index and the final node
    /// table. The split is needed for DuckDB v1.5+ where the fts
    /// PRAGMA can't see tables created in the same -c invocation; on
    /// v1.4 it just costs one extra CLI spawn.
    pub(crate) fn run_text_search(
        &self,
        db: &Path,
        secret_prefix: &str,
        node_id: &str,
        spec: &plan::TextSearchSpec,
    ) -> Result<String, EngineError> {
        let staging = plan::quote_ident(&spec.staging_table);
        let upstream = plan::quote_ident(&spec.from_view);
        let node_q = plan::quote_ident(node_id);
        let id_col_q = plan::quote_ident(&spec.id_col);
        let output_q = plan::quote_ident(&spec.output_col);

        // Phase 1: stage upstream into a named table that the next CLI
        // invocation will see.
        let stage_sql = format!(
            "{secret}INSTALL fts; LOAD fts; \
             DROP TABLE IF EXISTS {staging}; \
             CREATE TABLE {staging} AS SELECT * FROM {upstream};",
            secret = secret_prefix,
            staging = staging,
            upstream = upstream,
        );
        self.run(Some(db), &stage_sql, false)?;

        // Phase 2: PRAGMA create_fts_index sees the staged table from
        // disk; the same invocation then runs the BM25 SELECT.
        let text_args = spec
            .text_cols
            .iter()
            .map(|c| format!("'{}'", c.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        let index_schema = format!("fts_main_{}", spec.staging_table);
        let match_expr = format!(
            "{}.match_bm25({}, '{}')",
            index_schema,
            id_col_q,
            spec.query.replace('\'', "''")
        );
        let order_limit = match spec.top_k {
            Some(k) => format!(" ORDER BY {} DESC LIMIT {}", output_q, k),
            None => String::new(),
        };
        let index_sql = format!(
            "{secret}INSTALL fts; LOAD fts; \
             PRAGMA create_fts_index('{staging_raw}', '{id_col}', {text_args}); \
             CREATE OR REPLACE TABLE {node} AS \
               SELECT *, {match_expr} AS {output_q} FROM {staging} \
               WHERE {match_expr} IS NOT NULL{order_limit};",
            secret = secret_prefix,
            staging_raw = spec.staging_table.replace('\'', "''"),
            id_col = spec.id_col.replace('\'', "''"),
            text_args = text_args,
            node = node_q,
            match_expr = match_expr,
            output_q = output_q,
            staging = staging,
            order_limit = order_limit,
        );
        self.run(Some(db), &index_sql, false)
    }
}

/// Resolve a child-pipeline reference (Run Job / Iterate / Foreach / Try)
/// to a file path the engine can read. An explicit path - absolute, or
/// containing a separator, or ending in `.json` - is used verbatim. A bare
/// workspace pipeline id is looked up under `$DUCKLE_WORKSPACE/pipelines/`,
/// matching how the desktop stores pipelines. This is the single resolution
/// point that makes id references work for every run mode: interactive runs
/// pre-resolve in the frontend (and arrive here as a real path, untouched),
/// while headless runs (scheduler, file-watch) carry the bare id and resolve
/// here. A bare id that doesn't resolve is returned as-is so the caller's
/// open error names the original reference.
/// State file for an xf.incremental node:
/// `$DUCKLE_WORKSPACE/state/<pipeline>/<node>.json`. None when there's no
/// workspace (then the mark can't persist and every run loads from the
/// configured initial value, which is safe - just not incremental).
/// Scaffold an ephemeral one-model dbt project for xf.dbt inline mode. Writes
/// `dbt_project.yml` (profile `duckle`, matching the generated profiles.yml) and
/// `models/<model_name>.sql` holding the user's inline SQL (which may reference
/// `{{ var('duckle_input') }}` for the upstream table). Returns the temp project
/// dir. The model name is sanitized to a SQL/dbt-safe identifier.
/// Write `content` to `path` only if it differs from what's already there.
/// Preserves file mtime when unchanged, which keeps dbt's partial-parse cache
/// valid across runs.
fn write_str_if_changed(path: &Path, content: &str) -> std::io::Result<()> {
    if let Ok(existing) = std::fs::read_to_string(path) {
        if existing == content {
            return Ok(());
        }
    }
    std::fs::write(path, content)
}

fn scaffold_inline_dbt_project(
    node_id: &str,
    model_name: &str,
    model_sql: &str,
) -> std::io::Result<std::path::PathBuf> {
    // Same rule the planner uses for output_model (plan::sanitize_dbt_model_name)
    // so the table written here and the name the engine reads back agree.
    let safe_model: String = plan::sanitize_dbt_model_name(model_name);
    // Stable per-node project dir (NOT process-id keyed) so dbt's
    // target/partial_parse.msgpack survives across app launches. dbt-core's
    // parse is the dominant cost of an inline run; a warm partial-parse cache
    // shaves ~1s off an otherwise-cold start.
    let root = std::env::temp_dir().join(format!(
        "duckle_dbt_proj_{}",
        node_id.replace(|c: char| !c.is_alphanumeric(), "_")
    ));
    let models = root.join("models");
    std::fs::create_dir_all(&models)?;
    // Drop any stale model left by a previous run (e.g. the model was renamed),
    // so the project only ever contains the current inline model.
    if let Ok(entries) = std::fs::read_dir(&models) {
        for e in entries.flatten() {
            let p = e.path();
            if p.extension().and_then(|x| x.to_str()) == Some("sql")
                && p.file_stem().and_then(|x| x.to_str()) != Some(safe_model.as_str())
            {
                let _ = std::fs::remove_file(p);
            }
        }
    }
    let project_yml = "name: duckle\nversion: '1.0.0'\nprofile: duckle\nconfig-version: 2\nmodel-paths: [\"models\"]\nmodels:\n  duckle:\n    +materialized: table\n";
    // Write only when content differs: a touched dbt_project.yml forces dbt to
    // discard the partial-parse cache, and a re-touched model file needlessly
    // re-parses it. Identical content keeps the whole cache valid.
    write_str_if_changed(&root.join("dbt_project.yml"), project_yml)?;
    write_str_if_changed(&models.join(format!("{}.sql", safe_model)), model_sql)?;
    Ok(root)
}

/// Resolve the dbt executable. Order: explicit `dbtBin` prop -> DUCKLE_DBT_BIN
/// env -> a bundled dbt/Fusion binary next to the running executable (the
/// shipped sidecar) -> `dbt` on PATH. The bundled binary makes xf.dbt work
/// out of the box without a Python install.
fn resolve_dbt_bin(explicit: Option<&str>) -> String {
    if let Some(b) = explicit.filter(|s| !s.trim().is_empty()) {
        return b.to_string();
    }
    if let Ok(env) = std::env::var("DUCKLE_DBT_BIN") {
        if !env.is_empty() && Path::new(&env).exists() {
            return env;
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            // Names we may ship the bundled dbt under (Fusion or frozen dbt).
            for name in [
                "dbt-fusion",
                "dbt-fusion.exe",
                "dbtf",
                "dbtf.exe",
                "dbt",
                "dbt.exe",
            ] {
                let p = dir.join(name);
                if p.exists() {
                    return p.to_string_lossy().into_owned();
                }
            }
        }
    }
    "dbt".to_string()
}

/// Last `max` characters of `s` (UTF-8-safe) - used to keep the useful end
/// of a long tool log (dbt prints the failing model last) in error messages.
fn tail_chars(s: &str, max: usize) -> &str {
    let count = s.chars().count();
    if count <= max {
        return s;
    }
    let skip = count - max;
    let (idx, _) = s.char_indices().nth(skip).unwrap_or((0, ' '));
    &s[idx..]
}

fn incremental_state_path(pipeline_name: Option<&str>, node_id: &str) -> Option<std::path::PathBuf> {
    let ws = std::env::var("DUCKLE_WORKSPACE").ok().filter(|s| !s.is_empty())?;
    let folder = sanitize_path_segment(pipeline_name.unwrap_or("pipeline"));
    let file = format!("{}.json", sanitize_path_segment(node_id));
    Some(
        std::path::Path::new(&ws)
            .join("state")
            .join(folder)
            .join(file),
    )
}

/// Read a saved watermark as (value, type). Missing / unreadable / malformed
/// state reads as "no mark yet".
fn read_incremental_state(path: &std::path::PathBuf) -> Option<(String, String)> {
    let text = std::fs::read_to_string(path).ok()?;
    let v: JsonValue = serde_json::from_str(&text).ok()?;
    let value = v.get("value").and_then(|x| x.as_str())?.to_string();
    let ty = v
        .get("type")
        .and_then(|x| x.as_str())
        .unwrap_or("VARCHAR")
        .to_string();
    Some((value, ty))
}

/// Read a saved DuckLake snapshot id from CDC state. Missing / unreadable
/// reads as "no prior snapshot".
fn read_snapshot_state(path: &std::path::PathBuf) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    let v: JsonValue = serde_json::from_str(&text).ok()?;
    v.get("snapshot_id")
        .and_then(|x| x.as_u64().or_else(|| x.as_str().and_then(|s| s.parse::<u64>().ok())))
}

/// Keep a DuckDB type name safe to splice into a CAST. typeof() output is
/// engine-controlled, but we still strip anything outside the characters a
/// type name uses (e.g. `DECIMAL(18,3)`, `TIMESTAMP WITH TIME ZONE`).
fn sanitize_sql_type(ty: &str) -> String {
    let cleaned: String = ty
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '(' | ')' | ','))
        .collect();
    let cleaned = cleaned.trim().to_string();
    if cleaned.is_empty() {
        "VARCHAR".to_string()
    } else {
        cleaned
    }
}

/// Filesystem-safe single path segment (mirrors the run-log folder rule).
fn sanitize_path_segment(name: &str) -> String {
    let cleaned: String = name
        .trim()
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || matches!(c, ' ' | '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let cleaned = cleaned.trim().trim_matches('.').trim();
    if cleaned.is_empty() {
        "pipeline".to_string()
    } else {
        cleaned.to_string()
    }
}

/// The Snowflake SQL API (and the local emulator) can return HTTP 200 with a
/// SQL error in the body (a `message` plus a non-success `sqlState`). Detect
/// that so a failed statement fails the run instead of silently succeeding.
/// Returns Some(error) when the body indicates a SQL error, None on success.
fn snowflake_body_error(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let sql_state = v.get("sqlState").and_then(|s| s.as_str()).unwrap_or("");
    let msg = v.get("message").and_then(|m| m.as_str()).unwrap_or("");
    if !msg.is_empty() && !sql_state.is_empty() && sql_state != "00000" {
        Some(format!("{} (sqlState {})", msg.chars().take(300).collect::<String>(), sql_state))
    } else {
        None
    }
}

/// Build the SELECT expression that casts a Snowflake SQL-API cell (always a
/// VARCHAR after read_json) to its real DuckDB type, per the `jsonv2` encoding
/// (Snowflake "Handling responses" docs). `ident` is the already-quoted column
/// reference; `sf_type` is the lowercased rowType `type`. Temporal columns are
/// epoch-based numeric strings, so they must be converted, not parsed as
/// literals (GitHub #24). Unknown / text / semi-structured types stay VARCHAR.
fn snowflake_cast_expr(ident: &str, sf_type: &str, scale: i64, precision: i64) -> String {
    match sf_type {
        // NUMBER(p,s): decimal string. Scale 0 -> integer (BIGINT, or HUGEINT
        // when the precision can exceed i64); otherwise DECIMAL(p,s) clamped to
        // DuckDB's max precision of 38.
        "fixed" => {
            if scale > 0 {
                let p = precision.clamp(1, 38);
                let s = scale.clamp(0, p);
                format!("CAST({ident} AS DECIMAL({p},{s}))")
            } else if (1..=18).contains(&precision) {
                format!("CAST({ident} AS BIGINT)")
            } else {
                format!("CAST({ident} AS HUGEINT)")
            }
        }
        "real" => format!("CAST({ident} AS DOUBLE)"),
        "boolean" => format!("CAST({ident} AS BOOLEAN)"),
        // DATE: integer string = days since the Unix epoch.
        "date" => format!("(DATE '1970-01-01' + CAST({ident} AS INTEGER))"),
        // TIME: float string = seconds since midnight. make_timestamp builds a
        // naive timestamp from microseconds; the TIME cast keeps the time part.
        "time" => format!(
            "CAST(make_timestamp(CAST(round(CAST({ident} AS DOUBLE) * 1000000) AS BIGINT)) AS TIME)"
        ),
        // TIMESTAMP_NTZ: float seconds since epoch, wall-clock (no zone).
        "timestamp_ntz" => format!(
            "make_timestamp(CAST(round(CAST({ident} AS DOUBLE) * 1000000) AS BIGINT))"
        ),
        // TIMESTAMP_LTZ: float seconds since epoch = a UTC instant.
        "timestamp_ltz" => format!("to_timestamp(CAST({ident} AS DOUBLE))"),
        // TIMESTAMP_TZ: "<seconds.frac> <offset>"; the seconds part is the UTC
        // instant (the trailing offset is display-only). Take the instant.
        "timestamp_tz" => {
            format!("to_timestamp(CAST(split_part({ident}, ' ', 1) AS DOUBLE))")
        }
        // BINARY: hexadecimal string.
        "binary" => format!("unhex({ident})"),
        // text, variant, object, array, and anything unrecognized stay VARCHAR
        // (semi-structured values are returned as their JSON text).
        _ => ident.to_string(),
    }
}

/// Load context variables for a workspace: read `repository.json`, and for each
/// `type:"context"` item read `contexts/<id>.json` and expose its variables as
/// both `key` and `<contextName>.key`. Mirrors the frontend's buildContextVars
/// so a sub-pipeline read raw from disk resolves the same `${...}` references
/// the top-level pipeline does (the parent arrives pre-resolved, a foreach /
/// runjob child does not). Also exposes the `${workspace}` / `${projectroot}`
/// builtins. Best-effort: any missing or unparseable file is skipped.
pub(crate) fn context_vars_for_workspace(ws: &Path) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    let root = ws.to_string_lossy().replace('\\', "/");
    out.insert("workspace".to_string(), root.clone());
    out.insert("projectroot".to_string(), root);
    // Dynamic date/time builtins so foreach / runjob children resolve
    // ${date}/${datetime}/... in their paths just like the top-level run.
    crate::context::insert_time_builtins(&mut out);
    let repo: serde_json::Value = std::fs::read_to_string(ws.join("repository.json"))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
    for it in repo.as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
        if it.get("type").and_then(|v| v.as_str()) != Some("context") {
            continue;
        }
        let id = match it.get("id").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let name = it.get("name").and_then(|v| v.as_str()).unwrap_or(id);
        let payload: serde_json::Value = match std::fs::read_to_string(
            ws.join("contexts").join(format!("{}.json", id)),
        )
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        {
            Some(v) => v,
            None => continue,
        };
        if let Some(vars) = payload.get("variables").and_then(|v| v.as_array()) {
            for v in vars {
                if let (Some(k), Some(val)) = (
                    v.get("key").and_then(|x| x.as_str()),
                    v.get("value").and_then(|x| x.as_str()),
                ) {
                    out.insert(k.to_string(), val.to_string());
                    out.insert(format!("{}.{}", name, k), val.to_string());
                }
            }
        }
    }
    // Global context file: workspace-configured key/value file, applied last so
    // these runtime values override the static context defaults.
    for (k, v) in crate::context::context_file_vars(ws) {
        out.insert(k, v);
    }
    out
}

/// Context vars for the active workspace (`$DUCKLE_WORKSPACE`); empty if unset.
fn workspace_context_vars() -> std::collections::HashMap<String, String> {
    match std::env::var("DUCKLE_WORKSPACE") {
        Ok(w) if !w.is_empty() => context_vars_for_workspace(Path::new(&w)),
        _ => std::collections::HashMap::new(),
    }
}

fn resolve_subpipeline_ref(reference: &str) -> String {
    let looks_like_path =
        reference.contains('/') || reference.contains('\\') || reference.ends_with(".json");
    if looks_like_path {
        return reference.to_string();
    }
    if let Ok(ws) = std::env::var("DUCKLE_WORKSPACE") {
        if !ws.is_empty() {
            let candidate = std::path::Path::new(&ws)
                .join("pipelines")
                .join(format!("{}.json", reference));
            if candidate.exists() {
                return candidate.display().to_string();
            }
        }
    }
    reference.to_string()
}

/// Coerce a column name into a legal XML element name: the first char must be a
/// letter or `_`, the rest letters/digits/`-`/`.`/`_`. Illegal chars become `_`
/// and a non-letter first char is prefixed with `_`. The original name is kept
/// as a `name` attribute by the caller so the value still round-trips.
fn xml_safe_element_name(name: &str) -> String {
    let mut out = String::new();
    for (i, ch) in name.chars().enumerate() {
        let ok = ch.is_ascii_alphabetic()
            || ch == '_'
            || (i > 0 && (ch.is_ascii_digit() || ch == '-' || ch == '.'));
        out.push(if ok { ch } else { '_' });
    }
    if out.is_empty() {
        out.push('_');
    }
    let first = out.chars().next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        out.insert(0, '_');
    }
    out
}

/// Escape a raw value for embedding inside single quotes in a JsonNative
/// (Snowflake / Databricks) string literal: double backslashes (these engines
/// treat backslash as a string-literal escape char) then double single quotes.
/// Matches `sql_literal`'s JsonNative quoting so a hand-built predicate literal
/// resolves to the same runtime value as a projected source column.
fn jsonnative_quote_inner(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

/// CDC delete-flag match for the Mongo sink. The flag column can arrive as a
/// BSON string, bool, or number: DuckDB `-json` serializes BOOLEAN/INTEGER as
/// native JSON, so `bson::to_document` yields Bool/Int32/Int64/Double, not
/// String. Compare by stringifying so a boolean or numeric delete column
/// matches `delete_value` the same way the SQL sinks' `flag = 'value'`
/// coercion does, instead of silently never matching (which turned an intended
/// delete into an upsert).
fn bson_flag_matches(b: Option<&mongodb::bson::Bson>, target: &str) -> bool {
    use mongodb::bson::Bson;
    // Compare numeric flag columns numerically so both "1" and "1.0" match a
    // Double(1.0) - Rust's f64 Display strips the trailing zero, so a plain
    // to_string() compare would miss "1.0". This matches the SQL sinks'
    // implicit `flag = 'value'` cast (where '1' and '1.0' both equal 1.0).
    let num_eq = |v: f64| target.parse::<f64>().map(|t| t == v).unwrap_or(false);
    match b {
        Some(Bson::String(s)) => s == target,
        Some(Bson::Boolean(v)) => v.to_string() == target,
        Some(Bson::Int32(v)) => num_eq(*v as f64),
        Some(Bson::Int64(v)) => num_eq(*v as f64),
        Some(Bson::Double(v)) => num_eq(*v),
        _ => false,
    }
}

/// SFTP (SSH File Transfer Protocol) detection. SFTP is a different protocol
/// from FTP / FTPS and is not handled by src.ftp (suppaftp). Catch the common
/// targeting mistakes - the SSH port (22) or an sftp:// / ssh:// scheme on the
/// host - so the user gets a clear error instead of suppaftp's cryptic
/// "Response contains an invalid syntax" from reading an SSH banner (#16).
pub(crate) fn is_sftp_target(host: &str, port: u16) -> bool {
    let h = host.trim().to_ascii_lowercase();
    port == 22 || h.starts_with("sftp://") || h.starts_with("ssh://")
}

#[cfg(test)]
mod ftp_tests {
    use super::is_sftp_target;

    #[test]
    fn detects_sftp_targets_only() {
        // SFTP targets: the SSH port, or an explicit sftp/ssh scheme.
        assert!(is_sftp_target("files.example.com", 22));
        assert!(is_sftp_target("sftp://files.example.com", 2222));
        assert!(is_sftp_target("SSH://Host", 21));
        // Genuine FTP / FTPS targets are not flagged.
        assert!(!is_sftp_target("files.example.com", 21));
        assert!(!is_sftp_target("ftp://files.example.com", 21));
        assert!(!is_sftp_target("ftps://files.example.com", 990));
    }
}

#[cfg(test)]
mod connector_helper_tests {
    use super::{bson_flag_matches, jsonnative_quote_inner};
    use mongodb::bson::Bson;

    #[test]
    fn jsonnative_quoting_doubles_backslash_and_quote() {
        // Snowflake / Databricks treat backslash as a literal escape char, so
        // a delete_value with a backslash must be doubled to round-trip.
        assert_eq!(jsonnative_quote_inner("a\\b"), "a\\\\b");
        assert_eq!(jsonnative_quote_inner("o'reilly"), "o''reilly");
        assert_eq!(jsonnative_quote_inner("C:\\path\\x"), "C:\\\\path\\\\x");
        assert_eq!(jsonnative_quote_inner("delete"), "delete");
    }

    #[test]
    fn mongo_delete_flag_matches_non_string_bson() {
        // The flag column can be a native bool/number, not just a string.
        assert!(bson_flag_matches(Some(&Bson::String("delete".into())), "delete"));
        assert!(bson_flag_matches(Some(&Bson::Boolean(true)), "true"));
        assert!(bson_flag_matches(Some(&Bson::Int32(1)), "1"));
        assert!(bson_flag_matches(Some(&Bson::Int64(1)), "1"));
        assert!(bson_flag_matches(Some(&Bson::Double(1.0)), "1"));
        // A DOUBLE flag reads as "1.0" in the JSON preview; both forms match.
        assert!(bson_flag_matches(Some(&Bson::Double(1.0)), "1.0"));
        assert!(bson_flag_matches(Some(&Bson::Int64(1)), "1.0"));
        assert!(bson_flag_matches(Some(&Bson::Double(1.5)), "1.5"));
        // Non-matches and absent column.
        assert!(!bson_flag_matches(Some(&Bson::Boolean(false)), "true"));
        assert!(!bson_flag_matches(Some(&Bson::String("keep".into())), "delete"));
        assert!(!bson_flag_matches(None, "delete"));
    }
}

#[cfg(test)]
mod context_var_tests {
    use super::context_vars_for_workspace;

    #[test]
    fn loads_workspace_context_vars_for_sub_pipelines() {
        // A foreach / runjob child is read raw from disk, so its ${...} context
        // placeholders must resolve from the workspace's contexts the same way
        // the top-level pipeline does (a literal ${MOTHERDUCK_TOKEN} reaching
        // MotherDuck fails as an invalid JWT).
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        std::fs::write(
            ws.join("repository.json"),
            r#"[{"id":"md_secrets","name":"MotherDuck","type":"context","parentId":"contexts"}]"#,
        )
        .unwrap();
        std::fs::create_dir_all(ws.join("contexts")).unwrap();
        std::fs::write(
            ws.join("contexts").join("md_secrets.json"),
            r#"{"variables":[{"key":"MOTHERDUCK_TOKEN","value":"tok-123","secret":true}]}"#,
        )
        .unwrap();

        let vars = context_vars_for_workspace(ws);
        // Both the bare key and the context-namespaced key resolve.
        assert_eq!(vars.get("MOTHERDUCK_TOKEN").map(String::as_str), Some("tok-123"));
        assert_eq!(vars.get("MotherDuck.MOTHERDUCK_TOKEN").map(String::as_str), Some("tok-123"));
        // Built-in workspace placeholder is exposed too.
        assert!(vars.contains_key("workspace"));
    }

    #[test]
    fn missing_workspace_files_yield_only_builtins() {
        let dir = tempfile::tempdir().unwrap();
        let vars = context_vars_for_workspace(dir.path());
        assert!(vars.contains_key("workspace"));
        assert!(!vars.contains_key("MOTHERDUCK_TOKEN"));
    }
}

/// Render a JSON value as a DuckDB SQL literal for snk.gizmosql INSERTs. The
/// target column type (from DESCRIBE) drives any cast, so numeric-looking
/// strings are quoted safely.
fn gizmo_sql_literal(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}
