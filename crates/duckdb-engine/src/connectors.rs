//! Connector + transform runtime runners (impl DuckdbEngine).
//!
//! Every run_* method that executes a non-SQL source/sink/transform spec, the
//! ctl.* sub-pipeline helpers, and a couple of driver cell-to-JSON converters.
//! Extracted from lib.rs; the core engine (run/run_rows/execute_pipeline/
//! materialize helpers) stays there. self.run / self.bin etc. are reachable
//! because this is a child module of the crate root.

use crate::*;

/// Render one row into a line for a text / raw HTTP body (issue #147),
/// substituting `${column}` placeholders with the row's values. Missing keys and
/// JSON nulls become empty strings; strings are inserted verbatim; other values
/// (numbers, bools, nested) use their compact JSON form. Used for InfluxDB Line
/// Protocol writes (QuestDB /write) and other line-oriented endpoints.
pub(crate) fn render_text_template(template: &str, row: &serde_json::Value) -> String {
    use std::sync::OnceLock;
    static RE: OnceLock<Result<regex::Regex, regex::Error>> = OnceLock::new();
    let re = match RE.get_or_init(|| regex::Regex::new(r"\$\{([^}]+)\}")) {
        Ok(re) => re,
        Err(_) => return template.to_string(),
    };
    let obj = row.as_object();
    re.replace_all(template, |caps: &regex::Captures| {
        let key = caps[1].trim();
        match obj.and_then(|o| o.get(key)) {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        }
    })
    .into_owned()
}

/// Mint a fresh Salesforce access token via the OAuth 2.0 client-credentials
/// grant (#166). POSTs a form-encoded `grant_type=client_credentials` (plus the
/// connected-app client id/secret) to `{login_url}/services/oauth2/token` and
/// returns `(access_token, instance_url)` from the JSON response. A fresh
/// short-lived token per run replaces the pre-minted ~2h Bearer token users
/// otherwise re-paste, and because source and sink each mint from their own
/// connection, org-to-org migration (read Org A, write Org B) works out of the
/// box.
/// #195 generalizes this beyond Salesforce: the endpoint comes from the spec's
/// `token_url` and credentials go either in the POST body (Salesforce, the
/// default) or as an HTTP Basic header (Xero). For Salesforce the request is
/// unchanged: the same three form fields in the same order to the same URL.
pub(crate) fn mint_oauth_token(o: &plan::RestOAuth) -> Result<(String, String), EngineError> {
    let url = o.token_url.trim_end_matches('/').to_string();
    let mut req = crate::tls::http_agent()
        .post(&url)
        .set("Accept", "application/json");
    let mut form: Vec<(&str, &str)> = vec![("grant_type", "client_credentials")];
    match o.client_auth {
        plan::OAuthClientAuth::Body => {
            form.push(("client_id", &o.client_id));
            form.push(("client_secret", &o.client_secret));
        }
        plan::OAuthClientAuth::Basic => {
            use base64::engine::general_purpose::STANDARD as B64;
            use base64::Engine as _;
            let creds = B64.encode(format!("{}:{}", o.client_id, o.client_secret));
            req = req.set("Authorization", &format!("Basic {}", creds));
        }
    }
    if let Some(s) = &o.scope {
        form.push(("scope", s));
    }
    let resp = req.send_form(&form);
    let txt = match resp {
        Ok(r) => r.into_string().unwrap_or_default(),
        Err(ureq::Error::Status(code, r)) => {
            let b = r.into_string().unwrap_or_default();
            return Err(EngineError::Query(format!(
                "OAuth: token endpoint HTTP {} from {}: {}",
                code,
                url,
                b.chars().take(300).collect::<String>()
            )));
        }
        Err(e) => {
            return Err(EngineError::Query(format!(
                "OAuth: token endpoint transport to {}: {}",
                url, e
            )));
        }
    };
    let v: JsonValue = serde_json::from_str(&txt).map_err(|e| {
        EngineError::Query(format!(
            "OAuth: token endpoint returned non-JSON ({}): {}",
            e,
            txt.chars().take(200).collect::<String>()
        ))
    })?;
    let access = v
        .get("access_token")
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string();
    if access.is_empty() {
        return Err(EngineError::Query(format!(
            "OAuth: token endpoint response missing access_token: {}",
            txt.chars().take(200).collect::<String>()
        )));
    }
    let instance = v
        .get("instance_url")
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_string();
    Ok((access, instance))
}

/// Counts accumulated across DHIS2 import chunks. DHIS2 reports these under
/// two different key sets depending on the endpoint (`importCount` vs
/// `stats`), so both parsers normalise into this one shape.
#[derive(Default, Debug, PartialEq)]
pub(crate) struct Dhis2Counts {
    pub imported: i64,
    pub updated: i64,
    pub deleted: i64,
    pub ignored: i64,
}

impl Dhis2Counts {
    fn add(&mut self, o: &Dhis2Counts) {
        self.imported += o.imported;
        self.updated += o.updated;
        self.deleted += o.deleted;
        self.ignored += o.ignored;
    }
}

fn dhis2_i64(v: &JsonValue, key: &str) -> i64 {
    v.get(key).and_then(|x| x.as_i64()).unwrap_or(0)
}

/// Parse an aggregate `POST /api/dataValueSets` response.
///
/// The synchronous handler wraps the ImportSummary, so counts live at
/// `response.importCount` and conflicts at `response.conflicts`. Some
/// deployments return the bare ImportSummary, so both layouts are accepted.
///
/// The trap: an ImportConflict's human-readable text serialises under the key
/// `value`, NOT `message`. The Java field is named `message` but carries
/// `@JsonProperty("value")`, so a client reading `message` silently gets
/// nothing and every conflict looks blank.
pub(crate) fn parse_dhis2_import_summary(root: &JsonValue) -> (Dhis2Counts, Vec<String>) {
    let body = root.get("response").unwrap_or(root);
    let null = JsonValue::Null;
    let ic = body.get("importCount").unwrap_or(&null);
    let counts = Dhis2Counts {
        imported: dhis2_i64(ic, "imported"),
        updated: dhis2_i64(ic, "updated"),
        deleted: dhis2_i64(ic, "deleted"),
        ignored: dhis2_i64(ic, "ignored"),
    };
    let mut msgs = Vec::new();
    if let Some(arr) = body.get("conflicts").and_then(|c| c.as_array()) {
        for c in arr {
            let text = c
                .get("value")
                .and_then(|v| v.as_str())
                .unwrap_or("(no conflict text)");
            match c.get("object").and_then(|v| v.as_str()) {
                Some(obj) if !obj.is_empty() => msgs.push(format!("{}: {}", obj, text)),
                _ => msgs.push(text.to_string()),
            }
        }
    }
    // status ERROR with an empty conflicts array still has to surface, or a
    // failed import is reported as a clean run.
    let status = body
        .get("status")
        .or_else(|| root.get("status"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if status.eq_ignore_ascii_case("ERROR") && msgs.is_empty() {
        let d = body
            .get("description")
            .and_then(|v| v.as_str())
            .or_else(|| root.get("message").and_then(|v| v.as_str()))
            .unwrap_or("import reported status ERROR");
        msgs.push(d.to_string());
    }
    (counts, msgs)
}

/// Parse a `POST /api/tracker` ImportReport.
///
/// Nothing is shared with the aggregate shape: counts sit under `stats` with
/// different key names (`created`, not `imported`), and the error text is under
/// `message` here, the exact opposite of the aggregate `value`. One parser for
/// both would silently report zeroes and no errors.
pub(crate) fn parse_dhis2_tracker_report(root: &JsonValue) -> (Dhis2Counts, Vec<String>) {
    let null = JsonValue::Null;
    let stats = root
        .get("stats")
        .or_else(|| root.get("response").and_then(|r| r.get("stats")))
        .unwrap_or(&null);
    let counts = Dhis2Counts {
        imported: dhis2_i64(stats, "created"),
        updated: dhis2_i64(stats, "updated"),
        deleted: dhis2_i64(stats, "deleted"),
        ignored: dhis2_i64(stats, "ignored"),
    };
    let mut msgs = Vec::new();
    let reports = root
        .get("validationReport")
        .or_else(|| root.get("response").and_then(|r| r.get("validationReport")));
    if let Some(errs) = reports
        .and_then(|v| v.get("errorReports"))
        .and_then(|v| v.as_array())
    {
        for e in errs {
            let text = e
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("(no error text)");
            match e.get("errorCode").and_then(|v| v.as_str()) {
                Some(code) if !code.is_empty() => msgs.push(format!("{} {}", code, text)),
                _ => msgs.push(text.to_string()),
            }
        }
    }
    let status = root.get("status").and_then(|v| v.as_str()).unwrap_or("");
    if status.eq_ignore_ascii_case("ERROR") && msgs.is_empty() {
        msgs.push(
            root.get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("tracker import reported status ERROR")
                .to_string(),
        );
    }
    (counts, msgs)
}

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

    /// snk.execsource "Execute in Source" (#115): run each statement (a
    /// CREATE TABLE ... AS SELECT, optionally preceded by DROP) directly on the
    /// attached remote server through the extension passthrough
    /// (postgres_execute / mysql_execute). No DuckDB round-trip: the SELECT runs
    /// in the source and the result lands in a table there. One CALL per
    /// statement because the mysql extension rejects multi-statement batches.
    pub(crate) fn run_remote_exec(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &plan::RemoteExecSpec,
    ) -> Result<String, EngineError> {
        let mut n = 0usize;
        for stmt in &spec.statements {
            let exec_sql = format!(
                "{secret}{attach}CALL {fn_name}('duckle_dst', '{sql}');",
                secret = secret_prefix,
                attach = spec.attach,
                fn_name = spec.exec_fn,
                sql = stmt.replace('\'', "''")
            );
            self.run(Some(db), &exec_sql, false)?;
            n += 1;
        }
        Ok(format!("executed {} statement(s) on the source server", n))
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
        // When the user declares a form Content-Type header, encode each
        // row as application/x-www-form-urlencoded instead of JSON, so
        // snk.rest can POST to form-native APIs (Stripe, OAuth token
        // endpoints, legacy webhooks). Nested values are JSON-stringified;
        // nulls become empty strings.
        fn percent_encode_form(s: &str) -> String {
            let mut out = String::with_capacity(s.len());
            for b in s.bytes() {
                match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                        out.push(b as char)
                    }
                    b' ' => out.push_str("%20"),
                    _ => out.push_str(&format!("%{:02X}", b)),
                }
            }
            out
        }
        fn form_encode_row(row: &serde_json::Value) -> String {
            let obj = match row.as_object() {
                Some(o) => o,
                None => return String::new(),
            };
            obj.iter()
                .map(|(k, v)| {
                    let val = match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Null => String::new(),
                        other => other.to_string(),
                    };
                    format!("{}={}", percent_encode_form(k), percent_encode_form(&val))
                })
                .collect::<Vec<_>>()
                .join("&")
        }
        let form_encoded = spec.headers.iter().any(|(k, v)| {
            k.eq_ignore_ascii_case("content-type")
                && v.to_ascii_lowercase().contains("x-www-form-urlencoded")
        });
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
            "text" => {
                // #147: render each row through the template (${column}
                // placeholders) and newline-join into one raw body. Sent as
                // text/plain unless the user set a Content-Type header (the
                // dispatch closure lets a user header win). This is the shape
                // InfluxDB Line Protocol endpoints (QuestDB /write) expect.
                let template = spec.text_template.as_deref().unwrap_or("");
                let mut body = String::new();
                for (i, row) in rows.iter().enumerate() {
                    if i > 0 {
                        body.push('\n');
                    }
                    body.push_str(&render_text_template(template, row));
                }
                dispatch(body, "text/plain")?;
                Ok(format!("sent {} rows to {}", rows.len(), spec.url))
            }
            _ => {
                let mut sent = 0_usize;
                for row in &rows {
                    let (body, ct) = if form_encoded {
                        (form_encode_row(row), "application/x-www-form-urlencoded")
                    } else {
                        (
                            serde_json::to_string(row).unwrap_or_else(|_| "{}".into()),
                            "application/json",
                        )
                    };
                    dispatch(body, ct)?;
                    sent += 1;
                }
                Ok(format!("sent {} rows to {}", sent, spec.url))
            }
        }
    }

    /// Salesforce REST write sink (Tier 1: sObject Collections API).
    ///
    /// Reads the upstream view as JSON, chunks rows into <=200-record groups,
    /// and issues one request per chunk against the org's composite/sobjects
    /// endpoint. Auth is a Bearer OAuth access token. The response is an array
    /// of per-record `{id, success, errors}` results; failures are aggregated
    /// and, when `fail_on_error`, surfaced as a single Err. A first-class
    /// reject/error output stream is Tier 2 (see docs/salesforce-sink).
    ///
    /// Endpoints by operation:
    ///   insert  POST   {instance}/services/data/{ver}/composite/sobjects
    ///   update  PATCH  {instance}/services/data/{ver}/composite/sobjects
    ///   upsert  PATCH  {instance}/services/data/{ver}/composite/sobjects/{obj}/{extIdField}
    ///   delete  DELETE {instance}/services/data/{ver}/composite/sobjects?ids=..&allOrNone=..
    /// snk.dhis2: chunked import with real import-summary parsing.
    ///
    /// The parsing is the point. DHIS2 answers HTTP 200 in several situations
    /// where the import did not do what the caller asked:
    ///
    ///  * aggregate conflicts are WARNING on 2.40-2.42 and ERROR on 2.43+, and
    ///    2.43 remapped WARNING to HTTP 200;
    ///  * `importCount.ignored` can be non-zero on a plain 200 OK;
    ///  * synchronous tracker imports return 200 for status WARNING and only
    ///    409 for ERROR.
    ///
    /// Trusting the HTTP status therefore turns a failed import into a green
    /// run, which for reporting data is worse than an outright error.
    pub(crate) fn run_dhis2_sink(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &Dhis2SinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!(
            "{}SELECT * FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("dhis2: 0 rows to {}", spec.url));
        }

        // Query params. async=false on both endpoints is deliberate: /api/tracker
        // defaults to async=true and would return only a job reference, whose
        // outcome has to be polled from a separate endpoint that 404s until the
        // report exists and evicts it after a restart. Chunking keeps each
        // synchronous request small enough that this is the safer trade.
        let mut qs: Vec<(&str, String)> = vec![
            ("importStrategy", spec.import_strategy.clone()),
            ("async", "false".into()),
        ];
        if spec.import_type == "tracker" {
            qs.push(("atomicMode", spec.atomic_mode.clone()));
            if spec.dry_run {
                qs.push(("importMode", "VALIDATE".into()));
            }
        } else if spec.dry_run {
            qs.push(("dryRun", "true".into()));
        }
        let sep = if spec.url.contains('?') { '&' } else { '?' };
        let url = format!(
            "{}{}{}",
            spec.url,
            sep,
            qs.iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&")
        );

        // Aggregate rows go under "dataValues"; tracker rows go under the
        // collection key matching the resource type. DHIS2 rejects a bare array
        // in both cases.
        let wrapper: &str = if spec.import_type == "tracker" {
            &spec.tracker_resource
        } else {
            "dataValues"
        };

        let mut totals = Dhis2Counts::default();
        let mut problems: Vec<String> = Vec::new();

        for (idx, chunk) in rows.chunks(spec.chunk_size).enumerate() {
            self.check_cancelled()?;
            let mut body = serde_json::Map::new();
            body.insert(wrapper.to_string(), JsonValue::Array(chunk.to_vec()));
            let body_str =
                serde_json::to_string(&JsonValue::Object(body)).unwrap_or_else(|_| "{}".into());

            let mut req = crate::tls::http_agent()
                .post(&url)
                .set("Content-Type", "application/json")
                .set("Accept", "application/json");
            if let Some((name, value)) = &spec.auth_header {
                req = req.set(name, value);
            }
            // A 409 is a real import summary, not a transport failure: DHIS2
            // uses it for ERROR (and for WARNING before 2.43). Parse its body
            // rather than discarding it as an HTTP error.
            let (code, txt) = match req.send_string(&body_str) {
                Ok(resp) => (resp.status(), resp.into_string().unwrap_or_default()),
                Err(ureq::Error::Status(code, response)) => {
                    (code, response.into_string().unwrap_or_default())
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "dhis2: HTTP transport to {} failed on chunk {}: {}",
                        spec.url,
                        idx + 1,
                        e
                    )))
                }
            };
            let parsed: JsonValue = serde_json::from_str(&txt).unwrap_or(JsonValue::Null);
            if parsed.is_null() {
                return Err(EngineError::Query(format!(
                    "dhis2: chunk {} returned HTTP {} with an unparseable body: {}",
                    idx + 1,
                    code,
                    txt.chars().take(300).collect::<String>()
                )));
            }
            let (counts, mut msgs) = if spec.import_type == "tracker" {
                parse_dhis2_tracker_report(&parsed)
            } else {
                parse_dhis2_import_summary(&parsed)
            };
            totals.add(&counts);
            if !msgs.is_empty() {
                for m in msgs.drain(..) {
                    problems.push(format!("chunk {}: {}", idx + 1, m));
                }
            }
        }

        let summary = format!(
            "dhis2: imported {} updated {} deleted {} ignored {} across {} rows{}",
            totals.imported,
            totals.updated,
            totals.deleted,
            totals.ignored,
            rows.len(),
            if spec.dry_run { " (dry run)" } else { "" }
        );

        if problems.is_empty() && totals.ignored == 0 {
            return Ok(summary);
        }
        // Cap the echoed detail: a bad mapping can produce one conflict per row
        // and the error is meant to be readable.
        let shown: Vec<String> = problems.iter().take(10).cloned().collect();
        let more = problems.len().saturating_sub(shown.len());
        let detail = format!(
            "{}{}{}",
            summary,
            if shown.is_empty() {
                String::new()
            } else {
                format!("; {}", shown.join("; "))
            },
            if more > 0 {
                format!(" (+{} more)", more)
            } else {
                String::new()
            }
        );
        if spec.fail_on_conflict {
            Err(EngineError::Query(format!(
                "dhis2 import reported problems: {}",
                detail
            )))
        } else {
            Ok(detail)
        }
    }

    pub(crate) fn run_salesforce_sink(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &SalesforceSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!(
            "{}SELECT * FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("salesforce: 0 rows to {} {}", spec.operation, spec.object));
        }

        // #166: in OAuth client-credentials mode, mint a fresh token per run and
        // prefer the token response's instance_url; otherwise use the static
        // Bearer token + configured instanceUrl.
        let (access_token, instance_url) = match &spec.oauth {
            Some(o) => {
                let (tok, minted_instance) =
                    mint_oauth_token(o)?;
                let instance = if !minted_instance.is_empty() {
                    minted_instance
                } else if !spec.instance_url.is_empty() {
                    spec.instance_url.clone()
                } else {
                    return Err(EngineError::Config(
                        "salesforce: OAuth token response carried no instance_url and no \
                         instanceUrl was configured"
                            .into(),
                    ));
                };
                (tok, instance)
            }
            None => (spec.access_token.clone(), spec.instance_url.clone()),
        };
        let base = format!(
            "{}/services/data/{}/composite/sobjects",
            instance_url.trim_end_matches('/'),
            spec.api_version
        );
        let auth_header = format!("Bearer {}", access_token);
        let all_or_none = spec.all_or_none;

        // Build the (method, url, body) for one chunk of upstream rows.
        // `records` carries the per-record `attributes.type` envelope that the
        // Collections API requires and generic snk.rest cannot emit.
        let build_request = |chunk: &[JsonValue]| -> Result<(String, String, Option<String>), EngineError> {
            match spec.operation.as_str() {
                "delete" => {
                    // DELETE takes ids as a query param, no body.
                    let ids: Vec<String> = chunk
                        .iter()
                        .map(|r| {
                            r.get(&spec.id_field)
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                                .ok_or_else(|| EngineError::Query(format!(
                                    "salesforce delete: row missing id field '{}'", spec.id_field
                                )))
                        })
                        .collect::<Result<_, _>>()?;
                    let url = format!("{}?ids={}&allOrNone={}", base, ids.join(","), all_or_none);
                    Ok(("DELETE".into(), url, None))
                }
                op => {
                    // insert / update / upsert share the records-array body.
                    // sObject Collections update keys each record on `Id`, so a
                    // non-default idField column is mapped onto `Id` here (delete
                    // keys off id_field via the query string; upsert keys off the
                    // external-id field in the URL). Without this, update with a
                    // non-"Id" id column emits records with no Id and Salesforce
                    // rejects every one.
                    let records: Vec<JsonValue> = chunk
                        .iter()
                        .map(|row| {
                            let mut rec = salesforce_record_envelope(row, &spec.object);
                            if op == "update" && spec.id_field != "Id" {
                                if let Some(obj) = rec.as_object_mut() {
                                    match obj.remove(&spec.id_field).filter(|v| !v.is_null()) {
                                        Some(id) => {
                                            obj.insert("Id".into(), id);
                                        }
                                        None => return Err(EngineError::Query(format!(
                                            "salesforce update: row missing id field '{}'",
                                            spec.id_field
                                        ))),
                                    }
                                }
                            }
                            Ok(rec)
                        })
                        .collect::<Result<_, _>>()?;
                    let mut body = serde_json::Map::new();
                    body.insert("allOrNone".into(), JsonValue::Bool(all_or_none));
                    body.insert("records".into(), JsonValue::Array(records));
                    let body_str = serde_json::to_string(&JsonValue::Object(body))
                        .unwrap_or_else(|_| "{}".into());
                    let (method, url) = match op {
                        "insert" => ("POST".to_string(), base.clone()),
                        "update" => ("PATCH".to_string(), base.clone()),
                        "upsert" => {
                            let ext = spec.external_id_field.as_deref().unwrap_or_default();
                            ("PATCH".to_string(), format!("{}/{}/{}", base, spec.object, ext))
                        }
                        other => return Err(EngineError::Query(format!(
                            "salesforce: unsupported operation '{}'", other
                        ))),
                    };
                    Ok((method, url, Some(body_str)))
                }
            }
        };

        // One SfRecordResult per attempted input row, positionally aligned
        // with `rows`. The chunk loop lives in a closure so every exit path
        // (per-record failures, HTTP status, transport error, cancel) funnels
        // through the single results-file-writing point below - resultsPath
        // files must land even when the run aborts (#166).
        let mut record_results: Vec<SfRecordResult> = Vec::with_capacity(rows.len());
        let run_chunks = |record_results: &mut Vec<SfRecordResult>| -> Result<(), EngineError> {
            for chunk in rows.chunks(spec.batch_size) {
                self.check_cancelled()?;
                let (method, url, body) = build_request(chunk)?;
                let req = crate::tls::http_agent()
                    .request(&method, &url)
                    .set("Authorization", &auth_header)
                    .set("Content-Type", "application/json")
                    .set("Accept", "application/json");
                let send = match body {
                    Some(b) => req.send_string(&b),
                    None => req.call(),
                };
                match send {
                    Ok(resp) => {
                        let txt = resp.into_string().unwrap_or_default();
                        record_results.extend(parse_salesforce_results(&txt, chunk.len()));
                    }
                    Err(ureq::Error::Status(code, response)) => {
                        let b = response.into_string().unwrap_or_default();
                        let msg = format!(
                            "Salesforce HTTP {} from {}: {}",
                            code,
                            url,
                            b.chars().take(300).collect::<String>()
                        );
                        // The whole chunk was rejected: give each of its rows
                        // an error-file entry before aborting.
                        for _ in 0..chunk.len() {
                            record_results
                                .push(SfRecordResult::failure(&format!("HTTP_{}", code), msg.clone()));
                        }
                        return Err(EngineError::Query(msg));
                    }
                    Err(e) => {
                        let msg = format!("Salesforce HTTP transport to {}: {}", url, e);
                        for _ in 0..chunk.len() {
                            record_results
                                .push(SfRecordResult::failure("HTTP_TRANSPORT", msg.clone()));
                        }
                        return Err(EngineError::Query(msg));
                    }
                }
            }
            Ok(())
        };
        let loop_result = run_chunks(&mut record_results);

        let ok_count = record_results.iter().filter(|r| r.success).count();
        let fail_count = record_results.len() - ok_count;
        if let Some(dir) = spec.results_path.as_deref() {
            // Stamp the files with the job + run time so repeat runs
            // accumulate side by side (Data Loader parity).
            let stem = format!(
                "{}_{}_{}",
                spec.object,
                spec.operation,
                chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
            );
            let write_result = write_salesforce_results_files(
                std::path::Path::new(dir),
                &stem,
                &rows,
                &record_results,
            );
            // A loop error is the more useful diagnosis, so it wins over a
            // write error; both failing surfaces the loop error below.
            if let Err(e) = write_result {
                if loop_result.is_ok() {
                    return Err(e);
                }
            }
        }
        loop_result?;

        if fail_count > 0 && spec.fail_on_error {
            let first_errors: Vec<String> = record_results
                .iter()
                .filter(|r| !r.success)
                .take(5)
                .map(SfRecordResult::error_line)
                .collect();
            return Err(EngineError::Query(format!(
                "salesforce {} {}: {} succeeded, {} failed. First errors: {}",
                spec.operation, spec.object, ok_count, fail_count, first_errors.join("; ")
            )));
        }
        Ok(format!(
            "salesforce {} {}: {} succeeded, {} failed",
            spec.operation, spec.object, ok_count, fail_count
        ))
    }

    /// snk.salesforce.bulk: write the upstream view into Salesforce via Bulk API
    /// 2.0. DuckDB COPYs the view straight to size-capped CSV parts on disk, and
    /// each part runs the async job lifecycle (create -> upload -> UploadComplete
    /// -> poll -> fetch result sets). Only one <=90 MB part is ever held in
    /// memory, so a multi-GB load never blows the heap the way the Collections
    /// sink's in-memory Vec<JsonValue> would.
    pub(crate) fn run_salesforce_bulk_sink(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &SalesforceBulkSinkSpec,
    ) -> Result<String, EngineError> {
        // Empty input: nothing to load. Match snk.salesforce's message shape.
        let count_sql = format!(
            "{}SELECT count(*) AS c FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let n_rows = self
            .run_rows(Some(db), &count_sql)?
            .first()
            .and_then(|r| r.get("c"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if n_rows == 0 {
            return Ok(format!(
                "salesforce bulk: 0 rows to {} {}",
                spec.operation, spec.object
            ));
        }

        // Same auth resolution as snk.salesforce: mint a fresh token per run in
        // OAuth mode (preferring the token response's instance_url); otherwise
        // the static Bearer token + configured instanceUrl.
        let (access_token, instance_url) = match &spec.oauth {
            Some(o) => {
                let (tok, minted_instance) =
                    mint_oauth_token(o)?;
                let instance = if !minted_instance.is_empty() {
                    minted_instance
                } else if !spec.instance_url.is_empty() {
                    spec.instance_url.clone()
                } else {
                    return Err(EngineError::Config(
                        "salesforce bulk: OAuth token response carried no instance_url and no \
                         instanceUrl was configured"
                            .into(),
                    ));
                };
                (tok, instance)
            }
            None => (spec.access_token.clone(), spec.instance_url.clone()),
        };
        let auth_header = format!("Bearer {}", access_token);
        let ingest_base = format!(
            "{}/services/data/{}/jobs/ingest",
            instance_url.trim_end_matches('/'),
            spec.api_version
        );

        // DuckDB streams the view to size-capped CSV parts on disk - it does the
        // RFC-4180 quoting and the splitting, and FILE_SIZE_BYTES writes numbered
        // files each with their own header row, which is exactly one-part-per-job.
        // pid + a process-local counter, so concurrent Bulk stages (or parallel
        // tests) in one process never target the same directory - DuckDB refuses
        // to COPY into a non-empty one.
        static BULK_DIR_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = BULK_DIR_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let parts_dir = std::env::temp_dir()
            .join(format!("duckle-sfbulk-{}-{}", std::process::id(), seq));
        let _ = std::fs::remove_dir_all(&parts_dir);
        // Removes the temp dir on every exit path (success, error, cancel).
        let _cleanup = ScopedDir(parts_dir.clone());
        // Pre-create the staging dir owner-only so the plaintext CSV parts (the
        // full upstream payload) can't be read by other local users during the
        // upload window on a shared host. DuckDB then COPYs into this empty dir.
        create_private_dir(&parts_dir).map_err(|e| {
            EngineError::Other(format!("salesforce bulk: creating staging dir: {}", e))
        })?;
        // DuckDB accepts forward slashes on every platform; single quotes are the
        // only char it string-escapes.
        let parts_target = sql_escape(&parts_dir.to_string_lossy().replace('\\', "/"));
        // Bulk API 2.0 delete / hardDelete require a CSV of exactly one column
        // named `Id`; extra columns fail the job. Project just the id column
        // (aliased to Id) for those, and every other column for the rest.
        let select_list = if spec.operation == "delete" || spec.operation == "hardDelete" {
            format!("SELECT {} AS \"Id\"", plan::quote_ident(&spec.id_field))
        } else {
            "SELECT *".to_string()
        };
        let copy_sql = format!(
            "{}COPY ({} FROM {}) TO '{}' (FORMAT CSV, HEADER, FILE_SIZE_BYTES {});",
            secret_prefix,
            select_list,
            plan::quote_ident(&spec.from_view),
            parts_target,
            BULK_SPLIT_TARGET_BYTES
        );
        self.run(Some(db), &copy_sql, false)?;

        let mut parts: Vec<std::path::PathBuf> = std::fs::read_dir(&parts_dir)
            .map_err(|e| EngineError::Other(format!("salesforce bulk: reading CSV parts: {}", e)))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect();
        // DuckDB names parts data_0.csv .. data_N.csv without zero-padding, so a
        // plain lexicographic sort would run 0,1,10,11,..,2 once a load splits
        // into 10+ parts. Nothing breaks (parts are independent jobs and data_0
        // still sorts first for the results-file header), but jobs and result
        // rows should follow input order. Same-length names compare lexically,
        // so (len, name) yields numeric order for this fixed name shape.
        parts.sort_by_key(|p| {
            let name = p.file_name().map(|n| n.to_string_lossy().into_owned());
            (name.as_ref().map(String::len).unwrap_or(0), name)
        });
        if parts.is_empty() {
            return Err(EngineError::Other(
                "salesforce bulk: DuckDB wrote no CSV parts for a non-empty view".into(),
            ));
        }

        // One stem per run; parts accumulate into the same result files.
        let results_stem = spec.results_path.as_ref().map(|_| {
            format!(
                "{}_{}_{}",
                spec.object,
                spec.operation,
                chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
            )
        });

        const ERROR_SAMPLE_MAX: usize = 5;
        let mut total_processed: u64 = 0;
        let mut total_failed: u64 = 0;
        let mut job_ids: Vec<String> = Vec::new();
        let mut error_samples: Vec<String> = Vec::new();

        for (idx, part) in parts.iter().enumerate() {
            self.check_cancelled()?;
            let size = std::fs::metadata(part).map(|m| m.len()).unwrap_or(0);
            // A single part DuckDB couldn't split under the ceiling (pathological
            // very-wide row). Fail clearly rather than let Salesforce 400 on it.
            if size > BULK_UPLOAD_MAX_BYTES {
                return Err(EngineError::Query(format!(
                    "salesforce bulk: CSV part {} is {} bytes, over the {} MB Bulk upload limit; \
                     the row width may be too large to split - reduce columns or use snk.salesforce",
                    idx,
                    size,
                    BULK_UPLOAD_MAX_BYTES / (1024 * 1024)
                )));
            }

            let job_id = self.bulk_create_ingest_job(&ingest_base, &auth_header, spec)?;
            job_ids.push(job_id.clone());

            // One part is <=90 MB, so holding it for the PUT is bounded.
            let bytes = std::fs::read(part).map_err(|e| {
                EngineError::Other(format!("salesforce bulk: reading CSV part: {}", e))
            })?;
            if let Err(e) = self.bulk_upload_and_close(&ingest_base, &job_id, &auth_header, &bytes) {
                let _ = self.bulk_abort_job(&ingest_base, &job_id, &auth_header);
                return Err(e);
            }

            let status = match self.bulk_poll_job(
                &ingest_base,
                &job_id,
                &auth_header,
                spec.poll_interval_secs,
                spec.timeout_secs,
            )
            {
                Ok(s) => s,
                Err(e) => {
                    let _ = self.bulk_abort_job(&ingest_base, &job_id, &auth_header);
                    return Err(e);
                }
            };
            total_processed += status.records_processed;
            total_failed += status.records_failed;
            // Sample the first few failedResults rows so the run error can show
            // WHAT failed even when no resultsPath is configured (parity with
            // the Collections sink's first-5-errors message). Only fetched when
            // failOnError will actually surface them - with it off, the user
            // opted into counts-only and resultsPath is the error record.
            if spec.fail_on_error
                && status.records_failed > 0
                && error_samples.len() < ERROR_SAMPLE_MAX
            {
                error_samples.extend(self.bulk_first_failed_errors(
                    &ingest_base,
                    &job_id,
                    &auth_header,
                    ERROR_SAMPLE_MAX - error_samples.len(),
                ));
            }

            // Result sets come back already CSV-shaped; stream them to the
            // stamped files (best-effort per endpoint - a missing set never masks
            // the job outcome). Each result file keeps the header from its first
            // written body and strips it from later ones (decided per file, so a
            // set skipped on an earlier part can't leave a headerless file).
            if let (Some(dir), Some(stem)) = (spec.results_path.as_deref(), results_stem.as_ref()) {
                self.bulk_write_result_files(
                    &ingest_base,
                    &job_id,
                    &auth_header,
                    dir,
                    stem,
                )?;
            }

            if status.state != "JobComplete" {
                return Err(EngineError::Query(format!(
                    "salesforce bulk {} {}: job {} ended {}{}",
                    spec.operation,
                    spec.object,
                    job_id,
                    status.state,
                    if status.error_message.is_empty() {
                        String::new()
                    } else {
                        format!(" - {}", status.error_message)
                    }
                )));
            }
        }

        let succeeded = total_processed.saturating_sub(total_failed);
        if total_failed > 0 && spec.fail_on_error {
            let samples = if error_samples.is_empty() {
                String::new()
            } else {
                format!(" First errors: {}.", error_samples.join("; "))
            };
            return Err(EngineError::Query(format!(
                "salesforce bulk {} {}: {} succeeded, {} failed across {} job(s) [{}].{} \
                 Set resultsPath to capture every failed record, or failOnError off to continue.",
                spec.operation,
                spec.object,
                succeeded,
                total_failed,
                job_ids.len(),
                job_ids.join(","),
                samples
            )));
        }
        Ok(format!(
            "salesforce bulk {} {}: {} succeeded, {} failed across {} job(s)",
            spec.operation,
            spec.object,
            succeeded,
            total_failed,
            job_ids.len()
        ))
    }

    /// src.salesforce.bulk: read a SOQL result set through a Bulk API 2.0 query
    /// job. Create job -> poll to JobComplete -> walk the paged CSV result sets
    /// (Sforce-Locator) into a staging file -> read_csv materializes it as the
    /// node's table. The result pages stream to disk, so a multi-GB result set
    /// never lands in memory; only DuckDB's own reader touches the full file.
    pub(crate) fn run_salesforce_bulk_source(
        &self,
        db: &Path,
        spec: &SalesforceBulkSourceSpec,
    ) -> Result<String, EngineError> {
        let (access_token, instance_url) = match &spec.oauth {
            Some(o) => {
                let (tok, minted_instance) = mint_oauth_token(o)?;
                let instance = if !minted_instance.is_empty() {
                    minted_instance
                } else if !spec.instance_url.is_empty() {
                    spec.instance_url.clone()
                } else {
                    return Err(EngineError::Config(
                        "salesforce bulk: OAuth token response carried no instance_url and no \
                         instanceUrl was configured"
                            .into(),
                    ));
                };
                (tok, instance)
            }
            None => (spec.access_token.clone(), spec.instance_url.clone()),
        };
        let auth_header = format!("Bearer {}", access_token);
        let query_base = format!(
            "{}/services/data/{}/jobs/query",
            instance_url.trim_end_matches('/'),
            spec.api_version
        );

        // Same private staging pattern as the sink: the result set is org data,
        // so the dir is owner-only on Unix, and ScopedDir removes it on every
        // exit path. pid + process-local counter keeps concurrent stages apart.
        static BULKQ_DIR_SEQ: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let seq = BULKQ_DIR_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let staging_dir = std::env::temp_dir().join(format!(
            "duckle-sfbulkq-{}-{}",
            std::process::id(),
            seq
        ));
        let _ = std::fs::remove_dir_all(&staging_dir);
        let _cleanup = ScopedDir(staging_dir.clone());
        create_private_dir(&staging_dir).map_err(|e| {
            EngineError::Other(format!("salesforce bulk: creating staging dir: {}", e))
        })?;
        let result_path = staging_dir.join("result.csv");

        // Create the query job. Salesforce validates the SOQL here, so an
        // unsupported construct (GROUP BY, aggregate, parent-to-child subquery)
        // fails fast with the API's own message.
        let body = serde_json::json!({
            "operation": spec.operation,
            "query": spec.query,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF",
        });
        let resp = crate::tls::http_agent()
            .post(&query_base)
            .set("Authorization", &auth_header)
            .set("Content-Type", "application/json")
            .set("Accept", "application/json")
            .send_string(&body.to_string());
        let txt = bulk_read_body(resp, &query_base, "create query job")?;
        let v: JsonValue = serde_json::from_str(&txt).map_err(|e| {
            EngineError::Query(format!(
                "salesforce bulk: create query job: non-JSON response ({}): {}",
                e,
                tail_chars(&txt, 200)
            ))
        })?;
        let job_id = v
            .get("id")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                EngineError::Query(format!(
                    "salesforce bulk: create query job: response carried no job id: {}",
                    tail_chars(&txt, 200)
                ))
            })?
            .to_string();

        let status = match self.bulk_poll_job(
            &query_base,
            &job_id,
            &auth_header,
            spec.poll_interval_secs,
            spec.timeout_secs,
        ) {
            Ok(s) => s,
            Err(e) => {
                let _ = self.bulk_abort_job(&query_base, &job_id, &auth_header);
                return Err(e);
            }
        };
        if status.state != "JobComplete" {
            return Err(EngineError::Query(format!(
                "salesforce bulk {}: query job {} ended {}{}",
                spec.operation,
                job_id,
                status.state,
                if status.error_message.is_empty() {
                    String::new()
                } else {
                    format!(" - {}", status.error_message)
                }
            )));
        }

        // Walk the paged result sets. Each page streams to the staging file;
        // append_bulk_result_csv keeps the first page's header and strips later
        // ones. The last page is signalled by the LITERAL STRING "null" in the
        // Sforce-Locator header - not an absent header - a documented sharp
        // edge of the API.
        let mut total_rows: u64 = 0;
        let mut locator: Option<String> = None;
        let mut pages_fetched: u64 = 0;
        loop {
            self.check_cancelled()?;
            let mut url = format!("{}/{}/results", query_base, job_id);
            let mut sep = '?';
            if let Some(n) = spec.max_records {
                url.push_str(&format!("{}maxRecords={}", sep, n));
                sep = '&';
            }
            if let Some(loc) = &locator {
                // Percent-encode: the locator is an opaque server-generated
                // token echoed back verbatim, so a '+' would decode as a space
                // and an '&' would start a new parameter. Matches how the REST
                // cursor and Weaviate's `after` token are handled in this file.
                url.push_str(&format!("{}locator={}", sep, urlencode_simple(loc)));
            }
            let resp = crate::tls::http_agent()
                .get(&url)
                .set("Authorization", &auth_header)
                .set("Accept", "text/csv")
                .call();
            let resp = match resp {
                Ok(r) => r,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "salesforce bulk: fetch query results: HTTP {}: {}",
                        code,
                        tail_chars(&body, 300)
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "salesforce bulk: fetch query results: {}",
                        e
                    )))
                }
            };
            total_rows += resp
                .header("Sforce-NumberOfRecords")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let next = resp.header("Sforce-Locator").map(|s| s.to_string());
            append_bulk_result_csv(&result_path, resp.into_reader()).map_err(|e| {
                EngineError::Other(format!(
                    "salesforce bulk: writing {}: {}",
                    result_path.display(),
                    e
                ))
            })?;
            pages_fetched += 1;
            match next.as_deref() {
                Some("null") | Some("") | None => break,
                // Runaway guards: timeoutSecs only bounds the POLL phase, so
                // the page walk needs its own. A peer (or interfering
                // middlebox) that echoes the same locator back would otherwise
                // re-append the same page forever; a locator that keeps
                // changing is still capped far above any real result set
                // (50M pages even at the 1000-record floor is 50B records).
                Some(loc) if locator.as_deref() == Some(loc) => {
                    return Err(EngineError::Query(format!(
                        "salesforce bulk: query job {} returned a non-advancing \
                         Sforce-Locator ('{}') on page {}; aborting the result walk \
                         rather than re-fetching the same page forever",
                        job_id,
                        loc,
                        pages_fetched
                    )));
                }
                Some(loc) if pages_fetched >= BULK_QUERY_MAX_PAGES => {
                    return Err(EngineError::Query(format!(
                        "salesforce bulk: query job {} exceeded {} result pages \
                         (last locator '{}'); this is a runaway backstop, not a \
                         tunable - a genuine result set this large should be split \
                         with WHERE clauses",
                        job_id, BULK_QUERY_MAX_PAGES, loc
                    )));
                }
                Some(loc) => locator = Some(loc.to_string()),
            }
        }

        // Emptiness is decided by what was actually staged, not by
        // Sforce-NumberOfRecords alone. That header is optional and
        // non-standard: an egress proxy or gateway that strips or rewrites
        // Sforce-* headers makes `total_rows` stay 0 through `unwrap_or(0)`
        // even though every page's CSV was streamed to disk, and taking the
        // empty branch there would discard a complete multi-GB extract and
        // report the run as a successful 0-row read. A silent wrong answer is
        // the one outcome a migration source must never produce, so the file
        // is the authority and the header is only a fast path.
        let staged_has_rows = result_csv_has_data_rows(&result_path);
        if total_rows == 0 && staged_has_rows {
            eprintln!(
                "salesforce bulk: Sforce-NumberOfRecords was missing or unparseable on every \
                 page, but {} contains data rows; reading the staged file rather than \
                 reporting an empty result",
                result_path.display()
            );
        }
        if total_rows == 0 && !staged_has_rows {
            // The #170 contract: a 0-record query must yield a typed empty
            // relation (or a clear error when no schema is declared), never a
            // bare `json` column downstream SQL can't bind.
            crate::materialize_empty_result(
                self.binary(),
                db,
                &spec.node_id,
                spec.declared_schema.as_deref(),
            )?;
            return Ok(format!(
                "salesforce bulk {}: 0 rows (typed empty relation)",
                spec.operation
            ));
        }

        // DuckDB reads the staged CSV straight into the node's table. With a
        // declared schema the columns are pinned to those names and types
        // (all_varchar + TRY_CAST, so a stray unparseable cell becomes NULL
        // rather than failing a multi-GB load); without one, read_csv infers.
        let csv_target = sql_escape(&result_path.to_string_lossy().replace('\\', "/"));
        let create_sql = match spec.declared_schema.as_deref() {
            Some(cols) if !cols.is_empty() => {
                let select_list = cols
                    .iter()
                    .map(|c| {
                        format!(
                            "TRY_CAST({col} AS {ty}) AS {col}",
                            col = plan::quote_ident(&c.name),
                            ty = plan::data_type_to_duckdb_sql(&c.data_type)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "CREATE OR REPLACE TABLE {} AS SELECT {} FROM read_csv('{}', header=true, all_varchar=true);",
                    plan::quote_ident(&spec.node_id),
                    select_list,
                    csv_target
                )
            }
            // No declared schema: read everything as text rather than letting
            // read_csv sniff types, so a column's type does not depend on the
            // values a given extract happened to contain.
            //
            // The risk is type instability between runs, not leading zeros:
            // DuckDB's sniffer already keeps "01234" as VARCHAR. What it does
            // not keep stable is a column whose values are all numeric in one
            // extract and include one alphanumeric in the next. Verified on
            // DuckDB 1.5.4, that column comes back BIGINT the first time and
            // VARCHAR the second, so a sink's target table silently changes
            // shape between runs, or the load fails on a type mismatch.
            // Salesforce serves every value as CSV text anyway, so reading it
            // as text is both faithful and deterministic; casting is then a
            // downstream choice made deliberately, or by declaring a schema,
            // which pins types via TRY_CAST above.
            _ => format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_csv('{}', header=true, all_varchar=true);",
                plan::quote_ident(&spec.node_id),
                csv_target
            ),
        };
        self.run(Some(db), &create_sql, false)?;

        Ok(format!(
            "salesforce bulk {}: {} rows via job {}",
            spec.operation, total_rows, job_id
        ))
    }

    /// POST a Bulk API 2.0 ingest job and return its Id.
    fn bulk_create_ingest_job(
        &self,
        ingest_base: &str,
        auth_header: &str,
        spec: &SalesforceBulkSinkSpec,
    ) -> Result<String, EngineError> {
        let mut body = serde_json::Map::new();
        body.insert("object".into(), JsonValue::String(spec.object.clone()));
        body.insert("operation".into(), JsonValue::String(spec.operation.clone()));
        body.insert("contentType".into(), JsonValue::String("CSV".into()));
        // DuckDB's CSV writer emits LF on every platform (verified on Windows).
        body.insert("lineEnding".into(), JsonValue::String("LF".into()));
        if spec.operation == "upsert" {
            if let Some(ext) = &spec.external_id_field {
                body.insert("externalIdFieldName".into(), JsonValue::String(ext.clone()));
            }
        }
        if let Some(rule) = &spec.assignment_rule_id {
            body.insert("assignmentRuleId".into(), JsonValue::String(rule.clone()));
        }
        let body_str =
            serde_json::to_string(&JsonValue::Object(body)).unwrap_or_else(|_| "{}".into());
        let resp = crate::tls::http_agent()
            .post(ingest_base)
            .set("Authorization", auth_header)
            .set("Content-Type", "application/json")
            .set("Accept", "application/json")
            .send_string(&body_str);
        let txt = bulk_read_body(resp, ingest_base, "create job")?;
        let v: JsonValue = serde_json::from_str(&txt).map_err(|e| {
            EngineError::Query(format!(
                "salesforce bulk create job: non-JSON response ({}): {}",
                e,
                tail_chars(&txt, 200)
            ))
        })?;
        let id = v
            .get("id")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        if id.is_empty() {
            return Err(EngineError::Query(format!(
                "salesforce bulk create job: response missing job id: {}",
                tail_chars(&txt, 200)
            )));
        }
        Ok(id)
    }

    /// PUT one part's CSV to a job, then PATCH it to UploadComplete so Salesforce
    /// starts processing.
    fn bulk_upload_and_close(
        &self,
        ingest_base: &str,
        job_id: &str,
        auth_header: &str,
        csv: &[u8],
    ) -> Result<(), EngineError> {
        let upload_url = format!("{}/{}/batches", ingest_base, job_id);
        let resp = crate::tls::http_agent()
            .put(&upload_url)
            .set("Authorization", auth_header)
            .set("Content-Type", "text/csv")
            .set("Accept", "application/json")
            .send_bytes(csv);
        // A successful upload returns 201 with no body.
        bulk_read_body(resp, &upload_url, "upload CSV")?;

        let close_url = format!("{}/{}", ingest_base, job_id);
        let resp = crate::tls::http_agent()
            .request("PATCH", &close_url)
            .set("Authorization", auth_header)
            .set("Content-Type", "application/json")
            .set("Accept", "application/json")
            .send_string(r#"{"state":"UploadComplete"}"#);
        bulk_read_body(resp, &close_url, "close job")?;
        Ok(())
    }

    /// Poll a job until it reaches a terminal state, or the configured timeout
    /// elapses. Checks cancellation every iteration (unlike the Snowflake /
    /// Databricks pollers) because a Bulk job can legitimately run for hours.
    fn bulk_poll_job(
        &self,
        jobs_base: &str,
        job_id: &str,
        auth_header: &str,
        poll_interval_secs: u64,
        timeout_secs: u64,
    ) -> Result<BulkJobStatus, EngineError> {
        let url = format!("{}/{}", jobs_base, job_id);
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let interval = std::time::Duration::from_secs(poll_interval_secs);
        loop {
            self.check_cancelled()?;
            let resp = crate::tls::http_agent()
                .get(&url)
                .set("Authorization", auth_header)
                .set("Accept", "application/json")
                .call();
            let txt = bulk_read_body(resp, &url, "poll job")?;
            let v: JsonValue = serde_json::from_str(&txt).map_err(|e| {
                EngineError::Query(format!(
                    "salesforce bulk poll job: non-JSON response ({}): {}",
                    e,
                    tail_chars(&txt, 200)
                ))
            })?;
            let state = v
                .get("state")
                .and_then(|s| s.as_str())
                .unwrap_or_default()
                .to_string();
            if matches!(state.as_str(), "JobComplete" | "Failed" | "Aborted") {
                return Ok(BulkJobStatus {
                    state,
                    records_processed: v
                        .get("numberRecordsProcessed")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0),
                    records_failed: v
                        .get("numberRecordsFailed")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0),
                    error_message: v
                        .get("errorMessage")
                        .and_then(|x| x.as_str())
                        .unwrap_or_default()
                        .to_string(),
                });
            }
            if start.elapsed() >= timeout {
                return Err(EngineError::Query(format!(
                    "salesforce bulk: job {} did not finish within {}s (last state '{}')",
                    job_id, timeout_secs, state
                )));
            }
            std::thread::sleep(interval);
        }
    }

    /// PATCH a job to Aborted. Best-effort cleanup on timeout / upload failure /
    /// cancel, so the caller ignores the result.
    fn bulk_abort_job(
        &self,
        ingest_base: &str,
        job_id: &str,
        auth_header: &str,
    ) -> Result<(), EngineError> {
        let url = format!("{}/{}", ingest_base, job_id);
        let resp = crate::tls::http_agent()
            .request("PATCH", &url)
            .set("Authorization", auth_header)
            .set("Content-Type", "application/json")
            .set("Accept", "application/json")
            .send_string(r#"{"state":"Aborted"}"#);
        bulk_read_body(resp, &url, "abort job").map(|_| ())
    }

    /// Sample up to `max` error messages from a job's failedResults CSV, for the
    /// run error message. Streams and stops after `max` data lines - the full
    /// set can be ~100 MB and belongs in resultsPath, not an error string.
    /// Best-effort: any fetch/read problem just yields fewer (or no) samples.
    fn bulk_first_failed_errors(
        &self,
        ingest_base: &str,
        job_id: &str,
        auth_header: &str,
        max: usize,
    ) -> Vec<String> {
        use std::io::BufRead;
        let url = format!("{}/{}/failedResults", ingest_base, job_id);
        let resp = crate::tls::http_agent()
            .get(&url)
            .set("Authorization", auth_header)
            .set("Accept", "text/csv")
            .call();
        let Ok(r) = resp else { return Vec::new() };
        let mut out = Vec::new();
        // Row shape: "sf__Id","sf__Error",<input columns...>. Pull the second
        // field for a Collections-style "CODE:message" line, falling back to
        // the raw (truncated) line if the quoting isn't as expected.
        for line in std::io::BufReader::new(r.into_reader())
            .lines()
            .skip(1)
            .take(max)
        {
            let Ok(line) = line else { break };
            if line.trim().is_empty() {
                continue;
            }
            let err_field = line
                .splitn(3, "\",\"")
                .nth(1)
                .map(|s| s.trim_end_matches('"'))
                .filter(|s| !s.is_empty());
            out.push(match err_field {
                Some(e) => e.chars().take(200).collect(),
                None => line.chars().take(200).collect(),
            });
        }
        out
    }

    /// Fetch a job's three result sets and append them to the stamped files.
    /// Salesforce returns each already CSV-shaped (input columns plus `sf__Id` or
    /// `sf__Error`), so they stream to disk verbatim. `first` writes the header;
    /// later parts append data rows only.
    fn bulk_write_result_files(
        &self,
        ingest_base: &str,
        job_id: &str,
        auth_header: &str,
        dir: &str,
        stem: &str,
    ) -> Result<(), EngineError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| EngineError::Other(format!("salesforce bulk: creating resultsPath: {}", e)))?;
        for (endpoint, suffix) in [
            ("successfulResults", "success"),
            ("failedResults", "error"),
            ("unprocessedRecords", "unprocessed"),
        ] {
            let url = format!("{}/{}/{}", ingest_base, job_id, endpoint);
            let resp = crate::tls::http_agent()
                .get(&url)
                .set("Authorization", auth_header)
                .set("Accept", "text/csv")
                .call();
            // Best-effort: a Failed job may 400 on successfulResults, etc. Skip a
            // set we can't fetch rather than masking the job outcome. The body
            // MUST stream via into_reader(): a result set for a ~200k-record job
            // is ~100 MB, and ureq's into_string() silently caps at 10 MB (found
            // live - the success file came back empty for a completed 210k job).
            let body = match resp {
                Ok(r) => r.into_reader(),
                Err(_) => continue,
            };
            let path = std::path::Path::new(dir).join(format!("{}_{}.csv", stem, suffix));
            append_bulk_result_csv(&path, body).map_err(|e| {
                EngineError::Other(format!(
                    "salesforce bulk: writing {}: {}",
                    path.display(),
                    e
                ))
            })?;
        }
        Ok(())
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
        // "overwrite": the target holds this run's rows and nothing older. After
        // the CREATE so a first run against a table that does not exist yet still
        // works, and before the inserts so the two cannot interleave.
        if spec.truncate_first {
            post_stmt(format!("TRUNCATE TABLE {}", qualified))?;
        }

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

        // Truncate + insert write mode (#138): clear existing rows but keep the
        // table (and its grants / indexes) before the plain-insert path. Only
        // for non-upsert writes; upsert has its own MERGE path below.
        if spec.upsert_keys.is_empty() && spec.mode == "truncate" {
            conn.execute(&format!("TRUNCATE TABLE {}", qualified), &[])
                .map_err(|e| EngineError::Query(format!("oracle truncate: {}", e)))?;
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
        direct: Option<&DirectSinkTarget>,
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
        // A read-only transaction so that, if a column has to be measured before
        // it can be typed, the measuring query and the extract see one snapshot
        // rather than two. Without it Oracle takes a fresh snapshot per
        // statement, and a row deleted in between could let the measurement
        // miss a value the extract then hits. Unlike DBMS_FLASHBACK this needs
        // no grant. Best effort: if it is refused we simply do not measure, and
        // the older carry-as-text path still applies.
        let snapshot_pinned = match conn.execute("SET TRANSACTION READ ONLY", &[]) {
            Ok(_) => true,
            Err(e) => {
                mark(&format!("read-only transaction unavailable: {}", e));
                false
            }
        };
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
            .query_as::<EncodeRow>(&[])
            .map_err(|e| EngineError::Query(format!("oracle query: {}", e)))?;
        let cols: Vec<String> = rs
            .column_info()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        mark(&format!("query open; {} columns; streaming rows", cols.len()));

        // #221: when every column's Oracle type pins an Arrow type, stream
        // Arrow batches into a temp parquet instead of NDJSON text. The old
        // path formats every value to text and makes DuckDB parse it back,
        // which measured ~10x the cost of a parquet intermediate on a wide
        // fact table. Ambiguous schemas return None here and take the
        // unchanged NDJSON path below.
        if let Some((mut schema, mut numeric_text)) = Self::oracle_arrow_schema(rs.column_info()) {
            // Worth measuring only when a direct write is on the table: that is
            // the case where typing these columns up front removes a whole pass
            // rather than just moving where the work happens.
            if !numeric_text.is_empty() && direct.is_some() && snapshot_pinned {
                if let Some(pinned) = Self::oracle_probe_text_widths(
                    &conn,
                    &spec.query,
                    rs.column_info(),
                    &schema,
                    &numeric_text,
                    &mark,
                ) {
                    schema = pinned;
                    numeric_text = Vec::new();
                }
            }
            if numeric_text.is_empty() {
                mark("arrow fast path: all column types pinned");
            } else {
                mark(&format!(
                    "arrow fast path: {} column(s) carried as text and typed after the write",
                    numeric_text.len()
                ));
            }
            // A single extract sits at the OCI driver's per-cell floor, so the
            // only way past it is more sessions. Taken only when the read can
            // be pinned to one SCN, so every session sees the same snapshot.
            if let Some(par) = self.oracle_parallel_plan(&conn, spec, &mark) {
                drop(rs);
                drop(stmt);
                return self.oracle_parallel_to_parquet(db, spec, schema, &numeric_text, par, &mark);
            }
            // The file a direct write produces IS the user's output, so it must
            // carry real types. A text column would have to be cast after the
            // fact, which is exactly the second pass being skipped - so when any
            // column needs typing, the normal path runs and the sink does it.
            let direct = if numeric_text.is_empty() { direct } else { None };
            return self
                .oracle_rows_to_parquet(db, spec, rs, schema, &numeric_text, direct, &mark);
        }
        mark("arrow fast path unavailable (ambiguous column type); using NDJSON");

        // Stream rows straight to the NDJSON temp file. The previous
        // Vec<JsonValue> collector held the entire result set in RAM
        // before handing it to DuckDB - on a million-row x 37-col pull
        // that peaked at ~30 GB resident. Now the writer keeps a 64 KiB
        // buffer regardless of row count.
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let mut count = 0_usize;
        for row_res in rs {
            // The row was turned into JSON inside `RowValue::get`, off the
            // borrowed row, so no owned `Row` was ever built for it.
            let row = row_res.map_err(|e| EngineError::Query(format!("oracle row: {}", e)))?;
            if let Some(obj) = row.0 {
                writer.write_row(&obj)?;
            }
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
    /// Stream an open Oracle result set into a temp parquet via Arrow, then
    /// hand DuckDB `read_parquet` (#221). Mirrors `run_adbc_source`: the
    /// parquet encode runs on its own thread so it overlaps the next OCI
    /// fetch rather than running after it.
    fn oracle_rows_to_parquet(
        &self,
        db: &Path,
        spec: &OracleSourceSpec,
        rs: oracle::ResultSet<'_, EncodeRow>,
        schema: arrow_schema::Schema,
        numeric_text: &[usize],
        direct: Option<&DirectSinkTarget>,
        mark: &dyn Fn(&str),
    ) -> Result<String, EngineError> {
        let schema = std::sync::Arc::new(schema);
        let ncols = schema.fields().len().max(1);
        // Sibling of the run's db file so TempDbGuard sweeps it at run end; the
        // file has to outlive this stage when we hand back a lazy VIEW.
        let db_name = db.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let safe_node: String = spec
            .node_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        // When the only consumer is a plain Parquet sink, write ITS file and
        // skip the encode-decode-encode round trip entirely. The sink is told
        // to stand down only after the write succeeds.
        // A column carried as text has no type until it has been written and
        // measured, and that typing happens on exactly the pass this option
        // exists to skip. Writing the sink's own file straight from the source
        // would leave those columns as VARCHAR in the user's Parquet, with
        // nothing anywhere to say so - a bare NUMBER would arrive as a string.
        // Decline the shortcut and take the normal path rather than hand back
        // the wrong types faster.
        let direct = match direct {
            Some(_) if !numeric_text.is_empty() => {
                mark(&format!(
                    "direct write declined: {} column(s) travel as text and are typed after \
                     the write, which is the pass a direct write skips",
                    numeric_text.len()
                ));
                None
            }
            other => other,
        };
        let parquet_path = match direct {
            Some(t) => PathBuf::from(t.path),
            None => db.with_file_name(format!("{}.oraarrow-{}.parquet", db_name, safe_node)),
        };
        if let Some(parent) = parquet_path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        // A temp file wants no compression; the sink's own file must match what
        // the sink would have written, and the Parquet sink's default is ZSTD -
        // so an unset compression means ZSTD here, NOT uncompressed.
        //
        // Compressing the temp was tried and measured a net loss. It does make
        // the sink's read cheaper - 3579 MB and 22.0s uncompressed against
        // 1501 MB and 17.9s with SNAPPY - but this writer pays 5.3s to compress
        // it, so the run went 75.9s to 76.9s. (A DuckDB-to-DuckDB simulation
        // predicted a win; it was wrong because DuckDB, not this writer, did the
        // writing in that test.)
        let written = Self::oracle_write_parquet_part(
            rs,
            &schema,
            &parquet_path,
            direct.map(|t| t.compression.unwrap_or("ZSTD")),
            &self.cancel,
            Some(mark),
        )?;
        mark(&format!("parquet written: {} rows, {} cols", written, ncols));
        if let Some(t) = direct {
            // The relation still has to exist: downstream row counting and the
            // run preview read it, and it is now a view over the sink's own
            // output rather than over a temp file.
            let dest = t.path.replace('\\', "/").replace('\'', "''");
            self.run(
                Some(db),
                &format!(
                    "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')",
                    plan::quote_ident(&spec.node_id),
                    dest
                ),
                false,
            )?;
            t.written.store(true, std::sync::atomic::Ordering::Relaxed);
            mark("wrote the sink's parquet directly; skipping the second pass");
            return Ok(format!("oracle: {} rows into {}", written, spec.node_id));
        }

        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        // Single consumer: a lazy read_parquet VIEW. Copying the parquet into a
        // table costs a full decode-and-store pass over every column (measured
        // at ~3.2s of a 10.1s 1M-row x 40-col run) and throws away the pushdown
        // the consumer would otherwise get. 2+ consumers: materialize once as a
        // TABLE so the parquet is decoded a single time, then drop the temp.
        let kw = if spec.single_consumer { "VIEW" } else { "TABLE" };
        let projection = self.oracle_numeric_projection(db, &ppath, &schema, numeric_text, mark)?;
        let create = format!(
            "CREATE OR REPLACE {} {} AS SELECT {} FROM read_parquet('{}')",
            kw,
            plan::quote_ident(&spec.node_id),
            projection,
            ppath
        );
        self.run(Some(db), &create, false)?;
        if spec.single_consumer {
            mark("exposed as lazy read_parquet view");
        } else {
            let _ = std::fs::remove_file(&parquet_path);
            mark("materialized into duckdb");
        }
        Ok(format!("oracle: {} rows into {}", written, spec.node_id))
    }

    /// Decide whether this extract can be split across sessions, and how.
    ///
    /// Returns None - meaning read with one session - unless all of these hold,
    /// because a parallel read that cannot satisfy them is either wrong or
    /// pointless:
    ///
    /// - the user asked for it (a split column and a degree above 1);
    /// - the whole read can be pinned to a single SCN. This is the correctness
    ///   gate. Without it the sessions each get their own read-consistent view
    ///   taken at slightly different times, so a table being written to while
    ///   the extract runs could be torn across bands in a way a single session
    ///   would never produce. We decline rather than quietly risk it;
    /// - the split column has a usable range to cut into bands.
    ///
    /// Every refusal is reported through `mark`, so someone who configured
    /// parallelism and did not get it can see exactly why.
    #[cfg(feature = "oracle")]
    fn oracle_parallel_plan(
        &self,
        conn: &oracle::Connection,
        spec: &OracleSourceSpec,
        mark: &dyn Fn(&str),
    ) -> Option<OracleParallelPlan> {
        let degree = spec.parallel_degree;
        let column = spec.parallel_column.as_deref()?.trim().to_string();
        if degree <= 1 || column.is_empty() {
            return None;
        }
        // Refused rather than escaped: a split column is interpolated into SQL,
        // and a pipeline file is not a trusted source of SQL fragments.
        if column.is_empty()
            || column.len() > 128
            || !column
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == '#')
        {
            mark(&format!(
                "parallel read declined: {} is not a plain column name",
                column
            ));
            return None;
        }

        // The correctness gate. DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER needs
        // only EXECUTE on the package; CURRENT_SCN is the fallback for accounts
        // that have the view but not the package.
        let scn: Option<u64> = [
            "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM DUAL",
            "SELECT CURRENT_SCN FROM V$DATABASE",
        ]
        .iter()
        .find_map(|sql| conn.query_row_as::<u64>(sql, &[]).ok());
        let scn = match scn {
            Some(s) => s,
            None => {
                mark(
                    "parallel read declined: cannot pin a read SCN (needs EXECUTE on \
                     DBMS_FLASHBACK or SELECT on V_$DATABASE); reading with one session \
                     so the snapshot stays consistent",
                );
                return None;
            }
        };

        let body = spec.query.trim().trim_end_matches(';').trim().to_string();
        let quoted = format!("\"{}\"", column.to_ascii_uppercase());
        // Bands are cut on a NUMBER, so a DATE or TIMESTAMP column has to become
        // one first. Oracle date arithmetic yields days as a plain NUMBER, and a
        // TIMESTAMP needs the explicit CAST because subtracting timestamps gives
        // an INTERVAL instead. Describing the column against the user's own
        // query rather than the data dictionary means this also works when the
        // query is a join or a subquery, not just a bare table.
        let probe = format!("SELECT {} FROM ({}) WHERE 1=0", quoted, body);
        let is_datetime = match conn.statement(&probe).build().and_then(|mut st| {
            st.query(&[]).map(|rs| {
                matches!(
                    rs.column_info().first().map(|c| c.oracle_type()),
                    Some(oracle::sql_type::OracleType::Date)
                        | Some(oracle::sql_type::OracleType::Timestamp(_))
                )
            })
        }) {
            Ok(v) => v,
            Err(e) => {
                mark(&format!(
                    "parallel read declined: split column {} is not usable ({})",
                    column, e
                ));
                return None;
            }
        };
        // Bands are computed as numbers, but the PREDICATE has to be written
        // against the bare column, comparing to a value of the column's own
        // type. Wrapping the column in an expression is what stops Oracle
        // pruning partitions or using an index on it, which is the difference
        // between each session scanning its own slice and each session scanning
        // the whole table. So the arithmetic form is used only to find the
        // range; the bands are emitted as `col >= <literal>`.
        let bounds_expr = if is_datetime {
            format!("(CAST({} AS DATE) - DATE '1970-01-01')", quoted)
        } else {
            quoted.clone()
        };

        // MIN/MAX over an indexed column is an index scan, not a table scan.
        let bounds_sql = format!("SELECT MIN({0}), MAX({0}) FROM ({1})", bounds_expr, body);
        let (lo, hi) = match conn.query_row_as::<(Option<f64>, Option<f64>)>(&bounds_sql, &[]) {
            Ok((Some(lo), Some(hi))) => (lo, hi),
            Ok(_) => {
                mark("parallel read declined: split column is entirely NULL");
                return None;
            }
            Err(e) => {
                mark(&format!(
                    "parallel read declined: could not read the range of {} ({})",
                    column, e
                ));
                return None;
            }
        };
        if !(hi > lo) {
            mark("parallel read declined: split column holds a single value");
            return None;
        }

        mark(&format!(
            "parallel read: {} sessions split on {} at SCN {}",
            degree, column, scn
        ));
        Some(OracleParallelPlan {
            column: quoted,
            is_datetime,
            degree,
            scn,
            lo,
            hi,
            body,
        })
    }

    /// A band boundary, written as a literal of the split column's own type so
    /// the comparison stays sargable (see `oracle_parallel_plan`). Dates are
    /// carried as days since the epoch, so they convert back exactly.
    #[cfg(feature = "oracle")]
    fn oracle_band_literal(v: f64, is_datetime: bool) -> String {
        if is_datetime {
            format!("(DATE '1970-01-01' + {})", v)
        } else {
            format!("{}", v)
        }
    }

    /// Run the extract on `plan.degree` sessions, each fetching one band of the
    /// split column into its own parquet part, then expose the parts as one
    /// relation.
    ///
    /// The bands are disjoint and total by construction: band 0 also takes NULLs
    /// and anything below the observed minimum, and the last band is open-ended
    /// upward, so no row can be dropped or double-counted even if the data moved
    /// between the range probe and the read.
    #[cfg(feature = "oracle")]
    fn oracle_parallel_to_parquet(
        &self,
        db: &Path,
        spec: &OracleSourceSpec,
        schema: arrow_schema::Schema,
        numeric_text: &[usize],
        plan_: OracleParallelPlan,
        mark: &dyn Fn(&str),
    ) -> Result<String, EngineError> {
        let db_name = db.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let safe_node: String = spec
            .node_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let schema = std::sync::Arc::new(schema);
        let width = (plan_.hi - plan_.lo) / plan_.degree as f64;

        let mut handles = Vec::with_capacity(plan_.degree);
        for i in 0..plan_.degree {
            let lit = |v: f64| Self::oracle_band_literal(v, plan_.is_datetime);
            let predicate = if i == 0 {
                format!(
                    "{c} < {b} OR {c} IS NULL",
                    c = plan_.column,
                    b = lit(plan_.lo + width)
                )
            } else if i == plan_.degree - 1 {
                format!(
                    "{c} >= {b}",
                    c = plan_.column,
                    b = lit(plan_.lo + width * i as f64)
                )
            } else {
                format!(
                    "{c} >= {l} AND {c} < {h}",
                    c = plan_.column,
                    l = lit(plan_.lo + width * i as f64),
                    h = lit(plan_.lo + width * (i + 1) as f64)
                )
            };
            let sql = format!("SELECT * FROM ({}) WHERE {}", plan_.body, predicate);
            let part = db.with_file_name(format!(
                "{}.oraarrow-{}-p{:02}.parquet",
                db_name, safe_node, i
            ));
            let user = spec.user.clone();
            let password = spec.password.clone();
            let connect = spec.connect.clone();
            let schema = schema.clone();
            let scn = plan_.scn;
            let cancel = self.cancel.clone();
            handles.push(std::thread::spawn(move || -> Result<(usize, PathBuf), String> {
                let conn = oracle::Connection::connect(&user, &password, &connect)
                    .map_err(|e| format!("connect: {}", e))?;
                for nls in [
                    "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
                    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'",
                ] {
                    let _ = conn.execute(nls, &[]);
                }
                // Every session must read the same snapshot. If this fails the
                // session would silently read "now" instead, which is the exact
                // inconsistency the pin exists to prevent, so it is fatal.
                conn.execute(
                    "BEGIN DBMS_FLASHBACK.ENABLE_AT_SYSTEM_CHANGE_NUMBER(:1); END;",
                    &[&scn],
                )
                .map_err(|e| format!("could not pin session to SCN {}: {}", scn, e))?;
                let mut stmt = conn
                    .statement(&sql)
                    .prefetch_rows(5000)
                    .fetch_array_size(5000)
                    .build()
                    .map_err(|e| format!("prepare: {}", e))?;
                let rs = stmt
                    .query_as::<EncodeRow>(&[])
                    .map_err(|e| format!("query: {}", e))?;
                let n = Self::oracle_write_parquet_part(rs, &schema, &part, None, &cancel, None)
                    .map_err(|e| format!("band {}: {}", i, e))?;
                Ok((n, part))
            }));
        }

        let mut total = 0usize;
        let mut parts: Vec<PathBuf> = Vec::new();
        let mut failure: Option<String> = None;
        for h in handles {
            match h.join() {
                Ok(Ok((n, p))) => {
                    total += n;
                    parts.push(p);
                }
                Ok(Err(e)) => {
                    failure.get_or_insert(e);
                }
                Err(_) => {
                    failure.get_or_insert_with(|| "reader thread panicked".to_string());
                }
            }
        }
        if let Some(e) = failure {
            for p in &parts {
                let _ = std::fs::remove_file(p);
            }
            return Err(EngineError::Query(format!("oracle parallel read: {}", e)));
        }
        mark(&format!(
            "parallel read done: {} rows across {} bands",
            total,
            parts.len()
        ));

        let list = parts
            .iter()
            .map(|p| format!("'{}'", p.to_string_lossy().replace('\\', "/").replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        let kw = if spec.single_consumer { "VIEW" } else { "TABLE" };
        let projection =
            self.oracle_numeric_projection(db, &format!("[{}]", list), &schema, numeric_text, mark)?;
        let create = format!(
            "CREATE OR REPLACE {} {} AS SELECT {} FROM read_parquet([{}])",
            kw,
            plan::quote_ident(&spec.node_id),
            projection,
            list
        );
        self.run(Some(db), &create, false)?;
        if !spec.single_consumer {
            for p in &parts {
                let _ = std::fs::remove_file(p);
            }
        }
        Ok(format!("oracle: {} rows into {}", total, spec.node_id))
    }

    /// Give the text-carried NUMBER columns their real type.
    ///
    /// Oracle will not say how wide an unconstrained NUMBER actually is without
    /// reading every row, so the values travel as text and the question is
    /// answered here instead - against the Parquet just written, locally and
    /// columnar, where scanning a few columns costs milliseconds rather than a
    /// second pass over the wire. This is the same answer `read_json_auto` used
    /// to arrive at, reached without re-parsing every row.
    ///
    /// Returns the select list: `*` when there is nothing to do, otherwise
    /// `* REPLACE (CAST(...) AS col, ...)`.
    #[cfg(feature = "oracle")]
    fn oracle_numeric_projection(
        &self,
        db: &Path,
        parquet_ref: &str,
        schema: &arrow_schema::Schema,
        numeric_text: &[usize],
        mark: &dyn Fn(&str),
    ) -> Result<String, EngineError> {
        if numeric_text.is_empty() {
            return Ok("*".to_string());
        }
        let src = if parquet_ref.starts_with('[') {
            format!("read_parquet({})", parquet_ref)
        } else {
            format!("read_parquet('{}')", parquet_ref)
        };
        // One pass, all columns at once. Per column: whether any value has a
        // fraction or an exponent, the widest run of digits, and the deepest
        // fraction seen.
        let mut aggs = Vec::new();
        for &i in numeric_text {
            let c = plan::quote_ident(schema.field(i).name());
            // Digits LEFT of the point and digits RIGHT of it, counted
            // separately. Summing them is the only correct precision: a column
            // holding -17.5 and -0.25 has 3 digits in every value but needs
            // DECIMAL(4,2), and using the total would silently truncate it.
            let bare = format!("replace({}, '-', '')", c);
            aggs.push(format!(
                "max(CASE WHEN {c} IS NULL THEN 0 WHEN strpos({c}, '.') > 0 OR strpos(upper({c}), 'E') > 0 THEN 2 ELSE 1 END), max(CASE WHEN strpos({b}, '.') > 0 THEN strpos({b}, '.') - 1 ELSE length({b}) END), max(CASE WHEN strpos({c}, '.') > 0 THEN length({c}) - strpos({c}, '.') ELSE 0 END), max(CASE WHEN strpos(upper({c}), 'E') > 0 THEN 1 ELSE 0 END)",
                c = c,
                b = bare
            ));
        }
        let sql = format!("SELECT {} FROM {}", aggs.join(", "), src);
        let out = self.run(Some(db), &sql, true)?;
        let row: Vec<serde_json::Value> = serde_json::from_str::<Vec<JsonValue>>(&out)
            .ok()
            .and_then(|rows| rows.into_iter().next())
            .and_then(|r| r.as_object().map(|o| o.values().cloned().collect()))
            .unwrap_or_default();
        if row.len() < numeric_text.len() * 4 {
            // Could not read the probe back; leaving the columns as text is
            // wrong but silent, so say so and let them through untyped rather
            // than guessing a width.
            mark("could not probe unconstrained NUMBER widths; leaving them as text");
            return Ok("*".to_string());
        }
        let num = |v: &JsonValue| -> i64 {
            v.as_i64()
                .or_else(|| v.as_f64().map(|f| f as i64))
                .or_else(|| v.as_str().and_then(|t| t.parse().ok()))
                .unwrap_or(0)
        };
        let mut replaces = Vec::new();
        for (n, &i) in numeric_text.iter().enumerate() {
            let kind = num(&row[n * 4]);
            let int_digits = num(&row[n * 4 + 1]);
            let scale = num(&row[n * 4 + 2]);
            let exponent = num(&row[n * 4 + 3]) == 1;
            let digits = int_digits + scale;
            let name = schema.field(i).name();
            let c = plan::quote_ident(name);
            // Scientific notation cannot be pinned to a decimal width, and a
            // value past 38 digits does not fit one, so both take DOUBLE - the
            // same answer the old path reached.
            let ty = if kind == 0 {
                "BIGINT".to_string()
            } else if exponent || digits > 38 {
                "DOUBLE".to_string()
            } else if kind == 1 {
                if int_digits <= 18 {
                    "BIGINT".to_string()
                } else {
                    format!("DECIMAL({},0)", int_digits.max(1))
                }
            } else {
                let p = digits.clamp(1, 38);
                format!("DECIMAL({},{})", p, scale.clamp(0, p - 1).max(0))
            };
            // A strict CAST, not TRY_CAST. The width above was measured from
            // every value in the column, so it cannot overflow; if it ever does,
            // the run must stop and say so rather than turn the row into a NULL
            // nobody notices.
            replaces.push(format!("CAST({} AS {}) AS {}", c, ty, c));
        }
        mark(&format!("typed {} text-carried NUMBER column(s)", replaces.len()));
        Ok(format!("* REPLACE ({})", replaces.join(", ")))
    }

    /// Drain one Oracle result set into one parquet file, converting through
    /// Arrow. Shared by the single-session read and by every band of a parallel
    /// read, so both produce the same output for the same rows and there is
    /// exactly one place the type conversion can be wrong.
    #[cfg(feature = "oracle")]
    fn oracle_write_parquet_part(
        rs: oracle::ResultSet<'_, EncodeRow>,
        schema: &std::sync::Arc<arrow_schema::Schema>,
        path: &Path,
        compression: Option<&str>,
        cancel: &std::sync::atomic::AtomicBool,
        mark: Option<&dyn Fn(&str)>,
    ) -> Result<usize, EngineError> {
        use std::sync::atomic::Ordering;
        use arrow_array::builder::*;
        use arrow_schema::{DataType, TimeUnit};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::{EnabledStatistics, WriterProperties};

        let ncols = schema.fields().len().max(1);
        // Size the batch and the row group by CELLS, not rows. A fixed row
        // count silently scales the buffered working set with table width: at
        // 65 536 rows a 40-column table buffers 2.6M cells, but a 232-column
        // one buffers 15M, times the channel depth, which stops fitting in
        // cache and starts costing allocator and page-fault time. Wide fact
        // tables are exactly the case #221 is about.
        let batch_rows = (2_000_000 / ncols).clamp(1024, 65_536);
        // Same reasoning for the row group, plus: DuckDB reads a parquet row
        // group per thread, so a single giant group pins the downstream scan
        // to one core. Measured on 1M x 40, one 1M-row group made the read-back
        // 1.9s and 128K groups 0.8s.
        let row_group = (16_000_000 / ncols).clamp(8192, 131_072);
        let file = std::fs::File::create(path)
            .map_err(|e| EngineError::Query(format!("oracle: temp parquet: {}", e)))?;
        // Left uncompressed on purpose: this file is written once and read
        // once, by DuckDB, on the same machine. Measured at 232 columns,
        // snappy and zstd on the intermediate changed the read-back by less
        // than run-to-run noise and zstd cost 5s more to write.
        // A temp file that DuckDB reads once on the same machine is left
        // uncompressed on purpose (measured: snappy and zstd move the read-back
        // less than run-to-run noise, and zstd costs 5s more to write). When
        // this IS the user's output file, honour what they asked the sink for.
        let codec = match compression.map(|c| c.to_ascii_uppercase()).as_deref() {
            None | Some("UNCOMPRESSED") | Some("NONE") | Some("") => {
                parquet::basic::Compression::UNCOMPRESSED
            }
            Some("SNAPPY") => parquet::basic::Compression::SNAPPY,
            Some("GZIP") => {
                parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default())
            }
            Some("LZ4") | Some("LZ4_RAW") => parquet::basic::Compression::LZ4_RAW,
            // ZSTD is the Parquet sink's default, so this is the common case.
            _ => parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap(),
            ),
        };
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_size(row_group)
            .set_compression(codec)
            // The default 1 MB dictionary budget is per column, and a wide fact
            // table full of repeated text blows through it immediately; once it
            // does, the column falls back to PLAIN for the rest of the row group
            // and stops compressing. Measured on 232 columns: the default turned
            // a 324 MB file into 910 MB.
            .set_dictionary_page_size_limit(16 * 1024 * 1024)
            .set_write_batch_size(8192)
            .build();
        let wschema = schema.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel::<arrow_array::RecordBatch>(4);
        let writer = std::thread::spawn(move || -> Result<usize, String> {
            let mut w = ArrowWriter::try_new(file, wschema, Some(props)).map_err(|e| e.to_string())?;
            let mut n = 0usize;
            for batch in rx {
                n += batch.num_rows();
                w.write(&batch).map_err(|e| e.to_string())?;
            }
            w.close().map_err(|e| e.to_string())?;
            Ok(n)
        });

        // One builder per column, reused across batches. Concretely typed
        // rather than Box<dyn ArrayBuilder>: the append path runs once per
        // cell (40M times on a 1M-row x 40-col pull), and a downcast_mut per
        // cell was pure overhead on top of a dispatch the column type already
        // decides once.
        let builders: Vec<OraCol> = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                // Pre-sized: finish() hands its buffers to the RecordBatch and
                // leaves the builder empty, so every batch re-grows from zero.
                // Without a capacity hint that is a doubling realloc-and-copy
                // chain per column per batch.
                DataType::Int64 => OraCol::I64(Int64Builder::with_capacity(batch_rows)),
                DataType::Float64 => OraCol::F64(Float64Builder::with_capacity(batch_rows)),
                DataType::Float32 => OraCol::F32(Float32Builder::with_capacity(batch_rows)),
                DataType::Utf8 => {
                    OraCol::Str(StringBuilder::with_capacity(batch_rows, batch_rows * 16))
                }
                DataType::Binary => {
                    OraCol::Bin(BinaryBuilder::with_capacity(batch_rows, batch_rows * 16))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    OraCol::Ts(TimestampMicrosecondBuilder::with_capacity(batch_rows))
                }
                DataType::Decimal128(p, s) => OraCol::Dec(
                    Decimal128Builder::with_capacity(batch_rows)
                        .with_precision_and_scale(*p, *s)
                        .unwrap(),
                    *s,
                ),
                other => unreachable!("oracle_arrow_schema admitted {:?}", other),
            })
            .collect();

        let mut in_batch = 0usize;
        let mut total = 0usize;
        let mut send_err: Option<String> = None;

        // Optional attribution of the loop's two halves: waiting on ODPI for the
        // next row, versus our own Arrow encoding. Off unless
        // DUCKLE_ORACLE_TRACE_TIMING is set, because it costs an Instant::now()
        // pair per row. Without it there is no way to tell a slow server or link
        // apart from a slow encoder, and the two want opposite fixes.
        let trace_timing = std::env::var("DUCKLE_ORACLE_TRACE_TIMING")
            .map(|v| !v.is_empty() && v != "0")
            .unwrap_or(false);
        // From here the builders live in the thread-local sink, so each row can
        // be encoded inside `RowValue::get` while it is still borrowed.
        let guard = EncodeGuard::install(builders, trace_timing);
        let loop_start = std::time::Instant::now();
        for row_res in rs {
            if cancel.load(Ordering::Relaxed) {
                drop(tx);
                let _ = writer.join();
                return Err(EngineError::Cancelled);
            }
            // The row was already encoded as a side effect of `get`; this only
            // surfaces a driver-level failure.
            row_res.map_err(|e| EngineError::Query(format!("oracle row: {}", e)))?;
            if let Some(e) = guard.with(|s| s.err.take()) {
                return Err(e);
            }
            in_batch += 1;
            total += 1;
            if in_batch >= batch_rows {
                let batch = guard.with(|s| Self::finish_batch(schema, &mut s.builders))?;
                in_batch = 0;
                if tx.send(batch).is_err() {
                    send_err = Some("parquet writer stopped early".into());
                    break;
                }
                if let Some(m) = mark {
                    if total % (batch_rows * 8) == 0 {
                        m(&format!("{} rows encoded", total));
                    }
                }
            }
        }
        if send_err.is_none() && in_batch > 0 {
            let batch = guard.with(|s| Self::finish_batch(schema, &mut s.builders))?;
            let _ = tx.send(batch);
        }
        if trace_timing {
            if let Some(m) = mark {
                // Encode is timed inside `get`; whatever the loop spent beyond
                // that it spent waiting on the driver.
                let encode_ms = guard.with(|s| s.encode_nanos) / 1_000_000;
                let loop_ms = loop_start.elapsed().as_millis();
                m(&format!(
                    "timing: oracle fetch {}ms, arrow encode {}ms over {} rows",
                    loop_ms.saturating_sub(encode_ms),
                    encode_ms,
                    total
                ));
            }
        }
        drop(guard);
        drop(tx);
        let written = writer
            .join()
            .map_err(|_| EngineError::Query("oracle: parquet writer thread panicked".into()))?
            .map_err(|e| EngineError::Query(format!("oracle: write parquet: {}", e)))?;
        if let Some(e) = send_err {
            return Err(EngineError::Query(format!("oracle: {}", e)));
        }
        Ok(written)
    }

    fn finish_batch(
        schema: &std::sync::Arc<arrow_schema::Schema>,
        builders: &mut [OraCol],
    ) -> Result<arrow_array::RecordBatch, EngineError> {
        let arrays: Vec<arrow_array::ArrayRef> =
            builders.iter_mut().map(|b| b.finish()).collect();
        arrow_array::RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| EngineError::Query(format!("oracle: build arrow batch: {}", e)))
    }

    /// Map Oracle's declared column types to a fixed Arrow schema, or None if
    /// ANY column is ambiguous (#221).
    ///
    /// Returning None is the safety valve. The NDJSON path types a column from
    /// the VALUES it sees (a NUMBER becomes an integer, a double or a string
    /// depending on precision), which is why it needs `sample_size=-1`. Arrow
    /// must commit to one type per column up front, so this only claims the
    /// columns whose Oracle declaration already pins the type. Everything else
    /// - unconstrained NUMBER (reported as Number(0, -127), which is what
    /// COUNT/SUM expressions produce), LOBs, TZ-carrying timestamps, object
    /// types - falls back to the old path unchanged.
    /// The Oracle-type to Arrow-type decision, in one place so a test can
    /// exercise the real thing. `true` in the second slot means the column
    /// travels as text and gets its real numeric type after the write.
    #[cfg(feature = "oracle")]
    fn oracle_arrow_type(t: &oracle::sql_type::OracleType) -> Option<(arrow_schema::DataType, bool)> {
        use arrow_schema::{DataType, TimeUnit};
        use oracle::sql_type::OracleType;
        Some(match t {
            // A NUMBER with no precision - reported as Number(0, -127), and the
            // single most common thing in an Oracle warehouse. Its real width is
            // a property of the DATA, not the declaration, so it travels as text
            // and is typed from the values afterwards. That reproduces what the
            // NDJSON path got from read_json_auto without re-parsing every row.
            OracleType::Number(0, _) | OracleType::Float(_) => (DataType::Utf8, true),
            // A negative scale means Oracle rounds left of the point
            // (NUMBER(5,-2) stores hundreds), which an Arrow decimal cannot
            // express, so these travel as text too.
            OracleType::Number(_, s) if *s < 0 => (DataType::Utf8, true),
            // Fits i64 exactly: NUMBER(18) max is 999_999_999_999_999_999.
            OracleType::Number(p, 0) if *p >= 1 && *p <= 18 => (DataType::Int64, false),
            // Wider integers and every scaled NUMBER become exact decimals. The
            // old path degraded these to DOUBLE (losing digits beyond ~15, the
            // bug behind #196) or to VARCHAR; DECIMAL is what the column is.
            OracleType::Number(p, s) if *p >= 1 && *p <= 38 && *s >= 0 && (*s as u8) <= *p => {
                (DataType::Decimal128(*p, *s as i8), false)
            }
            OracleType::Varchar2(_)
            | OracleType::NVarchar2(_)
            | OracleType::Char(_)
            | OracleType::NChar(_) => (DataType::Utf8, false),
            // Oracle DATE carries a time component, so it is a timestamp.
            OracleType::Date | OracleType::Timestamp(_) => {
                (DataType::Timestamp(TimeUnit::Microsecond, None), false)
            }
            OracleType::BinaryDouble => (DataType::Float64, false),
            OracleType::BinaryFloat => (DataType::Float32, false),
            OracleType::Raw(_) => (DataType::Binary, false),
            // LOBs and zoned timestamps still have no exact mapping.
            _ => return None,
        })
    }

    /// Integer digits and fraction digits of an Oracle NUMBER's text form.
    ///
    /// Returns None for anything that is not plain decimal text, which is the
    /// signal to carry the column as text instead. Leading zeros do not count
    /// as integer digits, so "0.5" is (0, 1) and needs DECIMAL(1,1), while
    /// "-17.5" is (2, 1) and needs DECIMAL(3,1). Counting total characters
    /// instead is exactly the bug that once turned -17.5 into NULL.
    #[cfg(feature = "oracle")]
    fn oracle_number_width(s: &str) -> Option<(u32, u32)> {
        let t = s.trim().as_bytes();
        let mut i = 0usize;
        if matches!(t.first(), Some(b'-') | Some(b'+')) {
            i = 1;
        }
        let (mut int_digits, mut scale) = (0u32, 0u32);
        let mut seen_dot = false;
        let mut leading = true;
        let mut any = false;
        while i < t.len() {
            match t[i] {
                c @ b'0'..=b'9' => {
                    any = true;
                    if seen_dot {
                        scale += 1;
                    } else if !(leading && c == b'0') {
                        leading = false;
                        int_digits += 1;
                    }
                }
                b'.' if !seen_dot => seen_dot = true,
                _ => return None, // scientific notation, or not a number at all
            }
            i += 1;
        }
        if !any {
            return None;
        }
        Some((int_digits, scale))
    }

    /// Measure the columns Oracle would not pin a width for, so they can be
    /// typed before the write instead of after it.
    ///
    /// A bare `NUMBER` has no declared width, so its real one is a property of
    /// the values. Reading it as text and typing it afterwards is correct but
    /// costs a whole second pass over the parquet, because that is the pass a
    /// direct write skips. Reading just those columns first is far cheaper:
    /// measured on a 236-column, 1.47M-row table, the four ambiguous columns
    /// take 2.9s to fetch on their own against ~22s for the second pass.
    ///
    /// Returns the schema with those columns given real DECIMAL types, or None
    /// to leave them as text - which happens if any value is not plain decimal
    /// text, if a column is entirely NULL, or if the width exceeds DECIMAL's 38
    /// digits. Correctness rests on the caller having pinned a snapshot, so the
    /// measurement and the extract cannot see different rows.
    #[cfg(feature = "oracle")]
    fn oracle_probe_text_widths(
        conn: &oracle::Connection,
        query: &str,
        infos: &[oracle::ColumnInfo],
        schema: &arrow_schema::Schema,
        numeric_text: &[usize],
        mark: &dyn Fn(&str),
    ) -> Option<arrow_schema::Schema> {
        use arrow_schema::{DataType, Field};

        let names: Vec<&str> = numeric_text
            .iter()
            .map(|&i| infos.get(i).map(|c| c.name()).unwrap_or_default())
            .collect();
        if names.iter().any(|n| n.is_empty()) {
            return None;
        }
        let select = names
            .iter()
            .map(|n| format!("\"{}\"", n.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let probe_sql = format!("SELECT {} FROM ({})", select, query);

        PROBE_WIDTHS.with(|c| {
            *c.borrow_mut() = vec![ProbeWidth::default(); numeric_text.len()];
        });
        let started = Instant::now();
        let mut stmt = conn
            .statement(&probe_sql)
            .prefetch_rows(5000)
            .fetch_array_size(5000)
            .build()
            .ok()?;
        let rs = stmt.query_as::<ProbeRow>(&[]).ok()?;
        for row in rs {
            if row.is_err() {
                return None;
            }
        }
        let widths = PROBE_WIDTHS.with(|c| c.borrow().clone());

        let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();
        for (slot, &col) in widths.iter().zip(numeric_text.iter()) {
            if slot.unusable {
                mark(&format!(
                    "column {} stays text: a value is not plain decimal",
                    col + 1
                ));
                return None;
            }
            // Every value NULL means there is nothing to lose whatever width we
            // pick, so the column is typeable rather than a reason to give up.
            // Falling back here would cost the whole second pass over one empty
            // column.
            // Precision is integer digits plus scale. Deriving it from the
            // total digit count is what once gave -17.5 a DECIMAL(3,2) and
            // turned it into NULL, so it is spelled out here.
            let precision = slot.int_digits.max(1) + slot.scale;
            if precision > 38 {
                mark(&format!(
                    "column {} stays text: needs {} digits, DECIMAL holds 38",
                    col + 1,
                    precision
                ));
                return None;
            }
            let f = fields.get(col)?;
            // A whole number that fits an i64 becomes one, matching what a
            // declared NUMBER(p,0) already maps to. Otherwise downstream would
            // see the same integers as DECIMAL(6,0) purely because Oracle
            // happened not to declare a width.
            let ty = if slot.scale == 0 && precision <= 18 {
                DataType::Int64
            } else {
                DataType::Decimal128(precision as u8, slot.scale as i8)
            };
            fields[col] = Field::new(f.name(), ty, f.is_nullable());
        }
        let all_null = widths.iter().filter(|w| !w.seen).count();
        mark(&format!(
            "measured {} ambiguous column(s) in {:?}; typed before the write{}",
            numeric_text.len(),
            started.elapsed(),
            if all_null > 0 {
                format!(" ({} held only NULLs)", all_null)
            } else {
                String::new()
            }
        ));
        Some(arrow_schema::Schema::new(fields))
    }

    fn oracle_arrow_schema(
        infos: &[oracle::ColumnInfo],
    ) -> Option<(arrow_schema::Schema, Vec<usize>)> {
        use arrow_schema::Field;
        let mut fields = Vec::with_capacity(infos.len());
        // Columns carried as text because Oracle's declaration does not pin a
        // width. DuckDB gives them their real type after the write.
        let mut numeric_text = Vec::new();
        for (i, c) in infos.iter().enumerate() {
            let (dt, as_text) = Self::oracle_arrow_type(c.oracle_type())?;
            if as_text {
                numeric_text.push(i);
            }
            fields.push(Field::new(c.name(), dt, c.nullable()));
        }
        Some((arrow_schema::Schema::new(fields), numeric_text))
    }

    /// Parse Oracle's decimal text into the scaled i128 a Decimal128 column
    /// stores. "123.45" at scale 2 is 12345. Returns None when the value does
    /// not fit, so the caller can fail loudly rather than truncate silently.
    /// Scale an Oracle NUMBER's text form into the i128 a Decimal128 column wants.
    ///
    /// This runs once per cell for every NUMBER the driver cannot hand back as a
    /// native Int64 - which is every NUMBER with a scale. On a wide fact table
    /// that is most of the columns: a 236-column table with 60 scaled NUMBER
    /// columns runs this 88 million times in a single 1.5M-row extract. It
    /// therefore does not allocate. The previous version built a digits String
    /// per cell, and that one allocation was the single largest cost in the
    /// extract loop.
    ///
    /// Semantics are unchanged, deliberately, so this is a pure speedup:
    /// fraction digits beyond `scale` are truncated rather than rounded, a
    /// leading sign is honoured, and anything that is not digits with at most
    /// one decimal point - scientific notation, most obviously - returns None so
    /// the caller can fall back. Note that an all-zero value like "0.00" must
    /// come back as 0 rather than None; the old code reached that through
    /// `trim_start_matches('0')` leaving an empty string and `unwrap_or(0)`.
    fn oracle_decimal_to_i128(s: &str, scale: i8) -> Option<i128> {
        let t = s.trim().as_bytes();
        let mut i = 0usize;
        let neg = match t.first() {
            Some(b'-') => {
                i = 1;
                true
            }
            Some(b'+') => {
                i = 1;
                false
            }
            _ => false,
        };
        let want = scale.max(0) as usize;
        let mut v: i128 = 0;
        let mut seen_dot = false;
        let mut frac = 0usize;
        while i < t.len() {
            match t[i] {
                c @ b'0'..=b'9' => {
                    if seen_dot {
                        // Digits past the requested scale are dropped, matching
                        // the old truncating slice of the fractional part.
                        if frac < want {
                            v = v.checked_mul(10)?.checked_add((c - b'0') as i128)?;
                            frac += 1;
                        }
                    } else {
                        v = v.checked_mul(10)?.checked_add((c - b'0') as i128)?;
                    }
                }
                b'.' if !seen_dot => seen_dot = true,
                _ => return None, // scientific notation etc: let the caller fall back
            }
            i += 1;
        }
        // Pad a short fraction out to the declared scale.
        for _ in frac..want {
            v = v.checked_mul(10)?;
        }
        Some(if neg { -v } else { v })
    }

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
                    // An unconstrained NUMBER (and COUNT/SUM expressions) is
                    // reported as Number(0, -127), so integer values reach this
                    // arm rather than the scale==0 fast-path. Emit those as JSON
                    // integers; otherwise 42 becomes the float 42.0 (typing the
                    // column DOUBLE), or VARCHAR when mixed with >15-digit values.
                    let t = s.trim();
                    if !t.contains(&['.', 'e', 'E'][..]) {
                        if let Ok(n) = t.parse::<i64>() {
                            return JsonValue::from(n);
                        }
                    }
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
        use adbc_driver_manager::ManagedDriver;
        use adbc_core::{
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
                // Read PATH, decide, and write it back under one lock. The
                // check and the set are a read-modify-write, so two runs
                // starting together with different drivers could both read the
                // old PATH and the second write would drop the first driver's
                // directory - which surfaces much later as a driver that fails
                // to load. Only matters now that runs can overlap.
                static PATH_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
                let _held = PATH_LOCK.lock().unwrap_or_else(|p| p.into_inner());
                let cur = std::env::var("PATH").unwrap_or_default();
                let sep = if cfg!(windows) { ';' } else { ':' };
                // Only prepend the driver dir if it isn't already on PATH:
                // re-prepending on every run (e.g. under a long-lived `duckle
                // serve`) grows PATH unboundedly toward the OS env-block limit.
                let already = cur
                    .split(sep)
                    .any(|p| !p.is_empty() && Path::new(p) == parent);
                if !already {
                    std::env::set_var(
                        "PATH",
                        format!("{}{}{}", parent.display(), sep, cur),
                    );
                }
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

    /// snk.adbc / snk.teradata: COPY the upstream view to a Parquet temp file,
    /// then bulk-ingest it into the target table through a prebuilt ADBC driver
    /// loaded at runtime (the ADBC bind_stream + ingest API: no per-row
    /// round-trips, no in-process DuckDB write). Bulk ingest is
    /// create/append/replace only - upsert is rejected at plan time. Not
    /// feature-gated: adbc_core links unconditionally; a missing or incompatible
    /// driver surfaces as a clear engine error at load time.
    pub(crate) fn run_adbc_sink(
        &self,
        db: &Path,
        spec: &plan::AdbcSinkSpec,
    ) -> Result<String, EngineError> {
        use adbc_driver_manager::ManagedDriver;
        use adbc_core::{
            options::{AdbcVersion, IngestMode, OptionDatabase, OptionStatement, OptionValue},
            Connection, Database, Driver, Optionable, Statement,
        };
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // 1. COPY the upstream view to a temp parquet once (already typed), so
        // the ingest streams Arrow batches straight from disk.
        let safe: String = spec
            .from_view
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let db_name = db
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parquet_path = db.with_file_name(format!("{}.adbc-snk-{}.parquet", db_name, safe));
        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let copy = format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT parquet)",
            plan::quote_ident(&spec.from_view),
            ppath
        );
        self.run(Some(db), &copy, false)?;

        // 2. Load the ADBC driver. Prepend the driver's own directory to PATH so
        // a self-contained bundled driver folder loads without extra setup.
        let driver_path = Path::new(&spec.driver);
        if let Some(parent) = driver_path.parent() {
            if !parent.as_os_str().is_empty() {
                let cur = std::env::var("PATH").unwrap_or_default();
                let sep = if cfg!(windows) { ';' } else { ':' };
                let already = cur
                    .split(sep)
                    .any(|p| !p.is_empty() && Path::new(p) == parent);
                if !already {
                    std::env::set_var("PATH", format!("{}{}{}", parent.display(), sep, cur));
                }
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

        // 3. Configure the bulk-ingest target + mode. "overwrite" replaces the
        // table; "append" creates it if missing then appends.
        let mode = if spec.mode == "overwrite" {
            IngestMode::Replace
        } else {
            IngestMode::CreateAppend
        };
        stmt.set_option(OptionStatement::IngestMode, mode.into())
            .map_err(|e| EngineError::Query(format!("adbc: set ingest mode: {}", e)))?;
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String(spec.table.clone()),
        )
        .map_err(|e| EngineError::Query(format!("adbc: set target table: {}", e)))?;
        if let Some(schema) = spec.schema.as_deref().filter(|s| !s.is_empty()) {
            stmt.set_option(
                OptionStatement::TargetDbSchema,
                OptionValue::String(schema.to_string()),
            )
            .map_err(|e| EngineError::Query(format!("adbc: set target schema: {}", e)))?;
        }
        if let Some(catalog) = spec.catalog.as_deref().filter(|s| !s.is_empty()) {
            stmt.set_option(
                OptionStatement::TargetCatalog,
                OptionValue::String(catalog.to_string()),
            )
            .map_err(|e| EngineError::Query(format!("adbc: set target catalog: {}", e)))?;
        }

        // 4. Stream the parquet's Arrow batches into the driver and execute.
        let file = std::fs::File::open(&parquet_path)
            .map_err(|e| EngineError::Query(format!("adbc: open temp parquet: {}", e)))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| EngineError::Query(format!("adbc: read temp parquet: {}", e)))?
            .build()
            .map_err(|e| EngineError::Query(format!("adbc: parquet reader: {}", e)))?;
        stmt.bind_stream(Box::new(reader))
            .map_err(|e| EngineError::Query(format!("adbc: bind rows: {}", e)))?;
        let affected = stmt
            .execute_update()
            .map_err(|e| EngineError::Query(format!("adbc: ingest into {}: {}", spec.table, e)))?;
        let _ = std::fs::remove_file(&parquet_path);
        match affected {
            Some(n) if n >= 0 => Ok(format!("adbc: ingested {} rows into {}", n, spec.table)),
            _ => Ok(format!("adbc: ingested into {}", spec.table)),
        }
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
        // RESET search_path after the COPY: a custom-SQL attach source (#117)
        // sets `search_path='duckle_src'` in its prelude so the body's
        // unqualified catalog names resolve during the COPY; the run-db VIEW
        // that follows must be created back in the default (writable) catalog,
        // not the read-only attached one. A no-op for every other spec (none
        // touch search_path), so it is unconditional.
        let sql = format!(
            "{}COPY ({}) TO '{}' (FORMAT PARQUET); RESET search_path; \
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
            // XML: tiberius decodes it to ColumnData::Xml, which the &str
            // accessor does NOT match, so an xml column used to fall through to
            // the catch-all below and always read back NULL (#141 follow-up:
            // "some columns show empty/null"). Read it through the dedicated
            // XmlData accessor and emit its serialized text.
            ColumnType::Xml => row
                .try_get::<&tiberius::xml::XmlData, _>(i)
                .ok()
                .flatten()
                .map(|x| JsonValue::String(x.to_string()))
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
                let mut builder = scylla::client::session_builder::SessionBuilder::new();
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
                        .query_unpaged(stmt, &[])
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
        // Stream rows straight to the NDJSON writer instead of collecting the
        // whole result set into a Vec<JsonValue> on top of the driver's own row
        // buffer, then walking it again (mirrors the SQL Server source).
        let writer = JsonLinesWriter::open(&spec.node_id)?;
        let bin = self.binary();
        let count: usize = rt
            .block_on(async move {
                let mut writer = writer;
                let mut builder = scylla::client::session_builder::SessionBuilder::new();
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
                    .query_unpaged(spec.query.clone(), &[])
                    .await
                    .map_err(|e| format!("query: {}", e))?;
                // The result arrives undeserialised: the column specs and the rows both
                // come from the row view rather than off the result itself.
                let result = result
                    .into_rows_result()
                    .map_err(|e| format!("query: {}", e))?;
                let cols: Vec<String> = result
                    .column_specs()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect();
                let rows = result
                    .rows::<scylla::value::Row>()
                    .map_err(|e| format!("query: {}", e))?;
                let mut count = 0usize;
                for row in rows {
                    let row = row.map_err(|e| format!("row: {}", e))?;
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
            .map_err(|e| EngineError::Query(format!("cassandra source: {}", e)))?;
        Ok(format!(
            "cassandra: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// Teradata source over the Teradata ODBC driver. Connects with the
    /// supplied ODBC connection string, runs the query, and streams the result
    /// into one NDJSON file as text, then materializes it with per-column typed
    /// casts (read all VARCHAR, then TRY_CAST each column to its DuckDB type) so
    /// numbers / decimals / dates / timestamps keep their types - the same
    /// typed-finalize the Snowflake source uses. (#122)
    /// Shared ODBC read: connect, describe the result columns, stream the rows
    /// out as text and finalize with per-column typed casts. Teradata and DB2
    /// differ only in the driver behind the connection string and the name in
    /// the messages, so `family` is the only thing that varies.
    #[cfg(feature = "odbc")]
    pub(crate) fn run_odbc_source(
        &self,
        db: &Path,
        family: &str,
        conn_str: &str,
        query: &str,
        batch_rows: usize,
        node_id: &str,
    ) -> Result<String, EngineError> {
        use odbc_api::buffers::TextRowSet;
        use odbc_api::{ColumnDescription, ConnectionOptions, Cursor, Environment, ResultSetMetadata};

        let env = Environment::new()
            .map_err(|e| EngineError::Query(format!("{}: ODBC environment: {}", family, e)))?;
        let conn = env
            .connect_with_connection_string(conn_str, ConnectionOptions::default())
            .map_err(|e| EngineError::Query(format!("{}: connect failed: {}", family, e)))?;
        let mut cursor = conn
            .execute(query, (), None)
            .map_err(|e| EngineError::Query(format!("{}: query failed: {}", family, e)))?
            .ok_or_else(|| {
                EngineError::Query(format!("{}: the query returned no result set", family))
            })?;

        // Column metadata: build the index-aligned name list, the read_json
        // columns map (everything VARCHAR), and the typed projection.
        let ncols = cursor
            .num_result_cols()
            .map_err(|e| EngineError::Query(format!("{}: column count: {}", family, e)))?
            as u16;
        let mut names: Vec<String> = Vec::with_capacity(ncols as usize);
        let mut columns_spec_parts: Vec<String> = Vec::with_capacity(ncols as usize);
        let mut select_parts: Vec<String> = Vec::with_capacity(ncols as usize);
        let mut used_names: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cd = ColumnDescription::default();
        for i in 1..=ncols {
            cursor
                .describe_col(i, &mut cd)
                .map_err(|e| EngineError::Query(format!("{}: describe column {}: {}", family, i, e)))?;
            let raw = cd.name_to_string().unwrap_or_else(|_| format!("col{}", i));
            let name = unique_column_name(&raw, &mut used_names);
            let ident = plan::quote_ident(&name);
            columns_spec_parts.push(format!("'{}': 'VARCHAR'", name.replace('\'', "''")));
            match odbc_type_to_duckdb(&cd.data_type) {
                Some(ty) => select_parts.push(format!(
                    "TRY_CAST(NULLIF({i}, '') AS {ty}) AS {i}",
                    i = ident,
                    ty = ty
                )),
                None => select_parts.push(format!("{i} AS {i}", i = ident)),
            }
            names.push(name);
        }
        let columns_spec = columns_spec_parts.join(", ");
        let select_list = select_parts.join(", ");

        // Fetch in batches as text, writing each row to the NDJSON file. ODBC
        // text rendering keeps the source's textual form; the typed finalize
        // casts each column afterwards.
        let mut writer = JsonLinesWriter::open(node_id)?;
        let batch = batch_rows.max(1);
        let buffers = TextRowSet::for_cursor(batch, &mut cursor, Some(65536))
            .map_err(|e| EngineError::Query(format!("{}: alloc buffers: {}", family, e)))?;
        let mut rows_cursor = cursor
            .bind_buffer(buffers)
            .map_err(|e| EngineError::Query(format!("{}: bind buffers: {}", family, e)))?;
        let mut count = 0usize;
        while let Some(view) = rows_cursor
            .fetch()
            .map_err(|e| EngineError::Query(format!("{}: fetch: {}", family, e)))?
        {
            self.check_cancelled()?;
            for r in 0..view.num_rows() {
                let mut obj = serde_json::Map::with_capacity(names.len());
                for (c, name) in names.iter().enumerate() {
                    let v = match view.at(c, r) {
                        Some(bytes) => {
                            JsonValue::String(String::from_utf8_lossy(bytes).into_owned())
                        }
                        None => JsonValue::Null,
                    };
                    obj.insert(name.clone(), v);
                }
                writer.write_row(&JsonValue::Object(obj))?;
                count += 1;
            }
        }
        drop(rows_cursor);
        drop(conn);
        writer.finalize_typed(self.binary(), db, node_id, &columns_spec, &select_list)?;
        Ok(format!(
            "{}: materialized {} rows into {}",
            family, count, node_id
        ))
    }

    /// Teradata source over the Teradata ODBC driver (there is no DuckDB
    /// Teradata extension or native Rust driver).
    #[cfg(feature = "odbc")]
    pub(crate) fn run_teradata_source(
        &self,
        db: &Path,
        spec: &plan::TeradataSourceSpec,
    ) -> Result<String, EngineError> {
        self.run_odbc_source(
            db,
            "teradata",
            &spec.conn_str,
            &spec.query,
            spec.batch_rows,
            &spec.node_id,
        )
    }

    /// IBM DB2 source over the IBM Data Server ODBC driver. Same transport as
    /// Teradata; DB2 ships no DuckDB extension and no native Rust driver.
    #[cfg(feature = "odbc")]
    pub(crate) fn run_db2_source(
        &self,
        db: &Path,
        spec: &plan::Db2SourceSpec,
    ) -> Result<String, EngineError> {
        self.run_odbc_source(
            db,
            "db2",
            &spec.conn_str,
            &spec.query,
            spec.batch_rows,
            &spec.node_id,
        )
    }

    #[cfg(not(feature = "odbc"))]
    pub(crate) fn run_db2_source(
        &self,
        _db: &Path,
        _spec: &plan::Db2SourceSpec,
    ) -> Result<String, EngineError> {
        Err(EngineError::Config(
            "db2: this build was compiled without ODBC support (enable the `db2` feature)".into(),
        ))
    }

    #[cfg(not(feature = "odbc"))]
    pub(crate) fn run_teradata_source(
        &self,
        _db: &Path,
        _spec: &plan::TeradataSourceSpec,
    ) -> Result<String, EngineError> {
        Err(EngineError::Config(
            "teradata: this build was compiled without ODBC support (enable the `teradata` feature)".into(),
        ))
    }

    /// Teradata sink over the Teradata ODBC driver. Reads the upstream view and
    /// INSERTs each row through ODBC. Append creates the table if it is missing;
    /// overwrite clears it first. Teradata's VALUES clause is single-row, so
    /// rows are inserted one statement at a time (large loads should use
    /// Teradata's bulk utilities). No upsert. (#122)
    #[cfg(feature = "odbc")]
    pub(crate) fn run_teradata_sink(
        &self,
        db: &Path,
        spec: &plan::TeradataSinkSpec,
    ) -> Result<String, EngineError> {
        use odbc_api::{ConnectionOptions, Environment};

        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("teradata: 0 rows to insert into {}", spec.table));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => {
                return Err(EngineError::Query(
                    "teradata: upstream rows aren't JSON objects".into(),
                ));
            }
        };
        let col_types: std::collections::HashMap<String, String> =
            describe_columns(self, db, &spec.from_view).into_iter().collect();
        // Teradata delimited identifiers use double quotes (doubled to escape).
        let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let qualified = match &spec.database {
            Some(d) => format!("{}.{}", q(d), q(&spec.table)),
            None => q(&spec.table),
        };
        let col_defs = cols
            .iter()
            .map(|c| {
                let ty = duckdb_type_to_teradata(
                    col_types.get(c).map(|s| s.as_str()).unwrap_or("VARCHAR"),
                );
                format!("{} {}", q(c), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let cols_list = cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        let create_sql = format!("CREATE TABLE {} ({})", qualified, col_defs);

        let env = Environment::new()
            .map_err(|e| EngineError::Query(format!("teradata: ODBC environment: {}", e)))?;
        let conn = env
            .connect_with_connection_string(&spec.conn_str, ConnectionOptions::default())
            .map_err(|e| EngineError::Query(format!("teradata: connect failed: {}", e)))?;
        // Teradata has no CREATE TABLE IF NOT EXISTS, so create and tolerate the
        // "table already exists" error (3803).
        if let Err(e) = conn.execute(&create_sql, (), None) {
            let msg = e.to_string();
            if !(msg.contains("3803") || msg.to_lowercase().contains("already exists")) {
                return Err(EngineError::Query(format!("teradata: create table: {}", msg)));
            }
        }
        if spec.mode == "overwrite" {
            conn.execute(&format!("DELETE FROM {}", qualified), (), None)
                .map_err(|e| EngineError::Query(format!("teradata: clear table: {}", e)))?;
        }
        let mut total = 0usize;
        for row in &rows {
            self.check_cancelled()?;
            let obj = row.as_object();
            let vals: Vec<String> = cols
                .iter()
                .map(|c| {
                    let v = obj.and_then(|o| o.get(c)).unwrap_or(&JsonValue::Null);
                    sql_literal(v, col_types.get(c).map(|s| s.as_str()), Dialect::Teradata)
                })
                .collect();
            let stmt = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                qualified,
                cols_list,
                vals.join(", ")
            );
            conn.execute(&stmt, (), None)
                .map_err(|e| EngineError::Query(format!("teradata: insert: {}", e)))?;
            total += 1;
        }
        Ok(format!(
            "teradata: {} {} rows into {}",
            if spec.mode == "overwrite" { "overwrote with" } else { "inserted" },
            total,
            spec.table
        ))
    }

    #[cfg(not(feature = "odbc"))]
    pub(crate) fn run_teradata_sink(
        &self,
        _db: &Path,
        _spec: &plan::TeradataSinkSpec,
    ) -> Result<String, EngineError> {
        Err(EngineError::Config(
            "teradata: this build was compiled without ODBC support (enable the `teradata` feature)".into(),
        ))
    }

    /// IBM DB2 sink over ODBC: auto-create the target table from the upstream
    /// column types, then INSERT row by row. DB2 has no CREATE TABLE IF NOT
    /// EXISTS, so the create is attempted and "already exists" tolerated.
    #[cfg(feature = "odbc")]
    pub(crate) fn run_db2_sink(
        &self,
        db: &Path,
        spec: &plan::Db2SinkSpec,
    ) -> Result<String, EngineError> {
        use odbc_api::{ConnectionOptions, Environment};

        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("db2: 0 rows to insert into {}", spec.table));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => {
                return Err(EngineError::Query(
                    "db2: upstream rows aren't JSON objects".into(),
                ));
            }
        };
        let col_types: std::collections::HashMap<String, String> =
            describe_columns(self, db, &spec.from_view).into_iter().collect();
        // DB2 delimited identifiers use double quotes (doubled to escape).
        let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let qualified = match &spec.schema {
            Some(d) => format!("{}.{}", q(d), q(&spec.table)),
            None => q(&spec.table),
        };
        let col_defs = cols
            .iter()
            .map(|c| {
                let ty = duckdb_type_to_db2(
                    col_types.get(c).map(|s| s.as_str()).unwrap_or("VARCHAR"),
                );
                format!("{} {}", q(c), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let cols_list = cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        let create_sql = format!("CREATE TABLE {} ({})", qualified, col_defs);

        let env = Environment::new()
            .map_err(|e| EngineError::Query(format!("db2: ODBC environment: {}", e)))?;
        let conn = env
            .connect_with_connection_string(&spec.conn_str, ConnectionOptions::default())
            .map_err(|e| EngineError::Query(format!("db2: connect failed: {}", e)))?;
        // DB2 reports an existing table as SQLCODE -601 / SQLSTATE 42710.
        if let Err(e) = conn.execute(&create_sql, (), None) {
            let msg = e.to_string();
            let lower = msg.to_lowercase();
            if !(msg.contains("42710") || msg.contains("-601") || lower.contains("already exists"))
            {
                return Err(EngineError::Query(format!("db2: create table: {}", msg)));
            }
        }
        if spec.mode == "overwrite" {
            conn.execute(&format!("DELETE FROM {}", qualified), (), None)
                .map_err(|e| EngineError::Query(format!("db2: clear table: {}", e)))?;
        }
        let mut total = 0usize;
        for row in &rows {
            self.check_cancelled()?;
            let obj = row.as_object();
            let vals: Vec<String> = cols
                .iter()
                .map(|c| {
                    let v = obj.and_then(|o| o.get(c)).unwrap_or(&JsonValue::Null);
                    sql_literal(v, col_types.get(c).map(|s| s.as_str()), Dialect::Db2)
                })
                .collect();
            let stmt = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                qualified,
                cols_list,
                vals.join(", ")
            );
            conn.execute(&stmt, (), None)
                .map_err(|e| EngineError::Query(format!("db2: insert: {}", e)))?;
            total += 1;
        }
        Ok(format!(
            "db2: {} {} rows into {}",
            if spec.mode == "overwrite" { "overwrote with" } else { "inserted" },
            total,
            spec.table
        ))
    }

    #[cfg(not(feature = "odbc"))]
    pub(crate) fn run_db2_sink(
        &self,
        _db: &Path,
        _spec: &plan::Db2SinkSpec,
    ) -> Result<String, EngineError> {
        Err(EngineError::Config(
            "db2: this build was compiled without ODBC support (enable the `db2` feature)".into(),
        ))
    }

    /// Neo4j source over the HTTP Query API (`POST /db/{db}/query/v2`), which
    /// every Neo4j 5.x server and Aura exposes on the same port as Browser.
    /// Bolt would need a driver crate and a second wire protocol for no gain
    /// here: the API returns the whole result set as JSON, which is exactly
    /// what materializing a relation needs.
    ///
    /// The response is columnar - `{"data":{"fields":[..],"values":[[..]]}}` -
    /// so it is zipped back into one JSON object per row. Node and
    /// relationship values arrive as nested objects and are kept as-is, so
    /// DuckDB reads them as STRUCT rather than losing the properties.
    pub(crate) fn run_neo4j_source(
        &self,
        db: &Path,
        spec: &plan::Neo4jSourceSpec,
    ) -> Result<String, EngineError> {
        let url = format!(
            "{}/db/{}/query/v2",
            spec.endpoint.trim_end_matches('/'),
            spec.database
        );
        let body = serde_json::json!({
            "statement": spec.cypher,
            "parameters": spec.parameters.clone().unwrap_or_else(|| serde_json::json!({})),
        });
        let resp = match neo4j_request(spec.user.as_deref(), spec.password.as_deref(), &url)
            .send_json(body)
        {
            Ok(r) => r,
            Err(ureq::Error::Status(code, r)) => {
                return Err(EngineError::Query(format!(
                    "neo4j: HTTP {} on query: {}",
                    code,
                    neo4j_error_detail(r.into_string().unwrap_or_default())
                )));
            }
            Err(e) => return Err(EngineError::Query(format!("neo4j: HTTP transport: {}", e))),
        };
        let response: JsonValue = resp
            .into_json()
            .map_err(|e| EngineError::Query(format!("neo4j: response not JSON: {}", e)))?;
        let data = response.get("data");
        let fields: Vec<String> = data
            .and_then(|d| d.get("fields"))
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .enumerate()
                    .map(|(i, f)| match f.as_str() {
                        Some(s) if !s.is_empty() => s.to_string(),
                        _ => format!("col{}", i + 1),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let values = data
            .and_then(|d| d.get("values"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let rows: Vec<JsonValue> = values
            .iter()
            .map(|row| {
                let cells = row.as_array().cloned().unwrap_or_default();
                let mut obj = serde_json::Map::with_capacity(fields.len());
                for (i, name) in fields.iter().enumerate() {
                    obj.insert(name.clone(), cells.get(i).cloned().unwrap_or(JsonValue::Null));
                }
                JsonValue::Object(obj)
            })
            .collect();
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "neo4j: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// Neo4j sink: write upstream rows as nodes over the same Query API.
    /// Rows go up in batches as the `$rows` parameter and are expanded server
    /// side with UNWIND, so one round trip writes `batch_size` nodes rather
    /// than one statement per row.
    ///
    /// `merge_keys` picks MERGE over CREATE, so re-running a pipeline updates
    /// the matched nodes instead of duplicating them.
    pub(crate) fn run_neo4j_sink(
        &self,
        db: &Path,
        spec: &plan::Neo4jSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("neo4j: 0 rows to write to :{}", spec.label));
        }
        let url = format!(
            "{}/db/{}/query/v2",
            spec.endpoint.trim_end_matches('/'),
            spec.database
        );
        let cypher = match &spec.cypher {
            Some(c) => c.clone(),
            None if !spec.merge_keys.is_empty() => {
                let keys = spec
                    .merge_keys
                    .iter()
                    .map(|k| format!("{k}: row.{k}", k = cypher_ident(k)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "UNWIND $rows AS row MERGE (n:{} {{{}}}) SET n += row",
                    cypher_ident(&spec.label),
                    keys
                )
            }
            None => format!(
                "UNWIND $rows AS row CREATE (n:{}) SET n = row",
                cypher_ident(&spec.label)
            ),
        };

        let batch = spec.batch_size.max(1);
        let mut written = 0usize;
        for chunk in rows.chunks(batch) {
            self.check_cancelled()?;
            let body = serde_json::json!({
                "statement": cypher,
                "parameters": { "rows": chunk },
            });
            match neo4j_request(spec.user.as_deref(), spec.password.as_deref(), &url)
                .send_json(body)
            {
                Ok(_) => {}
                Err(ureq::Error::Status(code, r)) => {
                    return Err(EngineError::Query(format!(
                        "neo4j: HTTP {} writing the batch starting at row {}: {}",
                        code,
                        written,
                        neo4j_error_detail(r.into_string().unwrap_or_default())
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "neo4j: HTTP transport writing the batch starting at row {}: {}",
                        written, e
                    )))
                }
            }
            written += chunk.len();
        }
        Ok(if spec.label.is_empty() {
            format!("neo4j: wrote {} rows via the supplied cypher", written)
        } else {
            format!("neo4j: wrote {} rows as :{} nodes", written, spec.label)
        })
    }

    /// Turso / libSQL source over the HTTP pipeline API (`POST /v2/pipeline`).
    pub(crate) fn run_turso_source(
        &self,
        db: &Path,
        spec: &plan::TursoSourceSpec,
    ) -> Result<String, EngineError> {
        let url = format!("{}/v2/pipeline", turso_base_url(&spec.url));
        let body = serde_json::json!({
            "requests": [
                { "type": "execute", "stmt": { "sql": spec.query } },
                { "type": "close" },
            ]
        });
        let response = turso_send(spec.auth_token.as_deref(), &url, body)?;
        let result = response
            .get("results")
            .and_then(|v| v.as_array())
            .and_then(|results| {
                results.iter().find_map(|r| {
                    let resp = r.get("response")?;
                    if resp.get("type").and_then(|v| v.as_str()) == Some("execute") {
                        resp.get("result")
                    } else {
                        None
                    }
                })
            })
            .ok_or_else(|| {
                EngineError::Query("turso: the pipeline returned no execute result".into())
            })?;
        let names: Vec<String> = result
            .get("cols")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .enumerate()
                    .map(|(i, c)| {
                        c.get("name")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("col{}", i + 1))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let raw_rows = result
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let rows: Vec<JsonValue> = raw_rows
            .iter()
            .map(|row| {
                let cells = row.as_array().cloned().unwrap_or_default();
                let mut obj = serde_json::Map::with_capacity(names.len());
                for (i, name) in names.iter().enumerate() {
                    let v = cells.get(i).map(turso_cell_to_json).unwrap_or(JsonValue::Null);
                    obj.insert(name.clone(), v);
                }
                JsonValue::Object(obj)
            })
            .collect();
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "turso: materialized {} rows into {}",
            count, spec.node_id
        ))
    }

    /// Turso / libSQL sink. Turso is SQLite, so CREATE TABLE IF NOT EXISTS is
    /// available and the type set is the SQLite storage classes. Values go up
    /// as bound arguments rather than inlined literals, and many statements
    /// ride one pipeline round trip.
    pub(crate) fn run_turso_sink(
        &self,
        db: &Path,
        spec: &plan::TursoSinkSpec,
    ) -> Result<String, EngineError> {
        let select = format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view));
        let rows = self.run_rows(Some(db), &select)?;
        if rows.is_empty() {
            return Ok(format!("turso: 0 rows to insert into {}", spec.table));
        }
        let cols: Vec<String> = match rows[0].as_object() {
            Some(o) => o.keys().cloned().collect(),
            None => {
                return Err(EngineError::Query(
                    "turso: upstream rows aren't JSON objects".into(),
                ))
            }
        };
        let col_types: std::collections::HashMap<String, String> =
            describe_columns(self, db, &spec.from_view).into_iter().collect();
        let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        let table = q(&spec.table);
        let col_defs = cols
            .iter()
            .map(|c| {
                let ty =
                    duckdb_type_to_sqlite(col_types.get(c).map(|s| s.as_str()).unwrap_or("VARCHAR"));
                format!("{} {}", q(c), ty)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let cols_list = cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        let placeholders = vec!["?"; cols.len()].join(", ");
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table, cols_list, placeholders
        );
        let url = format!("{}/v2/pipeline", turso_base_url(&spec.url));

        let mut setup = vec![serde_json::json!({
            "type": "execute",
            "stmt": { "sql": format!("CREATE TABLE IF NOT EXISTS {} ({})", table, col_defs) }
        })];
        if spec.mode == "overwrite" {
            setup.push(serde_json::json!({
                "type": "execute",
                "stmt": { "sql": format!("DELETE FROM {}", table) }
            }));
        }
        setup.push(serde_json::json!({ "type": "close" }));
        turso_send(spec.auth_token.as_deref(), &url, serde_json::json!({ "requests": setup }))?;

        let batch = spec.batch_size.max(1);
        let mut total = 0usize;
        for chunk in rows.chunks(batch) {
            self.check_cancelled()?;
            let mut requests: Vec<JsonValue> = Vec::with_capacity(chunk.len() + 1);
            for row in chunk {
                let obj = row.as_object();
                let args: Vec<JsonValue> = cols
                    .iter()
                    .map(|c| {
                        json_to_turso_arg(obj.and_then(|o| o.get(c)).unwrap_or(&JsonValue::Null))
                    })
                    .collect();
                requests.push(serde_json::json!({
                    "type": "execute",
                    "stmt": { "sql": insert_sql, "args": args }
                }));
            }
            requests.push(serde_json::json!({ "type": "close" }));
            turso_send(
                spec.auth_token.as_deref(),
                &url,
                serde_json::json!({ "requests": requests }),
            )?;
            total += chunk.len();
        }
        Ok(format!(
            "turso: {} {} rows into {}",
            if spec.mode == "overwrite" { "overwrote with" } else { "inserted" },
            total,
            spec.table
        ))
    }

    /// Produce an EMPTY relation for a spool pass that read nothing new.
    ///
    /// An idle pass is the normal case for a streaming source - most polls of
    /// a quiet spool have nothing in them - so it must not fail the run. It
    /// would, though: materializing zero rows with no declared schema is an
    /// error, because there is no way to know what columns the empty result
    /// has (issue #170).
    ///
    /// The spool itself knows. It is append-only, so the records it already
    /// delivered are still there: the LAST complete line gives the column
    /// shape. Materialize that one row, then delete it, and downstream sees a
    /// 0-row relation with the right columns instead of an error.
    ///
    /// A spool with no records at all genuinely cannot say, and falls back to
    /// the declared-schema path and its error message.
    fn spool_empty_relation(
        &self,
        db: &Path,
        node_id: &str,
        path: &std::path::Path,
    ) -> Result<(), EngineError> {
        if let Some(row) = last_complete_json_line(path) {
            materialize_jsonobjects_as_table(&self.bin, db, node_id, &[row])?;
            self.run(
                Some(db),
                &format!("DELETE FROM {}", plan::quote_ident(node_id)),
                false,
            )?;
            return Ok(());
        }
        materialize_jsonobjects_as_table(&self.bin, db, node_id, &[])
    }

    /// xf.tumble: event-time tumbling windows across runs.
    ///
    /// Each run computes from `buffered rows UNION new rows`, emits the ones
    /// whose window has closed, and writes what remains to a NEW buffer file.
    /// The old buffer is left untouched until the run succeeds: the pointer to
    /// the current buffer lives in the deferred state, so a run that fails
    /// downstream leaves the previous buffer authoritative and the same rows
    /// come back next time.
    ///
    /// That replace-don't-mutate shape is what makes it safe. Appending to a
    /// shared buffer during the run would double-count on a retry, because the
    /// source position has not advanced either.
    pub(crate) fn run_tumble(
        &self,
        db: &Path,
        spec: &plan::TumbleSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        let state_path = incremental_state_path(pipeline_name, &spec.node_id).ok_or_else(|| {
            EngineError::Config(
                "xf.tumble: needs a workspace to keep its open windows in (DUCKLE_WORKSPACE)".into(),
            )
        })?;
        let dir = state_path.with_extension("tumble");
        std::fs::create_dir_all(&dir)
            .map_err(|e| EngineError::Query(format!("tumble: state dir {}: {}", dir.display(), e)))?;

        // The raw text as READ, so the flush can tell whether somebody changed
        // this while the run was in flight.
        let prior = crate::read_state_snapshot(&state_path);
        let saved: Option<JsonValue> = prior
            .as_deref()
            .and_then(|t| serde_json::from_str(t).ok());
        // The buffer the LAST SUCCESSFUL run left. Anything else in the folder
        // is from a run that did not finish, and is ignored then cleaned up.
        let prev_buf = saved
            .as_ref()
            .and_then(|v| v.get("buffer"))
            .and_then(|v| v.as_str())
            .map(|f| dir.join(f))
            .filter(|p| p.exists());
        let prev_watermark = saved
            .as_ref()
            .and_then(|v| v.get("watermark"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        // How far emission has already reached. A row for a window at or below
        // this arrived too late to be counted and is dropped rather than
        // emitted as a second, partial copy of a window already delivered.
        let emitted_through = saved
            .as_ref()
            .and_then(|v| v.get("emitted_through"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let ts = plan::quote_ident(&spec.time_column);
        let upstream = plan::quote_ident(&spec.from_view);
        let lit = |s: &str| format!("'{}'", s.replace('\'', "''"));
        let esc_path = |p: &std::path::Path| p.display().to_string().replace('\\', "/").replace('\'', "''");

        // Everything in play this run: what was still open, plus what arrived.
        let all = match &prev_buf {
            Some(p) => format!(
                "SELECT * FROM {up} UNION ALL BY NAME SELECT * FROM read_parquet('{buf}')",
                up = upstream,
                buf = esc_path(p)
            ),
            None => format!("SELECT * FROM {}", upstream),
        };

        // The watermark is computed ONCE, here, and used as a literal in every
        // statement below. Recomputing it per statement is how the equivalent
        // elsewhere ends up deleting more than it collected.
        // Real tables, not TEMP views: every self.run / self.run_rows below is a
        // separate duckdb invocation, and a temp view dies with the one that
        // created it. These live in the run's own database and are dropped at
        // the end.
        let t_all = plan::quote_ident(&format!("duckle_tumble_all_{}", spec.node_id));
        let t_b = plan::quote_ident(&format!("duckle_tumble_b_{}", spec.node_id));
        let t_late = plan::quote_ident(&format!("duckle_tumble_late_{}", spec.node_id));
        let wm_sql = format!(
            "CREATE OR REPLACE TABLE {t_all} AS {all};
             SELECT COALESCE(MAX({ts}), NULL)::VARCHAR AS wm FROM {t_all}",
            t_all = t_all,
            all = all,
            ts = ts
        );
        let wm_rows = self.run_rows(Some(db), &wm_sql)?;
        let batch_max = wm_rows
            .first()
            .and_then(|r| r.get("wm"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        // Monotonic: a batch of older data must not drag the watermark back and
        // re-open windows that already closed.
        let watermark = match (batch_max, prev_watermark.clone()) {
            (Some(b), Some(p)) => Some(if b > p { b } else { p }),
            (Some(b), None) => Some(b),
            (None, p) => p,
        };
        let watermark = match watermark {
            Some(w) => w,
            // Nothing has ever been seen, so nothing can be closed.
            None => {
                self.run(
                    Some(db),
                    &format!(
                        "CREATE OR REPLACE TABLE {out} AS SELECT *, \
                           CAST(NULL AS TIMESTAMP) AS window_start, \
                           CAST(NULL AS TIMESTAMP) AS window_end \
                         FROM {t_all} LIMIT 0",
                        out = plan::quote_ident(&spec.node_id),
                        t_all = t_all
                    ),
                    false,
                )?;
                return Ok(format!("tumble: no rows yet in {}", spec.node_id));
            }
        };

        let bucketed = format!(
            "SELECT *, \
               time_bucket(INTERVAL {size}, CAST({ts} AS TIMESTAMP)) AS window_start, \
               time_bucket(INTERVAL {size}, CAST({ts} AS TIMESTAMP)) + INTERVAL {size} AS window_end \
             FROM {t_all}",
            size = lit(&spec.size),
            ts = ts,
            t_all = t_all
        );
        let closed = format!(
            "window_end + INTERVAL {late} <= CAST({wm} AS TIMESTAMP)",
            late = lit(&spec.allowed_lateness),
            wm = lit(&watermark)
        );
        // A row whose window closed before the last emission is late beyond
        // rescue: emitting it now would deliver a second, partial copy of a
        // window a downstream consumer already has.
        let too_late = match &emitted_through {
            Some(e) => format!(
                "window_end + INTERVAL {late} <= CAST({e} AS TIMESTAMP)",
                late = lit(&spec.allowed_lateness),
                e = lit(e)
            ),
            None => "FALSE".to_string(),
        };

        let next_buf_name = format!(
            "buf-{}-{}.parquet",
            std::process::id(),
            TUMBLE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let next_buf = dir.join(&next_buf_name);
        let out = plan::quote_ident(&spec.node_id);

        // One script: stage everything, emit the closed windows, and write what
        // stays open to a NEW file. Nothing the previous run left is touched.
        let script = format!(
            "CREATE OR REPLACE TABLE {t_b} AS {bucketed};
             CREATE OR REPLACE TABLE {t_late} AS \
               SELECT * FROM {t_b} WHERE {too_late};
             CREATE OR REPLACE TABLE {out} AS \
               SELECT * FROM {t_b} WHERE {closed} AND NOT ({too_late});
             COPY (SELECT * EXCLUDE (window_start, window_end) FROM {t_b} \
                   WHERE NOT ({closed})) TO '{next}' (FORMAT PARQUET);",
            t_b = t_b,
            t_late = t_late,
            bucketed = bucketed,
            too_late = too_late,
            closed = closed,
            out = out,
            next = esc_path(&next_buf)
        );
        self.run(Some(db), &script, false)?;

        let emitted = self
            .run_rows(Some(db), &format!("SELECT count(*) AS n FROM {}", out))?
            .first()
            .and_then(|r| r.get("n"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let dropped = self
            .run_rows(Some(db), &format!("SELECT count(*) AS n FROM {}", t_late))?
            .first()
            .and_then(|r| r.get("n"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let still_open = self
            .run_rows(
                Some(db),
                &format!("SELECT count(*) AS n FROM read_parquet('{}')", esc_path(&next_buf)),
            )?
            .first()
            .and_then(|r| r.get("n"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // The scratch tables have served their purpose; leaving them behind would
        // put them in front of the user as if they were pipeline output.
        let _ = self.run(
            Some(db),
            &format!(
                "DROP TABLE IF EXISTS {t_all}; DROP TABLE IF EXISTS {t_b}; DROP TABLE IF EXISTS {t_late};"
            ),
            false,
        );

        // Emission only advances the mark when something was emitted; a quiet
        // run must not move it and turn merely-early rows into "too late".
        let next_emitted_through = if emitted > 0 {
            Some(watermark.clone())
        } else {
            emitted_through.clone()
        };
        pending.push(crate::PendingWrite::state(
            state_path,
            serde_json::json!({
                "buffer": next_buf_name,
                "watermark": watermark,
                "emitted_through": next_emitted_through,
            }),
            prior,
        ));
        // Old buffers from runs that did not finish are dead weight; the one
        // the last success pointed at stays until this run is itself committed.
        prune_tumble_buffers(&dir, &next_buf_name, prev_buf.as_deref());

        Ok(format!(
            "tumble: {} row(s) in closed windows into {}, {} still open{} (watermark {})",
            emitted,
            spec.node_id,
            still_open,
            if dropped > 0 {
                format!(", {} dropped as too late", dropped)
            } else {
                String::new()
            },
            watermark
        ))
    }

    /// src.changed: probe remote metadata and emit only what changed.
    ///
    /// Cheap check first is the whole point: a HEAD or a stat costs nothing
    /// next to the object it decides about. When nothing changed the node
    /// reports `unchanged` rather than a bare success, so a working poll and a
    /// broken one are told apart.
    pub(crate) fn run_changed_source(
        &self,
        db: &Path,
        spec: &plan::ChangedSourceSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<crate::PendingWrite>,
        artifacts: &mut Vec<crate::ArtifactRef>,
    ) -> Result<String, EngineError> {
        let state_path = if spec.track_state {
            incremental_state_path(pipeline_name, &spec.node_id)
        } else {
            None
        };
        let prior = state_path.as_deref().and_then(crate::read_state_snapshot);
        // What has already been processed: uri -> fingerprint.
        let mut seen: std::collections::BTreeMap<String, String> = prior
            .as_deref()
            .and_then(|t| serde_json::from_str::<JsonValue>(t).ok())
            .and_then(|v| v.get("seen").cloned())
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();

        let entries = if spec.listing {
            self.list_remote_entries(spec)?
        } else {
            vec![self.probe_remote_entry(spec)?]
        };

        let mut rows: Vec<JsonValue> = Vec::new();
        let mut unchanged_count = 0usize;
        for e in &entries {
            let status = match seen.get(&e.uri) {
                Some(prev) if *prev == e.fingerprint => {
                    unchanged_count += 1;
                    continue;
                }
                Some(_) => "changed",
                None => "new",
            };
            rows.push(serde_json::json!({
                "uri": e.uri,
                "name": e.name,
                "size": e.size,
                "modified_at": e.modified_at,
                "etag": e.etag,
                "fingerprint": e.fingerprint,
                "status": status,
            }));
            if rows.len() >= spec.max_entries {
                break;
            }
        }

        // Only what was EMITTED is recorded, and only if the run succeeds.
        // Recording an entry this run did not emit - because max_entries cut it
        // off - would skip it forever.
        for r in &rows {
            if let (Some(u), Some(f)) = (
                r.get("uri").and_then(|v| v.as_str()),
                r.get("fingerprint").and_then(|v| v.as_str()),
            ) {
                seen.insert(u.to_string(), f.to_string());
            }
        }

        // What the run OBSERVED, for the provenance manifest. No sha256: the
        // bytes were deliberately not read, which is the point of the component.
        // The ETag with the size and mtime is what can honestly be claimed.
        for e in &entries {
            artifacts.push(crate::ArtifactRef {
                node: spec.node_id.clone(),
                role: "input".into(),
                uri: e.uri.clone(),
                name: Some(e.name.clone()),
                media_type: None,
                size_bytes: e.size,
                sha256: None,
                etag: e.etag.clone(),
                modified_at: e.modified_at.clone(),
            });
        }

        let emitted = rows.len();
        if emitted == 0 {
            // A typed empty relation, so a downstream stage sees the right
            // columns rather than an error on a quiet poll.
            self.changed_empty_relation(db, &spec.node_id)?;
        } else {
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        }

        if let Some(p) = state_path {
            pending.push(crate::PendingWrite::state(
                p,
                serde_json::json!({ "seen": seen }),
                prior,
            ));
        }

        let msg = format!(
            "changed: {} of {} entr{} changed at {}{}",
            emitted,
            entries.len(),
            if entries.len() == 1 { "y" } else { "ies" },
            spec.uri,
            if unchanged_count > 0 {
                format!(" ({} unchanged)", unchanged_count)
            } else {
                String::new()
            }
        );
        Ok(if emitted == 0 {
            format!("{}{}", crate::UNCHANGED_MARKER, msg)
        } else {
            msg
        })
    }

    /// The shape src.changed always emits, with no rows in it.
    fn changed_empty_relation(&self, db: &Path, node_id: &str) -> Result<(), EngineError> {
        self.run(
            Some(db),
            &format!(
                "CREATE OR REPLACE TABLE {} (uri VARCHAR, name VARCHAR, size BIGINT, \
                 modified_at VARCHAR, etag VARCHAR, fingerprint VARCHAR, status VARCHAR)",
                plan::quote_ident(node_id)
            ),
            false,
        )
        .map(|_| ())
    }

    /// One object's metadata, without fetching it.
    fn probe_remote_entry(
        &self,
        spec: &plan::ChangedSourceSpec,
    ) -> Result<RemoteEntry, EngineError> {
        if spec.uri.starts_with("sftp://") {
            let (host, port, user, path) = parse_sftp_uri(&spec.uri)?;
            let user = spec.user.clone().or(user).unwrap_or_default();
            let stat = self.sftp_stat(spec, &host, port, &user, &path)?;
            return Ok(stat);
        }
        if spec.uri.starts_with("s3://") || spec.uri.starts_with("s3a://") {
            return self.s3_stat(spec);
        }
        if !(spec.uri.starts_with("http://") || spec.uri.starts_with("https://")) {
            return Err(EngineError::Config(format!(
                "changed: {} is not a URI this can probe - use https://, s3:// or sftp://",
                spec.uri
            )));
        }
        let mut req = crate::tls::http_agent().head(&spec.uri);
        for (k, v) in &spec.headers {
            req = req.set(k, v);
        }
        // A server that refuses HEAD is common enough to be worth naming, and
        // falling back to GET would download the object this exists to avoid.
        let resp = match req.call() {
            Ok(r) => r,
            Err(ureq::Error::Status(405, _)) => {
                return Err(EngineError::Query(format!(
                    "changed: {} rejected a HEAD request (405). This source cannot be \
                     checked without downloading it, which is what this component exists \
                     to avoid.",
                    spec.uri
                )))
            }
            Err(ureq::Error::Status(code, r)) => {
                let body = r.into_string().unwrap_or_default();
                return Err(EngineError::Query(format!(
                    "changed: HTTP {} probing {}: {}",
                    code,
                    spec.uri,
                    body.chars().take(200).collect::<String>()
                )));
            }
            Err(e) => {
                return Err(EngineError::Query(format!(
                    "changed: probing {}: {}",
                    spec.uri, e
                )))
            }
        };
        let etag = resp.header("etag").map(|s| s.trim_matches('"').to_string());
        let modified = resp.header("last-modified").map(|s| s.to_string());
        let size = resp
            .header("content-length")
            .and_then(|s| s.parse::<i64>().ok());
        let name = spec
            .uri
            .rsplit('/')
            .next()
            .unwrap_or(&spec.uri)
            .to_string();
        Ok(RemoteEntry {
            fingerprint: remote_fingerprint(etag.as_deref(), modified.as_deref(), size),
            uri: spec.uri.clone(),
            name,
            size,
            modified_at: modified,
            etag,
        })
    }

    /// The credentials for an `s3://` uri, or a message saying what is missing.
    ///
    /// An anonymous request to a private bucket comes back 403, which reads as
    /// "wrong keys" rather than "no keys", so the absence is named here instead
    /// of being discovered from a status code.
    fn s3_config<'a>(
        &self,
        spec: &'a plan::ChangedSourceSpec,
    ) -> Result<&'a crate::s3::S3Config, EngineError> {
        spec.s3.as_ref().ok_or_else(|| {
            EngineError::Config(format!(
                "changed: {} needs S3 credentials - pick a saved S3 connection on the node, \
                 or set its access key and secret key",
                spec.uri
            ))
        })
    }

    /// One object's size, ETag and mtime, over a HEAD. No bytes transferred,
    /// which is the whole reason this component exists.
    fn s3_stat(&self, spec: &plan::ChangedSourceSpec) -> Result<RemoteEntry, EngineError> {
        let cfg = self.s3_config(spec)?;
        let (bucket, key) = crate::s3::parse_s3_uri(&spec.uri)?;
        if key.is_empty() {
            return Err(EngineError::Config(format!(
                "changed: {} names a bucket but no object. Turn listing on to enumerate it.",
                spec.uri
            )));
        }
        let o = cfg.head(&bucket, &key)?;
        Ok(RemoteEntry {
            fingerprint: remote_fingerprint(o.etag.as_deref(), o.last_modified.as_deref(), o.size),
            uri: spec.uri.clone(),
            name: key.rsplit('/').next().unwrap_or(&key).to_string(),
            size: o.size,
            modified_at: o.last_modified,
            etag: o.etag,
        })
    }

    /// Every object under a prefix.
    fn s3_list(&self, spec: &plan::ChangedSourceSpec) -> Result<Vec<RemoteEntry>, EngineError> {
        let cfg = self.s3_config(spec)?;
        let (bucket, prefix) = crate::s3::parse_s3_uri(&spec.uri)?;
        // The cap goes DOWN into the listing rather than being applied after it:
        // a prefix holding a million objects must not be walked in full to hand
        // back a hundred. A suffix filter can discard some of what comes back,
        // so the request asks for enough to still fill the cap afterwards.
        let want = if spec.suffix.is_some() {
            spec.max_entries.saturating_mul(4).max(spec.max_entries)
        } else {
            spec.max_entries
        };
        let objects = cfg.list(&bucket, &prefix, want)?;
        let mut out: Vec<RemoteEntry> = objects
            .into_iter()
            .filter(|o| match &spec.suffix {
                Some(sfx) => o.key.ends_with(sfx.as_str()),
                None => true,
            })
            .map(|o| {
                let name = o.key.rsplit('/').next().unwrap_or(&o.key).to_string();
                RemoteEntry {
                    fingerprint: remote_fingerprint(
                        o.etag.as_deref(),
                        o.last_modified.as_deref(),
                        o.size,
                    ),
                    uri: format!("s3://{}/{}", bucket, o.key),
                    name,
                    size: o.size,
                    modified_at: o.last_modified,
                    etag: o.etag,
                }
            })
            .collect();
        // Oldest first, so a capped run works through a backlog in order rather
        // than taking an arbitrary slice of it - the same rule the SFTP listing
        // follows, and for the same reason. S3 returns keys in lexical order
        // already; sorting by the leaf name matches what the SFTP side does when
        // a prefix has sub-folders in it.
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// xf.artifact.copy: land the bytes named by the upstream rows somewhere
    /// durable, and emit a row per landed copy.
    ///
    /// An artifact is a reference, so a pipeline can carry one around for free.
    /// At some point somebody has to move the actual bytes, and that is this:
    /// the step between "the feed says there is a new 4GB PDF bundle" and "it is
    /// in our raw zone, hashed, and we can prove which bytes we parsed".
    ///
    /// Streamed throughout. The source is read in one pass, hashed on the way
    /// past, and written straight out, so memory is bounded by the part size and
    /// not by the object. Reading it twice - once to hash, once to upload -
    /// would double the transfer off a remote source, and hashing first would
    /// mean holding the whole thing.
    pub(crate) fn run_artifact_copy(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &plan::ArtifactCopySpec,
        artifacts: &mut Vec<crate::ArtifactRef>,
    ) -> Result<String, EngineError> {
        let select = format!(
            "{}SELECT * FROM {}",
            secret_prefix,
            plan::quote_ident(&spec.from_view)
        );
        let rows = self.run_rows(Some(db), &select)?;

        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        let mut copied = 0usize;
        let mut skipped = 0usize;
        let mut bytes_total: u64 = 0;
        for row in &rows {
            let src = row
                .get(&spec.uri_column)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    EngineError::Query(format!(
                        "artifact.copy: row has no '{}' to copy from. Set the URI column to \
                         whichever column names the artifact.",
                        spec.uri_column
                    ))
                })?;
            let landed = self.copy_one_artifact(spec, &src)?;
            if landed.copied {
                copied += 1;
                bytes_total += landed.size_bytes.unwrap_or(0) as u64;
            } else {
                skipped += 1;
            }
            // Both sides of the copy: what was read, and what was written. The
            // output carries a real sha256 because these bytes DID pass through
            // this run, which is the strongest provenance an artifact ever gets.
            artifacts.push(crate::ArtifactRef {
                node: spec.node_id.clone(),
                role: "input".into(),
                uri: src.clone(),
                name: Some(landed.name.clone()),
                media_type: Some(landed.media_type.to_string()),
                size_bytes: landed.size_bytes,
                sha256: landed.sha256.clone(),
                etag: None,
                modified_at: None,
            });
            artifacts.push(crate::ArtifactRef {
                node: spec.node_id.clone(),
                role: "output".into(),
                uri: landed.uri.clone(),
                name: Some(landed.name.clone()),
                media_type: Some(landed.media_type.to_string()),
                size_bytes: landed.size_bytes,
                sha256: landed.sha256.clone(),
                etag: None,
                modified_at: None,
            });
            out.push(serde_json::json!({
                "uri": landed.uri,
                "source_uri": src,
                "name": landed.name,
                "media_type": landed.media_type,
                "size_bytes": landed.size_bytes,
                "sha256": landed.sha256,
                "copied": landed.copied,
            }));
        }

        if out.is_empty() {
            // A typed empty relation, so a downstream stage sees the right
            // columns rather than an error on a run with nothing to copy.
            self.run(
                Some(db),
                &format!(
                    "CREATE OR REPLACE TABLE {} (uri VARCHAR, source_uri VARCHAR, name VARCHAR, \
                     media_type VARCHAR, size_bytes BIGINT, sha256 VARCHAR, copied BOOLEAN)",
                    plan::quote_ident(&spec.node_id)
                ),
                false,
            )?;
        } else {
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        }

        let msg = format!(
            "artifact.copy: {} copied ({}), {} already there, to {}",
            copied,
            human_bytes(bytes_total),
            skipped,
            spec.destination
        );
        // Nothing moved is a real outcome worth telling apart from a broken
        // copy, the same way an unchanged poll is.
        Ok(if copied == 0 && !rows.is_empty() {
            format!("{}{}", crate::UNCHANGED_MARKER, msg)
        } else {
            msg
        })
    }

    /// Copy one artifact, and describe what landed.
    fn copy_one_artifact(
        &self,
        spec: &plan::ArtifactCopySpec,
        src: &str,
    ) -> Result<LandedArtifact, EngineError> {
        let name = src
            .rsplit(['/', '\\'])
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("artifact")
            .to_string();
        let media_type = media_type_for(&name);

        // "hash" naming needs the content hash BEFORE choosing the key, which
        // means reading the object twice. That is the honest cost of a
        // content-addressed store and it is opt-in for exactly that reason;
        // "keep" and "path" are one pass.
        let key_hint = match spec.naming.as_str() {
            "path" => source_path_of(src),
            _ => name.clone(),
        };

        if spec.naming == "hash" {
            let (sha, size) = self.hash_source(spec, src)?;
            let ext = name.rsplit_once('.').map(|(_, e)| format!(".{e}")).unwrap_or_default();
            let dest = join_destination(&spec.destination, &format!("{sha}{ext}"));
            // A content-addressed key that already exists holds the same bytes
            // by construction, so the second copy is never worth making.
            if self.artifact_exists(spec, &dest)? {
                return Ok(LandedArtifact {
                    uri: dest,
                    name,
                    media_type,
                    size_bytes: Some(size as i64),
                    sha256: Some(sha),
                    copied: false,
                });
            }
            let (sha2, size2) = self.stream_artifact(spec, src, &dest)?;
            return Ok(LandedArtifact {
                uri: dest,
                name,
                media_type,
                size_bytes: Some(size2 as i64),
                sha256: Some(sha2),
                copied: true,
            });
        }

        let dest = join_destination(&spec.destination, &key_hint);
        match spec.if_exists.as_str() {
            "error" if self.artifact_exists(spec, &dest)? => {
                return Err(EngineError::Query(format!(
                    "artifact.copy: {} already exists and ifExists is 'error'",
                    dest
                )))
            }
            "skip" if self.artifact_exists(spec, &dest)? => {
                return Ok(LandedArtifact {
                    uri: dest,
                    name,
                    media_type,
                    size_bytes: None,
                    sha256: None,
                    copied: false,
                })
            }
            _ => {}
        }
        let (sha, size) = self.stream_artifact(spec, src, &dest)?;
        Ok(LandedArtifact {
            uri: dest,
            name,
            media_type,
            size_bytes: Some(size as i64),
            sha256: Some(sha),
            copied: true,
        })
    }

    /// Open a source for reading, whatever scheme it is written in.
    pub(crate) fn open_artifact(
        &self,
        auth: &plan::ArtifactAuth,
        src: &str,
    ) -> Result<Box<dyn std::io::Read + Send>, EngineError> {
        if src.starts_with("s3://") || src.starts_with("s3a://") {
            let cfg = auth.s3.as_ref().ok_or_else(|| {
                EngineError::Config(format!(
                    "artifact.copy: {} needs S3 credentials - pick a saved S3 connection on \
                     the node, or set its access key and secret key",
                    src
                ))
            })?;
            let (bucket, key) = crate::s3::parse_s3_uri(src)?;
            return cfg.get(&bucket, &key);
        }
        if src.starts_with("http://") || src.starts_with("https://") {
            let mut req = crate::tls::http_agent().get(src);
            for (k, v) in &auth.headers {
                req = req.set(k, v);
            }
            let resp = req
                .call()
                .map_err(|e| EngineError::Query(format!("artifact.copy: fetching {src}: {e}")))?;
            return Ok(Box::new(resp.into_reader()));
        }
        if src.starts_with("sftp://") {
            // SFTP is read into a temp file first, because the session has to be
            // driven on its own runtime and cannot be held open behind a plain
            // Read. The file is streamed out of afterwards, so the destination
            // upload is still bounded - only the local spool is not.
            return self.sftp_spool(auth, src);
        }
        let f = std::fs::File::open(src)
            .map_err(|e| EngineError::Query(format!("artifact.copy: opening {src}: {e}")))?;
        Ok(Box::new(f))
    }

    /// Read a source once, hashing it, and throw the bytes away. Only used by
    /// content-addressed naming, which has to know the hash before it knows
    /// where the object goes.
    fn hash_source(
        &self,
        spec: &plan::ArtifactCopySpec,
        src: &str,
    ) -> Result<(String, u64), EngineError> {
        let reader = self.open_artifact(&spec.auth, src)?;
        let mut hashing = crate::s3::HashingReader::new(reader);
        std::io::copy(&mut hashing, &mut std::io::sink())
            .map_err(|e| EngineError::Query(format!("artifact.copy: reading {src}: {e}")))?;
        Ok(hashing.finish())
    }

    /// Copy the bytes to the destination, hashing them on the way past.
    fn stream_artifact(
        &self,
        spec: &plan::ArtifactCopySpec,
        src: &str,
        dest: &str,
    ) -> Result<(String, u64), EngineError> {
        let reader = self.open_artifact(&spec.auth, src)?;
        self.land_bytes(&spec.auth, reader, dest, spec.part_size_bytes)
    }

    /// Write a reader's bytes to a destination, hashing them on the way past.
    ///
    /// Shared by the copy and by archive extraction, because "land these bytes
    /// somewhere durable and tell me their hash and size" is one operation and
    /// two implementations of it would disagree about atomicity the first time
    /// one was changed.
    pub(crate) fn land_bytes(
        &self,
        auth: &plan::ArtifactAuth,
        reader: impl std::io::Read,
        dest: &str,
        part_size: usize,
    ) -> Result<(String, u64), EngineError> {
        let mut hashing = crate::s3::HashingReader::new(reader);

        if dest.starts_with("s3://") || dest.starts_with("s3a://") {
            let cfg = auth.s3.as_ref().ok_or_else(|| {
                EngineError::Config(format!(
                    "writing to {} needs S3 credentials on the node",
                    dest
                ))
            })?;
            let (bucket, key) = crate::s3::parse_s3_uri(dest)?;
            // Multipart regardless of size: the source's length is not known
            // for an HTTP body without a Content-Length, and a plain PUT has to
            // declare one. Multipart streams in bounded parts either way.
            cfg.put_multipart(&bucket, &key, &mut hashing, part_size, Some(media_type_for(dest)))?;
            return Ok(hashing.finish());
        }

        // Local: write beside the target and rename, so a crash mid-copy never
        // leaves a half file that looks like a complete one. Everything else in
        // the engine that writes a file does the same.
        let path = std::path::Path::new(dest);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| {
                EngineError::Query(format!("creating {}: {e}", dir.display()))
            })?;
        }
        let tmp = path.with_extension(format!(
            "{}.duckle-partial",
            path.extension().and_then(|e| e.to_str()).unwrap_or("tmp")
        ));
        {
            let mut f = std::fs::File::create(&tmp).map_err(|e| {
                EngineError::Query(format!("creating {}: {e}", tmp.display()))
            })?;
            std::io::copy(&mut hashing, &mut f)
                .map_err(|e| EngineError::Query(format!("writing {dest}: {e}")))?;
        }
        // Windows will not rename over an existing file, so the old one goes
        // first. Anything else silently leaves the previous copy in place.
        let _ = std::fs::remove_file(path);
        std::fs::rename(&tmp, path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            EngineError::Query(format!("placing {dest}: {e}"))
        })?;
        Ok(hashing.finish())
    }

    /// Is something already at this destination?
    fn artifact_exists(
        &self,
        spec: &plan::ArtifactCopySpec,
        dest: &str,
    ) -> Result<bool, EngineError> {
        if dest.starts_with("s3://") || dest.starts_with("s3a://") {
            let Some(cfg) = spec.auth.s3.as_ref() else {
                return Ok(false);
            };
            let (bucket, key) = crate::s3::parse_s3_uri(dest)?;
            return match cfg.head(&bucket, &key) {
                Ok(_) => Ok(true),
                // A 404 is the answer to the question, not a failure. Anything
                // else is reported: treating a 403 as "not there" would re-copy
                // on every run and never say why.
                Err(e) if e.to_string().contains("HTTP 404") => Ok(false),
                Err(e) => Err(e),
            };
        }
        Ok(std::path::Path::new(dest).exists())
    }

    /// Read an SFTP object into a temp file, and hand back a reader over it.
    ///
    /// The session has to be driven on its own async runtime and cannot be held
    /// open behind a plain `Read`, so this is the one scheme that touches disk
    /// on the way past. The upload out of the spool is still streamed, so the
    /// memory bound holds; it is the local disk that pays.
    fn sftp_spool(
        &self,
        auth: &plan::ArtifactAuth,
        src: &str,
    ) -> Result<Box<dyn std::io::Read + Send>, EngineError> {
        let (host, port, user, path) = parse_sftp_uri(src)?;
        let user = auth.user.clone().or(user).unwrap_or_default();
        // src.changed's SFTP helpers take a ChangedSourceSpec, so the copy node's
        // equivalent auth is presented in that shape rather than duplicating the
        // connect-and-verify path. One implementation, one host-key policy.
        let as_changed = plan::ChangedSourceSpec {
            node_id: String::new(),
            uri: src.to_string(),
            listing: false,
            suffix: None,
            max_entries: 1,
            track_state: false,
            user: Some(user.clone()),
            password: auth.password.clone(),
            private_key: auth.private_key.clone(),
            key_passphrase: auth.key_passphrase.clone(),
            host_fingerprint: auth.host_fingerprint.clone(),
            headers: Vec::new(),
            s3: None,
        };
        let p = path.clone();
        let bytes = self.with_sftp(&as_changed, &host, port, &user, move |sftp| {
            Box::pin(async move {
                use tokio::io::AsyncReadExt;
                let mut f = sftp
                    .open(p.clone())
                    .await
                    .map_err(|e| format!("open {}: {}", p, e))?;
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)
                    .await
                    .map_err(|e| format!("read {}: {}", p, e))?;
                Ok(buf)
            })
        })?;
        let path = std::env::temp_dir().join(format!(
            "duckle_artifact_{}_{}.spool",
            std::process::id(),
            crate::now_nanos()
        ));
        std::fs::write(&path, &bytes)
            .map_err(|e| EngineError::Query(format!("artifact.copy: spool write: {e}")))?;
        let file = std::fs::File::open(&path)
            .map_err(|e| EngineError::Query(format!("artifact.copy: spool reopen: {e}")))?;
        // The guard removes the file when the reader is dropped, whether the
        // copy succeeded or not.
        Ok(Box::new(SpooledArtifact { path, file }))
    }

    /// src.ducklake.maintain: run one DuckLake maintenance operation and emit
    /// what it did.
    ///
    /// Deliberately thin. Each operation is one DuckLake function, its options
    /// are that function's options, and its output relation is that function's
    /// own result rows - so a compaction can be alerted on, quality-gated or
    /// joined exactly like anything else, and nothing here has to be kept in
    /// step with DuckLake's storage semantics as they change.
    pub(crate) fn run_ducklake_maintain(
        &self,
        db: &Path,
        spec: &plan::DuckLakeMaintainSpec,
    ) -> Result<String, EngineError> {
        // Two maintenance runs against one catalog must not race. DuckLake
        // itself would refuse the second commit, but a conflict error at the
        // end of a two-hour compaction is a worse answer than waiting, and a
        // scheduled weekly compact overlapping a monthly cleanup is exactly the
        // shape #279 asks to be serialised rather than raced.
        let _lock = std::env::var("DUCKLE_WORKSPACE")
            .ok()
            .filter(|w| !w.is_empty())
            .map(|w| {
                crate::runlock::lock_store(
                    std::path::Path::new(&w),
                    &format!("ducklake-maintain-{}", lock_key(&spec.catalog_path)),
                )
            })
            .transpose()
            .map_err(EngineError::Config)?;

        let before = self.ducklake_totals(db, spec).ok();
        let call = maintenance_call(spec)?;
        let sql = format!(
            "{}CREATE OR REPLACE TABLE {} AS SELECT * FROM {};",
            spec.attach,
            plan::quote_ident(&spec.node_id),
            call
        );
        self.run(Some(db), &sql, false)?;

        let rows = self
            .run_rows(
                Some(db),
                &format!("SELECT COUNT(*) AS n FROM {}", plan::quote_ident(&spec.node_id)),
            )
            .ok()
            .and_then(|r| r.first().and_then(|v| v.get("n")).and_then(|v| v.as_i64()))
            .unwrap_or(0);
        let after = self.ducklake_totals(db, spec).ok();

        Ok(format!(
            "ducklake {}{}: {} row(s){}",
            spec.operation,
            if spec.dry_run { " (dry run, nothing was deleted)" } else { "" },
            rows,
            match (before, after) {
                // What the operation actually changed, which is the part an
                // operator is reading the log for.
                (Some(b), Some(a)) if b != a => format!(
                    " - files {} -> {}, {} -> {}",
                    b.0,
                    a.0,
                    human_bytes(b.1),
                    human_bytes(a.1)
                ),
                _ => String::new(),
            }
        ))
    }

    /// Total files and bytes across the catalog, for the before/after line.
    /// Best-effort: a failure here must not fail the maintenance itself.
    fn ducklake_totals(
        &self,
        db: &Path,
        spec: &plan::DuckLakeMaintainSpec,
    ) -> Result<(u64, u64), EngineError> {
        let sql = format!(
            "{}SELECT COALESCE(SUM(file_count), 0) AS f, COALESCE(SUM(file_size_bytes), 0) AS b \
             FROM ducklake_table_info({});",
            spec.attach,
            sql_string(catalog_alias(&spec.attach).as_deref().unwrap_or("duckle_dst"))
        );
        let rows = self.run_rows(Some(db), &sql)?;
        let first = rows.first().ok_or_else(|| {
            EngineError::Query("ducklake: catalog reported no table info".into())
        })?;
        // A SUM comes back from DuckDB's JSON output as a STRING, because it
        // is a HUGEINT. Reading it only as a number yields 0 for both sides,
        // they compare equal, and the before/after line silently disappears.
        let num = |k: &str| -> u64 {
            first
                .get(k)
                .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0)
        };
        Ok((num("f"), num("b")))
    }

    /// The artifacts a parser should read this run, from its upstream relation
    /// or from its configured path.
    ///
    /// #282: one resolver for every parser, so `src.pdf`, `src.xml` and
    /// `src.html` agree about what a URI column is and what is carried out of
    /// it. Giving each of them its own would produce conventions that agree
    /// until one is changed.
    pub(crate) fn resolve_artifact_inputs(
        &self,
        db: &Path,
        secret_prefix: &str,
        input: &plan::ArtifactInput,
    ) -> Result<Vec<ResolvedArtifact>, EngineError> {
        let Some(view) = input.from_view.as_deref() else {
            return Ok(Vec::new());
        };
        let rows = self.run_rows(
            Some(db),
            &format!("{}SELECT * FROM {}", secret_prefix, plan::quote_ident(view)),
        )?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let uri = row
                .get(&input.uri_column)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| {
                    EngineError::Query(format!(
                        "no '{}' to read on an upstream row. Set the URI column to whichever \
                         column names the artifact.",
                        input.uri_column
                    ))
                })?;
            // Carried, never recomputed: whatever landed these bytes already
            // hashed exactly them, and hashing again would cost a second full
            // read AND describe whatever is at that URI now rather than what
            // was parsed.
            let sha256 = row
                .get(&input.sha_column)
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .map(str::to_string);
            out.push(ResolvedArtifact { uri, sha256, row });
        }
        Ok(out)
    }

    /// A local path for an artifact, fetching it first if it is remote.
    ///
    /// A format that can be parsed from a stream should be; this is for the
    /// ones that cannot. A PDF reader seeks - the cross-reference table is at
    /// the END of the file - so a PDF has to be a file. The spool is one
    /// artifact at a time and is deleted when the guard drops, whether the
    /// parse succeeded or not, so the bound is one artifact times concurrency
    /// rather than the size of the corpus.
    pub(crate) fn local_copy_of_artifact(
        &self,
        auth: &plan::ArtifactAuth,
        uri: &str,
    ) -> Result<SpooledInput, EngineError> {
        let remote = uri.starts_with("s3://")
            || uri.starts_with("s3a://")
            || uri.starts_with("http://")
            || uri.starts_with("https://")
            || uri.starts_with("sftp://");
        if !remote {
            // Already a file. Nothing is copied and nothing is deleted.
            return Ok(SpooledInput { path: PathBuf::from(uri), temp: false });
        }
        let mut reader = self.open_artifact(auth, uri)?;
        let name = uri
            .rsplit(['/', '\\'])
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("artifact");
        let path = std::env::temp_dir().join(format!(
            "duckle_input_{}_{}_{}",
            std::process::id(),
            crate::now_nanos(),
            safe_file_name(name)
        ));
        let mut f = std::fs::File::create(&path)
            .map_err(|e| EngineError::Query(format!("spooling {uri}: {e}")))?;
        std::io::copy(&mut reader, &mut f)
            .map_err(|e| EngineError::Query(format!("fetching {uri}: {e}")))?;
        Ok(SpooledInput { path, temp: true })
    }

    /// xf.archive.extract: one archive artifact in, one artifact per member out.
    ///
    /// Bulk data is published as archives far more often than as readable
    /// files, and unpacking one used to mean a shell stage. Doing it as an
    /// ARTIFACT operation rather than inside each parser means a ZIP of CSVs, a
    /// TAR of JSON and a GZIP of NDJSON all land the same way, with the same
    /// provenance, and each member then flows into whichever parser suits it.
    pub(crate) fn run_archive_extract(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &plan::ArchiveExtractSpec,
        artifacts: &mut Vec<crate::ArtifactRef>,
    ) -> Result<String, EngineError> {
        let archives = self.resolve_artifact_inputs(db, secret_prefix, &spec.input)?;
        let mut out: Vec<JsonValue> = Vec::new();
        let mut skipped_archives = 0usize;

        for archive in &archives {
            self.check_cancelled()?;
            match self.extract_one_archive(spec, archive, &mut out, artifacts) {
                Ok(()) => {}
                Err(e) if spec.on_error == "skip" => {
                    eprintln!("duckle: archive.extract: skipping {}: {e}", archive.uri);
                    skipped_archives += 1;
                }
                Err(e) => return Err(e),
            }
        }

        if out.is_empty() {
            // A typed empty relation, so a run where nothing new arrived still
            // gives a downstream stage the right columns to bind against.
            self.run(
                Some(db),
                &format!(
                    "CREATE OR REPLACE TABLE {} (archive_uri VARCHAR, member_name VARCHAR, member_index BIGINT, uri VARCHAR, media_type VARCHAR, compressed_size BIGINT, size_bytes BIGINT, sha256 VARCHAR)",
                    plan::quote_ident(&spec.node_id)
                ),
                false,
            )?;
        } else {
            materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        }

        let msg = format!(
            "archive.extract: {} member(s) from {} archive(s) to {}{}",
            out.len(),
            archives.len() - skipped_archives,
            spec.destination,
            if skipped_archives > 0 {
                format!(" ({} archive(s) skipped)", skipped_archives)
            } else {
                String::new()
            }
        );
        Ok(if out.is_empty() && !archives.is_empty() {
            format!("{}{}", crate::UNCHANGED_MARKER, msg)
        } else {
            msg
        })
    }

    /// Unpack one archive, landing each member that passes the filters.
    fn extract_one_archive(
        &self,
        spec: &plan::ArchiveExtractSpec,
        archive: &ResolvedArtifact,
        out: &mut Vec<JsonValue>,
        artifacts: &mut Vec<crate::ArtifactRef>,
    ) -> Result<(), EngineError> {
        let kind = archive_kind(&archive.uri);
        // A ZIP's central directory is at the END of the file, so a ZIP has to
        // be seekable and a remote one is spooled. TAR and GZIP are read
        // front to back and stream straight from the source, which is why they
        // are not spooled: an archive nobody has to hold is an archive whose
        // size does not matter.
        let spooled = match kind {
            ArchiveKind::Zip => Some(self.local_copy_of_artifact(&spec.input.auth, &archive.uri)?),
            _ => None,
        };

        let mut budget = MemberBudget {
            remaining_members: spec.max_members,
            remaining_bytes: spec.max_uncompressed_bytes,
            archive_uri: archive.uri.clone(),
        };

        match kind {
            ArchiveKind::Zip => {
                let path = &spooled.as_ref().expect("spooled above").path;
                let file = std::fs::File::open(path)
                    .map_err(|e| EngineError::Query(format!("archive: open {}: {e}", archive.uri)))?;
                let mut zip = zip::ZipArchive::new(file).map_err(|e| {
                    EngineError::Query(format!("archive: {} is not a readable zip: {e}", archive.uri))
                })?;
                for i in 0..zip.len() {
                    let (name, compressed) = {
                        let entry = zip.by_index(i).map_err(|e| {
                            EngineError::Query(format!("archive: {} member {i}: {e}", archive.uri))
                        })?;
                        if entry.is_dir() {
                            continue;
                        }
                        (entry.name().to_string(), entry.compressed_size())
                    };
                    if !member_wanted(&name, spec) {
                        continue;
                    }
                    budget.take_member(&name)?;
                    let entry = zip.by_index(i).map_err(|e| {
                        EngineError::Query(format!("archive: {} member {i}: {e}", archive.uri))
                    })?;
                    self.land_member(
                        spec,
                        archive,
                        &name,
                        i,
                        Some(compressed as i64),
                        entry,
                        &mut budget,
                        out,
                        artifacts,
                    )?;
                }
            }
            ArchiveKind::Tar | ArchiveKind::TarGz => {
                let raw = self.open_artifact(&spec.input.auth, &archive.uri)?;
                let stream: Box<dyn std::io::Read> = if matches!(kind, ArchiveKind::TarGz) {
                    Box::new(flate2::read::GzDecoder::new(raw))
                } else {
                    Box::new(raw)
                };
                let mut tar = tar::Archive::new(stream);
                let entries = tar.entries().map_err(|e| {
                    EngineError::Query(format!("archive: {} is not a readable tar: {e}", archive.uri))
                })?;
                for (i, entry) in entries.enumerate() {
                    let entry = entry.map_err(|e| {
                        EngineError::Query(format!("archive: {} member {i}: {e}", archive.uri))
                    })?;
                    if !entry.header().entry_type().is_file() {
                        continue;
                    }
                    let name = entry
                        .path()
                        .map(|p| p.to_string_lossy().replace('\\', "/"))
                        .unwrap_or_else(|_| format!("member-{i}"));
                    if !member_wanted(&name, spec) {
                        continue;
                    }
                    budget.take_member(&name)?;
                    let compressed = entry.header().size().ok().map(|n| n as i64);
                    self.land_member(
                        spec, archive, &name, i, compressed, entry, &mut budget, out, artifacts,
                    )?;
                }
            }
            ArchiveKind::Gzip => {
                // One compressed stream rather than named members, so the name
                // comes from the archive with its .gz taken off.
                let raw = self.open_artifact(&spec.input.auth, &archive.uri)?;
                let name = archive
                    .uri
                    .rsplit(['/', '\\'])
                    .next()
                    .unwrap_or("member")
                    .trim_end_matches(".gz")
                    .to_string();
                if member_wanted(&name, spec) {
                    budget.take_member(&name)?;
                    let decoded = flate2::read::GzDecoder::new(raw);
                    self.land_member(
                        spec, archive, &name, 0, None, decoded, &mut budget, out, artifacts,
                    )?;
                }
            }
            ArchiveKind::Unknown => {
                return Err(EngineError::Config(format!(
                    "archive: {} is not an archive this can open - expected .zip, .tar, .tar.gz, \
                     .tgz or .gz",
                    archive.uri
                )))
            }
        }
        Ok(())
    }

    /// Land one member and record what it produced.
    #[allow(clippy::too_many_arguments)]
    fn land_member(
        &self,
        spec: &plan::ArchiveExtractSpec,
        archive: &ResolvedArtifact,
        name: &str,
        index: usize,
        compressed_size: Option<i64>,
        reader: impl std::io::Read,
        budget: &mut MemberBudget,
        out: &mut Vec<JsonValue>,
        artifacts: &mut Vec<crate::ArtifactRef>,
    ) -> Result<(), EngineError> {
        let leaf = name.rsplit('/').next().unwrap_or(name).to_string();
        let key = match spec.naming.as_str() {
            "flat" => leaf.clone(),
            // Content-addressed naming would need the hash before the key, and
            // a member cannot be read twice out of a streaming archive without
            // spooling it. So it is landed under its own name first and the
            // hash reported; a content-addressed store is a copy away.
            _ => name.to_string(),
        };
        let dest = join_destination(&spec.destination, &key);

        // Bounded by the member, not by the archive: the reader is capped so a
        // small archive that expands to fill the disk is refused while it is
        // being read rather than after.
        let capped = CappedReader { inner: reader, remaining: budget.remaining_bytes };
        let (sha, size, remaining) = match spec.if_exists.as_str() {
            "skip" if self.archive_dest_size(spec, &dest)?.is_some() => {
                // #284: a skipped member used to be emitted with a NULL size
                // and sha256 and never reached the run manifest. That is the
                // normal retry path - extract, downstream fails, source state
                // does not advance, same archive arrives again - so the exact
                // same logical input produced weaker provenance the second
                // time round. It now carries the identity it had on the run
                // that wrote it.
                let existing = self.archive_dest_size(spec, &dest)?.unwrap_or(-1);
                let mut capped = capped;
                let (sha, size) = Self::hash_member(&mut capped)?;
                if existing != size {
                    return Err(EngineError::Query(format!(
                        concat!(
                            "archive: {} already exists at {} bytes but ",
                            "this member is {}. 'skip' means the destination is ",
                            "already this member; it is not. Use ifExists 'replace' to ",
                            "overwrite it or 'error' to stop sooner."
                        ),
                        dest, existing, size
                    )));
                }
                budget.remaining_bytes = capped.remaining;
                artifacts.push(crate::ArtifactRef {
                    node: spec.node_id.clone(),
                    role: "output".into(),
                    uri: dest.clone(),
                    name: Some(leaf.clone()),
                    media_type: Some(media_type_for(&leaf).to_string()),
                    size_bytes: Some(size),
                    sha256: Some(sha.clone()),
                    etag: None,
                    modified_at: None,
                });
                out.push(serde_json::json!({
                    "archive_uri": archive.uri,
                    "member_name": name,
                    "member_index": index as u64,
                    "uri": dest,
                    "media_type": media_type_for(&leaf),
                    "compressed_size": compressed_size,
                    "size_bytes": size,
                    "sha256": sha,
                }));
                return Ok(());
            }
            "error" if self.archive_dest_size(spec, &dest)?.is_some() => {
                return Err(EngineError::Query(format!(
                    "archive: {} already exists and ifExists is 'error'",
                    dest
                )))
            }
            _ => {
                let mut capped = capped;
                let (sha, size) =
                    self.land_bytes(&spec.input.auth, &mut capped, &dest, spec.part_size_bytes)?;
                if capped.remaining == 0 {
                    return Err(EngineError::Query(format!(
                        "archive: {} expands past the {} GB limit for one archive. An archive \
                         from an external publisher is untrusted input, so this refuses rather \
                         than filling the volume; raise the limit if the data really is that big.",
                        budget.archive_uri,
                        spec.max_uncompressed_bytes / (1024 * 1024 * 1024)
                    )));
                }
                (sha, size, capped.remaining)
            }
        };
        budget.remaining_bytes = remaining;

        artifacts.push(crate::ArtifactRef {
            node: spec.node_id.clone(),
            role: "output".into(),
            uri: dest.clone(),
            name: Some(leaf.clone()),
            media_type: Some(media_type_for(&leaf).to_string()),
            size_bytes: Some(size as i64),
            sha256: Some(sha.clone()),
            etag: None,
            modified_at: None,
        });
        out.push(serde_json::json!({
            "archive_uri": archive.uri,
            "member_name": name,
            "member_index": index as u64,
            "uri": dest,
            "media_type": media_type_for(&leaf),
            "compressed_size": compressed_size,
            "size_bytes": size as i64,
            "sha256": sha,
        }));
        Ok(())
    }

    /// The destination's size if it is already there.
    ///
    /// Size rather than a bare bool because `skip` has to report the artifact
    /// it skipped, and "something is at this path" is not an identity.
    fn archive_dest_size(
        &self,
        spec: &plan::ArchiveExtractSpec,
        dest: &str,
    ) -> Result<Option<i64>, EngineError> {
        if dest.starts_with("s3://") || dest.starts_with("s3a://") {
            let Some(cfg) = spec.input.auth.s3.as_ref() else {
                return Ok(None);
            };
            let (bucket, key) = crate::s3::parse_s3_uri(dest)?;
            return match cfg.head(&bucket, &key) {
                Ok(o) => Ok(Some(o.size.unwrap_or(-1))),
                Err(e) if e.to_string().contains("HTTP 404") => Ok(None),
                Err(e) => Err(e),
            };
        }
        Ok(std::fs::metadata(dest).ok().map(|m| m.len() as i64))
    }

    /// Read a member to its end, hashing it, writing nothing.
    ///
    /// This is what makes a skipped member keep its identity. The bytes have to
    /// come off a streaming archive anyway to reach the next member, so hashing
    /// them costs no extra I/O against the source, and it means a retry reports
    /// the same sha256 as the run that actually wrote the file.
    fn hash_member(reader: &mut impl std::io::Read) -> Result<(String, i64), EngineError> {
        let mut hashing = crate::s3::HashingReader::new(reader);
        std::io::copy(&mut hashing, &mut std::io::sink())
            .map_err(|e| EngineError::Query(format!("archive: reading member: {e}")))?;
        let (sha, size) = hashing.finish();
        Ok((sha, size as i64))
    }

    /// qa.baseline: compare this run against what previous runs looked like.
    ///
    /// #281: the dangerous failure is the one that stays green. Every row can
    /// satisfy the schema and every row-level rule while the dataset is nothing
    /// like what normally arrives, and that publishes successfully.
    ///
    /// Deterministic on purpose - rolling summary statistics and explicit
    /// thresholds, no model. What it compares against is the MEDIAN of the last
    /// N accepted profiles, so one odd day does not drag the baseline with it.
    pub(crate) fn run_baseline(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &plan::BaselineSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        let current = self.profile_relation(db, secret_prefix, spec)?;
        let path = baseline_state_path(pipeline_name, &spec.node_id);
        let prior = path.as_deref().and_then(crate::read_state_snapshot);
        let history: Vec<JsonValue> = prior
            .as_deref()
            .and_then(|t| serde_json::from_str::<JsonValue>(t).ok())
            .and_then(|v| v.get("profiles").cloned())
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();

        let mut rows: Vec<JsonValue> = Vec::new();
        let mut violations: Vec<String> = Vec::new();

        if history.is_empty() {
            // Nothing to compare against yet. This is the first run, not a
            // pass: saying "ok" would let the very first run establish any
            // baseline at all, including a broken one, and look verified doing
            // it.
            rows.push(serde_json::json!({
                "metric": "baseline",
                "column": JsonValue::Null,
                "group": JsonValue::Null,
                "baseline_value": JsonValue::Null,
                "current_value": JsonValue::Null,
                "change": JsonValue::Null,
                "change_pct": JsonValue::Null,
                "status": "first_run",
                "detail": "no accepted profile yet - this run becomes the first baseline",
            }));
        } else {
            for rule in &spec.rules {
                let key = metric_key(&rule.metric, rule.column.as_deref());
                let cur = current.get(&key).and_then(JsonValue::as_f64);
                let base = median_of(&history, &key);
                let (status, detail) = match (base, cur) {
                    (Some(b), Some(c)) => judge(rule, b, c),
                    (None, _) => (
                        "unknown".to_string(),
                        format!("no baseline for {key} in the accepted history"),
                    ),
                    (_, None) => (
                        "unknown".to_string(),
                        format!("{key} could not be measured on this run"),
                    ),
                };
                let change = match (base, cur) {
                    (Some(b), Some(c)) => Some(c - b),
                    _ => None,
                };
                let change_pct = match (base, cur) {
                    (Some(b), Some(c)) if b != 0.0 => Some((c - b) / b * 100.0),
                    _ => None,
                };
                if status == "violation" {
                    violations.push(detail.clone());
                }
                rows.push(serde_json::json!({
                    "metric": rule.metric,
                    "column": rule.column,
                    "group": JsonValue::Null,
                    "baseline_value": base,
                    "current_value": cur,
                    "change": change,
                    "change_pct": change_pct,
                    "status": status,
                    "detail": detail,
                }));
            }

            // A partition that disappeared is the case a row count cannot see:
            // the total can stay in range while a whole country stops arriving.
            if spec.require_existing_groups && !spec.group_by.is_empty() {
                let cur_groups = group_set(&current);
                let base_groups: std::collections::BTreeSet<String> = history
                    .iter()
                    .filter_map(|p| p.as_object())
                    .flat_map(|p| group_set(p).into_iter())
                    .collect();
                for missing in base_groups.difference(&cur_groups) {
                    let detail = format!("group '{missing}' was in the baseline and is not here");
                    violations.push(detail.clone());
                    rows.push(serde_json::json!({
                        "metric": "group_present",
                        "column": JsonValue::Null,
                        "group": missing,
                        "baseline_value": 1.0,
                        "current_value": 0.0,
                        "change": -1.0,
                        "change_pct": -100.0,
                        "status": "violation",
                        "detail": detail,
                    }));
                }
            }
        }

        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;

        // #281: record what this run MEASURED, whatever it then decides.
        //
        // Deliberately not deferred like the accepted history below. The
        // run that gets refused is precisely the one whose numbers an
        // operator needs to look at and possibly accept as the new normal;
        // writing the observation only on success would throw it away in
        // every case where accepting is the thing you want to do.
        if let Some(p) = path.as_deref() {
            let status = if !violations.is_empty() {
                "violation"
            } else if history.is_empty() {
                "first_run"
            } else {
                "ok"
            };
            crate::baseline::record_observation(
                &p.with_extension("observed.json"),
                &JsonValue::Object(current.clone().into_iter().collect()),
                status,
                &violations,
            );
        }

        // The new profile is accepted only if the whole run succeeds - the same
        // deferred flush a watermark gets, and for the same reason: a run that
        // failed downstream must not leave today's numbers as the new normal.
        if let Some(p) = path {
            let mut kept = history.clone();
            kept.push(JsonValue::Object(current.clone().into_iter().collect()));
            let keep_from = kept.len().saturating_sub(spec.history);
            let kept: Vec<JsonValue> = kept[keep_from..].to_vec();
            pending.push(crate::PendingWrite::state(
                p,
                serde_json::json!({ "profiles": kept }),
                prior,
            ));
        }

        if !violations.is_empty() && spec.mode == "gate" {
            return Err(EngineError::Query(format!(
                "baseline: this run does not look like the ones before it. {}",
                violations.join("; ")
            )));
        }
        Ok(if violations.is_empty() {
            format!("baseline: {} rule(s) checked, all within range", spec.rules.len())
        } else {
            format!(
                "baseline: {} finding(s) reported - {}",
                violations.len(),
                violations.join("; ")
            )
        })
    }

    /// Measure this run: the dataset-level and per-column numbers a rule can be
    /// written against, plus a row count per group when one is configured.
    fn profile_relation(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &plan::BaselineSpec,
    ) -> Result<serde_json::Map<String, JsonValue>, EngineError> {
        let view = plan::quote_ident(&spec.from_view);
        let described = self.run_rows(
            Some(db),
            &format!("{}DESCRIBE SELECT * FROM {}", secret_prefix, view),
        )?;
        let all: Vec<String> = described
            .iter()
            .filter_map(|r| r.get("column_name").and_then(|v| v.as_str()))
            .map(str::to_string)
            .collect();
        let columns: Vec<String> = if spec.columns.is_empty() {
            all
        } else {
            spec.columns
                .iter()
                .filter(|c| all.iter().any(|a| a == *c))
                .cloned()
                .collect()
        };

        let mut selects: Vec<String> = vec!["COUNT(*) AS row_count".to_string()];
        for c in &columns {
            let q = plan::quote_ident(c);
            let safe = metric_ident(c);
            selects.push(format!("COUNT({q}) AS \"{safe}__nonnull\""));
            selects.push(format!("approx_count_distinct({q}) AS \"{safe}__distinct\""));
            // Cast through DOUBLE so a rule can be written against any column
            // whose values are ordered; a non-numeric column simply yields NULL
            // rather than failing the whole profile.
            selects.push(format!("TRY_CAST(MIN({q}) AS DOUBLE) AS \"{safe}__min\""));
            selects.push(format!("TRY_CAST(MAX({q}) AS DOUBLE) AS \"{safe}__max\""));
            selects.push(format!("TRY_CAST(AVG(TRY_CAST({q} AS DOUBLE)) AS DOUBLE) AS \"{safe}__mean\""));
        }
        let sql = format!(
            "{}SELECT {} FROM {}",
            secret_prefix,
            selects.join(", "),
            view
        );
        let measured = self.run_rows(Some(db), &sql)?;
        let first = measured.first().cloned().unwrap_or(JsonValue::Null);

        let num = |v: Option<&JsonValue>| -> Option<f64> {
            v.and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
            })
        };
        let row_count = num(first.get("row_count")).unwrap_or(0.0);
        let mut out = serde_json::Map::new();
        out.insert("row_count".into(), serde_json::json!(row_count));
        for c in &columns {
            let safe = metric_ident(c);
            let nonnull = num(first.get(format!("{safe}__nonnull").as_str())).unwrap_or(0.0);
            let nulls = (row_count - nonnull).max(0.0);
            out.insert(metric_key("null_count", Some(c)), serde_json::json!(nulls));
            out.insert(
                metric_key("null_pct", Some(c)),
                serde_json::json!(if row_count > 0.0 { nulls / row_count } else { 0.0 }),
            );
            for m in ["distinct", "min", "max", "mean"] {
                let name = if m == "distinct" { "distinct_count" } else { m };
                if let Some(v) = num(first.get(format!("{safe}__{m}").as_str())) {
                    out.insert(metric_key(name, Some(c)), serde_json::json!(v));
                }
            }
        }

        if !spec.group_by.is_empty() {
            let keys: Vec<String> = spec.group_by.iter().map(|g| plan::quote_ident(g)).collect();
            let label = keys
                .iter()
                .map(|k| format!("COALESCE({k}::VARCHAR, '')"))
                .collect::<Vec<_>>()
                .join(" || '|' || ");
            let sql = format!(
                "{}SELECT {} AS g, COUNT(*) AS n FROM {} GROUP BY 1",
                secret_prefix, label, view
            );
            let mut groups = serde_json::Map::new();
            for r in self.run_rows(Some(db), &sql)? {
                if let Some(g) = r.get("g").and_then(|v| v.as_str()) {
                    groups.insert(g.to_string(), serde_json::json!(num(r.get("n")).unwrap_or(0.0)));
                }
            }
            out.insert("__groups".into(), JsonValue::Object(groups));
        }
        Ok(out)
    }

    /// Every entry in a remote directory.
    fn list_remote_entries(
        &self,
        spec: &plan::ChangedSourceSpec,
    ) -> Result<Vec<RemoteEntry>, EngineError> {
        if spec.uri.starts_with("s3://") || spec.uri.starts_with("s3a://") {
            return self.s3_list(spec);
        }
        if !spec.uri.starts_with("sftp://") {
            return Err(EngineError::Config(format!(
                "changed: listing needs sftp:// or s3://, not {}. HTTP has no \
                 standard directory listing, so there is nothing to enumerate over it.",
                spec.uri
            )));
        }
        let (host, port, user, path) = parse_sftp_uri(&spec.uri)?;
        let user = spec.user.clone().or(user).unwrap_or_default();
        self.sftp_list(spec, &host, port, &user, &path)
    }

    /// Connect, run `f` against the SFTP session, disconnect. Shared by the
    /// stat and list paths so the auth and host-key handling exist once.
    fn with_sftp<T, F>(
        &self,
        spec: &plan::ChangedSourceSpec,
        host: &str,
        port: u16,
        user: &str,
        f: F,
    ) -> Result<T, EngineError>
    where
        F: for<'a> FnOnce(
            &'a russh_sftp::client::SftpSession,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, String>> + 'a>,
        >,
    {
        use russh_sftp::client::SftpSession;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("changed/sftp: tokio rt: {}", e)))?;
        let result: Result<T, String> = rt.block_on(async {
            let config = std::sync::Arc::new(russh::client::Config::default());
            let refused = std::sync::Arc::new(std::sync::Mutex::new(None));
            let handler = SftpVerifier {
                expected: spec.host_fingerprint.clone(),
                hostport: format!("{}:{}", host, port),
                refused: refused.clone(),
            };
            let mut session = russh::client::connect(config, (host, port), handler)
                .await
                .map_err(|e| match refused.lock().unwrap().take() {
                    Some(why) => why,
                    None => format!("connect {}:{}: {}", host, port, e),
                })?;
            let authed = if let Some(pem) = &spec.private_key {
                let key = russh::keys::decode_secret_key(pem, spec.key_passphrase.as_deref())
                    .map_err(|e| format!("private key: {}", e))?;
                let with_alg = russh::keys::PrivateKeyWithHashAlg::new(
                    std::sync::Arc::new(key),
                    Some(russh::keys::HashAlg::Sha256),
                );
                session
                    .authenticate_publickey(user, with_alg)
                    .await
                    .map_err(|e| format!("publickey auth: {}", e))?
                    .success()
            } else if let Some(pw) = &spec.password {
                session
                    .authenticate_password(user, pw)
                    .await
                    .map_err(|e| format!("password auth: {}", e))?
                    .success()
            } else {
                return Err("no credentials: set a password or a private key".into());
            };
            if !authed {
                return Err(format!("authentication failed for user '{}'", user));
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
            f(&sftp).await
        });
        result.map_err(|e| EngineError::Query(format!("changed/sftp: {}", e)))
    }

    /// One remote file's size and mtime.
    fn sftp_stat(
        &self,
        spec: &plan::ChangedSourceSpec,
        host: &str,
        port: u16,
        user: &str,
        path: &str,
    ) -> Result<RemoteEntry, EngineError> {
        let uri = spec.uri.clone();
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        let p = path.to_string();
        let (size, mtime) = self.with_sftp(spec, host, port, user, move |sftp| {
            Box::pin(async move {
                let md = sftp
                    .metadata(p.clone())
                    .await
                    .map_err(|e| format!("stat {}: {}", p, e))?;
                Ok((md.size.map(|s| s as i64), md.mtime))
            })
        })?;
        let modified = mtime.map(|m| m.to_string());
        Ok(RemoteEntry {
            fingerprint: remote_fingerprint(None, modified.as_deref(), size),
            uri,
            name,
            size,
            modified_at: modified,
            etag: None,
        })
    }

    /// Every file in a remote directory, with size and mtime.
    fn sftp_list(
        &self,
        spec: &plan::ChangedSourceSpec,
        host: &str,
        port: u16,
        user: &str,
        dir: &str,
    ) -> Result<Vec<RemoteEntry>, EngineError> {
        let d = dir.to_string();
        let raw = self.with_sftp(spec, host, port, user, move |sftp| {
            Box::pin(async move {
                let entries = sftp
                    .read_dir(d.clone())
                    .await
                    .map_err(|e| format!("list {}: {}", d, e))?;
                let mut out = Vec::new();
                for e in entries {
                    let meta = e.metadata();
                    // Directories are not objects to process. Recursing would
                    // turn one poll into an unbounded walk.
                    if meta.is_dir() {
                        continue;
                    }
                    out.push((
                        e.file_name(),
                        meta.size.map(|s| s as i64),
                        meta.mtime,
                    ));
                }
                Ok(out)
            })
        })?;

        let base = format!(
            "sftp://{}{}{}",
            if user.is_empty() { String::new() } else { format!("{}@", user) },
            if port == 22 { host.to_string() } else { format!("{}:{}", host, port) },
            dir
        );
        let base = base.trim_end_matches('/').to_string();
        let mut out: Vec<RemoteEntry> = raw
            .into_iter()
            .filter(|(name, _, _)| match &spec.suffix {
                Some(s) => name.ends_with(s.as_str()),
                None => true,
            })
            .map(|(name, size, mtime)| {
                let modified = mtime.map(|m| m.to_string());
                RemoteEntry {
                    fingerprint: remote_fingerprint(None, modified.as_deref(), size),
                    uri: format!("{}/{}", base, name),
                    name,
                    size,
                    modified_at: modified,
                    etag: None,
                }
            })
            .collect();
        // Oldest first, so a capped run works through a backlog in order
        // rather than taking an arbitrary slice of it.
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// src.spool: read an append-only NDJSON file from where the last
    /// successful run stopped.
    ///
    /// Reads bytes `[saved_offset, EOF)`, keeps whole lines only, and queues
    /// the new offset for the deferred flush - so a run that fails downstream
    /// leaves the offset where it was and the next pass re-reads exactly the
    /// records that did not land.
    ///
    /// A partial trailing line is left for next time rather than parsed. The
    /// writer appends, so a line that is short right now is a line still being
    /// written, not a corrupt one.
    pub(crate) fn run_spool_source(
        &self,
        db: &Path,
        spec: &plan::SpoolSourceSpec,
        pipeline_name: Option<&str>,
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        use std::io::{Read, Seek, SeekFrom};

        let path = std::path::Path::new(&spec.path);
        let state_path = if spec.track_offset {
            incremental_state_path(pipeline_name, &spec.node_id)
        } else {
            None
        };
        let prior = state_path.as_deref().and_then(crate::read_state_snapshot);
        let saved = state_path
            .as_deref()
            .and_then(read_spool_offset_state)
            .unwrap_or(0);

        // A spool that does not exist yet is an empty one. A listener may not
        // have received anything, and that is not an error.
        let mut file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.spool_empty_relation(db, &spec.node_id, path)?;
                return Ok(format!(
                    "spool: {} does not exist yet; 0 records into {}",
                    spec.path, spec.node_id
                ));
            }
            Err(e) => {
                return Err(EngineError::Query(format!("spool: open {}: {}", spec.path, e)))
            }
        };
        let len = file
            .metadata()
            .map_err(|e| EngineError::Query(format!("spool: stat {}: {}", spec.path, e)))?
            .len();

        // The file got SHORTER than where we stopped, so it was truncated or
        // rotated under us. Resuming at the old offset would read from the
        // middle of a different file, so start again and say so - silently
        // skipping to the end would drop everything written since.
        let (start, rotated) = if saved > len { (0, true) } else { (saved, false) };

        let take = (len - start).min(spec.max_bytes);
        if take == 0 {
            self.spool_empty_relation(db, &spec.node_id, path)?;
            return Ok(format!(
                "{}spool: no new records in {}",
                crate::UNCHANGED_MARKER,
                spec.path
            ));
        }
        file.seek(SeekFrom::Start(start))
            .map_err(|e| EngineError::Query(format!("spool: seek {}: {}", spec.path, e)))?;
        let mut buf = vec![0u8; take as usize];
        file.read_exact(&mut buf)
            .map_err(|e| EngineError::Query(format!("spool: read {}: {}", spec.path, e)))?;

        // Only whole lines. Everything after the last newline is a record still
        // being written, or one cut off by max_bytes; either way it belongs to
        // the next pass, and the offset must stop before it.
        let consumed = match buf.iter().rposition(|b| *b == b'\n') {
            Some(i) => i + 1,
            None => 0,
        };
        if consumed == 0 {
            self.spool_empty_relation(db, &spec.node_id, path)?;
            return Ok(format!(
                "spool: {} has a partial record and no complete one; waiting for the rest",
                spec.path
            ));
        }
        let text = String::from_utf8_lossy(&buf[..consumed]);
        let mut rows: Vec<JsonValue> = Vec::new();
        let mut skipped = 0usize;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<JsonValue>(line) {
                Ok(v) => rows.push(v),
                // One unparseable line must not wedge the spool forever: the
                // offset moves past it either way, so count it and carry on.
                Err(_) => skipped += 1,
            }
        }
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;

        let next_offset = start + consumed as u64;
        if let Some(p) = state_path {
            pending.push(crate::PendingWrite::state(
                p,
                serde_json::json!({
                    "path": spec.path,
                    "next_offset": next_offset,
                }),
                prior,
            ));
        }
        Ok(format!(
            "spool: materialized {} record(s) into {}{}{}{}",
            count,
            spec.node_id,
            if skipped > 0 {
                format!(" ({} unparseable line(s) skipped)", skipped)
            } else {
                String::new()
            },
            if rotated {
                " (the file was shorter than the saved position, so it was read from the start)"
            } else {
                ""
            },
            if spec.track_offset {
                format!(" (resumes at byte {} if this run succeeds)", next_offset)
            } else {
                String::new()
            }
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
        // SCAN can return the same key on more than one page (documented
        // behavior, especially while the keyspace is rehashed under
        // concurrent writes), so de-duplicate as we walk and count the
        // limit against UNIQUE keys - otherwise duplicates both produce
        // duplicate output rows and prematurely trip the cap.
        let mut keys: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cursor: u64 = 0;
        'scan: loop {
            self.check_cancelled()?;
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&spec.key_pattern)
                .arg("COUNT")
                .arg(500_u32)
                .query(&mut conn)
                .map_err(|e| EngineError::Query(format!("redis: SCAN: {}", e)))?;
            for k in batch {
                if seen.insert(k.clone()) {
                    keys.push(k);
                    if keys.len() as u64 >= spec.limit {
                        break 'scan;
                    }
                }
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        let mut rows: Vec<JsonValue> = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(500) {
            self.check_cancelled()?;
            // Check each key's TYPE first (TYPE never returns WRONGTYPE),
            // then GET only the plain-string keys. A non-string key
            // (hash/list/set/zset/stream) under the matched pattern must
            // not abort the whole pipelined batch - it yields a NULL value.
            let mut type_pipe = redis::pipe();
            for k in chunk {
                type_pipe.cmd("TYPE").arg(k);
            }
            let types: Vec<String> = redis::Pipeline::query(&type_pipe, &mut conn)
                .map_err(|e| EngineError::Query(format!("redis: TYPE batch: {}", e)))?;
            let string_keys: Vec<&String> = chunk
                .iter()
                .zip(types.iter())
                .filter(|(_, t)| t.as_str() == "string")
                .map(|(k, _)| k)
                .collect();
            let values: Vec<Option<String>> = if string_keys.is_empty() {
                Vec::new()
            } else {
                let mut get_pipe = redis::pipe();
                for k in &string_keys {
                    get_pipe.cmd("GET").arg(*k);
                }
                redis::Pipeline::query(&get_pipe, &mut conn)
                    .map_err(|e| EngineError::Query(format!("redis: GET batch: {}", e)))?
            };
            let mut got_values = values.into_iter();
            for (k, t) in chunk.iter().zip(types.iter()) {
                let value = if t.as_str() == "string" {
                    got_values
                        .next()
                        .flatten()
                        .map(JsonValue::String)
                        .unwrap_or(JsonValue::Null)
                } else {
                    JsonValue::Null
                };
                let mut obj = serde_json::Map::new();
                obj.insert("key".into(), JsonValue::String(k.clone()));
                obj.insert("value".into(), value);
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
        let count = rows.len();
        // Move the rows into the JSON array rather than cloning the whole
        // dataset just to read its length back afterwards.
        let payload = JsonValue::Array(rows);
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
            count,
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

    /// snk.model: record one trained model's card (#253).
    ///
    /// The card IS the upstream row: whatever columns the training stage
    /// produced - artifact URI, framework, metrics, the hashes it chose to
    /// record - are what gets written, plus the model name and the moment it was
    /// registered. That keeps the engine out of the business of deciding what a
    /// model card should contain, which differs per team.
    ///
    /// The write is QUEUED, not done here. It happens at the end of the run and
    /// only if the whole run succeeded, so a training pipeline that blows up
    /// after this stage never leaves a registered model pointing at a model that
    /// was never finished. That gate, and the `latest` pointer, are the two
    /// things a plain file-writing convention cannot give you.
    pub(crate) fn run_model_card(
        &self,
        db: &Path,
        spec: &ModelCardSpec,
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        // One model, one card. Silently taking the first of several rows would
        // register a model chosen by whatever order the upstream happened to
        // produce, which is not a decision the engine should make.
        if rows.len() != 1 {
            return Err(EngineError::Query(format!(
                "snk.model: expected exactly one upstream row to register as a model card, got {}. Reduce the training stage's output to a single row first.",
                rows.len()
            )));
        }
        let mut card = match &rows[0] {
            JsonValue::Object(m) => m.clone(),
            other => {
                return Err(EngineError::Query(format!(
                    "snk.model: the upstream row is not a record: {}",
                    other
                )))
            }
        };
        // The version names the file, so it has to be there and has to be
        // something. Defaulting it would silently overwrite the previous card.
        let version = match card.get("version") {
            Some(JsonValue::String(v)) if !v.trim().is_empty() => v.trim().to_string(),
            Some(JsonValue::Number(n)) => n.to_string(),
            _ => {
                return Err(EngineError::Query(
                    "snk.model: the upstream row needs a non-empty `version` column - it names the card and is how an older model is still addressable after a retrain".into(),
                ))
            }
        };
        card.insert("name".into(), JsonValue::String(spec.name.clone()));
        card.insert(
            "registered_at".into(),
            JsonValue::String(chrono::Utc::now().to_rfc3339()),
        );
        let card = JsonValue::Object(card);

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
        let dir = std::path::Path::new(&spec.dir).join(safe(&spec.name));
        pending.push(crate::PendingWrite::output(
            dir.join(format!("{}.json", safe(&version))),
            card.clone(),
        ));
        // The pointer is a copy of the card, not a reference to it, so reading
        // `latest` is one read and cannot race with the versioned file moving.
        pending.push(crate::PendingWrite::output(dir.join("latest.json"), card));
        Ok(format!(
            "model: {} version {} will be registered if this run succeeds",
            spec.name, version
        ))
    }

    /// src.pdf: one row per PAGE of a PDF document (#248).
    ///
    /// A lot of real data engineering starts from documents rather than tables:
    /// filings, annual accounts, invoices, regulatory publications. This reads
    /// the text layer a PDF already carries, alongside the page geometry and the
    /// document's own metadata, so a page becomes a row a pipeline can filter,
    /// join and hand to a Python or AI stage like any other.
    ///
    /// It does NOT do OCR, and deliberately: a scanned page has no text layer,
    /// and rasterising one needs a native rendering engine plus per-language
    /// trained data, which would end the self-contained cross-OS build this repo
    /// protects everywhere else. `has_text_layer` is false for those pages, which
    /// is what makes them findable - filter on it and route them wherever your
    /// OCR lives.
    pub(crate) fn run_pdf_source(
        &self,
        db: &Path,
        secret_prefix: &str,
        spec: &PdfSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        // #282: the documents are whatever an upstream relation names, or the
        // configured path when nothing is wired in.
        let from_upstream = spec.input.from_view.is_some();
        let docs: Vec<(String, Option<String>, JsonValue)> = if from_upstream {
            self.resolve_artifact_inputs(db, secret_prefix, &spec.input)?
                .into_iter()
                .map(|a| (a.uri, a.sha256, a.row))
                .collect()
        } else {
            expand_pdf_paths(&spec.path, spec.recursive)
                .into_iter()
                .map(|f| (f, None, JsonValue::Null))
                .collect()
        };
        // An upstream that named nothing is a legitimate quiet run - a change
        // feed with no new documents - and must produce the empty typed
        // relation rather than an error or a relation of unknown shape.
        if docs.is_empty() && from_upstream {
            self.pdf_empty_relation(db, spec)?;
            return Ok(format!("{}pdf: 0 documents to read", crate::UNCHANGED_MARKER));
        }
        if docs.is_empty() {
            return Err(EngineError::Config(format!(
                "pdf: no .pdf files at {}",
                spec.path
            )));
        }
        let mut writer = match &spec.declared_schema {
            Some(schema) if !schema.is_empty() => {
                JsonLinesWriter::open_with_schema(&spec.node_id, Some(schema.clone()))?
            }
            _ => JsonLinesWriter::open(&spec.node_id)?,
        };
        let mut count: usize = 0;
        let mut skipped: usize = 0;
        for (uri, source_sha, upstream_row) in &docs {
            self.check_cancelled()?;
            // A PDF reader SEEKS - the cross-reference table is at the end of
            // the file - so a remote document has to become a local one. One at
            // a time, and removed when the guard drops however the parse ended.
            let spooled = match self.local_copy_of_artifact(&spec.input.auth, uri) {
                Ok(s) => s,
                Err(e) if spec.on_error == "skip" => {
                    eprintln!("duckle: pdf: skipping {uri}: {e}");
                    skipped += 1;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let file = &spooled.path.to_string_lossy().to_string();
            let doc = match lopdf::Document::load(file) {
                Ok(d) => d,
                Err(e) if spec.on_error == "skip" => {
                    eprintln!("duckle: pdf: skipping {uri}: {e}");
                    skipped += 1;
                    continue;
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("pdf: open {}: {}", uri, e)))
                }
            };
            let pages = doc.get_pages();
            let page_count = pages.len();

            // The document Info dictionary, when it has one. Missing entries are
            // simply absent rather than empty strings: a PDF with no author and
            // one with an empty author are different.
            let mut meta = serde_json::Map::new();
            if let Ok(lopdf::Object::Reference(id)) = doc.trailer.get(b"Info") {
                if let Ok(info) = doc.get_dictionary(*id) {
                    for (key, name) in [
                        (&b"Title"[..], "title"),
                        (&b"Author"[..], "author"),
                        (&b"Creator"[..], "creator"),
                        (&b"Producer"[..], "producer"),
                        (&b"CreationDate"[..], "created"),
                    ] {
                        if let Ok(v) = info.get(key) {
                            if let Ok(s) = v.as_str() {
                                meta.insert(
                                    name.to_string(),
                                    JsonValue::String(String::from_utf8_lossy(s).into_owned()),
                                );
                            }
                        }
                    }
                }
            }
            meta.insert("page_count".into(), JsonValue::from(page_count as u64));
            let meta = JsonValue::Object(meta);

            // pdf-extract has a reputation for panicking on malformed input, and
            // a panic here would take the whole run down rather than failing one
            // stage with a message naming the file.
            let path_owned = file.clone();
            let texts: Vec<String> =
                match std::panic::catch_unwind(move || pdf_extract::extract_text_by_pages(&path_owned)) {
                    Ok(Ok(t)) => t,
                    Ok(Err(e)) => {
                        return Err(EngineError::Query(format!(
                            "pdf: extract text from {}: {}",
                            file, e
                        )))
                    }
                    Err(_) => {
                        return Err(EngineError::Query(format!(
                            "pdf: {} could not be parsed (the file is malformed, or uses a feature the text extractor cannot read)",
                            file
                        )))
                    }
                };

            for (idx, (page_number, page_id)) in pages.iter().enumerate() {
                let (width, height) = page_media_box(&doc, *page_id);
                let text = texts.get(idx).cloned().unwrap_or_default();
                let mut row = serde_json::Map::new();
                // Same value src.artifact puts in `uri`, so the two join.
                // The URI, not the spool path: a temp file nobody can look at
                // afterwards is not provenance. Both names carry it, so a
                // pipeline joining on `document_id` keeps working.
                row.insert("document_id".into(), JsonValue::String(uri.clone()));
                row.insert("document_uri".into(), JsonValue::String(uri.clone()));
                // #282: the business keys that say what this document IS -
                // company_id, filing_id - live on the artifact row and are lost
                // the moment pages are emitted instead. Carrying them is what
                // lets a page be joined back to the thing it came from.
                for key in &spec.input.carry {
                    row.insert(
                        key.clone(),
                        upstream_row.get(key).cloned().unwrap_or(JsonValue::Null),
                    );
                }
                row.insert(
                    "source_sha256".into(),
                    match source_sha {
                        // Carried from whatever landed the bytes. Absent rather
                        // than recomputed: re-hashing would describe whatever is
                        // at that URI now, not what was parsed.
                        Some(h) => JsonValue::String(h.clone()),
                        None => JsonValue::Null,
                    },
                );
                row.insert("page_number".into(), JsonValue::from(*page_number as u64));
                // A page whose text is only whitespace has no usable text layer,
                // which is the scanned-page case worth routing elsewhere.
                row.insert(
                    "has_text_layer".into(),
                    JsonValue::Bool(!text.trim().is_empty()),
                );
                row.insert("text".into(), JsonValue::String(text));
                match width {
                    Some(w) => row.insert("width".into(), JsonValue::from(w)),
                    None => row.insert("width".into(), JsonValue::Null),
                };
                match height {
                    Some(h) => row.insert("height".into(), JsonValue::from(h)),
                    None => row.insert("height".into(), JsonValue::Null),
                };
                row.insert("metadata".into(), meta.clone());
                writer.write_row(&JsonValue::Object(row))?;
                count += 1;
            }
        }
        match &spec.declared_schema {
            Some(schema) if !schema.is_empty() => {
                let (columns_spec, select_list) = xml_declared_columns(schema);
                writer.finalize_typed(&self.bin, db, &spec.node_id, &columns_spec, &select_list)?;
            }
            _ => writer.finalize_into_table(&self.bin, db, &spec.node_id)?,
        }
        Ok(format!(
            "pdf: materialized {} page(s) from {} document(s){}",
            count,
            docs.len() - skipped,
            if skipped > 0 {
                format!(" ({} skipped as unreadable)", skipped)
            } else {
                String::new()
            }
        ))
    }

    /// The shape src.pdf always emits, with no rows in it.
    ///
    /// A change feed that found no new documents is a quiet success, and a
    /// downstream stage must see the right columns rather than an error or a
    /// relation whose shape depends on whether anything arrived.
    fn pdf_empty_relation(&self, db: &Path, spec: &PdfSourceSpec) -> Result<(), EngineError> {
        self.run(
            Some(db),
            &format!(
                "CREATE OR REPLACE TABLE {} (document_id VARCHAR, document_uri VARCHAR, source_sha256 VARCHAR, page_number BIGINT, text VARCHAR, has_text_layer BOOLEAN, width DOUBLE, height DOUBLE, metadata JSON)",
                plan::quote_ident(&spec.node_id)
            ),
            false,
        )
        .map(|_| ())
    }

    /// src.html: rows out of an HTML page, by CSS selector (#255).
    ///
    /// HTML is not XML. Real pages carry unclosed tags and unquoted attributes
    /// that the strict XML reader rejects outright, so this parses with a
    /// tolerant HTML parser and selects with CSS rather than an element path.
    ///
    /// Selectors are compiled ONCE up front, so a typo fails the stage
    /// immediately with the offending selector named, rather than silently
    /// producing empty columns for every row. `Document::select` panics on a bad
    /// selector and `try_select` cannot tell a bad selector from a page that
    /// simply had no matches, so neither is used here.
    pub(crate) fn run_html_source(
        &self,
        db: &Path,
        spec: &HtmlSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let lower = spec.path.to_ascii_lowercase();
        let html = if lower.starts_with("http://") || lower.starts_with("https://") {
            let agent = match &spec.transport {
                Some(t) => crate::tls::http_agent_with(t),
                None => crate::tls::http_agent(),
            };
            let mut req = agent.get(&spec.path);
            for (k, v) in &spec.headers {
                req = req.set(k, v);
            }
            match req.call() {
                Ok(r) => r
                    .into_string()
                    .map_err(|e| EngineError::Query(format!("html: read {}: {}", spec.path, e)))?,
                Err(ureq::Error::Status(code, r)) => {
                    let body = r.into_string().unwrap_or_default();
                    return Err(EngineError::Query(format!(
                        "html: HTTP {} from {}: {}",
                        code,
                        spec.path,
                        body.chars().take(300).collect::<String>()
                    )));
                }
                Err(e) => {
                    return Err(EngineError::Query(format!(
                        "html: HTTP transport to {}: {}",
                        spec.path, e
                    )))
                }
            }
        } else {
            // Lossy rather than strict: plenty of real pages are still served
            // as latin-1, and a replacement character in one cell beats
            // refusing to read the document at all.
            let bytes = std::fs::read(&spec.path)
                .map_err(|e| EngineError::Query(format!("html: read {}: {}", spec.path, e)))?;
            String::from_utf8_lossy(&bytes).into_owned()
        };

        let compile = |sel: &str| -> Result<dom_query::Matcher, EngineError> {
            dom_query::Matcher::new(sel).map_err(|_| {
                EngineError::Config(format!("html: {} is not a valid CSS selector", sel))
            })
        };
        let row_matcher = compile(&spec.row_selector)?;
        let mut col_matchers: Vec<Option<dom_query::Matcher>> = Vec::with_capacity(spec.columns.len());
        for c in &spec.columns {
            col_matchers.push(if c.selector.is_empty() {
                None
            } else {
                Some(compile(&c.selector)?)
            });
        }

        let doc = dom_query::Document::from(html);
        let mut writer = match &spec.declared_schema {
            Some(schema) if !schema.is_empty() => {
                JsonLinesWriter::open_with_schema(&spec.node_id, Some(schema.clone()))?
            }
            _ => JsonLinesWriter::open(&spec.node_id)?,
        };
        let mut count: usize = 0;
        let clean = |t: String| t.split_whitespace().collect::<Vec<_>>().join(" ");

        if spec.columns.is_empty() {
            // Table mode: the row selector names a table, its header cells name
            // the columns, and each body row is a row. This is the shape most
            // "the data is only published as an HTML table" pages have, and
            // making the user write a selector per column for it would be busy
            // work.
            let th = compile("th")?;
            let td = compile("td")?;
            let tr = compile("tr")?;
            for table in doc.select_matcher(&row_matcher).iter() {
                let headers: Vec<String> = table
                    .select_matcher(&th)
                    .iter()
                    .map(|h| clean(h.text().to_string()))
                    .collect();
                for (ri, row) in table.select_matcher(&tr).iter().enumerate() {
                    self.check_cancelled()?;
                    let cells: Vec<String> = row
                        .select_matcher(&td)
                        .iter()
                        .map(|c| clean(c.text().to_string()))
                        .collect();
                    // The header row itself has no td cells; skip it rather than
                    // emitting a row of nulls.
                    if cells.is_empty() {
                        continue;
                    }
                    let mut obj = serde_json::Map::new();
                    for (i, cell) in cells.iter().enumerate() {
                        let name = headers
                            .get(i)
                            .filter(|h| !h.is_empty())
                            .cloned()
                            .unwrap_or_else(|| format!("column_{}", i + 1));
                        obj.insert(name, JsonValue::String(cell.clone()));
                    }
                    let _ = ri;
                    writer.write_row(&JsonValue::Object(obj))?;
                    count += 1;
                }
            }
        } else {
            for row in doc.select_matcher(&row_matcher).iter() {
                self.check_cancelled()?;
                let mut obj = serde_json::Map::new();
                for (col, matcher) in spec.columns.iter().zip(col_matchers.iter()) {
                    // An empty selector means the row element itself, which is
                    // how you read an attribute off the matched element.
                    let value = match matcher {
                        None => match &col.attr {
                            Some(a) => row.attr(a).map(|v| v.to_string()),
                            None => Some(row.text().to_string()),
                        },
                        Some(m) => {
                            let found = row.select_matcher(m);
                            if found.is_empty() {
                                None
                            } else {
                                match &col.attr {
                                    Some(a) => found.attr(a).map(|v| v.to_string()),
                                    None => Some(found.text().to_string()),
                                }
                            }
                        }
                    };
                    // A column that did not match is NULL, not an empty string:
                    // a missing price and a blank price are different facts.
                    obj.insert(
                        col.name.clone(),
                        match value {
                            Some(v) => JsonValue::String(clean(v)),
                            None => JsonValue::Null,
                        },
                    );
                }
                writer.write_row(&JsonValue::Object(obj))?;
                count += 1;
            }
        }

        match &spec.declared_schema {
            Some(schema) if !schema.is_empty() => {
                let (columns_spec, select_list) = xml_declared_columns(schema);
                writer.finalize_typed(&self.bin, db, &spec.node_id, &columns_spec, &select_list)?;
            }
            // A page that matched nothing is an ordinary outcome for a scrape,
            // not a failure. With explicit columns the shape IS known even with
            // no rows, so type the empty relation from them rather than failing
            // the way an untypeable empty result has to (#170). Table mode has
            // no such luxury: without a header row there is nothing to name.
            _ if count == 0 && !spec.columns.is_empty() => {
                let cols: Vec<duckle_metadata::Column> = spec
                    .columns
                    .iter()
                    .map(|c| duckle_metadata::Column {
                        name: c.name.clone(),
                        data_type: duckle_metadata::DataType::String,
                        nullable: true,
                        format: None,
                        primary_key: None,
                    })
                    .collect();
                let (columns_spec, select_list) = xml_declared_columns(&cols);
                writer.finalize_typed(&self.bin, db, &spec.node_id, &columns_spec, &select_list)?;
            }
            _ => writer.finalize_into_table(&self.bin, db, &spec.node_id)?,
        }
        Ok(format!(
            "html: materialized {} rows into {}",
            count, spec.node_id
        ))
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
        use std::io::{BufReader, Read, Seek};
        // #282: the documents are whatever an upstream relation names, or
        // the configured path when nothing is wired in.
        let from_upstream = spec.input.from_view.is_some();
        let docs: Vec<(String, Option<String>, JsonValue)> = if from_upstream {
            self.resolve_artifact_inputs(db, "", &spec.input)?
                .into_iter()
                .map(|a| (a.uri, a.sha256, a.row))
                .collect()
        } else {
            vec![(spec.path.clone(), None, JsonValue::Null)]
        };
        // Object storage on the CONFIGURED PATH still has no signed streaming
        // GET here (DuckDB's httpfs cannot parse XML), so it fails early with a
        // pointer rather than opening a temp file we would leak. Reached through
        // an upstream artifact relation it works, because that route goes
        // through open_artifact, which does sign S3 reads (#282).
        let lower = spec.path.to_ascii_lowercase();
        if let Some(scheme) = ["s3://", "gs://", "gcs://", "az://", "azure://"]
            .iter()
            .find(|s| !from_upstream && lower.starts_with(**s))
        {
            return Err(EngineError::Config(format!(
                "xml: {} object storage is not supported for src.xml yet; use an https:// or sftp:// URL, or download the file to a local path",
                scheme.trim_end_matches("://")
            )));
        }

        // A declared schema pins the output to exactly those columns and types.
        //
        // #283: it also turns on bounded materialization. Without it every
        // parsed row goes to one NDJSON file that grows to the size of the whole
        // result, and NDJSON repeats every property name on every row - so a
        // 30 GB compressed source can put hundreds of gigabytes on the temp
        // volume. With it, the text is rolled to a compressed Parquet part every
        // `spec.batch_rows` rows and the NDJSON only ever holds the tail.
        let mut writer = match &spec.declared_schema {
            Some(schema) if !schema.is_empty() => {
                let (columns_spec, _) = xml_declared_columns(schema);
                JsonLinesWriter::open_with_schema(&spec.node_id, Some(schema.clone()))?
                    .spilling_every(&self.bin, db, &columns_spec, spec.batch_rows)?
            }
            _ => JsonLinesWriter::open(&spec.node_id)?,
        };
        let mut count: usize = 0;
        let mut skipped: usize = 0;
        if from_upstream {
            // #282: a CORPUS rather than one document.
            //
            // Streamed straight out of the artifact reader. The pull parser
            // never seeks, so spooling each document to disk first would buy
            // nothing and cost a full local copy of every one of them.
            //
            // The writer is shared across all of them on purpose: the
            // bounded-parts machinery from #283 then bounds the WHOLE corpus
            // rather than each file, so a million small documents cannot do
            // what one huge document already could not.
            for (uri, source_sha, upstream_row) in &docs {
                self.check_cancelled()?;
                let mut emit = |row: &JsonValue| -> Result<(), EngineError> {
                    let mut obj = match row {
                        JsonValue::Object(o) => o.clone(),
                        other => {
                            let mut m = serde_json::Map::new();
                            m.insert("value".into(), other.clone());
                            m
                        }
                    };
                    // The business keys that say what a document IS live on
                    // the artifact row and are lost the moment rows are
                    // emitted instead. Carrying them is what lets a row be
                    // joined back to the document it came from.
                    for key in &spec.input.carry {
                        obj.insert(
                            key.clone(),
                            upstream_row.get(key).cloned().unwrap_or(JsonValue::Null),
                        );
                    }
                    obj.insert("source_uri".into(), JsonValue::String(uri.clone()));
                    obj.insert(
                        "source_sha256".into(),
                        match source_sha {
                            // Carried from whatever landed the bytes, never
                            // recomputed: re-hashing would describe whatever
                            // is at that URI now, not what was parsed.
                            Some(h) => JsonValue::String(h.clone()),
                            None => JsonValue::Null,
                        },
                    );
                    writer.write_row(&JsonValue::Object(obj))?;
                    count += 1;
                    Ok(())
                };
                if uri.to_ascii_lowercase().ends_with(".zip") {
                    return Err(EngineError::Config(format!(
                        concat!(
                            "xml: {} is a zip, and a zip directory is at the END of ",
                            "the file, so it cannot be streamed. Put xf.archive.extract ",
                            "in front of this node to unpack it into artifacts, and ",
                            "parse those."
                        ),
                        uri
                    )));
                }
                let opened = match self.open_artifact(&spec.input.auth, uri) {
                    Ok(r) => Some(r),
                    Err(e) if spec.on_error == "skip" => {
                        eprintln!("duckle: xml: skipping {uri}: {e}");
                        skipped += 1;
                        None
                    }
                    Err(e) => return Err(e),
                };
                if let Some(reader) = opened {
                    match stream_remote_xml(reader, &spec.row_path, &self.cancel, &mut emit) {
                        Ok(()) => {}
                        Err(e) if spec.on_error == "skip" => {
                            eprintln!("duckle: xml: skipping {uri}: {e}");
                            skipped += 1;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        } else {
            let mut emit = |row: &JsonValue| -> Result<(), EngineError> {
                writer.write_row(row)?;
                count += 1;
                Ok(())
            };
            // Everything below streams: rows are emitted straight to an NDJSON
            // temp file as each element closes and DuckDB reads that back
            // out-of-core, so a multi-GB (and, uncompressed, far larger) document
            // never lands in RAM the way std::fs::read_to_string + a Vec of every
            // row did (issue #186). gzip (.gz) is decompressed on the fly for all
            // inputs; zip needs random access (its directory is at EOF) so it is
            // local-file only.
            if lower.starts_with("http://") || lower.starts_with("https://") {
                // Streaming GET via the shared proxy- and CA-aware agent; ureq's
                // gzip feature transparently inflates Content-Encoding: gzip, and
                // stream_remote_xml handles a gzipped file body on top.
                let resp = crate::tls::http_agent()
                    .get(&spec.path)
                    .call()
                    .map_err(|e| EngineError::Query(format!("xml: GET {}: {}", spec.path, e)))?;
                stream_remote_xml(resp.into_reader(), &spec.row_path, &self.cancel, &mut emit)?;
            } else if lower.starts_with("sftp://") {
                let (host, port, uri_user, remote) = parse_sftp_uri(&spec.path)?;
                let user = uri_user.ok_or_else(|| {
                    EngineError::Config(
                        "xml: an sftp URL needs a user, e.g. sftp://user@host/path/file.xml.gz"
                            .into(),
                    )
                })?;
                let reader = SftpFileReader::open(
                    &host,
                    port,
                    &user,
                    spec.sftp_password.as_deref(),
                    spec.sftp_private_key.as_deref(),
                    spec.sftp_key_passphrase.as_deref(),
                    spec.sftp_host_fingerprint.as_deref(),
                    &remote,
                )?;
                stream_remote_xml(reader, &spec.row_path, &self.cancel, &mut emit)?;
            } else {
                // Local file: a full seek is available, so also take the zip path.
                let mut file = std::fs::File::open(&spec.path)
                    .map_err(|e| EngineError::Query(format!("xml: read {}: {}", spec.path, e)))?;
                let mut magic = [0u8; 4];
                let n = file
                    .read(&mut magic)
                    .map_err(|e| EngineError::Query(format!("xml: read {}: {}", spec.path, e)))?;
                file.rewind()
                    .map_err(|e| EngineError::Query(format!("xml: seek {}: {}", spec.path, e)))?;
                let is_gzip = n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b;
                let is_zip = n >= 4 && &magic[0..4] == b"PK\x03\x04";
                if is_zip {
                    // Take the first *.xml entry, else the first entry; it then
                    // decompresses as a stream.
                    let mut archive = zip::ZipArchive::new(file).map_err(|e| {
                        EngineError::Query(format!("xml: open zip {}: {}", spec.path, e))
                    })?;
                    if archive.is_empty() {
                        return Err(EngineError::Query(format!("xml: zip {} is empty", spec.path)));
                    }
                    let name = archive
                        .file_names()
                        .find(|n| n.to_ascii_lowercase().ends_with(".xml"))
                        .map(|s| s.to_string());
                    let entry = match name {
                        Some(n) => archive.by_name(&n),
                        None => archive.by_index(0),
                    }
                    .map_err(|e| {
                        EngineError::Query(format!("xml: read zip entry {}: {}", spec.path, e))
                    })?;
                    stream_xml_rows(BufReader::new(entry), &spec.row_path, &self.cancel, &mut emit)?;
                } else if is_gzip {
                    let decoder = flate2::read::MultiGzDecoder::new(BufReader::new(file));
                    stream_xml_rows(BufReader::new(decoder), &spec.row_path, &self.cancel, &mut emit)?;
                } else {
                    stream_xml_rows(BufReader::new(file), &spec.row_path, &self.cancel, &mut emit)?;
                }
            }
        }
        let parts = match &spec.declared_schema {
            Some(schema) if !schema.is_empty() => {
                let (columns_spec, select_list) = xml_declared_columns(schema);
                writer.finalize_typed(&self.bin, db, &spec.node_id, &columns_spec, &select_list)?
            }
            _ => {
                writer.finalize_into_table(&self.bin, db, &spec.node_id)?;
                0
            }
        };
        Ok(format!(
            "xml: materialized {} rows into {}{}{}",
            count,
            spec.node_id,
            if skipped > 0 {
                // Named, not silent: a corpus that quietly lost documents is
                // the failure this whole contract exists to make visible.
                format!(" ({} document(s) skipped)", skipped)
            } else {
                String::new()
            },
            // #283: how many bounded parts it took. The number is the whole
            // point - it says the intermediate never held the full result.
            if parts > 0 {
                format!(" ({} bounded part(s))", parts)
            } else {
                String::new()
            }
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
        let create_result = self.run(Some(db), &create, false);
        // Remove the temp Parquet whether or not the load succeeded - a failed
        // CREATE (e.g. the working DB busy/locked) would otherwise leak it.
        let _ = std::fs::remove_file(&parquet_path);
        create_result?;
        Ok(format!(
            "gizmosql: materialized {} records into {}",
            count, spec.node_id
        ))
    }

    /// src.lancedb: run the duckle-lance sidecar to dump the Lance table to a
    /// Parquet temp file, then materialize it via read_parquet. The sidecar owns
    /// lancedb (arrow 58 / DataFusion); only Parquet bytes cross the boundary.
    pub(crate) fn run_lance_source(
        &self,
        db: &Path,
        spec: &LanceSourceSpec,
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
        let parquet_path = db.with_file_name(format!("{}.lance-{}.parquet", db_name, safe_node));
        let mut cmd = std::process::Command::new(resolve_lance_bin());
        cmd.arg("read")
            .arg("--uri")
            .arg(&spec.uri)
            .arg("--table")
            .arg(&spec.table)
            .arg("--out")
            .arg(&parquet_path);
        if let Some(k) = &spec.api_key {
            cmd.arg("--api-key").arg(k);
        }
        if let Some(r) = &spec.region {
            cmd.arg("--region").arg(r);
        }
        if let Some(l) = spec.limit {
            cmd.arg("--limit").arg(l.to_string());
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let out = cmd.output().map_err(|e| {
            EngineError::Query(format!(
                "lancedb: cannot run duckle-lance: {} (set DUCKLE_LANCE_BIN or bundle the sidecar)",
                e
            ))
        })?;
        if !out.status.success() {
            let _ = std::fs::remove_file(&parquet_path);
            return Err(EngineError::Query(format!(
                "lancedb read: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let create = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}')",
            plan::quote_ident(&spec.node_id),
            ppath
        );
        let create_result = self.run(Some(db), &create, false);
        // Remove the temp Parquet whether or not the load succeeded - a failed
        // CREATE (e.g. the working DB busy/locked) would otherwise leak it.
        let _ = std::fs::remove_file(&parquet_path);
        create_result?;
        Ok(format!("lancedb: materialized {} into {}", spec.table, spec.node_id))
    }

    /// snk.lancedb: COPY the upstream view to a Parquet temp file, then run the
    /// sidecar to create/append the Lance table from it.
    pub(crate) fn run_lance_sink(
        &self,
        db: &Path,
        spec: &LanceSinkSpec,
    ) -> Result<String, EngineError> {
        let safe: String = spec
            .from_view
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let db_name = db
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parquet_path = db.with_file_name(format!("{}.lance-snk-{}.parquet", db_name, safe));
        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let copy = format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT parquet)",
            plan::quote_ident(&spec.from_view),
            ppath
        );
        self.run(Some(db), &copy, false)?;
        let mut cmd = std::process::Command::new(resolve_lance_bin());
        cmd.arg("write")
            .arg("--uri")
            .arg(&spec.uri)
            .arg("--table")
            .arg(&spec.table)
            .arg("--in")
            .arg(&parquet_path)
            .arg("--mode")
            .arg(&spec.mode);
        if let Some(k) = &spec.api_key {
            cmd.arg("--api-key").arg(k);
        }
        if let Some(r) = &spec.region {
            cmd.arg("--region").arg(r);
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let out = cmd.output().map_err(|e| {
            EngineError::Query(format!(
                "lancedb: cannot run duckle-lance: {} (set DUCKLE_LANCE_BIN or bundle the sidecar)",
                e
            ))
        })?;
        let _ = std::fs::remove_file(&parquet_path);
        if !out.status.success() {
            return Err(EngineError::Query(format!(
                "lancedb write: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(format!("lancedb: wrote {} ({})", spec.table, spec.mode))
    }

    /// src.vortex: run the sidecar to read a Vortex file into a Parquet temp file,
    /// then materialize it. Reuses the duckle-lance binary (shared columnar-format
    /// sidecar) via its read-vortex subcommand.
    pub(crate) fn run_vortex_source(
        &self,
        db: &Path,
        spec: &VortexSourceSpec,
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
        let parquet_path = db.with_file_name(format!("{}.vortex-{}.parquet", db_name, safe_node));
        let mut cmd = std::process::Command::new(resolve_lance_bin());
        cmd.arg("read-vortex")
            .arg("--path")
            .arg(&spec.path)
            .arg("--out")
            .arg(&parquet_path);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let out = cmd.output().map_err(|e| {
            EngineError::Query(format!(
                "vortex: cannot run duckle-lance: {} (set DUCKLE_LANCE_BIN or bundle the sidecar)",
                e
            ))
        })?;
        if !out.status.success() {
            let _ = std::fs::remove_file(&parquet_path);
            return Err(EngineError::Query(format!(
                "vortex read: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let create = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}')",
            plan::quote_ident(&spec.node_id),
            ppath
        );
        let create_result = self.run(Some(db), &create, false);
        // Remove the temp Parquet whether or not the load succeeded - a failed
        // CREATE (e.g. the working DB busy/locked) would otherwise leak it.
        let _ = std::fs::remove_file(&parquet_path);
        create_result?;
        Ok(format!("vortex: materialized {} into {}", spec.path, spec.node_id))
    }

    /// snk.vortex: COPY the upstream view to a Parquet temp file, then run the
    /// sidecar to write it out as a Vortex file.
    pub(crate) fn run_vortex_sink(
        &self,
        db: &Path,
        spec: &VortexSinkSpec,
    ) -> Result<String, EngineError> {
        let safe: String = spec
            .from_view
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let db_name = db
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parquet_path = db.with_file_name(format!("{}.vortex-snk-{}.parquet", db_name, safe));
        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let copy = format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT parquet)",
            plan::quote_ident(&spec.from_view),
            ppath
        );
        self.run(Some(db), &copy, false)?;
        let mut cmd = std::process::Command::new(resolve_lance_bin());
        cmd.arg("write-vortex")
            .arg("--in")
            .arg(&parquet_path)
            .arg("--path")
            .arg(&spec.path);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let out = cmd.output().map_err(|e| {
            EngineError::Query(format!(
                "vortex: cannot run duckle-lance: {} (set DUCKLE_LANCE_BIN or bundle the sidecar)",
                e
            ))
        })?;
        let _ = std::fs::remove_file(&parquet_path);
        if !out.status.success() {
            return Err(EngineError::Query(format!(
                "vortex write: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(format!("vortex: wrote {}", spec.path))
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
        // apache-avro 0.22 returns a Result here: building a writer can fail on a
        // schema it cannot encode against, which used to surface later as a
        // confusing append error instead of at the point of the mistake.
        let mut writer = apache_avro::Writer::new(&schema, file)
            .map_err(|e| EngineError::Query(format!("avro: open writer: {}", e)))?;
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
                            // lapin 4 takes the AMQP ShortString type rather
                            // than a borrowed str for these fields.
                            spec.exchange.as_str().into(),
                            spec.routing_key.as_str().into(),
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
                    .basic_get(spec.queue.as_str().into(), BasicGetOptions::default())
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
        // Collect stdout/stderr, but DON'T block forever: a grandchild that
        // inherited the pipe write ends can keep them open after the shell
        // exits, so read_to_end never sees EOF. Bound the wait by the same
        // deadline (and honor cancellation) and give up on late output rather
        // than hanging the whole run past the configured timeout.
        let join_bounded = |handle: std::thread::JoinHandle<Vec<u8>>| -> Vec<u8> {
            loop {
                if handle.is_finished() {
                    return handle.join().unwrap_or_default();
                }
                if self.cancel.load(Ordering::Relaxed) {
                    return Vec::new();
                }
                if let Some(d) = deadline {
                    if std::time::Instant::now() >= d {
                        return Vec::new();
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        };
        let stdout_bytes = join_bounded(stdout_reader);
        let stderr_bytes = join_bounded(stderr_reader);
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
        // Scaffold/resolve the project, write profiles.yml, and assemble the
        // project/profiles/vars flags shared with the #146 pre-warm parse.
        let inv = prepare_dbt_invocation(spec, db)?;
        // dbt <user command tokens (default "run")> then the shared flags. The
        // command is split on whitespace (documented; no shell quoting), which
        // avoids cmd.exe/sh quoting pitfalls entirely.
        let mut args: Vec<String> =
            spec.command.split_whitespace().map(|s| s.to_string()).collect();
        if args.is_empty() {
            args.push("run".into());
        }
        args.extend(inv.shared_args.iter().cloned());

        let (status, stdout_text, stderr_text) = spawn_dbt_and_wait(
            &inv.dbt_bin,
            &args,
            &inv.project_dir,
            &self.cancel,
            spec.timeout_ms,
        )?;
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
        let results_path = inv.project_dir.join("target").join("run_results.json");
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
                    let attach_path = inv.target_db.replace('\'', "''");
                    format!(
                        "ATTACH '{}' AS __dbt_out (READ_ONLY); \
                         CREATE OR REPLACE TABLE {} AS SELECT * FROM __dbt_out.{}.{};",
                        attach_path,
                        plan::quote_ident(&spec.node_id),
                        plan::quote_ident(&spec.schema),
                        plan::quote_ident(model)
                    )
                } else {
                    // dbt builds the model into schema `spec.schema` in the run
                    // db, so qualify the read-back. An unqualified name only
                    // resolves against the default search path, so a non-default
                    // schema (e.g. "analytics") would fail "model not found"
                    // even though dbt succeeded.
                    format!(
                        "CREATE OR REPLACE TABLE {} AS SELECT * FROM {}.{};",
                        plan::quote_ident(&spec.node_id),
                        plan::quote_ident(&spec.schema),
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
                // A listing entry that can't be retrieved (a subdirectory - NLST
                // returns directory names with no type info - or a transiently
                // locked/denied file) must not abort the whole harvest; skip it,
                // mirroring the tolerant .ok() handling of size/mdtm above.
                Err(_) => continue,
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
            hostport: String,
            /// Why the key was refused. russh turns a `false` into a bare
            /// "unknown key", which tells the user nothing about what changed
            /// or what to do, so the reason is carried out this way.
            refused: std::sync::Arc<std::sync::Mutex<Option<String>>>,
        }
        impl russh::client::Handler for Verifier {
            type Error = russh::Error;
            async fn check_server_key(
                &mut self,
                server_public_key: &russh::keys::PublicKeyOrCertificate,
            ) -> Result<bool, Self::Error> {
                match verify_sftp_host_key(
                    server_public_key,
                    self.expected.as_deref(),
                    &self.hostport,
                ) {
                    Ok(()) => Ok(true),
                    Err(why) => {
                        *self.refused.lock().unwrap() = Some(why);
                        Ok(false)
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
            let refused = std::sync::Arc::new(std::sync::Mutex::new(None));
            let handler = Verifier {
                expected: spec.host_fingerprint.clone(),
                hostport: format!("{}:{}", spec.host, spec.port),
                refused: refused.clone(),
            };
            let mut session =
                russh::client::connect(config, (spec.host.as_str(), spec.port), handler)
                    .await
                    .map_err(|e| match refused.lock().unwrap().take() {
                        Some(why) => why,
                        None => format!("connect {}:{}: {}", spec.host, spec.port, e),
                    })?;

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
            // Stream the temp export straight from disk instead of slurping the
            // whole (potentially multi-GB) file into a Vec<u8> first.
            let total = std::fs::metadata(&temp)
                .map_err(|e| EngineError::Query(format!("ftp: stat temp {}: {}", temp.display(), e)))?
                .len();
            let mut ftp = FtpStream::connect(&addr)
                .map_err(|e| EngineError::Query(format!("ftp connect {}: {}", addr, e)))?;
            if spec.secure {
                return Err(EngineError::Config(
                    "snk.ftp: secure=true (FTPS) requires the rustls TLS wrapper which isn't wired up yet. Use plain FTP or wait for the FTPS-explicit feature.".into(),
                ));
            }
            ftp.login(&spec.user, &spec.password)
                .map_err(|e| EngineError::Query(format!("ftp login: {}", e)))?;
            let mut reader = std::io::BufReader::new(
                std::fs::File::open(&temp).map_err(|e| {
                    EngineError::Query(format!("ftp: open temp {}: {}", temp.display(), e))
                })?,
            );
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
            hostport: String,
            /// Why the key was refused. russh turns a `false` into a bare
            /// "unknown key", which tells the user nothing about what changed
            /// or what to do, so the reason is carried out this way.
            refused: std::sync::Arc<std::sync::Mutex<Option<String>>>,
        }
        impl russh::client::Handler for Verifier {
            type Error = russh::Error;
            async fn check_server_key(
                &mut self,
                server_public_key: &russh::keys::PublicKeyOrCertificate,
            ) -> Result<bool, Self::Error> {
                match verify_sftp_host_key(
                    server_public_key,
                    self.expected.as_deref(),
                    &self.hostport,
                ) {
                    Ok(()) => Ok(true),
                    Err(why) => {
                        *self.refused.lock().unwrap() = Some(why);
                        Ok(false)
                    }
                }
            }
        }

        let temp = self.ftp_copy_view_to_temp(db, &spec.from_view, &spec.format)?;
        let result: Result<u64, EngineError> = (|| {
            // Stream the temp export from disk rather than loading the whole
            // (potentially multi-GB) file into a Vec<u8>.
            let total = std::fs::metadata(&temp)
                .map_err(|e| {
                    EngineError::Query(format!("sftp: stat temp {}: {}", temp.display(), e))
                })?
                .len();

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| EngineError::Query(format!("sftp: tokio rt: {}", e)))?;

            let uploaded: Result<(), String> = rt.block_on(async {
                use russh_sftp::client::SftpSession;
                use tokio::io::AsyncWriteExt;

                let config = std::sync::Arc::new(russh::client::Config::default());
                let refused = std::sync::Arc::new(std::sync::Mutex::new(None));
                let handler = Verifier {
                    expected: spec.host_fingerprint.clone(),
                    hostport: format!("{}:{}", spec.host, spec.port),
                    refused: refused.clone(),
                };
                let mut session =
                    russh::client::connect(config, (spec.host.as_str(), spec.port), handler)
                        .await
                        .map_err(|e| match refused.lock().unwrap().take() {
                            Some(why) => why,
                            None => format!("connect {}:{}: {}", spec.host, spec.port, e),
                        })?;

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
                let mut local = tokio::fs::File::open(&temp)
                    .await
                    .map_err(|e| format!("open temp {}: {}", temp.display(), e))?;
                tokio::io::copy(&mut local, &mut remote)
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

    /// #142: build an OpenAI-compatible request URL. A custom `endpoint_path`
    /// is joined onto base_url (no double slashes); empty falls back to the
    /// component default (e.g. "/v1/chat/completions"), keeping existing
    /// pipelines byte-identical.
    fn ai_endpoint(base_url: &str, endpoint_path: &Option<String>, default_path: &str) -> String {
        let base = base_url.trim_end_matches('/');
        match endpoint_path {
            Some(p) if !p.trim().is_empty() => {
                format!("{}/{}", base, p.trim().trim_start_matches('/'))
            }
            _ => format!("{}{}", base, default_path),
        }
    }

    /// #142: apply the user's custom headers, then default `Authorization: Bearer`
    /// and JSON `Content-Type` only when the custom headers did not already set
    /// them (case-insensitive), so a custom gateway can override auth while
    /// existing pipelines (no custom headers) behave exactly as before.
    fn ai_post(endpoint: &str, headers: &[(String, String)], api_key: &str) -> ureq::Request {
        let mut req = crate::tls::http_agent().post(endpoint);
        let has = |name: &str| headers.iter().any(|(k, _)| k.eq_ignore_ascii_case(name));
        let (has_auth, has_ct) = (has("authorization"), has("content-type"));
        for (k, v) in headers {
            req = req.set(k, v);
        }
        if !has_auth {
            req = req.set("Authorization", &format!("Bearer {}", api_key));
        }
        if !has_ct {
            req = req.set("Content-Type", "application/json");
        }
        req
    }

    /// #258: how long to wait before retry `attempt` (0-based).
    ///
    /// A `Retry-After` given in whole seconds is obeyed exactly - that is the
    /// provider saying when it will serve again, and guessing shorter just
    /// earns another 429. Without one the wait doubles from 500ms, capped so a
    /// stalled provider cannot park a stage for an unbounded time.
    pub(crate) fn ai_retry_wait_ms(retry_after: Option<&str>, attempt: u32) -> u64 {
        if let Some(secs) = retry_after.and_then(|v| v.trim().parse::<u64>().ok()) {
            return (secs * 1000).min(300_000);
        }
        (500u64 << attempt.min(6)).min(30_000)
    }

    /// #258: sleep, but notice a cancelled run instead of sitting out a rate
    /// limit for the full interval.
    fn ai_sleep_cancellable(&self, ms: u64) -> Result<(), EngineError> {
        let mut left = ms;
        while left > 0 {
            let slice = left.min(200);
            std::thread::sleep(std::time::Duration::from_millis(slice));
            left -= slice;
            self.check_cancelled()?;
        }
        Ok(())
    }

    /// #258: send one AI request, retrying on HTTP 429 and 5xx.
    ///
    /// `make` rebuilds the request on each attempt because ureq consumes a
    /// Request when it is sent. Before this, the first rate limit returned Err
    /// and the stage threw away every row it had already paid for; the only
    /// retry in the engine is per stage, which re-sends the whole dataset from
    /// row 0. Transport errors are deliberately not retried, so a wrong host
    /// still fails as fast as it always did.
    fn ai_send_with_retry(
        &self,
        make: &dyn Fn() -> ureq::Request,
        body: &str,
        what: &str,
        max_retries: u32,
    ) -> Result<JsonValue, EngineError> {
        let mut attempt = 0u32;
        loop {
            self.check_cancelled()?;
            match make().send_string(body) {
                Ok(r) => {
                    return r
                        .into_json()
                        .map_err(|e| EngineError::Query(format!("{} parse: {}", what, e)))
                }
                Err(ureq::Error::Status(code, r)) => {
                    let retryable = code == 429 || (500..600).contains(&code);
                    if !retryable || attempt >= max_retries {
                        let b = r.into_string().unwrap_or_default();
                        return Err(EngineError::Query(format!(
                            "{} HTTP {}: {}",
                            what, code, b
                        )));
                    }
                    let wait = Self::ai_retry_wait_ms(r.header("Retry-After"), attempt);
                    self.ai_sleep_cancellable(wait)?;
                }
                Err(e) => {
                    return Err(EngineError::Query(format!("{} transport: {}", what, e)))
                }
            }
            attempt += 1;
        }
    }

    /// #258: map `n` items through `f` with at most `concurrency` requests in
    /// flight, writing every result back BY INDEX.
    ///
    /// Order is the thing this must not break. The sequential loops this
    /// replaces got row order for free; a dispatcher that pushed results as
    /// they completed would pair every row with another row's answer, and
    /// nothing downstream would report it. `concurrency` of 1 runs inline and
    /// is byte for byte the loop it replaces.
    fn ai_map_concurrent<T, F>(
        &self,
        n: usize,
        concurrency: usize,
        f: F,
    ) -> Result<Vec<T>, EngineError>
    where
        T: Send,
        F: Fn(&Self, usize) -> Result<T, EngineError> + Sync,
    {
        if concurrency <= 1 || n <= 1 {
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                self.check_cancelled()?;
                out.push(f(self, i)?);
            }
            return Ok(out);
        }
        use std::sync::atomic::{AtomicUsize, Ordering};
        let workers = concurrency.min(n);
        let next = AtomicUsize::new(0);
        let slots: Vec<std::sync::Mutex<Option<T>>> =
            (0..n).map(|_| std::sync::Mutex::new(None)).collect();
        let failure: std::sync::Mutex<Option<(usize, EngineError)>> =
            std::sync::Mutex::new(None);
        // One engine clone per worker, matching run_parallel_branches: it keeps
        // each worker's cancellation check its own.
        let engines: Vec<Self> = (0..workers).map(|_| self.clone()).collect();
        // Take references up front: each worker closure is `move`, and without
        // these it would move the shared state into the first worker.
        let (f, next, slots, failure) = (&f, &next, &slots, &failure);
        std::thread::scope(|scope| {
            for engine in &engines {
                scope.spawn(move || loop {
                    // Stop pulling work the moment any worker has failed, so a
                    // rate-limited 500k-row job stops paying for requests.
                    if failure.lock().unwrap().is_some() {
                        return;
                    }
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    if i >= n {
                        return;
                    }
                    match engine.check_cancelled().and_then(|_| f(engine, i)) {
                        Ok(v) => *slots[i].lock().unwrap() = Some(v),
                        Err(e) => {
                            let mut slot = failure.lock().unwrap();
                            // Report the lowest-index failure, so the message
                            // does not depend on which worker lost the race.
                            if slot.as_ref().map_or(true, |(j, _)| i < *j) {
                                *slot = Some((i, e));
                            }
                            return;
                        }
                    }
                });
            }
        });
        if let Some((_, e)) = failure.lock().unwrap().take() {
            return Err(e);
        }
        let mut out = Vec::with_capacity(n);
        for (i, slot) in slots.iter().enumerate() {
            match slot.lock().unwrap().take() {
                Some(v) => out.push(v),
                None => {
                    return Err(EngineError::Query(format!(
                        "ai: row {} produced no result",
                        i
                    )))
                }
            }
        }
        Ok(out)
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
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
            return Ok(format!(
                "ai.embed: 0 upstream rows -> {}",
                spec.node_id
            ));
        }
        let endpoint = Self::ai_endpoint(&spec.base_url, &spec.endpoint_path, "/v1/embeddings");
        // #258: one request per batch as before, but up to `concurrency`
        // batches in flight. Results come back per chunk and are flattened in
        // chunk order, so the output row order is exactly the input order.
        let chunks: Vec<&[JsonValue]> = rows.chunks(spec.batch_size).collect();
        let per_chunk = self.ai_map_concurrent(chunks.len(), spec.concurrency, |engine, ci| {
            let chunk = chunks[ci];
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
            let response = engine.ai_send_with_retry(
                &|| Self::ai_post(&endpoint, &spec.headers, &spec.api_key),
                &body.to_string(),
                "ai.embed",
                spec.max_retries,
            )?;
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
            let mut chunk_out = Vec::with_capacity(chunk.len());
            for (row, item) in chunk.iter().zip(data.iter()) {
                let embedding = item.get("embedding").cloned().unwrap_or(JsonValue::Null);
                let mut obj = match row {
                    JsonValue::Object(m) => m.clone(),
                    _ => serde_json::Map::new(),
                };
                obj.insert(spec.output_column.clone(), embedding);
                chunk_out.push(JsonValue::Object(obj));
            }
            Ok(chunk_out)
        })?;
        let out: Vec<JsonValue> = per_chunk.into_iter().flatten().collect();
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
        let mut last_got = 0usize;
        let mut shard_closed = false;
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
            polls += 1;
            last_got = got;
            // Advance the iterator. A null NextShardIterator means the
            // shard is closed (true end of data); follow it otherwise.
            match rec_resp.get("NextShardIterator").and_then(|v| v.as_str()) {
                Some(next) => shard_iter = next.to_string(),
                None => {
                    shard_closed = true;
                    break;
                }
            }
            // An empty poll does NOT mean end-of-shard: Kinesis returns
            // empty record pages while NextShardIterator keeps advancing
            // (a fresh iterator warming up, or a sparse region) with more
            // data still ahead. Don't break - sleep briefly to avoid a tight
            // loop and keep following the iterator until we hit the poll
            // budget or the shard actually closes.
            if got == 0 {
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
        }
        // Fail loud (like the DynamoDB source) if the 100-poll safety cap
        // cut us off while records were still actively flowing, instead of
        // silently reporting a truncated read as success.
        if polls >= 100 && !shard_closed && last_got > 0 && (out.len() as u64) < spec.max_records {
            return Err(EngineError::Query(format!(
                "kinesis: reached the 100-poll safety cap after {} record(s) from {}/shard[{}] \
                 with data still flowing; raise maxRecords or read the shard in smaller passes",
                out.len(),
                spec.stream_name,
                spec.shard_index
            )));
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
        // Notification mode: nothing upstream, so the message is the spec's own
        // (to, subject, body) rather than a row's columns.
        let rows = match &spec.fixed {
            Some((to, subject, body)) => vec![serde_json::json!({
                spec.to_column.clone(): to,
                spec.subject_column.clone(): subject,
                spec.body_column.clone(): body,
            })],
            None => self.run_rows(
                Some(db),
                &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
            )?,
        };
        if rows.is_empty() {
            return Ok("email sink: 0 upstream rows".to_string());
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

    /// src.websocket (#192): WebSocket client source. Connects to the URL,
    /// optionally sends one subscribe frame, reads up to `max_messages` frames
    /// (or until the `timeout_ms` idle/total deadline), parses each as JSON, and
    /// materializes the rows. Drives tokio-tungstenite on a current-thread
    /// runtime, the same shape as the SFTP reader.
    pub(crate) fn run_websocket_source(
        &self,
        db: &Path,
        spec: &WebSocketSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("websocket: tokio rt: {}", e)))?;
        let mut rows: Vec<JsonValue> = Vec::new();
        rt.block_on(async {
            use futures_util::{SinkExt, StreamExt};
            use tokio_tungstenite::tungstenite::Message;
            let request = websocket_request(&spec.url, &spec.headers)?;
            let (mut ws, _resp) = tokio_tungstenite::connect_async(request)
                .await
                .map_err(|e| format!("connect {}: {}", spec.url, e))?;
            if let Some(sub) = &spec.subscribe {
                ws.send(Message::Text(sub.clone().into()))
                    .await
                    .map_err(|e| format!("send subscribe: {}", e))?;
            }
            let deadline =
                std::time::Instant::now() + std::time::Duration::from_millis(spec.timeout_ms);
            while (rows.len() as u64) < spec.max_messages {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, ws.next()).await {
                    Ok(Some(Ok(msg))) => match msg {
                        Message::Text(t) => websocket_parse_into_rows(&t, &mut rows),
                        Message::Binary(b) => {
                            websocket_parse_into_rows(&String::from_utf8_lossy(&b), &mut rows)
                        }
                        Message::Close(_) => break,
                        // Ping/Pong/Frame: tungstenite answers pings automatically.
                        _ => {}
                    },
                    Ok(Some(Err(e))) => return Err(format!("recv: {}", e)),
                    Ok(None) => break, // server closed the stream
                    Err(_) => break,   // idle/total timeout reached
                }
            }
            let _ = ws.close(None).await;
            Ok::<(), String>(())
        })
        .map_err(EngineError::Query)?;
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        Ok(format!(
            "websocket: received {} message(s) from {} -> {}",
            count, spec.url, spec.node_id
        ))
    }

    /// snk.websocket (#192): WebSocket client sink. Reads the upstream view and
    /// sends each row as a text frame - the whole row as JSON, or one column's
    /// value when `message_column` is set - then closes.
    pub(crate) fn run_websocket_sink(
        &self,
        db: &Path,
        spec: &WebSocketSinkSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {}", plan::quote_ident(&spec.from_view)),
        )?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("websocket: tokio rt: {}", e)))?;
        let sent = rt
            .block_on(async {
                use futures_util::SinkExt;
                use tokio_tungstenite::tungstenite::Message;
                let request = websocket_request(&spec.url, &spec.headers)?;
                let (mut ws, _resp) = tokio_tungstenite::connect_async(request)
                    .await
                    .map_err(|e| format!("connect {}: {}", spec.url, e))?;
                let mut n = 0usize;
                for row in &rows {
                    let payload = match &spec.message_column {
                        Some(col) => match row.get(col) {
                            Some(JsonValue::String(s)) => s.clone(),
                            Some(v) => v.to_string(),
                            None => continue,
                        },
                        None => serde_json::to_string(row).unwrap_or_default(),
                    };
                    ws.send(Message::Text(payload.into()))
                        .await
                        .map_err(|e| format!("send: {}", e))?;
                    n += 1;
                }
                let _ = ws.close(None).await;
                Ok::<usize, String>(n)
            })
            .map_err(EngineError::Query)?;
        Ok(format!("websocket: sent {} message(s) to {}", sent, spec.url))
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
            // #170: type the empty result with src.email's fixed output columns
            // (see the per-message row below) so downstream SQL binds them
            // instead of a single `json` column.
            let schema = [
                ("uid", duckle_metadata::DataType::Int64),
                ("from", duckle_metadata::DataType::String),
                ("to", duckle_metadata::DataType::String),
                ("subject", duckle_metadata::DataType::String),
                ("date", duckle_metadata::DataType::String),
                ("body_text", duckle_metadata::DataType::String),
            ]
            .iter()
            .map(|(name, dt)| duckle_metadata::Column {
                name: (*name).to_string(),
                data_type: *dt,
                nullable: true,
                primary_key: None,
                format: None,
            })
            .collect::<Vec<_>>();
            materialize_jsonobjects_as_table_typed(
                &self.bin,
                db,
                &spec.node_id,
                &[],
                Some(schema.as_slice()),
            )?;
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
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
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
                    &boa_engine::JsValue::undefined(),
                    &[boa_engine::JsValue::from(js_string!(s.as_str()))],
                    &mut ctx,
                )
                .map_err(|e| EngineError::Query(format!("js: row -> JsValue: {}", e)))?;
            let result = transform
                .as_callable()
                .ok_or_else(|| EngineError::Query("js: transform not callable".into()))?
                .call(&boa_engine::JsValue::undefined(), &[js_in], &mut ctx)
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
                .call(&boa_engine::JsValue::undefined(), &[result], &mut ctx)
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

    /// xf.jq: apply a jq filter to a JSON column per row via the pure-Rust
    /// `jaq` engine (GitHub #173). No C libjq, no subprocess: the filter is
    /// compiled once and interpreted in-process against each row's column
    /// value. Row count is preserved 1:1 - the output stream folds into the
    /// output column as one value (1 result), a JSON array (>1) or null (0).
    pub(crate) fn run_jq(&self, db: &Path, spec: &JqSpec) -> Result<String, EngineError> {
        use jaq_core::load::{Arena, File, Loader};
        use jaq_core::{data, unwrap_valr, Compiler, Ctx, Vars};
        use jaq_json::Val;
        self.check_cancelled()?;

        // Compile the filter ONCE, up front, so a bad program fails the stage
        // immediately instead of once per row.
        let defs = jaq_core::defs().chain(jaq_std::defs()).chain(jaq_json::defs());
        let funs = jaq_core::funs().chain(jaq_std::funs()).chain(jaq_json::funs());
        let loader = Loader::new(defs);
        let arena = Arena::default();
        let modules = loader
            .load(&arena, File { code: spec.filter.as_str(), path: () })
            .map_err(|errs| {
                EngineError::Config(format!(
                    "xf.jq: could not parse filter `{}`: {}",
                    spec.filter,
                    errs.len()
                ))
            })?;
        let filter = Compiler::default()
            .with_funs(funs)
            .compile(modules)
            .map_err(|errs| {
                EngineError::Config(format!(
                    "xf.jq: could not compile filter `{}`: {}",
                    spec.filter,
                    errs.len()
                ))
            })?;

        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
            return Ok(format!("xf.jq: 0 upstream rows -> {}", spec.node_id));
        }

        let lenient = spec.on_error.eq_ignore_ascii_case("null");
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            self.check_cancelled()?;
            // Extract the target column's JSON value. A DuckDB JSON column
            // arrives already-nested; a VARCHAR column carrying JSON text
            // arrives as a string, so parse it when it parses (and otherwise
            // feed jq the raw string, which is a valid jq input).
            let input = match row.get(&spec.column) {
                Some(JsonValue::String(s)) => {
                    serde_json::from_str::<JsonValue>(s).unwrap_or_else(|_| JsonValue::String(s.clone()))
                }
                Some(v) => v.clone(),
                None => JsonValue::Null,
            };

            let mut results: Vec<JsonValue> = Vec::new();
            let mut row_err: Option<String> = None;
            // The value type is the jq engine's own, so a row crosses in through serde and
            // back out through its JSON rendering.
            let input_val = jaq_json::read::parse_single(input.to_string().as_bytes())
                .unwrap_or_default();
            let ctx = Ctx::<data::JustLut<Val>>::new(&filter.lut, Vars::new([]));
            for r in filter.id.run((ctx, input_val)).map(unwrap_valr) {
                match r {
                    Ok(v) => results.push(
                        serde_json::from_str(&v.to_string()).unwrap_or(JsonValue::Null),
                    ),
                    Err(e) => {
                        row_err = Some(e.to_string());
                        break;
                    }
                }
            }
            let value = if let Some(e) = row_err {
                if lenient {
                    JsonValue::Null
                } else {
                    return Err(EngineError::Query(format!(
                        "xf.jq: filter failed on a row (column `{}`): {}. Set On error to 'null' to skip such rows.",
                        spec.column, e
                    )));
                }
            } else {
                match results.len() {
                    0 => JsonValue::Null,
                    1 => results.pop().unwrap(),
                    _ => JsonValue::Array(results),
                }
            };

            // Enrich the row in place: keep every upstream column, add/replace
            // the output column with the jq result.
            let mut obj = match row {
                JsonValue::Object(m) => m.clone(),
                _ => serde_json::Map::new(),
            };
            obj.insert(spec.output_column.clone(), value);
            out.push(JsonValue::Object(obj));
        }
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        Ok(format!("xf.jq: transformed {} row(s) into {}", count, spec.node_id))
    }

    /// code.python: per-row transform via a real Python 3 interpreter (shelled
    /// out, so the user gets the full language + installed packages). The script
    /// defines process(row) -> dict; the engine wraps it in a harness that reads
    /// the upstream rows as JSON, applies process per row (None drops the row),
    /// and writes the result JSON back for materialization. No Python in-engine.
    pub(crate) fn run_python(
        &self,
        db: &Path,
        spec: &PythonSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        // A script defining `transform` is handed the WHOLE table through Parquet
        // instead of every row through JSON. Measured on 200k rows x 8 columns: 2.11s
        // for the JSON round trip against 0.74s for Parquet, and the per-row call was
        // never the cost - passing the same JSON as one list came out at 2.36s, no
        // better. The transport is the whole difference.
        //
        // It is also the difference between keeping a type and losing it. JSON leaves
        // through `default=str`, so every timestamp reaches Python as a string and any
        // decimal precision goes with it. Parquet carries timestamp[us, tz] as itself.
        if defines_streaming_entry(&spec.script) {
            return self.run_python_arrow(db, spec, true);
        }
        if defines_vectorized_entry(&spec.script) {
            return self.run_python_arrow(db, spec, false);
        }
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", plan::quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
            return Ok(format!("code.python: 0 upstream rows -> {}", spec.node_id));
        }
        let (in_path, out_path, script_path) = python_temp_paths(db, &spec.node_id);
        let cleanup = |a: &Path, b: &Path, c: &Path| {
            let _ = std::fs::remove_file(a);
            let _ = std::fs::remove_file(b);
            let _ = std::fs::remove_file(c);
        };
        if let Err(e) = std::fs::write(
            &in_path,
            serde_json::to_vec(&rows)
                .map_err(|e| EngineError::Query(format!("code.python: encode input: {}", e)))?,
        ) {
            return Err(EngineError::Query(format!("code.python: write input: {}", e)));
        }
        // Built line-by-line so the user's script keeps column 0 and the runner
        // lines keep exact Python indentation. default=str serializes dates/etc.
        let harness = [
            "import json, sys".to_string(),
            "__rows = json.load(open(sys.argv[1], encoding='utf-8'))".to_string(),
            spec.script.clone(),
            "__out = []".to_string(),
            "for __row in __rows:".to_string(),
            "    __r = process(__row)".to_string(),
            "    if __r is not None:".to_string(),
            "        __out.append(__r)".to_string(),
            "with open(sys.argv[2], 'w', encoding='utf-8') as __f:".to_string(),
            "    json.dump(__out, __f, default=str)".to_string(),
        ]
        .join("\n");
        if let Err(e) = std::fs::write(&script_path, harness) {
            cleanup(&in_path, &out_path, &script_path);
            return Err(EngineError::Query(format!("code.python: write script: {}", e)));
        }
        let mut cmd = std::process::Command::new(resolve_python_bin());
        cmd.arg(&script_path).arg(&in_path).arg(&out_path);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let output = match cmd.output() {
            Ok(o) => o,
            Err(e) => {
                cleanup(&in_path, &out_path, &script_path);
                return Err(EngineError::Query(format!(
                    "code.python: cannot run python: {} (install Python 3 or set DUCKLE_PYTHON_BIN)",
                    e
                )));
            }
        };
        if !output.status.success() {
            cleanup(&in_path, &out_path, &script_path);
            return Err(EngineError::Query(format!(
                "code.python: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let text = match std::fs::read_to_string(&out_path) {
            Ok(t) => t,
            Err(e) => {
                cleanup(&in_path, &out_path, &script_path);
                return Err(EngineError::Query(format!("code.python: read output: {}", e)));
            }
        };
        cleanup(&in_path, &out_path, &script_path);
        let result: Vec<JsonValue> = serde_json::from_str(&text)
            .map_err(|e| EngineError::Query(format!("code.python: parse output: {}", e)))?;
        let count = result.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &result)?;
        Ok(format!("code.python: transformed {} row(s) into {}", count, spec.node_id))
    }

    /// `code.python` handed the whole table, through Parquet.
    ///
    /// The same shape as the per-row path - write, shell out, read back - with the
    /// interchange swapped. DuckDB already writes Parquet everywhere in this engine and
    /// pyarrow, polars and pandas all read it, so the boundary costs a file each way
    /// rather than four format conversions.
    fn run_python_arrow(
        &self,
        db: &Path,
        spec: &PythonSpec,
        streaming: bool,
    ) -> Result<String, EngineError> {
        let (in_path, out_path, script_path) = python_temp_paths(db, &spec.node_id);
        let in_pq = in_path.with_extension("parquet");
        let out_pq = out_path.with_extension("parquet");
        let cleanup = |a: &Path, b: &Path, c: &Path| {
            let _ = std::fs::remove_file(a);
            let _ = std::fs::remove_file(b);
            let _ = std::fs::remove_file(c);
        };
        let esc = |p: &Path| p.to_string_lossy().replace('\\', "/").replace('\'', "''");

        // Hand the rows over as they are. An empty upstream still writes a file, so the
        // script sees a table with the right columns and no rows rather than nothing.
        self.run(
            Some(db),
            &format!(
                "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET);",
                plan::quote_ident(&spec.from_view),
                esc(&in_pq)
            ),
            false,
        )?;

        // pyarrow is imported HERE and only here, so a script using process(row) never
        // needs it and a bare-Python install keeps working exactly as before. Missing,
        // it says so and stops - falling back to JSON would make the pipeline quietly
        // slower and stringify its timestamps, which is the thing this avoids.
        let entry = if streaming { "transform_batches(batch)" } else { "transform(table)" };
        let mut harness: Vec<String> = vec![
            "import sys".to_string(),
            "try:".to_string(),
            "    import pyarrow.parquet as __pq".to_string(),
            "except ImportError:".to_string(),
            "    sys.stderr.write(".to_string(),
            format!("        'a script defining {} needs pyarrow in ' + sys.executable", entry),
            "        + \"; install it, or define process(row) instead to keep the row-at-a-time mode\")"
                .to_string(),
            "    raise SystemExit(1)".to_string(),
            // The input Parquet is already on disk, so naming it costs nothing and
            // lets a script reach past the harness - dataset.Scanner, polars
            // scan_parquet, or DuckDB over the same file (#245).
            "INPUT_PATH = sys.argv[1]".to_string(),
            "OUTPUT_PATH = sys.argv[2]".to_string(),
        ];
        if streaming {
            // Never materializes: batches in, batches out, one writer opened from
            // the first result's schema. This is the whole point of the mode - a
            // table that does not fit in memory still runs.
            harness.extend([
                "__pf = __pq.ParquetFile(sys.argv[1])".to_string(),
                spec.script.clone(),
                "__writer = None".to_string(),
                "__rows = 0".to_string(),
                "import pyarrow as __pa".to_string(),
                // 64k rows a batch: big enough that per-batch Python overhead
                // disappears, small enough that peak memory stays bounded, which
                // is the entire reason for this mode.
                "for __batch in __pf.iter_batches(batch_size=65536):".to_string(),
                "    __res = transform_batches(__batch)".to_string(),
                // Returning nothing means "unchanged", the same contract the
                // whole-table mode uses.
                "    if __res is None:".to_string(),
                "        __res = __batch".to_string(),
                "    if hasattr(__res, 'to_arrow'):".to_string(),
                "        __res = __res.to_arrow()".to_string(),
                "    if isinstance(__res, __pa.RecordBatch):".to_string(),
                "        __res = __pa.Table.from_batches([__res])".to_string(),
                "    elif not hasattr(__res, 'schema'):".to_string(),
                "        __res = __pa.Table.from_pandas(__res)".to_string(),
                "    if __writer is None:".to_string(),
                "        __writer = __pq.ParquetWriter(sys.argv[2], __res.schema)".to_string(),
                "    __writer.write_table(__res)".to_string(),
                "    __rows += __res.num_rows".to_string(),
                "if __writer is None:".to_string(),
                // An upstream with no rows still has to leave a file with the
                // right columns, or the next stage sees nothing rather than an
                // empty relation.
                "    __pq.write_table(__pf.schema_arrow.empty_table(), sys.argv[2])".to_string(),
                "else:".to_string(),
                "    __writer.close()".to_string(),
            ]);
        } else {
        harness.extend([
            "__table = __pq.read_table(sys.argv[1])".to_string(),
            spec.script.clone(),
            "__out = transform(__table)".to_string(),
            // Returning nothing means "unchanged", which is the reading that cannot be
            // confused with "no rows" - an empty table says that already.
            "if __out is None:".to_string(),
            "    __out = __table".to_string(),
            // polars and pandas both convert in one call, so a script may return either.
            "if hasattr(__out, 'to_arrow'):".to_string(),
            "    __out = __out.to_arrow()".to_string(),
            "elif not hasattr(__out, 'schema'):".to_string(),
            "    import pyarrow as __pa".to_string(),
            "    __out = __pa.Table.from_pandas(__out)".to_string(),
            "__pq.write_table(__out, sys.argv[2])".to_string(),
        ]);
        }
        let harness = harness.join("
");
        if let Err(e) = std::fs::write(&script_path, harness) {
            cleanup(&in_pq, &out_pq, &script_path);
            return Err(EngineError::Query(format!("code.python: write script: {}", e)));
        }

        let mut cmd = std::process::Command::new(resolve_python_bin());
        cmd.arg(&script_path).arg(&in_pq).arg(&out_pq);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let output = match cmd.output() {
            Ok(o) => o,
            Err(e) => {
                cleanup(&in_pq, &out_pq, &script_path);
                return Err(EngineError::Query(format!(
                    "code.python: cannot run python: {} (install Python 3 or set DUCKLE_PYTHON_BIN)",
                    e
                )));
            }
        };
        if !output.status.success() {
            cleanup(&in_pq, &out_pq, &script_path);
            return Err(EngineError::Query(format!(
                "code.python: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        if !out_pq.exists() {
            cleanup(&in_pq, &out_pq, &script_path);
            return Err(EngineError::Query(
                "code.python: transform(table) wrote nothing back".into(),
            ));
        }
        // Read it back as the node's own relation, types and all.
        self.run(
            Some(db),
            &format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}');",
                plan::quote_ident(&spec.node_id),
                esc(&out_pq)
            ),
            false,
        )?;
        let count = self.count_rows(db, &spec.node_id).unwrap_or(0);
        cleanup(&in_pq, &out_pq, &script_path);
        Ok(format!(
            "code.python: transformed {} row(s) into {} (vectorized)",
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
        if rows.is_empty() {
            // #170: empty upstream -> empty output shaped like upstream, so
            // downstream binds the real columns instead of erroring.
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
            return Ok(format!("ai.dedupe: 0 upstream rows -> {}", spec.node_id));
        }
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
        // Store each kept embedding alongside its precomputed L2 norm so the
        // O(N^2) comparison only does the dot-product pass instead of
        // recomputing both norms on every pair.
        let mut kept_embeddings: Vec<(Vec<f64>, f64)> = Vec::new();
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
                kept_embeddings.push((Vec::new(), 0.0));
                continue;
            };
            // Drop if any previously-kept embedding is within threshold. Reuse
            // each kept vector's stored norm and compute this row's norm once.
            let e_norm = l2_norm(&e);
            let is_dup = kept_embeddings
                .iter()
                .filter(|(p, _)| !p.is_empty())
                .any(|(p, pn)| cosine_similarity_with_norms(p, *pn, &e, e_norm) >= spec.threshold);
            if !is_dup {
                kept.push(row.clone());
                kept_embeddings.push((e, e_norm));
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
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
            return Ok(format!("ai.classify: 0 upstream rows -> {}", spec.node_id));
        }
        let endpoint = Self::ai_endpoint(&spec.base_url, &spec.endpoint_path, "/v1/chat/completions");
        let cat_list = spec.categories.join(", ");
        let system_prompt = format!(
            "You are a strict classifier. Pick exactly one of these categories: {}. \
             Reply with only the category name and nothing else.",
            cat_list
        );
        let out = self.ai_map_concurrent(rows.len(), spec.concurrency, |engine, i| {
            let row = &rows[i];
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
            let response = engine.ai_send_with_retry(
                &|| Self::ai_post(&endpoint, &spec.headers, &spec.api_key),
                &body.to_string(),
                "ai.classify",
                spec.max_retries,
            )?;
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
            Ok(JsonValue::Object(obj))
        })?;
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
    pub(crate) fn run_ai_llm(
        &self,
        db: &Path,
        spec: &AiLlmSpec,
        pipeline_name: Option<&str>,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        let rows = self.run_rows(
            Some(db),
            &format!("SELECT * FROM {};", quote_ident(&spec.from_view)),
        )?;
        if rows.is_empty() {
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
            return Ok(format!("ai.llm: 0 upstream rows -> {}", spec.node_id));
        }
        let endpoint = Self::ai_endpoint(&spec.base_url, &spec.endpoint_path, "/v1/chat/completions");

        // #252: an item that was already paid for is not bought again.
        //
        // The configuration is part of the identity, so a changed model, prompt
        // or temperature invalidates everything - the stored answer was produced
        // by the old one and reusing it would be silently wrong.
        let config_fp = crate::checkpoint::fingerprint(&serde_json::json!({
            "model": spec.model,
            "prompt": spec.prompt_template,
            "system": spec.system_prompt,
            "temperature": spec.temperature,
            "max_tokens": spec.max_tokens,
            "input_column": spec.input_column,
            "output_column": spec.output_column,
            "endpoint": endpoint,
        }));
        let store = if spec.checkpoint {
            match std::env::var("DUCKLE_WORKSPACE").ok().filter(|w| !w.is_empty()) {
                Some(ws) => Some(crate::checkpoint::Store::open(
                    std::path::Path::new(&ws),
                    pipeline_name.unwrap_or(UNNAMED_RUN_FOLDER),
                    &spec.node_id,
                )?),
                None => {
                    return Err(EngineError::Config(
                        concat!(
                            "ai.llm: checkpointing needs a workspace (DUCKLE_WORKSPACE) ",
                            "to keep completed items in"
                        )
                        .into(),
                    ))
                }
            }
        } else {
            None
        };
        let reused = std::sync::atomic::AtomicUsize::new(0);

        let out = self.ai_map_concurrent(rows.len(), spec.concurrency, |engine, i| {
            let row = &rows[i];
            // Reuse before spending. The key covers the logical key, the whole
            // input row and the configuration, so a hit means this exact work
            // was done with this exact setup.
            let ck = store
                .as_ref()
                .map(|_| {
                    crate::checkpoint::item_key(
                        row,
                        &spec.checkpoint_key,
                        &spec.checkpoint_fingerprint,
                        &config_fp,
                    )
                });
            if let (Some(store), Some(key)) = (store.as_ref(), ck.as_ref()) {
                if let Some(done) = store.get(key) {
                    reused.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(done.clone());
                }
            }
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
            let mut body = serde_json::json!({
                "model": spec.model,
                "messages": messages,
                "temperature": spec.temperature,
            });
            // #258: the GUI has offered Max tokens since #142 while the request
            // body never carried it, so every row was billed an unbounded reply.
            if let Some(max) = spec.max_tokens {
                body["max_tokens"] = serde_json::json!(max);
            }
            let response = engine.ai_send_with_retry(
                &|| Self::ai_post(&endpoint, &spec.headers, &spec.api_key),
                &body.to_string(),
                "ai.llm",
                spec.max_retries,
            )?;
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
            let produced = JsonValue::Object(obj);
            // Recorded HERE, as this item finishes, not when the stage does.
            // That is the whole guarantee: a failure on the next row keeps
            // everything already bought.
            if let (Some(store), Some(key)) = (store.as_ref(), ck.as_ref()) {
                store.record(key, &produced)?;
            }
            Ok(produced)
        })?;
        let count = out.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &out)?;
        let reused = reused.load(std::sync::atomic::Ordering::Relaxed);
        Ok(format!(
            "ai.llm ({}): {} row(s) -> {}{}",
            spec.model,
            count,
            spec.node_id,
            if reused > 0 {
                format!(" ({} reused from the checkpoint, {} called)", reused, count - reused)
            } else {
                String::new()
            }
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
        if rows.is_empty() {
            // #170: empty upstream -> empty output shaped like upstream, so
            // downstream binds the real columns instead of erroring.
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
            return Ok(format!("ai.pii: 0 upstream rows -> {}", spec.node_id));
        }
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
        if rows.is_empty() {
            // #170: empty upstream -> empty output shaped like upstream, so
            // downstream binds the real columns instead of erroring.
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
            return Ok(format!("ai.chunk: 0 upstream rows -> {}", spec.node_id));
        }
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
            materialize_empty_like_view(&self.bin, db, &spec.node_id, &spec.from_view)?;
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
        // 1.x folds the start function into instantiation rather than handing back a
        // pre-instance to start separately.
        let instance = linker
            .instantiate_and_start(&mut store, module)
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
        // Captured before the async block: `tls` would shadow the crate's tls
        // module inside it.
        let use_tls = spec.tls;
        let sasl = spec.sasl.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("kafka: tokio rt: {}", e)))?;
        let total: Result<usize, String> = rt.block_on(async {
            use rskafka::client::partition::{Compression, UnknownTopicHandling};
            use rskafka::record::Record;
            let client = kafka_client_builder(bootstrap, use_tls, sasl.as_ref())?
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
        pipeline_name: Option<&str>,
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        let cancel = self.cancel.clone();
        // Where the resume point lives, and what it says. Shares the state
        // folder xf.incremental uses: a node is one or the other, never both.
        let state_path = if spec.track_offset {
            incremental_state_path(pipeline_name, &spec.node_id)
        } else {
            None
        };
        let prior = state_path.as_deref().and_then(crate::read_state_snapshot);
        let resume = state_path
            .as_deref()
            .and_then(|p| read_kafka_offset_state(p, &spec.topic, spec.partition_id));
        let bootstrap: Vec<String> = spec
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        // Captured before the async block: `tls` would shadow the crate's tls
        // module inside it.
        let registry = spec.schema_registry_url.clone();
        let use_tls = spec.tls;
        let sasl = spec.sasl.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("kafka: tokio rt: {}", e)))?;
        let rows: Result<(Vec<JsonValue>, i64), String> = rt.block_on(async {
            use rskafka::client::partition::UnknownTopicHandling;
            let client = kafka_client_builder(bootstrap, use_tls, sasl.as_ref())?
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
            // A committed resume point wins over the configured start. That is
            // the whole point: `latest` would otherwise jump to the current tip
            // on every run and skip everything produced in between, while
            // `earliest` would re-read the entire backlog every time.
            let mut next_offset = if let Some(o) = resume {
                o
            } else if spec.start_offset == -2 {
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
            let mut schema_cache: std::collections::HashMap<u32, apache_avro::Schema> =
                std::collections::HashMap::new();
            let http = crate::tls::http_agent();
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
                    // A Confluent-framed field is decoded against the schema
                    // its id names; anything else stays text. Schemas are
                    // fetched once per id and kept for the rest of the read.
                    let mut decode = |b: &[u8]| -> Result<JsonValue, String> {
                        if let Some(reg) = registry.as_deref() {
                            if let Some((id, payload)) = confluent_envelope(b) {
                                if !schema_cache.contains_key(&id) {
                                    let sc = fetch_registry_schema(&http, reg, id)?;
                                    schema_cache.insert(id, sc);
                                }
                                return avro_datum_to_json(&schema_cache[&id], payload);
                            }
                        }
                        Ok(JsonValue::String(String::from_utf8_lossy(b).to_string()))
                    };
                    obj.insert(
                        "key".into(),
                        match r.record.key.as_ref() {
                            Some(b) => decode(b)?,
                            None => JsonValue::Null,
                        },
                    );
                    obj.insert(
                        "value".into(),
                        match r.record.value.as_ref() {
                            Some(b) => decode(b)?,
                            None => JsonValue::Null,
                        },
                    );
                    out.push(JsonValue::Object(obj));
                    next_offset = r.offset + 1;
                    if out.len() as u64 >= spec.max_records {
                        break;
                    }
                }
            }
            Ok((out, next_offset))
        });
        let (rows, next_offset) = match rows {
            Ok(r) => r,
            Err(e) if e == "cancelled" => return Err(EngineError::Cancelled),
            Err(e) => return Err(EngineError::Query(format!("kafka source: {}", e))),
        };
        let count = rows.len();
        materialize_jsonobjects_as_table(&self.bin, db, &spec.node_id, &rows)?;
        // Queue the resume point rather than writing it: it lands only if the
        // WHOLE run succeeds, so a failure downstream re-delivers these records
        // next run instead of losing them. At-least-once, deliberately - the
        // alternative is committing an offset for rows no sink ever wrote.
        //
        // Written even when nothing was read, because that is exactly the case
        // a `latest` start gets wrong: the first run pins the tip so the next
        // one resumes from it, instead of jumping to a new tip and skipping
        // whatever arrived in between.
        if let Some(path) = state_path {
            pending.push(crate::PendingWrite::state(
                path,
                serde_json::json!({
                    "topic": spec.topic,
                    "partition": spec.partition_id,
                    "next_offset": next_offset,
                }),
                prior,
            ));
        }
        Ok(format!(
            "kafka: materialized {} record(s) into {}{}",
            count,
            spec.node_id,
            if spec.track_offset {
                format!(" (resumes at offset {} if this run succeeds)", next_offset)
            } else {
                String::new()
            }
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
                if !spec.encrypt {
                    // #141: legacy servers (SQL Server 2014 and older) offer only
                    // TLS 1.0/1.1, which rustls refuses outright (it supports 1.2+
                    // only), so even trust_cert cannot get through the handshake.
                    // NotSupported skips TLS entirely; the login travels
                    // unencrypted, matching other tools' "encrypt = no".
                    config.encryption(tiberius::EncryptionLevel::NotSupported);
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
                // Truncate + insert write mode (#138): clear rows, keep the
                // table. Non-upsert only; upsert MERGEs below.
                if !is_upsert && spec.mode == "truncate" {
                    client
                        .execute(format!("TRUNCATE TABLE {}", qualified).as_str(), &[])
                        .await
                        .map_err(|e| format!("truncate table: {}", e))?;
                }
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
                if !spec.encrypt {
                    // #141: legacy servers (SQL Server 2014 and older) offer only
                    // TLS 1.0/1.1, which rustls refuses outright (it supports 1.2+
                    // only), so even trust_cert cannot get through the handshake.
                    // NotSupported skips TLS entirely; the login travels
                    // unencrypted, matching other tools' "encrypt = no".
                    config.encryption(tiberius::EncryptionLevel::NotSupported);
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
        // Disable 64-bit-integer quoting: ClickHouse's default JSON output
        // emits Int64/UInt64/Int128/Decimal as quoted strings, which would
        // make DuckDB infer those columns as VARCHAR. The HTTP interface reads
        // settings from URL params, so this is safe regardless of the query.
        let url = format!(
            "{}/?output_format_json_quote_64bit_integers=0",
            spec.endpoint.trim_end_matches('/')
        );
        let q = if spec
            .query
            .to_uppercase()
            .contains("FORMAT JSON")
        {
            spec.query.clone()
        } else {
            // Strip a trailing ';' before appending the FORMAT clause, else
            // `SELECT ...; FORMAT JSON` parses as a second, invalid statement.
            let base = spec.query.trim().trim_end_matches(';').trim_end();
            format!("{} FORMAT JSON", base)
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

    /// snk.huggingface: push the upstream to a Hugging Face Hub dataset repo.
    /// DuckDB's hf:// is read-only, so the write goes over the Hub HTTP API:
    /// materialize a local Parquet, then create-repo -> preupload -> git-LFS
    /// batch + PUT -> NDJSON commit. Parquet is always LFS-tracked on the Hub.
    pub(crate) fn run_huggingface_sink(
        &self,
        db: &Path,
        spec: &HuggingFaceSinkSpec,
    ) -> Result<String, EngineError> {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine as _;
        use sha2::{Digest, Sha256};
        self.check_cancelled()?;

        // 1. Materialize the upstream to a local Parquet file.
        let staging = std::env::temp_dir().join(format!(
            "duckle-hf-{}-{}.parquet",
            std::process::id(),
            HF_SINK_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let staging_sql = staging.display().to_string().replace('\\', "/");
        let copy = format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT parquet)",
            plan::quote_ident(&spec.from_view),
            staging_sql.replace('\'', "''")
        );
        self.run(Some(db), &copy, false)?;
        let bytes = std::fs::read(&staging)
            .map_err(|e| EngineError::Query(format!("huggingface: read staged parquet: {}", e)))?;
        let _ = std::fs::remove_file(&staging);
        let size = bytes.len() as u64;
        let oid = {
            let mut s = String::with_capacity(64);
            for b in Sha256::digest(&bytes) {
                s.push_str(&format!("{:02x}", b));
            }
            s
        };

        let agent = crate::tls::http_agent();
        let auth = format!("Bearer {}", spec.token);
        let (org, name) = spec.repo.split_once('/').unwrap_or(("", spec.repo.as_str()));

        // 2. Create the repo if it does not exist yet (409 = already there).
        match agent
            .post("https://huggingface.co/api/repos/create")
            .set("Authorization", &auth)
            .send_json(serde_json::json!({
                "type": "dataset",
                "name": name,
                "organization": if org.is_empty() { JsonValue::Null } else { JsonValue::String(org.to_string()) },
                "private": spec.private,
            })) {
            Ok(_) => {}
            Err(ureq::Error::Status(409, _)) => {}
            Err(ureq::Error::Status(code, r)) => {
                return Err(EngineError::Query(format!(
                    "huggingface: create repo failed ({}): {}",
                    code,
                    r.into_string().unwrap_or_default()
                )))
            }
            Err(e) => return Err(EngineError::Query(format!("huggingface: create repo: {}", e))),
        }

        // 3. Preupload classifies the file; a Parquet comes back as LFS.
        let sample_b64 = B64.encode(&bytes[..bytes.len().min(512)]);
        let pre: JsonValue = agent
            .post(&format!(
                "https://huggingface.co/api/datasets/{}/preupload/{}",
                spec.repo, spec.revision
            ))
            .set("Authorization", &auth)
            .send_json(serde_json::json!({
                "files": [{ "path": spec.path, "sample": sample_b64, "size": size }]
            }))
            .map_err(|e| EngineError::Query(format!("huggingface: preupload: {}", e)))?
            .into_json()
            .map_err(|e| EngineError::Query(format!("huggingface: preupload response: {}", e)))?;
        let upload_mode = pre
            .get("files")
            .and_then(|f| f.get(0))
            .and_then(|f| f.get("uploadMode"))
            .and_then(|v| v.as_str())
            .unwrap_or("lfs");

        // 4. Build the commit's file line; for LFS, upload the bytes first.
        let file_line = if upload_mode == "regular" {
            serde_json::json!({
                "key": "file",
                "value": { "path": spec.path, "content": B64.encode(&bytes), "encoding": "base64" }
            })
            .to_string()
        } else {
            let batch: JsonValue = agent
                .post(&format!(
                    "https://huggingface.co/datasets/{}.git/info/lfs/objects/batch",
                    spec.repo
                ))
                .set("Authorization", &auth)
                .set("Accept", "application/vnd.git-lfs+json")
                .set("Content-Type", "application/vnd.git-lfs+json")
                .send_json(serde_json::json!({
                    "operation": "upload",
                    "transfers": ["basic"],
                    "objects": [{ "oid": oid, "size": size }],
                    "hash_algo": "sha256"
                }))
                .map_err(|e| EngineError::Query(format!("huggingface: lfs batch: {}", e)))?
                .into_json()
                .map_err(|e| EngineError::Query(format!("huggingface: lfs batch response: {}", e)))?;
            let action = batch
                .get("objects")
                .and_then(|o| o.get(0))
                .and_then(|o| o.get("actions"))
                .and_then(|a| a.get("upload"));
            // No upload action means the object already exists on the Hub (dedup).
            if let Some(action) = action {
                let href = action.get("href").and_then(|v| v.as_str()).ok_or_else(|| {
                    EngineError::Query("huggingface: lfs upload href missing".into())
                })?;
                let mut put = agent.put(href);
                if let Some(hdrs) = action.get("header").and_then(|h| h.as_object()) {
                    for (k, v) in hdrs {
                        if let Some(vs) = v.as_str() {
                            put = put.set(k, vs);
                        }
                    }
                }
                put.send_bytes(&bytes)
                    .map_err(|e| EngineError::Query(format!("huggingface: lfs upload: {}", e)))?;
            }
            serde_json::json!({
                "key": "lfsFile",
                "value": { "path": spec.path, "algo": "sha256", "oid": oid, "size": size }
            })
            .to_string()
        };

        // 5. Commit (NDJSON: a header line, then the file line).
        let header_line = serde_json::json!({
            "key": "header",
            "value": { "summary": spec.commit_message, "description": "" }
        })
        .to_string();
        agent
            .post(&format!(
                "https://huggingface.co/api/datasets/{}/commit/{}",
                spec.repo, spec.revision
            ))
            .set("Authorization", &auth)
            .set("Content-Type", "application/x-ndjson")
            .send_string(&format!("{}\n{}\n", header_line, file_line))
            .map_err(|e| EngineError::Query(format!("huggingface: commit: {}", e)))?;

        Ok(format!(
            "huggingface: pushed {} ({} bytes) to {} @ {}",
            spec.path, size, spec.repo, spec.revision
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
        // Stream the upstream through newline-delimited JSON on disk instead of
        // materializing it. run_rows held the whole result set in memory and, on
        // a million rows, spent 7 s building it before a single document was
        // sent. DuckDB writing NDJSON lets the read, the BSON conversion and the
        // inserts overlap, at constant memory rather than proportional to the
        // row count.
        let staging_dir = std::env::temp_dir().join(format!(
            "duckle-mongo-{}-{}",
            std::process::id(),
            MONGO_STAGE_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&staging_dir);
        let _cleanup = ScopedDir(staging_dir.clone());
        create_private_dir(&staging_dir)
            .map_err(|e| EngineError::Other(format!("mongodb sink: staging dir: {}", e)))?;
        let ndjson = staging_dir.join("rows.ndjson");
        let copy = format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT JSON, ARRAY false)",
            plan::quote_ident(&spec.from_view),
            sql_escape(&ndjson.display().to_string().replace('\\', "/"))
        );
        self.run(Some(db), &copy, false)?;

        let cancel = self.cancel.clone();
        // Multi-threaded on purpose. serde_json -> BSON is CPU work, and on a
        // current-thread runtime it serialized behind the network waits; giving
        // it real threads lets conversion for one batch proceed while another
        // batch is in flight.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
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
                for chunk in mongo_ndjson_batches(&ndjson, spec.batch_size)
                    .map_err(|e| format!("reading staged rows: {}", e))?
                {
                    let chunk = &chunk;
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
            // Keep several batches in flight. insert_many is a network round
            // trip, so awaiting each one in turn left the connection idle for
            // most of the run; MongoDB happily takes concurrent batches.
            const IN_FLIGHT: usize = 6;
            let mut total = 0_usize;
            let mut pending: Vec<tokio::task::JoinHandle<Result<usize, String>>> = Vec::new();
            for chunk in mongo_ndjson_batches(&ndjson, spec.batch_size)
                .map_err(|e| format!("reading staged rows: {}", e))?
            {
                if cancel.load(Ordering::Relaxed) {
                    return Err("cancelled".into());
                }
                if pending.len() >= IN_FLIGHT {
                    let done = pending.remove(0);
                    total += done.await.map_err(|e| format!("insert task: {}", e))??;
                }
                let coll = collection.clone();
                pending.push(tokio::spawn(async move {
                    let docs: Vec<mongodb::bson::Document> = chunk
                        .iter()
                        .filter_map(|v| mongodb::bson::to_document(v).ok())
                        .collect();
                    if docs.is_empty() {
                        return Ok(0);
                    }
                    let n = docs.len();
                    coll.insert_many(docs)
                        .await
                        .map_err(|e| format!("insert_many: {}", e))?;
                    Ok(n)
                }));
            }
            for h in pending {
                total += h.await.map_err(|e| format!("insert task: {}", e))??;
            }
            Ok(format!(
                "mongodb: inserted {} docs into {}.{}",
                total, spec.database, spec.collection
            ))
        });
        // Wind the runtime down rather than dropping it outright. The driver
        // keeps background connection monitors alive, and tearing the runtime
        // out from under one makes it panic on a worker thread: harmless to the
        // result, which is already computed, but it prints a stack trace that
        // reads like a failed load. Observed once on the multi-threaded runtime.
        //
        // The grace period is deliberately short. Those monitors are long-lived
        // by design and never finish on their own, so a generous timeout is
        // simply paid in full: two seconds here cost two seconds on every run.
        // A brief window is enough for them to observe the shutdown signal.
        rt.shutdown_timeout(std::time::Duration::from_millis(150));
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
        // Stream documents straight to the NDJSON writer instead of buffering
        // the whole collection as Vec<Document> AND a second Vec<JsonValue> at
        // once (the deferred #7 mongo-source-into-RAM fix; the driver cursor
        // already streams server-side batches). BSON -> JSON conversion still
        // fails loud per document, and the table is only created on a clean
        // finalize, so a mid-stream error yields no partial table - same as
        // before.
        let writer = JsonLinesWriter::open(&spec.node_id)?;
        let bin = self.binary();
        let count: usize = rt
            .block_on(async move {
                let mut writer = writer;
                let client = mongodb::Client::with_uri_str(&spec.uri)
                    .await
                    .map_err(|e| format!("connect: {}", e))?;
                let collection = client
                    .database(&spec.database)
                    .collection::<mongodb::bson::Document>(&spec.collection);
                let mut count = 0usize;
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
                        let doc = cursor
                            .deserialize_current()
                            .map_err(|e| format!("deserialize: {}", e))?;
                        let row = serde_json::to_value(&doc)
                            .map_err(|e| format!("BSON to JSON: {}", e))?;
                        writer.write_row(&row).map_err(|e| format!("write row: {}", e))?;
                        count += 1;
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
                        let doc = cursor
                            .deserialize_current()
                            .map_err(|e| format!("deserialize: {}", e))?;
                        let row = serde_json::to_value(&doc)
                            .map_err(|e| format!("BSON to JSON: {}", e))?;
                        writer.write_row(&row).map_err(|e| format!("write row: {}", e))?;
                        count += 1;
                    }
                }
                writer
                    .finalize_into_table(bin, db, &spec.node_id)
                    .map_err(|e| format!("finalize: {}", e))?;
                Ok::<usize, String>(count)
            })
            .map_err(|e| EngineError::Query(format!("mongodb source: {}", e)))?;
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
                    let mut response = post(&body)?;
                    // Move the hits out instead of deep-cloning the whole array
                    // and then each _source again; `response` is dropped next.
                    let hits = response
                        .pointer_mut("/hits/hits")
                        .and_then(|v| v.as_array_mut())
                        .map(std::mem::take)
                        .unwrap_or_default();
                    let hit_count = hits.len();
                    for mut h in hits {
                        let source = h
                            .get_mut("_source")
                            .map(JsonValue::take)
                            .unwrap_or_else(|| JsonValue::Object(Default::default()));
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
                    let mut response = post(&body)?;
                    let hits = response
                        .pointer_mut("/hits/hits")
                        .and_then(|v| v.as_array_mut())
                        .map(std::mem::take)
                        .unwrap_or_default();
                    let hit_count = hits.len();
                    // Grab the last hit's sort before we move `hits`.
                    let next_after = hits
                        .last()
                        .and_then(|h| h.get("sort"))
                        .cloned();
                    for mut h in hits {
                        let source = h
                            .get_mut("_source")
                            .map(JsonValue::take)
                            .unwrap_or_else(|| JsonValue::Object(Default::default()));
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
        let mut all_rows: Vec<JsonValue> = Vec::new();
        let mut pages = 0_u64;
        // One Agent for the whole pagination walk so keep-alive connections
        // are reused across pages instead of a fresh TCP+TLS handshake each
        // request (ureq::request uses a throwaway agent per call).
        // #256: one agent for the whole walk, built from this node's transport
        // so a proxy, a timeout or a User-Agent set on a saved connection
        // applies to every request the node makes.
        let agent = match &spec.transport {
            Some(t) => crate::tls::http_agent_with(t),
            None => crate::tls::http_agent(),
        };
        // #166: src.salesforce OAuth client-credentials. Mint a fresh token once
        // per run and inject it as the Authorization header (replacing any static
        // one), so the whole pagination walk uses the same short-lived token.
        let mut eff_headers = spec.headers.clone();
        if let Some(o) = &spec.oauth {
            let (token, _instance) =
                mint_oauth_token(o)?;
            eff_headers.retain(|(k, _)| !k.eq_ignore_ascii_case("authorization"));
            eff_headers.push(("Authorization".into(), format!("Bearer {}", token)));
        }
        // #257: a parent endpoint can feed a child endpoint. Without a URL
        // template there is one pass and no substitution, exactly what this
        // function did before, so every existing pipeline and all the vendor
        // aliases are untouched. The agent, the once-per-run OAuth token, the
        // headers, the row extraction and all five pagination strategies below
        // are shared across the fan-out rather than redone per request.
        let parents: Vec<JsonValue> = match (&spec.from_view, &spec.url_template) {
            (Some(view), Some(_)) => self.run_rows(
                Some(db),
                &format!("SELECT * FROM {};", quote_ident(view)),
            )?,
            _ => vec![JsonValue::Null],
        };
        if spec.url_template.is_some() && parents.len() as u64 > spec.max_requests {
            return Err(EngineError::Query(format!(
                "rest: {} upstream rows would each make a request, past the cap of {}. Filter the upstream, or raise Max requests.",
                parents.len(),
                spec.max_requests
            )));
        }
        for parent in &parents {
            self.check_cancelled()?;
            let mut url = match &spec.url_template {
                Some(t) => render_url_template(t, parent)?,
                None => spec.url.clone(),
            };
            let mut truncated = false;
            // Mutable state for offset / page strategies; cursor uses
            // per-response extraction inside the loop. Reset per parent row:
            // each child endpoint paginates from its own beginning.
            let mut offset = 0_u64;
            let mut page_no = match &spec.pagination {
                RestPagination::Page { start_page, .. } => *start_page,
                _ => 1,
            };
            let mut parent_pages = 0_u64;
            // Seed the FIRST request with the start page; the loop only appends the
            // page param on subsequent requests, so without this the first call hit
            // the server's default page and a non-default start_page was skipped.
            if let RestPagination::Page { page_param, start_page } = &spec.pagination {
                let sep = if url.contains('?') { '&' } else { '?' };
                url = format!("{}{}{}={}", url, sep, page_param, start_page);
            }
            loop {
                self.check_cancelled()?;
                // Build request
                let mut req = agent.request(&spec.method, &url);
                let has_ct = eff_headers
                    .iter()
                    .any(|(k, _)| k.eq_ignore_ascii_case("content-type"));
                for (k, v) in &eff_headers {
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
                // The same, for provenance: once the body is read the response is gone, so
                // what answered and with what has to be taken here or not at all.
                let page_status = response_raw.status();
                let page_url = url.clone();
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
                // Stamp each row with where it came from. Underscore-prefixed, matching the
                // audit stamp the rest of the tool writes, and only on a row that is an
                // object - a scalar row has nowhere to put it.
                let rows = match spec.response_metadata {
                    false => rows,
                    true => {
                        let at = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        rows.into_iter()
                            .map(|mut r| {
                                if let Some(o) = r.as_object_mut() {
                                    o.insert("_http_url".into(), JsonValue::from(page_url.clone()));
                                    o.insert("_http_status".into(), JsonValue::from(page_status));
                                    o.insert("_fetched_at".into(), JsonValue::from(at));
                                }
                                r
                            })
                            .collect()
                    }
                };
                // #257: stamp the parent's key onto every row the child returned,
                // so child rows can be joined back to the parent that produced them.
                // The child rarely carries it: /companies/42/officers returns
                // officers, with nothing in them saying 42.
                let rows = match (&spec.parent_key_column, parent) {
                    (Some(col), JsonValue::Object(pm)) => {
                        let v = pm.get(col).cloned().unwrap_or(JsonValue::Null);
                        rows.into_iter()
                            .map(|r| match r {
                                JsonValue::Object(mut m) => {
                                    m.insert(col.clone(), v.clone());
                                    JsonValue::Object(m)
                                }
                                other => other,
                            })
                            .collect()
                    }
                    _ => rows,
                };
                all_rows.extend(rows);
                pages += 1;
                parent_pages += 1;
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
                if parent_pages >= spec.max_pages {
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
        }
        materialize_jsonobjects_as_table_typed(
            &self.bin,
            db,
            &spec.node_id,
            &all_rows,
            spec.declared_schema.as_deref(),
        )?;
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
        self.run_subpipeline_as(path, subs, None)
    }

    /// Write a `ctl.foreach`'s rows out as a batch instead of running them.
    ///
    /// Returns the line to report, so the caller decides how it surfaces. The
    /// count is in it deliberately: "queued 400 items" and "ran 400 items" look
    /// identical in a green run otherwise, and somebody has to notice that
    /// nothing has actually loaded yet.
    pub(crate) fn queue_foreach_batch(
        &self,
        node_id: &str,
        child: &str,
        per_row: &[(std::collections::HashMap<String, String>, Option<String>)],
        retry: Option<&crate::batch::RetryPolicy>,
    ) -> Result<String, EngineError> {
        // A batch is a file in the workspace, so without one there is nowhere
        // for the work to live and nothing could ever pick it up. Failing here
        // is much kinder than writing to a temp folder nobody will look in.
        let ws = std::env::var("DUCKLE_WORKSPACE").ok().filter(|s| !s.is_empty()).ok_or_else(|| {
            EngineError::Config(
                "dispatch \"queue\" needs a workspace (DUCKLE_WORKSPACE) to write the batch into"
                    .into(),
            )
        })?;
        let ws = std::path::Path::new(&ws);
        let batch_id = crate::batch::new_batch_id(node_id, chrono::Utc::now());
        let items: Vec<crate::batch::WorkItem> = per_row
            .iter()
            .enumerate()
            .map(|(index, (subs, item))| crate::batch::WorkItem {
                v: 1,
                batch: batch_id.clone(),
                index,
                item: item.clone(),
                child: child.to_string(),
                vars: subs.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                retry: retry.cloned(),
            })
            .collect();
        let path = crate::batch::write(ws, &batch_id, &items)?;

        // Say whether these items can actually be spread across workers. Both
        // "400 items each loading their own table" and "400 items appending to
        // one file" look identical on the canvas - one sink node with a
        // variable in it - and only the first is safe to run at once. Checked
        // here, before anyone picks the batch up, rather than discovered as
        // interleaved rows afterwards.
        let safety = crate::batch::inspect(&items, |child| {
            std::fs::read_to_string(resolve_subpipeline_ref(child)).ok()
        });
        let mut note = format!(
            "queued {} item(s) to {} - nothing has run yet; start a worker to pick them up",
            items.len(),
            path.display()
        );
        match safety.note() {
            Some(warning) => note.push_str(&format!("\nduckle: heads up - {warning}")),
            None => note.push_str(&format!(
                "\nduckle: {} item(s) write to targets nothing else in the batch writes, so they \
                 are safe to run at the same time",
                safety.disjoint
            )),
        }
        Ok(note)
    }

    /// Run one queued batch item: the same child execution a For Each does,
    /// reachable from outside the engine so a worker process can drive it.
    pub fn run_batch_item(
        &self,
        child: &str,
        vars: &std::collections::BTreeMap<String, String>,
        item: Option<&str>,
    ) -> Result<(), EngineError> {
        let subs: std::collections::HashMap<String, String> =
            vars.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        self.run_subpipeline_as(child, &subs, item)
    }

    /// Run a sub-pipeline under a name that also identifies the ITEM.
    ///
    /// `item` is the value of `ctl.foreach`'s `itemKey` column for this row. It
    /// makes the run `<child>@<item>` rather than plain `<child>`, which is what
    /// gives each iteration its own run log and - the reason it exists - its own
    /// `xf.incremental` watermark. Loading 400 tables through one child
    /// pipeline is 400 different loads; sharing one mark between them skips
    /// rows, silently.
    ///
    /// `None` keeps the whole child as one run, which is right when the
    /// iterations really are the same load and is the behaviour that predates
    /// `itemKey`.
    pub(crate) fn run_subpipeline_as(
        &self,
        path: &str,
        subs: &std::collections::HashMap<String, String>,
        item: Option<&str>,
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
        // What this job was handed flows on to whatever it runs, so a value named by a
        // caller still reaches a body lifted out further down. The call's own variables
        // win on a collision, since they are the more specific of the two.
        let merged: std::collections::HashMap<String, String> = {
            let inherited = self.inherited_subs.lock().unwrap_or_else(|e| e.into_inner());
            inherited.iter().map(|(k, v)| (k.clone(), v.clone())).chain(
                subs.iter().map(|(k, v)| (k.clone(), v.clone()))
            ).collect()
        };
        content = substitute_into_child(&content, &merged);
        let sub_doc: plan::PipelineDoc = serde_json::from_str(&content).map_err(|e| {
            EngineError::Config(format!("sub-pipeline: parse '{}': {}", path, e))
        })?;
        // Run it under the CHILD's own name. Unnamed, every sub-pipeline shared
        // one run-log folder and - far worse - one `xf.incremental` watermark
        // file per node id, so three different children driven by ctl.foreach
        // silently overwrote each other's marks and each resumed from whichever
        // ran last. The name comes from the child's own file, not the caller,
        // so the same child is the same run wherever it is invoked from.
        //
        // This does NOT separate the ITERATIONS of one child from each other:
        // 400 tables through one child pipeline still share its watermark,
        // because nothing here knows which property identifies an item. That
        // needs an explicit key on ctl.foreach and is deliberately not guessed
        // at - keying on ITER_INDEX would tie a watermark to a row's position
        // in the driving query, which changes when that query is reordered.
        let child_name = child_run_name(&resolved, item);
        // In scope only for this child's own run, so a sibling call does not inherit it.
        let previous = {
            let mut slot = self.inherited_subs.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::replace(&mut *slot, merged)
        };
        let result = match &child_name {
            Some(name) => self.execute_pipeline_named(&sub_doc, name),
            None => self.execute_pipeline(&sub_doc),
        };
        {
            let mut slot = self.inherited_subs.lock().unwrap_or_else(|e| e.into_inner());
            *slot = previous;
        }
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
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        let col_q = plan::quote_ident(&spec.column);
        let up_q = plan::quote_ident(&spec.from_view);
        let node_q = plan::quote_ident(&spec.node_id);

        let state_path = incremental_state_path(pipeline_name, &spec.node_id);
        let prior = state_path.as_deref().and_then(crate::read_state_snapshot);
        let saved = state_path
            .as_ref()
            .and_then(read_incremental_state)
            .or_else(|| inherited_incremental_state(pipeline_name, &spec.node_id));

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
                pending.push(crate::PendingWrite::state(
                    path,
                    serde_json::json!({
                        "column": spec.column,
                        "value": value,
                        "type": new_ty,
                    }),
                    prior,
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
        pending: &mut Vec<crate::PendingWrite>,
    ) -> Result<String, EngineError> {
        let path = spec.path.replace('\\', "/").replace('\'', "''");
        // This path builds its own ATTACH rather than reusing ducklake_attach,
        // so it needs the same DATA_PATH handling, or a Postgres-catalogued lake
        // would be readable everywhere except its change feed.
        let data_path = spec
            .data_path
            .as_deref()
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .map(|d| format!(", DATA_PATH '{}'", d.replace('\\', "/").replace('\'', "''")))
            .unwrap_or_default();
        let attach = format!(
            "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:{}' AS duckle_src (READ_ONLY{}); ",
            path, data_path
        );
        let node_q = plan::quote_ident(&spec.node_id);
        // Read the change feed via the global ducklake_table_changes(catalog,
        // schema, table, from, to): catalog + schema + table are passed as
        // separate args so an explicit (or non-default) schema resolves. The
        // catalog-method form duckle_src.table_changes('schema.table', ...)
        // mis-parses a schema-qualified name (the table is looked up literally
        // as "schema.table" and not found), and the schema manifest field
        // defaults to "main", so any schema-qualified CDC node hit that.
        let schema = spec
            .schema
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("main")
            .replace('\'', "''");
        let table = spec.table.replace('\'', "''");

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
        let prior = state_path.as_deref().and_then(crate::read_state_snapshot);
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
                    "{attach}CREATE OR REPLACE TABLE {node} AS SELECT * FROM ducklake_table_changes('duckle_src', '{schema}', '{table}', {cur}, {cur}) WHERE 1=0;",
                    attach = attach, node = node_q, schema = schema, table = table, cur = current,
                )
            };
            self.run(Some(db), &empty_sql, false)?;
            return Ok(format!(
                "ducklake-cdc: no new changes (snapshot {} -> {})",
                last, current
            ));
        }

        let materialize = format!(
            "{attach}CREATE OR REPLACE TABLE {node} AS SELECT * FROM ducklake_table_changes('duckle_src', '{schema}', '{table}', {last}, {cur}) WHERE snapshot_id > {last}{type_filter};",
            attach = attach,
            node = node_q,
            schema = schema,
            table = table,
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
            pending.push(crate::PendingWrite::state(
                path,
                serde_json::json!({ "snapshot_id": current }),
                prior,
            ));
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
        let response = if initial.get("data").is_some() {
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
        // Disambiguate duplicate result-column names (e.g. SELECT * over a join
        // where both tables have a STATUS column). Cells are positional, so we
        // suffix repeats (STATUS, STATUS_1, ...) and key the NDJSON object, the
        // read_json columns map, and the projection all on the unique name -
        // otherwise a duplicate struct key fails the read and the second cell
        // would silently overwrite the first.
        let mut used_names: std::collections::HashSet<String> = std::collections::HashSet::new();
        for c in row_type {
            // Bail rather than `continue` on a nameless column: the row data is
            // an array of cells positioned by the ORIGINAL column index, so
            // silently dropping one name would shift every later column name
            // onto the wrong cell. (Snowflake always names columns; this just
            // guarantees the name list stays index-aligned with the cells.)
            let Some(raw_name) = c.get("name").and_then(|n| n.as_str()) else {
                return Err(EngineError::Query(
                    "Snowflake rowType has a column with no name; cannot align result columns"
                        .into(),
                ));
            };
            let name = unique_column_name(raw_name, &mut used_names);
            let sf_type = c
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("text")
                .to_ascii_lowercase();
            let scale = c.get("scale").and_then(|s| s.as_i64()).unwrap_or(0);
            let precision = c.get("precision").and_then(|p| p.as_i64()).unwrap_or(38);
            let ident = plan::quote_ident(&name);
            columns_spec_parts.push(format!("'{}': 'VARCHAR'", name.replace('\'', "''")));
            select_parts.push(format!(
                "{} AS {}",
                snowflake_cast_expr(&ident, &sf_type, scale, precision),
                ident
            ));
            cols.push(name);
        }
        let columns_spec = columns_spec_parts.join(", ");
        let select_list = select_parts.join(", ");
        // Stream partitions into one NDJSON writer as they arrive instead of
        // accumulating the whole result set in an `all_data` Vec first - peak
        // memory drops from O(all partitions) to O(one partition) + the writer.
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let initial_rows = response
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut total_rows = initial_rows.len();
        write_arrayrows_to(&mut writer, &cols, &initial_rows)?;
        drop(initial_rows);
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
                    Some(rows) => {
                        total_rows += rows.len();
                        write_arrayrows_to(&mut writer, &cols, &rows)?;
                    }
                    None => {
                        return Err(EngineError::Query(format!(
                            "Snowflake partition {} returned no row data (unexpected response shape)",
                            i
                        )))
                    }
                }
            }
        }
        writer.finalize_typed(&self.bin, db, &spec.node_id, &columns_spec, &select_list)?;
        Ok(format!(
            "snowflake: materialized {} rows ({} partition(s)) into {}",
            total_rows,
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
        // Disambiguate duplicate result-column names (cells are positional, so
        // a SELECT * over a join that shares a column name would otherwise have
        // the second cell silently overwrite the first in the NDJSON object).
        let cols = dedupe_names(
            response
                .pointer("/manifest/schema/columns")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    EngineError::Query(
                        "Databricks response missing manifest.schema.columns".into(),
                    )
                })?
                .iter()
                .filter_map(|c| c.get("name").and_then(|n| n.as_str()).map(String::from))
                .collect::<Vec<_>>(),
        );
        // Stream each chunk into one NDJSON writer as it arrives instead of
        // accumulating the whole result in an `all_data` Vec first.
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let initial_rows = response
            .pointer("/result/data_array")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut total_rows = initial_rows.len();
        write_arrayrows_to(&mut writer, &cols, &initial_rows)?;
        drop(initial_rows);
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
                    total_rows += d.len();
                    write_arrayrows_to(&mut writer, &cols, d)?;
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
        writer.finalize_into_table(&self.bin, db, &spec.node_id)?;
        Ok(format!(
            "databricks: materialized {} rows ({} chunk(s)) into {}",
            total_rows,
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
/// The parts of an xf.dbt invocation that are the same for every dbt
/// subcommand: the resolved project dir, the target database (for reading a
/// built model back), the dbt binary, and the flags that follow the subcommand
/// (--project-dir / --profiles-dir / [--profile] / [--vars]).
struct DbtInvocation {
    project_dir: std::path::PathBuf,
    target_db: String,
    dbt_bin: String,
    shared_args: Vec<String>,
}

/// Prepare an xf.dbt run: scaffold (inline) or resolve the project, generate
/// profiles.yml, and build the flags shared by every dbt subcommand. Both the
/// real run and the #146 pre-warm parse go through this so they hand dbt an
/// identical project + vars - which is what keeps the pre-warm's partial-parse
/// cache valid for the run (dbt does a full re-parse when the vars change).
fn prepare_dbt_invocation(spec: &DbtSpec, db: &Path) -> Result<DbtInvocation, EngineError> {
    // Resolve the project directory. Inline mode (no project_dir) scaffolds an
    // ephemeral one-model project from spec.inline_model into a stable temp dir.
    let project_dir: std::path::PathBuf = match &spec.project_dir {
        Some(dir) => Path::new(dir).to_path_buf(),
        None => {
            let model = spec.inline_model.as_deref().ok_or_else(|| {
                EngineError::Config(
                    "xf.dbt: inline mode needs model SQL (or set projectDir)".into(),
                )
            })?;
            scaffold_inline_dbt_project(&spec.node_id, &spec.inline_model_name, model)
                .map_err(|e| EngineError::Query(format!("xf.dbt: scaffold inline project: {e}")))?
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
    // Name the generated profile after the project's `profile:` so the project
    // runs unmodified; fall back to "duckle" + an explicit --profile flag.
    let declared_profile = serde_yaml::from_str::<serde_yaml::Value>(&project_text)
        .ok()
        .and_then(|v| v.get("profile").and_then(|p| p.as_str().map(String::from)));
    let (profile_name, force_profile_flag) = match declared_profile {
        Some(p) if !p.trim().is_empty() => (p, false),
        _ => ("duckle".to_string(), true),
    };

    // Target database: the run db by default, so dbt composes with the rest of
    // the canvas. YAML wants forward slashes on Windows.
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
        "{}:\n  target: duckle\n  outputs:\n duckle:\n type: duckdb\n path: \"{}\"\n schema: {}\n threads: 1\n",
        profile_name, target_db_yaml, spec.schema
    );
    // write-if-changed: a rewritten profiles.yml would needlessly invalidate the
    // partial-parse cache between the pre-warm parse and the run.
    write_str_if_changed(&profiles_dir.join("profiles.yml"), &profiles_yaml)
        .map_err(|e| EngineError::Query(format!("xf.dbt: write profiles.yml: {}", e)))?;

    let mut shared_args: Vec<String> = vec![
        "--project-dir".into(),
        project_dir_str,
        "--profiles-dir".into(),
        profiles_dir.to_string_lossy().into_owned(),
    ];
    if force_profile_flag {
        shared_args.push("--profile".into());
        shared_args.push(profile_name);
    }
    // Expose the upstream tables to dbt: the first as var('duckle_input')
    // (back-compat / single-source) and ALL of them as the list
    // var('duckle_inputs') for multi-source inline models.
    if !spec.from_views.is_empty() {
        shared_args.push("--vars".into());
        shared_args.push(
            serde_json::json!({
                "duckle_input": spec.from_views.first(),
                "duckle_inputs": spec.from_views,
            })
            .to_string(),
        );
    } else if let Some(fv) = &spec.from_view {
        shared_args.push("--vars".into());
        shared_args.push(serde_json::json!({ "duckle_input": fv }).to_string());
    }

    Ok(DbtInvocation {
        project_dir,
        target_db,
        dbt_bin: resolve_dbt_bin(spec.dbt_bin.as_deref()),
        shared_args,
    })
}

/// Spawn a dbt process, drain both pipes on reader threads, and poll for
/// completion honouring `cancel` + an optional deadline (the pipe-drain
/// discipline run_shell uses so chatty dbt logs never fill the OS pipe buffer).
/// Returns (exit status, stdout, stderr). Shared by the real run and the #146
/// pre-warm parse.
fn spawn_dbt_and_wait(
    dbt_bin: &str,
    args: &[String],
    cwd: &Path,
    cancel: &std::sync::atomic::AtomicBool,
    timeout_ms: Option<u64>,
) -> Result<(std::process::ExitStatus, String, String), EngineError> {
    use std::io::Read;
    let mut cmd = std::process::Command::new(dbt_bin);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd.args(args);
    cmd.current_dir(cwd);
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
    let deadline =
        timeout_ms.map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) => {}
            Err(e) => {
                let _ = child.kill();
                return Err(EngineError::Query(format!("xf.dbt: wait: {}", e)));
            }
        }
        if cancel.load(Ordering::Relaxed) {
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
                    timeout_ms.unwrap_or(0)
                )));
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    let stdout_text =
        String::from_utf8_lossy(&stdout_reader.join().unwrap_or_default()).into_owned();
    let stderr_text =
        String::from_utf8_lossy(&stderr_reader.join().unwrap_or_default()).into_owned();
    Ok((status, stdout_text, stderr_text))
}

/// #146: warm dbt's partial-parse cache by running `dbt parse` with the exact
/// project + vars the upcoming run will use, so the run skips a cold parse. The
/// run loop starts this in the background while upstream stages execute, then
/// joins it before the dbt stage runs (so the two dbt processes never write the
/// project's target/ dir at the same time). Best-effort and silent: any error
/// (dbt missing, a parse failure, or a cancel) just leaves the run to parse
/// itself, exactly as before. `dbt parse` never opens the run database.
pub(crate) fn prewarm_dbt(cancel: &std::sync::atomic::AtomicBool, db: &Path, spec: &DbtSpec) {
    let inv = match prepare_dbt_invocation(spec, db) {
        Ok(i) => i,
        Err(_) => return,
    };
    let mut args = vec!["parse".to_string()];
    args.extend(inv.shared_args);
    // Bounded so a stuck parse can't outlive the run it was meant to speed up.
    let timeout = spec.timeout_ms.or(Some(120_000));
    let _ = spawn_dbt_and_wait(&inv.dbt_bin, &args, &inv.project_dir, cancel, timeout);
}

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

/// Resolve the duckle-lance sidecar. Order: DUCKLE_LANCE_BIN env -> a binary
/// bundled next to the running executable (the shipped sidecar) -> `duckle-lance`
/// on PATH. The sidecar owns lancedb so its arrow 58 / DataFusion / protoc cost
/// stays out of the engine.
fn resolve_lance_bin() -> String {
    if let Ok(env) = std::env::var("DUCKLE_LANCE_BIN") {
        if !env.is_empty() && Path::new(&env).exists() {
            return env;
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            for name in ["duckle-lance", "duckle-lance.exe"] {
                let p = dir.join(name);
                if p.exists() {
                    return p.to_string_lossy().into_owned();
                }
            }
        }
    }
    "duckle-lance".to_string()
}

/// Whether a script asks for the whole table rather than a row at a time.
///
/// The entry point IS the mode: a script defining `transform` is handed the table,
/// one defining `process` keeps the row-at-a-time behaviour it always had. Nothing to
/// set, and every saved pipeline goes on working unchanged.
pub(crate) fn defines_vectorized_entry(script: &str) -> bool {
    script.lines().any(|l| {
        let t = l.trim_start();
        // Only a definition at the top level of the script counts. `def transform` nested
        // inside something else is a helper, not the entry point the harness calls.
        l.starts_with("def transform(") || (t == l && t.starts_with("def transform("))
    })
}

/// A script defining `transform_batches` is streamed a RecordBatch at a time
/// rather than handed the whole table (#245).
///
/// `defines_vectorized_entry` cannot be reused: it matches `def transform(`
/// including the paren, so `def transform_batches(` is not a false positive
/// there - but it does mean the two are independent checks, and streaming is
/// tested first because a script may reasonably define both.
pub(crate) fn defines_streaming_entry(script: &str) -> bool {
    script.lines().any(|l| {
        let t = l.trim_start();
        l.starts_with("def transform_batches(") || (t == l && t.starts_with("def transform_batches("))
    })
}

/// Temp file paths (input JSON, output JSON, harness script) for a code.python
/// stage, unique to this run. (#203)
///
/// db_path is unique per run - `duckle_run_<pid>_<nanos>_<seq>.duckdb` - but
/// `with_file_name` keeps only the directory and drops that unique name, so
/// `py-in-<node>.json` collapsed to one shared `<temp_dir>/py-in-<node>.json`
/// across every run in the process. Two runs of the same node at once (a
/// parallel foreach, concurrent scheduled runs, parallel tests) then read and
/// wrote each other's input, script and output. Folding the run's db filename
/// back in restores that uniqueness, exactly as the sibling ADBC / lance /
/// vortex temp parquet paths already do with their `<db_name>.` prefix.
fn python_temp_paths(db: &Path, node_id: &str) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let safe: String = node_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect();
    let stem = db
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    (
        db.with_file_name(format!("{}.py-in-{}.json", stem, safe)),
        db.with_file_name(format!("{}.py-out-{}.json", stem, safe)),
        db.with_file_name(format!("{}.py-{}.py", stem, safe)),
    )
}

/// Resolve the Python 3 interpreter for code.python. Order: DUCKLE_PYTHON_BIN env
/// (e.g. a venv) -> `python` on Windows / `python3` on Unix, found on PATH.
fn resolve_python_bin() -> String {
    if let Ok(env) = std::env::var("DUCKLE_PYTHON_BIN") {
        if !env.trim().is_empty() {
            return env;
        }
    }
    // A workspace can carry its own interpreter, which is what makes a Python stage
    // reproducible between a laptop, CI and a headless runner: the packages a pipeline
    // needs are pinned beside the pipeline instead of being whatever the machine
    // happens to have. `uv venv` and `python -m venv` both produce this layout, so this
    // works with uv without depending on it, and nothing is installed at run time -
    // which keeps an air-gapped box air-gapped.
    if let Ok(ws) = std::env::var("DUCKLE_WORKSPACE") {
        if !ws.trim().is_empty() {
            if let Some(p) = python_in_workspace(Path::new(&ws)) {
                return p;
            }
        }
    }
    if cfg!(windows) {
        "python".to_string()
    } else {
        "python3".to_string()
    }
}

/// The interpreter inside a workspace's own virtual environment, if it has one.
///
/// Split from the environment lookup so it can be tested against a folder rather than
/// by setting a variable the rest of the suite can see.
pub(crate) fn python_in_workspace(ws: &Path) -> Option<String> {
    for rel in [
        // What uv and the stdlib venv module produce, on either platform.
        "Scripts/python.exe",
        "bin/python3",
        "bin/python",
    ] {
        let p = ws.join(".venv").join(rel);
        if p.is_file() {
            return Some(p.to_string_lossy().into_owned());
        }
    }
    None
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

/// Map an ODBC column data type to the DuckDB type the Teradata source should
/// TRY_CAST it to. `None` means "leave it as VARCHAR" (no cast) - for char /
/// binary / unknown types whose ODBC text rendering is already what we want.
/// Decimals keep their precision/scale (clamped to DuckDB's max of 38).

/// Backtick-quote a Cypher identifier. Labels and property keys cannot be
/// bound as parameters, so they are interpolated - which makes escaping the
/// only thing standing between a label like `` a`b `` and a broken query.
fn cypher_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// A Query API request with optional Basic auth. Neo4j accepts the same
/// user/password pair for a self-hosted server and for Aura.
fn neo4j_request(user: Option<&str>, password: Option<&str>, url: &str) -> ureq::Request {
    let mut req = crate::tls::http_agent()
        .post(url)
        .set("Content-Type", "application/json")
        .set("Accept", "application/json");
    if let Some(u) = user {
        use base64::engine::general_purpose::STANDARD as B64;
        use base64::Engine as _;
        let creds = B64.encode(format!("{}:{}", u, password.unwrap_or("")));
        req = req.set("Authorization", &format!("Basic {}", creds));
    }
    req
}

/// Pull the human-readable message out of a Query API error body. Neo4j
/// reports Cypher problems in an `errors` array; without this the user would
/// see only the status code, which never says which clause was wrong.
fn neo4j_error_detail(text: String) -> String {
    serde_json::from_str::<JsonValue>(&text)
        .ok()
        .and_then(|v| {
            let e = v.get("errors")?.as_array()?.first()?;
            e.get("message")
                .or_else(|| e.get("error"))?
                .as_str()
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| text.chars().take(300).collect())
}

/// Normalize a Turso database URL for HTTP. Turso hands out `libsql://` URLs,
/// which are the same host over HTTPS - rejecting them would mean every user
/// had to rewrite the URL the dashboard gave them.
fn turso_base_url(url: &str) -> String {
    let trimmed = url.trim().trim_end_matches('/');
    match trimmed.strip_prefix("libsql://") {
        Some(rest) => format!("https://{}", rest),
        None => trimmed.to_string(),
    }
}

/// POST a pipeline request and check it for statement-level failures.
///
/// The pipeline endpoint answers HTTP 200 even when a statement failed - the
/// failure is an `error` entry inside `results` - so without this check a
/// broken query would look like an empty table and a failed INSERT would look
/// like a successful write.
fn turso_send(
    auth_token: Option<&str>,
    url: &str,
    body: JsonValue,
) -> Result<JsonValue, EngineError> {
    let mut req = crate::tls::http_agent()
        .post(url)
        .set("Content-Type", "application/json");
    if let Some(t) = auth_token {
        req = req.set("Authorization", &format!("Bearer {}", t));
    }
    let resp = match req.send_json(body) {
        Ok(r) => r,
        Err(ureq::Error::Status(code, r)) => {
            let text = r.into_string().unwrap_or_default();
            return Err(EngineError::Query(format!(
                "turso: HTTP {}: {}",
                code,
                text.chars().take(300).collect::<String>()
            )));
        }
        Err(e) => return Err(EngineError::Query(format!("turso: HTTP transport: {}", e))),
    };
    let v: JsonValue = resp
        .into_json()
        .map_err(|e| EngineError::Query(format!("turso: response not JSON: {}", e)))?;
    if let Some(results) = v.get("results").and_then(|r| r.as_array()) {
        for r in results {
            if r.get("type").and_then(|t| t.as_str()) == Some("error") {
                let msg = r
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown error");
                return Err(EngineError::Query(format!("turso: {}", msg)));
            }
        }
    }
    Ok(v)
}

/// Decode one libSQL cell into plain JSON.
///
/// libSQL sends integers as STRINGS (`{"type":"integer","value":"42"}`) so
/// that 64-bit values survive JSON's double-precision numbers. Passing that
/// through untouched would make every integer column arrive as VARCHAR, so
/// it is parsed back to a number here - and left as text if it genuinely
/// does not fit an i64, which loses nothing.
fn turso_cell_to_json(cell: &JsonValue) -> JsonValue {
    let ty = cell.get("type").and_then(|v| v.as_str()).unwrap_or("null");
    match ty {
        "null" => JsonValue::Null,
        "integer" => match cell.get("value") {
            Some(JsonValue::String(s)) => match s.parse::<i64>() {
                Ok(n) => JsonValue::Number(n.into()),
                Err(_) => JsonValue::String(s.clone()),
            },
            Some(other) => other.clone(),
            None => JsonValue::Null,
        },
        "float" => cell.get("value").cloned().unwrap_or(JsonValue::Null),
        "text" => cell.get("value").cloned().unwrap_or(JsonValue::Null),
        // Blobs come back base64-encoded under `base64`, not `value`.
        "blob" => cell
            .get("base64")
            .or_else(|| cell.get("value"))
            .cloned()
            .unwrap_or(JsonValue::Null),
        _ => cell.get("value").cloned().unwrap_or(JsonValue::Null),
    }
}

/// Encode a JSON cell as a libSQL bound argument. The mirror of
/// `turso_cell_to_json`: integers go up as strings, and SQLite has no boolean
/// or nested types, so those become 1/0 and JSON text respectively.
fn json_to_turso_arg(v: &JsonValue) -> JsonValue {
    match v {
        JsonValue::Null => serde_json::json!({ "type": "null" }),
        JsonValue::Bool(b) => {
            serde_json::json!({ "type": "integer", "value": if *b { "1" } else { "0" } })
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::json!({ "type": "integer", "value": i.to_string() })
            } else {
                serde_json::json!({ "type": "float", "value": n.as_f64().unwrap_or(0.0) })
            }
        }
        JsonValue::String(s) => serde_json::json!({ "type": "text", "value": s }),
        JsonValue::Array(_) | JsonValue::Object(_) => serde_json::json!({
            "type": "text",
            "value": serde_json::to_string(v).unwrap_or_else(|_| "null".into())
        }),
    }
}

/// Map a DuckDB column type to a SQLite storage class, for auto-creating a
/// Turso table. SQLite has no boolean, date or decimal type: booleans land in
/// INTEGER as 1/0 and everything textual or temporal stays TEXT, which keeps
/// the ISO form readable and sortable.
fn duckdb_type_to_sqlite(t: &str) -> String {
    let up = t.trim().to_ascii_uppercase();
    match up.as_str() {
        "BOOLEAN" | "BOOL" | "TINYINT" | "UTINYINT" | "SMALLINT" | "INT2" | "USMALLINT"
        | "INTEGER" | "INT" | "INT4" | "UINTEGER" | "BIGINT" | "INT8" | "UBIGINT" => "INTEGER",
        "REAL" | "FLOAT" | "FLOAT4" | "DOUBLE" | "FLOAT8" => "REAL",
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => "BLOB",
        _ => "TEXT",
    }
    .to_string()
}

#[cfg(feature = "odbc")]
fn odbc_type_to_duckdb(dt: &odbc_api::DataType) -> Option<String> {
    use odbc_api::DataType as D;
    match dt {
        D::TinyInt => Some("TINYINT".into()),
        D::SmallInt => Some("SMALLINT".into()),
        D::Integer => Some("INTEGER".into()),
        D::BigInt => Some("BIGINT".into()),
        D::Real => Some("REAL".into()),
        D::Float { .. } | D::Double => Some("DOUBLE".into()),
        D::Decimal { precision, scale } | D::Numeric { precision, scale } => {
            let p = (*precision).clamp(1, 38);
            let s = ((*scale).max(0) as usize).min(p);
            Some(format!("DECIMAL({},{})", p, s))
        }
        D::Bit => Some("BOOLEAN".into()),
        D::Date => Some("DATE".into()),
        D::Time { .. } => Some("TIME".into()),
        D::Timestamp { .. } => Some("TIMESTAMP".into()),
        _ => None,
    }
}

/// Return a column name not already present in `used`, suffixing repeats as
/// `name_1`, `name_2`, ... Result-set cells are positional, so two columns that
/// share a name (e.g. SELECT * over a join) must be keyed uniquely or the
/// second cell silently overwrites the first (DuckDB also rejects a duplicate
/// struct key in read_json's columns map). Records the chosen name in `used`.
fn unique_column_name(raw: &str, used: &mut std::collections::HashSet<String>) -> String {
    let mut name = raw.to_string();
    if used.contains(&name) {
        let mut k = 1usize;
        loop {
            let cand = format!("{}_{}", raw, k);
            if !used.contains(&cand) {
                name = cand;
                break;
            }
            k += 1;
        }
    }
    used.insert(name.clone());
    name
}

/// Disambiguate a whole list of column names in order, suffixing duplicates.
fn dedupe_names(names: Vec<String>) -> Vec<String> {
    let mut used: std::collections::HashSet<String> = std::collections::HashSet::new();
    names
        .into_iter()
        .map(|n| unique_column_name(&n, &mut used))
        .collect()
}

/// The folder an unnamed run falls back to.
///
/// Every sub-pipeline used to land here, because the sub-pipeline runner called
/// `execute_pipeline` and that forwards no name. So a workspace running three
/// different children through `ctl.foreach` had all three sharing one watermark
/// file per node id, and each overwrote the others.
const UNNAMED_RUN_FOLDER: &str = "pipeline";

/// Put a child pipeline's variables in, exactly as a run would.
///
/// Shared by the runner and by the batch safety check, so what the check
/// inspects is what will actually execute. A second implementation here would
/// be a check of something nobody runs.
///
/// Workspace context variables are merged in first and the caller's
/// substitutions win on a collision, so a per-row `${ITER_ITEM_*}` beats a
/// workspace-wide value of the same name.
pub(crate) fn substitute_into_child(
    content: &str,
    subs: &std::collections::HashMap<String, String>,
) -> String {
    let mut merged = workspace_context_vars();
    for (k, v) in subs {
        merged.insert(k.clone(), v.clone());
    }
    let mut content = content.to_string();
    for (key, val) in &merged {
        let placeholder = format!("${{{}}}", key);
        if content.contains(&placeholder) {
            // JSON-escape the value before substitution so embedded quotes /
            // backslashes don't break parsing.
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
    content
}

/// The name a sub-pipeline run goes under: `<child>` or `<child>@<item>`.
///
/// Split out so a test drives the real construction rather than a restatement
/// of it. The name decides the run-log folder AND the `xf.incremental`
/// watermark path, so getting it wrong is a data bug, not a cosmetic one.
fn child_run_name(resolved_path: &str, item: Option<&str>) -> Option<String> {
    let stem = std::path::Path::new(resolved_path)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())?;
    match item.map(str::trim).filter(|i| !i.is_empty()) {
        Some(item) => Some(format!("{stem}@{item}")),
        None => Some(stem),
    }
}

/// The watermark a newly-named run should start from, when it has none yet.
///
/// Naming sub-pipeline runs moves their state from `state/pipeline/<node>.json`
/// to `state/<child>/<node>.json`. Without this, the first run after that change
/// would find no state, fall back to `initialValue`, and re-load the source from
/// the beginning - which on a large incremental sync is a very expensive
/// surprise for what is meant to be a bug fix.
///
/// So a named run with no state of its own inherits the old shared value once,
/// then writes its own from then on. That is not a guess about which child the
/// shared value belonged to: whichever child wrote it last, every child was
/// already reading exactly this number, so inheriting it is precisely today's
/// behaviour, and every child diverges correctly from the next run onward.
fn inherited_incremental_state(
    pipeline_name: Option<&str>,
    node_id: &str,
) -> Option<(String, String)> {
    // Only a NAMED run can inherit; an unnamed one already reads that file.
    pipeline_name?;
    let legacy = incremental_state_path(None, node_id)?;
    let state = read_incremental_state(&legacy)?;
    eprintln!(
        "duckle: {} has no watermark yet, so it inherited the one at {} \
         (written before sub-pipeline runs were named). It keeps its own from now on.",
        node_id,
        legacy.display()
    );
    Some(state)
}

fn incremental_state_path(pipeline_name: Option<&str>, node_id: &str) -> Option<std::path::PathBuf> {
    let ws = std::env::var("DUCKLE_WORKSPACE").ok().filter(|s| !s.is_empty())?;
    let folder = sanitize_path_segment(pipeline_name.unwrap_or(UNNAMED_RUN_FOLDER));
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

/// Split a Confluent-framed Kafka message into its schema id and Avro payload.
///
/// The framing is a zero magic byte, a big-endian u32 schema id, then the raw
/// Avro datum - note DATUM, not a container file: there is no embedded schema,
/// which is exactly why the id has to be resolved against a registry.
///
/// Anything not carrying that frame returns None and is left as text. That is
/// deliberate rather than lax: Confluent topics routinely pair a plain string
/// key with an Avro value, so refusing an unframed key would break the common
/// case. A zero first byte is not valid UTF-8 text, so this cannot misfire on a
/// string.
pub(crate) fn confluent_envelope(bytes: &[u8]) -> Option<(u32, &[u8])> {
    if bytes.len() < 5 || bytes[0] != 0 {
        return None;
    }
    let id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    Some((id, &bytes[5..]))
}

/// Decode one raw Avro datum against a schema and render it as JSON.
pub(crate) fn avro_datum_to_json(
    schema: &apache_avro::Schema,
    payload: &[u8],
) -> Result<JsonValue, String> {
    let mut cursor = payload;
    let value = apache_avro::from_avro_datum(schema, &mut cursor, None)
        .map_err(|e| format!("avro decode: {}", e))?;
    JsonValue::try_from(value).map_err(|e| format!("avro value to json: {}", e))
}

/// Fetch a writer schema from a Confluent Schema Registry by id.
///
/// Goes through the engine's shared agent, so a registry behind a corporate CA
/// or a proxy is reached the same way every other HTTPS call is. Credentials in
/// the URL (https://user:pass@host) are honoured by the agent.
fn fetch_registry_schema(
    agent: &ureq::Agent,
    registry: &str,
    id: u32,
) -> Result<apache_avro::Schema, String> {
    let url = format!("{}/schemas/ids/{}", registry.trim_end_matches('/'), id);
    let body: JsonValue = match agent.get(&url).call() {
        Ok(r) => r
            .into_json()
            .map_err(|e| format!("schema registry {}: response was not JSON: {}", url, e))?,
        Err(ureq::Error::Status(code, r)) => {
            let text = r.into_string().unwrap_or_default();
            return Err(format!(
                "schema registry {}: HTTP {}: {}",
                url,
                code,
                text.chars().take(200).collect::<String>()
            ));
        }
        Err(e) => return Err(format!("schema registry {}: {}", url, e)),
    };
    let text = body
        .get("schema")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("schema registry {}: no \"schema\" field in the response", url))?;
    apache_avro::Schema::parse_str(text)
        .map_err(|e| format!("schema registry {}: schema {} does not parse: {}", url, id, e))
}

/// Turn a mechanism name into the SASL config rskafka wants.
///
/// Only the mechanisms rskafka implements are accepted. An unrecognised one is
/// an error naming what IS supported, rather than a silent downgrade to an
/// unauthenticated connection - which is what happened while nothing read
/// these fields at all.
fn kafka_sasl_config(sasl: &plan::KafkaSasl) -> Result<rskafka::client::SaslConfig, String> {
    let creds = rskafka::client::Credentials::new(sasl.username.clone(), sasl.password.clone());
    // Accept the punctuation people actually type: SCRAM-SHA-256, scram_sha_256.
    let m = sasl
        .mechanism
        .to_ascii_uppercase()
        .replace('_', "-")
        .replace(' ', "");
    match m.as_str() {
        "PLAIN" => Ok(rskafka::client::SaslConfig::Plain(creds)),
        "SCRAM-SHA-256" => Ok(rskafka::client::SaslConfig::ScramSha256(creds)),
        "SCRAM-SHA-512" => Ok(rskafka::client::SaslConfig::ScramSha512(creds)),
        other => Err(format!(
            "kafka: SASL mechanism '{}' is not supported; use PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512",
            other
        )),
    }
}

/// Apply a node's transport security to a Kafka client builder.
///
/// TLS reuses the engine's shared trust config - the merged OS store plus the
/// bundled roots - so a broker behind a corporate CA works the same way every
/// other TLS connection in Duckle does.
fn kafka_client_builder(
    bootstrap: Vec<String>,
    tls: bool,
    sasl: Option<&plan::KafkaSasl>,
) -> Result<rskafka::client::ClientBuilder, String> {
    let mut builder = rskafka::client::ClientBuilder::new(bootstrap);
    if tls {
        builder = builder.tls_config(std::sync::Arc::new(crate::tls::build_client_config()));
    }
    if let Some(s) = sasl {
        builder = builder.sasl_config(kafka_sasl_config(s)?);
    }
    Ok(builder)
}

/// One remote object's identity, as far as a metadata probe can see it.
#[derive(Debug, Clone)]
pub(crate) struct RemoteEntry {
    pub uri: String,
    pub name: String,
    pub size: Option<i64>,
    pub modified_at: Option<String>,
    pub etag: Option<String>,
    pub fingerprint: String,
}

/// Combine whatever signals the protocol gave into one comparable string.
///
/// Conservative on purpose. None of these are guarantees: an ETag can be
/// absent, can weaken under compression, and on S3 is a digest-of-digests for
/// a multipart upload rather than the object's hash; Last-Modified has
/// one-second resolution; SFTP gives mtime and size. When NOTHING usable came
/// back the fingerprint is unique per call, so the object reads as changed and
/// gets processed. Re-reading something unnecessarily costs compute; skipping
/// something that did change loses data, and nothing would report it.
pub fn remote_fingerprint(
    etag: Option<&str>,
    modified: Option<&str>,
    size: Option<i64>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(e) = etag.map(str::trim).filter(|s| !s.is_empty()) {
        parts.push(format!("etag={}", e));
    }
    if let Some(m) = modified.map(str::trim).filter(|s| !s.is_empty()) {
        parts.push(format!("mtime={}", m));
    }
    if let Some(sz) = size {
        parts.push(format!("size={}", sz));
    }
    if parts.is_empty() {
        // Nothing to compare. Treat as changed rather than as unchanged.
        return format!(
            "unknown-{}-{}",
            std::process::id(),
            CHANGED_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
    }
    parts.join(" ")
}

static CHANGED_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Sequence for tumble buffer filenames, so two runs in one process never pick
/// the same name.
static TUMBLE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Delete buffer files that no longer matter: anything that is neither the one
/// this run just wrote nor the one the last SUCCESSFUL run pointed at. The
/// previous buffer has to survive until this run commits, or a failure would
/// leave nothing authoritative behind.
fn prune_tumble_buffers(dir: &std::path::Path, keep: &str, prev: Option<&std::path::Path>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("buf-") || !name.ends_with(".parquet") {
            continue;
        }
        if name == keep || prev.map(|p| p == path.as_path()).unwrap_or(false) {
            continue;
        }
        let _ = std::fs::remove_file(&path);
    }
}

/// The last complete JSON line in a file, used to learn a spool's column shape
/// when a pass has nothing new. Reads only the tail rather than the whole file,
/// which matters when a listener has been running for a long time.
fn last_complete_json_line(path: &std::path::Path) -> Option<JsonValue> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let window = len.min(256 * 1024);
    f.seek(SeekFrom::Start(len - window)).ok()?;
    let mut buf = vec![0u8; window as usize];
    f.read_exact(&mut buf).ok()?;
    let text = String::from_utf8_lossy(&buf);
    text.lines()
        .rev()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .find_map(|l| serde_json::from_str::<JsonValue>(l).ok())
}

/// Read a saved spool byte offset. Missing or malformed reads as "start".
fn read_spool_offset_state(path: &std::path::Path) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    let v: JsonValue = serde_json::from_str(&text).ok()?;
    v.get("next_offset").and_then(|x| x.as_u64())
}

/// Read a saved Kafka resume point.
///
/// The offset is only meaningful for the topic and partition it was written
/// for. Point the node at a different topic, or a different partition, and the
/// number means something else entirely - so a mismatch reads as "no saved
/// offset" and the node falls back to its configured start rather than
/// resuming at a position from another stream.
fn read_kafka_offset_state(path: &std::path::Path, topic: &str, partition: i32) -> Option<i64> {
    let text = std::fs::read_to_string(path).ok()?;
    let v: JsonValue = serde_json::from_str(&text).ok()?;
    if v.get("topic").and_then(|x| x.as_str()) != Some(topic) {
        return None;
    }
    if v.get("partition").and_then(|x| x.as_i64()) != Some(partition as i64) {
        return None;
    }
    v.get("next_offset")
        .and_then(|x| x.as_i64())
        .filter(|o| *o >= 0)
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
pub(crate) fn sanitize_path_segment(name: &str) -> String {
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

/// Wrap one upstream row as a Salesforce sObject Collections record: prepend
/// the mandatory `attributes: {type: <object>}` envelope, then copy the row's
/// fields. Null cells are kept (Salesforce treats an explicit null as a field
/// clear on update/upsert). Nested object/array cells are passed through as-is;
/// compound-field handling (Address, Location) is Tier 2.
fn salesforce_record_envelope(row: &JsonValue, object: &str) -> JsonValue {
    let mut rec = serde_json::Map::new();
    let mut attrs = serde_json::Map::new();
    attrs.insert("type".into(), JsonValue::String(object.to_string()));
    rec.insert("attributes".into(), JsonValue::Object(attrs));
    if let Some(obj) = row.as_object() {
        for (k, v) in obj {
            // Guard against a stray upstream "attributes" column shadowing ours.
            if k == "attributes" {
                continue;
            }
            rec.insert(k.clone(), v.clone());
        }
    }
    JsonValue::Object(rec)
}

/// Bulk API 2.0 accepts up to 150 MB of *base64-encoded* CSV per job. The
/// upload is base64-encoded server-side, which inflates raw CSV by ~33-50%, so
/// Salesforce's own guidance is to keep the raw upload under 100 MB. DuckDB's
/// FILE_SIZE_BYTES is a soft cap (it only flushes on row-group boundaries and
/// overshoots by a few percent), so we target 90 MB per part and still hard-
/// check each part against the 100 MB line before uploading. Do NOT "simplify"
/// these to 150 - the 150 is a post-base64 number, not a raw-CSV one.
const BULK_SPLIT_TARGET_BYTES: u64 = 90 * 1024 * 1024;
const BULK_UPLOAD_MAX_BYTES: u64 = 100 * 1024 * 1024;

/// Runaway backstop on the Bulk query result walk, NOT a tunable: at the
/// server's 1000-record page floor this is 50 billion records, far beyond any
/// legitimate extract. The walk's real guard is the non-advancing-locator
/// check; this cap only bounds a peer that keeps minting fresh locators.
const BULK_QUERY_MAX_PAGES: u64 = 50_000_000;

/// Terminal snapshot of a Bulk API 2.0 ingest job.
struct BulkJobStatus {
    /// "JobComplete" | "Failed" | "Aborted".
    state: String,
    records_processed: u64,
    records_failed: u64,
    /// Job-level failure reason (empty unless the job Failed early).
    error_message: String,
}

/// Removes a directory tree when dropped, so a Bulk run's temp CSV parts never
/// leak on any exit path (success, error, or cancel).
struct ScopedDir(std::path::PathBuf);

impl Drop for ScopedDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Create a directory only its owner can enter. A Bulk run stages the full
/// upstream payload as plaintext CSV parts here; on Unix a 0700 dir stops other
/// local users from traversing in and reading them under a shared temp dir
/// during the upload window (the CSV files themselves inherit the umask, but the
/// dir's missing group/other execute bit blocks access to them). On non-Unix
/// platforms there is no equivalent umask exposure, so this just creates the dir.
#[cfg(unix)]
fn create_private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(dir)
}
#[cfg(not(unix))]
fn create_private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

/// Read a Bulk API response body, turning an HTTP status or transport error
/// into a descriptive EngineError. A 2xx with no body yields an empty string.
fn bulk_read_body(
    resp: Result<ureq::Response, ureq::Error>,
    url: &str,
    what: &str,
) -> Result<String, EngineError> {
    match resp {
        Ok(r) => Ok(r.into_string().unwrap_or_default()),
        Err(ureq::Error::Status(code, r)) => {
            let b = r.into_string().unwrap_or_default();
            Err(EngineError::Query(format!(
                "salesforce bulk {}: HTTP {} from {}: {}",
                what,
                code,
                url,
                tail_chars(&b, 300)
            )))
        }
        Err(e) => Err(EngineError::Query(format!(
            "salesforce bulk {}: transport to {}: {}",
            what, url, e
        ))),
    }
}

/// Whether a staged Bulk result CSV holds at least one data row, i.e. any
/// non-blank content beyond the header line.
///
/// This is the authority on whether a query returned records. The
/// Sforce-NumberOfRecords response header is optional and non-standard, so a
/// proxy that strips it would otherwise make a fully staged extract look empty.
/// Reads only as far as the first data byte after the header, so it costs
/// nothing on a multi-GB file.
fn result_csv_has_data_rows(path: &Path) -> bool {
    use std::io::BufRead;
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return false, // never created: nothing was fetched
    };
    let mut reader = std::io::BufReader::new(file);
    let mut line = String::new();
    // Header.
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return false;
    }
    // First non-blank line after it.
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return false,
            Ok(_) => {
                if !line.trim().is_empty() {
                    return true;
                }
            }
            Err(_) => return false,
        }
    }
}

/// Append one job's CSV result set to a per-run file, streaming - a result set
/// can be ~100 MB, so it is never buffered whole. The first body written to a
/// given file keeps its whole content (header + rows); every later body strips
/// the header line and appends only data rows, so the accumulated file has
/// exactly one header. The header decision is made per file from its current
/// length, not from the part index, so a result set skipped on an earlier part
/// (a transient fetch error left the file uncreated) never leaves a later part
/// writing a headerless file.
fn append_bulk_result_csv(
    path: &std::path::Path,
    body: impl std::io::Read,
) -> std::io::Result<()> {
    use std::io::BufRead;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    // Empty file (just created, or created earlier with nothing written) => this
    // is the first body for it, so keep the header; otherwise strip it.
    let first = f.metadata()?.len() == 0;
    let mut reader = std::io::BufReader::new(body);
    if !first {
        // Drop the header line; the rest is data (empty when the job had none).
        // read_until (not read_line) so a non-UTF-8 byte can't error the copy.
        let mut header = Vec::new();
        reader.read_until(b'\n', &mut header)?;
    }
    std::io::copy(&mut reader, &mut f)?;
    Ok(())
}

/// Per-record outcome of one Salesforce Collections request (#166
/// resultsPath). `status_code` / `message` stay empty on success; `id` is the
/// created/updated record Id when Salesforce returned one.
struct SfRecordResult {
    success: bool,
    id: Option<String>,
    status_code: String,
    message: String,
}

impl SfRecordResult {
    fn failure(status_code: &str, message: String) -> Self {
        SfRecordResult {
            success: false,
            id: None,
            status_code: status_code.into(),
            message,
        }
    }

    /// "CODE: message" for run feedback, or just the message when there is no
    /// statusCode (API-level / transport failures).
    fn error_line(&self) -> String {
        if self.status_code.is_empty() {
            self.message.clone()
        } else {
            format!("{}: {}", self.status_code, self.message)
        }
    }
}

/// Parse a Salesforce composite/sobjects response body - an array of
/// `{id, success, errors: [{statusCode, message, fields}]}` - into one
/// SfRecordResult per submitted record, positionally aligned with the request
/// chunk. A non-array / unparseable body (e.g. an API-level error object)
/// fails all `expected` records with its message, so the caller doesn't
/// silently treat a broken batch as success; a short array pads the tail with
/// MISSING_RESULT failures.
fn parse_salesforce_results(body: &str, expected: usize) -> Vec<SfRecordResult> {
    let all_failed = |code: &str, msg: String| -> Vec<SfRecordResult> {
        (0..expected)
            .map(|_| SfRecordResult::failure(code, msg.clone()))
            .collect()
    };
    let parsed: JsonValue = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return all_failed(
                "UNPARSEABLE_RESPONSE",
                format!("unparseable response: {}", tail_chars(body, 200)),
            )
        }
    };
    let arr = match parsed.as_array() {
        Some(a) => a,
        None => {
            // API-level error shape: [{message, errorCode}] is an array, so a
            // bare object here is an unexpected/error envelope.
            let msg = parsed
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unexpected non-array response");
            return all_failed("API_ERROR", msg.to_string());
        }
    };
    let mut out: Vec<SfRecordResult> = arr
        .iter()
        .map(|item| {
            let success = item.get("success").and_then(|s| s.as_bool()).unwrap_or(false);
            let id = item
                .get("id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            if success {
                return SfRecordResult {
                    success: true,
                    id,
                    status_code: String::new(),
                    message: String::new(),
                };
            }
            let (status_code, message) = item
                .get("errors")
                .and_then(|e| e.as_array())
                .and_then(|a| a.first())
                .map(|e| {
                    (
                        e.get("statusCode").and_then(|c| c.as_str()).unwrap_or("").to_string(),
                        e.get("message").and_then(|m| m.as_str()).unwrap_or("").to_string(),
                    )
                })
                .unwrap_or_else(|| (String::new(), "unknown error".into()));
            SfRecordResult { success: false, id, status_code, message }
        })
        .collect();
    while out.len() < expected {
        out.push(SfRecordResult::failure(
            "MISSING_RESULT",
            "no result entry returned for this record".into(),
        ));
    }
    out
}

/// RFC 4180 field escaping: quote when the cell contains a comma, quote, CR
/// or LF; embedded quotes are doubled.
fn csv_escape(cell: &str) -> String {
    if cell.contains([',', '"', '\r', '\n']) {
        format!("\"{}\"", cell.replace('"', "\"\""))
    } else {
        cell.to_string()
    }
}

/// One input cell for the results CSVs: strings verbatim, null/absent empty,
/// other scalars and nested values in their compact JSON form (same policy as
/// the record envelope, which passes nested cells through as-is).
fn salesforce_result_cell(v: Option<&JsonValue>) -> String {
    match v {
        None | Some(JsonValue::Null) => String::new(),
        Some(JsonValue::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// Write the Data-Loader-style result files for a snk.salesforce run (#166
/// resultsPath): `<stem>_success.csv` = input columns + `sf__Id`,
/// `<stem>_error.csv` = input columns + `sf__StatusCode` + `sf__Message`.
/// The caller stamps `stem` with the job details + run time
/// (`{object}_{operation}_{utc}`) so repeat runs accumulate instead of
/// overwriting, like Data Loader's per-run files. Both files are always
/// written, header-only when a side is empty. The header takes the first
/// row's column order, union-extended with later rows' extras in first-seen
/// order; input columns that collide with the sf__ report names are skipped
/// so the report values win. `results` may be shorter than `rows` when the
/// run aborted mid-loop - unattempted rows land in neither file.
fn write_salesforce_results_files(
    dir: &std::path::Path,
    stem: &str,
    rows: &[JsonValue],
    results: &[SfRecordResult],
) -> Result<(), EngineError> {
    const REPORT_COLS: [&str; 3] = ["sf__Id", "sf__StatusCode", "sf__Message"];
    let mut cols: Vec<&str> = Vec::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for k in obj.keys() {
                if REPORT_COLS.contains(&k.as_str()) {
                    continue;
                }
                if !cols.contains(&k.as_str()) {
                    cols.push(k);
                }
            }
        }
    }
    let quoted: Vec<String> = cols.iter().map(|c| csv_escape(c)).collect();
    let header = |extra: &[&str]| -> String {
        let mut h = quoted.clone();
        h.extend(extra.iter().map(|s| s.to_string()));
        h.join(",") + "\n"
    };
    let mut success_buf = header(&["sf__Id"]);
    let mut error_buf = header(&["sf__StatusCode", "sf__Message"]);
    for (row, res) in rows.iter().zip(results) {
        let mut cells: Vec<String> = cols
            .iter()
            .map(|c| csv_escape(&salesforce_result_cell(row.get(c))))
            .collect();
        if res.success {
            cells.push(csv_escape(res.id.as_deref().unwrap_or("")));
            success_buf.push_str(&cells.join(","));
            success_buf.push('\n');
        } else {
            cells.push(csv_escape(&res.status_code));
            cells.push(csv_escape(&res.message));
            error_buf.push_str(&cells.join(","));
            error_buf.push('\n');
        }
    }
    std::fs::create_dir_all(dir).map_err(|e| {
        EngineError::Query(format!("salesforce results: create {}: {}", dir.display(), e))
    })?;
    for (suffix, buf) in [("success.csv", success_buf), ("error.csv", error_buf)] {
        let path = dir.join(format!("{}_{}", stem, suffix));
        std::fs::write(&path, buf).map_err(|e| {
            EngineError::Query(format!("salesforce results: write {}: {}", path.display(), e))
        })?;
    }
    Ok(())
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
    // Already a path that exists: an absolute one, or one relative to where the run was
    // started. Nothing to look for.
    if std::path::Path::new(reference).is_file() {
        return reference.to_string();
    }
    let Some(ws) = std::env::var("DUCKLE_WORKSPACE").ok().filter(|w| !w.is_empty()) else {
        return reference.to_string();
    };
    resolve_subpipeline_in(reference, std::path::Path::new(&ws))
}

/// The same, against a named workspace. Split out so it can be exercised without an
/// environment variable, which every test in the process would otherwise share.
fn resolve_subpipeline_in(reference: &str, root: &std::path::Path) -> String {

    // A reference is written the way the job wrote it - usually the child's bare name,
    // sometimes with an extension, occasionally a path. Try the arrangements a workspace
    // actually uses before going looking.
    let file = format!(
        "{}.json",
        std::path::Path::new(reference)
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| reference.to_string())
    );
    for candidate in [root.join(reference), root.join("pipelines").join(&file)] {
        if candidate.is_file() {
            return candidate.display().to_string();
        }
    }

    // A converted repository keeps the folder layout it came from rather than flattening
    // it, because two jobs in different folders routinely share a name. A job calls its
    // children by name and knows nothing about where the conversion put them, so the
    // workspace is searched for the file. Names are unique across a conversion - the
    // first to claim a bare name keeps it - so a match is the match.
    let mut found: Vec<std::path::PathBuf> = Vec::new();
    let mut queue = vec![root.to_path_buf()];
    let mut visited = 0usize;
    while let Some(dir) = queue.pop() {
        // A workspace holds run logs and outputs as well as pipelines, and a big one
        // should not turn every child call into a full disk walk.
        visited += 1;
        if visited > 4096 || found.len() > 1 {
            break;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if path.is_dir() {
                if !matches!(name.as_ref(), ".duckle" | "logs" | "runs" | "out" | "target")
                    && !name.starts_with('.')
                {
                    queue.push(path);
                }
            } else if name == file.as_str() {
                found.push(path);
            }
        }
    }
    match found.len() {
        1 => found[0].display().to_string(),
        // Nothing found, or more than one: hand back what was asked for so the caller
        // reports the name the job actually used rather than a guess at which it meant.
        _ => reference.to_string(),
    }
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
mod xml_remote_tests {
    use super::{parse_sftp_uri, xml_declared_columns};

    #[test]
    fn parse_sftp_uri_variants() {
        // user@host:port + absolute path
        let (h, p, u, path) =
            parse_sftp_uri("sftp://bob@host.example.com:2222/data/day.xml.gz").unwrap();
        assert_eq!(h, "host.example.com");
        assert_eq!(p, 2222);
        assert_eq!(u.as_deref(), Some("bob"));
        assert_eq!(path, "/data/day.xml.gz");

        // no user, default port, root-relative path
        let (h, p, u, path) = parse_sftp_uri("sftp://files.example.com/a/b.xml").unwrap();
        assert_eq!(h, "files.example.com");
        assert_eq!(p, 22);
        assert_eq!(u, None);
        assert_eq!(path, "/a/b.xml");

        // no path
        let (h, p, _, path) = parse_sftp_uri("sftp://host").unwrap();
        assert_eq!(h, "host");
        assert_eq!(p, 22);
        assert_eq!(path, "/");

        // wrong scheme / empty host are rejected
        assert!(parse_sftp_uri("https://host/x").is_err());
        assert!(parse_sftp_uri("sftp:///only/path").is_err());
    }

    #[test]
    fn declared_columns_build_varchar_read_and_typed_cast() {
        use duckle_metadata::{Column, DataType};
        let schema = vec![
            Column { name: "id".into(), data_type: DataType::Int64, nullable: true, primary_key: None, format: None },
            Column { name: "price".into(), data_type: DataType::Float64, nullable: true, primary_key: None, format: None },
            Column { name: "title".into(), data_type: DataType::String, nullable: true, primary_key: None, format: None },
        ];
        let (columns_spec, select_list) = xml_declared_columns(&schema);
        // read_json reads every declared column as text...
        assert_eq!(
            columns_spec,
            "'id': 'VARCHAR', 'price': 'VARCHAR', 'title': 'VARCHAR'"
        );
        // ...then each is TRY_CAST to its declared DuckDB type (empty -> NULL).
        assert_eq!(
            select_list,
            "TRY_CAST(NULLIF(\"id\", '') AS BIGINT) AS \"id\", \
             TRY_CAST(NULLIF(\"price\", '') AS DOUBLE) AS \"price\", \
             TRY_CAST(NULLIF(\"title\", '') AS VARCHAR) AS \"title\""
        );
    }
}

#[cfg(all(test, feature = "oracle"))]
mod oracle_arrow_tests {
    use super::DuckdbEngine;

    #[test]
    fn decimal_text_is_rescaled_without_losing_digits() {
        // The whole reason the Arrow path reads NUMBER as text: f64 only
        // round-trips ~15 significant digits, and NUMBER carries up to 38.
        // Rescaling from the exact text keeps every one (#196, #221).
        let f = DuckdbEngine::oracle_decimal_to_i128;
        assert_eq!(f("123.45", 2), Some(12345));
        assert_eq!(f("-123.45", 2), Some(-12345));
        assert_eq!(f("123", 2), Some(12300), "integer padded to scale");
        assert_eq!(f("0.5", 4), Some(5000));
        assert_eq!(f("-0.0001", 4), Some(-1));
        assert_eq!(f("+7", 0), Some(7));
        // 38 significant digits survive intact; an f64 would have mangled this.
        assert_eq!(
            f("123456.123456789012", 12),
            Some(123_456_123_456_789_012_i128)
        );
        // More fraction digits than the column declares are truncated, not
        // rounded, matching how Oracle stores into a narrower scale.
        assert_eq!(f("1.999", 2), Some(199));
        // Anything not plain decimal text is refused so the caller can fail
        // loudly instead of writing a wrong number.
        assert_eq!(f("1.2e5", 2), None);
        assert_eq!(f("abc", 2), None);
        // Zero has to survive as 0, not as a refusal. The allocation-free
        // parser replaced a version that reached this through
        // `trim_start_matches('0')` leaving an empty string and `unwrap_or(0)`,
        // so it is the case most likely to regress and the one that would be
        // silently wrong if it did: the caller turns None into a failed run.
        assert_eq!(f("0", 2), Some(0));
        assert_eq!(f("0.00", 2), Some(0));
        assert_eq!(f("-0.00", 4), Some(0));
        assert_eq!(f("0.0000", 0), Some(0));
        // A missing side of the decimal point is still plain decimal text.
        assert_eq!(f(".5", 2), Some(50));
        assert_eq!(f("5.", 2), Some(500));
        // Leading and trailing whitespace was trimmed before and still is.
        assert_eq!(f("  12.5  ", 1), Some(125));
        // A second decimal point is not a number.
        assert_eq!(f("1.2.3", 2), None);
        // The full 38 digits Oracle allows still fit an i128.
        assert_eq!(
            f("99999999999999999999999999999999999999", 0),
            Some(99_999_999_999_999_999_999_999_999_999_999_999_999_i128)
        );
        // Negative scale columns ask for scale 0 worth of fraction digits.
        assert_eq!(f("1234.9", -2), Some(1234));
    }

    #[test]
    fn the_arrow_type_table_is_what_ships() {
        // This used to re-state the match arms, so it kept passing while the
        // real decision changed underneath it. It now calls the function.
        use arrow_schema::DataType;
        use oracle::sql_type::OracleType;
        let t = |o| DuckdbEngine::oracle_arrow_type(&o);

        // Pinned by the declaration: exact types, no post-processing.
        assert_eq!(t(OracleType::Number(12, 0)), Some((DataType::Int64, false)));
        assert_eq!(
            t(OracleType::Number(15, 2)),
            Some((DataType::Decimal128(15, 2), false))
        );
        assert_eq!(t(OracleType::Varchar2(60)), Some((DataType::Utf8, false)));
        assert!(matches!(t(OracleType::Date), Some((DataType::Timestamp(..), false))));

        // NOT pinned by the declaration: carried as text, typed from the values
        // after the write. An unconstrained NUMBER is what COUNT/SUM produce and
        // what most Oracle warehouse columns are declared as, so this is the
        // difference between the fast path running and not running at all.
        assert_eq!(t(OracleType::Number(0, -127)), Some((DataType::Utf8, true)));
        assert_eq!(t(OracleType::Number(5, -2)), Some((DataType::Utf8, true)));

        // Still no exact mapping: these keep the NDJSON path.
        assert_eq!(t(OracleType::CLOB), None, "LOBs stay on the JSON path");
        assert_eq!(t(OracleType::BLOB), None);
    }
}

#[cfg(test)]
mod dhis2_summary_tests {
    use super::{parse_dhis2_import_summary, parse_dhis2_tracker_report, Dhis2Counts};
    use serde_json::json;

    #[test]
    fn aggregate_conflict_text_is_read_from_value_not_message() {
        // The whole reason this parser exists. DHIS2 serialises an
        // ImportConflict's human-readable text under `value`; the Java field is
        // called `message` but carries @JsonProperty("value"). Reading
        // `message` yields nothing, so every conflict renders blank and the
        // operator cannot tell what was rejected.
        let body = json!({
            "httpStatus": "Conflict", "httpStatusCode": 409, "status": "ERROR",
            "response": {
                "responseType": "ImportSummary", "status": "ERROR",
                "importCount": {"imported": 0, "updated": 3, "ignored": 2, "deleted": 0},
                "conflicts": [
                    {"object": "fbfJHSPpUQD",
                     "value": "Data element not found or not accessible",
                     "message": "SHOULD NOT BE READ",
                     "errorCode": "E7610"}
                ]
            }
        });
        let (counts, msgs) = parse_dhis2_import_summary(&body);
        assert_eq!(
            counts,
            Dhis2Counts { imported: 0, updated: 3, deleted: 0, ignored: 2 }
        );
        assert_eq!(msgs.len(), 1);
        assert!(
            msgs[0].contains("Data element not found"),
            "conflict text must come from `value`, got: {}",
            msgs[0]
        );
        assert!(!msgs[0].contains("SHOULD NOT BE READ"));
    }

    #[test]
    fn aggregate_success_with_ignored_rows_is_not_silently_clean() {
        // HTTP 200, status SUCCESS, no conflicts - but rows were ignored.
        // The caller treats a non-zero ignored count as a problem, because
        // "we accepted your request and wrote nothing" is the failure mode
        // this connector exists to catch.
        let body = json!({
            "status": "SUCCESS",
            "response": {
                "status": "SUCCESS",
                "importCount": {"imported": 0, "updated": 0, "ignored": 4, "deleted": 0},
                "conflicts": []
            }
        });
        let (counts, msgs) = parse_dhis2_import_summary(&body);
        assert_eq!(counts.ignored, 4);
        assert!(msgs.is_empty(), "no conflicts were reported by the server");
    }

    #[test]
    fn aggregate_error_without_conflicts_still_surfaces() {
        let body = json!({
            "status": "ERROR",
            "response": {"status": "ERROR", "description": "Data set not found"}
        });
        let (_, msgs) = parse_dhis2_import_summary(&body);
        assert_eq!(msgs, vec!["Data set not found".to_string()]);
    }

    #[test]
    fn tracker_report_uses_stats_and_message_not_importcount_and_value() {
        // Tracker shares no keys with the aggregate shape: counts are under
        // `stats` with `created` rather than `imported`, and the error text is
        // under `message`, the opposite of the aggregate `value`. Using one
        // parser for both would report zero counts and zero errors.
        let body = json!({
            "status": "ERROR",
            "stats": {"created": 2, "updated": 1, "deleted": 0, "ignored": 3, "total": 6},
            "validationReport": {
                "errorReports": [
                    {"message": "Could not find TrackedEntityType: `Q9GufDoplCL`.",
                     "errorCode": "E1005",
                     "trackerType": "TRACKED_ENTITY",
                     "uid": "Kj6vYde4LHh"}
                ],
                "warningReports": []
            }
        });
        let (counts, msgs) = parse_dhis2_tracker_report(&body);
        assert_eq!(
            counts,
            Dhis2Counts { imported: 2, updated: 1, deleted: 0, ignored: 3 }
        );
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].starts_with("E1005 "), "got: {}", msgs[0]);
        assert!(msgs[0].contains("Q9GufDoplCL"));
    }

    #[test]
    fn tracker_ok_report_is_clean() {
        let body = json!({
            "status": "OK",
            "stats": {"created": 5, "updated": 0, "deleted": 0, "ignored": 0, "total": 5},
            "validationReport": {"errorReports": [], "warningReports": []}
        });
        let (counts, msgs) = parse_dhis2_tracker_report(&body);
        assert_eq!(counts.imported, 5);
        assert!(msgs.is_empty());
    }

    #[test]
    fn each_parser_ignores_the_other_endpoints_shape() {
        // Guards against someone later "simplifying" the two parsers into one.
        let tracker_body = json!({
            "status": "OK",
            "stats": {"created": 7, "updated": 0, "deleted": 0, "ignored": 0}
        });
        let (aggregate_view, _) = parse_dhis2_import_summary(&tracker_body);
        assert_eq!(aggregate_view.imported, 0, "aggregate parser must not read `stats`");

        let aggregate_body = json!({
            "response": {"importCount": {"imported": 7, "updated": 0, "ignored": 0, "deleted": 0}}
        });
        let (tracker_view, _) = parse_dhis2_tracker_report(&aggregate_body);
        assert_eq!(tracker_view.imported, 0, "tracker parser must not read `importCount`");
    }
}

#[cfg(test)]
mod connector_helper_tests {
    use super::{bson_flag_matches, jsonnative_quote_inner, python_temp_paths};
    use mongodb::bson::Bson;

    #[test]
    fn a_url_template_names_a_column_or_fails_loudly() {
        // #257. A template naming a column the upstream does not have must fail
        // loudly. Blanking it the way a prompt template does would silently
        // request /companies//officers and the run would look like it worked.
        let row = serde_json::json!({ "id": 7, "name": "Acme" });
        assert_eq!(
            super::render_url_template("/c/{id}/o", &row).unwrap(),
            "/c/7/o"
        );
        // Values are percent-encoded, so an id carrying a slash or a space
        // cannot change the shape of the request.
        let odd = serde_json::json!({ "id": "a b/c?d" });
        assert_eq!(
            super::render_url_template("/c/{id}", &odd).unwrap(),
            "/c/a%20b%2Fc%3Fd"
        );
        let err = super::render_url_template("/c/{nope}/o", &row)
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope"), "got: {}", err);
        assert!(
            err.contains("id"),
            "the error should list the columns that ARE available: {}",
            err
        );
        // An unclosed brace stays literal, so the user sees it rather than
        // losing the tail of the URL.
        assert_eq!(
            super::render_url_template("/c/{id", &row).unwrap(),
            "/c/{id"
        );
    }

    #[test]
    fn a_rate_limit_waits_as_long_as_the_provider_says() {
        // #258. Retry-After is the provider stating when it will serve again;
        // guessing shorter than that just earns another 429.
        let w = crate::DuckdbEngine::ai_retry_wait_ms;
        assert_eq!(w(Some("2"), 0), 2_000);
        assert_eq!(w(Some(" 5 "), 3), 5_000, "surrounding space is not a parse failure");
        // A silly Retry-After must not park a stage indefinitely.
        assert_eq!(w(Some("99999"), 0), 300_000);
        // With no header the wait doubles from 500ms, and is capped.
        assert_eq!(w(None, 0), 500);
        assert_eq!(w(None, 1), 1_000);
        assert_eq!(w(None, 2), 2_000);
        assert_eq!(w(None, 30), 30_000, "shifting by a large attempt must not overflow");
        // An HTTP-date Retry-After is not a number: fall back to backoff
        // rather than reading it as zero and hammering the provider.
        assert_eq!(w(Some("Wed, 21 Oct 2026 07:28:00 GMT"), 1), 1_000);
    }

    #[test]
    fn a_workspace_can_carry_its_own_python() {
        // #246. A Python stage is only reproducible if the packages it needs are pinned
        // beside the pipeline rather than being whatever the machine happens to have -
        // which matters more now that transform(table) needs pyarrow. A workspace venv
        // is the unit that travels with the project, and `uv venv` produces exactly this
        // layout, so this works with uv without depending on it.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        assert!(super::python_in_workspace(ws).is_none(), "no venv, nothing claimed");

        let (dir, exe) = if cfg!(windows) { ("Scripts", "python.exe") } else { ("bin", "python3") };
        let bin = ws.join(".venv").join(dir);
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(bin.join(exe), b"").unwrap();
        let found = super::python_in_workspace(ws).expect("a venv is found");
        assert!(found.contains(".venv"), "got: {found}");
        assert!(found.ends_with(exe), "got: {found}");
    }

    #[test]
    fn the_entry_point_the_script_defines_picks_the_mode() {
        use super::defines_vectorized_entry as v;
        // Handed the whole table.
        assert!(v("def transform(table):
    return table"));
        // Row at a time, exactly as before - every saved pipeline keeps working.
        assert!(!v("def process(row):
    return row"));
        // A helper called transform INSIDE something else is not the entry point: the
        // harness calls the top-level name, and treating a nested one as the mode would
        // send a script down a path it never asked for.
        assert!(!v("def process(row):
    def transform(x):
        return x
    return row"));
        // Nothing at all is the old behaviour, which fails the same way it always did.
        assert!(!v("x = 1"));
    }

    #[test]
    fn python_temp_paths_are_unique_per_run_db() {
        // #203: two concurrent foreach iterations (or parallelize branches, or
        // parallel scheduled runs) each mint a distinct run db filename in the
        // shared temp dir. The old paths keyed only on node_id, so with_file_name
        // dropped that unique stem and both runs of the same node collapsed onto
        // one set of scratch files, racing each other's reads, writes and cleanup.
        let dir = std::env::temp_dir();
        let a = dir.join("duckle_run_100_5_0.duckdb");
        let b = dir.join("duckle_run_100_5_1.duckdb");
        let (a_in, a_out, a_sc) = python_temp_paths(&a, "normalize");
        let (b_in, b_out, b_sc) = python_temp_paths(&b, "normalize");
        // Same node, different run: every scratch file must differ.
        assert_ne!(a_in, b_in, "py-in collided across runs");
        assert_ne!(a_out, b_out, "py-out collided across runs");
        assert_ne!(a_sc, b_sc, "py script collided across runs");
        // The three files of one run sit beside its db and carry its unique stem.
        assert_eq!(a_in.parent(), a.parent());
        let a_name = a_in.file_name().unwrap().to_string_lossy();
        assert!(a_name.contains("duckle_run_100_5_0.duckdb"), "got: {}", a_name);
        // The input/output/script of one run are distinct from each other.
        assert_ne!(a_in, a_out);
        assert_ne!(a_in, a_sc);
        // A node_id with path-like characters cannot escape the filename.
        let (weird, _, _) = python_temp_paths(&a, "a/b.c\\d");
        let w = weird.file_name().unwrap().to_string_lossy();
        assert!(!w.contains("a/b.c"), "node_id not sanitised: {}", w);
        assert!(w.contains("a_b_c_d"), "expected sanitised node_id, got: {}", w);
    }

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
mod salesforce_results_tests {
    use super::{
        csv_escape, parse_salesforce_results, salesforce_result_cell,
        write_salesforce_results_files, SfRecordResult,
    };
    use serde_json::json;

    #[test]
    fn csv_escape_quotes_only_when_needed() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape(""), "");
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
        assert_eq!(csv_escape("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_escape("line\nbreak"), "\"line\nbreak\"");
    }

    #[test]
    fn result_cell_formats_by_json_type() {
        assert_eq!(salesforce_result_cell(Some(&json!("text"))), "text");
        assert_eq!(salesforce_result_cell(Some(&json!(null))), "");
        assert_eq!(salesforce_result_cell(None), "");
        assert_eq!(salesforce_result_cell(Some(&json!(42))), "42");
        assert_eq!(salesforce_result_cell(Some(&json!(true))), "true");
        // Nested values keep their compact JSON form.
        assert_eq!(salesforce_result_cell(Some(&json!({"a":1}))), "{\"a\":1}");
    }

    #[test]
    fn parse_walks_records_positionally() {
        let body = r#"[
            {"id":"001A","success":true,"errors":[]},
            {"success":false,"errors":[{"statusCode":"REQUIRED_FIELD_MISSING","message":"Name missing"}]}
        ]"#;
        let r = parse_salesforce_results(body, 2);
        assert_eq!(r.len(), 2);
        assert!(r[0].success);
        assert_eq!(r[0].id.as_deref(), Some("001A"));
        assert!(!r[1].success);
        assert_eq!(r[1].status_code, "REQUIRED_FIELD_MISSING");
        assert_eq!(r[1].message, "Name missing");
    }

    #[test]
    fn parse_non_array_fails_every_expected_record() {
        // An API-level error envelope must not leave later records looking
        // successful - every submitted record failed.
        let r = parse_salesforce_results(r#"{"message":"Session expired"}"#, 3);
        assert_eq!(r.len(), 3);
        assert!(r.iter().all(|x| !x.success && x.status_code == "API_ERROR"));
        assert_eq!(r[0].message, "Session expired");

        let u = parse_salesforce_results("<html>gateway error</html>", 2);
        assert_eq!(u.len(), 2);
        assert!(u.iter().all(|x| x.status_code == "UNPARSEABLE_RESPONSE"));
    }

    #[test]
    fn parse_short_array_pads_missing_results() {
        let r = parse_salesforce_results(r#"[{"id":"001A","success":true,"errors":[]}]"#, 3);
        assert_eq!(r.len(), 3);
        assert!(r[0].success);
        assert_eq!(r[1].status_code, "MISSING_RESULT");
        assert_eq!(r[2].status_code, "MISSING_RESULT");
    }

    #[test]
    fn results_files_split_rows_and_union_headers() {
        let dir = tempfile::tempdir().unwrap();
        // Second row carries an extra column -> header union, first-seen order;
        // a stray input sf__Id column is skipped so the report value wins.
        let rows = vec![
            json!({"Name":"Acme","sf__Id":"stale"}),
            json!({"Name":"Glo,bex","Region":"EMEA"}),
        ];
        let results = vec![
            SfRecordResult { success: true, id: Some("001A".into()), status_code: String::new(), message: String::new() },
            SfRecordResult::failure("REQUIRED_FIELD_MISSING", "Industry missing".into()),
        ];
        write_salesforce_results_files(dir.path(), "Account_insert_20260715T000000Z", &rows, &results).unwrap();
        let s = std::fs::read_to_string(dir.path().join("Account_insert_20260715T000000Z_success.csv")).unwrap();
        let e = std::fs::read_to_string(dir.path().join("Account_insert_20260715T000000Z_error.csv")).unwrap();
        assert_eq!(s, "Name,Region,sf__Id\nAcme,,001A\n");
        assert_eq!(
            e,
            "Name,Region,sf__StatusCode,sf__Message\n\"Glo,bex\",EMEA,REQUIRED_FIELD_MISSING,Industry missing\n"
        );
    }

    #[test]
    fn results_files_write_header_only_when_side_empty() {
        // Data Loader parity: both files always exist after a run.
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![json!({"Name":"Acme"})];
        let results = vec![SfRecordResult {
            success: true,
            id: Some("001A".into()),
            status_code: String::new(),
            message: String::new(),
        }];
        write_salesforce_results_files(dir.path(), "Account_insert_20260715T000000Z", &rows, &results).unwrap();
        let e = std::fs::read_to_string(dir.path().join("Account_insert_20260715T000000Z_error.csv")).unwrap();
        assert_eq!(e, "Name,sf__StatusCode,sf__Message\n");
    }

    #[test]
    fn results_files_skip_unattempted_rows() {
        // results shorter than rows (a chunk aborted the run): the tail rows
        // land in neither file.
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![json!({"Name":"Acme"}), json!({"Name":"Globex"})];
        let results = vec![SfRecordResult::failure("HTTP_401", "Salesforce HTTP 401".into())];
        write_salesforce_results_files(dir.path(), "Account_insert_20260715T000000Z", &rows, &results).unwrap();
        let s = std::fs::read_to_string(dir.path().join("Account_insert_20260715T000000Z_success.csv")).unwrap();
        let e = std::fs::read_to_string(dir.path().join("Account_insert_20260715T000000Z_error.csv")).unwrap();
        assert_eq!(s, "Name,sf__Id\n");
        assert_eq!(e.matches('\n').count(), 2, "header + exactly one error row: {}", e);
        assert!(e.contains("Acme,HTTP_401,"));
        assert!(!e.contains("Globex"));
    }
}

#[cfg(test)]
mod context_var_tests {
    use super::context_vars_for_workspace;

    #[test]
    fn a_child_job_is_found_wherever_the_conversion_put_it() {
        // A converted repository keeps the folder layout it came from, and a job calls
        // its children by name: the caller knows nothing about the folder the child
        // landed in, and the two are routinely in different ones. Resolved only against
        // the working directory, every master job failed on its first child.
        let ws = tempfile::tempdir().unwrap();
        let nested = ws.path().join("process").join("UNIFIED_PORTAL").join("LOAD");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("CHILD_JOB.json"), "{}").unwrap();

        let hit = super::resolve_subpipeline_in("CHILD_JOB.json", ws.path());
        let bare = super::resolve_subpipeline_in("CHILD_JOB", ws.path());
        let miss = super::resolve_subpipeline_in("NOT_THERE.json", ws.path());

        assert!(
            std::path::Path::new(&hit).is_file(),
            "found by name, wherever it sits: {hit}"
        );
        assert_eq!(bare, hit, "with or without the extension the job wrote");
        assert_eq!(
            miss, "NOT_THERE.json",
            "and a name that is nowhere comes back as asked, so the error names it"
        );
    }

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

#[cfg(test)]
mod salesforce_bulk_tests {
    use super::{
        append_bulk_result_csv, result_csv_has_data_rows, BULK_SPLIT_TARGET_BYTES,
        BULK_UPLOAD_MAX_BYTES,
    };

    /// The Bulk query source decided emptiness from the optional
    /// Sforce-NumberOfRecords header alone, defaulted to 0. A proxy that
    /// strips the non-standard Sforce-* headers therefore made a fully staged
    /// extract look empty, and the run discarded it and reported success with
    /// 0 rows. The staged file is now the authority, so these pin what it
    /// reports.
    #[test]
    fn staged_csv_row_detection_drives_the_empty_decision() {
        let dir = tempfile::tempdir().unwrap();

        // A real result set: header plus rows.
        let with_rows = dir.path().join("with_rows.csv");
        std::fs::write(&with_rows, "Id,Name
001,Acme
002,Globex
").unwrap();
        assert!(result_csv_has_data_rows(&with_rows));

        // A genuinely empty result: header only. This must stay the typed
        // empty-relation path (#170).
        let header_only = dir.path().join("header_only.csv");
        std::fs::write(&header_only, "Id,Name
").unwrap();
        assert!(!result_csv_has_data_rows(&header_only));

        // Header with only trailing blank lines is still empty.
        let blank_tail = dir.path().join("blank_tail.csv");
        std::fs::write(&blank_tail, "Id,Name


").unwrap();
        assert!(!result_csv_has_data_rows(&blank_tail));

        // A single row with no trailing newline still counts.
        let no_trailing_nl = dir.path().join("no_nl.csv");
        std::fs::write(&no_trailing_nl, "Id,Name
001,Acme").unwrap();
        assert!(result_csv_has_data_rows(&no_trailing_nl));

        // Nothing fetched at all: the file was never created.
        assert!(!result_csv_has_data_rows(&dir.path().join("missing.csv")));

        // Zero-length file (created but nothing written).
        let empty = dir.path().join("empty.csv");
        std::fs::write(&empty, "").unwrap();
        assert!(!result_csv_has_data_rows(&empty));
    }

    #[test]
    fn split_target_leaves_headroom_under_the_upload_ceiling() {
        // The split target must sit below the hard upload cap so DuckDB's
        // few-percent FILE_SIZE_BYTES overshoot still lands under the limit.
        assert!(
            BULK_SPLIT_TARGET_BYTES < BULK_UPLOAD_MAX_BYTES,
            "split target {} must be below the {} upload cap",
            BULK_SPLIT_TARGET_BYTES,
            BULK_UPLOAD_MAX_BYTES
        );
        // At least a 10% margin for the overshoot observed in testing (~3.6%).
        assert!(BULK_SPLIT_TARGET_BYTES <= BULK_UPLOAD_MAX_BYTES * 9 / 10);
    }

    #[test]
    fn first_part_keeps_header_later_parts_append_data_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acct_insert_success.csv");
        // First body to the file: whole body (header + rows).
        append_bulk_result_csv(&path, "sf__Id,Name\n001,Acme\n".as_bytes()).unwrap();
        // Second body: header stripped, only the data row appended.
        append_bulk_result_csv(&path, "sf__Id,Name\n002,Globex\n".as_bytes()).unwrap();
        let out = std::fs::read_to_string(&path).unwrap();
        assert_eq!(out, "sf__Id,Name\n001,Acme\n002,Globex\n");
    }

    #[test]
    fn header_only_result_body_appends_nothing_on_later_parts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acct_insert_error.csv");
        append_bulk_result_csv(&path, "sf__Id,sf__Error\n".as_bytes()).unwrap();
        // A later body with only a header (no failures) must add no rows.
        append_bulk_result_csv(&path, "sf__Id,sf__Error\n".as_bytes()).unwrap();
        let out = std::fs::read_to_string(&path).unwrap();
        assert_eq!(out, "sf__Id,sf__Error\n");
    }

    #[test]
    fn later_part_into_a_fresh_file_keeps_its_header() {
        // Regression: if an earlier part's result fetch was skipped (transient
        // error), the file does not exist yet. The header decision is per file,
        // so the first body actually written must keep its header rather than be
        // stripped as a "later part".
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acct_insert_success.csv");
        // Part 0 skipped -> nothing written. Part 1 is the first real body.
        append_bulk_result_csv(&path, "sf__Id,Name\n002,Globex\n".as_bytes()).unwrap();
        let out = std::fs::read_to_string(&path).unwrap();
        assert_eq!(out, "sf__Id,Name\n002,Globex\n");
    }

    #[test]
    fn result_bodies_over_ureq_string_cap_stream_intact() {
        // The live bug: ureq's into_string() caps at 10 MB, so a ~100 MB result
        // set silently became an empty file. The writer takes a reader and
        // streams, so a body well past that cap must land byte-complete.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acct_insert_success.csv");
        let row = "001xx000003DGb2AAG,true,Acme Corp 12345678901234567890\n";
        let rows = 12 * 1024 * 1024 / row.len(); // ~12 MB of data rows
        let mut body = String::with_capacity(rows * row.len() + 32);
        body.push_str("sf__Id,sf__Created,Name\n");
        for _ in 0..rows {
            body.push_str(row);
        }
        append_bulk_result_csv(&path, body.as_bytes()).unwrap();
        let written = std::fs::metadata(&path).unwrap().len();
        assert_eq!(written, body.len() as u64, "streamed body must be complete");
    }
}

/// Build a WebSocket handshake request (#192) from a URL plus optional extra
/// headers (e.g. Authorization). ws:// and wss:// are both handled; wss uses the
/// bundled webpki roots via tokio-tungstenite's rustls feature.
fn websocket_request(
    url: &str,
    headers: &[(String, String)],
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, String> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
    // Reject a non-ws scheme up front. into_client_request() happily parses
    // http:// / https:// (a common mistake for ws:// / wss://) and only fails
    // deep inside connect_async with an opaque message; catch it here instead.
    let scheme = url.split("://").next().unwrap_or("").to_ascii_lowercase();
    if scheme != "ws" && scheme != "wss" {
        return Err(format!(
            "websocket url must start with ws:// or wss:// (got '{}')",
            url
        ));
    }
    let mut request = url
        .into_client_request()
        .map_err(|e| format!("bad websocket url {}: {}", url, e))?;
    for (k, v) in headers {
        if let (Ok(name), Ok(val)) =
            (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(v))
        {
            request.headers_mut().insert(name, val);
        }
    }
    Ok(request)
}

/// Parse one WebSocket frame's text (#192) into rows: a JSON object becomes one
/// row, a JSON array a row per element (bare elements wrapped as `{value: ...}`),
/// and any non-JSON text a `{message: text}` row - the same shape src.webhook
/// uses so downstream transforms see consistent columns.
fn websocket_parse_into_rows(text: &str, rows: &mut Vec<JsonValue>) {
    match serde_json::from_str::<JsonValue>(text) {
        Ok(JsonValue::Object(o)) => rows.push(JsonValue::Object(o)),
        Ok(JsonValue::Array(arr)) => {
            for v in arr {
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
            let mut m = serde_json::Map::new();
            m.insert("message".into(), JsonValue::String(text.to_string()));
            rows.push(JsonValue::Object(m));
        }
    }
}

#[cfg(test)]
mod websocket_tests {
    use super::{websocket_parse_into_rows, websocket_request};
    use crate::JsonValue;

    fn parse(text: &str) -> Vec<JsonValue> {
        let mut rows = Vec::new();
        websocket_parse_into_rows(text, &mut rows);
        rows
    }

    #[test]
    fn object_frame_becomes_one_row() {
        let rows = parse(r#"{"symbol":"BTC","price":42}"#);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["symbol"], JsonValue::String("BTC".into()));
        assert_eq!(rows[0]["price"], JsonValue::from(42));
    }

    #[test]
    fn array_frame_fans_out_and_wraps_scalars() {
        // Objects pass through as rows; bare scalars are wrapped as {value}.
        let rows = parse(r#"[{"id":1}, "hello", 7]"#);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["id"], JsonValue::from(1));
        assert_eq!(rows[1]["value"], JsonValue::String("hello".into()));
        assert_eq!(rows[2]["value"], JsonValue::from(7));
    }

    #[test]
    fn non_json_frame_falls_back_to_message_column() {
        // Plain-text frames (e.g. "pong") must not be dropped; they land in a
        // single {message} row so the pipeline still sees them.
        let rows = parse("pong");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["message"], JsonValue::String("pong".into()));
    }

    #[test]
    fn request_carries_extra_headers() {
        let req = websocket_request(
            "wss://stream.example.com/socket",
            &[("Authorization".to_string(), "Bearer tok".to_string())],
        )
        .expect("request builds");
        assert_eq!(
            req.headers().get("Authorization").map(|v| v.to_str().unwrap()),
            Some("Bearer tok")
        );
    }

    #[test]
    fn request_rejects_non_ws_scheme() {
        assert!(websocket_request("https://example.com", &[]).is_err());
    }
}


/// Sequence for the MongoDB sink's staging directory, so two sinks in one run
/// cannot collide on a path.
static MONGO_STAGE_SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Sequence for the Hugging Face sink's staged Parquet, so concurrent sinks in
/// one run cannot collide on the temp path.
static HF_SINK_SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Read a DuckDB NDJSON export in fixed-size batches.
///
/// Returns an iterator so the caller never holds more than one batch: the
/// point of staging through a file is that a million-row migration costs the
/// same memory as a thousand-row one. A line that will not parse is skipped
/// rather than failing the whole load, matching how the previous in-memory
/// path treated an unconvertible row.
fn mongo_ndjson_batches(
    path: &Path,
    batch_size: usize,
) -> std::io::Result<impl Iterator<Item = Vec<JsonValue>>> {
    use std::io::BufRead;
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::with_capacity(1 << 20, file);
    let size = batch_size.max(1);
    let mut done = false;
    Ok(std::iter::from_fn(move || {
        if done {
            return None;
        }
        let mut batch = Vec::with_capacity(size);
        let mut line = String::new();
        while batch.len() < size {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    done = true;
                    break;
                }
                Ok(_) => {
                    let t = line.trim();
                    if t.is_empty() {
                        continue;
                    }
                    if let Ok(v) = serde_json::from_str::<JsonValue>(t) {
                        batch.push(v);
                    }
                }
                Err(_) => {
                    done = true;
                    break;
                }
            }
        }
        if batch.is_empty() {
            None
        } else {
            Some(batch)
        }
    }))
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

/// Build the `columns={...}` body and typed SELECT list for src.xml's declared
/// schema. Every column is read as VARCHAR (XML carries text) and TRY_CAST to
/// its declared DuckDB type, so the output is exactly the declared columns and
/// types - a column absent from a given day's file comes back NULL, and an
/// undeclared element is dropped, keeping the table shape stable across runs.
/// Mirrors the Snowflake / Teradata typed-finalize pattern (#186 follow-up).
fn xml_declared_columns(schema: &[duckle_metadata::Column]) -> (String, String) {
    let mut columns_spec_parts: Vec<String> = Vec::with_capacity(schema.len());
    let mut select_parts: Vec<String> = Vec::with_capacity(schema.len());
    for col in schema {
        let ident = plan::quote_ident(&col.name);
        columns_spec_parts.push(format!("'{}': 'VARCHAR'", col.name.replace('\'', "''")));
        let ty = plan::data_type_to_duckdb_sql(&col.data_type);
        select_parts.push(format!("TRY_CAST(NULLIF({i}, '') AS {ty}) AS {i}", i = ident, ty = ty));
    }
    (columns_spec_parts.join(", "), select_parts.join(", "))
}

/// Read up to `buf.len()` bytes, looping past short reads until the buffer is
/// full or EOF. `std::io::Read::read` may return fewer bytes than asked even
/// when more are available (common on network streams), so a single read can't
/// reliably peek a fixed-size magic header.
fn read_up_to<R: std::io::Read>(r: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(k) => filled += k,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// Stream XML rows from a non-seekable reader (http:// or sftp://). Peeks the
/// first bytes to pick gzip vs plain and chains them back, so nothing is
/// buffered whole. zip is rejected: its central directory lives at EOF and needs
/// random access, which a network stream can't give - use a .gz or a local path.
fn stream_remote_xml<R: std::io::Read>(
    reader: R,
    row_path: &str,
    cancel: &Arc<AtomicBool>,
    emit: &mut dyn FnMut(&JsonValue) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    use std::io::{BufReader, Read};
    let mut reader = reader;
    let mut head = [0u8; 4];
    let n = read_up_to(&mut reader, &mut head)
        .map_err(|e| EngineError::Query(format!("xml: read stream: {}", e)))?;
    if n >= 4 && &head[0..4] == b"PK\x03\x04" {
        return Err(EngineError::Config(
            "xml: a zip over http/sftp can't be streamed (its directory is at the end of the file); use a .gz file or a local path".into(),
        ));
    }
    // Buffer once, here, so BOTH branches read the transport in large chunks.
    // SftpFileReader::read is one SFTP READ round trip per call and nothing
    // reads ahead, so the buffer size IS the request size: std's default 8 KiB
    // caps a transfer at 8192 bytes per round trip, serially. A 1 GB document
    // is then ~131k sequential requests. The server will serve far more per
    // packet (russh-sftp negotiates a 256 KiB max_read_len against OpenSSH), we
    // just have to ask for it. The bytes delivered are identical either way.
    let chained = BufReader::with_capacity(
        256 * 1024,
        std::io::Cursor::new(head[..n].to_vec()).chain(reader),
    );
    if n >= 2 && head[0] == 0x1f && head[1] == 0x8b {
        let decoder = flate2::read::MultiGzDecoder::new(chained);
        stream_xml_rows(BufReader::new(decoder), row_path, cancel, emit)
    } else {
        // Already a BufRead. Re-wrapping in a default BufReader here would put
        // the 8 KiB request size straight back.
        stream_xml_rows(chained, row_path, cancel, emit)
    }
}

/// Parse `sftp://[user@]host[:port]/remote/path` into (host, port, user, path).
/// Port defaults to 22; the path keeps its leading `/` (absolute) unless the URL
/// has none. Auth secrets are NOT taken from the URL - they come from node props.
fn parse_sftp_uri(uri: &str) -> Result<(String, u16, Option<String>, String), EngineError> {
    let rest = uri
        .strip_prefix("sftp://")
        .ok_or_else(|| EngineError::Config(format!("xml: not an sftp URL: {}", uri)))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    let (user, hostport) = match authority.rfind('@') {
        Some(i) => (Some(authority[..i].to_string()), &authority[i + 1..]),
        None => (None, authority),
    };
    let (host, port) = match hostport.rfind(':') {
        Some(i) => (
            hostport[..i].to_string(),
            hostport[i + 1..].parse::<u16>().unwrap_or(22),
        ),
        None => (hostport.to_string(), 22),
    };
    if host.is_empty() {
        return Err(EngineError::Config(format!("xml: sftp URL has no host: {}", uri)));
    }
    Ok((host, port, user, path))
}

/// Does the host key the SFTP server presented match the pinned SHA256
/// fingerprint?
///
/// One function rather than the three copies that used to sit inline, because
/// three copies of a security check is three chances for one of them to drift.
///
/// russh 0.63 changed what the server can present: `check_server_key` now
/// receives a `PublicKeyOrCertificate` instead of a bare `PublicKey`, because
/// a host may answer with an OpenSSH host CERTIFICATE rather than a raw key.
///
/// A pinned fingerprint names one exact host key, so a certificate is accepted
/// only when the key it certifies IS that key, and refused otherwise.
///
/// That is exactly as strong as pinning the raw key, which is worth spelling
/// out because it is the whole security argument. russh documents that "the
/// key exchange is signed with the key the certificate contains", and it
/// verifies that signature before calling this. So a server can only present a
/// certificate for the pinned key if it holds that key's private half - the
/// same thing it must prove to present the key bare. An attacker with their
/// own CA cannot mint a certificate that gets them in, because they would
/// still have to sign the handshake with a key whose fingerprint we refuse.
///
/// The CA signature, validity window and principals are therefore NOT
/// consulted. There is no CA to check against here: the trust anchor is the
/// pinned key itself, not a delegation. Treating a certificate as trusted
/// because it is a certificate would accept keys the pin never named, which
/// is the failure this function exists to prevent.
///
/// Accepting it also means a host that later starts presenting a certificate
/// for the same key keeps working rather than failing to connect.
///
/// Comparison tolerates a `SHA256:` prefix on either side, and is otherwise
/// exact - base64 is case-significant.
pub(crate) fn sftp_host_key_matches(
    presented: &russh::keys::PublicKeyOrCertificate,
    expected: &str,
) -> bool {
    use russh::keys::PublicKeyOrCertificate;
    use russh::keys::HashAlg;
    let got = match presented {
        PublicKeyOrCertificate::PublicKey { key, .. } => {
            key.fingerprint(HashAlg::Sha256).to_string()
        }
        PublicKeyOrCertificate::Certificate(cert) => {
            cert.public_key().fingerprint(HashAlg::Sha256).to_string()
        }
    };
    let norm = |s: &str| s.trim().trim_start_matches("SHA256:").to_string();
    norm(&got) == norm(expected)
}


/// SFTP host-key pinning. This is the check that decides whether we are
/// talking to the right server, so it is tested against real OpenSSH key
/// material rather than mocks.
///
/// Two ed25519 host keys and one genuine host certificate, generated with
/// `ssh-keygen` and pasted verbatim. The certificate certifies key A and is
/// signed by a separate CA, which is the shape russh 0.63 can now hand to
/// `check_server_key`.
#[cfg(test)]
mod sftp_host_key_tests {
    use super::sftp_host_key_matches;
    use russh::keys::ssh_key::{Certificate, PublicKey};
    use russh::keys::PublicKeyOrCertificate;

    pub(super) const A_PUB: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHNVp/MHziYS4wV2vfmafB+E18nSV2BaMmWYWkE84KvN host-a";
    pub(super) const A_FP: &str = "SHA256:1DIjFMJ6GUWygd6cLo4NLs110cetW5xyQ2G14cRCLvo";
    pub(super) const B_PUB: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIP3A5IyXwvuYr2UKxn6b7Cojrd3YdI8NnzSLGM7rk+QH host-b";
    pub(super) const B_FP: &str = "SHA256:/pqI89pghckzSXZ9Bv/gh591hqgcKir1JWVadnrr+uQ";
    /// A host certificate for key A, signed by an unrelated CA.
    pub(super) const A_CERT: &str = "ssh-ed25519-cert-v01@openssh.com AAAAIHNzaC1lZDI1NTE5LWNlcnQtdjAxQG9wZW5zc2guY29tAAAAIHmHd8n5oQYlP+gkjXwD4kYvou8OvSLgxS8IH4ETkeccAAAAIHNVp/MHziYS4wV2vfmafB+E18nSV2BaMmWYWkE84KvNAAAAAAAAAAAAAAACAAAAC2hvc3QtYS1jZXJ0AAAAFAAAABBzZnRwLmV4YW1wbGUuY29tAAAAAGqOyLUAAAAAfU7uNQAAAAAAAAAAAAAAAAAAADMAAAALc3NoLWVkMjU1MTkAAAAgKC5vUjky6nk4ceKsLufuOAGlIT3wkfHjOzg+FsstFW0AAABTAAAAC3NzaC1lZDI1NTE5AAAAQOXEYugiHUPCBT01h6WSbhBBv/Dt7JI1fQ5epfAxVWf2kgKo7Qd1MdOvK0m8y2PAannkUXMx3KcHFAT/m9982QQ= host-a";

    pub(super) fn raw_key(openssh: &str) -> PublicKeyOrCertificate {
        PublicKeyOrCertificate::PublicKey {
            key: PublicKey::from_openssh(openssh).expect("parses"),
            hash_alg: None,
        }
    }

    fn cert(openssh: &str) -> PublicKeyOrCertificate {
        PublicKeyOrCertificate::Certificate(Certificate::from_openssh(openssh).expect("parses"))
    }

    #[test]
    fn the_pinned_key_is_accepted() {
        assert!(sftp_host_key_matches(&raw_key(A_PUB), A_FP));
    }

    /// The whole point. A different host key must be refused, or pinning is
    /// decoration.
    #[test]
    fn a_different_key_is_refused() {
        assert!(
            !sftp_host_key_matches(&raw_key(B_PUB), A_FP),
            "a server presenting a key other than the pinned one must be refused"
        );
        assert!(!sftp_host_key_matches(&raw_key(A_PUB), B_FP));
    }

    /// russh 0.63 can hand us a certificate where 0.62 always handed a key.
    /// A certificate FOR the pinned key is that key, so it is accepted - the
    /// server proved possession of the private half before this ran.
    #[test]
    fn a_certificate_for_the_pinned_key_is_accepted() {
        assert!(
            sftp_host_key_matches(&cert(A_CERT), A_FP),
            "a host that starts presenting a certificate for the same key must \
             keep working, not fail to connect"
        );
    }

    /// The new code path's real risk: reading a certificate as trusted because
    /// it is a certificate, rather than because it certifies the pinned key.
    #[test]
    fn a_certificate_for_a_different_key_is_refused() {
        assert!(
            !sftp_host_key_matches(&cert(A_CERT), B_FP),
            "a certificate is not a free pass - it must certify the pinned key, \
             and this one certifies a different one"
        );
    }

    /// The certificate's OWN fingerprint is not the certified key's, and is not
    /// what a user pins. Matching on it would accept the wrong host.
    #[test]
    fn the_pin_is_compared_against_the_certified_key_not_the_certificate_blob() {
        let c = Certificate::from_openssh(A_CERT).expect("parses");
        let inner = c
            .public_key()
            .fingerprint(russh::keys::HashAlg::Sha256)
            .to_string();
        assert_eq!(
            inner.trim_start_matches("SHA256:"),
            A_FP.trim_start_matches("SHA256:"),
            "the certified key must be key A"
        );
    }

    #[test]
    fn the_sha256_prefix_is_optional_on_either_side() {
        let bare = A_FP.trim_start_matches("SHA256:");
        assert!(sftp_host_key_matches(&raw_key(A_PUB), bare));
        assert!(sftp_host_key_matches(&raw_key(A_PUB), A_FP));
        assert!(sftp_host_key_matches(&raw_key(A_PUB), &format!("  {}  ", A_FP)));
    }

    /// Base64 is case-significant, so a fingerprint that differs only in case
    /// is a different key and must not be waved through.
    #[test]
    fn comparison_stays_case_sensitive() {
        assert!(
            !sftp_host_key_matches(&raw_key(A_PUB), &A_FP.to_lowercase()),
            "lower-casing a base64 fingerprint makes it a different value"
        );
    }

    #[test]
    fn nonsense_never_matches() {
        for junk in ["", "SHA256:", "not-a-fingerprint", "SHA256:AAAA"] {
            assert!(
                !sftp_host_key_matches(&raw_key(A_PUB), junk),
                "{junk:?} must not match"
            );
        }
    }
}

/// Where the workspace remembers SFTP host keys it has seen.
///
/// `None` when there is no workspace, which is the case in a bare unit test or
/// an embedded call with nothing configured. Without somewhere to remember, the
/// policy below degrades to the old accept-anything behaviour rather than
/// refusing every connection.
pub(crate) fn known_hosts_path() -> Option<std::path::PathBuf> {
    let ws = std::env::var("DUCKLE_WORKSPACE").ok().filter(|s| !s.is_empty())?;
    Some(std::path::Path::new(&ws).join(".duckle").join("known_hosts"))
}

/// Fingerprints already recorded for `host:port`.
///
/// The format is one `host:port SHA256:fingerprint` per line, `#` for comments.
/// Deliberately NOT OpenSSH's known_hosts: that format carries hashed
/// hostnames, per-algorithm entries and revocation markers, and half-reading it
/// would be worse than not claiming to read it at all. This is greppable, and a
/// line can be deleted by hand, which is the escape hatch when a host really
/// does rotate its key.
///
/// Several lines for one host are allowed and any of them matches - an SFTP
/// service behind a load balancer legitimately answers with a different key per
/// node. They only accumulate when a human adds them: a key that is not already
/// listed is refused, never quietly appended.
pub(crate) fn read_known_hosts(path: &std::path::Path, hostport: &str) -> Vec<String> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            let (h, fp) = l.split_once(char::is_whitespace)?;
            (h == hostport).then(|| normalize_fingerprint(fp.trim()))
        })
        .collect()
}

/// Record a host key on first sight. Best-effort: a workspace that cannot be
/// written still connects, because refusing to talk to a server over a failed
/// bookkeeping write would be a worse failure than the one being prevented.
fn record_known_host(path: &std::path::Path, hostport: &str, fingerprint: &str) {
    if let Some(parent) = path.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return;
        }
    }
    use std::io::Write as _;
    let line = format!("{} {}\n", hostport, fingerprint);
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut f| f.write_all(line.as_bytes()));
}

/// Strip the `SHA256:` prefix and surrounding space. Base64 is
/// case-significant, so nothing else is normalized.
pub(crate) fn normalize_fingerprint(s: &str) -> String {
    s.trim().trim_start_matches("SHA256:").to_string()
}

/// The SHA256 fingerprint of whatever the server presented.
///
/// A certificate is reduced to the key it certifies. See
/// `sftp_host_key_matches` for why that key, and not the certificate, is the
/// thing worth comparing.
pub(crate) fn presented_fingerprint(presented: &russh::keys::PublicKeyOrCertificate) -> String {
    use russh::keys::HashAlg;
    use russh::keys::PublicKeyOrCertificate;
    match presented {
        PublicKeyOrCertificate::PublicKey { key, .. } => {
            key.fingerprint(HashAlg::Sha256).to_string()
        }
        PublicKeyOrCertificate::Certificate(cert) => {
            cert.public_key().fingerprint(HashAlg::Sha256).to_string()
        }
    }
}

/// Decide whether to talk to this server, and say why not when the answer is no.
///
/// Three policies, in order of strength:
///
/// 1. **A pinned fingerprint wins outright.** Match or refuse; the known-hosts
///    file is not consulted, because the user named the key explicitly.
/// 2. **Otherwise, trust on first use - and actually remember it.** The first
///    key seen for a host is recorded, and a later connection offering a
///    different key is refused. Previously an unpinned connection accepted any
///    key on every connection, which is not trust-on-first-use at all: it means
///    a machine-in-the-middle is undetected on the first connection AND every
///    one after it.
/// 3. **`DUCKLE_SFTP_HOST_KEY_POLICY=accept-any` opts out**, for a host whose
///    key genuinely changes per connection. It is an env var rather than a node
///    field on purpose: it is an operator's decision about a machine, not a
///    property of a pipeline, and it should be visible in the deployment rather
///    than buried in a saved document.
pub(crate) fn verify_sftp_host_key(
    presented: &russh::keys::PublicKeyOrCertificate,
    pinned: Option<&str>,
    hostport: &str,
) -> Result<(), String> {
    if let Some(want) = pinned {
        return if sftp_host_key_matches(presented, want) {
            Ok(())
        } else {
            Err(format!(
                "sftp: {} presented host key {}, which does not match the pinned \
                 fingerprint {}. Refusing to connect.",
                hostport,
                presented_fingerprint(presented),
                want.trim()
            ))
        };
    }

    if std::env::var("DUCKLE_SFTP_HOST_KEY_POLICY")
        .map(|v| v.trim().eq_ignore_ascii_case("accept-any"))
        .unwrap_or(false)
    {
        return Ok(());
    }

    let got = presented_fingerprint(presented);
    let path = match known_hosts_path() {
        Some(p) => p,
        // Nothing to remember with. Behave as before rather than refusing every
        // connection in a workspace-less context.
        None => return Ok(()),
    };
    let known = read_known_hosts(&path, hostport);
    if known.is_empty() {
        record_known_host(&path, hostport, &got);
        return Ok(());
    }
    if known.iter().any(|k| *k == normalize_fingerprint(&got)) {
        return Ok(());
    }
    Err(format!(
        "sftp: {} presented host key {}, but this workspace has seen a different \
         key for it. Refusing to connect - this is what a machine-in-the-middle \
         looks like, and it is also what a legitimate key rotation looks like. \
         If the change is expected, remove the line for {} from {} and the new \
         key will be recorded on the next connection.",
        hostport,
        got,
        hostport,
        path.display()
    ))
}


/// Unpinned SFTP connections: trust on first use, and actually remember it.
///
/// Before this, an unpinned connection returned `Ok(true)` for any key on
/// every connection. That is not trust-on-first-use - nothing was trusted and
/// nothing was remembered, so a machine-in-the-middle was undetected on the
/// first connection and every one after. These tests pin the behaviour that
/// replaced it.
#[cfg(test)]
mod sftp_known_hosts_tests {
    use super::sftp_host_key_tests::{raw_key, A_FP, A_PUB, B_FP, B_PUB};
    use super::{read_known_hosts, verify_sftp_host_key};

    use crate::util::workspace_env_guard as guard;

    struct Workspace {
        _dir: tempfile::TempDir,
        _g: std::sync::MutexGuard<'static, ()>,
    }
    impl Workspace {
        fn new() -> Self {
            let g = guard();
            let dir = tempfile::tempdir().unwrap();
            std::env::set_var("DUCKLE_WORKSPACE", dir.path());
            std::env::remove_var("DUCKLE_SFTP_HOST_KEY_POLICY");
            Self { _dir: dir, _g: g }
        }
        fn known_hosts(&self) -> std::path::PathBuf {
            self._dir.path().join(".duckle").join("known_hosts")
        }
    }

    #[test]
    fn the_first_key_seen_is_accepted_and_recorded() {
        let ws = Workspace::new();
        assert!(verify_sftp_host_key(&raw_key(A_PUB), None, "sftp.example.com:22").is_ok());
        let recorded = read_known_hosts(&ws.known_hosts(), "sftp.example.com:22");
        assert_eq!(
            recorded,
            vec![A_FP.trim_start_matches("SHA256:").to_string()],
            "the key must be written down, or nothing can notice it changing"
        );
    }

    /// The reason this exists.
    #[test]
    fn a_changed_key_is_refused() {
        let ws = Workspace::new();
        verify_sftp_host_key(&raw_key(A_PUB), None, "sftp.example.com:22").expect("first is fine");
        let err = verify_sftp_host_key(&raw_key(B_PUB), None, "sftp.example.com:22")
            .expect_err("a different key for a known host must be refused");
        assert!(err.contains("different key"), "message should say what happened: {err}");
        assert!(
            err.contains(&ws.known_hosts().display().to_string()),
            "message should name the file to edit: {err}"
        );
        // And it must not have quietly appended itself.
        assert_eq!(
            read_known_hosts(&ws.known_hosts(), "sftp.example.com:22").len(),
            1,
            "a refused key must not be recorded, or the next connection would accept it"
        );
    }

    #[test]
    fn the_same_key_on_a_later_connection_is_accepted() {
        let _ws = Workspace::new();
        for _ in 0..3 {
            assert!(verify_sftp_host_key(&raw_key(A_PUB), None, "sftp.example.com:22").is_ok());
        }
    }

    /// Host and port together are the identity: the same hostname on another
    /// port is a different service and gets its own entry.
    #[test]
    fn hosts_are_tracked_separately() {
        let ws = Workspace::new();
        verify_sftp_host_key(&raw_key(A_PUB), None, "a.example.com:22").expect("a");
        verify_sftp_host_key(&raw_key(B_PUB), None, "b.example.com:22").expect("b");
        verify_sftp_host_key(&raw_key(B_PUB), None, "a.example.com:2222").expect("other port");
        assert_eq!(read_known_hosts(&ws.known_hosts(), "a.example.com:22").len(), 1);
        assert_eq!(read_known_hosts(&ws.known_hosts(), "b.example.com:22").len(), 1);
        assert_eq!(read_known_hosts(&ws.known_hosts(), "a.example.com:2222").len(), 1);
        // And key B is still refused on a.example.com:22 despite being known elsewhere.
        assert!(verify_sftp_host_key(&raw_key(B_PUB), None, "a.example.com:22").is_err());
    }

    /// Several keys for one host is the load-balancer case. They only
    /// accumulate when a human writes them, which is what the next test checks.
    #[test]
    fn any_key_a_human_listed_for_the_host_is_accepted() {
        let ws = Workspace::new();
        let path = ws.known_hosts();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!(
                "# a cluster behind one name\n\
                 sftp.example.com:22 {A_FP}\n\
                 sftp.example.com:22 {B_FP}\n"
            ),
        )
        .unwrap();
        assert!(verify_sftp_host_key(&raw_key(A_PUB), None, "sftp.example.com:22").is_ok());
        assert!(verify_sftp_host_key(&raw_key(B_PUB), None, "sftp.example.com:22").is_ok());
    }

    #[test]
    fn a_pin_outranks_the_known_hosts_file() {
        let ws = Workspace::new();
        let path = ws.known_hosts();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // The file says key B is fine for this host...
        std::fs::write(&path, format!("sftp.example.com:22 {B_FP}\n")).unwrap();
        // ...but the pipeline pinned key A, and the pin is the stronger claim.
        assert!(
            verify_sftp_host_key(&raw_key(B_PUB), Some(A_FP), "sftp.example.com:22").is_err(),
            "a pinned fingerprint must not be softened by what the file remembers"
        );
        assert!(verify_sftp_host_key(&raw_key(A_PUB), Some(A_FP), "sftp.example.com:22").is_ok());
    }

    #[test]
    fn a_pin_mismatch_says_what_was_presented() {
        let _ws = Workspace::new();
        let err = verify_sftp_host_key(&raw_key(B_PUB), Some(A_FP), "sftp.example.com:22")
            .expect_err("mismatch");
        assert!(err.contains(B_FP.trim_start_matches("SHA256:")), "names the key seen: {err}");
        assert!(err.contains(A_FP.trim_start_matches("SHA256:")), "names the key wanted: {err}");
    }

    #[test]
    fn the_opt_out_accepts_anything() {
        let _ws = Workspace::new();
        verify_sftp_host_key(&raw_key(A_PUB), None, "sftp.example.com:22").expect("first");
        std::env::set_var("DUCKLE_SFTP_HOST_KEY_POLICY", "accept-any");
        assert!(
            verify_sftp_host_key(&raw_key(B_PUB), None, "sftp.example.com:22").is_ok(),
            "the documented escape hatch for a host whose key changes per connection"
        );
        std::env::remove_var("DUCKLE_SFTP_HOST_KEY_POLICY");
        // ...and removing it restores the refusal, so the opt-out is not sticky.
        assert!(verify_sftp_host_key(&raw_key(B_PUB), None, "sftp.example.com:22").is_err());
    }

    /// Comments and blank lines are for humans editing the file by hand, which
    /// is the documented way to accept a rotated key.
    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let ws = Workspace::new();
        let path = ws.known_hosts();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!("\n# rotated 2026-08-01\n\n   sftp.example.com:22 {A_FP}   \n"),
        )
        .unwrap();
        assert!(verify_sftp_host_key(&raw_key(A_PUB), None, "sftp.example.com:22").is_ok());
    }

    /// With no workspace there is nowhere to remember, so the old behaviour
    /// stands rather than refusing every connection.
    #[test]
    fn without_a_workspace_it_does_not_refuse() {
        let _g = guard();
        let saved = std::env::var("DUCKLE_WORKSPACE").ok();
        std::env::remove_var("DUCKLE_WORKSPACE");
        std::env::remove_var("DUCKLE_SFTP_HOST_KEY_POLICY");
        let r = verify_sftp_host_key(&raw_key(A_PUB), None, "sftp.example.com:22");
        if let Some(v) = saved {
            std::env::set_var("DUCKLE_WORKSPACE", v);
        }
        assert!(r.is_ok(), "no workspace means nowhere to record; do not break the connection");
    }
}

/// Host-key verifier for src.xml's SFTP reader. A pinned SHA256 fingerprint
/// refuses any other server key; without one, the first key seen for the host
/// is remembered and a later change is refused. See `verify_sftp_host_key`.
struct SftpVerifier {
    expected: Option<String>,
    hostport: String,
    refused: std::sync::Arc<std::sync::Mutex<Option<String>>>,
}

impl russh::client::Handler for SftpVerifier {
    type Error = russh::Error;
    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        match verify_sftp_host_key(server_public_key, self.expected.as_deref(), &self.hostport) {
            Ok(()) => Ok(true),
            Err(why) => {
                *self.refused.lock().unwrap() = Some(why);
                Ok(false)
            }
        }
    }
}

/// One remote file over SFTP, exposed as a blocking `std::io::Read`. It owns the
/// tokio runtime that drives the russh run-loop plus the live SSH handle and
/// SFTP session (dropping either would close the stream), and each `read()`
/// pulls a single SFTP READ round-trip - nothing is buffered whole, which is
/// what lets src.xml stream a multi-GB remote file (issue #186). Mirrors the
/// connect / auth of run_sftp_source but keeps the file open instead of slurping
/// it into a base64 column.
struct SftpFileReader {
    // Fields drop in declaration order, so `rt` drops first. That is safe: the
    // russh / russh-sftp teardown (File and session close) only pushes to
    // unbounded channels and needs no running runtime. `rt` is a current-thread
    // runtime, so the connection run-loop only advances while we are inside
    // `block_on` - which is exactly when `read()` runs.
    rt: tokio::runtime::Runtime,
    file: russh_sftp::client::fs::File,
    _sftp: russh_sftp::client::SftpSession,
    _session: russh::client::Handle<SftpVerifier>,
}

impl SftpFileReader {
    #[allow(clippy::too_many_arguments)]
    fn open(
        host: &str,
        port: u16,
        user: &str,
        password: Option<&str>,
        private_key: Option<&str>,
        key_passphrase: Option<&str>,
        host_fingerprint: Option<&str>,
        remote_path: &str,
    ) -> Result<Self, EngineError> {
        use russh_sftp::client::SftpSession;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Query(format!("xml/sftp: tokio rt: {}", e)))?;
        let (session, sftp, file) = rt
            .block_on(async {
                let config = std::sync::Arc::new(russh::client::Config::default());
                let refused = std::sync::Arc::new(std::sync::Mutex::new(None));
                let handler = SftpVerifier {
                    expected: host_fingerprint.map(|s| s.to_string()),
                    hostport: format!("{}:{}", host, port),
                    refused: refused.clone(),
                };
                let mut session = russh::client::connect(config, (host, port), handler)
                    .await
                    .map_err(|e| match refused.lock().unwrap().take() {
                        Some(why) => why,
                        None => format!("connect {}:{}: {}", host, port, e),
                    })?;
                let authed = if let Some(pem) = private_key {
                    let key = russh::keys::decode_secret_key(pem, key_passphrase)
                        .map_err(|e| format!("private key: {}", e))?;
                    let with_alg = russh::keys::PrivateKeyWithHashAlg::new(
                        std::sync::Arc::new(key),
                        Some(russh::keys::HashAlg::Sha256),
                    );
                    session
                        .authenticate_publickey(user, with_alg)
                        .await
                        .map_err(|e| format!("publickey auth: {}", e))?
                        .success()
                } else if let Some(pw) = password {
                    session
                        .authenticate_password(user, pw)
                        .await
                        .map_err(|e| format!("password auth: {}", e))?
                        .success()
                } else {
                    return Err("no credentials: set a password or a private key".to_string());
                };
                if !authed {
                    return Err("authentication failed".to_string());
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
                let file = sftp
                    .open(remote_path)
                    .await
                    .map_err(|e| format!("open {}: {}", remote_path, e))?;
                Ok::<_, String>((session, sftp, file))
            })
            .map_err(|e| EngineError::Query(format!("xml/sftp: {}", e)))?;
        Ok(SftpFileReader {
            rt,
            file,
            _sftp: sftp,
            _session: session,
        })
    }
}

impl std::io::Read for SftpFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use tokio::io::AsyncReadExt;
        // Plain sync context (the XML parser calls this), so block_on is legal;
        // returns 0 at EOF, matching std::io::Read.
        self.rt.block_on(self.file.read(buf))
    }
}

/// A file sink that a source is allowed to write itself.
///
/// When an Oracle source is the only producer feeding a plain Parquet sink,
/// the rows would otherwise be encoded to Parquet once by the source, decoded
/// by DuckDB and encoded again by the sink. Handing the source the sink's own
/// destination collapses that to a single encode. `written` is how the source
/// tells the executor it actually took the path, so the sink is skipped ONLY
/// when the file really was produced; every path that declines leaves it false
/// and the sink runs normally.
pub(crate) struct DirectSinkTarget<'a> {
    pub path: &'a str,
    pub compression: Option<&'a str>,
    pub written: &'a std::sync::atomic::AtomicBool,
}

/// How a parallel Oracle read is split. Only ever built when the read could be
/// pinned to one SCN, so every session in it observes the same snapshot.
#[cfg(feature = "oracle")]
struct OracleParallelPlan {
    /// The bare, quoted split column. Bands compare against it directly so
    /// Oracle can prune partitions and use an index on it.
    column: String,
    /// True when the column is a DATE / TIMESTAMP, so band boundaries are
    /// emitted as date literals rather than plain numbers.
    is_datetime: bool,
    degree: usize,
    /// The system change number every session reads as of.
    scn: u64,
    lo: f64,
    hi: f64,
    /// The user's query, ready to wrap as a subquery.
    body: String,
}

// Per-thread landing zone for the Arrow builders while one result set encodes.
//
// `ResultSet<Row>` rebuilds every `SqlValue` for every row: measured at 11.5s
// of a 42.7s fetch on a 236-column, 1.47M-row table, against a 31.2s floor for
// the same query with no clone at all. The borrowed row that would avoid it is
// only reachable through `Stmt`, which the crate keeps `pub(crate)`.
//
// What is reachable is `query_as::<T>`, which hands `RowValue::get` the
// borrowed row and never builds an owned `Row` unless `T` is `Row`. So the
// encode moves into a `RowValue` impl - and since `get` is handed nothing but
// the row, the builders have to reach it through here.
//
// Single-threaded by construction: one result set encodes at a time, on the
// thread that installed the builders, and `get` is called synchronously from
// that same thread's iteration.
#[cfg(feature = "oracle")]
thread_local! {
    static ENCODE_SINK: std::cell::RefCell<Option<EncodeSink>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "oracle")]
struct EncodeSink {
    builders: Vec<OraCol>,
    /// The first encode failure. `RowValue::get` can only return the driver's
    /// error type, so ours is parked here and the driving loop checks it after
    /// every row - which is why later rows are skipped once it is set.
    err: Option<EngineError>,
    encode_nanos: u128,
    trace: bool,
}

/// Widest value seen so far in one column being measured before it is typed.
///
/// `unusable` latches: one value we cannot map - scientific notation, a
/// non-NUMBER, or something wider than DECIMAL's 38 digits - and the column
/// goes back to being carried as text. Refusing is always available and always
/// correct, so nothing here ever has to guess a width.
#[cfg(feature = "oracle")]
#[derive(Clone, Copy, Default)]
struct ProbeWidth {
    int_digits: u32,
    scale: u32,
    seen: bool,
    unusable: bool,
}

// Columns being measured by `ProbeRow`, in the probe query's column order.
#[cfg(feature = "oracle")]
thread_local! {
    static PROBE_WIDTHS: std::cell::RefCell<Vec<ProbeWidth>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Measures rather than stores: same borrowed-row trick as [`EncodeRow`].
#[cfg(feature = "oracle")]
struct ProbeRow;

#[cfg(feature = "oracle")]
impl oracle::RowValue for ProbeRow {
    fn get(row: &oracle::Row) -> oracle::Result<Self> {
        use oracle::sql_type::InnerValue;
        PROBE_WIDTHS.with(|cell| {
            let mut widths = cell.borrow_mut();
            for (i, w) in widths.iter_mut().enumerate() {
                if w.unusable {
                    continue;
                }
                let Some(sv) = row.sql_values().get(i) else {
                    w.unusable = true;
                    continue;
                };
                // A NULL tells us nothing about the width, which is fine: a
                // column that is entirely NULL is left to the text path.
                if matches!(sv.is_null(), Ok(true)) {
                    continue;
                }
                match sv.as_inner_value() {
                    Ok(InnerValue::Number(text)) => {
                        match DuckdbEngine::oracle_number_width(text) {
                            Some((int_digits, scale)) => {
                                w.seen = true;
                                w.int_digits = w.int_digits.max(int_digits);
                                w.scale = w.scale.max(scale);
                            }
                            None => w.unusable = true,
                        }
                    }
                    _ => w.unusable = true,
                }
            }
        });
        Ok(ProbeRow)
    }
}

/// What one row turns into, without ever materialising an owned `Row`.
///
/// Empty on the Arrow path, where the row was consumed for its side effect on
/// the builders. Carries the row as JSON on the NDJSON fallback, which needs
/// the same borrowed row and so may as well skip the clone too. One result set
/// serves both paths, so the query is only ever executed once.
#[cfg(feature = "oracle")]
struct EncodeRow(Option<JsonValue>);

#[cfg(feature = "oracle")]
impl oracle::RowValue for EncodeRow {
    fn get(row: &oracle::Row) -> oracle::Result<Self> {
        let encoded = ENCODE_SINK.with(|cell| {
            let mut slot = cell.borrow_mut();
            let Some(sink) = slot.as_mut() else {
                return false;
            };
            if sink.err.is_none() {
                let t = if sink.trace {
                    Some(std::time::Instant::now())
                } else {
                    None
                };
                for (i, b) in sink.builders.iter_mut().enumerate() {
                    if let Err(e) = b.push(row, i) {
                        sink.err = Some(e);
                        break;
                    }
                }
                if let Some(t) = t {
                    sink.encode_nanos += t.elapsed().as_nanos();
                }
            }
            true
        });
        if encoded {
            return Ok(EncodeRow(None));
        }
        // No builders installed: this is the NDJSON fallback.
        let mut obj = serde_json::Map::new();
        for (i, info) in row.column_info().iter().enumerate() {
            obj.insert(
                info.name().to_string(),
                DuckdbEngine::oracle_cell_to_json(row, i),
            );
        }
        Ok(EncodeRow(Some(JsonValue::Object(obj))))
    }
}

/// Installs the builders for one result set and clears them on the way out,
/// including on an early return or a panic.
#[cfg(feature = "oracle")]
struct EncodeGuard;

#[cfg(feature = "oracle")]
impl EncodeGuard {
    fn install(builders: Vec<OraCol>, trace: bool) -> Self {
        ENCODE_SINK.with(|c| {
            *c.borrow_mut() = Some(EncodeSink {
                builders,
                err: None,
                encode_nanos: 0,
                trace,
            });
        });
        EncodeGuard
    }

    /// Reach the builders from the driving loop. Never call this from inside
    /// `RowValue::get`; that would re-borrow the same RefCell.
    fn with<R>(&self, f: impl FnOnce(&mut EncodeSink) -> R) -> R {
        ENCODE_SINK.with(|c| {
            f(c.borrow_mut()
                .as_mut()
                .expect("encode sink is installed for the whole result set"))
        })
    }
}

#[cfg(feature = "oracle")]
impl Drop for EncodeGuard {
    fn drop(&mut self) {
        ENCODE_SINK.with(|c| {
            *c.borrow_mut() = None;
        });
    }
}

/// One concretely-typed Arrow builder per Oracle column for the #221 fast path.
///
/// The alternative, `Vec<Box<dyn ArrayBuilder>>`, forces an `as_any_mut()` +
/// `downcast_mut()` on every appended cell. The column's type is known once,
/// when the schema is pinned, so this resolves it there instead: the per-cell
/// work is a match on a small enum the compiler can lay out flat.
///
/// NULL is always appended as a null, never a sentinel, so a NULL never becomes
/// 0 or an empty string.
#[cfg(feature = "oracle")]
enum OraCol {
    I64(arrow_array::builder::Int64Builder),
    F64(arrow_array::builder::Float64Builder),
    F32(arrow_array::builder::Float32Builder),
    Str(arrow_array::builder::StringBuilder),
    Bin(arrow_array::builder::BinaryBuilder),
    Ts(arrow_array::builder::TimestampMicrosecondBuilder),
    /// Carries the declared scale so the text Oracle hands back can be rescaled.
    Dec(arrow_array::builder::Decimal128Builder, i8),
}

#[cfg(feature = "oracle")]
impl OraCol {
    /// Append cell `i` of `row`.
    ///
    /// Fast path: read the value straight out of ODPI's array-fetch buffer via
    /// `as_inner_value()`. `Char` and `Number` come back as borrowed `&[u8]` /
    /// `&str` pointing into that buffer, so a VARCHAR2 or a scaled NUMBER costs
    /// no allocation at all - the old `row.get::<Option<String>>()` allocated
    /// and freed a String per cell, ~19M of them on a 1M-row pull of this
    /// shape. The value is copied once, directly into the Arrow buffer.
    ///
    /// The buffer's native type is chosen by ODPI and is not always the one the
    /// pinned Arrow schema expects (the schema is decided from the *declared*
    /// Oracle type). Any mismatch falls through to `push_via_get`, which is the
    /// original typed `FromSql` path, so a surprising native shape converts
    /// correctly instead of being misread.
    fn push(&mut self, row: &oracle::Row, i: usize) -> Result<(), EngineError> {
        use oracle::sql_type::InnerValue;
        let sv = match row.sql_values().get(i) {
            Some(sv) => sv,
            None => return self.push_via_get(row, i),
        };
        match sv.is_null() {
            Ok(true) => {
                self.append_null();
                return Ok(());
            }
            Ok(false) => {}
            Err(_) => return self.push_via_get(row, i),
        }
        let inner = match sv.as_inner_value() {
            Ok(v) => v,
            Err(_) => return self.push_via_get(row, i),
        };
        match (&mut *self, inner) {
            (OraCol::I64(b), InnerValue::Int64(v)) => b.append_value(v),
            (OraCol::F64(b), InnerValue::Double(v)) => b.append_value(v),
            (OraCol::F32(b), InnerValue::Float(v)) => b.append_value(v),
            (OraCol::Str(b), InnerValue::Char(bytes)) => match std::str::from_utf8(bytes) {
                Ok(s) => b.append_value(s),
                // Not valid UTF-8 in the session charset: let the driver's own
                // conversion decide rather than corrupting the text here.
                Err(_) => return self.push_via_get(row, i),
            },
            (OraCol::Bin(b), InnerValue::Raw(bytes)) => b.append_value(bytes),
            (OraCol::Ts(b), InnerValue::Timestamp(t)) => {
                let micros = chrono::NaiveDate::from_ymd_opt(
                    t.year as i32,
                    t.month as u32,
                    t.day as u32,
                )
                .and_then(|d| {
                    d.and_hms_nano_opt(
                        t.hour as u32,
                        t.minute as u32,
                        t.second as u32,
                        t.fsecond,
                    )
                })
                .map(|dt| dt.and_utc().timestamp_micros());
                b.append_option(micros)
            }
            (OraCol::Dec(b, scale), InnerValue::Number(text)) => {
                match DuckdbEngine::oracle_decimal_to_i128(text, *scale) {
                    Some(n) => b.append_value(n),
                    None => {
                        return Err(EngineError::Query(format!(
                            "oracle: column {} value '{}' does not fit the declared \
                             DECIMAL scale {}",
                            i + 1,
                            text,
                            scale
                        )))
                    }
                }
            }
            _ => return self.push_via_get(row, i),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        match self {
            OraCol::I64(b) => b.append_null(),
            OraCol::F64(b) => b.append_null(),
            OraCol::F32(b) => b.append_null(),
            OraCol::Str(b) => b.append_null(),
            OraCol::Bin(b) => b.append_null(),
            OraCol::Ts(b) => b.append_null(),
            OraCol::Dec(b, _) => b.append_null(),
        }
    }

    fn push_via_get(&mut self, row: &oracle::Row, i: usize) -> Result<(), EngineError> {
        let bad = |what: &str, e: oracle::Error| {
            EngineError::Query(format!("oracle: column {} as {}: {}", i + 1, what, e))
        };
        match self {
            OraCol::I64(b) => b.append_option(row.get(i).map_err(|e| bad("BIGINT", e))?),
            OraCol::F64(b) => b.append_option(row.get(i).map_err(|e| bad("DOUBLE", e))?),
            OraCol::F32(b) => b.append_option(row.get(i).map_err(|e| bad("FLOAT", e))?),
            OraCol::Str(b) => {
                let v: Option<String> = row.get(i).map_err(|e| bad("VARCHAR", e))?;
                b.append_option(v)
            }
            OraCol::Bin(b) => {
                let v: Option<Vec<u8>> = row.get(i).map_err(|e| bad("BLOB", e))?;
                b.append_option(v)
            }
            OraCol::Ts(b) => {
                // The oracle crate's own Timestamp, not chrono: FromSql for
                // chrono types needs a cargo feature this crate does not enable.
                let v: Option<oracle::sql_type::Timestamp> =
                    row.get(i).map_err(|e| bad("TIMESTAMP", e))?;
                b.append_option(v.and_then(|t| {
                    chrono::NaiveDate::from_ymd_opt(t.year(), t.month(), t.day())
                        .and_then(|d| {
                            d.and_hms_nano_opt(t.hour(), t.minute(), t.second(), t.nanosecond())
                        })
                        .map(|dt| dt.and_utc().timestamp_micros())
                }))
            }
            OraCol::Dec(b, scale) => {
                // Read as text so no digit is lost on the way in; the exact
                // value is then rescaled to the column's declared scale.
                let v: Option<String> = row.get(i).map_err(|e| bad("DECIMAL", e))?;
                match v {
                    None => b.append_null(),
                    Some(text) => match DuckdbEngine::oracle_decimal_to_i128(&text, *scale) {
                        Some(n) => b.append_value(n),
                        None => {
                            return Err(EngineError::Query(format!(
                                "oracle: column {} value '{}' does not fit the declared \
                                 DECIMAL scale {}",
                                i + 1,
                                text,
                                scale
                            )))
                        }
                    },
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> arrow_array::ArrayRef {
        use arrow_array::builder::ArrayBuilder;
        match self {
            OraCol::I64(b) => ArrayBuilder::finish(b),
            OraCol::F64(b) => ArrayBuilder::finish(b),
            OraCol::F32(b) => ArrayBuilder::finish(b),
            OraCol::Str(b) => ArrayBuilder::finish(b),
            OraCol::Bin(b) => ArrayBuilder::finish(b),
            OraCol::Ts(b) => ArrayBuilder::finish(b),
            OraCol::Dec(b, _) => ArrayBuilder::finish(b),
        }
    }
}

/// Resolve the Python that has `pixeltable` installed (#223).
///
/// DUCKLE_PIXELTABLE_PYTHON is published by the desktop app after it
/// provisions a venv with uv, the same fetch-not-bundle model as dbt. Falling
/// back to `python` on PATH lets the headless runner and CI work against an
/// existing install without the desktop app.
fn resolve_pixeltable_python() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("DUCKLE_PIXELTABLE_PYTHON") {
        if !p.is_empty() {
            return std::path::PathBuf::from(p);
        }
    }
    std::path::PathBuf::from(if cfg!(windows) { "python.exe" } else { "python3" })
}

/// Render a Python string literal. Pixeltable table paths and file paths are
/// user data, so they are quoted rather than pasted in raw.
fn py_str(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{}'", escaped)
}

impl DuckdbEngine {
    /// src.pixeltable: export a Pixeltable table to Parquet, then load it (#223).
    pub(crate) fn run_pixeltable_source(
        &self,
        db: &Path,
        spec: &PixeltableSourceSpec,
    ) -> Result<String, EngineError> {
        let safe: String = spec
            .node_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let db_name = db
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        // A DIRECTORY, not a file: export_parquet partitions its output and
        // writes part-00000.parquet and friends inside the path it is given.
        // Verified against pixeltable 0.7.1 - passing a file path silently
        // produces a directory of that name, and reading it as a single file
        // finds nothing.
        let parquet_dir = db.with_file_name(format!("{}.pxt-{}", db_name, safe));
        let _ = std::fs::remove_dir_all(&parquet_dir);

        // The query is built up the way Pixeltable's own docs do: get_table,
        // then optional select / where / limit, then export_parquet. `filter`
        // is inlined as a Pixeltable expression (e.g. `t.score > 0.8`) rather
        // than escaped, because that is what it is - the same trust level as
        // the SQL a user types into code.sql.
        let mut q = String::from("t");
        if !spec.columns.is_empty() {
            let cols: Vec<String> = spec
                .columns
                .iter()
                .map(|c| format!("t[{}]", py_str(c)))
                .collect();
            q = format!("{}.select({})", q, cols.join(", "));
        }
        if let Some(f) = &spec.filter {
            q = format!("{}.where({})", q, f);
        }
        if let Some(l) = spec.limit {
            q = format!("{}.limit({})", q, l);
        }
        // export_parquet is typed `parquet_path: Path`, and passing a str fails
        // inside pixeltable with "'str' object has no attribute 'exists'", so
        // wrap it rather than relying on duck typing.
        let script = format!(
            "import pathlib\n\
             import pixeltable as pxt\n\
             t = pxt.get_table({table})\n\
             pxt.io.export_parquet({q}, pathlib.Path({out}))\n",
            table = py_str(&spec.table),
            q = q,
            out = py_str(&parquet_dir.to_string_lossy()),
        );
        self.run_pixeltable_python(&script, "read")?;
        if !parquet_dir.is_dir() {
            return Err(EngineError::Query(format!(
                "pixeltable: {} exported nothing. Check the table path and that pixeltable is installed",
                spec.table
            )));
        }
        // Glob the part files: the export is partitioned, so a single-file read
        // would silently see none of it.
        let ppath = parquet_dir
            .join("*.parquet")
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let create = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}')",
            plan::quote_ident(&spec.node_id),
            ppath
        );
        let create_result = self.run(Some(db), &create, false);
        // Remove the temp export whether or not the load succeeded, so a failed
        // CREATE does not leak it beside the run database.
        let _ = std::fs::remove_dir_all(&parquet_dir);
        create_result?;
        Ok(format!(
            "pixeltable: materialized {} into {}",
            spec.table, spec.node_id
        ))
    }

    /// snk.pixeltable: COPY the upstream view to Parquet, then insert it (#223).
    pub(crate) fn run_pixeltable_sink(
        &self,
        db: &Path,
        spec: &PixeltableSinkSpec,
    ) -> Result<String, EngineError> {
        let safe: String = spec
            .from_view
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        let db_name = db
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let parquet_path = db.with_file_name(format!("{}.pxt-snk-{}.parquet", db_name, safe));
        let ppath = parquet_path
            .to_string_lossy()
            .replace('\\', "/")
            .replace('\'', "''");
        let copy = format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT parquet)",
            plan::quote_ident(&spec.from_view),
            ppath
        );
        self.run(Some(db), &copy, false)?;

        // `create` builds the table from the incoming rows and infers the
        // schema; `insert` requires it to exist already. Both take the Parquet
        // path directly, so no rows cross the process boundary one at a time.
        let script = if spec.mode == "create" {
            format!(
                "import pixeltable as pxt\n\
                 pxt.create_table({table}, source={src})\n",
                table = py_str(&spec.table),
                src = py_str(&parquet_path.to_string_lossy()),
            )
        } else {
            format!(
                "import pixeltable as pxt\n\
                 t = pxt.get_table({table})\n\
                 t.insert({src})\n",
                table = py_str(&spec.table),
                src = py_str(&parquet_path.to_string_lossy()),
            )
        };
        let result = self.run_pixeltable_python(&script, "write");
        let _ = std::fs::remove_file(&parquet_path);
        result?;
        Ok(format!("pixeltable: wrote {} ({})", spec.table, spec.mode))
    }

    /// Run one short Python program against the provisioned interpreter.
    fn run_pixeltable_python(&self, script: &str, op: &str) -> Result<(), EngineError> {
        let python = resolve_pixeltable_python();
        let mut cmd = std::process::Command::new(&python);
        cmd.arg("-c").arg(script);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let out = cmd.output().map_err(|e| {
            EngineError::Query(format!(
                "pixeltable {}: cannot run {}: {}. Install pixeltable, or set \
                 DUCKLE_PIXELTABLE_PYTHON to a Python that has it",
                op,
                python.display(),
                e
            ))
        })?;
        if !out.status.success() {
            let err = String::from_utf8_lossy(&out.stderr);
            // Python tracebacks put the useful line last, so keep the tail.
            let tail: String = err.trim().lines().rev().take(6).collect::<Vec<_>>().join(" | ");
            return Err(EngineError::Query(format!("pixeltable {}: {}", op, tail)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod incremental_state_tests {
    /// The two Arrow entry points are detected independently, and a script
    /// defining `transform_batches` must not be read as defining `transform`.
    #[test]
    fn the_two_arrow_entry_points_are_told_apart() {
            use super::{defines_streaming_entry, defines_vectorized_entry};

        assert!(defines_streaming_entry("def transform_batches(batch):\n return batch"));
        assert!(!defines_vectorized_entry(
            "def transform_batches(batch):\n return batch"
        ));
        assert!(defines_vectorized_entry("def transform(table):\n return table"));
        assert!(!defines_streaming_entry("def transform(table):\n return table"));
        // A nested def is a helper, not the entry point the harness calls.
        assert!(!defines_streaming_entry(
            "def outer():\n def transform_batches(b):\n return b"
        ));
        // A script may define both; streaming is tested first, so both report true
        // and the caller's ordering decides.
        let both = "def transform(table):\n return table\n\n\ndef transform_batches(batch):\n return batch";
        assert!(defines_streaming_entry(both) && defines_vectorized_entry(both));
    }


    #[test]
    fn a_confluent_framed_message_decodes_against_its_schema() {
        // Build a real Confluent-framed message the way a producer does: a zero
        // magic byte, a big-endian schema id, then a RAW Avro datum - no
        // container header, which is why the id has to name the schema.
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"Order","fields":[
                 {"name":"id","type":"long"},
                 {"name":"customer","type":"string"},
                 {"name":"total","type":"double"}
               ]}"#,
        )
        .unwrap();
        let mut rec = apache_avro::types::Record::new(&schema).unwrap();
        rec.put("id", 77i64);
        rec.put("customer", "acme");
        rec.put("total", 12.5f64);
        let datum = apache_avro::to_avro_datum(&schema, rec).unwrap();

        let mut framed = vec![0u8];
        framed.extend_from_slice(&4242u32.to_be_bytes());
        framed.extend_from_slice(&datum);

        let (id, payload) = super::confluent_envelope(&framed).expect("framed message");
        assert_eq!(id, 4242, "the schema id is a big-endian u32 after the magic byte");

        let json = super::avro_datum_to_json(&schema, payload).unwrap();
        assert_eq!(json.get("id").and_then(|v| v.as_i64()), Some(77));
        assert_eq!(json.get("customer").and_then(|v| v.as_str()), Some("acme"));
        assert_eq!(json.get("total").and_then(|v| v.as_f64()), Some(12.5));
    }

    #[test]
    fn plain_text_is_not_mistaken_for_a_framed_message() {
        // Confluent topics routinely pair a plain string key with an Avro
        // value, so an unframed field has to pass through as text rather than
        // fail the read. A zero first byte is not valid UTF-8 text, so this
        // check cannot misfire on a string.
        assert!(super::confluent_envelope(b"just-a-key").is_none());
        assert!(super::confluent_envelope(br#"{"id":1}"#).is_none());
        // Too short to carry an id, even though it starts with zero.
        assert!(super::confluent_envelope(&[0u8, 1, 2]).is_none());
        assert!(super::confluent_envelope(&[]).is_none());
        // A zero byte followed by four bytes IS the frame, even with no payload
        // left - an empty datum is the schema's problem, not the framing's.
        assert_eq!(
            super::confluent_envelope(&[0u8, 0, 0, 0, 7]).map(|(id, p)| (id, p.len())),
            Some((7, 0))
        );
    }

    #[test]
    fn a_datum_that_does_not_match_its_schema_is_an_error_not_garbage() {
        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"R","fields":[{"name":"n","type":"long"}]}"#,
        )
        .unwrap();
        // Bytes that are not a valid encoding of this record must fail loudly
        // rather than produce a plausible-looking wrong row.
        let err = super::avro_datum_to_json(&schema, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert!(err.is_err(), "malformed datum should not decode");
    }

    #[test]
    fn a_kafka_sasl_mechanism_is_recognised_or_refused() {
        let creds = |m: &str| crate::plan::KafkaSasl {
            mechanism: m.to_string(),
            username: "svc".into(),
            password: "hunter2".into(),
        };
        // The three rskafka implements, in the punctuation people actually type.
        for m in ["PLAIN", "plain", "SCRAM-SHA-256", "scram_sha_256", "SCRAM-SHA-512"] {
            assert!(
                super::kafka_sasl_config(&creds(m)).is_ok(),
                "{} should be accepted",
                m
            );
        }
        // Anything else must FAIL rather than quietly connect without
        // authenticating, which is what happened while nothing read these
        // fields at all.
        let err = super::kafka_sasl_config(&creds("GSSAPI")).unwrap_err();
        assert!(err.contains("GSSAPI"), "the error should name what was asked for: {}", err);
        assert!(
            err.contains("PLAIN") && err.contains("SCRAM-SHA-256"),
            "the error should name what IS supported: {}",
            err
        );
    }

    #[test]
    fn a_kafka_resume_point_is_only_used_for_the_stream_it_came_from() {
        // The saved offset is a position in ONE topic partition. Re-point the
        // node and the number means something else entirely, so it must be
        // ignored rather than resumed from - reading another stream's position
        // would silently skip or re-read an arbitrary amount of data.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("kafka.json");
        std::fs::write(
            &path,
            r#"{"topic":"orders","partition":0,"next_offset":4200}"#,
        )
        .unwrap();

        assert_eq!(
            super::read_kafka_offset_state(&path, "orders", 0),
            Some(4200),
            "the stream it was written for must resume"
        );
        assert_eq!(
            super::read_kafka_offset_state(&path, "shipments", 0),
            None,
            "a different topic must not resume from this offset"
        );
        assert_eq!(
            super::read_kafka_offset_state(&path, "orders", 3),
            None,
            "a different partition must not resume from this offset"
        );

        // Nothing saved yet, and anything unreadable, both mean "start where
        // the node is configured to start" rather than failing the run.
        assert_eq!(
            super::read_kafka_offset_state(&tmp.path().join("absent.json"), "orders", 0),
            None
        );
        let bad = tmp.path().join("bad.json");
        std::fs::write(&bad, "not json at all").unwrap();
        assert_eq!(super::read_kafka_offset_state(&bad, "orders", 0), None);

        // A negative offset is not a position; treat it as absent rather than
        // handing it to the broker as a sentinel and reading the wrong end.
        let neg = tmp.path().join("neg.json");
        std::fs::write(
            &neg,
            r#"{"topic":"orders","partition":0,"next_offset":-1}"#,
        )
        .unwrap();
        assert_eq!(super::read_kafka_offset_state(&neg, "orders", 0), None);

        // Offset zero IS a valid position - the start of a partition - and must
        // not be confused with "nothing saved".
        let zero = tmp.path().join("zero.json");
        std::fs::write(
            &zero,
            r#"{"topic":"orders","partition":0,"next_offset":0}"#,
        )
        .unwrap();
        assert_eq!(super::read_kafka_offset_state(&zero, "orders", 0), Some(0));
    }

    use super::{child_run_name, incremental_state_path, inherited_incremental_state};

    /// Serialised: these tests set DUCKLE_WORKSPACE, which is process-global.
    /// Shared with the SFTP known-hosts tests - see workspace_env_guard.
    use crate::util::workspace_env_guard;

    fn workspace(tag: &str) -> std::path::PathBuf {
        let ws = std::env::temp_dir().join(format!("duckle_state_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&ws);
        std::fs::create_dir_all(&ws).unwrap();
        ws
    }

    fn write_state(path: &std::path::Path, value: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, format!(r#"{{"column":"modified","value":"{value}","type":"TIMESTAMP"}}"#))
            .unwrap();
    }

    /// Each item of a For Each keeps its own watermark, once itemKey names it.
    ///
    /// Naming the child fixed collisions BETWEEN children. It did nothing for
    /// the case that actually bites: 400 tables driven through ONE child
    /// pipeline, all sharing that child's mark, so each table resumed from
    /// wherever the previous table finished and skipped everything in between.
    /// `itemKey` is what separates them, and it has to be given rather than
    /// inferred - keying on row position would move every watermark the moment
    /// the driving query is reordered.
    #[test]
    fn each_item_of_a_foreach_keeps_its_own_watermark() {
        let _g = workspace_env_guard();
        let ws = workspace("peritem");
        std::env::set_var("DUCKLE_WORKSPACE", &ws);

        // The names the REAL construction produces for two rows of one child,
        // not a restatement of them.
        let name = |item: Option<&str>| {
            child_run_name("/ws/pipelines/sync-one-table.json", item).expect("no name")
        };
        assert_eq!(name(Some("orders")), "sync-one-table@orders");
        assert_eq!(name(None), "sync-one-table", "no itemKey must keep the old single name");
        // A blank or whitespace item is not an item.
        assert_eq!(name(Some("   ")), "sync-one-table");

        let orders = incremental_state_path(Some(&name(Some("orders"))), "inc1").unwrap();
        let customers = incremental_state_path(Some(&name(Some("customers"))), "inc1").unwrap();
        assert_ne!(orders, customers, "two tables still share one watermark file");

        // Both are still under the child, so a workspace stays navigable.
        assert!(orders.to_string_lossy().contains("sync-one-table"));

        // Without an itemKey the child is one run, which is the pre-existing
        // behaviour and correct when the iterations are genuinely one load.
        let shared = incremental_state_path(Some(&name(None)), "inc1").unwrap();
        assert_ne!(shared, orders);

        std::env::remove_var("DUCKLE_WORKSPACE");
        let _ = std::fs::remove_dir_all(&ws);
    }

    /// Two different children must not share one watermark.
    ///
    /// Every sub-pipeline ran unnamed, so `state/pipeline/<node>.json` was the
    /// path for all of them. Two children driven by ctl.foreach overwrote each
    /// other's mark and each resumed from whichever ran last - which silently
    /// skips rows, the exact failure incremental loading exists to prevent.
    #[test]
    fn two_children_keep_separate_watermarks() {
        let _g = workspace_env_guard();
        let ws = workspace("split");
        std::env::set_var("DUCKLE_WORKSPACE", &ws);

        let a = incremental_state_path(Some("load-orders"), "inc1").unwrap();
        let b = incremental_state_path(Some("load-customers"), "inc1").unwrap();
        assert_ne!(a, b, "two children still share one watermark file");
        assert!(a.ends_with("state/load-orders/inc1.json") || a.ends_with(r"state\load-orders\inc1.json"), "{}", a.display());

        // And an unnamed run keeps the old location, so nothing else moves.
        let legacy = incremental_state_path(None, "inc1").unwrap();
        assert!(legacy.ends_with("state/pipeline/inc1.json") || legacy.ends_with(r"state\pipeline\inc1.json"), "{}", legacy.display());
        assert_ne!(a, legacy);

        std::env::remove_var("DUCKLE_WORKSPACE");
        let _ = std::fs::remove_dir_all(&ws);
    }

    /// Naming the runs must not silently re-load everything.
    ///
    /// A named child looks in a path that has never existed before. Without an
    /// inheritance step it finds nothing, falls back to initialValue and reads
    /// the source from the beginning - turning a bug fix into a full re-sync on
    /// somebody's production load.
    #[test]
    fn a_newly_named_child_inherits_the_old_shared_watermark_once() {
        let _g = workspace_env_guard();
        let ws = workspace("inherit");
        std::env::set_var("DUCKLE_WORKSPACE", &ws);

        write_state(&incremental_state_path(None, "inc1").unwrap(), "2026-08-01T00:00:00");

        let inherited = inherited_incremental_state(Some("load-orders"), "inc1")
            .expect("a named child did not inherit the existing watermark");
        assert_eq!(inherited.0, "2026-08-01T00:00:00");
        assert_eq!(inherited.1, "TIMESTAMP");

        // Only where there is nothing of its own: a child with its own mark is
        // asked for it first, so inheritance never overwrites a real value.
        // (run_incremental reads its own path before calling this.)
        assert!(
            inherited_incremental_state(None, "inc1").is_none(),
            "an unnamed run must not inherit from itself"
        );
        // A node nobody ever ran has nothing to inherit.
        assert!(inherited_incremental_state(Some("load-orders"), "inc-unknown").is_none());

        std::env::remove_var("DUCKLE_WORKSPACE");
        let _ = std::fs::remove_dir_all(&ws);
    }
}

/// #257: resolve `{column}` placeholders in a child endpoint's URL from one
/// parent row.
///
/// Deliberately `{column}` and not `${column}`: `${...}` is already run
/// variables and workspace context, and is substituted before a builder ever
/// sees the property. This is the same syntax an xf.ai.llm prompt uses.
///
/// A name that is not a column of the parent row is an error rather than an
/// empty string. render_prompt_template blanks a missing column, which is right
/// for prose and wrong for a URL: it would silently request
/// `/companies//officers` and the run would look like it worked.
pub(crate) fn render_url_template(
    template: &str,
    row: &JsonValue,
) -> Result<String, EngineError> {
    let obj = match row {
        JsonValue::Object(m) => m,
        _ => {
            return Err(EngineError::Query(
                "rest: a URL template needs an upstream row to resolve it".into(),
            ))
        }
    };
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let close = match after.find('}') {
            // An unclosed brace stays literal, so the user sees it in the URL
            // rather than losing the tail of their template.
            None => {
                out.push_str(&rest[open..]);
                return Ok(out);
            }
            Some(c) => c,
        };
        let name = &after[..close];
        let value = obj.get(name).ok_or_else(|| {
            EngineError::Query(format!(
                "rest: URL template refers to {{{}}}, which is not a column of the upstream row (have: {})",
                name,
                obj.keys().cloned().collect::<Vec<_>>().join(", ")
            ))
        })?;
        let text = match value {
            JsonValue::String(s) => s.clone(),
            JsonValue::Null => String::new(),
            other => other.to_string(),
        };
        out.push_str(&percent_encode_path(&text));
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Percent-encode a value being spliced into a URL. Unreserved characters pass
/// through; everything else is escaped, so an id containing a space, a slash or
/// a question mark cannot change the shape of the request.
fn percent_encode_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// #248: the .pdf files at a path - the file itself, or the ones in a folder.
/// Sorted, so a run over a folder is reproducible rather than filesystem order.
fn expand_pdf_paths(path: &str, recursive: bool) -> Vec<String> {
    let p = std::path::Path::new(path);
    if p.is_file() {
        return vec![path.to_string()];
    }
    let mut out: Vec<String> = Vec::new();
    let mut stack = vec![p.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if recursive {
                    stack.push(path);
                }
            } else if path
                .extension()
                .map(|e| e.eq_ignore_ascii_case("pdf"))
                .unwrap_or(false)
            {
                out.push(path.to_string_lossy().into_owned());
            }
        }
    }
    out.sort();
    out
}

/// #248: a page's width and height in PDF points.
///
/// MediaBox is inheritable: a document may set it once on the page tree rather
/// than on every page, so a page without one is not a page without a size - walk
/// up to Parent before giving up.
fn page_media_box(doc: &lopdf::Document, page_id: lopdf::ObjectId) -> (Option<f64>, Option<f64>) {
    let mut id = page_id;
    for _ in 0..16 {
        let Ok(dict) = doc.get_dictionary(id) else {
            return (None, None);
        };
        if let Ok(obj) = dict.get(b"MediaBox") {
            let resolved = doc.dereference(obj).map(|(_, o)| o).unwrap_or(obj);
            if let Ok(arr) = resolved.as_array() {
                if arr.len() == 4 {
                    let num = |i: usize| -> Option<f64> {
                        arr.get(i).and_then(|v| match v {
                            lopdf::Object::Integer(n) => Some(*n as f64),
                            lopdf::Object::Real(r) => Some(*r as f64),
                            _ => None,
                        })
                    };
                    if let (Some(x0), Some(y0), Some(x1), Some(y1)) =
                        (num(0), num(1), num(2), num(3))
                    {
                        return (Some((x1 - x0).abs()), Some((y1 - y0).abs()));
                    }
                }
            }
        }
        match dict.get(b"Parent") {
            Ok(lopdf::Object::Reference(parent)) => id = *parent,
            _ => return (None, None),
        }
    }
    (None, None)
}

/// What one copy produced.
struct LandedArtifact {
    uri: String,
    name: String,
    media_type: &'static str,
    size_bytes: Option<i64>,
    sha256: Option<String>,
    /// False when the destination already held it and nothing was transferred.
    copied: bool,
}

/// Named from the extension. Enough to route a pipeline - a PDF one way, an
/// image another - without pretending to sniff content. The same table
/// `src.artifact` uses, so the two agree about what a file is.
fn media_type_for(name: &str) -> &'static str {
    match name.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase()).as_deref() {
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("tif") | Some("tiff") => "image/tiff",
        Some("zip") => "application/zip",
        Some("gz") => "application/gzip",
        Some("json") => "application/json",
        Some("xml") => "application/xml",
        Some("csv") => "text/csv",
        Some("txt") => "text/plain",
        Some("html") | Some("htm") => "text/html",
        Some("parquet") => "application/vnd.apache.parquet",
        _ => "application/octet-stream",
    }
}

/// The source's path below its host or bucket, for "preserve the layout"
/// naming. Leading slashes are dropped so joining cannot escape the prefix.
fn source_path_of(src: &str) -> String {
    let after_scheme = src.split_once("://").map(|(_, r)| r).unwrap_or(src);
    let path = after_scheme.split_once('/').map(|(_, r)| r).unwrap_or(after_scheme);
    path.trim_start_matches('/').replace('\\', "/")
}

/// Join a destination prefix and a key, without doubling or dropping the slash.
///
/// `..` is rejected rather than resolved: a source-derived name reaching a
/// destination path is exactly the shape that writes outside the prefix, and a
/// raw zone that can be escaped is not one.
fn join_destination(prefix: &str, key: &str) -> String {
    let safe: String = key
        .split('/')
        .filter(|seg| !seg.is_empty() && *seg != "." && *seg != "..")
        .collect::<Vec<_>>()
        .join("/");
    format!("{}/{}", prefix.trim_end_matches('/'), safe)
}

/// Bytes as something a person reads in a run log.
fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", n, UNITS[0])
    } else {
        format!("{:.1} {}", v, UNITS[i])
    }
}

/// Keeps an SFTP spool file alive for as long as something is reading it, and
/// removes it afterwards however the copy ended.
struct SpooledArtifact {
    path: std::path::PathBuf,
    file: std::fs::File,
}

impl std::io::Read for SpooledArtifact {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Drop for SpooledArtifact {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A stable, filesystem-safe key for one catalog, so two runs against the same
/// lake take the same lock and two runs against different lakes do not.
fn lock_key(catalog_path: &str) -> String {
    use sha2::{Digest, Sha256};
    let h = Sha256::digest(catalog_path.as_bytes());
    h.iter().take(8).map(|b| format!("{:02x}", b)).collect()
}

/// The alias the attach prelude bound the catalog to.
fn catalog_alias(attach: &str) -> Option<String> {
    let after = attach.rsplit_once(" AS ")?.1;
    Some(
        after
            .split(|c: char| c == ' ' || c == ';' || c == '(')
            .next()?
            .trim()
            .to_string(),
    )
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The DuckLake call for one operation, with only the options that were set.
///
/// Every argument is passed by NAME. DuckLake's overloads take their options in
/// different orders - `merge_adjacent_files` has two signatures whose second
/// argument differs - so a positional call would bind the wrong option to the
/// wrong meaning depending on which overload matched.
fn maintenance_call(spec: &plan::DuckLakeMaintainSpec) -> Result<String, EngineError> {
    let alias = catalog_alias(&spec.attach).unwrap_or_else(|| "duckle_dst".to_string());
    let cat = sql_string(&alias);
    let mut args: Vec<String> = vec![cat];

    // A table-scoped operation takes the table as its second positional
    // argument; a catalog-wide one must not be given one at all.
    let table_scoped = matches!(spec.operation.as_str(), "compact" | "rewrite");
    if table_scoped {
        if let Some(t) = &spec.table_name {
            args.push(sql_string(t));
            if let Some(sc) = &spec.schema_name {
                args.push(format!("schema => {}", sql_string(sc)));
            }
        } else if spec.schema_name.is_some() {
            return Err(EngineError::Config(format!(
                "ducklake {}: a schema without a table has nothing to scope - name the table \
                 too, or leave both blank to maintain the whole catalog",
                spec.operation
            )));
        }
    }

    let mut named: Vec<String> = Vec::new();
    let mut push = |k: &str, v: String| named.push(format!("{k} => {v}"));

    match spec.operation.as_str() {
        "compact" => {
            if let Some(n) = spec.min_file_size {
                push("min_file_size", n.to_string());
            }
            if let Some(n) = spec.max_file_size {
                push("max_file_size", n.to_string());
            }
            if let Some(n) = spec.max_compacted_files {
                push("max_compacted_files", n.to_string());
            }
        }
        "rewrite" => {
            if let Some(t) = spec.delete_threshold {
                push("delete_threshold", t.to_string());
            }
        }
        "expireSnapshots" => {
            if let Some(o) = &spec.older_than {
                push("older_than", sql_string(o));
            }
            if let Some(v) = &spec.versions {
                // A list literal, so several versions can be named at once.
                let items: Vec<String> = v
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect();
                if !items.is_empty() {
                    push("versions", format!("[{}]", items.join(", ")));
                }
            }
            push("dry_run", spec.dry_run.to_string());
        }
        "cleanupFiles" | "deleteOrphans" => {
            if let Some(o) = &spec.older_than {
                push("older_than", sql_string(o));
            }
            if spec.cleanup_all {
                push("cleanup_all", "true".to_string());
            }
            push("dry_run", spec.dry_run.to_string());
        }
        "flushInlined" => {
            if let Some(t) = &spec.table_name {
                push("table_name", sql_string(t));
            }
            if let Some(sc) = &spec.schema_name {
                push("schema_name", sql_string(sc));
            }
        }
        "stats" => {}
        other => {
            return Err(EngineError::Config(format!(
                "ducklake: unknown maintenance operation '{other}'"
            )))
        }
    }
    args.extend(named);

    let func = match spec.operation.as_str() {
        "compact" => "ducklake_merge_adjacent_files",
        "rewrite" => "ducklake_rewrite_data_files",
        "expireSnapshots" => "ducklake_expire_snapshots",
        "cleanupFiles" => "ducklake_cleanup_old_files",
        "deleteOrphans" => "ducklake_delete_orphaned_files",
        "flushInlined" => "ducklake_flush_inlined_data",
        _ => "ducklake_table_info",
    };
    Ok(format!("{}({})", func, args.join(", ")))
}

/// One artifact a parser was asked to read, and what the upstream row said
/// about it.
pub(crate) struct ResolvedArtifact {
    pub uri: String,
    /// The hash of these bytes, if whatever produced the row knew it.
    pub sha256: Option<String>,
    /// The whole upstream row, so a reject can carry it back out.
    pub row: JsonValue,
}

/// A local file for a parser to read, removed on drop when this fetched it.
pub(crate) struct SpooledInput {
    pub path: PathBuf,
    temp: bool,
}

impl Drop for SpooledInput {
    fn drop(&mut self) {
        // Deterministic, and on every exit path: a parser that failed must not
        // leave the document behind, or a long run fills the disk with the
        // documents it could not read.
        if self.temp {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// A file name that cannot escape the temp directory.
fn safe_file_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect()
}

/// Which archive a URI names, from its extension.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ArchiveKind {
    Zip,
    Tar,
    TarGz,
    Gzip,
    Unknown,
}

fn archive_kind(uri: &str) -> ArchiveKind {
    let lower = uri.to_ascii_lowercase();
    // Order matters: .tar.gz has to be recognised before .gz, or a tar of many
    // members is treated as one compressed stream.
    if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
        ArchiveKind::TarGz
    } else if lower.ends_with(".zip") {
        ArchiveKind::Zip
    } else if lower.ends_with(".tar") {
        ArchiveKind::Tar
    } else if lower.ends_with(".gz") {
        ArchiveKind::Gzip
    } else {
        ArchiveKind::Unknown
    }
}

/// How much one archive is still allowed to produce.
struct MemberBudget {
    remaining_members: usize,
    remaining_bytes: u64,
    archive_uri: String,
}

impl MemberBudget {
    fn take_member(&mut self, name: &str) -> Result<(), EngineError> {
        if self.remaining_members == 0 {
            return Err(EngineError::Query(format!(
                "archive: {} holds more members than the limit allows (stopped at '{}'). Raise \
                 the member limit, or narrow the include filter.",
                self.archive_uri, name
            )));
        }
        self.remaining_members -= 1;
        Ok(())
    }
}

/// A reader that stops after a fixed number of bytes.
///
/// The bound has to apply while the member is being READ, not after: an archive
/// is a compression format, so a small one can expand to fill a volume, and
/// discovering that from the disk-full error is too late.
struct CappedReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: std::io::Read> std::io::Read for CappedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let cap = buf.len().min(self.remaining as usize);
        let n = self.inner.read(&mut buf[..cap])?;
        self.remaining -= n as u64;
        Ok(n)
    }
}

/// Does this member pass the include / exclude filters?
fn member_wanted(name: &str, spec: &plan::ArchiveExtractSpec) -> bool {
    let matches = |pat: &String| glob_match(pat, name);
    if !spec.include.is_empty() && !spec.include.iter().any(matches) {
        return false;
    }
    !spec.exclude.iter().any(matches)
}

/// Where a node's accepted profiles live.
///
/// A sub-directory, so `watermark::list` - which reads the top level and takes
/// `*.json` - does not report a profile history as a resume position that could
/// then be hand-edited.
fn baseline_state_path(pipeline_name: Option<&str>, node_id: &str) -> Option<std::path::PathBuf> {
    let ws = std::env::var("DUCKLE_WORKSPACE").ok().filter(|s| !s.is_empty())?;
    let folder = sanitize_path_segment(pipeline_name.unwrap_or(UNNAMED_RUN_FOLDER));
    Some(
        std::path::Path::new(&ws)
            .join("state")
            .join(folder)
            .join("baselines")
            .join(format!("{}.json", sanitize_path_segment(node_id))),
    )
}

/// The key one metric is stored under.
fn metric_key(metric: &str, column: Option<&str>) -> String {
    match column {
        Some(c) => format!("{c}::{metric}"),
        None => metric.to_string(),
    }
}

/// A column name reduced to something safe inside a generated alias.
fn metric_ident(column: &str) -> String {
    column
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

/// The middle value of this metric across the accepted history.
///
/// Median rather than mean: one bad Tuesday should not drag the baseline
/// towards itself, and the whole point is to notice a day that is unlike the
/// others.
fn median_of(history: &[JsonValue], key: &str) -> Option<f64> {
    let mut vals: Vec<f64> = history
        .iter()
        .filter_map(|p| p.get(key).and_then(JsonValue::as_f64))
        .collect();
    if vals.is_empty() {
        return None;
    }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = vals.len() / 2;
    Some(if vals.len() % 2 == 0 {
        (vals[mid - 1] + vals[mid]) / 2.0
    } else {
        vals[mid]
    })
}

/// The groups a profile saw.
fn group_set(profile: &serde_json::Map<String, JsonValue>) -> std::collections::BTreeSet<String> {
    profile
        .get("__groups")
        .and_then(JsonValue::as_object)
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default()
}

/// Does this movement break the rule?
fn judge(rule: &plan::BaselineRule, base: f64, cur: f64) -> (String, String) {
    let label = match &rule.column {
        Some(c) => format!("{} {}", c, rule.metric),
        None => rule.metric.clone(),
    };
    let diff = cur - base;
    let pct = if base != 0.0 { diff / base * 100.0 } else { f64::INFINITY };

    let fail = |why: String| ("violation".to_string(), why);
    if let Some(limit) = rule.max_decrease_pct {
        if base != 0.0 && -pct > limit {
            return fail(format!(
                "{label} decreased {:.1}% ({} -> {}), limit {:.1}%",
                -pct,
                pretty(base),
                pretty(cur),
                limit
            ));
        }
    }
    if let Some(limit) = rule.max_increase_pct {
        if base != 0.0 && pct > limit {
            return fail(format!(
                "{label} increased {:.1}% ({} -> {}), limit {:.1}%",
                pct,
                pretty(base),
                pretty(cur),
                limit
            ));
        }
    }
    // Absolute limits, for metrics where a percentage says nothing: a null rate
    // going from 0% to 5% is an infinite percentage increase.
    if let Some(limit) = rule.max_increase {
        if diff > limit {
            return fail(format!(
                "{label} rose by {} ({} -> {}), limit {}",
                pretty(diff),
                pretty(base),
                pretty(cur),
                pretty(limit)
            ));
        }
    }
    if let Some(limit) = rule.max_decrease {
        if -diff > limit {
            return fail(format!(
                "{label} fell by {} ({} -> {}), limit {}",
                pretty(-diff),
                pretty(base),
                pretty(cur),
                pretty(limit)
            ));
        }
    }
    if let Some(limit) = rule.max_difference {
        if diff.abs() > limit {
            return fail(format!(
                "{label} moved by {} ({} -> {}), limit {}",
                pretty(diff.abs()),
                pretty(base),
                pretty(cur),
                pretty(limit)
            ));
        }
    }
    ("ok".to_string(), format!("{label} within range"))
}

/// A number as a person reads it in a run log.
fn pretty(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{:.4}", v)
    }
}
