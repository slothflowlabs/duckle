//! Tool, resource and prompt implementations for the Duckle MCP server.
//!
//! Tools return their structured result as a single pretty-printed JSON text
//! content block (the universally supported MCP content type); a tool failure
//! is reported with `isError: true` rather than a JSON-RPC error, so the model
//! can read and react to it.

use crate::catalog;
use duckle_duckdb_engine::{compile_pipeline_sql, DuckdbEngine, PipelineDoc};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Sent to the client on initialize: a compact operating guide.
pub const INSTRUCTIONS: &str = "\
Duckle MCP: generate, validate, run and build Duckle ETL pipelines.

A pipeline is JSON: { \"name\", \"nodes\": [...], \"edges\": [...] }.
- node: { \"id\": \"n1\", \"type\": \"source|transform|sink\", \"position\": {\"x\":0,\"y\":0}, \
\"data\": { \"label\": \"...\", \"componentId\": \"src.csv\", \"properties\": { ... } } }
- edge: { \"id\": \"e1\", \"source\": \"n1\", \"target\": \"n2\", \"sourceHandle\": \"main\", \
\"targetHandle\": \"main\", \"data\": { \"connectionType\": \"main\" } }

Workflow: call list_components to find component ids, get_component_schema for a \
component's property keys, then create_pipeline (it validates before writing). Use \
validate_pipeline to compile-check without running and run_pipeline to execute headlessly. \
Never hardcode secrets: use ${ENV:KEY} placeholders in properties and supply the value via \
the environment at run time. run_pipeline and build_pipeline need a DuckDB binary \
(DUCKLE_DUCKDB_BIN env or a 'duckdb' arg); build_pipeline also needs the duckle-runner binary.";

// ---------------------------------------------------------------------------
// tools/list
// ---------------------------------------------------------------------------

pub fn list_tools() -> Value {
    json!([
        tool("list_components",
            "List Duckle components (sources, transforms, sinks, control, quality, custom code). Optionally filter by kind or a search query.",
            json!({ "type": "object", "properties": {
                "kind": { "type": "string", "enum": ["source","transform","sink","control","quality","custom"], "description": "Filter to one kind." },
                "query": { "type": "string", "description": "Case-insensitive substring over id/label/summary." }
            }})),
        tool("get_component_schema",
            "Get the full property schema (form fields + input/output ports) for one component id, so you know which properties to set.",
            json!({ "type": "object", "properties": {
                "componentId": { "type": "string", "description": "e.g. src.csv, xf.map, snk.postgres" }
            }, "required": ["componentId"] })),
        tool("create_pipeline",
            "Validate a pipeline and write it. Prefer 'workspace' (writes pipelines/<id>.json and registers it in repository.json so it shows in the GUI immediately); 'directory' writes a loose <name>.json (not GUI-listed). Fails (without writing) if it does not compile, unless validate=false.",
            json!({ "type": "object", "properties": {
                "workspace": { "type": "string", "description": "Workspace root. Recommended: writes pipelines/<id>.json + registers in repository.json so the GUI lists it." },
                "directory": { "type": "string", "description": "Alternative to 'workspace': write a loose <name>.json here (not registered in the GUI)." },
                "name": { "type": "string", "description": "Pipeline display name." },
                "id": { "type": "string", "description": "Pipeline id (file stem under workspace). Optional; generated if absent." },
                "pipeline": { "type": "object", "description": "The pipeline object with at least a 'nodes' array (and usually 'edges')." },
                "overwrite": { "type": "boolean", "description": "Replace an existing file. Default false." },
                "validate": { "type": "boolean", "description": "Compile-check before writing. Default true." }
            }, "required": ["name","pipeline"] })),
        tool("update_pipeline",
            "Merge a PARTIAL change into an existing pipeline (no need to resend the whole thing). Deep-merges 'patch' into the on-disk pipeline (nodes/edges merged by id, so you can patch one node's property), validates, then writes. Locate it via 'workspace'+'id' or a direct 'path'.",
            json!({ "type": "object", "properties": {
                "workspace": { "type": "string", "description": "Workspace root (with 'id' -> pipelines/<id>.json)." },
                "id": { "type": "string", "description": "Pipeline id under the workspace." },
                "path": { "type": "string", "description": "Direct path to the pipeline .json (use instead of workspace+id)." },
                "patch": { "type": "object", "description": "Partial pipeline to merge, e.g. {\"nodes\":[{\"id\":\"k1\",\"data\":{\"properties\":{\"path\":\"out2.csv\"}}}]} or {\"name\":\"New\"}." },
                "validate": { "type": "boolean", "description": "Compile-check the merged result before writing. Default true." }
            }, "required": ["patch"] })),
        tool("validate_pipeline",
            "Compile a pipeline to SQL without running it. Returns the per-stage SQL on success, or a structured error.",
            json!({ "type": "object", "properties": {
                "pipeline": { "type": "object", "description": "Inline pipeline object." },
                "path": { "type": "string", "description": "Path to a pipeline .json (use instead of 'pipeline')." }
            }})),
        tool("run_pipeline",
            "Run a pipeline headlessly through the DuckDB engine. Returns per-node status, row counts, errors and a small result preview. Needs a DuckDB binary.",
            json!({ "type": "object", "properties": {
                "pipeline": { "type": "object" },
                "path": { "type": "string" },
                "duckdb": { "type": "string", "description": "Path to the DuckDB CLI. Defaults to DUCKLE_DUCKDB_BIN or 'duckdb' on PATH." },
                "workspace": { "type": "string", "description": "Workspace root for run logs + child-job resolution." }
            }})),
        tool("pipeline_lineage",
            "Resolve column-level lineage for a pipeline: for each node, map its output columns back to their root source columns. Read-only (writes nothing); needs a DuckDB binary.",
            json!({ "type": "object", "properties": {
                "pipeline": { "type": "object", "description": "Inline pipeline object." },
                "path": { "type": "string", "description": "Path to a pipeline .json (use instead of 'pipeline')." },
                "duckdb": { "type": "string", "description": "Path to the DuckDB CLI. Defaults to DUCKLE_DUCKDB_BIN or 'duckdb' on PATH." }
            }})),
        tool("verify_pipeline",
            "Check a pipeline without running it: compiles it to SQL, resolves column lineage, and reports structural risks (joins without keys, unconnected nodes, sink with no input, no sink). Returns one pass/fail verdict an agent can act on. Read-only; writes nothing.",
            json!({ "type": "object", "properties": {
                "pipeline": { "type": "object", "description": "Inline pipeline object." },
                "path": { "type": "string", "description": "Path to a pipeline .json (use instead of 'pipeline')." },
                "duckdb": { "type": "string", "description": "DuckDB CLI path for lineage resolution. Defaults to DUCKLE_DUCKDB_BIN or 'duckdb' on PATH." }
            }})),
        tool("suggest_contracts",
            "Profile a pipeline's columns and suggest data contracts to add: PII tags (heuristic, name-based) and source requireColumns anchors. Returns per-node suggestedContracts you can merge with update_pipeline, after which verify_pipeline enforces the PII-to-sink guard. Static; uses declared schemas, and column lineage too when a DuckDB binary is available.",
            json!({ "type": "object", "properties": {
                "pipeline": { "type": "object", "description": "Inline pipeline object." },
                "path": { "type": "string", "description": "Path to a pipeline .json (use instead of 'pipeline')." },
                "duckdb": { "type": "string", "description": "DuckDB CLI path - lets lineage cover transform/sink columns too. Defaults to DUCKLE_DUCKDB_BIN or 'duckdb' on PATH." }
            }})),
        tool("pipeline_impact",
            "Blast-radius / impact analysis: inverts column lineage to show, for each source column, which downstream node columns derive from it - so you can see what breaks before changing or dropping a column. Scope to one column with 'column' (plus optional 'node'). Read-only; needs a DuckDB binary.",
            json!({ "type": "object", "properties": {
                "pipeline": { "type": "object", "description": "Inline pipeline object." },
                "path": { "type": "string", "description": "Path to a pipeline .json (use instead of 'pipeline')." },
                "node": { "type": "string", "description": "Optional source node id to scope the query to." },
                "column": { "type": "string", "description": "Optional source column name; returns only its downstream dependents." },
                "duckdb": { "type": "string", "description": "Path to the DuckDB CLI. Defaults to DUCKLE_DUCKDB_BIN or 'duckdb' on PATH." }
            }})),
        tool("list_pipelines",
            "List pipeline .json files in a directory with their node/edge counts.",
            json!({ "type": "object", "properties": {
                "directory": { "type": "string" }
            }, "required": ["directory"] })),
        tool("read_pipeline",
            "Read and return a pipeline .json file.",
            json!({ "type": "object", "properties": {
                "path": { "type": "string" }
            }, "required": ["path"] })),
        tool("read_run_logs",
            "Read the tail of a pipeline's NDJSON run log (component-level events).",
            json!({ "type": "object", "properties": {
                "pipelineName": { "type": "string" },
                "workspace": { "type": "string", "description": "Reads <workspace>/logs/<name>/runtime.log." },
                "logDir": { "type": "string", "description": "Log dir directly (use instead of 'workspace')." },
                "tail": { "type": "integer", "description": "Number of trailing lines. Default 100." }
            }, "required": ["pipelineName"] })),
        tool("build_pipeline",
            "Build a pipeline into ONE self-contained executable for server deployment (the Talend Build Job equivalent). Needs the duckle-runner binary (DUCKLE_RUNNER_BIN or on PATH).",
            json!({ "type": "object", "properties": {
                "pipeline": { "type": "object" },
                "path": { "type": "string" },
                "name": { "type": "string", "description": "Display/file name for the artifact." },
                "out": { "type": "string", "description": "Output artifact file path." },
                "secrets": { "type": "string", "enum": ["env","passphrase"], "description": "Secret delivery mode. Default env. Passphrase needs DUCKLE_BUNDLE_PASSPHRASE." },
                "duckdb": { "type": "string" }
            }, "required": ["out"] })),
        tool("list_connections",
            "List the workspace's saved connections (secret fields masked).",
            json!({ "type": "object", "properties": {
                "workspace": { "type": "string" }
            }, "required": ["workspace"] })),
        tool("create_connection",
            "Create a workspace saved connection JSON so pipelines can reference its fields. Writes connections/<id>.json and registers it in repository.json when present.",
            json!({ "type": "object", "properties": {
                "workspace": { "type": "string" },
                "name": { "type": "string" },
                "connection": { "type": "object", "description": "Fields like { kind, host, port, database, username, password }." }
            }, "required": ["workspace","name","connection"] }))
    ])
}

fn tool(name: &str, description: &str, schema: Value) -> Value {
    json!({ "name": name, "description": description, "inputSchema": schema })
}

// ---------------------------------------------------------------------------
// tools/call
// ---------------------------------------------------------------------------

pub fn call_tool(params: Value) -> Result<Value, (i64, String)> {
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing tool name".to_string()))?;
    let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));

    let result = match name {
        "list_components" => t_list_components(&args),
        "get_component_schema" => t_get_component_schema(&args),
        "create_pipeline" => t_create_pipeline(&args),
        "update_pipeline" => t_update_pipeline(&args),
        "validate_pipeline" => t_validate_pipeline(&args),
        "run_pipeline" => t_run_pipeline(&args),
        "pipeline_lineage" => t_pipeline_lineage(&args),
        "verify_pipeline" => t_verify_pipeline(&args),
        "suggest_contracts" => t_suggest_contracts(&args),
        "pipeline_impact" => t_pipeline_impact(&args),
        "list_pipelines" => t_list_pipelines(&args),
        "read_pipeline" => t_read_pipeline(&args),
        "read_run_logs" => t_read_run_logs(&args),
        "build_pipeline" => t_build_pipeline(&args),
        "list_connections" => t_list_connections(&args),
        "create_connection" => t_create_connection(&args),
        other => return Err((-32602, format!("unknown tool: {other}"))),
    };

    Ok(match result {
        Ok(v) => content_ok(&v),
        Err(e) => content_err(&e),
    })
}

fn content_ok(v: &Value) -> Value {
    let text = serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string());
    json!({ "content": [ { "type": "text", "text": text } ], "isError": false })
}

fn content_err(msg: &str) -> Value {
    json!({ "content": [ { "type": "text", "text": msg } ], "isError": true })
}

// ---------------------------------------------------------------------------
// Tool implementations
// ---------------------------------------------------------------------------

fn t_list_components(args: &Value) -> Result<Value, String> {
    Ok(catalog::list(arg_str(args, "kind"), arg_str(args, "query")))
}

fn t_get_component_schema(args: &Value) -> Result<Value, String> {
    let id = arg_str(args, "componentId").ok_or("missing 'componentId'")?;
    catalog::schema(id).ok_or_else(|| format!("unknown componentId: {id}"))
}

fn t_validate_pipeline(args: &Value) -> Result<Value, String> {
    let (v, _name) = load_pipeline_value(args)?;
    let doc = to_doc(&v)?;
    match compile_pipeline_sql(&doc) {
        Ok(stages) => Ok(json!({
            "ok": true,
            "stageCount": stages.len(),
            "stages": serde_json::to_value(&stages).unwrap_or_else(|_| json!([]))
        })),
        Err(e) => Ok(json!({ "ok": false, "error": e.to_string() })),
    }
}

/// Shape the engine's per-node column lineage into stable JSON:
/// `{ node_id: [ { "column": name, "roots": [ { node, column }, ... ] } ] }`.
fn lineage_to_json(
    lineage: std::collections::HashMap<
        String,
        Vec<(String, Vec<duckle_duckdb_engine::lineage::RootColumn>)>,
    >,
) -> Value {
    let mut out = serde_json::Map::new();
    for (node_id, cols) in lineage {
        let arr: Vec<Value> = cols
            .into_iter()
            .map(|(column, roots)| {
                json!({
                    "column": column,
                    "roots": serde_json::to_value(&roots).unwrap_or_else(|_| json!([])),
                })
            })
            .collect();
        out.insert(node_id, Value::Array(arr));
    }
    Value::Object(out)
}

fn t_pipeline_lineage(args: &Value) -> Result<Value, String> {
    let (v, _name) = load_pipeline_value(args)?;
    let doc = to_doc(&v)?;
    let duckdb = resolve_duckdb(arg_str(args, "duckdb"))
        .ok_or("no DuckDB binary found (set DUCKLE_DUCKDB_BIN or pass 'duckdb')")?;
    let engine = DuckdbEngine::new(duckdb);
    let lineage = engine
        .pipeline_column_lineage(&doc)
        .map_err(|e| e.to_string())?;
    Ok(json!({ "ok": true, "lineage": lineage_to_json(lineage) }))
}

/// The engine's per-node column lineage: node id -> [(output column, root source columns)].
type Lineage =
    std::collections::HashMap<String, Vec<(String, Vec<duckle_duckdb_engine::lineage::RootColumn>)>>;

/// Invert lineage into a forward blast-radius map: for each root source column
/// `(node, column)`, the set of downstream `(node, column)` that derive from it.
/// A column's own identity entry is skipped so dependents are strictly downstream.
fn impact_from_lineage(
    lineage: &Lineage,
) -> std::collections::BTreeMap<(String, String), std::collections::BTreeSet<(String, String)>> {
    let mut map: std::collections::BTreeMap<
        (String, String),
        std::collections::BTreeSet<(String, String)>,
    > = std::collections::BTreeMap::new();
    for (node_id, cols) in lineage {
        for (col, roots) in cols {
            for r in roots {
                if &r.node == node_id && &r.column == col {
                    continue;
                }
                map.entry((r.node.clone(), r.column.clone()))
                    .or_default()
                    .insert((node_id.clone(), col.clone()));
            }
        }
    }
    map
}

fn t_pipeline_impact(args: &Value) -> Result<Value, String> {
    let (v, _name) = load_pipeline_value(args)?;
    let doc = to_doc(&v)?;
    let duckdb = resolve_duckdb(arg_str(args, "duckdb"))
        .ok_or("no DuckDB binary found (set DUCKLE_DUCKDB_BIN or pass 'duckdb')")?;
    let engine = DuckdbEngine::new(duckdb);
    let lineage = engine
        .pipeline_column_lineage(&doc)
        .map_err(|e| e.to_string())?;
    let map = impact_from_lineage(&lineage);

    let want_node = arg_str(args, "node");
    let want_col = arg_str(args, "column");

    let roots: Vec<Value> = map
        .iter()
        .filter(|((rn, rc), _)| {
            want_node.map(|n| n == rn).unwrap_or(true) && want_col.map(|c| c == rc).unwrap_or(true)
        })
        .map(|((rn, rc), deps)| {
            json!({
                "rootNode": rn,
                "rootColumn": rc,
                "dependents": deps
                    .iter()
                    .map(|(n, c)| json!({ "node": n, "column": c }))
                    .collect::<Vec<_>>(),
                "dependentCount": deps.len(),
            })
        })
        .collect();

    if want_col.is_some() && roots.is_empty() {
        return Ok(json!({
            "ok": true,
            "query": { "node": want_node, "column": want_col },
            "roots": [],
            "note": "no source column matched, or it has no downstream dependents"
        }));
    }
    Ok(json!({ "ok": true, "roots": roots }))
}

/// Cheap, deterministic graph checks that do not require execution. Reads the
/// raw pipeline JSON so it is independent of the typed engine structs.
fn structural_risks(pipeline: &Value) -> Vec<Value> {
    let mut risks: Vec<Value> = Vec::new();
    let (nodes, edges) = match (
        pipeline.get("nodes").and_then(|n| n.as_array()),
        pipeline.get("edges").and_then(|e| e.as_array()),
    ) {
        (Some(n), Some(e)) => (n, e),
        _ => return risks,
    };

    let str_at = |v: &Value, path: &[&str]| -> String {
        let mut cur = v;
        for p in path {
            cur = match cur.get(p) {
                Some(c) => c,
                None => return String::new(),
            };
        }
        cur.as_str().unwrap_or("").to_string()
    };
    let has_incoming =
        |id: &str| edges.iter().any(|e| e.get("target").and_then(|v| v.as_str()) == Some(id));
    let has_outgoing =
        |id: &str| edges.iter().any(|e| e.get("source").and_then(|v| v.as_str()) == Some(id));

    let mut sink_count = 0usize;
    for n in nodes {
        let id = n.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if id.is_empty() {
            continue;
        }
        let cid = str_at(n, &["data", "componentId"]);
        let label = str_at(n, &["data", "label"]);
        let is_source = cid.starts_with("src.");
        let is_sink = cid.starts_with("snk.");
        if is_sink {
            sink_count += 1;
        }

        if cid.starts_with("xf.join") {
            let props = n.get("data").and_then(|d| d.get("properties"));
            let has_key = |k: &str| {
                props
                    .and_then(|p| p.get(k))
                    .and_then(|v| v.as_str())
                    .map(|s| !s.is_empty())
                    .unwrap_or(false)
            };
            if !(has_key("leftKey") && has_key("rightKey")) {
                risks.push(json!({
                    "severity": "warning", "node": id, "label": label,
                    "code": "join_without_keys",
                    "message": "join has no leftKey/rightKey, which can fan out into a cross join"
                }));
            }
        }

        if is_sink && !has_incoming(id) {
            risks.push(json!({
                "severity": "error", "node": id, "label": label,
                "code": "sink_without_input",
                "message": "sink has no incoming edge, so it would write nothing"
            }));
        }

        if !is_source && !is_sink && !has_incoming(id) && !has_outgoing(id) {
            risks.push(json!({
                "severity": "warning", "node": id, "label": label,
                "code": "orphan_node",
                "message": "node is not connected to the rest of the pipeline"
            }));
        }
    }

    if sink_count == 0 {
        risks.push(json!({
            "severity": "warning", "code": "no_sink",
            "message": "pipeline has no sink, so no output is written"
        }));
    }

    risks
}

fn t_verify_pipeline(args: &Value) -> Result<Value, String> {
    let (v, _name) = load_pipeline_value(args)?;
    let doc = to_doc(&v)?;

    // 1. Compile to SQL (no execution).
    let (compiled_ok, compile) = match compile_pipeline_sql(&doc) {
        Ok(stages) => (true, json!({ "ok": true, "stageCount": stages.len() })),
        Err(e) => (false, json!({ "ok": false, "error": e.to_string() })),
    };

    // 2. Resolve column lineage (best-effort; needs a DuckDB binary).
    let mut lineage_resolved = false;
    let mut lineage = Value::Null;
    if compiled_ok {
        if let Some(duckdb) = resolve_duckdb(arg_str(args, "duckdb")) {
            if let Ok(l) = DuckdbEngine::new(duckdb).pipeline_column_lineage(&doc) {
                lineage_resolved = true;
                lineage = lineage_to_json(l);
            }
        }
    }

    // 3. Structural risks straight off the graph (no execution).
    let risks = structural_risks(&v);
    let has_error = !compiled_ok || risks.iter().any(|r| r["severity"] == "error");

    Ok(json!({
        "ok": !has_error,
        "mode": "static",
        "executed": false,
        "compile": compile,
        "lineageResolved": lineage_resolved,
        "lineage": lineage,
        "risks": risks,
    }))
}

/// Heuristic, name-based PII classification. Returns a category label when a
/// column name looks like personal data, else None. Keywords are matched against
/// the name with separators removed (so first_name, firstName and FIRSTNAME all
/// hit "firstname"). Deliberately high-precision: these are SUGGESTIONS a human
/// or agent reviews, not a proof, so we favour few false positives over recall.
fn looks_like_pii(column: &str) -> Option<&'static str> {
    let norm: String = column
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect();
    // Ordered most-specific first; first match wins so emailAddress -> "email".
    const PATTERNS: &[(&str, &str)] = &[
        ("email", "email"),
        ("ssn", "national_id"),
        ("socialsecurity", "national_id"),
        ("passport", "national_id"),
        ("nationalid", "national_id"),
        ("taxid", "national_id"),
        ("dateofbirth", "date_of_birth"),
        ("birthdate", "date_of_birth"),
        ("birthday", "date_of_birth"),
        ("firstname", "name"),
        ("lastname", "name"),
        ("fullname", "name"),
        ("surname", "name"),
        ("maidenname", "name"),
        ("creditcard", "financial"),
        ("cardnumber", "financial"),
        ("iban", "financial"),
        ("accountnumber", "financial"),
        ("routingnumber", "financial"),
        ("driverlicense", "license"),
        ("driverslicense", "license"),
        ("licensenumber", "license"),
        ("ipaddress", "ip_address"),
        ("streetaddress", "address"),
        ("homeaddress", "address"),
        ("postalcode", "address"),
        ("zipcode", "address"),
        ("phone", "phone"),
        ("mobilenumber", "phone"),
        ("telephone", "phone"),
    ];
    PATTERNS
        .iter()
        .find(|(kw, _)| norm.contains(kw))
        .map(|(_, cat)| *cat)
}

/// Column names a node declares statically in its Schema panel (works without a
/// DuckDB binary). Reads the raw `data.schema` array of `{ name }` entries.
fn declared_columns(node: &Value) -> Vec<String> {
    node.get("data")
        .and_then(|d| d.get("schema"))
        .and_then(|s| s.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    c.get("name")
                        .and_then(|n| n.as_str())
                        .or_else(|| c.as_str())
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
}

fn t_suggest_contracts(args: &Value) -> Result<Value, String> {
    let (v, _name) = load_pipeline_value(args)?;

    // Collect each node's known columns: declared schema (static) unioned with
    // column lineage (covers transforms/sinks too) when a DuckDB binary is found.
    let mut cols_by_node: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    if let Some(nodes) = v.get("nodes").and_then(|n| n.as_array()) {
        for n in nodes {
            let id = match n.get("id").and_then(|x| x.as_str()) {
                Some(s) if !s.is_empty() => s,
                _ => continue,
            };
            let entry = cols_by_node.entry(id.to_string()).or_default();
            entry.extend(declared_columns(n));
        }
    }
    let mut lineage_used = false;
    if let Ok(doc) = to_doc(&v) {
        if let Some(duckdb) = resolve_duckdb(arg_str(args, "duckdb")) {
            if let Ok(l) = DuckdbEngine::new(duckdb).pipeline_column_lineage(&doc) {
                lineage_used = true;
                for (node_id, cols) in l {
                    let entry = cols_by_node.entry(node_id).or_default();
                    for (col, _roots) in cols {
                        entry.insert(col);
                    }
                }
            }
        }
    }

    // Build per-node suggestions: PII tags wherever a column name looks personal,
    // plus requireColumns on sources to anchor "this source must deliver these".
    let mut suggestions: Vec<Value> = Vec::new();
    if let Some(nodes) = v.get("nodes").and_then(|n| n.as_array()) {
        for n in nodes {
            let id = match n.get("id").and_then(|x| x.as_str()) {
                Some(s) if !s.is_empty() => s,
                _ => continue,
            };
            let cid = n
                .get("data")
                .and_then(|d| d.get("componentId"))
                .and_then(|c| c.as_str())
                .unwrap_or("");
            let label = n
                .get("data")
                .and_then(|d| d.get("label"))
                .and_then(|c| c.as_str())
                .unwrap_or("");
            let cols = match cols_by_node.get(id) {
                Some(c) if !c.is_empty() => c,
                _ => continue,
            };

            let pii_matches: Vec<Value> = cols
                .iter()
                .filter_map(|c| looks_like_pii(c).map(|cat| json!({ "column": c, "category": cat })))
                .collect();

            let mut contracts = serde_json::Map::new();
            if !pii_matches.is_empty() {
                let pii_cols: Vec<&str> =
                    pii_matches.iter().filter_map(|m| m["column"].as_str()).collect();
                contracts.insert("pii".to_string(), json!(pii_cols));
            }
            if cid.starts_with("src.") {
                contracts.insert(
                    "requireColumns".to_string(),
                    json!(cols.iter().collect::<Vec<_>>()),
                );
            }
            if contracts.is_empty() {
                continue;
            }
            suggestions.push(json!({
                "nodeId": id,
                "label": label,
                "componentId": cid,
                "piiMatches": pii_matches,
                "suggestedContracts": Value::Object(contracts),
            }));
        }
    }

    let columns_known = cols_by_node.values().any(|c| !c.is_empty());
    Ok(json!({
        "ok": true,
        "columnsKnown": columns_known,
        "lineageUsed": lineage_used,
        "suggestions": suggestions,
        "note": "Heuristic name-based PII detection - review before applying. Merge each suggestedContracts into that node's data.properties.contracts (e.g. via update_pipeline), then verify_pipeline blocks a tagged column reaching a sink. To allow one, add a qa.mask upstream or set contracts.allowPii=true on the sink. Pass a DuckDB binary to also cover transform/sink columns via lineage.",
    }))
}

fn t_create_pipeline(args: &Value) -> Result<Value, String> {
    let name = arg_str(args, "name").ok_or("missing 'name'")?;
    let workspace = arg_str(args, "workspace");
    let dir = arg_str(args, "directory");
    if workspace.is_none() && dir.is_none() {
        return Err(
            "provide 'workspace' (recommended - registers the pipeline so the GUI lists it) or 'directory'".to_string(),
        );
    }
    let pipeline = args
        .get("pipeline")
        .filter(|v| v.is_object())
        .ok_or("missing 'pipeline' object")?;
    let do_validate = arg_bool(args, "validate", true);
    let overwrite = arg_bool(args, "overwrite", false);

    // Normalize into the full saved-pipeline shape the GUI also writes.
    let mut obj = pipeline.as_object().cloned().unwrap_or_default();
    if !obj.get("nodes").map(|n| n.is_array()).unwrap_or(false) {
        return Err("pipeline must have a 'nodes' array".to_string());
    }
    obj.entry("edges").or_insert_with(|| json!([]));
    obj.entry("version").or_insert_with(|| json!(1));
    obj.entry("name").or_insert_with(|| json!(name));
    // A caller-pinned id (or one already in the pipeline) lets create+overwrite
    // target a known file; otherwise generate one. The id is also the file stem
    // under a workspace, matching the GUI's pipelines/<id>.json layout.
    let id = arg_str(args, "id")
        .map(String::from)
        .or_else(|| obj.get("id").and_then(|v| v.as_str()).map(String::from))
        .unwrap_or_else(|| gen_id("p"));
    obj.insert("id".to_string(), json!(id));
    let full = Value::Object(obj);

    let mut validation = Value::Null;
    if do_validate {
        let doc = to_doc(&full)?;
        match compile_pipeline_sql(&doc) {
            Ok(stages) => validation = json!({ "ok": true, "stageCount": stages.len() }),
            Err(e) => return Err(format!("pipeline did not validate (not written): {e}")),
        }
    }
    let pretty = serde_json::to_string_pretty(&full).map_err(|e| e.to_string())?;

    let (path, registered) = if let Some(ws) = workspace {
        // v2 workspace layout: pipelines/<id>.json + repository.json entry so the
        // GUI lists it immediately (the reporter's main friction, #92).
        let pdir = std::path::Path::new(ws).join("pipelines");
        std::fs::create_dir_all(&pdir).map_err(|e| format!("mkdir: {e}"))?;
        let path = pdir.join(format!("{id}.json"));
        if path.exists() && !overwrite {
            return Err(format!("{} already exists (pass overwrite=true to replace)", path.display()));
        }
        std::fs::write(&path, &pretty).map_err(|e| format!("write {}: {e}", path.display()))?;
        let registered = register_pipeline_in_repo(ws, &id, name);
        (path, registered)
    } else {
        let dir = dir.unwrap();
        let fname = format!("{}.json", sanitize_filename(name));
        let path = std::path::Path::new(dir).join(&fname);
        if path.exists() && !overwrite {
            return Err(format!("{} already exists (pass overwrite=true to replace)", path.display()));
        }
        std::fs::create_dir_all(dir).map_err(|e| format!("mkdir {dir}: {e}"))?;
        std::fs::write(&path, &pretty).map_err(|e| format!("write {}: {e}", path.display()))?;
        (path, false)
    };

    Ok(json!({ "ok": true, "id": id, "path": path.to_string_lossy(), "registeredInRepository": registered, "validation": validation }))
}

/// Best-effort: upsert a pipeline entry into <ws>/repository.json so the GUI
/// lists an MCP-created/updated pipeline. Places it under the "pipelines" folder
/// when one exists (v2 layout), else at the root. Returns true if written.
fn register_pipeline_in_repo(ws: &str, id: &str, name: &str) -> bool {
    let repo_path = std::path::Path::new(ws).join("repository.json");
    let mut repo: Value = std::fs::read_to_string(&repo_path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_else(|| json!([]));
    let arr = match repo.as_array_mut() {
        Some(a) => a,
        None => return false,
    };
    if let Some(existing) = arr
        .iter_mut()
        .find(|e| e.get("id").and_then(|v| v.as_str()) == Some(id))
    {
        if let Some(o) = existing.as_object_mut() {
            o.insert("name".to_string(), json!(name));
        }
    } else {
        let has_folder = arr.iter().any(|e| {
            e.get("id").and_then(|v| v.as_str()) == Some("pipelines")
                && e.get("type").and_then(|v| v.as_str()) == Some("folder")
        });
        let mut entry = serde_json::Map::new();
        entry.insert("id".to_string(), json!(id));
        entry.insert("name".to_string(), json!(name));
        entry.insert("type".to_string(), json!("pipeline"));
        if has_folder {
            entry.insert("parentId".to_string(), json!("pipelines"));
        }
        arr.push(Value::Object(entry));
    }
    std::fs::write(&repo_path, serde_json::to_string_pretty(&repo).unwrap_or_default()).is_ok()
}

/// Deep-merge `patch` into `base` for update_pipeline. `nodes`/`edges` arrays are
/// merged by element `id` (so a caller can patch one node's property without
/// resending the whole array); other objects deep-merge; scalars/other arrays
/// replace.
fn merge_pipeline(base: &mut Value, patch: &Value) {
    let (b, p) = match (base.as_object_mut(), patch.as_object()) {
        (Some(b), Some(p)) => (b, p),
        _ => {
            *base = patch.clone();
            return;
        }
    };
    for (k, pv) in p {
        if (k == "nodes" || k == "edges") && pv.is_array() {
            merge_by_id(b.entry(k.clone()).or_insert_with(|| json!([])), pv);
        } else {
            match b.get_mut(k) {
                Some(bv) if bv.is_object() && pv.is_object() => deep_merge(bv, pv),
                _ => {
                    b.insert(k.clone(), pv.clone());
                }
            }
        }
    }
}

fn merge_by_id(base_arr: &mut Value, patch_arr: &Value) {
    let pitems = match patch_arr.as_array() {
        Some(a) => a,
        None => return,
    };
    let barr = match base_arr.as_array_mut() {
        Some(a) => a,
        None => {
            *base_arr = patch_arr.clone();
            return;
        }
    };
    for pi in pitems {
        let pid = pi.get("id").and_then(|v| v.as_str());
        if let Some(pid) = pid {
            if let Some(existing) = barr
                .iter_mut()
                .find(|e| e.get("id").and_then(|v| v.as_str()) == Some(pid))
            {
                deep_merge(existing, pi);
                continue;
            }
        }
        barr.push(pi.clone());
    }
}

fn deep_merge(base: &mut Value, patch: &Value) {
    match (base.as_object_mut(), patch.as_object()) {
        (Some(b), Some(p)) => {
            for (k, pv) in p {
                match b.get_mut(k) {
                    Some(bv) if bv.is_object() && pv.is_object() => deep_merge(bv, pv),
                    _ => {
                        b.insert(k.clone(), pv.clone());
                    }
                }
            }
        }
        _ => *base = patch.clone(),
    }
}

fn t_update_pipeline(args: &Value) -> Result<Value, String> {
    let patch = args
        .get("patch")
        .filter(|v| v.is_object())
        .ok_or("missing 'patch' object")?;
    let do_validate = arg_bool(args, "validate", true);
    let workspace = arg_str(args, "workspace");
    let id = arg_str(args, "id");
    let path: std::path::PathBuf = if let Some(p) = arg_str(args, "path") {
        std::path::PathBuf::from(p)
    } else if let (Some(ws), Some(id)) = (workspace, id) {
        std::path::Path::new(ws).join("pipelines").join(format!("{id}.json"))
    } else {
        return Err("provide 'path', or 'workspace' + 'id'".to_string());
    };
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut doc_val: Value =
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", path.display()))?;
    merge_pipeline(&mut doc_val, patch);

    let mut validation = Value::Null;
    if do_validate {
        let d = to_doc(&doc_val)?;
        match compile_pipeline_sql(&d) {
            Ok(stages) => validation = json!({ "ok": true, "stageCount": stages.len() }),
            Err(e) => return Err(format!("merged pipeline did not validate (not written): {e}")),
        }
    }
    let pretty = serde_json::to_string_pretty(&doc_val).map_err(|e| e.to_string())?;
    std::fs::write(&path, pretty).map_err(|e| format!("write {}: {e}", path.display()))?;

    // Keep the repo name in sync if we know the workspace + id.
    let mut registered = false;
    if let (Some(ws), Some(id)) = (workspace.as_ref(), id.as_ref()) {
        if let Some(name) = doc_val.get("name").and_then(|v| v.as_str()) {
            registered = register_pipeline_in_repo(ws, id, name);
        }
    }
    Ok(json!({ "ok": true, "path": path.to_string_lossy(), "registeredInRepository": registered, "validation": validation }))
}

fn t_run_pipeline(args: &Value) -> Result<Value, String> {
    let (v, name) = load_pipeline_value(args)?;
    let doc = to_doc(&v)?;
    let duckdb = resolve_duckdb(arg_str(args, "duckdb"))
        .ok_or("no DuckDB binary found; set DUCKLE_DUCKDB_BIN or pass 'duckdb'")?;
    std::env::set_var("DUCKLE_DUCKDB_BIN", &duckdb);
    // This is a long-lived stdio server; set the workspace env deterministically
    // every call so one run_pipeline doesn't inherit a previous call's workspace
    // (which would write logs / resolve child jobs against the wrong folder).
    if let Some(ws) = arg_str(args, "workspace") {
        std::env::set_var("DUCKLE_WORKSPACE", ws);
        std::env::set_var("DUCKLE_LOG_DIR", std::path::Path::new(ws).join("logs"));
    } else {
        std::env::remove_var("DUCKLE_WORKSPACE");
        std::env::remove_var("DUCKLE_LOG_DIR");
    }

    let engine = DuckdbEngine::new(duckdb);
    let result = engine.execute_pipeline_named(&doc, &name);

    let mut out = serde_json::to_value(&result).map_err(|e| e.to_string())?;
    // Cap preview rows so the response stays small.
    if let Some(prev) = out.get_mut("preview").and_then(|p| p.as_array_mut()) {
        for node in prev.iter_mut() {
            if let Some(rows) = node.get_mut("rows").and_then(|r| r.as_array_mut()) {
                rows.truncate(20);
            }
        }
    }
    Ok(out)
}

fn t_list_pipelines(args: &Value) -> Result<Value, String> {
    let dir = arg_str(args, "directory").ok_or("missing 'directory'")?;
    let rd = std::fs::read_dir(dir).map_err(|e| format!("read_dir {dir}: {e}"))?;
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let v: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(nodes) = v.get("nodes").and_then(|n| n.as_array()) {
            out.push(json!({
                "file": path.to_string_lossy(),
                "name": v.get("name").and_then(|x| x.as_str()).unwrap_or(""),
                "nodeCount": nodes.len(),
                "edgeCount": v.get("edges").and_then(|e| e.as_array()).map(|a| a.len()).unwrap_or(0),
            }));
        }
    }
    Ok(json!({ "count": out.len(), "pipelines": out }))
}

fn t_read_pipeline(args: &Value) -> Result<Value, String> {
    let path = arg_str(args, "path").ok_or("missing 'path'")?;
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("parse {path}: {e}"))
}

fn t_read_run_logs(args: &Value) -> Result<Value, String> {
    let pipeline_name = arg_str(args, "pipelineName").ok_or("missing 'pipelineName'")?;
    let tail = args.get("tail").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
    let base: PathBuf = if let Some(ld) = arg_str(args, "logDir") {
        PathBuf::from(ld)
    } else if let Some(ws) = arg_str(args, "workspace") {
        PathBuf::from(ws).join("logs")
    } else {
        return Err("provide 'logDir' or 'workspace'".to_string());
    };
    let file = base.join(sanitize_segment(pipeline_name)).join("runtime.log");
    let text = std::fs::read_to_string(&file).map_err(|e| format!("read {}: {e}", file.display()))?;
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let start = lines.len().saturating_sub(tail);
    let entries: Vec<Value> = lines[start..]
        .iter()
        .map(|l| serde_json::from_str::<Value>(l).unwrap_or_else(|_| json!({ "raw": l })))
        .collect();
    Ok(json!({ "file": file.to_string_lossy(), "lineCount": entries.len(), "entries": entries }))
}

fn t_build_pipeline(args: &Value) -> Result<Value, String> {
    let (v, default_name) = load_pipeline_value(args)?;
    to_doc(&v)?; // reject an invalid pipeline before staging anything
    let out = arg_str(args, "out").ok_or("missing 'out' (output artifact path)")?;
    let secrets = arg_str(args, "secrets").unwrap_or("env");
    if secrets != "env" && secrets != "passphrase" {
        return Err("secrets must be 'env' or 'passphrase'".to_string());
    }
    let name = arg_str(args, "name").unwrap_or(&default_name).to_string();

    let runner = resolve_runner().ok_or(
        "duckle-runner binary not found; set DUCKLE_RUNNER_BIN or put duckle-runner on PATH / next to duckle-mcp",
    )?;

    // Synthesize the minimal workspace layout `duckle-runner build` understands.
    let ws = std::env::temp_dir().join(format!(
        "duckle-mcp-build-{}-{}",
        sanitize_filename(&name),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&ws);
    std::fs::create_dir_all(ws.join("pipelines")).map_err(|e| format!("mkdir: {e}"))?;
    let pid = "p1";
    let repo = json!([{ "id": pid, "name": name, "type": "pipeline" }]);
    std::fs::write(
        ws.join("repository.json"),
        serde_json::to_string_pretty(&repo).unwrap_or_default(),
    )
    .map_err(|e| format!("write repository.json: {e}"))?;
    std::fs::write(
        ws.join("pipelines").join(format!("{pid}.json")),
        serde_json::to_string_pretty(&v).unwrap_or_default(),
    )
    .map_err(|e| format!("write pipeline: {e}"))?;

    let mut cmd = std::process::Command::new(&runner);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd.arg("build")
        .arg("--workspace")
        .arg(&ws)
        .arg("--pipeline-id")
        .arg(pid)
        .arg("--out")
        .arg(out)
        .arg("--secrets")
        .arg(secrets);
    if let Some(d) = arg_str(args, "duckdb") {
        cmd.arg("--duckdb").arg(d);
    }
    let output = cmd.output().map_err(|e| format!("spawn duckle-runner: {e}"))?;
    let _ = std::fs::remove_dir_all(&ws);

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if output.status.success() {
        Ok(json!({ "ok": true, "out": out, "secrets": secrets, "log": stderr.trim() }))
    } else {
        let detail = if stderr.trim().is_empty() { stdout } else { stderr };
        Err(format!("duckle-runner build failed: {}", detail.trim()))
    }
}

fn t_list_connections(args: &Value) -> Result<Value, String> {
    let ws = arg_str(args, "workspace").ok_or("missing 'workspace'")?;
    let dir = std::path::Path::new(ws).join("connections");
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let text = match std::fs::read_to_string(&path) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let mut v: Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            mask_secrets(&mut v);
            out.push(json!({
                "file": path.file_name().map(|s| s.to_string_lossy().into_owned()),
                "connection": v
            }));
        }
    }
    Ok(json!({ "count": out.len(), "connections": out }))
}

fn t_create_connection(args: &Value) -> Result<Value, String> {
    let ws = arg_str(args, "workspace").ok_or("missing 'workspace'")?;
    let name = arg_str(args, "name").ok_or("missing 'name'")?;
    let conn = args
        .get("connection")
        .filter(|v| v.is_object())
        .ok_or("missing 'connection' object")?;
    // Do not persist literal secrets: the MCP server cannot encrypt at rest
    // (that key lives in the desktop app), so secret fields must use a
    // ${ENV:KEY} placeholder resolved from the environment at run time.
    if let Some(k) = first_plaintext_secret(conn) {
        return Err(format!(
            "connection field '{k}' contains a literal secret; MCP-created connections must use a ${{ENV:KEY}} placeholder for secret fields (the value is supplied from the environment at run time)"
        ));
    }
    let dir = std::path::Path::new(ws).join("connections");
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    let id = gen_id("c");
    let path = dir.join(format!("{id}.json"));
    std::fs::write(&path, serde_json::to_string_pretty(conn).unwrap_or_default())
        .map_err(|e| format!("write: {e}"))?;

    // Best-effort: register in repository.json so the GUI lists it.
    let repo_path = std::path::Path::new(ws).join("repository.json");
    let mut registered = false;
    if let Ok(text) = std::fs::read_to_string(&repo_path) {
        if let Ok(mut repo) = serde_json::from_str::<Value>(&text) {
            if let Some(arr) = repo.as_array_mut() {
                arr.push(json!({ "id": id, "name": name, "type": "connection" }));
                if std::fs::write(&repo_path, serde_json::to_string_pretty(&repo).unwrap_or_default())
                    .is_ok()
                {
                    registered = true;
                }
            }
        }
    }
    Ok(json!({ "ok": true, "id": id, "path": path.to_string_lossy(), "registeredInRepository": registered }))
}

// ---------------------------------------------------------------------------
// resources/list + resources/read
// ---------------------------------------------------------------------------

pub fn list_resources() -> Value {
    json!([
        { "uri": "duckle://catalog", "name": "Component catalog", "description": "All Duckle components with property schemas + ports.", "mimeType": "application/json" },
        { "uri": "duckle://pipeline-format", "name": "Pipeline JSON format", "description": "The shape of a Duckle pipeline file.", "mimeType": "text/markdown" }
    ])
}

pub fn read_resource(params: Value) -> Result<Value, (i64, String)> {
    let uri = params
        .get("uri")
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing 'uri'".to_string()))?;
    let (text, mime) = match uri {
        "duckle://catalog" => (
            serde_json::to_string_pretty(catalog::full()).unwrap_or_else(|_| "{}".to_string()),
            "application/json",
        ),
        "duckle://pipeline-format" => (PIPELINE_FORMAT_DOC.to_string(), "text/markdown"),
        other => return Err((-32602, format!("unknown resource: {other}"))),
    };
    Ok(json!({ "contents": [ { "uri": uri, "mimeType": mime, "text": text } ] }))
}

const PIPELINE_FORMAT_DOC: &str = "\
# Duckle pipeline format

```json
{
  \"name\": \"my pipeline\",
  \"nodes\": [
    { \"id\": \"src\", \"type\": \"source\", \"position\": {\"x\":0,\"y\":0},
      \"data\": { \"label\": \"orders\", \"componentId\": \"src.csv\",
                  \"properties\": { \"path\": \"orders.csv\", \"hasHeader\": true } } },
    { \"id\": \"snk\", \"type\": \"sink\", \"position\": {\"x\":300,\"y\":0},
      \"data\": { \"label\": \"out\", \"componentId\": \"snk.csv\",
                  \"properties\": { \"path\": \"out.csv\" } } }
  ],
  \"edges\": [
    { \"id\": \"e1\", \"source\": \"src\", \"target\": \"snk\",
      \"sourceHandle\": \"main\", \"targetHandle\": \"main\",
      \"data\": { \"connectionType\": \"main\" } }
  ]
}
```

- Find component ids + property keys with list_components / get_component_schema.
- Handles: most nodes use the `main` port; transforms add ports like `reject`,
  `lookup_1`, `case_1`, `main_1`. Edge `data.connectionType` mirrors the handle.
- Secrets: put `${ENV:KEY}` in a property and set the env var at run time; never
  inline real credentials.";

// ---------------------------------------------------------------------------
// prompts/list + prompts/get
// ---------------------------------------------------------------------------

pub fn list_prompts() -> Value {
    json!([
        { "name": "generate_pipeline", "description": "Generate a Duckle pipeline from a plain-English goal.",
          "arguments": [ { "name": "goal", "description": "What the pipeline should do.", "required": true } ] }
    ])
}

pub fn get_prompt(params: Value) -> Result<Value, (i64, String)> {
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or((-32602, "missing prompt name".to_string()))?;
    if name != "generate_pipeline" {
        return Err((-32602, format!("unknown prompt: {name}")));
    }
    let goal = params
        .get("arguments")
        .and_then(|a| a.get("goal"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let text = format!(
        "Build a Duckle pipeline that: {goal}\n\nFirst call list_components and \
get_component_schema to choose components and property keys. Then call create_pipeline \
to write and validate it. Keep credentials as ${{ENV:KEY}} placeholders. {INSTRUCTIONS}"
    );
    Ok(json!({
        "messages": [ { "role": "user", "content": { "type": "text", "text": text } } ]
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn arg_str<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(|v| v.as_str()).filter(|s| !s.is_empty())
}

fn arg_bool(args: &Value, key: &str, default: bool) -> bool {
    args.get(key).and_then(|v| v.as_bool()).unwrap_or(default)
}

/// Load a pipeline from inline `pipeline` or a `path`, returning the raw JSON
/// value and a derived display name.
fn load_pipeline_value(args: &Value) -> Result<(Value, String), String> {
    if let Some(p) = args.get("pipeline").filter(|v| v.is_object()) {
        let name = p
            .get("name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| "pipeline".to_string());
        Ok((p.clone(), name))
    } else if let Some(path) = arg_str(args, "path") {
        let text = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
        let v: Value = serde_json::from_str(&text).map_err(|e| format!("parse {path}: {e}"))?;
        let name = v
            .get("name")
            .and_then(|x| x.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                std::path::Path::new(path)
                    .file_stem()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "pipeline".to_string())
            });
        Ok((v, name))
    } else {
        Err("provide either 'pipeline' (object) or 'path' (string)".to_string())
    }
}

fn to_doc(v: &Value) -> Result<PipelineDoc, String> {
    serde_json::from_value(v.clone()).map_err(|e| format!("not a valid pipeline: {e}"))
}

fn resolve_duckdb(explicit: Option<&str>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        let pb = PathBuf::from(p);
        if pb.exists() {
            return Some(pb);
        }
    }
    if let Ok(env) = std::env::var("DUCKLE_DUCKDB_BIN") {
        let pb = PathBuf::from(env);
        if pb.exists() {
            return Some(pb);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            for c in ["duckdb", "duckdb.exe"] {
                let pb = dir.join(c);
                if pb.exists() {
                    return Some(pb);
                }
            }
        }
    }
    Some(PathBuf::from(if cfg!(windows) { "duckdb.exe" } else { "duckdb" }))
}

fn resolve_runner() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("DUCKLE_RUNNER_BIN") {
        let pb = PathBuf::from(p);
        if pb.exists() {
            return Some(pb);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            for c in ["duckle-runner", "duckle-runner.exe"] {
                let pb = dir.join(c);
                if pb.exists() {
                    return Some(pb);
                }
            }
        }
    }
    Some(PathBuf::from(if cfg!(windows) {
        "duckle-runner.exe"
    } else {
        "duckle-runner"
    }))
}

/// Keys whose values are credentials (case-insensitive). Mirrors the desktop
/// secrets.rs SENSITIVE_KEYS + the engine's is_secret_prop_key set.
fn is_secret_key(lower_key: &str) -> bool {
    const KEYS: &[&str] = &[
        "password", "secretkey", "accesskey", "accountkey", "accountname",
        "sessiontoken", "pat", "token", "apikey", "passphrase", "secret",
    ];
    KEYS.contains(&lower_key)
}

/// Redact the `user:pass@` userinfo from a connection URL (amqp/mongo/postgres
/// style) so credentials embedded in a url/uri field aren't surfaced.
fn redact_url_userinfo(s: &str) -> Option<String> {
    let scheme_end = s.find("://")?;
    let after = &s[scheme_end + 3..];
    let at = after.find('@')?;
    let slash = after.find('/').unwrap_or(after.len());
    if at >= slash {
        return None;
    }
    Some(format!("{}://***@{}", &s[..scheme_end], &after[at + 1..]))
}

/// Recursively mask secret values anywhere in a connection object - secret-keyed
/// string fields (including nested `extra` maps) become "***", and url/uri-style
/// fields have any embedded credentials stripped.
fn mask_secrets(v: &mut Value) {
    match v {
        Value::Object(obj) => {
            for (k, val) in obj.iter_mut() {
                let lk = k.to_ascii_lowercase();
                if is_secret_key(&lk) && val.is_string() {
                    *val = json!("***");
                    continue;
                }
                if matches!(lk.as_str(), "url" | "uri" | "endpoint" | "connectionstring" | "dsn") {
                    if let Some(s) = val.as_str() {
                        if let Some(red) = redact_url_userinfo(s) {
                            *val = json!(red);
                            continue;
                        }
                    }
                }
                mask_secrets(val);
            }
        }
        Value::Array(arr) => arr.iter_mut().for_each(mask_secrets),
        _ => {}
    }
}

/// Find the first secret-keyed field holding a literal (non-`${...}`) value, so
/// create_connection can reject writing plaintext credentials to disk.
fn first_plaintext_secret(v: &Value) -> Option<String> {
    match v {
        Value::Object(obj) => {
            for (k, val) in obj {
                if is_secret_key(&k.to_ascii_lowercase()) {
                    if let Some(s) = val.as_str() {
                        let t = s.trim();
                        if !t.is_empty() && !t.starts_with("${") {
                            return Some(k.clone());
                        }
                    }
                }
                if let Some(found) = first_plaintext_secret(val) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(arr) => arr.iter().find_map(first_plaintext_secret),
        _ => None,
    }
}

/// A short, unique-enough id (no Date/random deps needed): prefix + pid + counter.
fn gen_id(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(1);
    let n = N.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", prefix, std::process::id(), n)
}

fn sanitize_filename(name: &str) -> String {
    let mut out = String::new();
    let mut prev_us = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch);
            prev_us = false;
        } else if !prev_us {
            out.push('_');
            prev_us = true;
        }
    }
    let t = out.trim_matches(|c| c == '_' || c == '.');
    if t.is_empty() {
        "pipeline".to_string()
    } else {
        t.to_string()
    }
}

fn sanitize_segment(name: &str) -> String {
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
    let c = cleaned.trim().trim_matches('.').trim();
    if c.is_empty() {
        "pipeline".to_string()
    } else {
        c.to_string()
    }
}

#[cfg(test)]
mod verify_tests {
    use super::*;

    #[test]
    fn structural_risks_clean_pipeline_has_none() {
        let p = json!({
            "nodes": [
                { "id": "s", "data": { "componentId": "src.csv", "label": "A" } },
                { "id": "k", "data": { "componentId": "snk.csv", "label": "K" } }
            ],
            "edges": [ { "source": "s", "target": "k" } ]
        });
        assert!(structural_risks(&p).is_empty(), "{:?}", structural_risks(&p));
    }

    #[test]
    fn structural_risks_flags_join_without_keys_and_missing_sink() {
        let p = json!({
            "nodes": [
                { "id": "s", "data": { "componentId": "src.csv", "label": "A" } },
                { "id": "j", "data": { "componentId": "xf.join.inner", "label": "J", "properties": {} } }
            ],
            "edges": [ { "source": "s", "target": "j" } ]
        });
        let codes: Vec<String> = structural_risks(&p)
            .iter()
            .filter_map(|r| r["code"].as_str().map(|s| s.to_string()))
            .collect();
        assert!(codes.iter().any(|c| c == "join_without_keys"), "{codes:?}");
        assert!(codes.iter().any(|c| c == "no_sink"), "{codes:?}");
    }

    #[test]
    fn structural_risks_flags_sink_without_input() {
        let p = json!({
            "nodes": [ { "id": "k", "data": { "componentId": "snk.csv", "label": "K" } } ],
            "edges": []
        });
        assert!(structural_risks(&p)
            .iter()
            .any(|r| r["code"] == "sink_without_input" && r["severity"] == "error"));
    }

    #[test]
    fn looks_like_pii_matches_common_names_and_ignores_plain_ones() {
        assert_eq!(looks_like_pii("email"), Some("email"));
        assert_eq!(looks_like_pii("emailAddress"), Some("email"));
        assert_eq!(looks_like_pii("customer_email"), Some("email"));
        assert_eq!(looks_like_pii("first_name"), Some("name"));
        assert_eq!(looks_like_pii("SSN"), Some("national_id"));
        assert_eq!(looks_like_pii("date_of_birth"), Some("date_of_birth"));
        assert_eq!(looks_like_pii("phone_number"), Some("phone"));
        assert_eq!(looks_like_pii("order_id"), None);
        assert_eq!(looks_like_pii("amount"), None);
        assert_eq!(looks_like_pii("created_at"), None);
    }

    #[test]
    fn impact_inverts_lineage_and_excludes_self() {
        use duckle_duckdb_engine::lineage::RootColumn;
        let root = |n: &str, c: &str| RootColumn { node: n.to_string(), column: c.to_string() };
        let mut lineage: Lineage = std::collections::HashMap::new();
        // s.email flows through to k.email; s.email also feeds k.email_hash.
        lineage.insert("s".into(), vec![("email".into(), vec![root("s", "email")])]);
        lineage.insert(
            "k".into(),
            vec![
                ("email".into(), vec![root("s", "email")]),
                ("email_hash".into(), vec![root("s", "email")]),
            ],
        );
        let map = impact_from_lineage(&lineage);
        let deps = map
            .get(&("s".to_string(), "email".to_string()))
            .expect("s.email has dependents");
        // The source's own identity entry is excluded; both sink columns remain.
        assert_eq!(deps.len(), 2, "{deps:?}");
        assert!(deps.contains(&("k".to_string(), "email".to_string())));
        assert!(deps.contains(&("k".to_string(), "email_hash".to_string())));
        assert!(!deps.contains(&("s".to_string(), "email".to_string())));
    }

    #[test]
    fn declared_columns_reads_schema_array() {
        let n = json!({ "id": "s", "data": { "schema": [ { "name": "email" }, { "name": "qty" } ] } });
        assert_eq!(declared_columns(&n), vec!["email".to_string(), "qty".to_string()]);
    }

    #[test]
    fn suggest_contracts_tags_pii_from_source_schema_without_duckdb() {
        let pipeline = json!({
            "nodes": [
                { "id": "s", "data": { "componentId": "src.csv", "label": "People",
                    "schema": [ { "name": "email" }, { "name": "order_id" }, { "name": "amount" } ] } },
                { "id": "k", "data": { "componentId": "snk.csv", "label": "Out" } }
            ],
            "edges": [ { "source": "s", "target": "k" } ]
        });
        let out = t_suggest_contracts(&json!({ "pipeline": pipeline })).unwrap();
        let suggestions = out["suggestions"].as_array().unwrap();
        let src = suggestions
            .iter()
            .find(|s| s["nodeId"] == "s")
            .expect("source has a suggestion");
        let pii = src["suggestedContracts"]["pii"].as_array().unwrap();
        assert_eq!(pii.len(), 1);
        assert_eq!(pii[0], "email");
        // requireColumns anchors the source's declared columns.
        let req = src["suggestedContracts"]["requireColumns"].as_array().unwrap();
        assert_eq!(req.len(), 3);
        // A sink with no declared/lineage columns yields no suggestion.
        assert!(suggestions.iter().all(|s| s["nodeId"] != "k"));
    }
}
