//! Context resolution: the Rust port of frontend/src/run-resolve.ts.
//!
//! Reads a workspace's on-disk repository.json, contexts/, routines/ and a
//! single pipeline, then resolves it for headless execution:
//!   1. Inline a referenced SQL routine into Custom-SQL nodes.
//!   2. Substitute `${var}` / `${context.var}` references in every string
//!      field of every node's properties with the workspace context vars.
//!   3. Rewrite a child-pipeline reference (Run Job / Iterate / Foreach /
//!      Try) stored as a workspace pipeline id/name to its on-disk file path.
//!
//! Used by the `build` subcommand. The browser hydrates context/routine
//! payloads before calling resolveForRun; this port loads them from disk
//! itself (a naive port reading only repository.json would see zero vars).

use crate::PipelineDoc;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Property keys that hold a reference to another pipeline the engine reads
/// from disk. The dropdown stores a portable pipeline id; the engine needs
/// a file path, so we resolve here at build time.
const PIPELINE_REF_KEYS: [&str; 4] = [
    "pipelineRef",
    "iteratePipelineRef",
    "foreachPipelineRef",
    "fallbackPipelineRef",
];

/// A repository.json entry. Only id/name/type are needed; parentId and any
/// other keys are ignored.
#[derive(Deserialize)]
struct RepoItem {
    id: String,
    name: String,
    #[serde(rename = "type")]
    kind: String,
}

/// contexts/<id>.json payload.
#[derive(Deserialize)]
struct ContextPayload {
    #[serde(default)]
    variables: Vec<ContextVariable>,
}

#[derive(Deserialize)]
struct ContextVariable {
    key: String,
    value: String,
    #[serde(default)]
    secret: bool,
}

/// routines/<id>.json payload.
#[derive(Deserialize)]
struct RoutinePayload {
    language: String,
    code: String,
}

/// The resolved pipeline plus the raw plaintext values of secret context
/// vars (captured BEFORE resolution) so the build step can value-match
/// redact them and run the leak guard.
pub struct Resolved {
    pub doc: PipelineDoc,
    pub secret_values: Vec<String>,
}

/// Read+parse repository.json into the repo item list. A missing file yields
/// an empty list (no contexts / routines / pipeline-refs to resolve), so
/// resolve_workspace then behaves like a plain pipeline load instead of failing
/// the run - important for headless callers (the scheduler) and minimal
/// workspaces. Only a present-but-corrupt repository.json is an error.
fn read_repo(workspace: &Path) -> Result<Vec<RepoItem>, String> {
    let path = workspace.join("repository.json");
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {}", path.display(), e)),
    };
    serde_json::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))
}

/// Dynamic date/time builtins so source / sink paths can carry a timestamp
/// (e.g. `${workspace}/exports/${date}/orders.parquet` or `out_${datetime}.csv`).
/// All UTC so a run names files the same on any machine / in CI.
///   `${date}`      -> YYYY-MM-DD
///   `${time}`      -> HHMMSS
///   `${datetime}`  -> YYYY-MM-DD_HHMMSS   (filename-safe, no colons)
///   `${timestamp}` -> epoch seconds
///   `${now}`       -> ISO-8601 (has colons; for values, not paths)
pub(crate) fn insert_time_builtins(vars: &mut HashMap<String, String>) {
    let now = chrono::Utc::now();
    vars.insert("date".to_string(), now.format("%Y-%m-%d").to_string());
    vars.insert("time".to_string(), now.format("%H%M%S").to_string());
    vars.insert("datetime".to_string(), now.format("%Y-%m-%d_%H%M%S").to_string());
    vars.insert("timestamp".to_string(), now.timestamp().to_string());
    vars.insert("now".to_string(), now.format("%Y-%m-%dT%H:%M:%SZ").to_string());
}

/// Resolve the dynamic date/time builtins (see [`insert_time_builtins`]) in
/// every string property of every node, in place. This is a RUN-TIME pass,
/// kept separate from [`build_context_vars`] / [`resolve_workspace`] on
/// purpose: those also run at BUILD time (the `build` subcommand), and a built
/// bundle must stamp the date when it RUNS, not when it was built. So the
/// resolvers leave `${date}` & friends untouched and the run-time callers (the
/// scheduler, the headless runner) apply this just before executing. An
/// unknown `${...}` is left verbatim, exactly like the context-var pass.
pub fn apply_time_builtins(doc: &mut PipelineDoc) {
    let mut vars: HashMap<String, String> = HashMap::new();
    insert_time_builtins(&mut vars);
    let re = match regex::Regex::new(r"\$\{([^}]+)\}") {
        Ok(re) => re,
        Err(_) => return,
    };
    let replace = |s: &str| -> String {
        re.replace_all(s, |caps: &regex::Captures| match vars.get(caps[1].trim()) {
            Some(v) => v.clone(),
            None => caps[0].to_string(),
        })
        .into_owned()
    };
    for node in &mut doc.nodes {
        if let Some(props) = node.data.properties.as_mut() {
            substitute_deep(props, &replace);
        }
    }
}

/// Resolve the portable workspace placeholders (`${workspace}` / `${projectroot}`),
/// the date/time builtins, and any workspace context-file variables in every node
/// property, in place. Used by the headless run paths (the CLI runner and the web
/// server) so a pipeline LOADED FROM A FILE resolves the same `${workspace}`-style
/// paths that the desktop's by-id load ([`resolve_workspace`]) and foreach children
/// already do. An unknown `${...}` is left verbatim.
pub fn apply_workspace_context(doc: &mut PipelineDoc, workspace: &Path) {
    let vars = crate::connectors::context_vars_for_workspace(workspace);
    if vars.is_empty() {
        return;
    }
    let re = match regex::Regex::new(r"\$\{([^}]+)\}") {
        Ok(re) => re,
        Err(_) => return,
    };
    let replace = |s: &str| -> String {
        re.replace_all(s, |caps: &regex::Captures| match vars.get(caps[1].trim()) {
            Some(v) => v.clone(),
            None => caps[0].to_string(),
        })
        .into_owned()
    };
    for node in &mut doc.nodes {
        if let Some(props) = node.data.properties.as_mut() {
            substitute_deep(props, &replace);
        }
    }
}

/// Global context file: a workspace setting (`.duckle/settings.json`
/// "context_file") can point at a key/value file whose entries load
/// into the global context for every run, so `${KEY}` resolves everywhere
/// without wiring a node. Formats by extension: `.json` (a flat object), `.csv`
/// (two columns key,value), otherwise `KEY=VALUE` / `KEY: VALUE` lines (e.g.
/// .env / .properties; `#` and `;` lines are comments). A relative path resolves
/// against the workspace root. These OVERRIDE the static context defaults
/// (runtime values win). Best-effort: a missing/unreadable file yields no vars.
pub fn context_file_vars(workspace: &Path) -> HashMap<String, String> {
    let settings: JsonValue = match std::fs::read_to_string(
        workspace.join(".duckle").join("settings.json"),
    )
    .ok()
    .and_then(|t| serde_json::from_str(&t).ok())
    {
        Some(v) => v,
        None => return HashMap::new(),
    };
    let file = match settings
        .get("context_file")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(f) => f,
        None => return HashMap::new(),
    };
    let p = Path::new(file);
    let path = if p.is_absolute() {
        p.to_path_buf()
    } else {
        workspace.join(file)
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => parse_kv(&path, &text),
        Err(_) => HashMap::new(),
    }
}

fn parse_kv(path: &Path, text: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if ext == "json" {
        if let Ok(JsonValue::Object(m)) = serde_json::from_str::<JsonValue>(text) {
            for (k, v) in m {
                let val = match v {
                    JsonValue::String(s) => s,
                    other => other.to_string(),
                };
                out.insert(k, val);
            }
        }
        return out;
    }
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        let pair = if ext == "csv" {
            line.split_once(',')
        } else {
            line.split_once('=').or_else(|| line.split_once(':'))
        };
        if let Some((k, v)) = pair {
            let k = k.trim().trim_matches('"');
            let v = v.trim().trim_matches('"');
            if !k.is_empty() {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

/// Build the context-var map (bare + `<contextName>.key`) and capture the
/// raw values of secret:true vars. Port of buildContextVars plus secret
/// capture. When `context` is Some, only that named context is loaded.
fn build_context_vars(
    workspace: &Path,
    repo: &[RepoItem],
    context: Option<&str>,
) -> Result<(HashMap<String, String>, Vec<String>), String> {
    let mut vars: HashMap<String, String> = HashMap::new();
    let mut secret_values: Vec<String> = Vec::new();
    let mut matched_requested = false;

    // Built-in placeholders for the workspace root so paths can be written
    // relative to it and the workspace folder stays portable (#37). Inserted
    // first so an explicit context variable of the same name still wins.
    // Separators normalized to `/` for parity with the frontend builtinVars
    // (DuckDB accepts them on every platform).
    let ws_root = workspace.to_string_lossy().replace('\\', "/");
    vars.insert("workspace".to_string(), ws_root.clone());
    vars.insert("projectroot".to_string(), ws_root);

    for item in repo {
        if item.kind != "context" {
            continue;
        }
        // --context filter (runner-only superset over the TS, which always
        // merges all contexts). Skip non-matching items; require a match.
        if let Some(want) = context {
            if item.name != want {
                continue;
            }
            matched_requested = true;
        }

        let path = workspace.join("contexts").join(format!("{}.json", item.id));
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            // Missing file -> skip (mirrors TS `if (!payload?.variables)`).
            Err(_) => continue,
        };
        let payload: ContextPayload = serde_json::from_str(&text)
            .map_err(|e| format!("parse {}: {}", path.display(), e))?;

        for v in &payload.variables {
            // Both the bare key and a context-namespaced key resolve;
            // in-array-order insert gives last-write-wins like JS `out[k]=`.
            vars.insert(v.key.clone(), v.value.clone());
            vars.insert(format!("{}.{}", item.name, v.key), v.value.clone());
            if v.secret && !v.value.is_empty() {
                secret_values.push(v.value.clone());
            }
        }
    }

    if let Some(want) = context {
        if !matched_requested {
            return Err(format!("context not found: {}", want));
        }
    }

    Ok((vars, secret_values))
}

/// Build the sqlRoutines map (id + name -> code). Gated on language=="sql"
/// and non-empty code, matching resolveForRun (the source of truth; the
/// brief's "regardless of language" is intentionally not followed).
fn build_sql_routines(workspace: &Path, repo: &[RepoItem]) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = HashMap::new();
    for item in repo {
        if item.kind != "routine" {
            continue;
        }
        let path = workspace.join("routines").join(format!("{}.json", item.id));
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(_) => continue, // missing routine file -> skip, no inline.
        };
        let payload: RoutinePayload = match serde_json::from_str(&text) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if payload.language == "sql" && !payload.code.is_empty() {
            out.insert(item.id.clone(), payload.code.clone());
            out.insert(item.name.clone(), payload.code);
        }
    }
    out
}

/// Build the pipelinePaths map (id + name -> absolute on-disk path).
fn build_pipeline_paths(workspace: &Path, repo: &[RepoItem]) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = HashMap::new();
    for item in repo {
        if item.kind != "pipeline" {
            continue;
        }
        let file: PathBuf = workspace.join("pipelines").join(format!("{}.json", item.id));
        // Normalize to forward slashes to match the TS joinPath (run-resolve.ts)
        // so the rewritten ref string is byte-identical to the canvas/run path.
        // The engine reads the value via fs::read_to_string, which accepts both
        // separators on Windows, so this is a parity (not correctness) change.
        let s = file.to_string_lossy().replace('\\', "/");
        out.insert(item.id.clone(), s.clone());
        out.insert(item.name.clone(), s);
    }
    out
}

/// Deep `${expr}` substitution walker, shared by the context pass and the
/// run-time ENV pass. Recurses arrays + object VALUES (never object keys);
/// numbers/bools/null pass through unchanged.
pub fn substitute_deep(value: &mut JsonValue, replace: &impl Fn(&str) -> String) {
    match value {
        JsonValue::String(s) => *s = replace(s),
        JsonValue::Array(a) => {
            for v in a {
                substitute_deep(v, replace);
            }
        }
        JsonValue::Object(m) => {
            for (_k, v) in m.iter_mut() {
                substitute_deep(v, replace);
            }
        }
        _ => {}
    }
}

/// Resolve a workspace pipeline for execution. See module docs.
pub fn resolve_workspace(
    workspace: &Path,
    pipeline_id: &str,
    context: Option<&str>,
) -> Result<Resolved, String> {
    let repo = read_repo(workspace)?;
    let (vars, secret_values) = build_context_vars(workspace, &repo, context)?;
    let sql_routines = build_sql_routines(workspace, &repo);
    let pipeline_paths = build_pipeline_paths(workspace, &repo);

    let pipe_path = workspace
        .join("pipelines")
        .join(format!("{}.json", pipeline_id));
    let text = std::fs::read_to_string(&pipe_path)
        .map_err(|e| format!("read {}: {}", pipe_path.display(), e))?;
    let mut doc: PipelineDoc = serde_json::from_str(&text)
        .map_err(|e| format!("parse {}: {}", pipe_path.display(), e))?;

    // Compile the placeholder regex once and capture vars for the closure.
    let re = regex::Regex::new(r"\$\{([^}]+)\}").map_err(|e| e.to_string())?;
    let replace = |s: &str| -> String {
        re.replace_all(s, |caps: &regex::Captures| {
            let key = caps[1].trim();
            match vars.get(key) {
                Some(v) => v.clone(),
                // Unknown key -> leave the FULL original match verbatim.
                None => caps[0].to_string(),
            }
        })
        .into_owned()
    };
    let has_vars = !vars.is_empty();

    for node in &mut doc.nodes {
        let cid = node.data.component_id.as_deref();
        let is_sql = matches!(cid, Some("code.sql") | Some("code.sqltemplate"));

        // Determine whether routine inlining will apply, so we know if we
        // need to materialize an object when properties was None.
        let inline_code: Option<String> = if is_sql {
            node.data.properties.as_ref().and_then(|p| {
                let r#ref = p.get("routineRef").and_then(JsonValue::as_str).unwrap_or("");
                let inline = p
                    .get("sql")
                    .and_then(JsonValue::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if !r#ref.is_empty() && inline.is_empty() {
                    sql_routines.get(r#ref).cloned()
                } else {
                    None
                }
            })
        } else {
            None
        };

        // When properties is None there is nothing to substitute or
        // rewrite (no keys to find); only routine inlining can create an
        // object. Otherwise leave it None to preserve the skip_serializing_if
        // round-trip.
        if node.data.properties.is_none() && inline_code.is_none() {
            continue;
        }

        // 1. Routine inline FIRST.
        if let Some(code) = inline_code {
            let props = node
                .data
                .properties
                .get_or_insert_with(|| JsonValue::Object(serde_json::Map::new()));
            if let Some(obj) = props.as_object_mut() {
                obj.insert("sql".to_string(), JsonValue::String(code));
            }
        }

        // 2. Deep substitution over the WHOLE props object (so `${VAR}`
        //    inside an inlined routine body also resolves).
        if has_vars {
            if let Some(props) = node.data.properties.as_mut() {
                substitute_deep(props, &replace);
            }
        }

        // 3. Pipeline-ref rewrite on the POST-substitution props.
        if !pipeline_paths.is_empty() {
            if let Some(JsonValue::Object(obj)) = node.data.properties.as_mut() {
                for key in PIPELINE_REF_KEYS {
                    if let Some(v) = obj.get(key).and_then(JsonValue::as_str) {
                        if let Some(path) = pipeline_paths.get(v) {
                            let path = path.clone();
                            obj.insert(key.to_string(), JsonValue::String(path));
                        }
                    }
                }
            }
        }
    }

    Ok(Resolved { doc, secret_values })
}

#[cfg(test)]
mod tests {
    use super::resolve_workspace;
    use std::fs;

    fn write(path: &std::path::Path, content: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    #[test]
    fn context_file_loads_kv_lines() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        write(&ws.join(".duckle/settings.json"), r#"{"context_file":"ctx.env"}"#);
        write(&ws.join("ctx.env"), "# comment\nGREETING=hello\nNUM = 42\n");
        let vars = super::context_file_vars(ws);
        assert_eq!(vars.get("GREETING").map(String::as_str), Some("hello"));
        assert_eq!(vars.get("NUM").map(String::as_str), Some("42"));
    }

    #[test]
    fn context_file_loads_json() {
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        write(&ws.join(".duckle/settings.json"), r#"{"context_file":"ctx.json"}"#);
        write(&ws.join("ctx.json"), r#"{"A":"1","B":"two"}"#);
        let vars = super::context_file_vars(ws);
        assert_eq!(vars.get("A").map(String::as_str), Some("1"));
        assert_eq!(vars.get("B").map(String::as_str), Some("two"));
    }

    #[test]
    fn context_file_absent_when_unconfigured() {
        let dir = tempfile::tempdir().unwrap();
        assert!(super::context_file_vars(dir.path()).is_empty());
    }

    #[test]
    fn resolves_context_var_password_in_node_properties() {
        // issue #32: a ${context.X} / ${X} password must be substituted before
        // execution. The canvas did this in the frontend; scheduled runs now go
        // through resolve_workspace so they substitute too instead of sending
        // the raw placeholder to the driver (ORA-01017).
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        write(
            &ws.join("repository.json"),
            r#"[{"id":"ctx1","name":"Prod","type":"context"}]"#,
        );
        write(
            &ws.join("contexts/ctx1.json"),
            r#"{"variables":[{"key":"ORACLE_PW","value":"s3cr3t","secret":true}]}"#,
        );
        write(
            &ws.join("pipelines/p1.json"),
            r#"{"nodes":[{"id":"o","position":{"x":0,"y":0},"data":{"label":"Oracle","componentId":"src.oracle","properties":{"host":"db","password":"${Prod.ORACLE_PW}","user":"${ORACLE_PW}"}}}],"edges":[]}"#,
        );

        let resolved = resolve_workspace(ws, "p1", None).unwrap();
        let props = resolved.doc.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(
            props["password"],
            serde_json::json!("s3cr3t"),
            "context-namespaced var ${{ContextName.KEY}} must substitute"
        );
        assert_eq!(
            props["user"],
            serde_json::json!("s3cr3t"),
            "bare var must substitute too"
        );
        assert!(
            resolved.secret_values.contains(&"s3cr3t".to_string()),
            "secret value captured for the leak guard"
        );
    }

    #[test]
    fn missing_repository_json_loads_pipeline_without_failing() {
        // A workspace with no repository.json must still load the pipeline (no
        // contexts to resolve), not error - this is what keeps a scheduled run
        // working when there is nothing to substitute.
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        write(
            &ws.join("pipelines/p1.json"),
            r#"{"nodes":[{"id":"o","position":{"x":0,"y":0},"data":{"label":"X","componentId":"src.csv","properties":{"path":"${UNSET}"}}}],"edges":[]}"#,
        );
        let resolved = resolve_workspace(ws, "p1", None).unwrap();
        let props = resolved.doc.nodes[0].data.properties.as_ref().unwrap();
        // No vars -> unknown placeholder left verbatim (not an error).
        assert_eq!(props["path"], serde_json::json!("${UNSET}"));
    }

    #[test]
    fn resolves_builtin_workspace_placeholder() {
        // issue #37: ${workspace} (and the ${projectroot} alias) resolve to the
        // workspace root with no context defined, so paths can be written
        // relative to it and the workspace folder stays portable.
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        write(
            &ws.join("pipelines/p1.json"),
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"CSV","componentId":"src.csv","properties":{"path":"${workspace}/input_data/orders.csv","alt":"${projectroot}/out.parquet"}}}],"edges":[]}"#,
        );
        let resolved = resolve_workspace(ws, "p1", None).unwrap();
        let props = resolved.doc.nodes[0].data.properties.as_ref().unwrap();
        let root = ws.to_string_lossy().replace('\\', "/");
        assert_eq!(
            props["path"],
            serde_json::json!(format!("{}/input_data/orders.csv", root)),
            "${{workspace}} must resolve to the workspace root"
        );
        assert_eq!(
            props["alt"],
            serde_json::json!(format!("{}/out.parquet", root)),
            "${{projectroot}} alias must resolve to the workspace root"
        );
    }

    #[test]
    fn applies_dynamic_datetime_placeholders() {
        // discussion #61: ${date}/${datetime}/${timestamp} let a sink path carry
        // a run-time stamp (e.g. exports/${date}/orders.parquet). The run-time
        // pass resolves them; we can't assert the wall-clock value, so check the
        // shape. An unknown ${...} must survive untouched.
        let mut doc: crate::PipelineDoc = serde_json::from_str(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"P","componentId":"snk.parquet","properties":{"path":"exports/${date}/orders_${datetime}.parquet","ts":"${timestamp}","keep":"${UNKNOWN}"}}}],"edges":[]}"#,
        )
        .unwrap();
        super::apply_time_builtins(&mut doc);
        let props = doc.nodes[0].data.properties.as_ref().unwrap();
        let path = props["path"].as_str().unwrap();
        assert!(
            !path.contains("${date}") && !path.contains("${datetime}"),
            "date placeholders must be substituted, got {path}"
        );
        // exports/YYYY-MM-DD/orders_YYYY-MM-DD_HHMMSS.parquet
        let re_ok = path.starts_with("exports/")
            && path.matches(|c: char| c == '-').count() >= 4
            && path.ends_with(".parquet");
        assert!(re_ok, "path shape unexpected: {path}");
        let ts = props["ts"].as_str().unwrap();
        assert!(
            ts.chars().all(|c| c.is_ascii_digit()) && ts.len() >= 10,
            "${{timestamp}} must be epoch seconds, got {ts}"
        );
        assert_eq!(
            props["keep"],
            serde_json::json!("${UNKNOWN}"),
            "an unknown placeholder must be left verbatim"
        );
    }

    #[test]
    fn resolve_workspace_does_not_bake_datetime() {
        // Build-safety guard: resolve_workspace (also used by the `build`
        // subcommand) must NOT resolve the date/time builtins, so a built bundle
        // stamps the date when it RUNS, not when it was built. ${workspace}
        // resolves; ${date} survives for the run-time pass to fill.
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        write(
            &ws.join("pipelines/p1.json"),
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"P","componentId":"snk.parquet","properties":{"path":"${workspace}/exports/${date}/orders.parquet"}}}],"edges":[]}"#,
        );
        let resolved = resolve_workspace(ws, "p1", None).unwrap();
        let path = resolved.doc.nodes[0].data.properties.as_ref().unwrap()["path"]
            .as_str()
            .unwrap()
            .to_string();
        let root = ws.to_string_lossy().replace('\\', "/");
        assert_eq!(
            path,
            format!("{}/exports/${{date}}/orders.parquet", root),
            "${{workspace}} resolves but ${{date}} must remain for the run-time pass"
        );
    }
}
