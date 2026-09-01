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
    /// Layer this context sits on when several are active (#204). Higher wins;
    /// absent is the base layer. Mirrors ContextPayload.priority in the TS so
    /// a headless run resolves exactly what the canvas resolved.
    #[serde(default)]
    priority: i64,
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

/// Format one time builtin (`date` / `time` / `datetime` / `timestamp` / `now`)
/// from a resolved instant. Returns None for any other base name.
fn format_time_builtin(base: &str, t: chrono::DateTime<chrono::Utc>) -> Option<String> {
    Some(match base {
        "date" => t.format("%Y-%m-%d").to_string(),
        "time" => t.format("%H%M%S").to_string(),
        "datetime" => t.format("%Y-%m-%d_%H%M%S").to_string(),
        "timestamp" => t.timestamp().to_string(),
        "now" => t.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        _ => return None,
    })
}

/// Parse a relative offset like `+1d`, `-2h`, `+30m`, `-45s`, or a combination
/// (`+1d6h30m`) into a signed Duration (issue #191). A leading `+`/`-` (default
/// `+`) sets the sign for the whole offset; each segment is `<digits><unit>`
/// with unit in d / h / m / s. Returns None on anything malformed so the caller
/// leaves the placeholder verbatim rather than guessing.
fn parse_offset(s: &str) -> Option<chrono::Duration> {
    let (sign, rest) = match s.as_bytes().first() {
        Some(b'+') => (1i32, &s[1..]),
        Some(b'-') => (-1i32, &s[1..]),
        _ => (1i32, s),
    };
    if rest.is_empty() {
        return None;
    }
    let mut total = chrono::Duration::zero();
    let mut num = String::new();
    for ch in rest.chars() {
        if ch.is_ascii_digit() {
            num.push(ch);
            continue;
        }
        if num.is_empty() {
            return None; // a unit with no preceding number
        }
        let n: i64 = num.parse().ok()?;
        num.clear();
        let seg = match ch {
            'd' => chrono::Duration::days(n),
            'h' => chrono::Duration::hours(n),
            'm' => chrono::Duration::minutes(n),
            's' => chrono::Duration::seconds(n),
            _ => return None, // unknown unit
        };
        total = total.checked_add(&seg)?;
    }
    if !num.is_empty() {
        return None; // trailing digits with no unit
    }
    Some(total * sign)
}

/// Resolve a time-builtin placeholder name, with an optional relative offset, to
/// its value at `now` (issue #191). `date` / `time` / `datetime` / `timestamp` /
/// `now` resolve directly; the same names followed by a signed offset (`date+1d`,
/// `now-2h`, `datetime+30m`) resolve to the shifted instant. Returns None for any
/// non-builtin name or a malformed offset, so unknown `${...}` is left verbatim.
pub(crate) fn resolve_time_builtin(name: &str, now: chrono::DateTime<chrono::Utc>) -> Option<String> {
    // Longest bases first so `datetime` / `timestamp` win over `date` / `time`.
    const BASES: [&str; 5] = ["timestamp", "datetime", "date", "time", "now"];
    for base in BASES {
        if name == base {
            return format_time_builtin(base, now);
        }
        if let Some(rest) = name.strip_prefix(base) {
            if rest.starts_with('+') || rest.starts_with('-') {
                let dur = parse_offset(rest)?;
                return format_time_builtin(base, now + dur);
            }
        }
    }
    None
}

/// Whether a placeholder name is a time builtin (with or without an offset).
fn is_time_builtin(name: &str) -> bool {
    resolve_time_builtin(name, chrono::Utc::now()).is_some()
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
    // One `now` for the whole pass so every placeholder (and every offset) in a
    // run stamps the same instant.
    let now = chrono::Utc::now();
    let re = match regex::Regex::new(r"\$\{([^}]+)\}") {
        Ok(re) => re,
        Err(_) => return,
    };
    let replace = |s: &str| -> String {
        re.replace_all(s, |caps: &regex::Captures| {
            match resolve_time_builtin(caps[1].trim(), now) {
                Some(v) => v,
                None => caps[0].to_string(),
            }
        })
        .into_owned()
    };
    for node in &mut doc.nodes {
        if let Some(props) = node.data.properties.as_mut() {
            substitute_deep(props, &replace);
        }
    }
}

/// Resolve `${VAULT:NAME}` placeholders by asking an external secret store.
///
/// Organisations that keep credentials in a vault do not want them copied into
/// a workspace, an environment variable or an image layer. This fetches each
/// one at run time and holds it only for the run.
///
/// The command comes from `DUCKLE_VAULT_COMMAND`, which the operator sets on
/// the host - NEVER from the pipeline. A pipeline that could name its own
/// command would be remote code execution by anyone allowed to author one, and
/// authoring is not meant to be shell access. The pipeline supplies only the
/// object name.
///
/// The template is split into arguments and run directly, without a shell, so a
/// name cannot inject a second command. `{name}` is replaced inside whichever
/// argument holds it, and the secret is whatever the command prints on stdout,
/// trimmed of its trailing newline.
///
/// Example, for a vault whose CLI takes a query string:
///   DUCKLE_VAULT_COMMAND=CLIPasswordSDK GetPassword -p Query=Object={name} -o Password
///
/// A name that does not resolve is left verbatim, exactly like the other
/// passes, so a missing secret fails where it is used and says which one.
pub fn apply_vault(doc: &mut PipelineDoc) {
    let template = match std::env::var("DUCKLE_VAULT_COMMAND") {
        Ok(t) if !t.trim().is_empty() => t,
        _ => return,
    };
    let re = match regex::Regex::new(r"\$\{VAULT:([^}]+)\}") {
        Ok(re) => re,
        Err(_) => return,
    };
    let argv: Vec<String> = template.split_whitespace().map(str::to_string).collect();
    if argv.is_empty() {
        return;
    }
    // One fetch per distinct name per run: a credential is usually referenced by
    // several nodes and a vault call is neither free nor silent.
    let cache: std::cell::RefCell<std::collections::HashMap<String, String>> =
        std::cell::RefCell::new(Default::default());
    let replace = |s: &str| -> String {
        re.replace_all(s, |caps: &regex::Captures| {
            let name = caps[1].trim();
            // A name is an identifier in someone else's system. Anything that
            // could carry a newline or a NUL into an argument is refused rather
            // than passed on.
            if name.is_empty() || name.chars().any(|c| c.is_control()) {
                return caps[0].to_string();
            }
            if let Some(v) = cache.borrow().get(name) {
                return v.clone();
            }
            let args: Vec<String> = argv[1..]
                .iter()
                .map(|a| a.replace("{name}", name))
                .collect();
            let mut cmd = std::process::Command::new(&argv[0]);
            cmd.args(&args);
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
            }
            match cmd.output() {
                Ok(out) if out.status.success() => {
                    let v = String::from_utf8_lossy(&out.stdout).trim_end().to_string();
                    if v.is_empty() {
                        return caps[0].to_string();
                    }
                    cache.borrow_mut().insert(name.to_string(), v.clone());
                    v
                }
                // Deliberately no stderr in the message: a vault client often
                // echoes the query, and the query names the secret.
                _ => caps[0].to_string(),
            }
        })
        .into_owned()
    };
    for node in &mut doc.nodes {
        if let Some(props) = node.data.properties.as_mut() {
            substitute_deep(props, &replace);
        }
    }
}

/// Resolve `${ENV:NAME}` placeholders from the process environment, in place.
/// This is the run-time env pass shared by the desktop interactive run, the
/// desktop scheduler, and the headless runner so a pipeline can reference OS
/// environment variables (issue #137). An unset NAME is left verbatim, exactly
/// like the other passes. The runner layers its own extra tiers (secrets.env /
/// secrets.enc) on top of this process-env tier.
pub fn apply_env(doc: &mut PipelineDoc) {
    let re = match regex::Regex::new(r"\$\{ENV:([^}]+)\}") {
        Ok(re) => re,
        Err(_) => return,
    };
    let replace = |s: &str| -> String {
        re.replace_all(s, |caps: &regex::Captures| match std::env::var(caps[1].trim()) {
            Ok(v) => v,
            Err(_) => caps[0].to_string(),
        })
        .into_owned()
    };
    for node in &mut doc.nodes {
        if let Some(props) = node.data.properties.as_mut() {
            substitute_deep(props, &replace);
        }
    }
}

/// Resolve `${ENV:NAME}` placeholders in a single node's options in place, the
/// value-level counterpart to [`apply_env`]. The inspect / autodetect path works
/// on one node's options rather than a whole document, and without this pass a
/// host or password stored as `${ENV:...}` reached the ATTACH verbatim, the
/// connection failed, and autodetect fell back to a fake schema (issue #148).
/// An unset NAME is left verbatim, exactly like the run-time pass.
pub fn apply_env_to_value(value: &mut JsonValue) {
    let re = match regex::Regex::new(r"\$\{ENV:([^}]+)\}") {
        Ok(re) => re,
        Err(_) => return,
    };
    let replace = |s: &str| -> String {
        re.replace_all(s, |caps: &regex::Captures| match std::env::var(caps[1].trim()) {
            Ok(v) => v,
            Err(_) => caps[0].to_string(),
        })
        .into_owned()
    };
    substitute_deep(value, &replace);
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
    // A name a node in this pipeline works out while the run is under way belongs to
    // that node, not to the static context. The context routinely declares the same
    // name with no value - that is how a job declares one it means to fill in later -
    // and taking it from there first would replace the placeholder with nothing, so
    // the step meant to read the run value would quietly read an empty string instead.
    let set_at_run_time = crate::plan::run_var_names(doc);
    let replace = |s: &str| -> String {
        re.replace_all(s, |caps: &regex::Captures| {
            let name = caps[1].trim();
            if set_at_run_time.contains(name) {
                return caps[0].to_string();
            }
            match vars.get(name) {
                Some(v) => v.clone(),
                None => caps[0].to_string(),
            }
        })
        .into_owned()
    };
    for node in &mut doc.nodes {
        if let Some(props) = node.data.properties.as_mut() {
            substitute_deep(props, &replace);
        }
    }
}

/// Resolve user-supplied runtime parameters (`${KEY}` -> value) in every node
/// property, in place. The web dashboard's "run with parameters" form sends
/// these so an operator can override context variables for a single manual run
/// without editing the workspace context. Apply this AFTER
/// [`apply_time_builtins`] and BEFORE [`apply_workspace_context`] so a supplied
/// value wins over the static context default, while any `${KEY}` left unset
/// falls through to the normal context resolution. An unknown `${...}` is left
/// verbatim.
/// Properties whose value is EXECUTED rather than read.
///
/// `code.shell` takes its command from `code`, falling back to `command`
/// (plan/mod.rs), and hands it to an interpreter. A parameter reaching one of
/// these is not data, it is program text.
const EXECUTED_PROPS: [&str; 6] = ["code", "command", "script", "shell", "args", "workingDir"];

/// Characters that end one shell word and begin something else. Conservative on
/// purpose: a parameter is meant to be a VALUE, so anything that could restructure
/// the command is refused rather than escaped, because escaping correctly differs
/// per interpreter and this code does not know which one will run it.
const SHELL_METACHARACTERS: [char; 13] = [
    ';', '&', '|', '$', '`', '(', ')', '<', '>', '\n', '\r', '{', '}',
];

fn is_reserved_param(name: &str) -> bool {
    // Exactly what discover_parameters refuses to offer. A caller supplying one of
    // these is not filling in a parameter, it is redefining a builtin: overriding
    // ${workspace} or ${projectroot} repoints every path the pipeline reads and
    // writes, and ${ENV:...} is meant to come from secrets, never from the request.
    name.starts_with("ENV:") || name == "workspace" || name == "projectroot" || is_time_builtin(name)
}

/// Substitute caller-supplied `${KEY}` values into node properties.
///
/// Called AFTER [`apply_time_builtins`] and BEFORE [`apply_workspace_context`] so a
/// supplied value wins over the static context default, while any `${KEY}` left
/// unset falls through to normal context resolution. An unknown `${...}` is left
/// verbatim.
///
/// Returns Err when a parameter would inject shell syntax into an executed
/// property. `POST /api/run` needs only the operator role while `POST /api/deploy`
/// needs admin, so silently substituting there would hand an operator the code
/// execution the authorization table reserves for an administrator.
/// #317: validate supplied parameters against the pipeline's declared contract.
///
/// Separate from [`apply_params`] so a surface that wants to RENDER the problems
/// - a form marking three fields, an agent correcting itself - gets them
/// structured rather than as one string it has to parse back apart.
///
/// A pipeline with no declared parameters returns the values unchanged: the
/// #127 behaviour, where an unresolved `${name}` is simply prompted for.
pub fn validate_params(
    doc: &PipelineDoc,
    params: &HashMap<String, String>,
) -> Result<crate::params::Resolved, Vec<crate::params::ParamError>> {
    if doc.parameters.is_empty() {
        return Ok(Default::default());
    }
    let supplied: std::collections::BTreeMap<String, String> =
        params.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    crate::params::validate(&doc.parameters, &supplied)
}

/// Substitute `${name}` throughout the document, after validating the supplied
/// values against whatever contract the pipeline declares.
///
/// Returns what was actually used, with every declared secret replaced by `***`
/// (#309). Returned rather than left for each surface to reconstruct: this is
/// the one place that knows both the effective values (defaults included) and
/// which of them are secret, and a caller that rebuilt the map from its own
/// inputs would record the wrong thing on both counts.
pub fn apply_params(
    doc: &mut PipelineDoc,
    params: &HashMap<String, String>,
) -> Result<std::collections::BTreeMap<String, String>, String> {
    // One unnamed source. A caller that knows where its values came from should
    // say so through `apply_params_from`, which is what makes an override
    // visible.
    let supplied: Vec<crate::params::Supplied> = params
        .iter()
        .map(|(name, value)| crate::params::Supplied {
            name: name.clone(),
            value: value.clone(),
            source: "run input".to_string(),
        })
        .collect();
    Ok(apply_params_from(doc, &supplied)?.0)
}

/// #317: substitute, knowing where each value came from.
///
/// Returns the redacted map for history AND what each parameter displaced.
/// Louis's case: a schedule binds `jurisdiction = BE` and the run that starts
/// supplies `NL`. Last-write-wins is a fine RULE - what is not fine is that the
/// result cannot afterwards be told apart from someone binding the same name
/// twice by accident. Sources are given lowest-authority first.
pub fn apply_params_from(
    doc: &mut PipelineDoc,
    supplied: &[crate::params::Supplied],
) -> Result<(std::collections::BTreeMap<String, String>, Vec<crate::params::Effective>), String> {
    let params: HashMap<String, String> =
        crate::params::merge(supplied).0.into_iter().collect();
    let params = &params;
    // #317: the one normalization boundary. Every surface reaches substitution
    // through here, so validating here is what makes the desktop, the console,
    // the CLI, MCP and the scheduler agree - validating per surface is how one
    // of them ends up accepting a value another refuses.
    // Provenance survives validation, so what is reported is what actually ran.
    let provenance = crate::params::merge(supplied).1;
    let resolved = validate_params(doc, params).map_err(|errs| {
        errs.iter()
            .map(|e| e.message.clone())
            .collect::<Vec<_>>()
            .join("; ")
    })?;
    // What gets recorded (#309). A pipeline that DECLARES its parameters says
    // which are secret, and those become `***`. A pipeline that declares
    // nothing has said nothing about any of them, and one of them may well be
    // a password - so the value is replaced by a digest of itself. That keeps
    // "this parameter changed between the two runs" answerable without a
    // credential ever reaching a file, which is the same trade #308 makes for
    // context values.
    let recorded: std::collections::BTreeMap<String, String> = if doc.parameters.is_empty() {
        params.iter().map(|(k, v)| (k.clone(), digest(v))).collect()
    } else {
        resolved.for_history()
    };
    // Built from `recorded`, so a secret is `***` here for exactly the same
    // reason it is there - a provenance record must not become the one place a
    // credential is written down.
    let effective: Vec<crate::params::Effective> = recorded
        .iter()
        .map(|(name, value)| {
            let (source, overrode) = provenance
                .get(name)
                .cloned()
                .unwrap_or_else(|| ("default".to_string(), Vec::new()));
            crate::params::Effective {
                name: name.clone(),
                value: value.clone(),
                source,
                overrode,
            }
        })
        .collect();
    // Declared parameters carry defaults, so the effective set can be larger
    // than what the caller supplied.
    let params: HashMap<String, String> = if doc.parameters.is_empty() {
        params.clone()
    } else {
        resolved.values().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };
    let params = &params;
    if params.is_empty() {
        return Ok((recorded, effective));
    }
    let re = match regex::Regex::new(r"\$\{([^}]+)\}") {
        Ok(re) => re,
        Err(_) => return Ok((recorded, effective)),
    };
    for node in &mut doc.nodes {
        if let Some(props) = node.data.properties.as_mut() {
            substitute_params_deep(props, None, params, &re, &node.id)?;
        }
    }
    Ok((recorded, effective))
}

/// A short, stable stand-in for a value nobody declared the sensitivity of.
///
/// Marked with a leading `#` so a reader can tell a digest from a value at a
/// glance rather than wondering why a parameter is eight hex characters. Not a
/// cryptographic claim: it answers same-or-different and nothing else.
fn digest(value: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut h);
    format!("#{:08x}", h.finish() as u32)
}

/// Key-aware walk. `substitute_deep` discards the property name, which is exactly
/// the information needed to tell a SQL predicate from a shell command, so this
/// carries it down. An array inherits its parent's key, because `args: ["${x}"]`
/// is still the args property.
fn substitute_params_deep(
    value: &mut JsonValue,
    key: Option<&str>,
    params: &HashMap<String, String>,
    re: &regex::Regex,
    node_id: &str,
) -> Result<(), String> {
    match value {
        JsonValue::String(s) => {
            let executed = key.is_some_and(|k| EXECUTED_PROPS.contains(&k));
            let mut failure: Option<String> = None;
            let out = re
                .replace_all(s, |caps: &regex::Captures| {
                    let name = caps[1].trim();
                    if is_reserved_param(name) {
                        return caps[0].to_string();
                    }
                    match params.get(name) {
                        Some(v) => {
                            if executed && v.contains(SHELL_METACHARACTERS) {
                                failure.get_or_insert_with(|| {
                                    format!(
                                        "node {node_id}: parameter '{name}' contains shell syntax and the '{}' property is executed, so it is refused. Pass a plain value, or move the command into the pipeline.",
                                        key.unwrap_or("?")
                                    )
                                });
                                return caps[0].to_string();
                            }
                            v.clone()
                        }
                        None => caps[0].to_string(),
                    }
                })
                .into_owned();
            if let Some(msg) = failure {
                return Err(msg);
            }
            *s = out;
        }
        JsonValue::Array(a) => {
            for v in a {
                substitute_params_deep(v, key, params, re, node_id)?;
            }
        }
        JsonValue::Object(m) => {
            for (k, v) in m.iter_mut() {
                substitute_params_deep(v, Some(k.as_str()), params, re, node_id)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// List the overridable `${...}` parameter names referenced anywhere in the
/// pipeline's node properties, so a UI can offer a value for each. Excludes the
/// date/time builtins and the `${workspace}` / `${projectroot}` path builtins
/// (resolved automatically) and `${ENV:...}` secrets (supplied via secrets.env).
/// Sorted and de-duplicated.
pub fn discover_parameters(doc: &PipelineDoc) -> Vec<String> {
    let re = match regex::Regex::new(r"\$\{([^}]+)\}") {
        Ok(re) => re,
        Err(_) => return Vec::new(),
    };
    let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for node in &doc.nodes {
        if let Some(props) = node.data.properties.as_ref() {
            collect_param_names(props, &re, &mut set);
        }
    }
    set.into_iter().collect()
}

fn collect_param_names(
    value: &JsonValue,
    re: &regex::Regex,
    out: &mut std::collections::BTreeSet<String>,
) {
    // Path builtins resolved automatically; the date/time family (including
    // offset forms like date+1d, #191) is excluded via is_time_builtin.
    const PATH_BUILTINS: [&str; 2] = ["workspace", "projectroot"];
    match value {
        JsonValue::String(s) => {
            for caps in re.captures_iter(s) {
                let name = caps[1].trim();
                if name.starts_with("ENV:")
                    || PATH_BUILTINS.contains(&name)
                    || is_time_builtin(name)
                {
                    continue;
                }
                out.insert(name.to_string());
            }
        }
        JsonValue::Array(a) => a.iter().for_each(|v| collect_param_names(v, re, out)),
        JsonValue::Object(m) => m.values().for_each(|v| collect_param_names(v, re, out)),
        _ => {}
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
    bake_workspace: bool,
) -> Result<(HashMap<String, String>, Vec<String>), String> {
    let mut vars: HashMap<String, String> = HashMap::new();
    let mut secret_values: Vec<String> = Vec::new();
    let mut matched_requested = false;

    // Built-in placeholders for the workspace root so paths can be written
    // relative to it and the workspace folder stays portable (#37). Inserted
    // first so an explicit context variable of the same name still wins.
    // Separators normalized to `/` for parity with the frontend builtinVars
    // (DuckDB accepts them on every platform).
    //
    // #145: when bake_workspace is false (the portable-artifact build path) we
    // deliberately leave `${workspace}` / `${projectroot}` UNRESOLVED so they
    // survive as placeholders in the embedded pipeline and get re-resolved on
    // the run host. Baking them here would tie the artifact to the build host's
    // filesystem. Normal execution (scheduler, by-id load) keeps baking.
    if bake_workspace {
        let ws_root = workspace.to_string_lossy().replace('\\', "/");
        vars.insert("workspace".to_string(), ws_root.clone());
        vars.insert("projectroot".to_string(), ws_root);
    }

    // Read every context first, then merge lowest layer upward, so a context
    // that declares a higher priority overrides the base regardless of the
    // order the repo lists them in (#204). Loading before merging is what
    // makes that possible: the layer is inside the payload.
    let mut loaded: Vec<(&RepoItem, ContextPayload)> = Vec::new();
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
        loaded.push((item, payload));
    }
    // Stable, so contexts sharing a layer keep repo order and a workspace that
    // sets no priorities merges exactly as it did before layers existed.
    loaded.sort_by_key(|(_, p)| p.priority);

    for (item, payload) in &loaded {
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

/// Resolve a workspace pipeline for execution. See module docs. `${workspace}`
/// and `${projectroot}` are baked to this host's paths, so this is for running
/// on the same machine (the by-id load, foreach children, the scheduler).
pub fn resolve_workspace(
    workspace: &Path,
    pipeline_id: &str,
    context: Option<&str>,
) -> Result<Resolved, String> {
    resolve_workspace_impl(workspace, pipeline_id, context, true)
}

/// Like [`resolve_workspace`] but leaves `${workspace}` / `${projectroot}` as
/// placeholders (#145). Used when building a portable pipeline artifact: the
/// placeholders survive into the embedded pipeline and are re-resolved on the
/// run host, so one artifact runs on any machine or OS. Context vars, SQL
/// routine inlining, secret capture, and child-pipeline rewrites still apply.
pub fn resolve_workspace_portable(
    workspace: &Path,
    pipeline_id: &str,
    context: Option<&str>,
) -> Result<Resolved, String> {
    resolve_workspace_impl(workspace, pipeline_id, context, false)
}

fn resolve_workspace_impl(
    workspace: &Path,
    pipeline_id: &str,
    context: Option<&str>,
    bake_workspace: bool,
) -> Result<Resolved, String> {
    let repo = read_repo(workspace)?;
    let (vars, secret_values) = build_context_vars(workspace, &repo, context, bake_workspace)?;
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
    use super::*;

    /// DUCKLE_VAULT_COMMAND is process-wide, so these tests must not overlap.
    static VAULT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn vault_placeholders_resolve_and_refuse_what_they_should() {
        let _g = VAULT_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let doc = |v: &str| -> PipelineDoc {
            serde_json::from_str(&format!(
                r#"{{"nodes":[{{"id":"n","type":"source","position":{{"x":0,"y":0}},
                   "data":{{"label":"n","componentId":"src.inline",
                            "properties":{{"columns":[{{"key":"c","value":"{}"}}]}}}}}}],"edges":[]}}"#,
                v
            ))
            .unwrap()
        };
        let value_of = |d: &PipelineDoc| -> String {
            d.nodes[0].data.properties.as_ref().unwrap()["columns"][0]["value"]
                .as_str()
                .unwrap()
                .to_string()
        };

        // With no command configured the placeholder is left alone: an operator
        // who has not opted in must not have pipelines silently change meaning.
        std::env::remove_var("DUCKLE_VAULT_COMMAND");
        let mut d = doc("${VAULT:ANY}");
        apply_vault(&mut d);
        assert_eq!(value_of(&d), "${VAULT:ANY}");

        // Echo the name back: enough to prove the substitution happened and
        // that {name} landed in the right argument.
        #[cfg(windows)]
        std::env::set_var("DUCKLE_VAULT_COMMAND", "cmd /c echo {name}");
        #[cfg(not(windows))]
        std::env::set_var("DUCKLE_VAULT_COMMAND", "echo {name}");
        let mut d = doc("${VAULT:ORDERS}");
        apply_vault(&mut d);
        assert_eq!(value_of(&d), "ORDERS", "the fetched value replaces the placeholder");

        // A name carrying a control character is refused rather than passed
        // into an argument list.
        let mut d = doc("${VAULT:bad\\u0007name}");
        apply_vault(&mut d);
        assert!(
            value_of(&d).starts_with("${VAULT:"),
            "a control character must not reach the command, got {}",
            value_of(&d)
        );
        std::env::remove_var("DUCKLE_VAULT_COMMAND");
    }
    use super::{resolve_workspace, resolve_workspace_portable};
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
    fn higher_priority_context_layers_over_the_base() {
        // #204: a shared base plus a per-environment override. The base is
        // listed LAST in the repo, so without layers its values would win and
        // the environment override would be silently discarded.
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        write(
            &ws.join("repository.json"),
            r#"[{"id":"env","name":"Prod","type":"context"},
                {"id":"base","name":"Base","type":"context"}]"#,
        );
        write(
            &ws.join("contexts/base.json"),
            r#"{"priority":0,"variables":[
                {"key":"DB_HOST","value":"localhost"},
                {"key":"RETRIES","value":"3"}]}"#,
        );
        write(
            &ws.join("contexts/env.json"),
            r#"{"priority":10,"variables":[{"key":"DB_HOST","value":"prod.internal"}]}"#,
        );
        write(
            &ws.join("pipelines/p1.json"),
            r#"{"nodes":[{"id":"o","position":{"x":0,"y":0},"data":{"label":"S","componentId":"src.oracle","properties":{"host":"${DB_HOST}","tries":"${RETRIES}"}}}],"edges":[]}"#,
        );

        let resolved = resolve_workspace(ws, "p1", None).unwrap();
        let props = resolved.doc.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(
            props["host"],
            serde_json::json!("prod.internal"),
            "the higher layer must win regardless of repo order"
        );
        assert_eq!(
            props["tries"],
            serde_json::json!("3"),
            "keys the higher layer does not define still come from the base"
        );
    }

    #[test]
    fn contexts_without_priorities_keep_repo_order() {
        // Back-compat: every existing workspace has no priorities, so all
        // contexts share layer 0 and the last one listed still wins.
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        write(
            &ws.join("repository.json"),
            r#"[{"id":"a","name":"A","type":"context"},
                {"id":"b","name":"B","type":"context"}]"#,
        );
        write(&ws.join("contexts/a.json"), r#"{"variables":[{"key":"K","value":"first"}]}"#);
        write(&ws.join("contexts/b.json"), r#"{"variables":[{"key":"K","value":"second"}]}"#);
        write(
            &ws.join("pipelines/p1.json"),
            r#"{"nodes":[{"id":"o","position":{"x":0,"y":0},"data":{"label":"S","componentId":"src.oracle","properties":{"host":"${K}"}}}],"edges":[]}"#,
        );

        let resolved = resolve_workspace(ws, "p1", None).unwrap();
        let props = resolved.doc.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(props["host"], serde_json::json!("second"));
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


    use super::super::PipelineDoc;
    use std::collections::HashMap;

    fn doc_with(props: &str) -> PipelineDoc {
        serde_json::from_str(&format!(
            r#"{{"nodes":[{{"id":"n1","position":{{"x":0,"y":0}},"data":{{"label":"X","componentId":"code.shell","properties":{props}}}}}],"edges":[]}}"#
        ))
        .unwrap()
    }

    /// POST /api/run needs only the operator role; POST /api/deploy needs admin.
    /// A parameter substituted into an EXECUTED property is program text, not data,
    /// so allowing shell syntax there hands an operator the execution the
    /// authorization table reserves for an administrator.
    #[test]
    fn a_parameter_cannot_inject_shell_syntax_into_an_executed_property() {
        let mut doc = doc_with(r#"{"code":"echo ${greeting}"}"#);
        let params = HashMap::from([("greeting".to_string(), "hi; rm -rf /".to_string())]);
        let err = super::apply_params(&mut doc, &params)
            .expect_err("shell syntax in an executed property must be refused");
        assert!(err.contains("greeting") && err.contains("code"), "unhelpful error: {err}");
    }

    /// The restriction must not break the feature. A parameter in SQL is the ordinary
    /// documented use, and a plain value in a command is fine.
    #[test]
    fn ordinary_parameter_use_still_works() {
        let mut doc = doc_with(r#"{"sql":"select * from t where m = '${month}'","code":"echo ${month}"}"#);
        let params = HashMap::from([("month".to_string(), "2026-08".to_string())]);
        super::apply_params(&mut doc, &params).expect("a plain value must substitute");
        let props = doc.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(props["sql"], serde_json::json!("select * from t where m = '2026-08'"));
        assert_eq!(props["code"], serde_json::json!("echo 2026-08"));
    }

    /// `discover_parameters` never offers the builtins, so a request naming one is not
    /// filling a parameter in - it is redefining where the pipeline reads and writes.
    /// #317: the declared contract is enforced at the boundary every surface
    /// goes through, not at each surface.
    #[test]
    fn a_declared_contract_is_enforced_before_substitution() {
        let doc_json = serde_json::json!({
            "nodes": [{
                "id": "n", "position": { "x": 0, "y": 0 },
                "data": { "label": "n", "componentId": "src.inline",
                          "properties": { "columns": [{ "key": "c", "value": "${jurisdiction}" }] } }
            }],
            "edges": [],
            "parameters": {
                "jurisdiction": { "type": "string", "enum": ["BE", "NL"], "required": true },
                "full_refresh": { "type": "boolean", "default": "false" }
            }
        });

        // A value outside the enum is refused, and nothing is substituted.
        let mut doc: PipelineDoc = serde_json::from_value(doc_json.clone()).unwrap();
        let mut bad = HashMap::new();
        bad.insert("jurisdiction".to_string(), "FR".to_string());
        let err = super::apply_params(&mut doc, &bad).unwrap_err();
        assert!(err.contains("jurisdiction"), "must name the parameter: {err}");
        assert_eq!(
            doc.nodes[0].data.properties.as_ref().unwrap()["columns"][0]["value"],
            "${jurisdiction}",
            "a refused set must not half-substitute"
        );

        // A valid one substitutes, and the declared default is applied even
        // though the caller never supplied it.
        let mut doc: PipelineDoc = serde_json::from_value(doc_json).unwrap();
        let mut good = HashMap::new();
        good.insert("jurisdiction".to_string(), "BE".to_string());
        super::apply_params(&mut doc, &good).expect("valid");
        assert_eq!(
            doc.nodes[0].data.properties.as_ref().unwrap()["columns"][0]["value"],
            "BE"
        );
        let resolved = super::validate_params(&doc, &good).unwrap();
        assert_eq!(
            resolved.values().get("full_refresh").map(String::as_str),
            Some("false"),
            "a default is part of the resolved set, not something each surface adds"
        );
    }

    /// A pipeline that declares nothing keeps the #127 behaviour exactly.
    #[test]
    fn a_pipeline_with_no_contract_is_unchanged() {
        let mut doc: PipelineDoc = serde_json::from_value(serde_json::json!({
            "nodes": [{
                "id": "n", "position": { "x": 0, "y": 0 },
                "data": { "label": "n", "componentId": "src.inline",
                          "properties": { "columns": [{ "key": "c", "value": "${anything}" }] } }
            }],
            "edges": []
        }))
        .unwrap();
        let mut p = HashMap::new();
        p.insert("anything".to_string(), "value".to_string());
        super::apply_params(&mut doc, &p).expect("no contract, no refusal");
        assert_eq!(doc.nodes[0].data.properties.as_ref().unwrap()["columns"][0]["value"], "value");
    }

    #[test]
    fn a_request_cannot_redefine_the_path_builtins_or_env_secrets() {
        let mut doc = doc_with(r#"{"path":"${workspace}/a.csv","alt":"${projectroot}/b","tok":"${ENV:TOKEN}"}"#);
        let params = HashMap::from([
            ("workspace".to_string(), "/tmp/attacker".to_string()),
            ("projectroot".to_string(), "/tmp/attacker".to_string()),
            ("ENV:TOKEN".to_string(), "stolen".to_string()),
        ]);
        super::apply_params(&mut doc, &params).unwrap();
        let props = doc.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(props["path"], serde_json::json!("${workspace}/a.csv"));
        assert_eq!(props["alt"], serde_json::json!("${projectroot}/b"));
        assert_eq!(props["tok"], serde_json::json!("${ENV:TOKEN}"));
    }

    #[test]
    fn portable_build_keeps_workspace_placeholder() {
        // #145: the portable-artifact build must NOT bake ${workspace} /
        // ${projectroot} to the build host's path - they survive as placeholders
        // for the run host to re-resolve. Other context vars still substitute.
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        write(
            &ws.join("pipelines/p1.json"),
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"CSV","componentId":"src.csv","properties":{"path":"${workspace}/data/in.csv","alt":"${projectroot}/out.parquet"}}}],"edges":[]}"#,
        );
        let resolved = resolve_workspace_portable(ws, "p1", None).unwrap();
        let props = resolved.doc.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(
            props["path"],
            serde_json::json!("${workspace}/data/in.csv"),
            "portable build must leave ${{workspace}} unresolved"
        );
        assert_eq!(
            props["alt"],
            serde_json::json!("${projectroot}/out.parquet"),
            "portable build must leave ${{projectroot}} unresolved"
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
    fn time_builtin_offsets_shift_the_instant() {
        // #191: a signed d/h/m/s offset shifts the builtin's instant.
        let now = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        assert_eq!(
            super::resolve_time_builtin("date", now).unwrap(),
            now.format("%Y-%m-%d").to_string()
        );
        assert_eq!(
            super::resolve_time_builtin("date+1d", now).unwrap(),
            (now + chrono::Duration::days(1)).format("%Y-%m-%d").to_string()
        );
        assert_eq!(
            super::resolve_time_builtin("timestamp-2h", now).unwrap(),
            (now.timestamp() - 7200).to_string()
        );
        // datetime wins over date (longest base), minute offset, filename-safe.
        assert_eq!(
            super::resolve_time_builtin("datetime-45m", now).unwrap(),
            (now - chrono::Duration::minutes(45)).format("%Y-%m-%d_%H%M%S").to_string()
        );
        // combined segments.
        assert_eq!(
            super::resolve_time_builtin("now+1d6h30m", now).unwrap(),
            (now + chrono::Duration::days(1) + chrono::Duration::hours(6) + chrono::Duration::minutes(30))
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string()
        );
    }

    #[test]
    fn time_builtin_offsets_reject_garbage() {
        let now = chrono::Utc::now();
        for bad in ["date+", "date+1", "date+1y", "date+x", "nope", "datexyz", "now-"] {
            assert!(
                super::resolve_time_builtin(bad, now).is_none(),
                "{bad} should not resolve"
            );
        }
    }

    #[test]
    fn discover_parameters_excludes_offset_builtins() {
        // #191: date+1d / now-2h are builtins, not user parameters.
        let doc: crate::PipelineDoc = serde_json::from_str(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"P","componentId":"snk.parquet","properties":{"path":"out/${date+1d}/${REGION}.parquet","x":"${now-2h}"}}}],"edges":[]}"#,
        )
        .unwrap();
        assert_eq!(super::discover_parameters(&doc), vec!["REGION".to_string()]);
    }

    #[test]
    fn a_name_a_node_sets_is_not_taken_from_the_static_context() {
        // A name a node works out while the run is under way belongs to that node. The
        // static context routinely declares the same name with no value - that is how a
        // Talend job declares one it intends to fill in at run time - and substituting
        // it first replaces the placeholder with nothing at all, so the step that was
        // meant to read the run value silently compares against an empty string.
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        std::fs::write(ws.join("repository.json"), r#"[{"type":"context","id":"c","name":"Default"}]"#).unwrap();
        std::fs::create_dir_all(ws.join("contexts")).unwrap();
        std::fs::write(
            ws.join("contexts").join("c.json"),
            r#"{"variables":[{"key":"batch_date","value":""},{"key":"REGION","value":"EU"}]}"#,
        )
        .unwrap();
        std::fs::write(
            ws.join("context.env"),
            "batch_date=\nREGION=EU\n",
        )
        .unwrap();

        let mut doc: crate::PipelineDoc = serde_json::from_str(
            r#"{"nodes":[
                 {"id":"v","position":{"x":0,"y":0},"data":{"label":"Set","componentId":"ctl.setvar",
                   "properties":{"name":"batch_date","value":"max(D)"}}},
                 {"id":"q","position":{"x":0,"y":0},"data":{"label":"SQL","componentId":"code.sql",
                   "properties":{"sql":"SELECT * FROM input WHERE d = '${batch_date}' AND r = '${REGION}'"}}}
               ],"edges":[]}"#,
        )
        .unwrap();
        super::apply_workspace_context(&mut doc, ws);
        let sql = doc.nodes[1].data.properties.as_ref().unwrap()["sql"].as_str().unwrap();
        assert!(
            sql.contains("'${batch_date}'"),
            "the name its own node sets is left for the run to fill: {sql}"
        );
        // Everything else still resolves exactly as before.
        assert!(sql.contains("r = 'EU'"), "got: {sql}");
    }

    #[test]
    fn apply_workspace_context_substitutes_workspace_placeholder() {
        // serve /api/run and scheduled runs load a pipeline file directly and
        // must resolve ${workspace} the same way the headless CLI runner does.
        let dir = tempfile::tempdir().unwrap();
        let ws = dir.path();
        let mut doc: crate::PipelineDoc = serde_json::from_str(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"CSV","componentId":"src.csv","properties":{"path":"${workspace}/data/orders.csv"}}}],"edges":[]}"#,
        )
        .unwrap();
        super::apply_workspace_context(&mut doc, ws);
        let props = doc.nodes[0].data.properties.as_ref().unwrap();
        let root = ws.to_string_lossy().replace('\\', "/");
        assert_eq!(
            props["path"],
            serde_json::json!(format!("{}/data/orders.csv", root)),
            "apply_workspace_context must resolve ${{workspace}} for file-loaded pipelines"
        );
    }

    #[test]
    fn run_params_override_and_discover() {
        // The web dashboard discovers ${...} parameters and lets an operator
        // override them for a single run; provided values win, blanks fall back.
        let json = r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{"label":"CSV","componentId":"src.csv","properties":{"path":"${workspace}/sales_${MONTH}.csv","where":"region = '${REGION}'","key":"${ENV:SECRET}","stamp":"${date}"}}}],"edges":[]}"#;

        // Discovery lists MONTH + REGION but never the builtins or ENV secrets.
        let doc: crate::PipelineDoc = serde_json::from_str(json).unwrap();
        assert_eq!(
            super::discover_parameters(&doc),
            vec!["MONTH".to_string(), "REGION".to_string()],
        );

        // Applying a param substitutes only the provided key; the rest survive
        // for the later context / env / time passes.
        let mut doc: crate::PipelineDoc = serde_json::from_str(json).unwrap();
        let mut params = std::collections::HashMap::new();
        params.insert("MONTH".to_string(), "03".to_string());
        let _ = super::apply_params(&mut doc, &params);
        let props = doc.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(props["path"], serde_json::json!("${workspace}/sales_03.csv"));
        assert_eq!(props["where"], serde_json::json!("region = '${REGION}'"));
        assert_eq!(props["key"], serde_json::json!("${ENV:SECRET}"));
        assert_eq!(props["stamp"], serde_json::json!("${date}"));
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
