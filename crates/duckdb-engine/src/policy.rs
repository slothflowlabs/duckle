//! Enforceable workspace and server policy (#285).
//!
//! RBAC answers "which control-plane actions may this key invoke". This answers
//! a different question: **which capabilities may a pipeline in this environment
//! contain at all**. An AI agent, a CI job or an operator with legitimate write
//! access to the pipeline repository defeats the first entirely, which is
//! exactly the case this exists for.
//!
//! "Do not modify production data" in a prompt is guidance. This is a boundary.
//!
//! # Three properties that make it one
//!
//! **Enforcement is at PLAN time**, not at validation. An agent that can write
//! the pipeline can also invoke a path that skips a validation step, so the
//! check sits where a pipeline is turned into something executable - there is
//! then nowhere for a denied capability to run rather than merely a check it
//! failed.
//!
//! **Narrowing is the only operation the format has.** A layer contributes
//! DENIES, which union, and ALLOWLISTS, which intersect. There is no expressible
//! way to remove a deny or extend an allowlist, so "a workspace may never widen
//! a server policy" is structural rather than a merge rule somebody has to keep
//! getting right. An empty allowlist that was never set means unrestricted; once
//! any layer sets one, every later layer can only cut it down.
//!
//! **`mode` comes from the server policy alone.** A workspace file is writable
//! by whatever writes the pipelines, so a workspace that could set `mode:
//! report` could switch the boundary off from inside the thing being bounded.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

use crate::{EngineError, PipelineDoc};

/// Where a policy came from, so a refusal can name the file that refused.
#[derive(Debug, Clone, Default)]
pub struct Policy {
    /// "enforce" refuses; "report" logs and allows. Server policy only.
    pub mode: String,
    /// Files that contributed, most authoritative first.
    pub sources: Vec<String>,
    /// Component ids or families ("code.*") that may not appear at all.
    pub denied_components: BTreeSet<String>,
    /// When set, the only hosts a pipeline may reach.
    pub allowed_domains: Option<BTreeSet<String>>,
    /// When set, the only saved connections a sink may use.
    pub allowed_connections: Option<BTreeSet<String>>,
    /// When set, the only object-storage prefixes a sink may write under.
    pub allowed_s3_prefixes: Option<BTreeSet<String>>,
    /// Schemas no sink may write to, whatever else allows it.
    pub denied_schemas: BTreeSet<String>,
    /// When set, the only local directories a sink may write under.
    pub allowed_paths: Option<BTreeSet<String>>,
    /// May a pipeline change saved state - a watermark, a resume position?
    pub allow_state_mutation: bool,
    /// May unsigned DuckDB extensions load?
    pub allow_unsigned_extensions: bool,
    /// May DuckDB itself perform external I/O (httpfs, S3, remote ATTACH)?
    ///
    /// Only consulted where a network allowlist exists. DuckDB has no
    /// per-domain allowlist of its own, so under a restricted network the
    /// choice is all or nothing, and the safe default is nothing. An operator
    /// who genuinely needs DuckDB-native remote reads sets this back to true
    /// and accepts that those reads are outside the domain boundary.
    pub allow_duckdb_external_io: bool,
    /// May this workspace ship lineage events to an external collector (#311)?
    ///
    /// Separate from writing them locally, which is not an egress. The thing a
    /// server policy needs to be able to forbid is the POST: a workspace file
    /// naming an endpoint is otherwise enough to send the shape of the estate
    /// somewhere the operator did not choose.
    pub allow_lineage_export: bool,
}

impl Policy {
    /// A policy that forbids nothing, which is what an environment with no
    /// policy file means.
    fn unrestricted() -> Self {
        Policy {
            mode: "enforce".into(),
            sources: Vec::new(),
            denied_components: BTreeSet::new(),
            allowed_domains: None,
            allowed_connections: None,
            allowed_s3_prefixes: None,
            denied_schemas: BTreeSet::new(),
            allowed_paths: None,
            allow_state_mutation: true,
            allow_unsigned_extensions: true,
            allow_duckdb_external_io: false,
            allow_lineage_export: true,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Apply another layer, which may only NARROW this one.
    ///
    /// Denies union, allowlists intersect, permissions AND. There is no branch
    /// here that makes anything more permissive, which is the whole point: the
    /// guarantee is a property of the operation rather than of remembering to
    /// check a precedence order.
    fn narrow_with(&mut self, other: Layer, source: &str) {
        self.sources.push(source.to_string());
        self.denied_components.extend(other.denied_components);
        self.denied_schemas.extend(other.denied_schemas);
        intersect(&mut self.allowed_domains, other.allowed_domains);
        intersect(&mut self.allowed_connections, other.allowed_connections);
        intersect(&mut self.allowed_s3_prefixes, other.allowed_s3_prefixes);
        intersect(&mut self.allowed_paths, other.allowed_paths);
        if let Some(false) = other.allow_state_mutation {
            self.allow_state_mutation = false;
        }
        if let Some(false) = other.allow_unsigned_extensions {
            self.allow_unsigned_extensions = false;
        }
        if let Some(false) = other.allow_duckdb_external_io {
            self.allow_duckdb_external_io = false;
        }
        if let Some(false) = other.allow_lineage_export {
            self.allow_lineage_export = false;
        }
    }
}

/// Restrict an allowlist. None means "this layer said nothing", which cannot
/// widen; Some means intersect, which cannot widen either.
fn intersect(current: &mut Option<BTreeSet<String>>, add: Option<BTreeSet<String>>) {
    let Some(add) = add else {
        return;
    };
    *current = Some(match current.take() {
        Some(have) => have.intersection(&add).cloned().collect(),
        None => add,
    });
}

/// One policy file, before it is narrowed into the effective policy.
#[derive(Debug, Default)]
struct Layer {
    mode: Option<String>,
    denied_components: BTreeSet<String>,
    denied_schemas: BTreeSet<String>,
    allowed_domains: Option<BTreeSet<String>>,
    allowed_connections: Option<BTreeSet<String>>,
    allowed_s3_prefixes: Option<BTreeSet<String>>,
    allowed_paths: Option<BTreeSet<String>>,
    allow_state_mutation: Option<bool>,
    allow_unsigned_extensions: Option<bool>,
    allow_duckdb_external_io: Option<bool>,
    allow_lineage_export: Option<bool>,
}

fn set_of(v: Option<&JsonValue>) -> Option<BTreeSet<String>> {
    let arr = v?.as_array()?;
    Some(
        arr.iter()
            .filter_map(|x| x.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
    )
}

fn parse_layer(text: &str, what: &str) -> Result<Layer, EngineError> {
    // YAML is a superset of JSON, so one parser reads either and a policy can
    // be written in whichever the environment already manages.
    let v: JsonValue = serde_yaml::from_str(text)
        .map_err(|e| EngineError::Config(format!("policy: {what} is not valid YAML or JSON: {e}")))?;
    let get = |a: &str, b: &str| -> Option<JsonValue> {
        v.get(a).and_then(|s| s.get(b)).cloned()
    };
    Ok(Layer {
        mode: v.get("mode").and_then(|m| m.as_str()).map(str::to_string),
        denied_components: set_of(get("components", "deny").as_ref()).unwrap_or_default(),
        denied_schemas: set_of(get("sinks", "deniedSchemas").as_ref()).unwrap_or_default(),
        allowed_domains: set_of(get("network", "allowedDomains").as_ref()),
        allowed_connections: set_of(get("sinks", "allowedConnections").as_ref()),
        allowed_s3_prefixes: set_of(get("sinks", "allowedS3Prefixes").as_ref()),
        allowed_paths: set_of(get("filesystem", "allowedPaths").as_ref()),
        allow_state_mutation: get("state", "allowMutation").and_then(|b| b.as_bool()),
        allow_unsigned_extensions: get("extensions", "allowUnsigned").and_then(|b| b.as_bool()),
        allow_duckdb_external_io: get("network", "allowDuckdbExternalIo").and_then(|b| b.as_bool()),
        allow_lineage_export: get("network", "allowLineageExport").and_then(|b| b.as_bool()),
    })
}

/// The effective policy for this environment.
///
/// The server file is authoritative and is read from outside the workspace, so
/// whatever writes pipelines cannot reach it. The workspace file may then only
/// narrow what the server allowed.
pub fn load(workspace: Option<&Path>) -> Result<Policy, EngineError> {
    let mut policy = Policy::unrestricted();

    let server = std::env::var("DUCKLE_POLICY_FILE")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from);
    if let Some(path) = server {
        let text = std::fs::read_to_string(&path).map_err(|e| {
            // A named policy that cannot be read is a refusal, not a warning.
            // Falling back to "no policy" would mean a typo in the environment
            // silently removes the boundary.
            EngineError::Config(format!(
                "policy: DUCKLE_POLICY_FILE names {} and it could not be read ({e}). Refusing to \
                 run without the policy it points at.",
                path.display()
            ))
        })?;
        let layer = parse_layer(&text, &path.display().to_string())?;
        // Mode comes from here and nowhere else: a workspace file is writable by
        // whatever writes the pipelines, so a workspace that could set
        // "report" could switch the boundary off from inside it.
        if let Some(m) = &layer.mode {
            policy.mode = m.clone();
        }
        // Letting DuckDB back onto the network WIDENS the boundary, so like
        // `mode` it is server-only. `narrow_with` below can still turn it off.
        if let Some(v) = layer.allow_duckdb_external_io {
            policy.allow_duckdb_external_io = v;
        }
        policy.narrow_with(layer, &path.display().to_string());
    }

    if let Some(ws) = workspace {
        let path = ws.join(".duckle").join("policy.yaml");
        if path.exists() {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| EngineError::Config(format!("policy: {}: {e}", path.display())))?;
            let layer = parse_layer(&text, &path.display().to_string())?;
            // `mode` deliberately ignored here.
            policy.narrow_with(layer, &path.display().to_string());
        }
    }
    Ok(policy)
}

/// May the control plane change saved state here?
///
/// `state.allowMutation` has to mean the same thing on every surface, or it
/// means nothing: a pipeline that is refused permission to advance a watermark
/// while `duckle-runner backfill --clear`, the HTTP API, MCP and the web panel
/// can all clear the same file is not a boundary, it is a speed bump. So the
/// three mutators go through here, and every surface reaches them.
///
/// Reading state is not gated - an operator has to be able to SEE what they
/// are not allowed to change.
pub fn state_mutation_allowed(workspace: &Path) -> Result<(), EngineError> {
    let policy = load(Some(workspace))?;
    if policy.allow_state_mutation {
        return Ok(());
    }
    let from = policy.sources.join(", ");
    if policy.mode == "report" {
        eprintln!(
            "duckle: policy findings (mode: report, from {from}): saved state was changed, {}",
            "and state mutation is not permitted here"
        );
        return Ok(());
    }
    Err(EngineError::Config(format!(
        concat!(
            "policy: changing saved state is not permitted in this environment ",
            "(policy from {}). Clearing a watermark is a silent full reload, ",
            "which is why it is its own permission."
        ),
        from
    )))
}

/// The outbound network allowlist, as the HTTP layer needs it.
///
/// Cached, keyed on the two environment values that decide it, so a process
/// does not re-read the policy file on every connection and a test that swaps
/// the policy is not answered from a stale cache.
fn network_allowlist() -> Option<BTreeSet<String>> {
    static CACHE: std::sync::Mutex<Option<(String, Option<BTreeSet<String>>)>> =
        std::sync::Mutex::new(None);
    let key = format!(
        "{}|{}",
        std::env::var("DUCKLE_POLICY_FILE").unwrap_or_default(),
        std::env::var("DUCKLE_WORKSPACE").unwrap_or_default()
    );
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((have, list)) = cache.as_ref() {
        if *have == key {
            return list.clone();
        }
    }
    let ws = std::env::var("DUCKLE_WORKSPACE").ok().filter(|w| !w.is_empty()).map(PathBuf::from);
    // A policy that cannot be read is handled at compile time, where the error
    // can be reported. Here, an unreadable policy leaves no allowlist rather
    // than an empty one, because refusing every host from a DNS callback is
    // indistinguishable from the network being down.
    let list = load(ws.as_deref()).ok().and_then(|p| p.allowed_domains);
    *cache = Some((key, list.clone()));
    list
}

/// The settings that take DuckDB itself off the network, for a run whose policy
/// restricts it.
///
/// Duckle's own HTTP client enforces the domain allowlist, but SQL that DuckDB
/// executes never passes through that client: `read_parquet('https://...')`,
/// a remote `ATTACH`, `COPY ... TO` a remote URI, an extension that fetches on
/// its own. Scanning the SQL for those is not a boundary, because the SQL is
/// generated by dbt, Python, templates and MCP, and a path can be built at run
/// time from a row.
///
/// So the boundary is set inside DuckDB instead, which is what makes it hold
/// against SQL that Duckle did not write:
///
/// - `disabled_filesystems` names the remote filesystems. DuckDB refuses to
///   re-enable a filesystem in a running database, so this cannot be undone by
///   a later statement. Local file access is untouched: only remote
///   filesystems are named, and an unrecognised name is accepted, so naming
///   the ones an extension would register keeps them shut if it is ever loaded.
/// - `allow_community_extensions` is likewise one-way, and closes the path
///   where an extension carries its own network code rather than a filesystem.
/// - `autoinstall_known_extensions` stops a missing extension from being
///   fetched on demand. Autoload is deliberately left alone: it only loads
///   extensions that are already on disk, and turning it off breaks ordinary
///   local queries.
///
/// Known residual: an explicit `INSTALL` of a core extension still succeeds,
/// because extension download does not go through the disabled filesystems.
/// Naming that extension's filesystem above is what keeps it from reaching
/// anything.
pub const DUCKDB_OFF_NETWORK: &str = concat!(
    "SET disabled_filesystems=",
    "'HTTPFileSystem,S3FileSystem,AzureBlobStorageFileSystem,GCSFileSystem';\n",
    "SET allow_community_extensions=false;\n",
    "SET autoinstall_known_extensions=false;\n",
);

/// Must DuckDB be taken off the network for this run?
///
/// True only where a network allowlist exists and the mode is `enforce`: a
/// policy that reports rather than refuses should not change what a run can
/// reach, and an environment with no network policy is not restricted at all.
pub fn duckdb_external_io_denied() -> bool {
    static CACHE: std::sync::Mutex<Option<(String, bool)>> = std::sync::Mutex::new(None);
    let key = format!(
        "{}|{}",
        std::env::var("DUCKLE_POLICY_FILE").unwrap_or_default(),
        std::env::var("DUCKLE_WORKSPACE").unwrap_or_default()
    );
    let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((have, v)) = cache.as_ref() {
        if *have == key {
            return *v;
        }
    }
    let ws = std::env::var("DUCKLE_WORKSPACE").ok().filter(|w| !w.is_empty()).map(PathBuf::from);
    // An unreadable policy is refused where it can be reported, exactly as the
    // allowlist does; here it must not silently harden a run that never asked.
    let denied = load(ws.as_deref())
        .map(|p| p.mode == "enforce" && p.allowed_domains.is_some() && !p.allow_duckdb_external_io)
        .unwrap_or(false);
    *cache = Some((key, denied));
    denied
}

/// Is there a network boundary to enforce at run time at all?
pub fn network_is_restricted() -> bool {
    network_allowlist().is_some()
}

/// May we open a connection to this host, RIGHT NOW?
///
/// `check` reads a URL out of a node's properties. That is where an operator
/// can see the refusal, and it is not where the request happens. A URL built at
/// run time from a row, a paginated `next` link, and above all a 302 to
/// somewhere else all reach the socket without passing that check again. So the
/// HTTP agent calls this from its resolver - which fires once per connection
/// and again for every redirect hop - and from a middleware that sees the
/// request URL before anything is sent.
///
/// `netloc` may be a bare host or `host:port`.
pub fn outbound_host_allowed(netloc: &str) -> Result<(), String> {
    let Some(allowed) = network_allowlist() else {
        return Ok(());
    };
    let host = netloc_host(netloc);
    if host_allowed(&allowed, &host) {
        return Ok(());
    }
    Err(format!(
        "policy: {host} is outside every allowed domain, so this request was not sent"
    ))
}

/// The host out of a bare host, a `host:port`, or a URL.
///
/// Separate from `host_of` because that one is fed node PROPERTIES, which are
/// always URLs, and returns None without a scheme. The resolver is handed a
/// bare `host:port`, and a None there would mean "allowed".
///
/// IPv6 literals are unbracketed rather than cut at their first colon.
fn netloc_host(netloc: &str) -> String {
    let s = match netloc.split_once("://") {
        Some((_, rest)) => rest,
        None => netloc,
    };
    let s = s.split(['/', '?', '#']).next().unwrap_or(s);
    let s = s.rsplit('@').next().unwrap_or(s);
    let s = if let Some(rest) = s.strip_prefix('[') {
        rest.split(']').next().unwrap_or(rest)
    } else {
        s.split(':').next().unwrap_or(s)
    };
    s.to_ascii_lowercase()
}

/// One thing a pipeline wanted to do that the environment does not permit.
#[derive(Debug, Clone)]
pub struct Violation {
    pub node: String,
    pub detail: String,
}

/// Does a component id match a deny entry? `code.*` denies the family.
fn denied(entry: &str, component: &str) -> bool {
    match entry.strip_suffix('*') {
        Some(prefix) => component.starts_with(prefix),
        None => entry == component,
    }
}

/// The host part of a URL-ish property, for the domain allowlist.
fn host_of(value: &str) -> Option<String> {
    let after = value.split_once("://")?.1;
    let hostport = after.split(['/', '?']).next()?;
    let host = hostport.rsplit('@').next()?;
    let host = host.split(':').next()?;
    if host.is_empty() {
        None
    } else {
        Some(host.to_ascii_lowercase())
    }
}

/// Is this host inside the allowlist? An entry matches itself and its
/// subdomains, so `example.com` covers `api.example.com` but never
/// `notexample.com`.
fn host_allowed(allowed: &BTreeSet<String>, host: &str) -> bool {
    allowed.iter().any(|a| {
        let a = a.trim_start_matches("*.").to_ascii_lowercase();
        host == a || host.ends_with(&format!(".{a}"))
    })
}

/// Check a whole pipeline against the policy.
///
/// Returns every violation rather than the first, because an agent fixing them
/// one run at a time is the slow version of this being useful.
pub fn check(policy: &Policy, doc: &PipelineDoc) -> Vec<Violation> {
    let mut out = Vec::new();
    if policy.is_empty() {
        return out;
    }
    for node in &doc.nodes {
        if node.data.disabled.unwrap_or(false) {
            continue;
        }
        let Some(component) = node.data.component_id.as_deref() else {
            continue;
        };
        let label = node.data.label.clone();
        let props = node.data.properties.clone().unwrap_or(JsonValue::Null);
        let str_prop = |k: &str| -> Option<String> {
            props.get(k).and_then(|v| v.as_str()).map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
        };

        if let Some(entry) = policy
            .denied_components
            .iter()
            .find(|e| denied(e, component))
        {
            out.push(Violation {
                node: node.id.clone(),
                detail: format!("'{label}' is a {component}, which '{entry}' forbids here"),
            });
        }

        // Every property that can name a host. A pipeline reaching an
        // unapproved domain is how source rows leave, so this covers sources
        // as well as sinks.
        if let Some(allowed) = &policy.allowed_domains {
            for key in ["url", "uri", "endpoint", "host", "baseUrl", "webhookUrl", "path"] {
                let Some(value) = str_prop(key) else { continue };
                let Some(host) = host_of(&value) else { continue };
                if !host_allowed(allowed, &host) {
                    out.push(Violation {
                        node: node.id.clone(),
                        detail: format!("'{label}' reaches {host}, which is not an allowed domain"),
                    });
                }
            }
        }

        // Writes are where the damage is, so the sink checks are the ones that
        // matter most.
        let is_sink = component.starts_with("snk.");
        if is_sink {
            if let Some(allowed) = &policy.allowed_connections {
                if let Some(conn) = str_prop("connectionRef") {
                    if !allowed.contains(&conn) {
                        out.push(Violation {
                            node: node.id.clone(),
                            detail: format!(
                                "'{label}' writes through connection '{conn}', which is not allowed here"
                            ),
                        });
                    }
                }
            }
            if !policy.denied_schemas.is_empty() {
                for key in ["schemaName", "schema", "database"] {
                    if let Some(s) = str_prop(key) {
                        if policy.denied_schemas.contains(&s) {
                            out.push(Violation {
                                node: node.id.clone(),
                                detail: format!("'{label}' writes to schema '{s}', which is denied here"),
                            });
                        }
                    }
                }
            }
            for key in ["path", "destination", "uri"] {
                let Some(target) = str_prop(key) else { continue };
                let lower = target.to_ascii_lowercase();
                if lower.starts_with("s3://") || lower.starts_with("s3a://") {
                    if let Some(allowed) = &policy.allowed_s3_prefixes {
                        if !allowed.iter().any(|p| within_prefix(&target, p)) {
                            out.push(Violation {
                                node: node.id.clone(),
                                detail: format!(
                                    "'{label}' writes to {target}, which is outside every allowed \
                                     object-storage prefix"
                                ),
                            });
                        }
                    }
                } else if let Some(allowed) = &policy.allowed_paths {
                    // A local write. `..` is resolved away first, or a path
                    // that starts inside an allowed directory and climbs out
                    // would pass.
                    let normal = normalize(&target);
                    if !allowed.iter().any(|p| within_prefix(&normal, &normalize(p))) {
                        out.push(Violation {
                            node: node.id.clone(),
                            detail: format!(
                                "'{label}' writes to {target}, which is outside every allowed path"
                            ),
                        });
                    }
                }
            }
        }

        // Clearing a production watermark is a silent full reload, which is why
        // state mutation is its own permission rather than part of the sinks.
        if !policy.allow_state_mutation {
            let mutates = matches!(component, "xf.incremental" | "src.ducklake.changes")
                && props
                    .get("trackState")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
            if mutates {
                out.push(Violation {
                    node: node.id.clone(),
                    detail: format!(
                        "'{label}' advances saved state, and state mutation is not permitted here"
                    ),
                });
            }
        }
    }
    out
}

/// Is `target` inside `prefix`, at a real component boundary?
///
/// A string prefix is not a containment test, and the difference is a bypass:
///
/// ```text
/// allowed: /var/lake/dev
/// target:  /var/lake/development/file
/// ```
///
/// which is lexically inside the prefix and is a different directory. The same
/// applies to an S3 prefix that does not end in `/`:
///
/// ```text
/// allowed: s3://co-development
/// target:  s3://co-development-prod/x
/// ```
///
/// So the target must either BE the prefix, or continue it with a separator.
fn within_prefix(target: &str, prefix: &str) -> bool {
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        // An allowlist entry of "/" or "" allows everything below it, which is
        // what it says. An empty allowLIST is a different thing and is handled
        // by the Option being None.
        return true;
    }
    if target == prefix {
        return true;
    }
    target
        .strip_prefix(prefix)
        .map(|rest| rest.starts_with('/'))
        .unwrap_or(false)
}

/// Collapse `.` and `..` and normalise separators, so a path cannot start
/// inside an allowed directory and climb out of it.
fn normalize(p: &str) -> String {
    let unified = p.replace('\\', "/");
    let mut parts: Vec<&str> = Vec::new();
    for seg in unified.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    let joined = parts.join("/");
    if p.starts_with('/') {
        format!("/{joined}")
    } else {
        joined
    }
}

/// Refuse a pipeline the environment does not permit, or report it.
pub fn enforce(policy: &Policy, doc: &PipelineDoc) -> Result<(), EngineError> {
    let violations = check(policy, doc);
    if violations.is_empty() {
        return Ok(());
    }
    let listed = violations
        .iter()
        .map(|v| format!("  - {}", v.detail))
        .collect::<Vec<_>>()
        .join("\n");
    // The rule AND the file that carries it: "not allowed" sends someone to
    // read the pipeline, which is not where the answer is.
    let from = policy.sources.join(", ");
    if policy.mode == "report" {
        eprintln!("duckle: policy findings (mode: report, from {from}):\n{listed}");
        return Ok(());
    }
    Err(EngineError::Config(format!(
        "policy: this pipeline is not permitted in this environment (policy from {from}):\n{listed}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_of(nodes: JsonValue) -> PipelineDoc {
        serde_json::from_value(serde_json::json!({ "nodes": nodes, "edges": [] })).unwrap()
    }

    fn node(id: &str, component: &str, props: JsonValue) -> JsonValue {
        serde_json::json!({
            "id": id,
            "position": { "x": 0, "y": 0 },
            "data": { "label": id, "componentId": component, "properties": props }
        })
    }

    fn policy_from(yaml: &str) -> Policy {
        let mut p = Policy::unrestricted();
        let layer = parse_layer(yaml, "test").unwrap();
        if let Some(m) = &layer.mode {
            p.mode = m.clone();
        }
        p.narrow_with(layer, "test");
        p
    }

    /// The headline case: an agent writes a pipeline that would write to
    /// production, and the environment makes it impossible rather than
    /// discouraged.
    #[test]
    fn a_write_outside_the_allowed_prefix_is_refused() {
        let p = policy_from(
            "sinks:\n  allowedS3Prefixes:\n    - s3://signumi-development/\n",
        );
        let d = doc_of(serde_json::json!([
            node("w", "snk.parquet", serde_json::json!({ "path": "s3://signumi-production/out.parquet" }))
        ]));
        let err = enforce(&p, &d).unwrap_err().to_string();
        assert!(err.contains("signumi-production"), "names what was refused: {err}");
        assert!(err.contains("test"), "names the policy that refused it: {err}");

        // The allowed prefix still goes through.
        let ok = doc_of(serde_json::json!([
            node("w", "snk.parquet", serde_json::json!({ "path": "s3://signumi-development/out.parquet" }))
        ]));
        assert!(enforce(&p, &ok).is_ok());
    }

    /// A workspace policy may add restrictions and must never remove them.
    /// This is the property the whole design turns on.
    #[test]
    fn a_workspace_layer_can_only_narrow_a_server_layer() {
        let mut p = policy_from("mode: enforce\ncomponents:\n  deny:\n    - code.shell\n");
        // The workspace tries to re-enable the component and widen the domains.
        let ws = parse_layer(
            "mode: report\ncomponents:\n  deny: []\nnetwork:\n  allowedDomains:\n    - evil.example\n",
            "ws",
        )
        .unwrap();
        p.narrow_with(ws, "ws");

        assert!(p.denied_components.contains("code.shell"), "a deny cannot be removed");
        assert_eq!(p.mode, "enforce", "a workspace cannot switch the boundary off");
        // The workspace introduced the only domain allowlist, which NARROWS
        // from unrestricted - it did not widen anything.
        let d = doc_of(serde_json::json!([
            node("s", "src.rest", serde_json::json!({ "url": "https://api.other.example/x" }))
        ]));
        assert!(enforce(&p, &d).is_err(), "the workspace's own allowlist still binds it");
    }

    /// Two allowlists intersect. A workspace naming a domain the server did not
    /// allow must not gain it.
    #[test]
    fn allowlists_intersect_rather_than_merge() {
        let mut p = policy_from("network:\n  allowedDomains:\n    - a.example\n    - b.example\n");
        let ws = parse_layer("network:\n  allowedDomains:\n    - b.example\n    - evil.example\n", "ws")
            .unwrap();
        p.narrow_with(ws, "ws");
        let allowed = p.allowed_domains.clone().unwrap();
        assert_eq!(allowed.len(), 1);
        assert!(allowed.contains("b.example"));
        assert!(!allowed.contains("evil.example"), "a workspace cannot add a domain");
        assert!(!allowed.contains("a.example"), "and the intersection is the narrower set");
    }

    /// A denied family covers every component in it, so a new shell-shaped
    /// component does not arrive outside the policy.
    #[test]
    fn a_denied_family_covers_its_members() {
        let p = policy_from("components:\n  deny:\n    - code.*\n");
        let d = doc_of(serde_json::json!([
            node("c", "code.python", serde_json::json!({}))
        ]));
        assert!(enforce(&p, &d).is_err());
        let other = doc_of(serde_json::json!([node("c", "src.csv", serde_json::json!({}))]));
        assert!(enforce(&p, &other).is_ok());
    }

    /// A subdomain of an allowed domain is allowed; a name that merely ENDS
    /// with it is not, which is the classic allowlist bypass.
    #[test]
    fn a_domain_allowlist_matches_subdomains_and_not_lookalikes() {
        let p = policy_from("network:\n  allowedDomains:\n    - example.com\n");
        let ok = doc_of(serde_json::json!([
            node("s", "src.rest", serde_json::json!({ "url": "https://api.example.com/v1" }))
        ]));
        assert!(enforce(&p, &ok).is_ok(), "a subdomain is inside the allowance");

        for bad in ["https://notexample.com/x", "https://example.com.evil.net/x"] {
            let d = doc_of(serde_json::json!([
                node("s", "src.rest", serde_json::json!({ "url": bad }))
            ]));
            assert!(enforce(&p, &d).is_err(), "{bad} must not pass");
        }
    }

    /// A path that starts inside an allowed directory and climbs out of it is
    /// the same bypass in another shape.
    #[test]
    fn an_allowed_path_cannot_be_climbed_out_of() {
        let p = policy_from("filesystem:\n  allowedPaths:\n    - /var/lake/dev\n");
        let bad = doc_of(serde_json::json!([
            node("w", "snk.csv", serde_json::json!({ "path": "/var/lake/dev/../prod/out.csv" }))
        ]));
        assert!(enforce(&p, &bad).is_err(), "climbing out must be refused");
        let ok = doc_of(serde_json::json!([
            node("w", "snk.csv", serde_json::json!({ "path": "/var/lake/dev/sub/out.csv" }))
        ]));
        assert!(enforce(&p, &ok).is_ok());
    }

    /// Clearing a production watermark is a silent full reload, so state
    /// mutation is its own permission.
    #[test]
    fn state_mutation_can_be_withheld() {
        let p = policy_from("state:\n  allowRead: true\n  allowMutation: false\n");
        let d = doc_of(serde_json::json!([
            node("i", "xf.incremental", serde_json::json!({ "column": "updated_at" }))
        ]));
        let err = enforce(&p, &d).unwrap_err().to_string();
        assert!(err.contains("state mutation"), "{err}");
    }

    /// Report mode says what it found and allows it, for rolling a policy out
    /// before switching it on.
    #[test]
    fn report_mode_allows_but_still_reports() {
        let p = policy_from("mode: report\ncomponents:\n  deny:\n    - code.shell\n");
        let d = doc_of(serde_json::json!([node("c", "code.shell", serde_json::json!({}))]));
        assert!(enforce(&p, &d).is_ok(), "report mode does not refuse");
        assert_eq!(check(&p, &d).len(), 1, "and it still found it");
    }

    /// A disabled node cannot execute, so refusing the run for it would be a
    /// false positive nobody can act on except by deleting the node.
    #[test]
    fn a_disabled_node_is_not_a_violation() {
        let p = policy_from("components:\n  deny:\n    - code.shell\n");
        let mut d = doc_of(serde_json::json!([node("c", "code.shell", serde_json::json!({}))]));
        d.nodes[0].data.disabled = Some(true);
        assert!(enforce(&p, &d).is_ok());
    }

    /// With no policy files at all, nothing is forbidden - the feature has to
    /// be opt-in or every existing workspace breaks.
    #[test]
    fn no_policy_forbids_nothing() {
        let p = Policy::unrestricted();
        let d = doc_of(serde_json::json!([node("c", "code.shell", serde_json::json!({}))]));
        assert!(p.is_empty());
        assert!(enforce(&p, &d).is_ok());
    }

    /// Every violation is reported, not just the first: an agent fixing them
    /// one run at a time is the slow version of this being useful.
    #[test]
    fn every_violation_is_reported_at_once() {
        let p = policy_from(
            "components:\n  deny:\n    - code.shell\nsinks:\n  deniedSchemas:\n    - production\n",
        );
        let d = doc_of(serde_json::json!([
            node("a", "code.shell", serde_json::json!({})),
            node("b", "snk.postgres", serde_json::json!({ "schemaName": "production" })),
        ]));
        assert_eq!(check(&p, &d).len(), 2);
        let err = enforce(&p, &d).unwrap_err().to_string();
        assert!(err.contains("code.shell") && err.contains("production"), "{err}");
    }

    /// A string prefix is not a containment test. `/var/lake/development` is
    /// lexically inside `/var/lake/dev` and is a different directory - the
    /// sibling-prefix bypass.
    #[test]
    fn a_sibling_directory_is_not_inside_the_allowed_path() {
        let p = policy_from("filesystem:\n  allowedPaths:\n    - /var/lake/dev\n");
        let sibling = doc_of(serde_json::json!([node(
            "w",
            "snk.csv",
            serde_json::json!({ "path": "/var/lake/development/out.csv" })
        )]));
        assert!(
            enforce(&p, &sibling).is_err(),
            "/var/lake/development is not inside /var/lake/dev"
        );
        // The real directory still works, and so does the directory itself.
        for ok in ["/var/lake/dev/out.csv", "/var/lake/dev/sub/deep.csv"] {
            let d = doc_of(serde_json::json!([node(
                "w",
                "snk.csv",
                serde_json::json!({ "path": ok })
            )]));
            assert!(enforce(&p, &d).is_ok(), "{ok} is inside and must pass");
        }
    }

    /// The same bypass for an S3 prefix that does not end in a slash.
    #[test]
    fn a_sibling_bucket_is_not_inside_the_allowed_s3_prefix() {
        let p = policy_from("sinks:\n  allowedS3Prefixes:\n    - s3://co-development\n");
        let sibling = doc_of(serde_json::json!([node(
            "w",
            "snk.parquet",
            serde_json::json!({ "path": "s3://co-development-prod/out.parquet" })
        )]));
        assert!(
            enforce(&p, &sibling).is_err(),
            "s3://co-development-prod is a different bucket"
        );
        let inside = doc_of(serde_json::json!([node(
            "w",
            "snk.parquet",
            serde_json::json!({ "path": "s3://co-development/raw/out.parquet" })
        )]));
        assert!(enforce(&p, &inside).is_ok());
    }

    /// A key prefix inside one bucket has the same boundary requirement.
    #[test]
    fn a_sibling_key_prefix_is_not_inside_the_allowed_one() {
        let p = policy_from("sinks:\n  allowedS3Prefixes:\n    - s3://lake/dev\n");
        let sibling = doc_of(serde_json::json!([node(
            "w",
            "snk.parquet",
            serde_json::json!({ "path": "s3://lake/development/out.parquet" })
        )]));
        assert!(enforce(&p, &sibling).is_err(), "dev and development are different prefixes");
        let inside = doc_of(serde_json::json!([node(
            "w",
            "snk.parquet",
            serde_json::json!({ "path": "s3://lake/dev/out.parquet" })
        )]));
        assert!(enforce(&p, &inside).is_ok());
    }

}
