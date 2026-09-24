//! #311: emit OpenLineage events, without becoming the lineage system.
//!
//! Duckle already knows what a run read and wrote, when it started, whether it
//! finished and which run it was. An organisation that has already chosen
//! Marquez or DataHub should be able to see that without replacing Duckle's own
//! catalog, and the standard event format is how.
//!
//! ## Telemetry never fails the run
//!
//! Every function here returns a document; nothing in it can refuse. Emission
//! writes to a local file first and only then tries the network, so a collector
//! that is down costs a POST timeout and leaves the events on disk. A lineage
//! export that can fail a data run is worse than no lineage export.
//!
//! ## Off unless asked
//!
//! No `openlineage.json` in the workspace means no events, no file and no
//! network. #311 asks for additive and disabled by default, and an observability
//! feature that turns itself on is one that surprises somebody's egress rules.
//!
//! ## What it will not claim
//!
//! Column lineage is emitted only where the resolver actually produced it. A
//! `code.python` stage is opaque and the honest answer is to say nothing about
//! it rather than to assert a plausible mapping - a lineage graph nobody can
//! trust is worse than a sparse one, because the gaps are invisible.

use crate::catalog::{Catalog, Direction};
use crate::retry::RunReceipt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::Path;

/// The `producer` every event carries, identifying what emitted it.
pub const PRODUCER: &str = "https://github.com/slothflowlabs/duckle";
/// The spec revision these documents are shaped to.
pub const SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    Start,
    Complete,
    Fail,
    Abort,
}

impl EventType {
    pub fn as_str(self) -> &'static str {
        match self {
            EventType::Start => "START",
            EventType::Complete => "COMPLETE",
            EventType::Fail => "FAIL",
            EventType::Abort => "ABORT",
        }
    }

    /// How a finished run's status maps onto the spec's terminal types.
    ///
    /// `interrupted` is ABORT rather than FAIL, which is the distinction #259
    /// exists to keep: the run stopped being observed, it did not fail, and a
    /// consumer that treats those the same will re-run work that may well have
    /// completed.
    pub fn from_status(status: &str) -> EventType {
        match status {
            "ok" | "finished" => EventType::Complete,
            "cancelled" | "interrupted" => EventType::Abort,
            _ => EventType::Fail,
        }
    }
}

/// Where events go. Absent from the workspace means nowhere.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    /// The job namespace every event carries. Conventionally the environment
    /// or team, not the machine.
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// An OpenLineage HTTP endpoint. Absent means the local file only, which is
    /// a perfectly good way to run this: something else can ship the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Seconds to wait on the collector. Deliberately short and not
    /// configurable upward without thought: this runs on the path of a run
    /// finishing.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Replace dataset names with a digest, for an organisation that wants the
    /// shape of its graph in a shared tool without the table names.
    #[serde(default)]
    pub hash_dataset_names: bool,
}

fn default_namespace() -> String {
    "duckle".to_string()
}

fn default_timeout() -> u64 {
    5
}

impl Default for Config {
    fn default() -> Self {
        Config {
            namespace: default_namespace(),
            endpoint: None,
            timeout_secs: default_timeout(),
            hash_dataset_names: false,
        }
    }
}

pub fn config_path(workspace: &Path) -> std::path::PathBuf {
    workspace.join("openlineage.json")
}

/// The configuration, or `None` when the workspace has not asked for this.
///
/// A file that exists but cannot be parsed is also `None`, with a warning: a
/// typo in an observability config must not stop runs, and silently falling
/// back to defaults would export to somewhere nobody chose.
pub fn load(workspace: &Path) -> Option<Config> {
    let path = config_path(workspace);
    let text = std::fs::read_to_string(&path).ok()?;
    match serde_json::from_str(&text) {
        Ok(cfg) => Some(cfg),
        Err(e) => {
            eprintln!("duckle: {} is not readable, lineage export is off: {e}", path.display());
            None
        }
    }
}

/// A dataset, in the two parts the spec splits every name into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dataset {
    pub namespace: String,
    pub name: String,
}

/// Split a catalog asset id into an OpenLineage namespace and name.
///
/// Catalog ids are already URI-shaped and already credential-free - they are
/// built through `catalog::public_address`, which strips userinfo and DSN
/// credential segments. What is left to remove here is the query string, which
/// is where a signed object-store URL keeps its signature.
pub fn dataset_of(asset_id: &str, hash_names: bool) -> Dataset {
    let clean = asset_id.split(['?', '#']).next().unwrap_or(asset_id);
    let dataset = match clean.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('/') {
            // `s3://bucket/key` -> namespace `s3://bucket`, name `key`, which
            // is what the naming spec asks for: the namespace addresses the
            // system, the name addresses the thing inside it.
            Some((authority, tail)) => Dataset {
                namespace: format!("{scheme}://{authority}"),
                name: tail.to_string(),
            },
            // `salesforce://Account` with no path: the authority IS the name.
            None => Dataset { namespace: format!("{scheme}://"), name: rest.to_string() },
        },
        // A local path. `file` is the spec's namespace for it; the path is the
        // name, forward-slashed already by the catalog.
        None => Dataset { namespace: "file".to_string(), name: clean.to_string() },
    };
    match hash_names {
        false => dataset,
        // The namespace stays: knowing a graph spans two Postgres instances is
        // the point, and it names no table.
        true => Dataset { namespace: dataset.namespace, name: digest(&dataset.name) },
    }
}

fn digest(value: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(value.as_bytes());
    h.finalize().iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// A stable UUID for a Duckle run id.
///
/// The spec requires `runId` to be a UUID and Duckle's ids are readable strings
/// (`run-scheduled-nightly-1788203742570`), so they are mapped rather than
/// passed through. Derived, not random, because START and COMPLETE are emitted
/// by different calls and must agree - a random id per event would produce two
/// unrelated runs in the collector and no completed one at all. The original id
/// travels in a facet, so the mapping is reversible by looking, not by
/// computing.
pub fn run_uuid(duckle_run_id: &str) -> String {
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, duckle_run_id.as_bytes()).to_string()
}

/// Datasets a run touched, from the catalog joined to the receipt by node id.
///
/// The catalog says which asset each NODE touches and the receipt says what
/// each node did, so the join is exact rather than a guess from names. A node
/// the receipt never recorded is left out: the run stopped before reaching it,
/// and reporting it as a dataset with no rows would claim it was touched.
fn datasets(
    receipt: &RunReceipt,
    catalog: &Catalog,
    pipeline_id: &str,
    direction: Direction,
    hash_names: bool,
) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for touch in catalog
        .touches
        .iter()
        .filter(|t| t.pipeline_id == pipeline_id && t.direction == direction)
    {
        let Some(node) = receipt.nodes.get(&touch.node_id) else { continue };
        // Present in the receipt is not the same as ran. The engine back-fills
        // every stage a budget stop or an earlier failure prevented, as
        // `skipped`, so joining on presence alone asserts the run wrote a table
        // it never opened - and a freshness or impact query built on that edge
        // is wrong in the direction that matters.
        if node.status == "skipped" {
            continue;
        }
        let ds = dataset_of(&touch.asset, hash_names);
        if !seen.insert(format!("{}|{}", ds.namespace, ds.name)) {
            continue;
        }
        let mut entry = json!({ "namespace": ds.namespace, "name": ds.name });
        // Which system the dataset lives in, so a collector can group
        // "orders in postgres://warehouse" apart from "orders in s3://lake".
        // The namespace is already the URI form and already credential-free;
        // `name` repeats it because the facet wants a display name and a uri
        // and there is no friendlier name recorded.
        entry["facets"]["dataSource"] = json!({
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
            "name": ds.namespace,
            "uri": ds.namespace,
        });
        // The columns the pipelines declare for this asset, which the catalog
        // already unions across every node that touches it. Emitted only when
        // there are some: an empty field list would read as "this dataset has
        // no columns", and the catalog cannot tell that apart from "nobody
        // declared any" - which it says so itself.
        if let Some(asset) = catalog.assets.iter().find(|a| a.id == touch.asset) {
            if !asset.columns.is_empty() {
                entry["facets"]["schema"] = json!({
                    "_producer": PRODUCER,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/SchemaDatasetFacet.json",
                    "fields": asset
                        .columns
                        .iter()
                        .map(|c| json!({ "name": c }))
                        .collect::<Vec<_>>()
                });
            }
        }
        // The output-statistics facet, only when a count was actually
        // recorded. Absent is not zero: a run that stopped early counted
        // nothing, and emitting 0 would report an empty table.
        // A name still holding a `${...}` is not an address: the catalog records
        // what the pipeline says, and this one is decided at run time. #311 is
        // explicit that dynamic references are marked rather than asserted, so
        // a consumer can tell "this dataset" from "some dataset whose identity
        // Duckle does not know". Same rule as the affected-pipeline walk.
        if touch.asset.contains("${") {
            // Assigned into `facets`, not over it. Replacing the object dropped
            // the schema facet written just above, and a dated path
            // (`/lake/orders_${date}.parquet`) is both the common shape for
            // this and exactly the case that triggers it.
            entry["facets"]["duckle"] = json!({
                "_producer": PRODUCER,
                "_schemaURL": SCHEMA_URL,
                "unresolved": true,
                "reason": "the reference is decided at run time, so this name does not address a single dataset"
            });
        }
        // The spec has a different facet for each direction, and an
        // OutputDatasetFacet on an input is not a valid input facet: a
        // collector that validates against `_schemaURL` either rejects it or
        // files rows-read under a key no consumer of input statistics reads.
        if let Some(rows) = node.rows {
            let (field, schema) = match direction {
                Direction::Read => (
                    "inputStatistics",
                    "https://openlineage.io/spec/facets/1-0-0/InputStatisticsInputDatasetFacet.json",
                ),
                Direction::Write => (
                    "outputStatistics",
                    "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json",
                ),
            };
            entry["facets"][field] =
                json!({ "_producer": PRODUCER, "_schemaURL": schema, "rowCount": rows });
        }
        out.push(entry);
    }
    out
}

/// One OpenLineage RunEvent.
pub fn event(
    cfg: &Config,
    kind: EventType,
    receipt: &RunReceipt,
    catalog: &Catalog,
    pipeline_id: &str,
) -> Value {
    let mut run_facets = json!({
        // Not a spec facet: a facet under our own producer carrying the ids the
        // spec has no field for, so a consumer can join an event back to the
        // Duckle run it came from without reversing the UUID.
        "duckle": {
            "_producer": PRODUCER,
            "_schemaURL": SCHEMA_URL,
            "runId": receipt.run_id,
            "trigger": receipt.trigger,
            "state": receipt.state,
            "engineVersion": receipt.engine_version,
            "pipelineHash": receipt.pipeline_hash,
        }
    });
    if let Some(parent) = &receipt.parent_run_id {
        // The spec's own parent facet, so a collector draws the tree rather
        // than showing unrelated runs.
        run_facets["parent"] = json!({
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
            "run": { "runId": run_uuid(parent) },
            "job": { "namespace": cfg.namespace, "name": pipeline_id }
        });
    }
    if matches!(kind, EventType::Fail) {
        // The message only. A stack or a SQL statement can carry a table name,
        // a path, or a value from the data, and this document is leaving the
        // building.
        run_facets["errorMessage"] = json!({
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
            "message": receipt.status,
            "programmingLanguage": "SQL"
        });
    }

    json!({
        "eventType": kind.as_str(),
        // When THIS event happened, not when the run began. `receipt.at` is
        // stamped once, in `begin`; using it for the terminal event too gave
        // every run in a collector a duration of zero, and left two events
        // sharing a timestamp with no defined order between them.
        "eventTime": match kind {
            EventType::Start => receipt.at.clone(),
            _ => chrono::Utc::now().to_rfc3339(),
        },
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": { "runId": run_uuid(&receipt.run_id), "facets": run_facets },
        "job": { "namespace": cfg.namespace, "name": pipeline_id },
        "inputs": datasets(receipt, catalog, pipeline_id, Direction::Read, cfg.hash_dataset_names),
        "outputs": datasets(receipt, catalog, pipeline_id, Direction::Write, cfg.hash_dataset_names),
    })
}

/// Whether policy allows shipping events off the machine (#311).
///
/// The local file is not gated by this: writing a workspace's own lineage into
/// its own logs is not an egress, and refusing it would take away the
/// operator's copy for no security gain. What a server policy has to be able to
/// forbid is the POST, because a workspace file naming an endpoint is otherwise
/// enough to send the shape of the estate somewhere nobody chose.
///
/// A policy that cannot be READ refuses. An unreadable policy file is exactly
/// when an operator most wants the conservative answer, and this cannot end a
/// run either way.
pub fn export_permitted(workspace: &Path) -> bool {
    crate::policy::load(Some(workspace)).map(|p| p.allow_lineage_export).unwrap_or(false)
}

/// Append the event to the workspace's local log, then try the collector.
///
/// In that order deliberately. The file IS the buffer #311 asks for: a
/// collector that is down costs one timeout and the event is already durable,
/// rather than being held in memory and lost with the process. Nothing here
/// returns an error, because nothing here may end a run.
pub fn emit(workspace: &Path, cfg: &Config, event: &Value) {
    let line = match serde_json::to_string(event) {
        Ok(l) => l,
        Err(_) => return,
    };
    let dir = workspace.join("logs");
    if std::fs::create_dir_all(&dir).is_ok() {
        // One write, and a torn tail terminated first: `ndjson` does both, so two
        // runs in one workspace cannot interleave and a killed one cannot take
        // the next event down with it.
        let path = dir.join("openlineage.ndjson");
        let _ = crate::ndjson::append_records(&path, &line);
        // The bound is enforced where events arrive, not where the drain
        // rewrites: a collector down for weeks must not grow the file without
        // limit, and enforcing it here keeps that true for the whole outage.
        enforce_buffer_bound(&path);
    }
    let Some(endpoint) = cfg.endpoint.as_deref().filter(|e| !e.trim().is_empty()) else {
        return;
    };
    // The local file is written either way; only the egress is gated. A server
    // policy has to be able to forbid shipping the shape of the estate to a
    // collector a workspace file named, and refusing the write as well would
    // take away the operator's own copy for no security gain.
    if !export_permitted(workspace) {
        eprintln!(
            "duckle: policy forbids lineage export; the event is in logs/openlineage.ndjson only"
        );
        return;
    }
    // One pass over the buffer rather than a POST for just this event: the
    // event just written is the newest line in the file, so draining sends it
    // AND whatever a down collector missed earlier - the shipper #311 asked
    // for, on the path of the run that produces the events. Capped per emit:
    // a collector coming back after a week owes thousands of events, and
    // draining them serially from inside a run that has already written its
    // receipt would hold the process open for minutes. `openlineage flush`
    // is the unbounded path an operator or a timer reaches for.
    let outcome = flush_bounded(workspace, cfg, EMIT_DRAIN_CAP);
    if outcome.kept > 0 {
        eprintln!(
            "duckle: lineage export to {endpoint}: {} event(s) kept in logs/openlineage.ndjson",
            outcome.kept
        );
    }
}

/// Most events one `emit` drains in a pass. A collector that comes back after
/// a week still gets its backlog, in slices, across the runs that produce it;
/// `openlineage flush` is the unbounded path for draining it at once.
const EMIT_DRAIN_CAP: usize = 100;

/// Most lines the buffer holds. A collector down for weeks must not grow a
/// run's log file without bound: past this the oldest events are moved to
/// `logs/openlineage.dropped.ndjson` with one log line, which beats both a
/// full disk and a gap nobody can account for. Enforced in `emit`, where
/// events arrive.
const BUFFER_CAP_LINES: usize = 10_000;

/// Keep the buffer at most `BUFFER_CAP_LINES` lines, shedding the oldest.
///
/// Runs on every `emit` so the bound holds for the entire outage rather than
/// only when the collector comes back. The count costs one read of the file
/// and no parsing; the rewrite only happens while the file is over the cap.
/// The buffer is moved aside before the rewrite so an event appended by a
/// concurrent run lands in the fresh file and is kept, and what the bound
/// sheds is appended to `openlineage.dropped.ndjson` rather than deleted, so
/// an operator can still tell which events never reached the collector.
fn enforce_buffer_bound(path: &Path) {
    let Ok(bytes) = std::fs::read(path) else {
        return;
    };
    if bytes.iter().filter(|&&b| b == b'\n').count() <= BUFFER_CAP_LINES {
        return;
    }
    // Move the file aside before rewriting: a concurrent emit appends to the
    // fresh buffer, so no event in flight is lost to the trim.
    static TRIM_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = TRIM_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let trimming = path.with_extension(format!("ndjson.sending.{}.{seq}", std::process::id()));
    if std::fs::rename(path, &trimming).is_err() {
        return;
    }
    // Read the moved file rather than trusting the earlier snapshot: a line
    // appended between that read and the rename only exists inside `trimming`.
    let Ok(bytes) = std::fs::read(&trimming) else {
        let _ = std::fs::rename(&trimming, path);
        return;
    };
    let text = String::from_utf8_lossy(&bytes);
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    if lines.len() <= BUFFER_CAP_LINES {
        // Another writer trimmed first; put this copy back through the
        // appender so nothing it holds is lost.
        if crate::ndjson::append_records(path, &text).is_ok() {
            let _ = std::fs::remove_file(&trimming);
        }
        return;
    }
    let dropped = &lines[..lines.len() - BUFFER_CAP_LINES];
    let kept = &lines[lines.len() - BUFFER_CAP_LINES..];
    eprintln!(
        "duckle: lineage buffer over {BUFFER_CAP_LINES} events; moved the {} oldest to logs/openlineage.dropped.ndjson",
        dropped.len()
    );
    let dropped_path = path.with_file_name("openlineage.dropped.ndjson");
    let _ = crate::ndjson::append_records(&dropped_path, &dropped.join("\n"));
    if crate::ndjson::append_records(path, &kept.join("\n")).is_ok() {
        let _ = std::fs::remove_file(&trimming);
    }
}

/// What one POST told the drain about this line.
enum Post {
    /// Accepted.
    Sent,
    /// A transport error, a 5xx, or a transient 4xx (429, 408, auth refused
    /// mid-recovery): worth keeping for the next pass, and the lines after it
    /// are very likely refused the same way, so the pass stops.
    Retry(String),
    /// A status that says the event itself is malformed - 400, 413, 422: the
    /// collector will never take it. Keeping it would stop the queue at the
    /// same poison line on every pass, so it is quarantined and the pass goes
    /// on. Other 4xx are transient states (rate limits, auth), not verdicts
    /// on the event, and are retried rather than quarantined.
    Rejected(u16),
}

/// One POST to the collector.
fn post(endpoint: &str, timeout_secs: u64, line: &str) -> Post {
    let agent = crate::tls::http_agent_with(&crate::tls::HttpTransport {
        read_timeout_secs: Some(timeout_secs),
        connect_timeout_secs: Some(timeout_secs),
        ..Default::default()
    });
    match agent
        .post(endpoint)
        .set("Content-Type", "application/json")
        .send_string(line)
    {
        Ok(_) => Post::Sent,
        Err(ureq::Error::Status(code, _)) if matches!(code, 400 | 413 | 422) => {
            Post::Rejected(code)
        }
        Err(e) => Post::Retry(e.to_string()),
    }
}

/// The runId a buffered event carries, for the one log line a quarantined or
/// dropped event gets. Events always carry it under the duckle facet; absent,
/// the event itself cannot be named and that is worth saying too.
fn run_id_of(line: &str) -> String {
    serde_json::from_str::<Value>(line)
        .ok()
        .and_then(|v| {
            v["run"]["facets"]["duckle"]["runId"]
                .as_str()
                .map(str::to_string)
        })
        .unwrap_or_else(|| "<unknown>".into())
}

/// What a `flush` pass did, for the caller to report.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Events accepted by the collector this pass.
    pub sent: usize,
    /// Events still in the buffer - refused, or never attempted after an
    /// earlier failure in the same pass.
    pub kept: usize,
    /// Events the collector rejected outright (4xx), quarantined to
    /// `logs/openlineage.rejected.ndjson` rather than kept.
    pub rejected: usize,
}

/// Drain `logs/openlineage.ndjson` to the configured collector (#311).
///
/// The NDJSON file is the buffer `emit` writes before it ever tries the
/// network, so it accumulates every event a down collector missed. This sends
/// each buffered line once; what the collector refuses stays for the next
/// pass. Called from `emit` with a per-pass cap, and from the
/// `openlineage flush` command with none, for an operator or a scheduler that
/// wants the backlog drained without waiting for a run.
///
/// The pass moves the buffer aside with one atomic rename before reading it.
/// A concurrent `emit` then simply starts a fresh buffer, so no append can be
/// lost between a read and a rewrite, no partially written append can be
/// spliced into the rebuilt file, and two flushes cannot write through one
/// shared temp name (the race `alerts.rs` already paid for). Events carry
/// their own `eventTime`, so sending the moved file while newer events land
/// in the fresh buffer reorders nothing a collector can see.
///
/// Best effort like everything in this module: it returns what it did rather
/// than an error a run could trip on, and it is gated by `export_permitted`
/// because draining is the same egress `emit` gates.
pub fn flush(workspace: &Path, cfg: &Config) -> FlushOutcome {
    flush_bounded(workspace, cfg, usize::MAX)
}

fn flush_bounded(workspace: &Path, cfg: &Config, cap: usize) -> FlushOutcome {
    let mut outcome = FlushOutcome::default();
    let path = workspace.join("logs").join("openlineage.ndjson");
    // Recovery is not gated on the collector still being configured: an
    // interrupted pass leaves the only copy of undelivered events under the
    // `sending` name, and they belong back in the buffer whatever happens next.
    adopt_stranded_buffers(&path);
    let Some(endpoint) = cfg.endpoint.as_deref().filter(|e| !e.trim().is_empty()) else {
        return outcome;
    };
    if !export_permitted(workspace) {
        return outcome;
    }
    // A temp name of this writer's own: two flushes in one workspace (serve
    // plus the scheduler, or a flush on a timer) must never share one.
    static SEND_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEND_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let sending = path.with_extension(format!("ndjson.sending.{}.{seq}", std::process::id()));
    if std::fs::rename(&path, &sending).is_err() {
        // No buffer, or a platform that cannot move it: either way there is
        // nothing this pass can honestly claim to have drained.
        return outcome;
    }
    let Ok(snapshot) = std::fs::read(&sending) else {
        // Leave it under the `sending` name: a later pass adopts it once this
        // process is gone, which beats guessing at contents it cannot read.
        return outcome;
    };
    // Lines stay raw: a partially written tail is kept in the buffer for the
    // next pass rather than filtered out of the file it was torn in.
    let mut kept: Vec<String> = Vec::new();
    let mut lines = String::from_utf8_lossy(&snapshot)
        .into_owned()
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect::<Vec<_>>()
        .into_iter();
    let mut rejected_lines: Vec<String> = Vec::new();
    for line in lines.by_ref() {
        if outcome.sent + outcome.rejected >= cap {
            // The emit cap: keep the rest unattempted. The command passes
            // usize::MAX and drains everything.
            kept.push(line);
            kept.extend(lines);
            break;
        }
        if serde_json::from_str::<Value>(&line).is_err() {
            // Not an event the collector can take - most likely a torn tail
            // from a killed writer. It stays buffered, never posted.
            kept.push(line);
            continue;
        }
        match post(endpoint, cfg.timeout_secs, &line) {
            Post::Sent => outcome.sent += 1,
            Post::Rejected(code) => {
                outcome.rejected += 1;
                eprintln!(
                    "duckle: lineage export: collector refused an event (HTTP {code}, runId {}); quarantined to logs/openlineage.rejected.ndjson",
                    run_id_of(&line)
                );
                rejected_lines.push(line);
            }
            Post::Retry(e) => {
                // A collector that has gone down again will refuse the rest
                // too, and a timeout apiece is the cost of asking. Keep this
                // line and everything after it for the next pass.
                eprintln!("duckle: lineage export to {endpoint} failed: {e}");
                kept.push(line);
                kept.extend(lines);
                break;
            }
        }
    }
    outcome.kept = kept.len();
    if !rejected_lines.is_empty() {
        let rejected = workspace.join("logs").join("openlineage.rejected.ndjson");
        let _ = crate::ndjson::append_records(&rejected, &rejected_lines.join("\n"));
    }
    // Kept lines go back through the appender even when nothing changed:
    // renaming `sending` over a buffer a concurrent emit just started would
    // lose its events, which is the race the move-aside exists to prevent.
    // `sending` is removed only once its contents are safely back - a failed
    // re-append must not delete the only copy of undelivered events.
    if kept.is_empty() || crate::ndjson::append_records(&path, &kept.join("\n")).is_ok() {
        let _ = std::fs::remove_file(&sending);
    }
    outcome
}

/// Fold orphaned `openlineage.ndjson.sending.<pid>.<seq>` files back into the
/// buffer. An interrupted pass - Ctrl-C on a long recovery flush, a reboot -
/// leaves the moved-aside buffer under a name nothing reads while the next
/// `emit` starts a fresh file and the command reports nothing buffered. Only
/// files whose writer is no longer live are taken; a pass still running keeps
/// its own.
fn adopt_stranded_buffers(path: &Path) {
    let Some(dir) = path.parent() else { return };
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    let prefix = "openlineage.ndjson.sending.";
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix(prefix) else { continue };
        let Some(pid) = rest.split('.').next().and_then(|p| p.parse::<u32>().ok()) else {
            continue;
        };
        if crate::runlock::process_alive(pid) {
            continue;
        }
        let Ok(content) = std::fs::read(entry.path()) else { continue };
        if content.is_empty() {
            let _ = std::fs::remove_file(entry.path());
            continue;
        }
        eprintln!("duckle: recovering a lineage buffer stranded by an interrupted flush ({name})");
        if crate::ndjson::append_records(path, &String::from_utf8_lossy(&content)).is_ok() {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::ReceiptNode;
    use std::collections::BTreeMap;

    fn receipt(status: &str) -> RunReceipt {
        RunReceipt {
            run_id: "run-scheduled-nightly-1788203742570".into(),
            trigger: "scheduled".into(),
            state: "finished".into(),
            pid: None,
            parent_run_id: None,
            at: "2026-09-01T00:00:00Z".into(),
            status: status.into(),
            pipeline_name: "nightly".into(),
            pipeline_path: "pipelines/nightly.json".into(),
            pipeline_hash: "abc123".into(),
            engine_version: "1.5.4".into(),
            parameters: BTreeMap::new(),
            parameter_sources: Vec::new(),
            release_id: None,
            components: Vec::new(),
            artifacts: Vec::new(),
            outputs: Default::default(),
            partition_key: None,
            resource_pool: None,
            queue_reason: None,
            queued_at: None,
            started_at: None,
            queue_ms: None,
            nodes: BTreeMap::from([
                (
                    "src".to_string(),
                    ReceiptNode {
                        status: "ok".into(),
                        kind: Some("source".into()),
                        output_cache_key: None,
                        rows: Some(100),
                        duration_ms: Some(10),
                    },
                ),
                (
                    "out".to_string(),
                    ReceiptNode {
                        status: "ok".into(),
                        kind: Some("sink".into()),
                        output_cache_key: None,
                        rows: Some(100),
                        duration_ms: Some(20),
                    },
                ),
            ]),
        }
    }

    fn catalog() -> Catalog {
        crate::catalog::build_from_documents(&[(
            "nightly".to_string(),
            serde_json::json!({
                "name": "nightly",
                "nodes": [
                    { "id": "src", "type": "source", "data": { "componentId": "src.parquet",
                      "properties": { "path": "s3://lake/raw/orders.parquet" } } },
                    { "id": "out", "type": "sink", "data": { "componentId": "snk.parquet",
                      "properties": { "path": "s3://lake/curated/orders.parquet" } } }
                ],
                "edges": []
            }),
        )])
    }

    #[test]
    fn a_run_id_is_a_uuid_and_the_same_one_every_time() {
        // START and COMPLETE are emitted by different calls and must agree, or
        // the collector shows two unrelated runs and no completed one.
        let a = run_uuid("run-manual-x-1");
        let b = run_uuid("run-manual-x-1");
        assert_eq!(a, b);
        assert!(uuid::Uuid::parse_str(&a).is_ok(), "{a} is not a UUID");
        assert_ne!(a, run_uuid("run-manual-x-2"));
    }

    #[test]
    fn an_object_store_asset_splits_into_system_and_thing() {
        let d = dataset_of("s3://lake/curated/orders.parquet", false);
        assert_eq!(d.namespace, "s3://lake");
        assert_eq!(d.name, "curated/orders.parquet");
    }

    #[test]
    fn a_local_path_uses_the_file_namespace() {
        let d = dataset_of("data/orders.csv", false);
        assert_eq!(d.namespace, "file");
        assert_eq!(d.name, "data/orders.csv");
    }

    #[test]
    fn a_signed_url_does_not_carry_its_signature_off_the_machine() {
        let d = dataset_of(
            "s3://lake/x.parquet?X-Amz-Signature=deadbeef&X-Amz-Credential=AKIA",
            false,
        );
        assert_eq!(d.name, "x.parquet");
        assert!(!format!("{d:?}").contains("deadbeef"), "{d:?}");
        assert!(!format!("{d:?}").contains("AKIA"), "{d:?}");
    }

    #[test]
    fn hashing_hides_the_table_and_keeps_the_system() {
        let d = dataset_of("postgres://db:5432/sales.orders", true);
        assert_eq!(d.namespace, "postgres://db:5432", "the shape of the graph is the point");
        assert!(!d.name.contains("orders"));
        assert_eq!(d.name, dataset_of("postgres://db:5432/sales.orders", true).name);
    }

    #[test]
    fn inputs_and_outputs_come_from_the_catalog_joined_by_node() {
        let e = event(&Config::default(), EventType::Complete, &receipt("ok"), &catalog(), "nightly");
        assert_eq!(e["eventType"], "COMPLETE");
        assert_eq!(e["inputs"][0]["name"], "raw/orders.parquet");
        assert_eq!(e["outputs"][0]["name"], "curated/orders.parquet");
        assert_eq!(e["outputs"][0]["facets"]["outputStatistics"]["rowCount"], 100);
        assert_eq!(e["run"]["facets"]["duckle"]["runId"], "run-scheduled-nightly-1788203742570");
    }

    #[test]
    fn a_run_time_reference_is_marked_rather_than_asserted() {
        // `${workspace}/data/orders.csv` is what the catalog records, and it is
        // not an address: two workspaces produce the same name for different
        // files. Emitting it silently would put a dataset in someone's lineage
        // graph that joins to the wrong thing.
        let cat = crate::catalog::build_from_documents(&[(
            "nightly".to_string(),
            serde_json::json!({
                "name": "nightly",
                "nodes": [{ "id": "src", "type": "source", "data": { "componentId": "src.csv",
                  "properties": { "path": "${workspace}/data/orders.csv" } } }],
                "edges": []
            }),
        )]);
        let e = event(&Config::default(), EventType::Complete, &receipt("ok"), &cat, "nightly");
        assert_eq!(e["inputs"][0]["facets"]["duckle"]["unresolved"], true, "{e}");
    }

    #[test]
    fn a_node_the_run_never_reached_is_not_reported_as_touched() {
        // Absent is not zero: a run that stopped at node one touched nothing
        // after it, and listing those as datasets with no rows would claim
        // otherwise.
        let mut r = receipt("error");
        r.nodes.remove("out");
        let e = event(&Config::default(), EventType::Fail, &r, &catalog(), "nightly");
        assert_eq!(e["inputs"].as_array().unwrap().len(), 1);
        assert!(e["outputs"].as_array().unwrap().is_empty());
    }

    #[test]
    fn interrupted_is_abort_and_not_fail() {
        // The #259 distinction, preserved across the boundary: the run stopped
        // being observed, it did not fail, and a consumer that treats those the
        // same re-runs work that may have finished.
        assert_eq!(EventType::from_status("interrupted"), EventType::Abort);
        assert_eq!(EventType::from_status("cancelled"), EventType::Abort);
        assert_eq!(EventType::from_status("error"), EventType::Fail);
        assert_eq!(EventType::from_status("ok"), EventType::Complete);
    }

    #[test]
    fn a_parent_run_is_linked_by_the_same_mapping() {
        let mut r = receipt("ok");
        r.parent_run_id = Some("run-plan-parent-1".into());
        let e = event(&Config::default(), EventType::Complete, &r, &catalog(), "nightly");
        assert_eq!(e["run"]["facets"]["parent"]["run"]["runId"], run_uuid("run-plan-parent-1"));
    }

    #[test]
    fn every_surface_emits_because_begin_and_finish_do() {
        // The point of wiring it there rather than into each caller: a feed
        // that covers six of eight surfaces is one nobody can reason about,
        // because the missing runs look like runs that never happened.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::write(config_path(ws), r#"{"namespace":"prod"}"#).unwrap();

        let r = crate::retry::begin(
            ws,
            "run-manual-nightly-1",
            "manual",
            "nightly",
            "pipelines/nightly.json",
            "abc123",
            None,
        );
        crate::retry::finish(ws, r, "ok", Default::default());

        let log = std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap();
        let events: Vec<serde_json::Value> =
            log.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(events.len(), 2, "one START and one terminal event: {log}");
        assert_eq!(events[0]["eventType"], "START");
        assert_eq!(events[1]["eventType"], "COMPLETE");
        // Both halves of the run must carry the SAME runId or the collector
        // shows two runs and no completed one.
        assert_eq!(events[0]["run"]["runId"], events[1]["run"]["runId"]);
        assert_eq!(events[0]["job"]["namespace"], "prod");
        assert_eq!(events[0]["job"]["name"], "nightly");
        // No catalog in this workspace: the empty dataset lists are UNKNOWN,
        // not empty, and the event says which.
        assert_eq!(events[1]["run"]["facets"]["duckle"]["catalogAvailable"], false);
    }

    #[test]
    fn a_run_in_a_workspace_that_has_not_asked_writes_no_file() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let r = crate::retry::begin(ws, "run-x-1", "manual", "p", "pipelines/p.json", "h", None);
        crate::retry::finish(ws, r, "ok", Default::default());
        assert!(
            !ws.join("logs/openlineage.ndjson").exists(),
            "export must be off unless the workspace asked for it"
        );
    }

    #[test]
    fn declared_columns_travel_with_the_dataset() {
        let cat = crate::catalog::build_from_documents(&[(
            "nightly".to_string(),
            serde_json::json!({
                "name": "nightly",
                "nodes": [{ "id": "out", "type": "sink", "data": { "componentId": "snk.parquet",
                  "properties": { "path": "s3://lake/curated/orders.parquet" },
                  "schema": [{ "name": "id", "type": "BIGINT" }, { "name": "amount", "type": "DOUBLE" }] } }],
                "edges": []
            }),
        )]);
        let e = event(&Config::default(), EventType::Complete, &receipt("ok"), &cat, "nightly");
        let fields = e["outputs"][0]["facets"]["schema"]["fields"].as_array().expect("schema facet");
        let names: Vec<&str> = fields.iter().filter_map(|f| f["name"].as_str()).collect();
        assert!(names.contains(&"id") && names.contains(&"amount"), "{names:?}");
    }

    #[test]
    fn an_asset_nobody_declared_columns_for_gets_no_schema_facet() {
        // An empty field list reads as "this dataset has no columns", which is
        // a different claim from "nobody said".
        let e = event(&Config::default(), EventType::Complete, &receipt("ok"), &catalog(), "nightly");
        assert!(e["outputs"][0]["facets"].get("schema").is_none(), "{e}");
    }

    #[test]
    fn policy_can_forbid_the_egress_without_taking_the_local_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        assert!(export_permitted(ws), "no policy means no restriction");

        std::fs::create_dir_all(ws.join(".duckle")).unwrap();
        std::fs::write(ws.join(".duckle/policy.yaml"), "network:
  allowLineageExport: false
")
            .unwrap();
        assert!(!export_permitted(ws), "policy did not forbid the export");

        // The operator still gets their own copy: the file is not an egress.
        let cfg = Config { endpoint: Some("http://collector.invalid/x".into()), ..Config::default() };
        emit(ws, &cfg, &json!({ "eventType": "START" }));
        let log = std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap();
        assert!(log.contains("START"), "{log}");
    }

    #[test]
    fn a_terminal_event_is_stamped_when_it_happened() {
        // Both events carrying `receipt.at` gave every run in a collector a
        // duration of zero, and left two events sharing one timestamp with no
        // defined order between them.
        let r = receipt("ok");
        let start = event(&Config::default(), EventType::Start, &r, &catalog(), "nightly");
        let done = event(&Config::default(), EventType::Complete, &r, &catalog(), "nightly");
        assert_eq!(start["eventTime"], r.at, "START is when the run began");
        assert_ne!(done["eventTime"], start["eventTime"], "the run took no time at all");
    }

    #[test]
    fn a_skipped_node_is_not_a_dataset_the_run_touched() {
        // The engine back-fills every stage a budget stop or an earlier failure
        // prevented, as `skipped`. Joining on presence alone asserts the run
        // wrote a table it never opened.
        let mut r = receipt("error");
        r.nodes.get_mut("out").unwrap().status = "skipped".into();
        r.nodes.get_mut("out").unwrap().rows = None;
        let e = event(&Config::default(), EventType::Fail, &r, &catalog(), "nightly");
        assert!(
            e["outputs"].as_array().unwrap().is_empty(),
            "a skipped sink was reported as written: {e}"
        );
        assert_eq!(e["inputs"].as_array().unwrap().len(), 1, "the source did run");
    }

    #[test]
    fn statistics_match_the_direction_they_are_on() {
        let e = event(&Config::default(), EventType::Complete, &receipt("ok"), &catalog(), "nightly");
        assert_eq!(e["inputs"][0]["facets"]["inputStatistics"]["rowCount"], 100);
        assert!(e["inputs"][0]["facets"].get("outputStatistics").is_none(),
            "an OutputDatasetFacet on an input is not a valid input facet");
        assert_eq!(e["outputs"][0]["facets"]["outputStatistics"]["rowCount"], 100);
    }

    #[test]
    fn an_unresolved_name_keeps_its_schema_facet() {
        // A dated path is both the common shape for a template placeholder and
        // exactly the case where the two facets meet.
        let cat = crate::catalog::build_from_documents(&[(
            "nightly".to_string(),
            serde_json::json!({
                "name": "nightly",
                "nodes": [{ "id": "out", "type": "sink", "data": { "componentId": "snk.parquet",
                  "properties": { "path": "/lake/orders_${date}.parquet" },
                  "schema": [{ "name": "id", "type": "BIGINT" }] } }],
                "edges": []
            }),
        )]);
        let e = event(&Config::default(), EventType::Complete, &receipt("ok"), &cat, "nightly");
        let out = &e["outputs"][0]["facets"];
        assert_eq!(out["duckle"]["unresolved"], true, "{e}");
        assert!(out.get("schema").is_some(), "the schema facet was overwritten: {e}");
        assert!(out.get("outputStatistics").is_some(), "{e}");
    }

    #[test]
    fn an_interrupted_run_gets_a_terminal_event() {
        // Without one a collector shows the run RUNNING forever, which is
        // indistinguishable from a run still in flight - the exact state the
        // ABORT distinction exists to avoid.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::write(config_path(ws), r#"{"namespace":"prod"}"#).unwrap();
        let r = crate::retry::begin(ws, "run-x-9", "scheduled", "p", "pipelines/p.json", "h", None);
        drop(r);
        // Nothing is alive, so reconcile calls it interrupted.
        let changed = crate::retry::reconcile(ws, &|_| false);
        assert_eq!(changed, vec!["run-x-9"]);
        let events: Vec<serde_json::Value> =
            std::fs::read_to_string(ws.join("logs/openlineage.ndjson"))
                .unwrap()
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
        assert_eq!(events.len(), 2, "START and a terminal event");
        assert_eq!(events[1]["eventType"], "ABORT");
        assert_eq!(events[0]["run"]["runId"], events[1]["run"]["runId"]);
    }

    #[test]
    fn a_workspace_that_has_not_asked_gets_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(load(tmp.path()).is_none(), "export must be off unless configured");
    }

    #[test]
    fn an_unparseable_config_disables_rather_than_defaults() {
        // Falling back to defaults would export to a namespace nobody chose.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(config_path(tmp.path()), "{ not json").unwrap();
        assert!(load(tmp.path()).is_none());
    }

    #[test]
    fn emitting_without_a_collector_still_leaves_the_event_on_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        emit(tmp.path(), &cfg, &json!({ "eventType": "START" }));
        let log = std::fs::read_to_string(tmp.path().join("logs/openlineage.ndjson")).unwrap();
        assert!(log.contains("START"), "{log}");
    }

    #[test]
    fn the_datasource_facet_names_the_system() {
        let e = event(&Config::default(), EventType::Complete, &receipt("ok"), &catalog(), "nightly");
        assert_eq!(e["outputs"][0]["facets"]["dataSource"]["uri"], "s3://lake");
        assert_eq!(e["inputs"][0]["facets"]["dataSource"]["name"], "s3://lake");
    }

    /// A collector stub: one connection per POST, the next status per accept.
    /// Returns the endpoint and every body it received.
    fn stub_collector(
        statuses: Vec<u16>,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let got = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let g = got.clone();
        std::thread::spawn(move || {
            for status in statuses {
                let Ok((stream, _)) = listener.accept() else { break };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut len = 0usize;
                loop {
                    let mut h = String::new();
                    if reader.read_line(&mut h).is_err() || h.trim().is_empty() {
                        break;
                    }
                    if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; len];
                let _ = reader.read_exact(&mut body);
                g.lock().unwrap().push(String::from_utf8_lossy(&body).into_owned());
                let _ = stream.try_clone().unwrap().write_all(
                    format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                );
            }
        });
        (format!("http://127.0.0.1:{port}/api/v1/lineage"), got)
    }

    fn buffered(ws: &Path, events: &[&str]) {
        let dir = ws.join("logs");
        std::fs::create_dir_all(&dir).unwrap();
        crate::ndjson::append_records(&dir.join("openlineage.ndjson"), &events.join("\n")).unwrap();
    }

    #[test]
    fn flush_sends_the_backlog_and_keeps_what_is_refused() {
        // A collector that was down accumulates events; when it comes back the
        // buffer drains in order and only the refused lines stay.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (endpoint, got) = stub_collector(vec![200, 500]);
        let cfg = Config { endpoint: Some(endpoint), ..Config::default() };
        buffered(ws, &[r#"{"eventType":"START","n":1}"#, r#"{"eventType":"COMPLETE","n":2}"#]);

        let out = flush(ws, &cfg);
        assert_eq!(out, FlushOutcome { sent: 1, kept: 1, rejected: 0 });
        let left = std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap();
        assert!(!left.contains("\"n\":1"), "delivered events leave the buffer: {left}");
        assert!(left.contains("\"n\":2"), "a refused event stays for the next pass: {left}");
        assert_eq!(got.lock().unwrap().len(), 2);
    }

    #[test]
    fn a_successful_flush_empties_the_buffer_and_a_dead_collector_keeps_it() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (endpoint, got) = stub_collector(vec![200, 200]);
        let cfg = Config { endpoint: Some(endpoint), ..Config::default() };
        buffered(ws, &[r#"{"eventType":"START"}"#, r#"{"eventType":"COMPLETE"}"#]);
        assert_eq!(flush(ws, &cfg), FlushOutcome { sent: 2, kept: 0, rejected: 0 });
        assert_eq!(
            std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap_or_default(),
            "",
            "a fully drained buffer leaves nothing owed"
        );
        assert_eq!(got.lock().unwrap().len(), 2);

        // Nothing listening: the events stay buffered rather than being lost,
        // and the buffer comes back byte-identical - a pass that changed
        // nothing must not pay a parse and a rewrite.
        buffered(ws, &[r#"{"eventType":"START"}"#]);
        let before = std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap();
        let cfg = Config {
            endpoint: Some("http://127.0.0.1:9/none".into()),
            timeout_secs: 1,
            ..Config::default()
        };
        assert_eq!(flush(ws, &cfg), FlushOutcome { sent: 0, kept: 1, rejected: 0 });
        assert_eq!(std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap(), before);
    }

    /// A 4xx is a poison event, not a retryable one: keeping it would wedge
    /// the queue at the same line on every pass. It is quarantined, logged,
    /// and the pass goes on to the events behind it.
    #[test]
    fn a_4xx_is_quarantined_and_the_queue_moves_on() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (endpoint, got) = stub_collector(vec![422, 200]);
        let cfg = Config { endpoint: Some(endpoint), ..Config::default() };
        buffered(ws, &[r#"{"eventType":"START","n":1}"#, r#"{"eventType":"COMPLETE","n":2}"#]);

        let out = flush(ws, &cfg);
        assert_eq!(out, FlushOutcome { sent: 1, kept: 0, rejected: 1 });
        assert_eq!(got.lock().unwrap().len(), 2, "the pass continues past the poison line");
        let quarantined =
            std::fs::read_to_string(ws.join("logs/openlineage.rejected.ndjson")).unwrap();
        assert!(quarantined.contains("\"n\":1"), "{quarantined}");
        assert!(!quarantined.contains("\"n\":2"), "{quarantined}");
    }

    /// `emit` drains a slice, not the whole backlog: a collector returning
    /// after a long outage must not hold a finished run open for one POST per
    /// buffered event. The `openlineage flush` command is the unbounded path.
    #[test]
    fn a_bounded_pass_sends_at_most_the_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (endpoint, got) = stub_collector(vec![200, 200, 200]);
        let cfg = Config { endpoint: Some(endpoint), ..Config::default() };
        buffered(
            ws,
            &[r#"{"eventType":"START","n":1}"#, r#"{"eventType":"COMPLETE","n":2}"#, r#"{"eventType":"COMPLETE","n":3}"#],
        );

        let out = flush_bounded(ws, &cfg, 1);
        assert_eq!(out, FlushOutcome { sent: 1, kept: 2, rejected: 0 });
        assert_eq!(got.lock().unwrap().len(), 1);
        let left = std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap();
        assert!(!left.contains("\"n\":1"), "{left}");
        assert!(left.contains("\"n\":2") && left.contains("\"n\":3"), "{left}");
    }

    #[test]
    fn flush_without_an_endpoint_or_policy_is_a_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        buffered(ws, &[r#"{"eventType":"START"}"#]);
        assert_eq!(flush(ws, &Config::default()), FlushOutcome::default());
        assert!(std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap().contains("START"));

        std::fs::create_dir_all(ws.join(".duckle")).unwrap();
        std::fs::write(ws.join(".duckle/policy.yaml"), "network:\n  allowLineageExport: false\n")
            .unwrap();
        let cfg = Config {
            endpoint: Some("http://127.0.0.1:9/none".into()),
            ..Config::default()
        };
        assert_eq!(flush(ws, &cfg), FlushOutcome::default(), "draining is the same egress emit gates");
    }

    /// The buffer bound is enforced where events arrive: a dead collector
    /// means every pass refuses, so a bound that only lives in the drain path
    /// never fires while the file grows. `emit` sheds the oldest over
    /// BUFFER_CAP_LINES to `openlineage.dropped.ndjson` instead.
    #[test]
    fn the_buffer_stops_growing_while_the_collector_is_down() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join(".duckle")).unwrap();
        std::fs::write(ws.join(".duckle/policy.yaml"), "network:\n  allowLineageExport: true\n")
            .unwrap();
        let cfg = Config {
            endpoint: Some("http://127.0.0.1:9/none".into()),
            timeout_secs: 1,
            ..Config::default()
        };
        let events: Vec<String> = (0..BUFFER_CAP_LINES + 5)
            .map(|n| format!(r#"{{"eventType":"START","n":{n}}}"#))
            .collect();
        buffered(ws, &events.iter().map(String::as_str).collect::<Vec<_>>());

        emit(ws, &cfg, &serde_json::json!({"eventType":"COMPLETE","n":99999}));

        let left = std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap();
        assert_eq!(
            left.lines().count(),
            BUFFER_CAP_LINES,
            "the buffer holds at most the cap through the whole outage"
        );
        assert!(!left.contains("\"n\":0"), "the oldest events are the ones shed");
        assert!(left.contains("\"n\":99999"), "the event just emitted is kept");
        let dropped = std::fs::read_to_string(ws.join("logs/openlineage.dropped.ndjson")).unwrap();
        assert!(dropped.contains("\"n\":0"), "shed events are accounted for, not deleted");
    }

    /// A 429 is a rate limit on the pass, not a verdict on the event: the
    /// line stays buffered, the pass stops, and nothing is quarantined.
    #[test]
    fn a_429_stops_the_pass_without_quarantining() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (endpoint, got) = stub_collector(vec![429, 200]);
        let cfg = Config { endpoint: Some(endpoint), ..Config::default() };
        buffered(ws, &[r#"{"eventType":"START","n":1}"#, r#"{"eventType":"COMPLETE","n":2}"#]);

        let out = flush(ws, &cfg);
        assert_eq!(out, FlushOutcome { sent: 0, kept: 2, rejected: 0 });
        assert_eq!(got.lock().unwrap().len(), 1, "the pass stops at the rate limit");
        assert!(
            std::fs::read_to_string(ws.join("logs/openlineage.rejected.ndjson")).is_err(),
            "a rate-limited event is not a poison event"
        );
        let left = std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap();
        assert!(left.contains("\"n\":1") && left.contains("\"n\":2"), "{left}");
    }

    /// A pass killed mid-drain leaves its moved-aside buffer behind; the next
    /// pass folds it back rather than reporting the workspace drained while
    /// the only copy sits under a name nothing reads.
    #[test]
    fn a_stranded_sending_buffer_is_adopted_by_the_next_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let dir = ws.join("logs");
        std::fs::create_dir_all(&dir).unwrap();
        // A pid that cannot be alive: one above any real pid_max.
        std::fs::write(
            dir.join("openlineage.ndjson.sending.4000000.0"),
            "{\"eventType\":\"START\",\"n\":1}\n",
        )
        .unwrap();
        let (endpoint, got) = stub_collector(vec![200]);
        let cfg = Config { endpoint: Some(endpoint), ..Config::default() };

        let out = flush(ws, &cfg);
        assert_eq!(out, FlushOutcome { sent: 1, kept: 0, rejected: 0 });
        assert_eq!(got.lock().unwrap().len(), 1);
        assert!(
            std::fs::read_dir(&dir).unwrap().all(|e| !e.unwrap().file_name().to_string_lossy().contains("sending")),
            "the stranded file is folded back, not left behind"
        );
    }

    /// A torn tail is not an event, but it is not dropped either: it stays in
    /// the buffer while the valid lines around it drain.
    #[test]
    fn an_unparseable_line_is_kept_not_posted() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (endpoint, got) = stub_collector(vec![200]);
        let cfg = Config { endpoint: Some(endpoint), ..Config::default() };
        buffered(ws, &[r#"{"eventType":"START","n":1}"#, r#"{"eventType":"STA"#]);

        let out = flush(ws, &cfg);
        assert_eq!(out, FlushOutcome { sent: 1, kept: 1, rejected: 0 });
        assert_eq!(got.lock().unwrap().len(), 1);
        let left = std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap();
        assert!(left.contains(r#"{"eventType":"STA"#), "the torn line stays buffered: {left}");
    }

    /// A stub that runs `on_body` after reading each POST, before answering:
    /// a deterministic way to land an append inside the drain window.
    fn stub_collector_touching(
        statuses: Vec<u16>,
        on_body: impl Fn() + Send + Sync + 'static,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let got = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let g = got.clone();
        let on_body = std::sync::Arc::new(on_body);
        std::thread::spawn(move || {
            for status in statuses {
                let Ok((stream, _)) = listener.accept() else { break };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut len = 0usize;
                loop {
                    let mut h = String::new();
                    if reader.read_line(&mut h).is_err() || h.trim().is_empty() {
                        break;
                    }
                    if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; len];
                let _ = reader.read_exact(&mut body);
                g.lock().unwrap().push(String::from_utf8_lossy(&body).into_owned());
                on_body();
                let _ = stream.try_clone().unwrap().write_all(
                    format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                );
            }
        });
        (format!("http://127.0.0.1:{port}/api/v1/lineage"), got)
    }

    /// An emit that lands while a drain holds the buffer aside must not be
    /// lost: it goes to a fresh `openlineage.ndjson`, and the drain touching
    /// only its moved-aside copy never sees it to delete it.
    #[test]
    fn an_append_during_the_drain_survives_it() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let buffer = ws.join("logs").join("openlineage.ndjson");
        let b = buffer.clone();
        let (endpoint, got) = stub_collector_touching(vec![200], move || {
            // Inside the drain window: the buffer is renamed away and this
            // append starts the fresh file a concurrent emit would write.
            crate::ndjson::append_records(&b, r#"{"eventType":"COMPLETE","n":2}"#).unwrap();
        });
        let cfg = Config { endpoint: Some(endpoint), ..Config::default() };
        buffered(ws, &[r#"{"eventType":"START","n":1}"#]);

        let out = flush(ws, &cfg);
        assert_eq!(out, FlushOutcome { sent: 1, kept: 0, rejected: 0 });
        assert_eq!(got.lock().unwrap().len(), 1);
        let left = std::fs::read_to_string(&buffer).unwrap();
        assert_eq!(left.trim(), r#"{"eventType":"COMPLETE","n":2}"#, "{left}");
    }

    /// Two drains in one workspace cannot collide: the loser's rename fails on
    /// the missing buffer and it reports having drained nothing, while the
    /// winner delivers every line exactly once.
    #[test]
    fn two_concurrent_drains_do_not_collide() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (endpoint, got) = stub_collector(vec![200, 200]);
        let cfg = Config { endpoint: Some(endpoint), ..Config::default() };
        buffered(ws, &[r#"{"eventType":"START","n":1}"#, r#"{"eventType":"COMPLETE","n":2}"#]);

        let cfg2 = cfg.clone();
        let ws2 = ws.to_path_buf();
        let other = std::thread::spawn(move || flush(&ws2, &cfg2));
        let mine = flush(ws, &cfg);
        let theirs = other.join().unwrap();

        assert_eq!(mine.sent + theirs.sent, 2, "each event delivered exactly once");
        assert_eq!(got.lock().unwrap().len(), 2, "no event is POSTed twice");
        assert_eq!(
            std::fs::read_to_string(ws.join("logs/openlineage.ndjson")).unwrap_or_default(),
            "",
        );
        assert!(
            std::fs::read_dir(ws.join("logs"))
                .unwrap()
                .all(|e| !e.unwrap().file_name().to_string_lossy().contains("sending")),
            "the drained-away file is not stranded"
        );
    }
}
