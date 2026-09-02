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
        use std::io::Write;
        if let Ok(mut f) =
            std::fs::OpenOptions::new().create(true).append(true).open(dir.join("openlineage.ndjson"))
        {
            // One syscall. `writeln!` issues a write per format piece, and
            // O_APPEND makes each write atomic but not the pair - two runs in
            // one workspace interleave and the file stops being NDJSON, which
            // is the durability this whole path rests on.
            let _ = f.write_all(format!("{line}\n").as_bytes());
        }
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
    let agent = crate::tls::http_agent_with(&crate::tls::HttpTransport {
        read_timeout_secs: Some(cfg.timeout_secs),
        connect_timeout_secs: Some(cfg.timeout_secs),
        ..Default::default()
    });
    // One attempt. A retry on the path of a run finishing buys little - the
    // event is already on disk - and costs another timeout, twice, every time
    // a collector is down.
    if let Err(e) = agent.post(endpoint).set("Content-Type", "application/json").send_string(&line)
    {
        eprintln!("duckle: lineage export to {endpoint} failed, event kept in logs/openlineage.ndjson: {e}");
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
}
