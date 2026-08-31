//! #299: how old is this file, and can this build be trusted with it.
//!
//! A workspace outlives the build that wrote it. Without a marker, a missing
//! field is ambiguous - an old default, or something a newer client wrote and
//! this build cannot see - and the dangerous half is silent. A build that does
//! not know about `parameters` reads a pipeline that declares a typed contract,
//! ignores the contract, and runs with values nobody validated. Nothing fails;
//! the run is simply not the run the file describes.
//!
//! So a document says which format it is in, and a build says which formats it
//! will accept. Too new is refused, loudly. Too old is migrated, deliberately.
//!
//! ## Version 0 is a real version
//!
//! Every pipeline written before this existed has no marker, and there are
//! working ones in the field. Absent means 0, 0 is readable, and nothing has to
//! be migrated to keep running - migration is how a file stops being ambiguous,
//! not a toll for opening it.
//!
//! ## Migration works on the raw document
//!
//! Never through [`crate::PipelineDoc`]. That struct is what the ENGINE needs -
//! it does not carry `name`, and it is not the only thing that reads these
//! files. Round-tripping through it would silently delete every key the engine
//! happens not to use, which is the exact opposite of what a migration owes the
//! person running it.

use serde_json::Value;

/// The oldest format this build reads. 0 is every file written before the
/// marker existed.
pub const MIN_READABLE: u32 = 0;
/// The newest format this build understands.
pub const MAX_READABLE: u32 = 1;
/// What migration writes.
pub const WRITABLE: u32 = 1;

/// Component properties that were renamed, and are still honoured under the old
/// name.
///
/// Shared with the #298 property check rather than listed twice: there, the old
/// name is reported as deprecated with the new one named; here, it is what a
/// migration rewrites. One table means the checker cannot call something
/// deprecated that migration does not know how to fix, or the reverse.
///
/// `(component, old name, current name)`.
pub const ALIASES: [(&str, &str, &str); 1] =
    [("snk.csv", "hasHeader", "writeHeader")];

/// A document this build will not touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TooNew {
    pub found: u32,
    pub max: u32,
}

impl std::fmt::Display for TooNew {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "this file is format version {} and this build reads up to {}. \
             Upgrade Duckle rather than running it - a newer format may carry \
             settings this build cannot see, and ignoring them would run \
             something other than what the file describes.",
            self.found, self.max
        )
    }
}

/// The declared version, or 0 for a document written before the marker existed.
///
/// A non-integer or negative marker reads as 0 rather than as an error: it is a
/// hand-edited file, and refusing to open it helps nobody. The migration will
/// stamp it correctly.
pub fn version_of(doc: &Value) -> u32 {
    doc.get("formatVersion").and_then(Value::as_u64).unwrap_or(0).min(u32::MAX as u64) as u32
}

/// Whether this build can be trusted with the document.
pub fn check(doc: &Value) -> Result<u32, TooNew> {
    let found = version_of(doc);
    match found > MAX_READABLE {
        true => Err(TooNew { found, max: MAX_READABLE }),
        false => Ok(found),
    }
}

/// The same judgement as [`check`], for a document that has already been parsed.
///
/// Both exist because the two callers genuinely differ: migration works on the
/// raw JSON so it cannot lose a key, while execution has a [`crate::PipelineDoc`]
/// and no raw document to consult.
pub fn refuse_if_too_new(found: u32) -> Result<(), TooNew> {
    match found > MAX_READABLE {
        true => Err(TooNew { found, max: MAX_READABLE }),
        false => Ok(()),
    }
}

/// The result of migrating one document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Migrated {
    pub doc: Value,
    /// What changed, in the order it changed, so a dry run reads as a plan
    /// rather than a diff someone has to interpret.
    pub changes: Vec<String>,
}

impl Migrated {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
}

/// Bring a document up to [`WRITABLE`].
///
/// Deterministic and idempotent: running it twice produces the same document
/// and reports nothing the second time. Every key the migration does not
/// understand is left exactly where it was.
pub fn migrate(doc: &Value) -> Result<Migrated, TooNew> {
    let from = check(doc)?;
    let mut out = doc.clone();
    let mut changes = Vec::new();

    // Renames first, so the stamp is only written once the document actually
    // matches the version being claimed.
    if let Some(nodes) = out.get_mut("nodes").and_then(Value::as_array_mut) {
        for node in nodes.iter_mut() {
            let node_id =
                node.get("id").and_then(Value::as_str).unwrap_or("?").to_string();
            let Some(data) = node.get_mut("data").and_then(Value::as_object_mut) else {
                continue;
            };
            let component =
                data.get("componentId").and_then(Value::as_str).unwrap_or("").to_string();
            let Some(props) = data.get_mut("properties").and_then(Value::as_object_mut) else {
                continue;
            };
            for (c, old, new) in ALIASES {
                if c != component || !props.contains_key(old) {
                    continue;
                }
                // The current name already present means the builder is
                // already using it; the old one is dead weight, and moving it
                // over the live value would change what runs.
                if props.contains_key(new) {
                    props.remove(old);
                    changes.push(format!(
                        "{node_id}: dropped {component}.{old}, superseded by {new}"
                    ));
                    continue;
                }
                if let Some(value) = props.remove(old) {
                    props.insert(new.to_string(), value);
                    changes.push(format!("{node_id}: renamed {component}.{old} to {new}"));
                }
            }
        }
    }

    if from != WRITABLE {
        if let Some(object) = out.as_object_mut() {
            object.insert("formatVersion".into(), Value::from(WRITABLE));
        }
        changes.push(format!("stamped formatVersion {WRITABLE} (was {from})"));
    }
    Ok(Migrated { doc: out, changes })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn csv_sink(props: Value) -> Value {
        serde_json::json!({
            "name": "keep me",
            "nodes": [{ "id": "out", "type": "sink", "position": { "x": 1, "y": 2 },
                        "data": { "label": "Out", "componentId": "snk.csv",
                                  "properties": props } }],
            "edges": [],
            "x-authored-by": "some other tool"
        })
    }

    #[test]
    fn a_file_with_no_marker_is_version_zero_and_still_readable() {
        let doc = serde_json::json!({ "nodes": [], "edges": [] });
        assert_eq!(version_of(&doc), 0);
        assert_eq!(check(&doc), Ok(0));
    }

    #[test]
    fn a_newer_format_is_refused_rather_than_half_read() {
        let doc = serde_json::json!({ "formatVersion": MAX_READABLE + 1, "nodes": [] });
        let e = check(&doc).unwrap_err();
        assert_eq!(e.found, MAX_READABLE + 1);
        // The message has to say what to do, because the reader's instinct is
        // to delete the field and try again.
        assert!(e.to_string().contains("Upgrade Duckle"), "{e}");
    }

    #[test]
    fn migration_never_loses_a_key_it_does_not_understand() {
        let before = csv_sink(serde_json::json!({ "path": "o.csv" }));
        let after = migrate(&before).unwrap().doc;
        assert_eq!(after["name"], "keep me");
        assert_eq!(after["x-authored-by"], "some other tool");
        assert_eq!(after["nodes"][0]["position"], serde_json::json!({ "x": 1, "y": 2 }));
        assert_eq!(after["nodes"][0]["data"]["label"], "Out");
    }

    #[test]
    fn a_deprecated_alias_is_renamed_to_the_name_the_manifest_declares() {
        let before = csv_sink(serde_json::json!({ "path": "o.csv", "hasHeader": false }));
        let m = migrate(&before).unwrap();
        let props = &m.doc["nodes"][0]["data"]["properties"];
        assert_eq!(props["writeHeader"], false, "the VALUE must survive the rename");
        assert!(props.get("hasHeader").is_none());
        assert!(m.changes.iter().any(|c| c.contains("renamed snk.csv.hasHeader")), "{:?}", m.changes);
    }

    #[test]
    fn an_alias_beside_the_current_name_is_dropped_not_moved_over_it() {
        // writeHeader already wins in the builder, so moving hasHeader on top
        // of it would change what the pipeline does. A migration must not.
        let before =
            csv_sink(serde_json::json!({ "path": "o.csv", "hasHeader": false, "writeHeader": true }));
        let m = migrate(&before).unwrap();
        let props = &m.doc["nodes"][0]["data"]["properties"];
        assert_eq!(props["writeHeader"], true, "the live value must not change");
        assert!(props.get("hasHeader").is_none());
    }

    #[test]
    fn the_engine_struct_really_would_have_lost_data() {
        // The reason migration works on the raw document. If this ever stops
        // being true the constraint can be relaxed - but it must be measured,
        // not assumed, because the failure is silent deletion of a user's file.
        let before = csv_sink(serde_json::json!({ "path": "o.csv" }));
        let through: crate::PipelineDoc =
            serde_json::from_value(before.clone()).expect("parses");
        let after = serde_json::to_value(&through).expect("serialises");
        assert!(before.get("name").is_some());
        assert!(after.get("name").is_none(), "PipelineDoc now carries name; re-check migrate()");
        assert!(after.get("x-authored-by").is_none());
    }

    #[test]
    fn the_version_refusal_comes_before_every_other_check() {
        // Found by running it: the refusal was placed after the "is the engine
        // installed" check, so on a machine without DuckDB a document from a
        // newer build reported the wrong problem entirely, and on a machine
        // with one it reported nothing until much later.
        let engine = crate::DuckdbEngine::new(std::path::PathBuf::from(
            "no-such-duckdb-binary-anywhere",
        ));
        let doc: crate::PipelineDoc = serde_json::from_value(serde_json::json!({
            "formatVersion": MAX_READABLE + 1, "nodes": [], "edges": []
        }))
        .expect("parses");
        let result = engine.execute_pipeline_named(&doc, "t");
        let error = result.error.unwrap_or_default();
        assert!(error.contains("format version"), "reported instead: {error}");
    }

    #[test]
    fn migration_is_idempotent() {
        let before = csv_sink(serde_json::json!({ "path": "o.csv", "hasHeader": false }));
        let once = migrate(&before).unwrap();
        assert!(!once.is_empty());
        let twice = migrate(&once.doc).unwrap();
        assert_eq!(twice.doc, once.doc, "a second run must change nothing");
        assert!(twice.is_empty(), "and must say so: {:?}", twice.changes);
    }

    #[test]
    fn migration_stamps_the_version_it_wrote() {
        let m = migrate(&serde_json::json!({ "nodes": [], "edges": [] })).unwrap();
        assert_eq!(m.doc["formatVersion"], WRITABLE);
        assert!(m.changes.iter().any(|c| c.contains("stamped formatVersion")));
    }

    #[test]
    fn a_future_document_is_refused_by_migration_too() {
        let doc = serde_json::json!({ "formatVersion": 99, "nodes": [] });
        assert!(migrate(&doc).is_err(), "migrating down is not a thing this can do");
    }

    #[test]
    fn a_nonsense_marker_reads_as_unversioned_rather_than_failing() {
        // Hand-edited files exist. Refusing to open one helps nobody, and the
        // migration will stamp it correctly.
        for junk in [serde_json::json!("seven"), serde_json::json!(-3), serde_json::json!(null)] {
            let doc = serde_json::json!({ "formatVersion": junk, "nodes": [] });
            assert_eq!(version_of(&doc), 0, "{junk:?}");
        }
    }
}
