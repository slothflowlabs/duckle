//! #298: a property nobody reads must not disappear quietly.
//!
//! The failure mode from #198: the SDK emitted `count`, the runtime wanted
//! `limit`, the unknown key was ignored, validation passed, and the run used
//! the default. Nothing anywhere said no. That is the worst possible shape for
//! a bug - the pipeline runs, the numbers are wrong, and there is no signal to
//! chase.
//!
//! The declared manifests already describe what each component takes; they are
//! exported to `catalog.json` and drive the desktop forms, the web editor and
//! MCP. This checks a pipeline against them, so the same declaration that draws
//! the form also decides what the engine will accept.
//!
//! ## What it will not do
//!
//! **It will not judge a component the catalog does not list.** The engine's
//! dispatch accepts aliases the exported catalog has no entry for -
//! `xf.join.inner` is one, and it works - so treating "absent from the catalog"
//! as "invalid" would reject pipelines that run correctly today. Those are
//! reported as a catalog gap and never fail anything.
//!
//! **It will not reject `x-` keys.** #298 asks for a namespace third-party
//! metadata can round-trip through, and this is it: preserved, never
//! interpreted.
//!
//! ## Where strictness lives
//!
//! `validate` fails on these, because a lint that cannot fail is one people
//! stop reading. Execution warns, and only fails under
//! `DUCKLE_STRICT_PROPERTIES=1` - an existing pipeline that has quietly carried
//! a dead property for a year should start telling its operator, not stop
//! running the moment they upgrade.

use crate::PipelineDoc;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

/// The same document the desktop forms, the web editor and MCP are built from,
/// so there is one declaration rather than four that agree until they do not.
const CATALOG: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../duckle-mcp/catalog.json"));

/// Keys a builder reads that the component's manifest does not declare.
///
/// Each is a real gap between the two, not an exception to the rule: the engine
/// honours these, so rejecting them would break working pipelines, and dropping
/// the check for the whole component would hide everything else. They are
/// listed here with the line that reads them so the list can be worked off
/// rather than grown.
const ACCEPTED: [(&str, &str, &str); 2] = [
    ("xf.groupby", "materialize", "read at plan/mod.rs:6440 for every component"),
    ("code.sql", "materialize", "read at plan/mod.rs:6440 for every component"),
];

/// One property problem, in the shape #298 asked for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Finding {
    /// Stable: `unknown_component_property` or `component_not_in_catalog`.
    pub code: String,
    pub node: String,
    pub component: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<String>,
    /// The declared name this was probably meant to be. Absent when nothing is
    /// close enough - a wrong guess sends the reader off to check a name that
    /// was never the point.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
    pub message: String,
    /// Whether this alone should fail a strict check. A catalog gap is
    /// information about the catalog, not about the pipeline.
    pub fails: bool,
}

/// `component id -> every key its manifest declares, at any nesting depth`.
///
/// Deliberately permissive about where a key is found: manifests nest fields
/// inside sections, conditional groups and list item schemas, and a key missed
/// by the extraction becomes a false rejection of a valid pipeline. Collecting
/// every `key` under the manifest cannot miss one.
fn declared() -> &'static BTreeMap<String, BTreeSet<String>> {
    static ONCE: OnceLock<BTreeMap<String, BTreeSet<String>>> = OnceLock::new();
    ONCE.get_or_init(|| {
        fn collect(node: &serde_json::Value, out: &mut BTreeSet<String>) {
            match node {
                serde_json::Value::Object(map) => {
                    if let Some(k) = map.get("key").and_then(|k| k.as_str()) {
                        out.insert(k.to_string());
                    }
                    for v in map.values() {
                        collect(v, out);
                    }
                }
                serde_json::Value::Array(items) => {
                    for v in items {
                        collect(v, out);
                    }
                }
                _ => {}
            }
        }
        let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let Ok(doc) = serde_json::from_str::<serde_json::Value>(CATALOG) else { return out };
        let Some(components) = doc.get("components").and_then(|c| c.as_array()) else {
            return out;
        };
        for component in components {
            let Some(id) = component.get("id").and_then(|i| i.as_str()) else { continue };
            let mut keys = BTreeSet::new();
            if let Some(manifest) = component.get("manifest") {
                collect(manifest, &mut keys);
            }
            for (c, key, _) in ACCEPTED {
                if c == id {
                    keys.insert(key.to_string());
                }
            }
            // #299 owns the renames. Accepted here so a pipeline still running
            // on the old name is not failed for it - it is reported as
            // deprecated below, with the name migration will move it to.
            for (c, old, _) in crate::format::ALIASES {
                if c == id {
                    keys.insert(old.to_string());
                }
            }
            out.insert(id.to_string(), keys);
        }
        out
    })
}

/// The closest declared name, when one is close enough to be worth saying.
///
/// Levenshtein rather than a length comparison: `count` and `limit` are the same
/// length and share nothing, while `has_header` and `hasHeader` differ in length
/// and are obviously the same intent.
fn nearest<'a>(name: &str, candidates: impl Iterator<Item = &'a String>) -> Option<String> {
    fn distance(a: &str, b: &str) -> usize {
        let a: Vec<char> = a.to_lowercase().chars().collect();
        let b: Vec<char> = b.to_lowercase().chars().collect();
        let mut prev: Vec<usize> = (0..=b.len()).collect();
        let mut row = vec![0usize; b.len() + 1];
        for (i, ca) in a.iter().enumerate() {
            row[0] = i + 1;
            for (j, cb) in b.iter().enumerate() {
                let cost = usize::from(ca != cb);
                row[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(row[j] + 1);
            }
            std::mem::swap(&mut prev, &mut row);
        }
        prev[b.len()]
    }
    // A third of the name, at least one edit. Looser than this starts
    // suggesting `path` for `topic`, which is worse than saying nothing.
    let budget = (name.chars().count() / 3).max(1);
    candidates
        .map(|c| (distance(name, c), c))
        .filter(|(d, _)| *d <= budget)
        .min_by_key(|(d, c)| (*d, c.len()))
        .map(|(_, c)| c.clone())
}

/// Every property problem in the document. Empty means every supplied property
/// is one its component declares.
pub fn check(doc: &PipelineDoc) -> Vec<Finding> {
    let declared = declared();
    let mut findings = Vec::new();
    for node in &doc.nodes {
        let Some(component) = node.data.component_id.as_deref() else { continue };
        let Some(props) = node.data.properties.as_ref().and_then(|p| p.as_object()) else {
            continue;
        };
        let Some(known) = declared.get(component) else {
            // The engine dispatch accepts ids the exported catalog omits, so
            // this says nothing about the pipeline - only that the two are out
            // of step. Reported once per node, never fatal.
            findings.push(Finding {
                code: "component_not_in_catalog".into(),
                node: node.id.clone(),
                component: component.to_string(),
                property: None,
                suggestion: None,
                message: format!(
                    "{component} is not in the exported catalog, so its properties cannot be \
                     checked. The engine may still accept it."
                ),
                fails: false,
            });
            continue;
        };
        for key in props.keys() {
            // A renamed property still works, and saying nothing about it is
            // how a workspace stays on a name that will eventually stop being
            // honoured. Reported with the current name and never fatal - the
            // pipeline is correct today, and #299's migration rewrites it.
            if let Some((_, _, current)) =
                crate::format::ALIASES.iter().find(|(c, o, _)| *c == component && o == key)
            {
                findings.push(Finding {
                    code: "deprecated_component_property".into(),
                    node: node.id.clone(),
                    component: component.to_string(),
                    property: Some(key.clone()),
                    suggestion: Some(current.to_string()),
                    message: format!(
                        "{component} still reads {key}, but it is now called {current}. `duckle-runner migrate` renames it."
                    ),
                    fails: false,
                });
                continue;
            }
            // The namespace #298 reserves for third-party metadata: it round
            // trips and never reaches a builder.
            if key.starts_with("x-") || known.contains(key) {
                continue;
            }
            let suggestion = nearest(key, known.iter());
            findings.push(Finding {
                code: "unknown_component_property".into(),
                node: node.id.clone(),
                component: component.to_string(),
                property: Some(key.clone()),
                message: match &suggestion {
                    Some(s) => format!("{component} does not read {key}. Did you mean {s}?"),
                    None => format!(
                        "{component} does not read {key}, so setting it changes nothing"
                    ),
                },
                suggestion,
                fails: true,
            });
        }
    }
    findings
}

/// The accepted property names, per component, as a document.
///
/// #298 asks for this so agents and editors do not have to scrape source or
/// re-derive the contract from a 2 MB manifest dump. It is the SAME map the
/// checker uses, so what this promises and what the engine accepts cannot
/// diverge - publishing a second, separately built copy is how they would.
pub fn schema_json() -> serde_json::Value {
    let components: serde_json::Map<String, serde_json::Value> = declared()
        .iter()
        .map(|(id, keys)| {
            (id.clone(), serde_json::json!({ "properties": keys.iter().collect::<Vec<_>>() }))
        })
        .collect();
    serde_json::json!({
        "schemaVersion": 1,
        "componentCount": components.len(),
        "components": components,
        "extensionPrefix": "x-",
    })
}

#[cfg(test)]
fn doc_for_test(component: &str, key: &str) -> PipelineDoc {
    serde_json::from_value(serde_json::json!({
        "name": "t",
        "nodes": [{ "id": "n1", "type": "source", "position": { "x": 0, "y": 0 },
                    "data": { "label": "n", "componentId": component,
                              "properties": { key: "v" } } }],
        "edges": []
    }))
    .expect("fixture parses")
}

/// Whether execution should refuse a pipeline with these findings.
///
/// Off by default: a pipeline that has quietly carried a dead property for a
/// year should start telling its operator, not stop running the day they
/// upgrade. `validate` does not consult this - a lint that cannot fail is one
/// people stop reading.
pub fn strict_execution() -> bool {
    std::env::var("DUCKLE_STRICT_PROPERTIES").map(|v| v == "1").unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(component: &str, props: serde_json::Value) -> PipelineDoc {
        serde_json::from_value(serde_json::json!({
            "name": "t",
            "nodes": [{ "id": "n1", "type": "transform", "position": { "x": 0, "y": 0 },
                        "data": { "label": "n", "componentId": component, "properties": props } }],
            "edges": []
        }))
        .expect("fixture parses")
    }

    #[test]
    fn the_catalog_actually_loaded() {
        // Every assertion below is vacuous if it did not.
        assert!(declared().len() > 300, "only {} components", declared().len());
        assert!(declared()["src.csv"].contains("path"));
    }

    #[test]
    fn the_198_shape_is_reported_instead_of_silently_defaulting() {
        // xf.topn reads `count`. Anyone coming from SQL writes `limit`, and
        // before this the row cap simply did not apply and the run looked fine.
        let f = check(&doc("xf.topn", serde_json::json!({ "limit": 10 })));
        assert_eq!(f.len(), 1, "{f:?}");
        assert_eq!(f[0].code, "unknown_component_property");
        assert_eq!(f[0].node, "n1");
        assert_eq!(f[0].component, "xf.topn");
        assert_eq!(f[0].property.as_deref(), Some("limit"));
        assert!(f[0].fails);
    }

    #[test]
    fn a_near_miss_names_what_was_meant() {
        let f = check(&doc("src.csv", serde_json::json!({ "path": "a.csv", "hasHeaders": true })));
        assert_eq!(f.len(), 1, "{f:?}");
        assert_eq!(f[0].suggestion.as_deref(), Some("hasHeader"));
        assert!(f[0].message.contains("Did you mean hasHeader?"));
    }

    #[test]
    fn a_declared_property_is_accepted() {
        assert!(check(&doc("xf.topn", serde_json::json!({ "count": 10 }))).is_empty());
    }

    #[test]
    fn a_key_the_builder_reads_but_the_manifest_omits_is_not_rejected() {
        // Rejecting these would break pipelines the engine runs correctly.
        assert!(
            check(&doc("xf.groupby", serde_json::json!({ "materialize": true }))).is_empty()
        );
    }

    #[test]
    fn a_renamed_property_is_reported_as_deprecated_and_never_fails() {
        let f = check(&doc("snk.csv", serde_json::json!({ "path": "o.csv", "hasHeader": true })));
        assert_eq!(f.len(), 1, "{f:?}");
        assert_eq!(f[0].code, "deprecated_component_property");
        assert_eq!(f[0].suggestion.as_deref(), Some("writeHeader"));
        assert!(!f[0].fails, "the pipeline is correct today");
    }

    #[test]
    fn an_x_prefixed_key_round_trips_untouched() {
        let f = check(&doc(
            "xf.topn",
            serde_json::json!({ "count": 5, "x-authored-by": "some tool" }),
        ));
        assert!(f.is_empty(), "{f:?}");
    }

    #[test]
    fn a_component_the_catalog_omits_is_a_catalog_gap_not_a_pipeline_error() {
        // xf.join.inner is a real alias in the engine dispatch and absent from
        // the exported catalog.
        let f = check(&doc("xf.join.inner", serde_json::json!({ "leftKey": "id" })));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].code, "component_not_in_catalog");
        assert!(!f[0].fails, "a working pipeline must not be failed for this");
    }

    #[test]
    fn nothing_close_enough_gets_no_suggestion() {
        let f = check(&doc("xf.topn", serde_json::json!({ "zzzzzzqqq": 1 })));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].suggestion, None, "a wrong guess is worse than none");
        assert!(f[0].message.contains("changes nothing"));
    }

    #[test]
    fn every_problem_is_reported_not_just_the_first() {
        let f = check(&doc("xf.topn", serde_json::json!({ "limit": 1, "skipp": 2 })));
        assert_eq!(f.len(), 2, "{f:?}");
    }

    #[test]
    fn the_published_schema_is_the_one_the_checker_enforces() {
        let doc = schema_json();
        let listed = doc["components"]["src.csv"]["properties"]
            .as_array()
            .expect("src.csv is published")
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<BTreeSet<_>>();
        // Anything the schema publishes must be accepted, and anything it omits
        // must be refused - otherwise it is documentation rather than a
        // contract.
        for key in &listed {
            let d = super::doc_for_test("src.csv", key);
            assert!(check(&d).is_empty(), "{key} is published but refused");
        }
        assert!(!listed.contains("nosuchkey"));
        let d = super::doc_for_test("src.csv", "nosuchkey");
        assert!(!check(&d).is_empty(), "an unpublished key must be refused");
    }

    #[test]
    fn the_shipped_examples_have_no_dead_properties() {
        // The regression gate. These are the pipelines users copy from, so a
        // dead property here is one that spreads.
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
        let mut offenders: Vec<String> = Vec::new();
        let mut checked = 0usize;
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else { continue };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("json") {
                    continue;
                }
                let Ok(text) = std::fs::read_to_string(&path) else { continue };
                let Ok(parsed) = serde_json::from_str::<PipelineDoc>(&text) else { continue };
                checked += 1;
                for f in check(&parsed).into_iter().filter(|f| f.fails) {
                    offenders.push(format!(
                        "{}: {} {}",
                        path.file_name().unwrap_or_default().to_string_lossy(),
                        f.component,
                        f.property.unwrap_or_default()
                    ));
                }
            }
        }
        assert!(checked > 0, "found no example pipelines to check");
        assert!(offenders.is_empty(), "dead properties in shipped examples: {offenders:#?}");
    }
}
