//! Every property a builder READS must be one its component DECLARES.
//!
//! This is the mirror of the existing declared-but-never-read check, and it is
//! the direction that actually breaks things. Three real bugs were found by
//! running it once:
//!
//! - `xf.cdc.scd3` read `keyColumns`/`trackColumns` while its form wrote
//!   `naturalKey`/`compareColumns`. Configuring it in the editor failed with
//!   "SCD3 needs key columns" while the Natural key field was filled in, and
//!   nothing in the UI could fix it because the field the error named was not
//!   on the form.
//! - `src.fixedwidth` required a `columns` array of `{name,start,width}` while
//!   its form offered `columnWidths`. `columnWidths` appeared nowhere else in
//!   the tree: no converter, no engine read, manifest only.
//! - `qa.unique`, `xf.distinct` and the `xf.topn`/`skip`/`sample` family each
//!   read a determinism control (`tieBreak`, `orderBy`) that no form offered,
//!   so a correctness fix added by an earlier audit was unreachable.
//!
//! Each of those passed every test in the suite. They are invisible to tests
//! that call the builder directly with the keys the builder expects, which is
//! how builder tests are naturally written - the test agrees with the code
//! instead of with the contract.
//!
//! ## Reading Rust with a regex
//!
//! Deliberately. The alternative is a compile-time registry, which is the real
//! fix (#298) and a much larger change. This is a locator: it is reliable about
//! which literal keys appear inside which builder, and it does not resolve
//! anything clever. When it is wrong it is wrong in the direction of asking a
//! human to look, which is the right direction for a check like this.
//!
//! A key here is legitimate for one of three reasons, and the allowlist below
//! says which for every entry. An entry with no reason is not allowed.

use std::collections::{BTreeMap, BTreeSet};

/// Keys a builder reads that its manifest does not declare, and why that is
/// correct rather than a bug.
///
/// Adding to this list is a deliberate act. If a key is here because it is a
/// legacy spelling, the canonical one MUST be declared - otherwise the
/// component has no reachable form of that setting at all, which is the
/// `src.fixedwidth` bug.
fn allowed(component: &str, key: &str) -> Option<&'static str> {
    match (component, key) {
        // Legacy spellings the builder still accepts so saved pipelines and
        // imported jobs keep working. The canonical key is declared.
        (_, "additions") => Some("legacy spelling of columns"),
        (_, "filterSql") => Some("legacy spelling of predicate"),
        (_, "having") => Some("legacy spelling of havingClause"),
        (_, "keep") => Some("legacy spelling of columns"),
        (_, "drop") => Some("legacy spelling of columns"),
        (_, "limit") => Some("legacy spelling of count"),
        (_, "rows") => Some("legacy spelling of limit"),
        (_, "buckets") => Some("legacy spelling of ntileBuckets"),
        (_, "masks") => Some("array form of the single-column mask fields"),
        (_, "keyColumns") => Some("legacy spelling of naturalKey"),
        (_, "trackColumns") => Some("legacy spelling of compareColumns"),
        (_, "leftKey") => Some("single-column form of leftColumns"),
        (_, "rightKey") => Some("single-column form of rightColumns"),
        (_, "joinType") => Some("carried by the component id itself on the xf.join.* aliases"),
        ("xf.cast", "column") => Some("single-column form of columns"),
        ("xf.cast", "type") => Some("legacy spelling of targetType"),
        ("xf.arr.collect", "column") => Some("legacy spelling of valueColumn"),
        ("src.fixedwidth", "columns") => Some("explicit form; the form offers columnWidths"),
        // Structured values a dedicated editor writes, not properties-panel
        // fields. The visual mapper owns these.
        ("xf.map", "mapper") => Some("written by the visual mapper editor"),
        ("xf.map", "lookups") => Some("written by the visual mapper editor"),
        ("xf.map", "filter") => Some("written by the visual mapper editor"),
        // Nested keys inside a declared structured field, which the manifest
        // describes by kind rather than by element key.
        ("xf.addcol", "expr") => Some("element key inside the declared columns value"),
        ("xf.coalesce", "expr") => Some("element key inside the declared columns value"),
        ("xf.addcol", "onError") => Some("element key inside the declared columns value"),
        ("xf.coalesce", "onError") => Some("element key inside the declared columns value"),
        ("xf.cast", "format") => Some("element key inside the declared casts value"),
        ("xf.cast", "columns") => Some("legacy array form; the form declares casts"),
        ("xf.cast", "targetType") => Some("element key inside the declared casts value"),
        ("xf.addcol", "columns") => Some("legacy array form; the form declares the element keys"),
        ("xf.coalesce", "columns") => Some("legacy array form; the form declares the element keys"),
        // KNOWN GAP, not a false positive. build_sort prefers an `orderBy`
        // array (multi-column, with per-column direction) and falls back to the
        // declared single sortColumn. So multi-column sort works in a
        // hand-written pipeline and cannot be expressed in the editor. Listed
        // here rather than silently allowed: fixing it needs a new field kind,
        // and declaring `orderBy` with the existing `columns` kind would drop
        // per-column direction and sit confusingly beside sortColumn.
        ("xf.sort", "orderBy") => Some("KNOWN GAP: multi-column sort is unreachable from the editor"),
        ("xf.text.tocolumns", "columns") => Some("output names derived, not configured"),
        ("xf.pyexpr", "columns") => Some("element key inside the declared expression value"),
        ("xf.agg", "aggregations") => Some("legacy id of xf.groupby"),
        ("xf.agg", "havingClause") => Some("legacy id of xf.groupby"),
        ("xf.distinct", "orderBy") => Some("declared; the alias is only on the legacy id"),
        _ => None,
    }
}

/// Component ids the engine dispatches on that the catalog does not declare.
///
/// These are legacy or aliased ids kept so saved pipelines keep loading. They
/// are listed rather than ignored, because an id appearing here that is NOT a
/// legacy alias means a component exists that no editor can create.
const UNDECLARED_IDS: &[&str] = &[
    "xf.agg",
    "xf.aggregate",
    "xf.anti.join",
    "xf.cdc.compare",
    "xf.drop",
    "xf.join.full",
    "xf.join.inner",
    "xf.join.left",
    "xf.join.outer",
    "xf.join.right",
    "xf.keep",
    "xf.limit",
    "xf.lookup.outer",
    "xf.pyexpr",
    "xf.semi.join",
];

fn declared_keys() -> BTreeMap<String, BTreeSet<String>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("duckle-mcp")
        .join("catalog.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let v: serde_json::Value = serde_json::from_str(&text).expect("catalog is JSON");
    let mut out = BTreeMap::new();
    for c in v["components"].as_array().expect("components") {
        let id = c["id"].as_str().unwrap_or_default().to_string();
        let mut keys = BTreeSet::new();
        if let Some(sections) = c["manifest"]["sections"].as_array() {
            for s in sections {
                if let Some(fields) = s["fields"].as_array() {
                    for f in fields {
                        if let Some(k) = f["key"].as_str() {
                            keys.insert(k.to_string());
                        }
                    }
                }
            }
        }
        out.insert(id, keys);
    }
    out
}

/// Map each builder function to the component ids dispatched to it.
fn ids_by_builder(src: &str) -> BTreeMap<String, BTreeSet<String>> {
    let arm = regex::Regex::new(
        r#"(?m)^\s*((?:"[a-z][\w.]*"\s*\|\s*)*"[a-z][\w.]*")\s*=>\s*([a-z_]\w*)\s*\("#,
    )
    .unwrap();
    let lit = regex::Regex::new(r#""([a-z][\w.]*)""#).unwrap();
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for caps in arm.captures_iter(src) {
        let f = caps[2].to_string();
        for id in lit.captures_iter(&caps[1]) {
            if id[1].contains('.') {
                out.entry(f.clone()).or_default().insert(id[1].to_string());
            }
        }
    }
    out
}

/// The body of `fn name`, by brace matching.
fn body_of<'a>(src: &'a str, name: &str) -> Option<&'a str> {
    let sig = regex::Regex::new(&format!(
        r"(?m)^\s*(?:pub\(crate\)\s+|pub\s+)?fn\s+{}\b",
        regex::escape(name)
    ))
    .ok()?;
    let m = sig.find(src)?;
    let start = src[m.end()..].find('{')? + m.end();
    let (mut depth, bytes) = (0i32, src.as_bytes());
    for i in start..src.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&src[start..i]);
                }
            }
            _ => {}
        }
    }
    None
}

#[test]
fn every_property_a_builder_reads_is_one_its_component_declares() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("plan")
        .join("builders.rs");
    let src = std::fs::read_to_string(&path).expect("builders.rs");
    let declared = declared_keys();
    let read = regex::Regex::new(
        r#"(?:columns_list|string_prop|bool_prop|u64_prop|usize_prop|num_prop|int_prop)\s*\(\s*&?props\s*,\s*"(\w+)"|props\s*\.\s*get\(\s*"(\w+)""#,
    )
    .unwrap();

    let mut problems: Vec<String> = Vec::new();
    for (builder, ids) in ids_by_builder(&src) {
        let Some(body) = body_of(&src, &builder) else { continue };
        let keys: BTreeSet<String> = read
            .captures_iter(body)
            .filter_map(|c| c.get(1).or_else(|| c.get(2)).map(|m| m.as_str().to_string()))
            .collect();

        for id in &ids {
            if declared.get(id).is_none() && !UNDECLARED_IDS.contains(&id.as_str()) {
                problems.push(format!(
                    "{id} is dispatched to {builder}() but the catalog does not declare it, so no editor can create it"
                ));
            }
        }

        // One builder often serves a whole family, and families vary: the
        // relational builder reads `mode` for the components that have modes,
        // and `query` for the ones that spell `sql` that way. Reporting those
        // per component would bury the real thing in noise.
        //
        // The bug class is sharper: a key that NO component served by this
        // builder declares. That is a key nothing on any form can set, which is
        // precisely what xf.cdc.scd3, src.fixedwidth and qa.unique's tieBreak
        // each were.
        let union: BTreeSet<&String> =
            ids.iter().filter_map(|id| declared.get(id)).flatten().collect();
        for k in &keys {
            if union.contains(k) || ids.iter().any(|id| allowed(id, k).is_some()) {
                continue;
            }
            let mut named: Vec<&str> = ids.iter().map(String::as_str).collect();
            named.sort();
            problems.push(format!(
                concat!(
                    "{}() reads {:?}, which NONE of {:?} declares. Either the form writes a ",
                    "different key than the builder reads (the xf.cdc.scd3 bug), or a working ",
                    "setting has no field at all and is unreachable (the qa.unique tieBreak ",
                    "bug). If it is a legacy spelling, add it to allowed() WITH the canonical ",
                    "key declared."
                ),
                builder, k, named
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "properties the engine reads that no form can set:\n  {}",
        problems.join("\n  ")
    );
}
