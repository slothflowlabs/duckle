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
        // The `orderBy` KNOWN GAP was here. It is closed: xf.sort declares
        // orderBy with the `sort-keys` kind and the editor writes the ordered
        // array. What is listed now is the single-key form orderBy replaced -
        // still read by build_sort's fallback for every pipeline saved before
        // the change, for the desktop assistant's prompt and for the Talend
        // importer, and deliberately not drawn beside the list that supersedes
        // it. Not an ALIASES entry either: an alias is a pure key rename, and
        // sortColumn -> orderBy is a shape change that would leave build_sort a
        // bare string and drop the ORDER BY in silence.
        ("xf.sort", "sortColumn") => Some("legacy single-key form; the form declares orderBy"),
        ("xf.sort", "direction") => Some("legacy single-key form; the form declares orderBy"),
        ("xf.sort", "nullsLast") => Some("legacy single-key form; the form declares orderBy"),
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

/// Settings the Properties Panel writes on ANY node, declared by no manifest.
/// Mirrors `props::UNIVERSAL`, which an integration test cannot see.
const UNIVERSAL: &[&str] = &[
    "materialize",
    "materializePath",
    "cache",
    "retryAttempts",
    "retryBackoffMs",
    "memoryLimitMb",
    "continueOnFailure",
    "logRowCount",
    "sqlOverride",
    "contracts",
];

/// The other half of the engine, which the test above cannot see.
///
/// `every_property_a_builder_reads_is_one_its_component_declares` reads
/// `plan/builders.rs` and matches `"id" => builder_fn(` dispatch arms. Most
/// SOURCES are not built that way: they are inline `} else if
/// matches!(component_id, ...) {` arms inside `plan/mod.rs` that build a spec
/// rather than returning SQL. That whole half was unchecked, which is how
/// `src.graphql` shipped requiring a `query` prop no form declared - the
/// component could not be configured at all, and the suite stayed green.
///
/// This looks only for the sharpest shape: a prop read in a chain ending in
/// `.ok_or_else`, which is the engine saying the node REFUSES TO PLAN without
/// it. If nothing declares that name, no form can produce a working node.
/// Optional props read in these arms are a real question too, but a much
/// noisier one - alternate spellings, element keys inside structured values -
/// and are left to the builders test's union rule rather than guessed at here.
///
/// Validated against history: run against the catalog from before `src.graphql`
/// was fixed it reports `query`, and against the one after it does not.
mod plan_arms {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    fn predicate_ids(src: &str) -> BTreeMap<String, BTreeSet<String>> {
        let sig = regex::Regex::new(r"(?s)pub fn (\w+)\(component_id: &str\) -> bool \{(.*?)\n\}")
            .unwrap();
        let lit = regex::Regex::new(r#""([a-z][\w.]*)""#).unwrap();
        sig.captures_iter(src)
            .map(|c| {
                let ids: BTreeSet<String> =
                    lit.captures_iter(&c[2]).map(|m| m[1].to_string()).collect();
                (c[1].to_string(), ids)
            })
            .collect()
    }

    /// Line indices where a top-level arm of the component if/else chain opens.
    fn arm_starts(lines: &[&str]) -> Vec<usize> {
        lines
            .iter()
            .enumerate()
            .filter(|(_, l)| {
                l.starts_with("    } else if ")
                    || l.starts_with("    if component_id == ")
                    || l.starts_with("    if matches!(")
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// The ids an arm's condition names, and the line its body opens on.
    fn ids_of(
        lines: &[&str],
        start: usize,
        preds: &BTreeMap<String, BTreeSet<String>>,
    ) -> (BTreeSet<String>, usize) {
        let lit = regex::Regex::new(r#""([a-z][\w.]*\.[\w.]+)""#).unwrap();
        let (mut i, mut depth, mut cond) = (start, 0i32, String::new());
        while i < lines.len() {
            cond.push_str(lines[i]);
            cond.push('\n');
            depth += lines[i].matches('(').count() as i32;
            depth -= lines[i].matches(')').count() as i32;
            if lines[i].trim_end().ends_with('{') && depth <= 0 {
                break;
            }
            i += 1;
        }
        let mut ids: BTreeSet<String> =
            lit.captures_iter(&cond).map(|c| c[1].to_string()).collect();
        for (name, pid) in preds {
            if cond.contains(&format!("{name}(component_id)")) {
                ids.extend(pid.iter().cloned());
            }
        }
        (ids, i)
    }

    #[test]
    fn every_required_property_a_source_arm_reads_is_one_its_component_declares() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("plan")
            .join("mod.rs");
        let src = std::fs::read_to_string(&path).expect("plan/mod.rs");
        let lines: Vec<&str> = src.lines().collect();
        let declared = declared_keys();
        let preds = predicate_ids(&src);
        let starts = arm_starts(&lines);
        let read = regex::Regex::new(r#"string_prop\(\s*&?props\s*,\s*"(\w+)"\s*\)"#).unwrap();

        let mut problems: Vec<String> = Vec::new();
        for (n, &start) in starts.iter().enumerate() {
            let (ids, open) = ids_of(&lines, start, &preds);
            if ids.is_empty() || !ids.iter().any(|i| declared.contains_key(i)) {
                continue;
            }
            let end = starts.get(n + 1).copied().unwrap_or(lines.len());
            let body = lines[open..end].join("\n");
            let union: BTreeSet<&String> =
                ids.iter().filter_map(|id| declared.get(id)).flatten().collect();

            let mut consumed = 0usize;
            for m in read.find_iter(&body) {
                if m.start() < consumed {
                    continue;
                }
                // One statement: the whole `let x = string_prop(..)...?;` chain.
                let stop =
                    body[m.start()..].find(';').map(|o| m.start() + o).unwrap_or(body.len());
                let chain = &body[m.start()..stop];
                if !chain.contains(".ok_or_else") {
                    continue;
                }
                consumed = stop;
                // Any spelling the chain accepts satisfies it.
                let names: BTreeSet<String> =
                    read.captures_iter(chain).map(|c| c[1].to_string()).collect();
                if names.iter().any(|k| union.contains(k) || UNIVERSAL.contains(&k.as_str())) {
                    continue;
                }
                let key: Vec<&str> = names.iter().map(String::as_str).collect();
                let mut named: Vec<&str> = ids.iter().map(String::as_str).collect();
                named.sort_unstable();
                problems.push(format!(
                    "{:?} REQUIRE {:?} and no field declares it, so no node the editor can \
                     produce will plan. Either the form writes a different name than the arm \
                     reads, or the component is drawn by a synthesizer branch meant for another \
                     family (the src.couchdb bug).",
                    named,
                    key.join("/")
                ));
            }
        }
        problems.sort();
        problems.dedup();
        assert!(
            problems.is_empty(),
            "components that cannot be configured from their own form:\n  {}",
            problems.join("\n  ")
        );
    }
}
