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
        (_, "recordPath") => Some("singular spelling of recordsPath"),
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
    // `Ok(` has to be seen through: an arm written `=> Ok(build_x(props))`
    // is as much a dispatch as `=> build_x(props)`, and skipping those made
    // both contract tests blind to 30-odd components, src.filelist and
    // src.inline among them.
    let arm = regex::Regex::new(
        r#"(?m)^\s*((?:"[a-z][\w.]*"\s*\|\s*)*"[a-z][\w.]*")\s*=>\s*(?:Ok\(\s*)?([a-z_]\w*)\s*\("#,
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

    /// Keys a `plan/mod.rs` arm reads OPTIONALLY that no component it serves
    /// declares, and why each is correct rather than a missing field.
    ///
    /// Adding to this list is a deliberate act, and every entry names the
    /// source line that settles it. "It looks like an alias" is not a reason:
    /// open the arm and confirm the canonical key is the one declared.
    fn arm_allowed(component: &str, key: &str) -> Option<&'static str> {
        match (component, key) {
            // Legacy or alternate spellings, resolved by `.or_else` onto a key
            // the form DOES declare.
            (_, "textColumn") => Some("alternate spelling of inputColumn (mod.rs:6225)"),
            (_, "prompt") => Some("#142 legacy spelling of promptTemplate (mod.rs:6362)"),
            (_, "token") => Some("alternate spelling of authToken (mod.rs:3503, 5498)"),
            (_, "partitionColumn") => Some("alternate spelling of parallelColumn (mod.rs:4159)"),
            (_, "rootPath") => Some("alternate spelling of rowPath (mod.rs:4873)"),
            ("snk.teradata", "schema") => Some("alternate spelling of database (mod.rs:3417)"),
            ("snk.rest" | "snk.webhook", "bodyShape") => {
                Some("override of the declared batchMode, which maps onto it (mod.rs:2184)")
            }
            ("src.kafka" | "src.redpanda", "startOffset") => Some(
                "numeric form of the declared `offset` select; the arm's own comment says the UI \
                 exposes latest/earliest and a hand-authored number wins (mod.rs:4324)",
            ),

            // Read, but nothing a form could usefully offer.
            ("snk.salesforce", "api") => Some(
                "the only accepted value is `collections`; `bulk` returns an error pointing at \
                 snk.salesforce.bulk, so there is no choice to present (mod.rs:2809)",
            ),
            ("src.ftp" | "snk.ftp", "secure") => Some(
                "OR-ed with a protocol of ftps, and the protocol select already offers FTPS \
                 (mod.rs:3202, 4677)",
            ),
            ("ctl.deadletter", "mode") => Some(
                "the dead-letter writer is a COPY, which always replaces the file, so overwrite \
                 is the only mode it could honour (mod.rs:4079)",
            ),
            ("xf.ai.llm", "inputColumn") => Some(
                "used only when prompt_template is empty, and the form makes promptTemplate \
                 required, so it never applies to an editor-built node (connectors.rs:13067)",
            ),
            (_, "privateKeyPath") => Some(
                "hand-set alternative to the declared privateKey, named in that field's own \
                 description",
            ),

            // Rendered by PropertiesPanel itself rather than by a manifest
            // section, so it is reachable in the editor and invisible to a
            // check that reads manifests.
            (_, "materialize" | "materializePath") => {
                Some("panel-level field, PropertiesPanel.tsx:92 and :110")
            }

            // Not an arm at all: shared prologue, which this splitter attributes
            // to whichever arm follows it.
            (_, "cacheOutput") => Some(
                "shared prologue gated by CACHEABLE_COMPONENTS, declared via outputCacheSection",
            ),
            (_, "publishGroup") => Some("guarded to snk.ducklake, which declares it (mod.rs:6995)"),
            _ => None,
        }
    }

    /// The same union rule as the builders test, over every key a `plan/mod.rs`
    /// arm reads - not only the required ones.
    ///
    /// The required-props test below deliberately stopped at keys whose absence
    /// FAILS the run. But an optional prop with a default fails quietly, which
    /// is worse, and running this found 14 real gaps at once. Three of them
    /// silently truncated data:
    ///
    /// - `src.kafka` and `snk.kafka` read `partitionId`, defaulting to 0, with
    ///   no field, so the editor could only ever read or write partition 0.
    /// - `src.kafka` capped every read at `maxRecords`, default 1000.
    /// - `src.elastic` / `src.opensearch` read `paginationMode`, so without a
    ///   field every run used from/size, which both engines refuse past
    ///   `index.max_result_window` - 10,000 documents by default.
    ///
    /// The alias rule is what makes the result readable: within one `let ..;`
    /// statement, if any key is declared then every key in that statement is a
    /// spelling of the same setting. Without it this reports 57 arms; with it,
    /// 26, of which 14 were real.
    #[test]
    fn every_optional_property_an_arm_reads_is_one_some_component_declares() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("plan")
            .join("mod.rs");
        let src = std::fs::read_to_string(&path).expect("plan/mod.rs");
        let lines: Vec<&str> = src.lines().collect();
        let declared = declared_keys();
        let preds = predicate_ids(&src);
        let starts = arm_starts(&lines);
        let read = regex::Regex::new(
            r#"(?:string_prop|bool_prop|columns_list|kv_pairs)\(\s*&?props\s*,\s*"(\w+)"\s*\)|props\s*\.\s*get\(\s*"(\w+)"\s*\)"#,
        )
        .unwrap();
        let stmt = regex::Regex::new(r"(?s)let\s.{0,600}?;").unwrap();
        let lit = regex::Regex::new(r#""(\w+)""#).unwrap();

        let mut problems: Vec<String> = Vec::new();
        // This check reads Rust with a regex, so it can pass by matching
        // NOTHING - which is how the builders test silently skipped every
        // `=> Ok(build_x(` arm for months. Count what was actually inspected
        // and refuse to be green on an empty sweep.
        let (mut arms_seen, mut keys_seen) = (0usize, 0usize);
        for (n, &start) in starts.iter().enumerate() {
            let (ids, open) = ids_of(&lines, start, &preds);
            let ids: BTreeSet<String> =
                ids.into_iter().filter(|i| declared.contains_key(i)).collect();
            if ids.is_empty() {
                continue;
            }
            arms_seen += 1;
            let end = starts.get(n + 1).copied().unwrap_or(lines.len());
            let body = lines[open..end].join("\n");
            let union: BTreeSet<&str> = ids
                .iter()
                .filter_map(|id| declared.get(id))
                .flatten()
                .map(String::as_str)
                .collect();

            // Alias rule: a statement naming a declared key vouches for every
            // other key in that same statement.
            let mut covered: BTreeSet<String> = union.iter().map(|s| s.to_string()).collect();
            for m in stmt.find_iter(&body) {
                let keys: Vec<String> =
                    lit.captures_iter(m.as_str()).map(|c| c[1].to_string()).collect();
                if keys.iter().any(|k| union.contains(k.as_str())) {
                    covered.extend(keys);
                }
            }

            for c in read.captures_iter(&body) {
                let key = c.get(1).or_else(|| c.get(2)).map(|m| m.as_str()).unwrap_or_default();
                keys_seen += 1;
                if key.is_empty() || covered.contains(key) {
                    continue;
                }
                if ids.iter().any(|id| arm_allowed(id, key).is_some()) {
                    continue;
                }
                let mut named: Vec<&str> = ids.iter().map(String::as_str).collect();
                named.sort_unstable();
                problems.push(format!(
                    "{named:?} read `{key}` and no form declares it, so the setting works and is \
                     reachable only by hand-editing the pipeline JSON"
                ));
            }
        }
        assert!(
            arms_seen > 100 && keys_seen > 500,
            "this check inspected {arms_seen} arms and {keys_seen} prop reads, which is far too \
             few - the arm splitter or the read pattern has stopped matching, and a green result \
             here would mean nothing"
        );
        problems.sort();
        problems.dedup();
        assert!(
            problems.is_empty(),
            "engine settings with no field to set them:\n  {}",
            problems.join("\n  ")
        );
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
        let required_msg = regex::Regex::new(r#""[^"\\]*required[^"\\]*""#).unwrap();
        let word = regex::Regex::new(r"[A-Za-z][A-Za-z0-9_]*").unwrap();

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

            // The OTHER shape of the same requirement. code.wasm writes it as
            // `if let Some(a) = .. else if let Some(b) = .. else { return Err }`
            // - no ok_or_else to match - and slipped past the pass above while
            // being exactly as unbuildable. So read the refusal instead: these
            // arms all phrase it "<name> required".
            //
            // Grounded twice, or it would be noise. The identifiers taken out
            // of the message are kept only if the arm actually reads them via
            // string_prop, which is what stops "upstream input required" from
            // counting. And the statement the refusal belongs to is scanned for
            // alternates the message does not name: `url required` sits at the
            // end of `string_prop("url").or_else(|| string_prop("connectionString"))`,
            // and connectionString satisfies it.
            let reads: BTreeSet<String> =
                read.captures_iter(&body).map(|c| c[1].to_string()).collect();
            for m in required_msg.find_iter(&body) {
                let text = m.as_str();
                let mut names: BTreeSet<String> = word
                    .find_iter(text)
                    .map(|w| w.as_str().to_string())
                    .filter(|w| reads.contains(w))
                    .collect();
                if names.is_empty() {
                    continue;
                }
                let from = m.start().saturating_sub(700);
                let back = &body[from..m.start()];
                let cut = back
                    .rfind("
        let ")
                    .max(back.rfind("
            let "))
                    .unwrap_or(0);
                names.extend(read.captures_iter(&back[cut..]).map(|c| c[1].to_string()));
                if names.iter().any(|k| union.contains(k) || UNIVERSAL.contains(&k.as_str())) {
                    continue;
                }
                let key: Vec<&str> = names.iter().map(String::as_str).collect();
                let mut named: Vec<&str> = ids.iter().map(String::as_str).collect();
                named.sort_unstable();
                problems.push(format!(
                    "{:?} REQUIRE {:?} and no field declares it, so no node the editor can                      produce will plan. Either the form writes a different name than the arm                      reads, or the component is drawn by a synthesizer branch meant for another                      family (the src.couchdb bug).",
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

/// Every component the palette calls `available` has to be one the engine
/// dispatches.
///
/// Run Events was declared with the `ctl()` helper while sitting between two
/// `src()` entries in a source group, so the palette shipped `ctl.runevents`
/// and the engine only ever matched `src.runevents`. Dragging it onto the
/// canvas produced a node that fell through to the final else and refused with
/// "'ctl.runevents' isn't executable on the DuckDB engine yet - it's a preview
/// component" - a failed run AND a wrong explanation, since the palette says
/// available.
#[test]
fn every_available_component_is_one_the_engine_dispatches() {
    const ENGINE_SOURCES: [&str; 10] = [
        "crates/duckdb-engine/src/plan/builders.rs",
        "crates/duckdb-engine/src/plan/mod.rs",
        "crates/duckdb-engine/src/plan/graph.rs",
        "crates/duckdb-engine/src/lib.rs",
        "crates/duckdb-engine/src/connectors.rs",
        "crates/duckdb-engine/src/policy.rs",
        "crates/duckdb-engine/src/chunking.rs",
        "crates/duckdb-engine/src/capabilities.rs",
        "crates/duckdb-engine/src/format.rs",
        "crates/duckdb-engine/src/props.rs",
    ];
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    let id_lit = regex::Regex::new(r#""((?:src|snk|xf|ctl|code|qa)\.[a-z0-9_.]+)""#).unwrap();
    let mut named: BTreeSet<String> = BTreeSet::new();
    for rel in ENGINE_SOURCES {
        let Ok(src) = std::fs::read_to_string(root.join(rel)) else { continue };
        named.extend(id_lit.captures_iter(&src).map(|c| c[1].to_string()));
    }
    assert!(named.len() > 200, "only {} ids found - the scan is broken", named.len());

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("duckle-mcp")
        .join("catalog.json");
    let v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("catalog")).expect("json");
    let mut orphans: Vec<String> = Vec::new();
    for c in v["components"].as_array().expect("components") {
        if c["availability"].as_str().unwrap_or("available") != "available" {
            continue;
        }
        let id = c["id"].as_str().unwrap_or_default();
        if !named.contains(id) {
            orphans.push(id.to_string());
        }
    }
    assert!(
        orphans.is_empty(),
        "the palette offers these as available and no engine source names them, so each \
         refuses with \"isn't executable ... it's a preview component\": {orphans:?}"
    );
}

/// A reject port must be one something can fill.
///
/// portsForComponent's default is `[MAIN_OUT, REJECT_OUT]`, so 281 available
/// components advertised one and 17 ids could produce the `<node>__reject`
/// relation a wired edge reads. Wiring any of the others failed the whole run -
/// measured on a plain xf.distinct:
///
///     Catalog Error: Table with name t__reject does not exist!
///
/// The allowed set is derived from the RUST side here, so adding a port in the
/// frontend without an engine path behind it fails this test rather than a
/// user's pipeline.
#[test]
fn every_declared_reject_port_is_one_something_can_fill() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let builders = std::fs::read_to_string(root.join("src").join("plan").join("builders.rs"))
        .expect("builders.rs");
    let plan = std::fs::read_to_string(root.join("src").join("plan").join("mod.rs"))
        .expect("plan/mod.rs");

    // 1. build_reject_sql's own match arms.
    let start = builders.find("pub(crate) fn build_reject_sql").expect("build_reject_sql");
    let open = builders[start..].find('{').expect("body") + start;
    let (mut depth, bytes) = (0i32, builders.as_bytes());
    let mut end = builders.len();
    for i in open..builders.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    let body = &builders[open..end];
    let lit = regex::Regex::new(r#""([a-z][\w.]*)""#).unwrap();
    let arm = regex::Regex::new(r#"(?m)^\s{8}((?:"[a-z][\w.]*"\s*\|\s*\n?\s*)*"[a-z][\w.]*")\s*=>"#)
        .unwrap();
    let mut fillable: BTreeSet<String> = arm
        .captures_iter(body)
        .flat_map(|c| {
            lit.captures_iter(c.get(1).unwrap().as_str())
                .map(|m| m[1].to_string())
                .collect::<Vec<_>>()
        })
        .collect();

    // 2. The REST family writes a reject relation when onParentError is
    //    "reject". The GraphQL arm hardcodes it to "fail", so it cannot.
    let rest = regex::Regex::new(r"pub fn reads_incremental\(component_id: &str\) -> bool \{(?s:.*?)\n\}")
        .unwrap()
        .find(&plan)
        .expect("reads_incremental");
    for c in lit.captures_iter(rest.as_str()) {
        fillable.insert(c[1].to_string());
    }
    for graphql in ["src.graphql", "src.linear", "src.monday"] {
        fillable.remove(graphql);
    }
    assert!(fillable.len() > 30, "only {} fillable ids - the scan broke", fillable.len());

    let path = root.join("..").join("duckle-mcp").join("catalog.json");
    let v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("catalog")).expect("json");
    let mut unfillable: Vec<String> = Vec::new();
    for c in v["components"].as_array().expect("components") {
        let id = c["id"].as_str().unwrap_or_default();
        let has_reject = c["ports"]["outputs"]
            .as_array()
            .map(|o| o.iter().any(|p| p["id"] == "reject"))
            .unwrap_or(false);
        if has_reject && !fillable.contains(id) && !id.starts_with("ext.") {
            unfillable.push(id.to_string());
        }
    }
    unfillable.sort();
    assert!(
        unfillable.is_empty(),
        "these declare a reject port and nothing can produce their __reject relation, so \
         wiring it fails the run: {unfillable:?}"
    );
}

/// A form that sets nothing its builder reads is a form that does nothing.
///
/// The union check above is per BUILDER, so a family passes as long as SOME
/// member declares each key. That hides the opposite failure: a component
/// whose own form shares no key at all with the builder it dispatches to.
/// The user fills the panel in, every value lands in the pipeline, and the
/// builder reads none of them - so the node runs on its defaults and says
/// nothing. src.inline had a bare notes box and returned
/// `SELECT NULL WHERE false`, zero rows and no error; src.filelist had the
/// same box and globbed the filesystem root.
#[test]
fn every_component_declares_at_least_one_key_its_builder_reads() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("plan")
        .join("builders.rs");
    let src = std::fs::read_to_string(&path).expect("builders.rs");
    let declared = declared_keys();
    let read = regex::Regex::new(
        r#"(?:columns_list|kv_pairs|string_prop|bool_prop|u64_prop|usize_prop|num_prop|int_prop)\s*\(\s*&?props\s*,\s*"(\w+)"|props\s*\.\s*get\(\s*"(\w+)""#,
    )
    .unwrap();

    let mut deaf: Vec<String> = Vec::new();
    for (builder, ids) in ids_by_builder(&src) {
        let Some(body) = body_of(&src, &builder) else { continue };
        let keys: BTreeSet<String> = read
            .captures_iter(body)
            .filter_map(|c| c.get(1).or_else(|| c.get(2)).map(|m| m.as_str().to_string()))
            .collect();
        // Nothing to declare, so nothing to get wrong.
        if keys.is_empty() {
            continue;
        }
        for id in &ids {
            let Some(fields) = declared.get(id) else { continue };
            // A component with no form at all is the other test's business.
            if fields.is_empty() {
                continue;
            }
            if fields.iter().any(|k| keys.contains(k)) {
                continue;
            }
            deaf.push(format!(
                "{id} -> {builder}() reads {keys:?} but its form only sets {fields:?}"
            ));
        }
    }
    deaf.sort();
    assert!(
        deaf.is_empty(),
        "these components have a form that sets nothing the engine reads, so filling it \
         in changes nothing about the run: {deaf:#?}"
    );
}
