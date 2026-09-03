//! #313: a capability registry generated from the component manifests.
//!
//! A hand-maintained connector matrix drifts from the code the week after it is
//! written, and a prose list of ~400 components cannot reliably answer "which
//! sources do incremental?". So nothing here is written down twice: every
//! answer is derived from the same catalog the editor renders its forms from
//! and the MCP server serves to agents.
//!
//! That is also the honest limit of it. A capability is inferred from the
//! DECLARED surface - a component supports incremental reads if it declares an
//! `incrementalColumn` field - so this reports what a component OFFERS, not
//! what the engine does with it. Where those two disagree the manifest is
//! wrong, which is the bug the prop-contract test exists to catch, and this
//! registry inherits its accuracy rather than adding to it.

use serde_json::Value;
use std::process::ExitCode;

/// The catalog is a committed artifact of `npm run export-catalog`, embedded at
/// build time so the command needs no workspace and no network.
const CATALOG_JSON: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../duckle-mcp/catalog.json"));

/// One component's declared surface, reduced to the questions people ask.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    pub component: String,
    pub kind: String,
    pub label: String,
    pub availability: String,
    /// Named ports other than `main`, which is what "does it emit rejects" and
    /// "does it take a lookup input" actually mean.
    pub reject_output: bool,
    pub lookup_input: bool,
    /// Reads or writes files/objects described as artifacts.
    pub artifact_io: bool,
    /// Takes a saved connection rather than inline credentials.
    pub connection_ref: bool,
    /// Declares any credential-shaped field.
    pub credentials: bool,
    /// Can be given SQL to run at the source.
    pub custom_sql: bool,
    /// Declares an incremental / watermark column.
    pub incremental: bool,
    /// Declares predicate or projection pushdown.
    pub pushdown: bool,
    /// The values a write-mode field offers, when it has one.
    pub write_modes: Vec<String>,
    /// Opt-in output caching (#252).
    pub cacheable: bool,
    /// Chunked extraction strategies this connector may be asked for (#306).
    ///
    /// From the engine's own allowlist rather than inferred from the manifest:
    /// chunking is refused for anything not on it, so a guess from a field name
    /// would advertise something the executor declines.
    pub chunking: Vec<String>,
    /// The SQL family predicates are written for, and ONLY when this connector
    /// can be chunked. The engine's `dialect_of` answers Postgres for anything
    /// it does not recognise, which is a sensible default inside the chunk
    /// planner and would be a false claim here.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dialect: Option<String>,
    /// DuckDB extensions the run-time prelude loads for this component, which
    /// is also what a self-contained bundle has to embed.
    pub extensions: Vec<String>,
    /// Every declared property key, so an agent can check a name without
    /// guessing at it.
    pub properties: Vec<String>,
}

fn field_keys(c: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(sections) = c["manifest"]["sections"].as_array() {
        for s in sections {
            if let Some(fields) = s["fields"].as_array() {
                for f in fields {
                    if let Some(k) = f["key"].as_str() {
                        out.push(k.to_string());
                    }
                }
            }
        }
    }
    out
}

/// The options a select-style field offers, if it is one.
fn options_of(c: &Value, key: &str) -> Vec<String> {
    let Some(sections) = c["manifest"]["sections"].as_array() else { return Vec::new() };
    for s in sections {
        let Some(fields) = s["fields"].as_array() else { continue };
        for f in fields {
            if f["key"].as_str() != Some(key) {
                continue;
            }
            if let Some(opts) = f["options"].as_array() {
                return opts
                    .iter()
                    .filter_map(|o| {
                        o["value"].as_str().map(str::to_string).or_else(|| o.as_str().map(str::to_string))
                    })
                    .collect();
            }
        }
    }
    Vec::new()
}

fn port_of(c: &Value, side: &str, ty: &str) -> bool {
    c["ports"][side]
        .as_array()
        .map(|ps| ps.iter().any(|p| p["type"].as_str() == Some(ty)))
        .unwrap_or(false)
}

/// Credential-shaped property names, matched case-insensitively on a substring
/// so `sslKeyPassword` counts as much as `password`.
const CREDENTIAL_HINTS: &[&str] =
    &["password", "secret", "token", "apikey", "accesskey", "privatekey", "credentials"];

pub fn derive(c: &Value) -> Capabilities {
    let keys = field_keys(c);
    let has = |k: &str| keys.iter().any(|x| x == k);
    let lower: Vec<String> = keys.iter().map(|k| k.to_ascii_lowercase()).collect();
    let credentials = lower
        .iter()
        .any(|k| CREDENTIAL_HINTS.iter().any(|h| k.contains(h)));
    // A write mode is spelled differently by different families; report
    // whichever one the component actually declares rather than inventing a
    // canonical name it does not use.
    let write_modes = if has("writeMode") {
        options_of(c, "writeMode")
    } else if has("mode") {
        options_of(c, "mode")
    } else {
        Vec::new()
    };
    // The engine's answers, asked of the engine. These are the axes #313 wants
    // that a manifest cannot supply, and each is a lookup against code that
    // already decides the question at run time.
    let id = c["id"].as_str().unwrap_or_default();
    let chunking: Vec<String> = duckle_duckdb_engine::chunking::capabilities(id)
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let dialect = (!chunking.is_empty()).then(|| {
        format!("{:?}", duckle_duckdb_engine::chunking::dialect_of(id)).to_ascii_lowercase()
    });
    let extensions = duckle_duckdb_engine::extensions_for_component(
        id,
        c.get("manifest").unwrap_or(&Value::Null),
    );
    Capabilities {
        component: c["id"].as_str().unwrap_or_default().to_string(),
        kind: c["kind"].as_str().unwrap_or_default().to_string(),
        label: c["label"].as_str().unwrap_or_default().to_string(),
        availability: c["availability"].as_str().unwrap_or("available").to_string(),
        reject_output: port_of(c, "outputs", "reject"),
        lookup_input: port_of(c, "inputs", "lookup"),
        artifact_io: port_of(c, "inputs", "artifact") || port_of(c, "outputs", "artifact"),
        connection_ref: has("connectionRef"),
        credentials,
        custom_sql: has("sql") || has("query") || has("rawSql"),
        incremental: has("incrementalColumn") || has("incrementalField"),
        pushdown: has("pushdown"),
        write_modes,
        cacheable: has("cacheOutput"),
        chunking,
        dialect,
        extensions,
        properties: keys,
    }
}

pub fn all() -> Vec<Capabilities> {
    let v: Value = match serde_json::from_str(CATALOG_JSON) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    v["components"]
        .as_array()
        .map(|cs| cs.iter().map(derive).collect())
        .unwrap_or_default()
}

pub fn run() -> ExitCode {
    let mut json_out = false;
    let mut kind_filter: Option<String> = None;
    let mut it = std::env::args().skip(2);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--json" => json_out = true,
            "--kind" => kind_filter = it.next(),
            other => {
                eprintln!(
                    "duckle-runner capabilities: unknown argument {other}. \
                     Use --json and --kind source|sink|transform|quality|control|custom."
                );
                return ExitCode::from(2);
            }
        }
    }
    let mut caps = all();
    if let Some(k) = &kind_filter {
        caps.retain(|c| &c.kind == k);
    }
    if caps.is_empty() {
        eprintln!("duckle-runner capabilities: no components matched");
        return ExitCode::from(2);
    }
    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schemaVersion": crate::report::SCHEMA_VERSION,
                "command": "capabilities",
                "count": caps.len(),
                "components": caps,
            }))
            .unwrap_or_default()
        );
    } else {
        println!(
            "{:<26} {:<10} {:<5} {:<5} {:<5} {:<5} {}",
            "component", "kind", "sql", "incr", "push", "rej", "write modes"
        );
        for c in &caps {
            let y = |b: bool| if b { "yes" } else { "-" };
            println!(
                "{:<26} {:<10} {:<5} {:<5} {:<5} {:<5} {}",
                c.component,
                c.kind,
                y(c.custom_sql),
                y(c.incremental),
                y(c.pushdown),
                y(c.reject_output),
                c.write_modes.join("/")
            );
        }
        println!("\n{} component(s)", caps.len());
    }
    ExitCode::from(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find(id: &str) -> Capabilities {
        all().into_iter().find(|c| c.component == id).unwrap_or_else(|| panic!("{id} in catalog"))
    }

    /// The registry covers the whole catalog, not a curated subset - that is
    /// the point of generating it.
    #[test]
    fn every_component_in_the_catalog_has_a_record() {
        let caps = all();
        assert!(caps.len() > 380, "got {}", caps.len());
        assert!(caps.iter().all(|c| !c.component.is_empty() && !c.kind.is_empty()));
    }

    /// Spot-check a source whose answers are known, so a change in how the
    /// catalog is shaped cannot quietly turn every capability into false.
    #[test]
    fn a_relational_source_reports_what_its_form_offers() {
        let pg = find("src.postgres");
        assert_eq!(pg.kind, "source");
        assert!(pg.custom_sql, "src.postgres declares sql");
        assert!(pg.incremental, "it declares incrementalColumn");
        assert!(pg.pushdown, "it declares pushdown");
        assert!(pg.connection_ref, "it takes a saved connection");
        assert!(pg.credentials, "it declares a password");
        assert!(pg.reject_output, "it has a reject port");
    }

    /// A transform has none of the source-shaped capabilities, which is the
    /// check that the derivation is reading fields rather than defaulting.
    #[test]
    fn a_pure_transform_claims_nothing_it_does_not_have() {
        let f = find("xf.filter");
        assert_eq!(f.kind, "transform");
        assert!(!f.incremental);
        assert!(!f.pushdown);
        assert!(!f.connection_ref);
        assert!(!f.credentials);
        assert!(f.write_modes.is_empty());
    }

    /// Write modes are read from the field's own options rather than assumed,
    /// because families spell the key differently.
    #[test]
    fn write_modes_come_from_the_declared_options() {
        let csv = find("snk.csv");
        assert_eq!(csv.kind, "sink");
        assert!(
            csv.write_modes.iter().any(|m| m == "overwrite" || m == "append"),
            "got {:?}",
            csv.write_modes
        );
    }

    /// Only the six components that declare cacheOutput are cacheable, which is
    /// a fact people keep having to rediscover.
    #[test]
    fn cacheable_is_the_small_set_that_declares_it() {
        let cacheable: Vec<String> =
            all().into_iter().filter(|c| c.cacheable).map(|c| c.component).collect();
        assert!(cacheable.len() <= 8, "got {cacheable:?}");
        assert!(cacheable.iter().any(|c| c == "code.python"), "got {cacheable:?}");
    }

    /// A credential hint is a substring match, so a differently-named secret
    /// field still counts.
    #[test]
    fn credentials_are_detected_by_shape_not_by_an_exact_name() {
        let c = serde_json::json!({
            "id": "x.y", "kind": "source", "label": "X", "ports": {},
            "manifest": { "sections": [ { "fields": [ { "key": "sslKeyPassword" } ] } ] }
        });
        assert!(derive(&c).credentials);
        let c2 = serde_json::json!({
            "id": "x.z", "kind": "transform", "label": "Z", "ports": {},
            "manifest": { "sections": [ { "fields": [ { "key": "columns" } ] } ] }
        });
        assert!(!derive(&c2).credentials);
    }
}

#[cfg(test)]
mod readme_counts {
    use super::*;

    /// The README is a second list, so it must not be an independent one.
    ///
    /// #313's premise, checked rather than argued: "hand-maintained
    /// README/roadmap lists can drift from the code". They had. The README
    /// claimed 113 sources, 130 transforms and 73 sinks against a catalog with
    /// 119, 144 and 72 - two counts stale upward and one stale DOWNWARD, so it
    /// was not simply lagging behind additions, it was unmaintained.
    ///
    /// The prose and the groupings stay hand-written, because they are
    /// editorial. Only the numbers are derived, because only the numbers have a
    /// right answer.
    const README: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../README.md"));

    fn available(kind: &str) -> usize {
        let all: Value = serde_json::from_str(CATALOG_JSON).expect("catalog parses");
        all["components"]
            .as_array()
            .expect("catalog has a components array")
            .iter()
            .filter(|c| {
                c.get("kind").and_then(Value::as_str) == Some(kind)
                    && c.get("availability").and_then(Value::as_str) == Some("available")
            })
            .count()
    }

    /// `**113 sources available today.**` -> 113
    fn claimed(noun: &str) -> usize {
        let needle = format!(" {noun} available today");
        let line = README
            .lines()
            .find(|l| l.contains(&needle))
            .unwrap_or_else(|| panic!("the README no longer states how many {noun} are available"));
        line.chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse()
            .unwrap_or_else(|_| panic!("no number in {line:?}"))
    }

    #[test]
    fn the_readme_counts_match_the_catalog() {
        for (noun, kind) in [("sources", "source"), ("transforms", "transform"), ("sinks", "sink")] {
            assert_eq!(
                claimed(noun),
                available(kind),
                "the README says {} {noun} are available and the catalog has {}. Update the \
                 README, in the same change as whatever added or removed the component.",
                claimed(noun),
                available(kind)
            );
        }
    }
}

#[cfg(test)]
mod engine_facts {
    use super::*;

    fn of(id: &str) -> Capabilities {
        all().into_iter().find(|c| c.component == id).unwrap_or_else(|| panic!("{id} in catalog"))
    }

    /// #313's own example record asks for `"chunking": ["range", "time"]`, and
    /// that answer exists in the engine. Reporting it from the manifest would
    /// have been a guess; the engine REFUSES chunking for anything not on its
    /// allowlist, so a guess would advertise what the executor declines.
    #[test]
    fn chunking_and_dialect_come_from_the_engine_not_from_a_field_name() {
        let pg = of("src.postgres");
        assert!(pg.chunking.contains(&"range".to_string()), "{:?}", pg.chunking);
        assert_eq!(pg.dialect.as_deref(), Some("postgres"));

        let ora = of("src.oracle");
        assert_eq!(ora.dialect.as_deref(), Some("oracle"), "the family is per connector");

        // A connector the engine will not chunk claims nothing, and carries no
        // dialect at all - `dialect_of` answers Postgres for anything it does
        // not know, which would be a false claim here.
        let csv = of("src.csv");
        assert!(csv.chunking.is_empty(), "{:?}", csv.chunking);
        assert_eq!(csv.dialect, None, "src.csv is not a SQL family");
    }

    /// The extensions a bundle must embed, on the record an agent reads.
    #[test]
    fn required_extensions_are_reported() {
        assert_eq!(of("src.gdb").extensions, vec!["spatial".to_string()]);
        assert_eq!(of("src.avro").extensions, vec!["avro".to_string()]);
        assert!(of("src.csv").extensions.is_empty());
    }
}
