//! Measure how many shipped sources and sinks `catalog::asset_of` can name,
//! by calling the real function rather than re-implementing its rules.
//!
//! Two hand-written simulations of those rules disagreed with each other, which
//! is the usual outcome of restating logic in a second place. This reads the
//! committed component catalog, builds a property bag from each connector's
//! REQUIRED fields, and asks the engine itself.
//!
//! Run with: cargo run -p duckle-duckdb-engine --example asset_coverage

use serde_json::{Map, Value};

fn main() {
    let catalog = concat!(env!("CARGO_MANIFEST_DIR"), "/../duckle-mcp/catalog.json");
    let text = std::fs::read_to_string(catalog).expect("read the component catalog");
    let doc: Value = serde_json::from_str(&text).expect("parse the component catalog");
    let comps = doc
        .get("components")
        .and_then(Value::as_array)
        .expect("the catalog has a components array")
        .clone();

    let mut named = 0usize;
    let mut unnamed: Vec<String> = Vec::new();
    let mut total = 0usize;

    for c in &comps {
        let id = c.get("id").and_then(Value::as_str).unwrap_or("");
        if !(id.starts_with("src.") || id.starts_with("snk.")) {
            continue;
        }
        total += 1;

        // Every field the manifest marks required, set to a plausible value.
        // That is the least a correctly configured node of this type can carry.
        let mut props = Map::new();
        if let Some(sections) = c.pointer("/manifest/sections").and_then(Value::as_array) {
            for s in sections {
                for f in s.get("fields").and_then(Value::as_array).into_iter().flatten() {
                    let required = f.get("required").and_then(Value::as_bool).unwrap_or(false);
                    let key = f.get("key").and_then(Value::as_str).unwrap_or("");
                    if required && !key.is_empty() {
                        props.insert(key.to_string(), Value::String("x".into()));
                    }
                }
            }
        }

        match duckle_duckdb_engine::catalog::asset_of(id, &Value::Object(props)) {
            Ok(_) => named += 1,
            Err(_) => unnamed.push(id.to_string()),
        }
    }

    println!("asset_of names {named}/{total} shipped sources and sinks by required fields alone");
    println!("({} could not be named)", unnamed.len());
    for id in &unnamed {
        println!("  {id}");
    }

    // The exact JSON keys alerts.json accepts, printed from the real types, so
    // the README documents what serde actually does rather than what the field
    // names look like in Rust.
    println!("\nalerts.json rule as serde writes it:");
    let rule = duckle_duckdb_engine::alerts::AlertRule {
        pattern: "nightly-*".into(),
        on: vec![duckle_duckdb_engine::alerts::Event::Failure],
        cooldown_minutes: 15,
        // Unset, so the rule routes purely on its glob - which is what every
        // alerts.json written before owner/tag routing existed means.
        owner: None,
        tags: Vec::new(),
        channel: duckle_duckdb_engine::alerts::Channel::Email {
            smtp_host: "smtp.example.com".into(),
            smtp_port: 587,
            username: String::new(),
            password: String::new(),
            from: "duckle@example.com".into(),
            to: vec!["oncall@example.com".into()],
        },
    };
    println!("{}", serde_json::to_string_pretty(&rule).unwrap());
}
