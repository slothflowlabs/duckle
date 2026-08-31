//! #301: mask sensitive columns on the surfaces people INSPECT.
//!
//! ## The distinction that makes this safe
//!
//! A pipeline's sinks can be perfectly governed and its data still be read off
//! a screen. Previews, profiles, reject rows, error bodies, API responses and
//! MCP tool results are all legitimate inspection surfaces, and each one is a
//! place production person-data can appear without anyone's permissions being
//! wrong.
//!
//! So masking here is deliberately **not** a transform. It changes what an
//! inspection surface shows and never what the pipeline writes. A run that
//! masks a preview still writes the real value to its sink, because the sink is
//! governed by policy and the screen is not. Changing the written data would be
//! a different feature, and `qa.mask` already is it.
//!
//! ## Why previews, and why here
//!
//! Every inspection surface in Duckle reads the same `RunResult.preview`: the
//! desktop panel, `duckle-runner`'s CLI output, the console API, and the MCP
//! tools an agent calls. Masking as the previews are assembled therefore makes
//! them consistent by construction rather than by four teams remembering. That
//! is acceptance criterion 1, satisfied structurally.
//!
//! ## Tags
//!
//! Tags are declared on the column, in the pipeline's own schema, so they are
//! deterministic and reviewable:
//!
//! ```json
//! { "name": "national_id", "type": "string", "tags": ["pii"] }
//! { "name": "api_secret",  "type": "string", "tags": ["secret"] }
//! ```
//!
//! Duckle's PII findings can suggest these, but nothing here infers a tag from
//! a column name. A heuristic that silently masked `company_name` because it
//! contains "name" would teach people to distrust the masking, and a heuristic
//! that silently DIDN'T mask something is worse.

use crate::NodePreview;
use duckle_metadata::Column;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

/// What to show instead of the value.
#[derive(Debug, Clone, PartialEq)]
pub enum Strategy {
    /// A fixed marker. Nothing about the value survives, not even its length.
    Redact,
    /// Null, for a surface that needs the shape but not the content.
    Null,
    /// A short stable digest. The same value masks to the same token, so rows
    /// can still be told apart and joined by eye while the value itself does
    /// not appear. Not reversible, and not a security boundary against a
    /// guessable domain: a hashed two-letter country code is still a country
    /// code to anyone who tries all of them.
    Hash,
    /// The last N characters, for the "is this the right card" case.
    Last(usize),
}

pub const REDACTED: &str = "***";

/// The strategy a column's tags ask for, if any.
///
/// `secret` always wins and always redacts. A column tagged secret cannot be
/// shown by any other tag on it, which is acceptance criterion 2: there is no
/// combination of tags that reveals one.
pub fn strategy_for(tags: &[String]) -> Option<Strategy> {
    let has = |t: &str| tags.iter().any(|x| x.trim().eq_ignore_ascii_case(t));
    if has("secret") {
        return Some(Strategy::Redact);
    }
    for t in tags {
        let t = t.trim().to_ascii_lowercase();
        if let Some(rest) = t.strip_prefix("mask:") {
            return match rest {
                "redact" => Some(Strategy::Redact),
                "null" => Some(Strategy::Null),
                "hash" => Some(Strategy::Hash),
                _ => rest
                    .strip_prefix("last")
                    .and_then(|n| n.parse::<usize>().ok())
                    .map(Strategy::Last)
                    // An unreadable mask: tag must not quietly mean "show it".
                    // Falling back to Redact is the safe direction to be wrong.
                    .or(Some(Strategy::Redact)),
            };
        }
    }
    // A plain `pii` tag hashes rather than redacts, so a preview stays useful
    // for telling rows apart while the values themselves do not appear.
    has("pii").then_some(Strategy::Hash)
}

/// Apply a strategy to one value. Null stays null: masking a value that is not
/// there would invent the impression that it was.
pub fn apply(value: &JsonValue, strategy: &Strategy) -> JsonValue {
    if value.is_null() {
        return JsonValue::Null;
    }
    match strategy {
        Strategy::Redact => JsonValue::String(REDACTED.to_string()),
        Strategy::Null => JsonValue::Null,
        Strategy::Hash => {
            let text = match value {
                JsonValue::String(s) => s.clone(),
                other => other.to_string(),
            };
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(text.as_bytes());
            let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
            JsonValue::String(format!("#{}", &hex[..10]))
        }
        Strategy::Last(n) => {
            let text = match value {
                JsonValue::String(s) => s.clone(),
                other => other.to_string(),
            };
            let chars: Vec<char> = text.chars().collect();
            if chars.len() <= *n {
                // Showing the whole value because it happens to be short is
                // exactly the case a mask exists for.
                return JsonValue::String(REDACTED.to_string());
            }
            let tail: String = chars[chars.len() - n..].iter().collect();
            JsonValue::String(format!("{REDACTED}{tail}"))
        }
    }
}

/// Column tags declared per node, read from the pipeline itself.
pub type TagMap = BTreeMap<String, BTreeMap<String, Vec<String>>>;

/// Collect the declared tags for every node in a pipeline.
pub fn tags_from_doc(doc: &crate::PipelineDoc) -> TagMap {
    let mut out: TagMap = BTreeMap::new();
    for node in &doc.nodes {
        let Some(schema) = node.data.schema.as_ref() else { continue };
        let cols: BTreeMap<String, Vec<String>> = schema
            .iter()
            .filter(|c: &&Column| !c.tags.is_empty())
            .map(|c| (c.name.clone(), c.tags.clone()))
            .collect();
        if !cols.is_empty() {
            out.insert(node.id.clone(), cols);
        }
    }
    out
}

/// Mask a run's previews in place.
///
/// Only the VALUES change. The column list, its types and the row count are
/// untouched, because those are what makes a preview useful for debugging a
/// pipeline and none of them is the sensitive part.
pub fn mask_previews(previews: &mut [NodePreview], tags: &TagMap) {
    if tags.is_empty() {
        return;
    }
    for p in previews.iter_mut() {
        let Some(node_tags) = tags.get(&p.node_id) else { continue };
        // Resolve the strategy per column once, by name, rather than per cell.
        let by_name: BTreeMap<&str, Strategy> = node_tags
            .iter()
            .filter_map(|(name, t)| strategy_for(t).map(|s| (name.as_str(), s)))
            .collect();
        if by_name.is_empty() {
            continue;
        }
        for row in p.rows.iter_mut() {
            match row {
                JsonValue::Object(map) => {
                    for (k, v) in map.iter_mut() {
                        if let Some(s) = by_name.get(k.as_str()) {
                            *v = apply(v, s);
                        }
                    }
                }
                // A row shaped as an array is positional, so the column list is
                // what names it.
                JsonValue::Array(cells) => {
                    for (i, cell) in cells.iter_mut().enumerate() {
                        let Some(col) = p.columns.get(i) else { continue };
                        if let Some(s) = by_name.get(col.name.as_str()) {
                            *cell = apply(cell, s);
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, tags: &[&str]) -> Column {
        Column {
            name: name.into(),
            data_type: duckle_metadata::DataType::String,
            nullable: true,
            primary_key: None,
            format: None,
            tags: tags.iter().map(|t| t.to_string()).collect(),
        }
    }

    /// Acceptance criterion 2: nothing reveals a secret.
    #[test]
    fn a_secret_is_always_redacted_whatever_else_it_is_tagged() {
        assert_eq!(strategy_for(&["secret".into()]), Some(Strategy::Redact));
        // Even asked explicitly to show the tail.
        assert_eq!(
            strategy_for(&["secret".into(), "mask:last4".into()]),
            Some(Strategy::Redact),
            "no combination of tags may reveal a secret"
        );
        assert_eq!(
            strategy_for(&["pii".into(), "secret".into()]),
            Some(Strategy::Redact)
        );
    }

    /// Nothing is inferred from a column name. A heuristic that masked
    /// `company_name` would teach people to distrust the masking.
    #[test]
    fn an_untagged_column_is_not_masked() {
        assert_eq!(strategy_for(&[]), None);
        assert_eq!(strategy_for(&["description".into()]), None);
    }

    /// A hash is stable, so rows can still be told apart by eye.
    #[test]
    fn a_hash_is_stable_and_does_not_contain_the_value() {
        let a = apply(&JsonValue::String("alice@example.com".into()), &Strategy::Hash);
        let b = apply(&JsonValue::String("alice@example.com".into()), &Strategy::Hash);
        let c = apply(&JsonValue::String("bob@example.com".into()), &Strategy::Hash);
        assert_eq!(a, b, "the same value must mask to the same token");
        assert_ne!(a, c, "different values must stay distinguishable");
        assert!(!a.as_str().unwrap().contains("alice"), "got {a}");
    }

    /// A tail mask on a value shorter than the tail would show all of it.
    #[test]
    fn a_tail_mask_never_shows_the_whole_value() {
        let long = apply(&JsonValue::String("4111111111111234".into()), &Strategy::Last(4));
        assert_eq!(long, JsonValue::String("***1234".into()));
        let short = apply(&JsonValue::String("12".into()), &Strategy::Last(4));
        assert_eq!(short, JsonValue::String("***".into()), "must not fall back to showing it");
    }

    /// Masking a null would invent the impression that a value was there.
    #[test]
    fn a_null_stays_null() {
        assert_eq!(apply(&JsonValue::Null, &Strategy::Redact), JsonValue::Null);
        assert_eq!(apply(&JsonValue::Null, &Strategy::Hash), JsonValue::Null);
    }

    /// An unreadable mask: tag must not quietly mean "show it".
    #[test]
    fn an_unreadable_mask_tag_redacts_rather_than_revealing() {
        assert_eq!(strategy_for(&["mask:whatever".into()]), Some(Strategy::Redact));
    }

    #[test]
    fn a_preview_is_masked_by_column_and_leaves_the_rest_alone() {
        let mut previews = vec![NodePreview {
            node_id: "people".into(),
            columns: vec![col("id", &[]), col("email", &["pii"]), col("token", &["secret"])],
            rows: vec![
                serde_json::json!({ "id": 1, "email": "a@example.com", "token": "sk-live-1" }),
                serde_json::json!({ "id": 2, "email": "b@example.com", "token": "sk-live-2" }),
            ],
            sql_types: vec![],
        }];
        let mut tags: TagMap = BTreeMap::new();
        tags.insert(
            "people".into(),
            [
                ("email".to_string(), vec!["pii".to_string()]),
                ("token".to_string(), vec!["secret".to_string()]),
            ]
            .into_iter()
            .collect(),
        );
        mask_previews(&mut previews, &tags);

        let r0 = &previews[0].rows[0];
        assert_eq!(r0["id"], 1, "an untagged column is untouched");
        assert_eq!(r0["token"], REDACTED, "secret is redacted");
        assert!(
            r0["email"].as_str().unwrap().starts_with('#'),
            "pii is hashed: {}",
            r0["email"]
        );
        assert_ne!(
            previews[0].rows[0]["email"], previews[0].rows[1]["email"],
            "two different addresses must stay distinguishable"
        );
        assert_eq!(previews[0].columns.len(), 3, "the column list is not the sensitive part");
    }

    /// A preview for a node with no tags is left entirely alone, so this costs
    /// nothing for the pipelines that do not use it.
    #[test]
    fn a_node_with_no_tags_is_untouched() {
        let mut previews = vec![NodePreview {
            node_id: "other".into(),
            columns: vec![col("email", &[])],
            rows: vec![serde_json::json!({ "email": "a@example.com" })],
            sql_types: vec![],
        }];
        let mut tags: TagMap = BTreeMap::new();
        tags.insert(
            "people".into(),
            [("email".to_string(), vec!["pii".to_string()])].into_iter().collect(),
        );
        mask_previews(&mut previews, &tags);
        assert_eq!(previews[0].rows[0]["email"], "a@example.com");
    }
}
