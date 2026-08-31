//! #317: a typed parameter contract, validated once for every surface.
//!
//! #127 gave interactive runs a prompt for any unresolved `${name}`. That is
//! the right shape for a person at a canvas and not enough for production,
//! because a bare string cannot answer:
//!
//! - is this missing, or deliberately empty?
//! - is `false` a boolean or the four-character word?
//! - is `jurisdiciton` a new parameter or a typo?
//! - is this value safe to write into run history?
//!
//! ## One normalization boundary
//!
//! Everything here happens **once**, before compilation, and every surface gets
//! the same typed result: desktop, console, CLI, HTTP API, MCP, scheduler,
//! Plans, backfills, retry. That is the actual requirement. Validating per
//! surface is how the desktop ends up accepting a value the scheduler rejects,
//! and the bug is then in neither of them.
//!
//! ## Secrets are a type, not a naming convention
//!
//! A parameter declared `secret` is never written to run history, a receipt, or
//! any other record. Inferring that from a name would mean `api_token` is
//! protected and `credential` is not, decided by whoever typed the name.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParamType {
    String,
    Integer,
    Number,
    Boolean,
    Date,
    Datetime,
    /// Any string, but never persisted anywhere.
    Secret,
}

/// What a pipeline says about one of its parameters.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParamSpec {
    #[serde(rename = "type", default = "default_type")]
    pub kind: ParamType,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// The only values accepted. Named `enum` in the document.
    #[serde(rename = "enum", default, skip_serializing_if = "Vec::is_empty")]
    pub allowed: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

fn default_type() -> ParamType {
    ParamType::String
}

pub type Schema = BTreeMap<String, ParamSpec>;

/// A refusal, in terms a caller can act on without reading prose.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParamError {
    pub parameter: String,
    /// Stable: `param:unknown`, `param:missing`, `param:type`, `param:enum`,
    /// `param:range`, `param:pattern`.
    pub code: String,
    pub message: String,
    /// What the contract asks for, so a UI can render the right control and an
    /// agent can correct itself without guessing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
}

/// Validated parameters, ready to substitute.
#[derive(Debug, Clone, Default)]
pub struct Resolved {
    values: BTreeMap<String, String>,
    secrets: BTreeSet<String>,
}

impl Resolved {
    pub fn values(&self) -> &BTreeMap<String, String> {
        &self.values
    }

    pub fn is_secret(&self, name: &str) -> bool {
        self.secrets.contains(name)
    }

    /// The same parameters, safe to persist.
    ///
    /// Secrets are replaced rather than omitted: a run record that silently
    /// drops a parameter reads as though it was never supplied, and "was this
    /// run given a token?" is a question worth being able to answer.
    pub fn for_history(&self) -> BTreeMap<String, String> {
        self.values
            .iter()
            .map(|(k, v)| {
                let shown = if self.secrets.contains(k) { "***".to_string() } else { v.clone() };
                (k.clone(), shown)
            })
            .collect()
    }
}

fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn type_ok(kind: ParamType, v: &str) -> Result<(), &'static str> {
    match kind {
        ParamType::String | ParamType::Secret => Ok(()),
        ParamType::Integer => {
            v.trim().parse::<i64>().map(|_| ()).map_err(|_| "a whole number, e.g. 42")
        }
        ParamType::Number => {
            v.trim().parse::<f64>().map(|_| ()).map_err(|_| "a number, e.g. 3.5")
        }
        ParamType::Boolean => parse_bool(v).map(|_| ()).ok_or("true or false"),
        ParamType::Date => chrono::NaiveDate::parse_from_str(v.trim(), "%Y-%m-%d")
            .map(|_| ())
            .map_err(|_| "a date as YYYY-MM-DD"),
        ParamType::Datetime => chrono::DateTime::parse_from_rfc3339(v.trim())
            .map(|_| ())
            .map_err(|_| "a timestamp as RFC3339, e.g. 2026-01-31T09:00:00Z"),
    }
}

/// Validate supplied values against the contract, once.
///
/// Returns EVERY problem rather than the first. A caller filling in a form, or
/// an agent correcting itself, should not have to make one round trip per
/// mistake.
pub fn validate(
    schema: &Schema,
    supplied: &BTreeMap<String, String>,
) -> Result<Resolved, Vec<ParamError>> {
    let mut errors = Vec::new();
    let mut out = Resolved::default();

    // A name nobody declared is a typo far more often than it is a new
    // parameter, and a silently ignored typo means the run used the default and
    // nobody noticed.
    for name in supplied.keys() {
        if !schema.contains_key(name) {
            let mut near: Vec<&str> = schema
                .keys()
                .filter(|k| k.len().abs_diff(name.len()) <= 2)
                .map(String::as_str)
                .collect();
            near.sort();
            errors.push(ParamError {
                parameter: name.clone(),
                code: "param:unknown".into(),
                message: format!("{name} is not a parameter this pipeline declares"),
                expected: (!near.is_empty()).then(|| format!("one of: {}", near.join(", "))),
            });
        }
    }

    for (name, spec) in schema {
        let value = supplied.get(name).cloned().or_else(|| spec.default.clone());
        let Some(value) = value else {
            if spec.required {
                errors.push(ParamError {
                    parameter: name.clone(),
                    code: "param:missing".into(),
                    message: match &spec.description {
                        Some(d) => format!("{name} is required: {d}"),
                        None => format!("{name} is required"),
                    },
                    expected: Some(describe(spec)),
                });
            }
            continue;
        };

        if let Err(want) = type_ok(spec.kind, &value) {
            errors.push(ParamError {
                parameter: name.clone(),
                code: "param:type".into(),
                message: format!("{name} must be {want}"),
                expected: Some(describe(spec)),
            });
            continue;
        }
        if !spec.allowed.is_empty() && !spec.allowed.iter().any(|a| a == &value) {
            errors.push(ParamError {
                parameter: name.clone(),
                code: "param:enum".into(),
                // The value is echoed for every constraint EXCEPT a secret,
                // where echoing it would put it in a log.
                message: if spec.kind == ParamType::Secret {
                    format!("{name} is not one of the accepted values")
                } else {
                    format!("{name} is {value:?}, which is not one of the accepted values")
                },
                expected: Some(format!("one of: {}", spec.allowed.join(", "))),
            });
            continue;
        }
        if spec.minimum.is_some() || spec.maximum.is_some() {
            if let Ok(n) = value.trim().parse::<f64>() {
                let below = spec.minimum.is_some_and(|m| n < m);
                let above = spec.maximum.is_some_and(|m| n > m);
                if below || above {
                    errors.push(ParamError {
                        parameter: name.clone(),
                        code: "param:range".into(),
                        message: format!("{name} is {value}, which is outside the allowed range"),
                        expected: Some(describe(spec)),
                    });
                    continue;
                }
            }
        }
        if let Some(p) = &spec.pattern {
            match regex::Regex::new(p) {
                Ok(re) if !re.is_match(&value) => {
                    errors.push(ParamError {
                        parameter: name.clone(),
                        code: "param:pattern".into(),
                        message: format!("{name} does not match the required pattern"),
                        expected: Some(format!("matching {p}")),
                    });
                    continue;
                }
                // A pattern that will not compile is the pipeline's bug, not the
                // caller's, and refusing their value for it would be blaming the
                // wrong person.
                Err(_) => {}
                _ => {}
            }
        }

        if spec.kind == ParamType::Secret {
            out.secrets.insert(name.clone());
        }
        out.values.insert(name.clone(), value);
    }

    if errors.is_empty() {
        Ok(out)
    } else {
        errors.sort_by(|a, b| a.parameter.cmp(&b.parameter));
        Err(errors)
    }
}

fn describe(spec: &ParamSpec) -> String {
    let t = match spec.kind {
        ParamType::String => "a string",
        ParamType::Integer => "a whole number",
        ParamType::Number => "a number",
        ParamType::Boolean => "true or false",
        ParamType::Date => "a date as YYYY-MM-DD",
        ParamType::Datetime => "an RFC3339 timestamp",
        ParamType::Secret => "a secret value",
    };
    let mut s = t.to_string();
    if !spec.allowed.is_empty() {
        s.push_str(&format!(", one of: {}", spec.allowed.join(", ")));
    }
    match (spec.minimum, spec.maximum) {
        (Some(a), Some(b)) => s.push_str(&format!(", between {a} and {b}")),
        (Some(a), None) => s.push_str(&format!(", at least {a}")),
        (None, Some(b)) => s.push_str(&format!(", at most {b}")),
        _ => {}
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(json: serde_json::Value) -> Schema {
        serde_json::from_value(json).unwrap()
    }

    fn given(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn registry() -> Schema {
        schema(serde_json::json!({
            "jurisdiction": { "type": "string", "enum": ["BE", "NL", "GB"], "required": true },
            "effective_date": { "type": "date", "required": true },
            "full_refresh": { "type": "boolean", "default": "false" },
            "max_companies": { "type": "integer", "minimum": 1.0 },
            "api_token": { "type": "secret", "required": true }
        }))
    }

    #[test]
    fn a_complete_valid_set_resolves_and_applies_defaults() {
        let r = validate(
            &registry(),
            &given(&[
                ("jurisdiction", "BE"),
                ("effective_date", "2026-01-31"),
                ("api_token", "sk-live-1"),
            ]),
        )
        .expect("valid");
        assert_eq!(r.values().get("full_refresh").map(String::as_str), Some("false"), "the default");
        assert!(r.is_secret("api_token"));
        assert!(!r.is_secret("jurisdiction"));
    }

    /// The question a bare string cannot answer.
    #[test]
    fn a_wrong_type_is_refused_with_what_was_wanted() {
        let e = validate(
            &registry(),
            &given(&[
                ("jurisdiction", "BE"),
                ("effective_date", "31/01/2026"),
                ("api_token", "x"),
            ]),
        )
        .unwrap_err();
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].code, "param:type");
        assert_eq!(e[0].parameter, "effective_date");
        assert!(e[0].expected.as_deref().unwrap().contains("YYYY-MM-DD"));
    }

    /// A typo is far more likely than a new parameter, and a silently ignored
    /// one means the run used the default and nobody noticed.
    #[test]
    fn an_undeclared_parameter_is_refused_and_suggests_the_near_ones() {
        let e = validate(
            &registry(),
            &given(&[
                ("jurisdiciton", "BE"),
                ("jurisdiction", "BE"),
                ("effective_date", "2026-01-31"),
                ("api_token", "x"),
            ]),
        )
        .unwrap_err();
        assert_eq!(e[0].code, "param:unknown");
        assert!(e[0].expected.as_deref().unwrap().contains("jurisdiction"), "{:?}", e[0]);
    }

    #[test]
    fn a_missing_required_parameter_says_which_and_what_it_wants() {
        let e = validate(&registry(), &given(&[("jurisdiction", "BE")])).unwrap_err();
        let codes: Vec<&str> = e.iter().map(|x| x.code.as_str()).collect();
        assert_eq!(codes, vec!["param:missing", "param:missing"], "{e:?}");
        let names: Vec<&str> = e.iter().map(|x| x.parameter.as_str()).collect();
        assert_eq!(names, vec!["api_token", "effective_date"]);
    }

    #[test]
    fn a_value_outside_the_enum_or_the_range_is_refused() {
        let e = validate(
            &registry(),
            &given(&[
                ("jurisdiction", "FR"),
                ("effective_date", "2026-01-31"),
                ("api_token", "x"),
                ("max_companies", "0"),
            ]),
        )
        .unwrap_err();
        let by = |c: &str| e.iter().find(|x| x.code == c).cloned();
        assert!(by("param:enum").is_some(), "{e:?}");
        assert!(by("param:range").is_some(), "{e:?}");
        assert!(by("param:enum").unwrap().expected.unwrap().contains("BE, NL, GB"));
    }

    /// Every problem at once, or a form fills in one mistake per round trip.
    #[test]
    fn every_problem_is_reported_not_just_the_first() {
        let e = validate(&registry(), &given(&[("nope", "1"), ("jurisdiction", "FR")])).unwrap_err();
        assert!(e.len() >= 3, "expected unknown + enum + two missing, got {e:?}");
    }

    /// A secret is a declared type, not a guess from the name.
    #[test]
    fn a_secret_is_redacted_for_history_and_never_echoed_in_an_error() {
        let r = validate(
            &registry(),
            &given(&[
                ("jurisdiction", "BE"),
                ("effective_date", "2026-01-31"),
                ("api_token", "sk-live-super-secret"),
            ]),
        )
        .unwrap();
        let h = r.for_history();
        assert_eq!(h.get("api_token").map(String::as_str), Some("***"));
        assert_eq!(h.get("jurisdiction").map(String::as_str), Some("BE"), "only secrets are hidden");
        assert!(
            h.contains_key("api_token"),
            "replaced, not dropped - a missing key reads as never supplied"
        );

        // And a constraint failure on a secret must not put it in the message.
        let s = schema(serde_json::json!({
            "tok": { "type": "secret", "enum": ["a"], "required": true }
        }));
        let e = validate(&s, &given(&[("tok", "leak-me")])).unwrap_err();
        assert!(!e[0].message.contains("leak-me"), "the value reached an error: {:?}", e[0]);
    }

    /// A pattern the pipeline author wrote wrongly is their bug, not the
    /// caller's, and refusing the caller's value for it blames the wrong person.
    #[test]
    fn an_uncompilable_pattern_does_not_refuse_the_value() {
        let s = schema(serde_json::json!({ "x": { "type": "string", "pattern": "([" } }));
        assert!(validate(&s, &given(&[("x", "anything")])).is_ok());
    }

    #[test]
    fn booleans_accept_what_people_actually_type() {
        let s = schema(serde_json::json!({ "b": { "type": "boolean" } }));
        for v in ["true", "false", "1", "0", "yes", "no", "ON"] {
            assert!(validate(&s, &given(&[("b", v)])).is_ok(), "{v} should parse");
        }
        assert!(validate(&s, &given(&[("b", "maybe")])).is_err());
    }
}
