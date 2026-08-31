//! #312: machine-readable output for the checking commands.
//!
//! CI systems and coding agents should not have to scrape console text. Three
//! shapes cover what they actually consume:
//!
//! - **json** - a versioned envelope, so a consumer can tell when the shape
//!   changed instead of discovering it through a parse error.
//! - **junit** - what every CI system already renders as a test report.
//! - **sarif** - what GitHub Code Scanning and most editors read, so a finding
//!   lands on the file it is about rather than in a log nobody opens.
//!
//! ## Exit codes are part of the contract
//!
//! A format is useless if the caller still has to grep to know whether to fail
//! the build:
//!
//! | code | meaning |
//! |---|---|
//! | 0 | everything checked passed |
//! | 1 | a check failed - the thing being checked is wrong |
//! | 2 | the tool could not run - bad flag, unreadable file, no input |
//!
//! 1 and 2 are deliberately different: a CI job wants to fail loudly on 1 and
//! usually wants to fail differently on 2, because 2 means the gate did not
//! actually run and treating it as a pass is how a broken gate goes unnoticed.

/// One check, passing or failing.
///
/// A passing finding is kept rather than filtered, because JUnit reports need
/// the passes to show what was covered - a report with two failures and no
/// passes cannot be told from a report where only two things ran.
#[derive(Debug, Clone)]
pub struct Finding {
    /// The file the finding is about, as given on the command line.
    pub file: String,
    /// The node inside it, when the finding is about one.
    pub node: Option<String>,
    /// A short stable identifier for the KIND of check, for SARIF rules and for
    /// a consumer that wants to filter without reading prose.
    pub rule: String,
    pub message: String,
    pub ok: bool,
}

impl Finding {
    pub fn pass(file: &str, rule: &str, message: String) -> Self {
        Finding { file: file.into(), node: None, rule: rule.into(), message, ok: true }
    }
    pub fn fail(file: &str, rule: &str, message: String) -> Self {
        Finding { file: file.into(), node: None, rule: rule.into(), message, ok: false }
    }
}

/// The version of the JSON envelope. Bump when a consumer would break.
pub const SCHEMA_VERSION: u32 = 1;

pub fn json(command: &str, findings: &[Finding], extra: serde_json::Value) -> String {
    let failed = findings.iter().filter(|f| !f.ok).count();
    let mut doc = serde_json::json!({
        "schemaVersion": SCHEMA_VERSION,
        "command": command,
        "ok": failed == 0,
        "checked": findings.len(),
        "failed": failed,
        "findings": findings
            .iter()
            .map(|f| {
                let mut o = serde_json::json!({
                    "file": f.file,
                    "rule": f.rule,
                    "ok": f.ok,
                    "message": f.message,
                });
                if let Some(n) = &f.node {
                    o["node"] = serde_json::Value::String(n.clone());
                }
                o
            })
            .collect::<Vec<_>>(),
    });
    if let serde_json::Value::Object(add) = extra {
        for (k, v) in add {
            doc[k] = v;
        }
    }
    serde_json::to_string_pretty(&doc).unwrap_or_default()
}

/// Escape text for XML character data and attribute values.
///
/// Doing this by hand rather than pulling a writer in, because the escaping IS
/// the risk: a SQL error message routinely contains `<`, `&` and quotes, and an
/// unescaped one produces a report file that the CI system silently fails to
/// parse - which reads as "no tests ran", the worst possible answer from a gate.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            // XML 1.0 forbids most control characters outright; a raw one makes
            // the document unparseable, so they are dropped rather than escaped.
            c if (c as u32) < 0x20 && c != '\n' && c != '\t' && c != '\r' => {}
            c => out.push(c),
        }
    }
    out
}

pub fn junit(command: &str, findings: &[Finding]) -> String {
    let failed = findings.iter().filter(|f| !f.ok).count();
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(&format!(
        "<testsuites name=\"duckle {}\" tests=\"{}\" failures=\"{}\">\n",
        xml_escape(command),
        findings.len(),
        failed
    ));
    out.push_str(&format!(
        "  <testsuite name=\"{}\" tests=\"{}\" failures=\"{}\">\n",
        xml_escape(command),
        findings.len(),
        failed
    ));
    for f in findings {
        let name = match &f.node {
            Some(n) => format!("{} :: {}", f.file, n),
            None => f.file.clone(),
        };
        out.push_str(&format!(
            "    <testcase classname=\"{}\" name=\"{}\"",
            xml_escape(&f.rule),
            xml_escape(&name)
        ));
        if f.ok {
            out.push_str(" />\n");
        } else {
            out.push_str(">\n");
            out.push_str(&format!(
                "      <failure message=\"{}\">{}</failure>\n",
                xml_escape(f.message.lines().next().unwrap_or("")),
                xml_escape(&f.message)
            ));
            out.push_str("    </testcase>\n");
        }
    }
    out.push_str("  </testsuite>\n</testsuites>\n");
    out
}

pub fn sarif(command: &str, findings: &[Finding]) -> String {
    // Only failures become SARIF results: SARIF is a findings format, and a
    // "result" for something that passed would show up as an annotation on a
    // file that is fine.
    let rules: std::collections::BTreeSet<&str> =
        findings.iter().filter(|f| !f.ok).map(|f| f.rule.as_str()).collect();
    let doc = serde_json::json!({
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [{
            "tool": { "driver": {
                "name": "duckle",
                "informationUri": "https://duckle.org",
                "rules": rules.iter().map(|r| serde_json::json!({
                    "id": r,
                    "shortDescription": { "text": format!("duckle {command}: {r}") }
                })).collect::<Vec<_>>(),
            }},
            "results": findings.iter().filter(|f| !f.ok).map(|f| {
                let mut r = serde_json::json!({
                    "ruleId": f.rule,
                    "level": "error",
                    "message": { "text": f.message },
                    "locations": [{
                        "physicalLocation": {
                            // A workspace-relative URI, and forward slashes, or
                            // Code Scanning cannot match it to a repo file.
                            "artifactLocation": { "uri": f.file.replace('\\', "/") }
                        }
                    }],
                });
                if let Some(n) = &f.node {
                    r["partialFingerprints"] = serde_json::json!({ "duckleNode": n });
                }
                r
            }).collect::<Vec<_>>(),
        }]
    });
    serde_json::to_string_pretty(&doc).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<Finding> {
        vec![
            Finding::pass("pipelines/ok.json", "compile", "3 stages".into()),
            Finding::fail(
                "pipelines/bad.json",
                "compile",
                "Binder Error: column \"a\" not found <in> 'x' & 'y'".into(),
            ),
        ]
    }

    /// The envelope is versioned, so a consumer can tell a shape change from a
    /// parse error.
    #[test]
    fn json_is_versioned_and_counts_what_failed() {
        let v: serde_json::Value = serde_json::from_str(&json("validate", &sample(), serde_json::json!({}))).unwrap();
        assert_eq!(v["schemaVersion"], 1);
        assert_eq!(v["command"], "validate");
        assert_eq!(v["ok"], false);
        assert_eq!(v["checked"], 2);
        assert_eq!(v["failed"], 1);
    }

    /// The escaping IS the risk. A SQL error routinely contains < and &, and an
    /// unescaped report is one a CI system silently fails to parse, which reads
    /// as "no tests ran" - the worst answer a gate can give.
    #[test]
    fn junit_escapes_what_would_break_the_parser() {
        let x = junit("validate", &sample());
        assert!(!x.contains("<in>"), "raw markup must not survive: {x}");
        assert!(x.contains("&lt;in&gt;"), "got: {x}");
        assert!(x.contains("&amp;"), "a bare ampersand breaks XML: {x}");
        assert!(x.contains("failures=\"1\""));
        // A passing case is still present, or the report cannot show coverage.
        assert!(x.contains("pipelines/ok.json"));
    }

    /// A control character makes an XML document unparseable, so it is dropped
    /// rather than escaped.
    #[test]
    fn junit_drops_control_characters() {
        let f = vec![Finding::fail("a.json", "compile", "bad\u{0007}value".into())];
        let x = junit("validate", &f);
        assert!(!x.contains('\u{0007}'), "a control char must not reach the file");
        assert!(x.contains("badvalue"));
    }

    /// SARIF is a findings format: a passing check is not a finding, and
    /// emitting one would annotate a file that is fine.
    #[test]
    fn sarif_reports_only_failures_and_locates_them() {
        let v: serde_json::Value = serde_json::from_str(&sarif("validate", &sample())).unwrap();
        assert_eq!(v["version"], "2.1.0");
        let results = v["runs"][0]["results"].as_array().unwrap();
        assert_eq!(results.len(), 1, "only the failure is a finding");
        assert_eq!(
            results[0]["locations"][0]["physicalLocation"]["artifactLocation"]["uri"],
            "pipelines/bad.json"
        );
        let rules = v["runs"][0]["tool"]["driver"]["rules"].as_array().unwrap();
        assert_eq!(rules.len(), 1, "one rule, not one per finding");
    }

    /// Code Scanning matches a URI against repo paths, and a Windows separator
    /// never matches.
    #[test]
    fn sarif_uris_use_forward_slashes() {
        let f = vec![Finding::fail("pipelines\\sub\\bad.json", "compile", "x".into())];
        let v: serde_json::Value = serde_json::from_str(&sarif("validate", &f)).unwrap();
        assert_eq!(
            v["runs"][0]["results"][0]["locations"][0]["physicalLocation"]["artifactLocation"]["uri"],
            "pipelines/sub/bad.json"
        );
    }

    /// Everything passing is a clean report rather than an empty one.
    #[test]
    fn a_clean_run_still_produces_a_valid_report() {
        let f = vec![Finding::pass("a.json", "compile", "1 stage".into())];
        let v: serde_json::Value = serde_json::from_str(&json("validate", &f, serde_json::json!({}))).unwrap();
        assert_eq!(v["ok"], true);
        let s: serde_json::Value = serde_json::from_str(&sarif("validate", &f)).unwrap();
        assert_eq!(s["runs"][0]["results"].as_array().unwrap().len(), 0);
        assert!(junit("validate", &f).contains("failures=\"0\""));
    }
}
