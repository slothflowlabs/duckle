//! #314: read DuckDB's diagnostics instead of throwing them away.
//!
//! DuckDB already says exactly what is wrong and where:
//!
//! ```text
//! Binder Error: Referenced column "regionn" not found in FROM clause!
//! Candidate bindings: "region"
//!
//! LINE 1: CREATE OR REPLACE VIEW n AS SELECT id, regionn, amount FROM input;
//!                                                ^
//! ```
//!
//! All of that was being collapsed into one error string, so the editor caught
//! a typo, learned nothing from it, and showed the node as simply not
//! resolving. The candidate list is the most useful part and was the first
//! thing lost.
//!
//! ## Pure on purpose
//!
//! Input text in, structure out. The engine tests skip themselves without a
//! DuckDB binary, and the part of this worth being sure about is the parsing -
//! so it is separated from anything that needs a process to run.
//!
//! ## A wrong position is worse than none
//!
//! The column DuckDB reports is an offset into the SQL **Duckle compiled**,
//! which is not the SQL the user wrote: a `WITH input AS (...)` preamble is
//! prepended, an extension prelude can be, and secrets are redacted for
//! display. Every position here is relative to the text that was actually run,
//! and translating it to the user's own SQL is the caller's job - one it must
//! decline when it cannot do it honestly.

use serde::Serialize;

/// One thing DuckDB objected to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostic {
    /// The coarse bucket, from [`crate::error_category`] rather than a second
    /// taxonomy of its own: `syntax` for a parser error, `schema` for a binder
    /// or catalog one.
    pub kind: String,
    /// DuckDB's own sentence, with its `Xxx Error: ` prefix removed - the
    /// prefix is what `kind` already says.
    pub message: String,
    /// 1-based, into the SQL that was run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    /// 1-based, into that line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
    /// What DuckDB suggested instead: `Candidate bindings:` for a column, `Did
    /// you mean` for a table. The single most useful part of the message and
    /// the first one a collapsed error string loses.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub candidates: Vec<String>,
}

/// The `LINE n: ` prefix DuckDB prints before the offending source line. The
/// caret on the following line is aligned to the whole printed line, so the
/// prefix has to come off to get a column into the SQL itself.
fn line_prefix(line: &str) -> Option<(u32, usize)> {
    let rest = line.strip_prefix("LINE ")?;
    let (number, _) = rest.split_once(':')?;
    let n: u32 = number.trim().parse().ok()?;
    Some((n, "LINE ".len() + number.len() + ": ".len()))
}

/// Names inside double quotes, in order, without duplicates.
///
/// Public because the caller needs the same extraction to find the offending
/// token in the user's own SQL: DuckDB truncates the source line it echoes
/// (`LINE 1: ... VIEW "q" AS ...`) whenever the statement is wide, and the
/// caret is then aligned to a window whose start is unknown - so for the common
/// case the token is the only honest way back to a position.
pub fn quoted(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find('"') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('"') else { break };
        let name = &after[..close];
        if !name.is_empty() && !out.iter().any(|s| s == name) {
            out.push(name.to_string());
        }
        rest = &after[close + 1..];
    }
    out
}

/// Everything DuckDB said, as structure.
///
/// One diagnostic per `Xxx Error:` line, so a run that reports several is not
/// reduced to its first.
pub fn parse(stderr: &str) -> Vec<Diagnostic> {
    let lines: Vec<&str> = stderr.lines().collect();
    let mut out: Vec<Diagnostic> = Vec::new();
    for (i, raw) in lines.iter().enumerate() {
        let line = raw.trim_end();
        // `Binder Error: ...`, `Parser Error: ...`, `Catalog Error: ...`, and
        // whatever else DuckDB names the same way.
        let Some(colon) = line.find(" Error: ") else { continue };
        if !line[..colon].chars().all(|c| c.is_ascii_alphabetic() || c == ' ') {
            continue;
        }
        let message = line[colon + " Error: ".len()..].trim().to_string();
        if message.is_empty() {
            continue;
        }
        let mut diagnostic = Diagnostic {
            kind: crate::error_category::categorize_error(line).to_string(),
            message,
            line: None,
            column: None,
            candidates: Vec::new(),
        };
        // Look ahead only as far as the next error, so two stacked messages do
        // not swap positions or candidates.
        for follow in lines.iter().skip(i + 1) {
            if follow.contains(" Error: ") {
                break;
            }
            let f = follow.trim_end();
            if let Some(rest) = f.strip_prefix("Candidate bindings:") {
                diagnostic.candidates.extend(quoted(rest));
                continue;
            }
            if f.trim_start().starts_with("Did you mean") {
                diagnostic.candidates.extend(quoted(f));
                continue;
            }
            if let Some((number, prefix)) = line_prefix(f) {
                diagnostic.line = Some(number);
                // The caret sits on the NEXT line, under the offending token.
                // A `LINE n: ...` that DuckDB truncated is a window into a long
                // line, so the caret's offset is not an offset into the SQL and
                // no column is reported at all - a wrong one would send the
                // reader to the wrong token with total confidence.
                let truncated = f[prefix.min(f.len())..].starts_with("...");
                if let Some(caret) = lines.iter().skip_while(|l| *l != follow).nth(1) {
                    if let Some(at) = caret.find('^') {
                        if !truncated && at >= prefix {
                            diagnostic.column = Some((at - prefix) as u32 + 1);
                        }
                    }
                }
                continue;
            }
        }
        out.push(diagnostic);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim from DuckDB 1.5.4, not from memory.
    const BINDER: &str = "Binder Error: Referenced column \"regionn\" not found in FROM clause!\nCandidate bindings: \"region\"\n\nLINE 1: CREATE OR REPLACE VIEW n AS SELECT id, regionn, amount FROM input;\n                                               ^\n";

    const PARSER: &str = "Parser Error: syntax error at or near \"WHERE\"\n\nLINE 1: CREATE OR REPLACE VIEW n AS SELECT FROM WHERE;\n                                                ^\n";

    const CATALOG: &str = "Catalog Error: Table with name no_such_table does not exist!\nDid you mean \"pg_tables\"?\n\nLINE 1: SELECT * FROM no_such_table;\n                      ^\n";

    #[test]
    fn a_missing_column_yields_its_name_position_and_the_near_one() {
        let d = parse(BINDER);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].kind, "schema");
        assert!(d[0].message.starts_with("Referenced column"), "{}", d[0].message);
        assert!(!d[0].message.contains("Binder Error"), "the prefix is what `kind` says");
        assert_eq!(d[0].line, Some(1));
        assert_eq!(d[0].candidates, vec!["region"], "the most useful part of the message");
        // The caret is under `regionn`, which starts at column 48 of the SQL
        // (1-based) once `LINE 1: ` is removed.
        let column = d[0].column.expect("a column") as usize;
        let sql = "CREATE OR REPLACE VIEW n AS SELECT id, regionn, amount FROM input;";
        assert_eq!(&sql[column - 1..column - 1 + 7], "regionn", "column {column} points elsewhere");
    }

    #[test]
    fn a_syntax_error_is_syntax_and_not_schema() {
        let d = parse(PARSER);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].kind, "syntax");
        assert_eq!(d[0].line, Some(1));
        assert!(d[0].candidates.is_empty());
    }

    #[test]
    fn did_you_mean_counts_as_a_candidate_too() {
        let d = parse(CATALOG);
        assert_eq!(d[0].kind, "schema");
        assert_eq!(d[0].candidates, vec!["pg_tables"]);
    }

    #[test]
    fn a_truncated_line_reports_no_column_at_all() {
        // DuckDB windows a long line with `...`, so the caret's offset is not
        // an offset into the SQL. A confident wrong position sends the reader
        // to the wrong token, which is worse than sending them nowhere.
        let text = "Binder Error: nope\n\nLINE 1: ...SELECT a, bb, ccc FROM input;\n                     ^\n";
        let d = parse(text);
        assert_eq!(d[0].line, Some(1));
        assert_eq!(d[0].column, None);
    }

    #[test]
    fn output_with_no_error_yields_nothing() {
        assert!(parse("").is_empty());
        assert!(parse("some rows\nand more\n").is_empty());
        // A column literally called "Error: " in data must not become one.
        assert!(parse("value\nno Error here\n").is_empty());
    }

    #[test]
    fn two_errors_do_not_share_one_position() {
        let text = format!("{BINDER}\n{PARSER}");
        let d = parse(&text);
        assert_eq!(d.len(), 2, "{d:?}");
        assert_eq!(d[0].candidates, vec!["region"]);
        assert!(d[1].candidates.is_empty(), "the second inherited the first's candidates");
        assert_ne!(d[0].column, d[1].column, "and its position");
    }

    #[test]
    fn a_message_with_no_position_still_parses() {
        let d = parse("Binder Error: something with no LINE at all\n");
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].line, None);
        assert_eq!(d[0].column, None);
    }
}
