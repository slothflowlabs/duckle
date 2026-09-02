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

/// #314: what can honestly be said about SQL destined for another engine.
///
/// A source's query runs on Postgres, BigQuery or Snowflake. Checking it with
/// DuckDB would tell you about DuckDB, so nothing here parses the SQL against
/// any dialect. What it reports is the small set of things that are wrong in
/// EVERY dialect, plus one that is wrong in Duckle regardless of dialect.
///
/// Deliberately not here: "that function does not exist in BigQuery". Duckle
/// does not know what your BigQuery has, and a confident wrong warning about a
/// perfectly good query is worse than no warning - people stop reading the ones
/// that are right.
///
/// Also not here: a trailing semicolon. Duckle trims it before wrapping the
/// query, so warning about it would be a false alarm about something already
/// handled.
pub fn remote_hints(sql: &str) -> Vec<Diagnostic> {
    let mut out = Vec::new();
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return out;
    }
    let hint = |message: String| Diagnostic {
        kind: "hint".to_string(),
        message,
        line: None,
        column: None,
        candidates: Vec::new(),
    };

    // 1. An unclosed quote or bracket. Wrong everywhere, and the error the
    //    remote engine returns for it is usually about a token far from the
    //    real mistake.
    let mut single = 0usize;
    let mut double = 0usize;
    let mut depth: i64 = 0;
    for c in trimmed.chars() {
        match c {
            // Counting is enough for an escaped quote: `''` inside a literal is
            // two quotes, so a balanced string stays balanced. A special case
            // for it was written and did nothing that counting did not already
            // do.
            '\'' if double % 2 == 0 => single += 1,
            '"' if single % 2 == 0 => double += 1,
            '(' if single % 2 == 0 && double % 2 == 0 => depth += 1,
            ')' if single % 2 == 0 && double % 2 == 0 => depth -= 1,
            _ => {}
        }
    }
    if single % 2 == 1 {
        out.push(hint("an unclosed single quote".into()));
    }
    if double % 2 == 1 {
        out.push(hint("an unclosed double quote".into()));
    }
    if depth != 0 {
        out.push(hint(match depth > 0 {
            true => format!("{depth} unclosed parenthesis/es"),
            false => format!("{} unmatched closing parenthesis/es", -depth),
        }));
    }

    // 2. More than one statement. Duckle wraps a source query as
    //    `SELECT * FROM (<your sql>)`, so a second statement is a syntax error
    //    wherever it lands - it never runs, whatever the engine.
    let outside_literals: String = {
        let (mut s, mut in_single, mut in_double) = (String::new(), false, false);
        for c in trimmed.chars() {
            match c {
                '\'' if !in_double => in_single = !in_single,
                '"' if !in_single => in_double = !in_double,
                _ => {}
            }
            s.push(if in_single || in_double { ' ' } else { c });
        }
        s
    };
    if outside_literals.trim_end_matches(';').contains(';') {
        out.push(hint(
            "more than one statement: a source sends ONE query, wrapped as `SELECT * FROM (...)`, so anything after the first semicolon is a syntax error rather than a second step"
                .into(),
        ));
    }

    // 3. A statement that does not return rows, in a position that reads them.
    let first = outside_literals
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    if matches!(
        first.as_str(),
        "INSERT" | "UPDATE" | "DELETE" | "DROP" | "TRUNCATE" | "ALTER" | "CREATE" | "GRANT"
    ) {
        out.push(hint(format!(
            "starts with {first}, and a source position expects something that returns rows - Duckle wraps it as `SELECT * FROM (...)`. If the remote system must be changed, that is a job for a step that is allowed to change it."
        )));
    }

    // 4. A placeholder nothing filled in. Not a dialect question at all: it
    //    would be sent to the remote system literally, as the characters
    //    `${name}`.
    if let Some(start) = outside_literals.find("${") {
        let name: String = outside_literals[start + 2..]
            .chars()
            .take_while(|c| *c != '}')
            .collect();
        out.push(hint(format!(
            "`${{{name}}}` was not substituted, so it would reach the remote system as those characters. Declare it as a parameter, or set it in the context."
        )));
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
    fn an_unclosed_quote_is_wrong_on_every_engine() {
        let h = remote_hints("SELECT * FROM t WHERE name = 'ada");
        assert_eq!(h.len(), 1, "{h:?}");
        assert!(h[0].message.contains("unclosed single quote"), "{}", h[0].message);
        assert_eq!(h[0].kind, "hint", "a hint, not a validation");
        // A doubled quote is an escaped one, not a close and an open.
        assert!(remote_hints("SELECT 'it''s fine' FROM t").is_empty());
    }

    #[test]
    fn unbalanced_parentheses_are_counted_not_guessed() {
        assert!(remote_hints("SELECT * FROM (SELECT 1").is_empty() == false);
        assert!(remote_hints("SELECT * FROM (SELECT 1)").is_empty());
        // A bracket inside a literal is text, not structure.
        assert!(remote_hints("SELECT '(' FROM t").is_empty());
    }

    #[test]
    fn a_second_statement_never_runs_so_it_is_worth_saying() {
        // A source query is wrapped as `SELECT * FROM (...)`, so anything after
        // the first semicolon is a syntax error rather than a second step.
        let h = remote_hints("SELECT 1; SELECT 2");
        assert!(h.iter().any(|d| d.message.contains("more than one statement")), "{h:?}");
        // A trailing semicolon is trimmed by Duckle, so it is NOT a hint - a
        // warning about something already handled teaches people to ignore
        // warnings.
        assert!(remote_hints("SELECT 1;").is_empty());
        // And a semicolon inside a string is not a statement break.
        assert!(remote_hints("SELECT 'a;b' FROM t").is_empty());
    }

    #[test]
    fn a_write_in_a_read_position_is_flagged() {
        let h = remote_hints("DELETE FROM orders WHERE id = 1");
        assert!(h.iter().any(|d| d.message.contains("returns rows")), "{h:?}");
        for ok in ["SELECT 1", "WITH t AS (SELECT 1) SELECT * FROM t", "select * from x"] {
            assert!(remote_hints(ok).is_empty(), "{ok} was flagged");
        }
    }

    #[test]
    fn an_unsubstituted_placeholder_would_be_sent_literally() {
        // Not a dialect question at all: those characters reach the remote
        // system as themselves.
        let h = remote_hints("SELECT * FROM t WHERE d = ${as_of}");
        assert!(h.iter().any(|d| d.message.contains("as_of")), "{h:?}");
        assert!(h[0].message.contains("not substituted"), "{}", h[0].message);
    }

    #[test]
    fn nothing_here_claims_to_know_the_remote_dialect() {
        // The line this must not cross: Duckle does not know what functions
        // that engine has, and a confident wrong warning about a good query is
        // worse than none - people stop reading the ones that are right.
        for fine in [
            "SELECT ARRAY_AGG(x) FROM t",              // Postgres
            "SELECT GENERATE_UUID()",                   // BigQuery
            "SELECT toDateTime(x) FROM t",              // ClickHouse
            "SELECT LISTAGG(x, ',') FROM t",            // Oracle / Snowflake
            "SELECT * FROM t QUALIFY row_number() OVER () = 1",
        ] {
            assert!(remote_hints(fine).is_empty(), "{fine} was flagged as a problem");
        }
    }

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
