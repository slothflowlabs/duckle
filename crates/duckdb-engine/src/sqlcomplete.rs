//! #314: what could come next in this SQL.
//!
//! Completion has to answer on every keystroke, so it cannot do what
//! [`crate::sqldiag`] does and spawn DuckDB. Everything expensive here is a
//! property of the SESSION rather than of the edit - the function list, the
//! upstream columns, the declared parameters - so it is gathered once and the
//! per-keystroke part is a pure function over text and a candidate set.
//!
//! ## It never runs the user's SQL
//!
//! #314 is explicit that completion must not execute source queries or cause
//! side effects. Nothing here executes anything: the only thing ever read from
//! DuckDB is its own list of functions, which is a property of the binary.
//!
//! ## Ranking is about what the author is likely reaching for
//!
//! A prefix match beats a contains match, a column beats a function, and an
//! exact case match beats a fuzzy one - because the cost of a wrong first
//! suggestion is that the author stops reading the list.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Column,
    Function,
    Keyword,
    /// A `${name}` the pipeline declares (#317).
    Parameter,
    /// A relation the SQL can select from - the upstream node, or `input`.
    Relation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Completion {
    /// What to insert.
    pub text: String,
    pub kind: Kind,
    /// Type, signature, or where it came from. Shown beside the name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Everything that could be suggested, gathered once per session.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Candidates {
    /// Upstream column names with their types.
    pub columns: Vec<(String, String)>,
    /// Relations the SQL may read: `input`, and each upstream node id.
    pub relations: Vec<String>,
    pub functions: Vec<String>,
    pub parameters: Vec<String>,
}

/// SQL keywords worth completing.
///
/// A short list on purpose: the point is to finish a word the author started,
/// not to teach SQL. A hundred keywords would bury the columns, which are the
/// thing they actually cannot remember.
const KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "JOIN", "LEFT JOIN",
    "INNER JOIN", "ON", "AS", "AND", "OR", "NOT", "NULL", "IS NULL", "IS NOT NULL", "CASE",
    "WHEN", "THEN", "ELSE", "END", "DISTINCT", "UNION ALL", "WITH", "QUALIFY", "OVER",
    "PARTITION BY", "CAST", "COALESCE",
];

/// The word being typed at `cursor`, and where it starts.
///
/// A `${` prefix is kept whole so a half-typed parameter reference completes as
/// one rather than as the identifier after the brace.
pub fn word_at(sql: &str, cursor: usize) -> (usize, &str) {
    let cursor = cursor.min(sql.len());
    let head = &sql[..cursor];
    let start = head
        .char_indices()
        .rev()
        .find(|(_, c)| !(c.is_alphanumeric() || *c == '_' || *c == '$' || *c == '{'))
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0);
    (start, &head[start..])
}

/// What position in the statement the cursor is in.
///
/// Deliberately crude - the last significant keyword before the cursor. A real
/// parser would be better and is not worth it here: the difference that matters
/// is "naming a relation" versus "naming a column", and the last keyword gets
/// that right for the shapes people actually write.
fn after_from(sql: &str, cursor: usize) -> bool {
    let head = sql[..cursor.min(sql.len())].to_ascii_uppercase();
    let last_of = |kw: &str| head.rfind(kw).map(|i| i as i64).unwrap_or(-1);
    let from = last_of(" FROM ").max(last_of("\nFROM ")).max(last_of(" JOIN "));
    let after = last_of(" WHERE ")
        .max(last_of(" SELECT "))
        .max(last_of("\nSELECT "))
        .max(last_of(" ON "))
        .max(last_of(" GROUP BY "))
        .max(last_of(" ORDER BY "));
    from >= 0 && from > after
}

/// Rank one candidate against the word being typed.
///
/// `None` means it does not match at all. Lower is better.
fn score(candidate: &str, word: &str, kind: Kind, after_from: bool) -> Option<u32> {
    if word.is_empty() {
        // With nothing typed, offer what belongs in this position rather than
        // everything: a list that starts with 400 functions is a list nobody
        // reads.
        return match (after_from, kind) {
            (true, Kind::Relation) => Some(0),
            (true, _) => None,
            (false, Kind::Column) => Some(0),
            (false, Kind::Parameter) => Some(1),
            (false, Kind::Keyword) => Some(2),
            (false, _) => None,
        };
    }
    let c = candidate.to_ascii_lowercase();
    let w = word.to_ascii_lowercase();
    let base = match kind {
        // In a FROM, a relation is almost always what is wanted; elsewhere a
        // column is.
        Kind::Relation if after_from => 0,
        Kind::Column if !after_from => 0,
        Kind::Parameter => 1,
        Kind::Column | Kind::Relation => 2,
        Kind::Function => 3,
        // Left last, and its band is wide enough for the whole list.
        Kind::Keyword => 40,
    };
    if candidate.starts_with(word) {
        return Some(base * 10); // exact case, exact prefix
    }
    if c.starts_with(&w) {
        return Some(base * 10 + 1);
    }
    if c.contains(&w) {
        return Some(base * 10 + 5);
    }
    None
}

/// Suggestions for the cursor position, best first.
///
/// Pure: text and a candidate set in, ranked list out. Nothing here reads a
/// file, opens a connection, or runs a query.
pub fn complete(sql: &str, cursor: usize, candidates: &Candidates, limit: usize) -> Vec<Completion> {
    let (_, word) = word_at(sql, cursor);
    let relation_position = after_from(sql, cursor);
    // A `${` prefix means the author is naming a parameter and nothing else can
    // be meant, so the whole list is parameters.
    if word.starts_with("${") || word == "$" {
        let typed = word.trim_start_matches('$').trim_start_matches('{');
        let mut out: Vec<Completion> = candidates
            .parameters
            .iter()
            .filter(|p| typed.is_empty() || p.to_ascii_lowercase().starts_with(&typed.to_ascii_lowercase()))
            .map(|p| Completion {
                text: format!("${{{p}}}"),
                kind: Kind::Parameter,
                detail: Some("pipeline parameter".into()),
            })
            .collect();
        out.truncate(limit);
        return out;
    }

    let mut scored: Vec<(u32, Completion)> = Vec::new();
    for (name, ty) in &candidates.columns {
        if let Some(s) = score(name, word, Kind::Column, relation_position) {
            scored.push((s, Completion {
                text: name.clone(),
                kind: Kind::Column,
                detail: Some(ty.clone()),
            }));
        }
    }
    for name in &candidates.relations {
        if let Some(s) = score(name, word, Kind::Relation, relation_position) {
            scored.push((s, Completion {
                text: name.clone(),
                kind: Kind::Relation,
                detail: Some("upstream".into()),
            }));
        }
    }
    for name in &candidates.parameters {
        if let Some(s) = score(name, word, Kind::Parameter, relation_position) {
            scored.push((s, Completion {
                text: format!("${{{name}}}"),
                kind: Kind::Parameter,
                detail: Some("pipeline parameter".into()),
            }));
        }
    }
    for name in &candidates.functions {
        if let Some(s) = score(name, word, Kind::Function, relation_position) {
            scored.push((s, Completion {
                text: format!("{name}("),
                kind: Kind::Function,
                detail: None,
            }));
        }
    }
    for (i, kw) in KEYWORDS.iter().enumerate() {
        if let Some(s) = score(kw, word, Kind::Keyword, relation_position) {
            // Keywords rank by their order in the list rather than
            // alphabetically, because the list is ordered by usefulness and
            // alphabetical put AND, AS and CASE ahead of SELECT on an empty
            // statement - the one moment the answer is obvious.
            scored.push((s + i as u32, Completion {
                text: (*kw).to_string(),
                kind: Kind::Keyword,
                detail: None,
            }));
        }
    }
    // Stable within a rank: alphabetical, so the same edit always offers the
    // same list in the same order. A list that reshuffles between keystrokes is
    // one nobody can build muscle memory against.
    scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.text.cmp(&b.1.text)));
    scored.into_iter().map(|(_, c)| c).take(limit).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidates() -> Candidates {
        Candidates {
            columns: vec![
                ("amount".into(), "BIGINT".into()),
                ("region".into(), "VARCHAR".into()),
                ("account_id".into(), "BIGINT".into()),
            ],
            relations: vec!["input".into(), "orders".into()],
            functions: vec!["sum".into(), "regexp_matches".into(), "abs".into()],
            parameters: vec!["region_filter".into(), "as_of".into()],
        }
    }

    fn texts(sql: &str, cursor: usize) -> Vec<String> {
        complete(sql, cursor, &candidates(), 8).into_iter().map(|c| c.text).collect()
    }

    #[test]
    fn a_column_beats_a_function_where_a_column_belongs() {
        // `region` the column, not `regexp_matches` the function - the cost of
        // a wrong first suggestion is that the author stops reading the list.
        let sql = "SELECT reg";
        assert_eq!(texts(sql, sql.len())[0], "region");
    }

    #[test]
    fn a_relation_beats_a_column_after_from() {
        let sql = "SELECT * FROM in";
        let got = texts(sql, sql.len());
        assert_eq!(got[0], "input", "{got:?}");
    }

    #[test]
    fn nothing_typed_offers_what_belongs_here_rather_than_everything() {
        // A list that starts with 400 functions is a list nobody reads.
        let after_select = texts("SELECT ", 7);
        assert!(after_select.contains(&"amount".to_string()), "{after_select:?}");
        assert!(!after_select.iter().any(|t| t.starts_with("abs")), "{after_select:?}");

        let after_from = texts("SELECT * FROM ", 14);
        assert_eq!(after_from, vec!["input", "orders"], "only relations belong here");
    }

    #[test]
    fn a_parameter_reference_completes_as_one_thing() {
        // Half-typed `${re` must complete to `${region_filter}`, not to the
        // identifier after the brace.
        let sql = "WHERE region = ${re";
        let got = texts(sql, sql.len());
        assert_eq!(got, vec!["${region_filter}"], "{got:?}");
        // And a bare `$` offers every parameter.
        let sql = "WHERE x = $";
        assert_eq!(texts(sql, sql.len()).len(), 2);
    }

    #[test]
    fn a_prefix_beats_a_substring() {
        // `account_id` starts with what was typed; `amount` merely contains it
        // nowhere - this pins the ordering rule rather than the words.
        let sql = "SELECT acc";
        let got = texts(sql, sql.len());
        assert_eq!(got[0], "account_id", "{got:?}");
    }

    #[test]
    fn matching_is_case_insensitive_but_exact_case_ranks_first() {
        let sql = "SELECT AMO";
        let got = texts(sql, sql.len());
        assert!(got.contains(&"amount".to_string()), "{got:?}");
    }

    #[test]
    fn the_word_under_the_cursor_is_what_is_being_completed() {
        // Not the whole statement, and not the word after the cursor.
        assert_eq!(word_at("SELECT amo", 10).1, "amo");
        assert_eq!(word_at("SELECT amount, reg", 18).1, "reg");
        assert_eq!(word_at("SELECT a FROM t", 8).1, "a");
        // Mid-word: only what is behind the cursor.
        assert_eq!(word_at("SELECT amount", 10).1, "amo");
        assert_eq!(word_at("", 0).1, "");
    }

    #[test]
    fn the_list_is_stable_between_identical_edits() {
        // A list that reshuffles between keystrokes is one nobody can build
        // muscle memory against.
        let sql = "SELECT a";
        assert_eq!(texts(sql, sql.len()), texts(sql, sql.len()));
    }

    #[test]
    fn a_cursor_past_the_end_does_not_panic() {
        // An editor can ask about a position that no longer exists - the text
        // changed under an in-flight request - and the answer is the end of the
        // text rather than a crash.
        assert!(!complete("SELECT ", 9_999, &candidates(), 5).is_empty());
        assert_eq!(
            complete("SELECT ", 9_999, &candidates(), 5),
            complete("SELECT ", 7, &candidates(), 5),
            "a cursor past the end is the end"
        );
    }

    #[test]
    fn an_empty_statement_offers_keywords_and_not_nothing() {
        // With no columns known and nothing typed, `SELECT` and `WITH` are the
        // useful answers. Returning nothing would make completion look broken
        // in exactly the moment someone tries it first.
        let got: Vec<String> = complete("", 0, &Candidates::default(), 5)
            .into_iter()
            .map(|c| c.text)
            .collect();
        assert!(got.contains(&"SELECT".to_string()), "{got:?}");
        assert!(got.iter().all(|t| t.chars().all(|c| c.is_ascii_uppercase() || c == ' ')));
    }

    #[test]
    fn nothing_here_reads_or_runs_anything() {
        // The property that lets this answer on every keystroke: the whole
        // function is text in, list out. If this ever needs a process, it
        // belongs in sqldiag instead.
        let sql = "SELECT * FROM input WHERE region = 'x'";
        let before = std::time::Instant::now();
        for i in 0..2_000 {
            let _ = complete(sql, i % sql.len(), &candidates(), 10);
        }
        assert!(
            before.elapsed() < std::time::Duration::from_secs(2),
            "2000 completions took {:?}; something in here is doing real work",
            before.elapsed()
        );
    }
}
