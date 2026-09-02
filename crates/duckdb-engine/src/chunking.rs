//! #306: split one enormous extract into bounded, deterministic chunks.
//!
//! A single query over a billion-row table holds a snapshot for hours, fails
//! near the end and restarts from zero, and shows no progress while it does.
//! Chunking replaces it with N bounded queries that can be retried
//! individually - but only where the source can actually give stable
//! semantics, which is the part this module is careful about.
//!
//! ## Planning is pure; probing and executing are not
//!
//! `plan` takes the bounds it needs as values, so every predicate this will
//! ever send to a database can be tested without one. The bounds themselves
//! come from a probe query, which is a different concern and a different
//! function.
//!
//! ## Refusing is a feature
//!
//! #306 asks that unsupported connectors fail clearly rather than silently
//! doing something unsafe. So every refusal here carries the reason, and the
//! reasons are specific: a nullable key silently drops rows from every chunk,
//! a non-plain column name is SQL injection, and a source that cannot pin a
//! snapshot cannot promise that N queries saw one consistent table.
//!
//! ## What a chunk plan does NOT claim
//!
//! Chunks are separate queries. Unless the connector pins a snapshot, they see
//! the table at N different moments, and a row written between them is in one
//! chunk or none. That is stated in the plan rather than hidden, because the
//! alternative - implying consistency the source cannot give - is how a
//! backfill silently loses rows.

use serde::{Deserialize, Serialize};

/// What the source can be asked to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Strategy {
    /// Contiguous ranges over an ordered numeric key.
    #[serde(rename_all = "camelCase")]
    Range { column: String, chunk_size: u64 },
    /// One chunk per calendar interval over a date/timestamp column.
    #[serde(rename_all = "camelCase")]
    Time { column: String, interval: crate::partition::Cadence },
    /// Modulo buckets over a hash of the key, for keys with no usable order.
    #[serde(rename_all = "camelCase")]
    Hash { column: String, buckets: u32 },
}

impl Strategy {
    pub fn column(&self) -> &str {
        match self {
            Strategy::Range { column, .. }
            | Strategy::Time { column, .. }
            | Strategy::Hash { column, .. } => column,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Strategy::Range { .. } => "range",
            Strategy::Time { .. } => "time",
            Strategy::Hash { .. } => "hash",
        }
    }
}

/// What the source promises about consistency across the chunks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Snapshot {
    /// Every chunk reads the same committed state, pinned by this identifier -
    /// an Oracle SCN, a Postgres exported snapshot.
    Pinned { id: String },
    /// Each chunk sees the table when it runs. Rows written in between are in
    /// one chunk or none.
    BestEffort,
    /// Bounded by a cutoff the caller supplied, so late arrivals are excluded
    /// rather than half-included.
    Watermark { column: String, at: String },
}

impl Snapshot {
    pub fn describes_one_state(&self) -> bool {
        matches!(self, Snapshot::Pinned { .. })
    }
}

/// The SQL family a predicate is being written for.
///
/// Only where the families genuinely differ. Hash is the one that does: there
/// is no portable spelling of "bucket this key".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    Postgres,
    Oracle,
    MsSql,
    MySql,
    Duckdb,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    pub id: usize,
    /// Human-readable: `1..1000000`, `2020-03`, `bucket 7 of 64`.
    pub key: String,
    /// The WHERE fragment for this chunk, already safe to interpolate because
    /// the column was validated and the bounds are numbers or quoted literals.
    pub predicate: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub strategy: String,
    pub column: String,
    pub chunks: Vec<Chunk>,
    pub concurrency: usize,
    pub snapshot: Snapshot,
    /// Everything the operator should know before running it, in words.
    pub notes: Vec<String>,
}

/// The numeric or temporal extent of the key, from a probe query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bounds {
    /// Inclusive min and max of an integer key.
    Range { min: i64, max: i64 },
    /// Inclusive first and last date, `YYYY-MM-DD`.
    Time { from: String, to: String },
    /// Hash needs no bounds - the buckets are the plan.
    None,
}

/// A column name safe to interpolate into SQL.
///
/// Refused rather than escaped: a pipeline file is not a trusted source of SQL
/// fragments, and quoting rules differ per family. The same rule the Oracle
/// parallel reader already applies, lifted so every strategy gets it.
pub fn plain_column(name: &str) -> Result<String, String> {
    let c = name.trim();
    if c.is_empty() || c.len() > 128 {
        return Err(format!("{name:?} is not a plain column name"));
    }
    if !c.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$' || ch == '#') {
        return Err(format!(
            "{name:?} is not a plain column name, and a chunk column is interpolated into SQL"
        ));
    }
    Ok(c.to_string())
}

/// The probe a caller must run to learn the bounds of a range or time key.
///
/// Also counts NULLs, because a nullable key silently drops rows from every
/// chunk: `id < 100 OR id >= 100` excludes every NULL, and the extract simply
/// comes out short with nothing to show for it.
pub fn probe_sql(strategy: &Strategy, table: &str) -> Result<String, String> {
    let column = plain_column(strategy.column())?;
    Ok(match strategy {
        Strategy::Hash { .. } => format!(
            "SELECT COUNT(*) AS nulls FROM {table} WHERE {column} IS NULL"
        ),
        _ => format!(
            "SELECT MIN({column}) AS lo, MAX({column}) AS hi, \
             COUNT(*) - COUNT({column}) AS nulls FROM {table}"
        ),
    })
}

/// Build the chunk plan.
///
/// `nulls` is what the probe found. A nullable key is refused rather than
/// warned about: every predicate below excludes NULL, so the extract would be
/// short by exactly that many rows and nothing would say so.
pub fn plan(
    strategy: &Strategy,
    bounds: &Bounds,
    nulls: u64,
    concurrency: usize,
    snapshot: Snapshot,
    dialect: Dialect,
) -> Result<Plan, String> {
    let column = plain_column(strategy.column())?;
    if nulls > 0 {
        return Err(format!(
            "{column} has {nulls} NULL row(s), and every chunk predicate excludes NULL - the \
             extract would silently come out {nulls} row(s) short. Use a NOT NULL key, or add a \
             chunk for the NULLs deliberately."
        ));
    }
    let mut notes = Vec::new();
    if !snapshot.describes_one_state() {
        notes.push(
            "chunks are separate queries: a row written while the extract runs is in one chunk \
             or none. This source cannot pin a snapshot across them."
                .to_string(),
        );
    }
    let chunks = match (strategy, bounds) {
        (Strategy::Range { chunk_size, .. }, Bounds::Range { min, max }) => {
            let size = (*chunk_size).max(1) as i64;
            if max < min {
                return Err(format!("{column} has no rows to chunk (max < min)"));
            }
            let mut out = Vec::new();
            let mut lo = *min;
            // Half-open [lo, hi) throughout, with the last chunk closed on the
            // max, so no row is in two chunks and none is in none.
            while lo <= *max {
                let hi = lo.saturating_add(size);
                let is_final = hi > *max;
                let predicate = match is_final {
                    true => format!("{column} >= {lo} AND {column} <= {max}"),
                    false => format!("{column} >= {lo} AND {column} < {hi}"),
                };
                // The label names the LAST value in the chunk, not the first
                // of the next one: the predicate is half-open, and `1..1000001`
                // beside `>= 1 AND < 1000001` reads as though 1000001 were
                // included.
                let last = (hi - 1).min(*max);
                out.push(Chunk { id: out.len(), key: format!("{lo}..{last}"), predicate });
                if is_final {
                    break;
                }
                lo = hi;
            }
            out
        }
        (Strategy::Time { interval, .. }, Bounds::Time { from, to }) => {
            let def = crate::partition::PartitionDef::Time {
                cadence: *interval,
                timezone: "UTC".into(),
                parameter_start: "s".into(),
                parameter_end: "e".into(),
            };
            // The same generator partitioned backfills use, so a month means
            // the same thing in both and a DST boundary is handled once.
            let parts = crate::partition::generate(&def, from, to)?;
            parts
                .into_iter()
                .enumerate()
                .map(|(id, p)| {
                    let s = p.start.unwrap_or_default();
                    let e = p.end.unwrap_or_default();
                    Chunk {
                        id,
                        key: p.key,
                        predicate: format!(
                            "{column} >= TIMESTAMP '{}' AND {column} < TIMESTAMP '{}'",
                            sql_literal(&s),
                            sql_literal(&e)
                        ),
                    }
                })
                .collect()
        }
        (Strategy::Hash { buckets, .. }, _) => {
            let n = (*buckets).max(1);
            (0..n)
                .map(|b| Chunk {
                    id: b as usize,
                    key: format!("bucket {b} of {n}"),
                    predicate: hash_predicate(dialect, &column, b, n),
                })
                .collect()
        }
        (s, _) => {
            return Err(format!(
                "the {} strategy needs bounds of a matching kind",
                s.name()
            ))
        }
    };
    if chunks.is_empty() {
        return Err("that configuration produces no chunks".to_string());
    }
    Ok(Plan {
        strategy: strategy.name().to_string(),
        column,
        chunks,
        concurrency: concurrency.max(1),
        snapshot,
        notes,
    })
}

/// A single-quoted SQL literal with quotes doubled.
fn sql_literal(v: &str) -> String {
    v.replace('\'', "''")
}

/// Bucketing, spelled the way each family spells it.
///
/// There is no portable "hash this key into N buckets": Postgres has
/// `hashtext`, Oracle `ORA_HASH`, SQL Server `CHECKSUM`, MySQL `CRC32`. Getting
/// this wrong does not error - it silently produces overlapping or empty
/// buckets - so each is written out rather than approximated.
fn hash_predicate(dialect: Dialect, column: &str, bucket: u32, buckets: u32) -> String {
    match dialect {
        // abs() because hashtext and CRC32 are signed and a negative modulo
        // would land in a bucket nothing else generates.
        Dialect::Postgres => format!("abs(hashtext({column}::text)) % {buckets} = {bucket}"),
        Dialect::Oracle => format!("ORA_HASH({column}, {}) = {bucket}", buckets - 1),
        Dialect::MsSql => format!("ABS(CHECKSUM({column})) % {buckets} = {bucket}"),
        Dialect::MySql => format!("CRC32({column}) % {buckets} = {bucket}"),
        Dialect::Duckdb => format!("abs(hash({column})) % {buckets} = {bucket}"),
    }
}

/// What each source component can actually do (#306 capability negotiation).
///
/// Deliberately a short allowlist rather than a guess from the component name.
/// A connector missing here refuses, which is the behaviour the issue asks for:
/// emulating parallelism on a source that cannot give stable semantics is worse
/// than not offering it.
pub fn capabilities(component_id: &str) -> &'static [&'static str] {
    match component_id {
        // Ordered keys, time columns and a hash function, plus a pinnable
        // snapshot through the existing SCN path.
        "src.oracle" => &["range", "time", "hash"],
        "src.postgres" => &["range", "time", "hash"],
        "src.mssql" | "src.mysql" => &["range", "time", "hash"],
        // DuckDB reads a file or an attached database; chunking is available
        // and a snapshot is whatever the file was when it was opened.
        "src.duckdb" | "src.ducklake" => &["range", "time", "hash"],
        _ => &[],
    }
}

/// Whether this component may be asked for this strategy, with the reason when
/// it may not.
pub fn check_supported(component_id: &str, strategy: &Strategy) -> Result<(), String> {
    let supported = capabilities(component_id);
    if supported.is_empty() {
        return Err(format!(
            "{component_id} does not support chunked extraction. Chunking splits one query into \
             many, which needs a stable key and predictable ordering; emulating that on a source \
             that cannot give it would silently drop or duplicate rows."
        ));
    }
    if !supported.contains(&strategy.name()) {
        return Err(format!(
            "{component_id} supports {} but not {}",
            supported.join(", "),
            strategy.name()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(size: u64) -> Strategy {
        Strategy::Range { column: "company_id".into(), chunk_size: size }
    }

    #[test]
    fn ranges_cover_every_row_exactly_once() {
        // The property that matters: no row in two chunks, none in none.
        let p = plan(&range(100), &Bounds::Range { min: 1, max: 250 }, 0, 4, Snapshot::BestEffort, Dialect::Postgres)
            .unwrap();
        assert_eq!(p.chunks.len(), 3);
        assert_eq!(p.chunks[0].predicate, "company_id >= 1 AND company_id < 101");
        assert_eq!(p.chunks[1].predicate, "company_id >= 101 AND company_id < 201");
        // The last chunk closes on the max rather than running past it.
        assert_eq!(p.chunks[2].predicate, "company_id >= 201 AND company_id <= 250");
    }

    #[test]
    fn a_chunk_label_names_its_last_value_not_the_next_ones_first() {
        // The predicate is half-open. `1..101` beside `>= 1 AND < 101` reads as
        // though 101 were included, which is exactly the off-by-one an operator
        // checking a boundary by hand would then chase.
        let p = plan(&range(100), &Bounds::Range { min: 1, max: 250 }, 0, 1, Snapshot::BestEffort, Dialect::Postgres)
            .unwrap();
        assert_eq!(p.chunks[0].key, "1..100");
        assert_eq!(p.chunks[1].key, "101..200");
        assert_eq!(p.chunks[2].key, "201..250");
    }

    #[test]
    fn a_single_row_table_is_one_chunk() {
        let p = plan(&range(1000), &Bounds::Range { min: 7, max: 7 }, 0, 1, Snapshot::BestEffort, Dialect::Postgres)
            .unwrap();
        assert_eq!(p.chunks.len(), 1);
        assert_eq!(p.chunks[0].predicate, "company_id >= 7 AND company_id <= 7");
    }

    #[test]
    fn a_nullable_key_is_refused_and_says_how_many_rows_would_be_lost() {
        // Every predicate excludes NULL, so the extract would come out short
        // with nothing to show for it.
        let e = plan(&range(100), &Bounds::Range { min: 1, max: 10 }, 42, 1, Snapshot::BestEffort, Dialect::Postgres)
            .unwrap_err();
        assert!(e.contains("42 NULL"), "{e}");
        assert!(e.contains("short"), "{e}");
    }

    #[test]
    fn a_column_name_that_is_not_plain_is_refused_rather_than_escaped() {
        // A pipeline file is not a trusted source of SQL fragments.
        for bad in ["id; DROP TABLE t", "a b", "\"quoted\"", "", "x'"] {
            let s = Strategy::Range { column: bad.into(), chunk_size: 10 };
            assert!(
                plan(&s, &Bounds::Range { min: 1, max: 2 }, 0, 1, Snapshot::BestEffort, Dialect::Postgres).is_err(),
                "{bad:?} was accepted"
            );
        }
        assert!(plain_column("company_id").is_ok());
        assert!(plain_column("COMPANY$ID#1").is_ok(), "Oracle identifiers are legal");
    }

    #[test]
    fn time_chunks_reuse_the_partition_generator() {
        let s = Strategy::Time { column: "filing_date".into(), interval: crate::partition::Cadence::Month };
        let p = plan(&s, &Bounds::Time { from: "2020-01-05".into(), to: "2020-03-02".into() }, 0, 2, Snapshot::BestEffort, Dialect::Postgres)
            .unwrap();
        assert_eq!(p.chunks.len(), 3, "{:?}", p.chunks);
        assert_eq!(p.chunks[0].key, "2020-01");
        assert!(p.chunks[0].predicate.starts_with("filing_date >= TIMESTAMP '2020-01-01"), "{:?}", p.chunks[0]);
        // Half-open, so a row on the boundary is in exactly one chunk.
        assert!(p.chunks[0].predicate.contains("filing_date < TIMESTAMP '2020-02-01"), "{:?}", p.chunks[0]);
    }

    #[test]
    fn hash_buckets_are_spelled_the_way_each_family_spells_them() {
        // There is no portable bucketing, and getting it wrong does not error -
        // it silently produces overlapping or empty buckets.
        let s = Strategy::Hash { column: "company_number".into(), buckets: 64 };
        let of = |d| {
            plan(&s, &Bounds::None, 0, 4, Snapshot::BestEffort, d).unwrap().chunks[0].predicate.clone()
        };
        assert!(of(Dialect::Postgres).contains("hashtext"), "{}", of(Dialect::Postgres));
        assert!(of(Dialect::Oracle).contains("ORA_HASH"), "{}", of(Dialect::Oracle));
        assert!(of(Dialect::MsSql).contains("CHECKSUM"), "{}", of(Dialect::MsSql));
        assert!(of(Dialect::MySql).contains("CRC32"), "{}", of(Dialect::MySql));
        // ORA_HASH takes the max bucket, not the count - off by one and the
        // last bucket is never produced.
        assert!(of(Dialect::Oracle).contains(", 63)"), "{}", of(Dialect::Oracle));
    }

    #[test]
    fn every_hash_bucket_is_generated_exactly_once() {
        let s = Strategy::Hash { column: "k".into(), buckets: 8 };
        let p = plan(&s, &Bounds::None, 0, 4, Snapshot::BestEffort, Dialect::Postgres).unwrap();
        assert_eq!(p.chunks.len(), 8);
        let keys: std::collections::BTreeSet<_> = p.chunks.iter().map(|c| c.key.clone()).collect();
        assert_eq!(keys.len(), 8, "buckets must not repeat");
    }

    #[test]
    fn a_best_effort_plan_says_so_rather_than_implying_a_snapshot() {
        // Implying consistency the source cannot give is how a backfill
        // silently loses rows.
        let p = plan(&range(10), &Bounds::Range { min: 1, max: 20 }, 0, 1, Snapshot::BestEffort, Dialect::Postgres)
            .unwrap();
        assert!(p.notes.iter().any(|n| n.contains("one chunk or none")), "{:?}", p.notes);
        assert!(!p.snapshot.describes_one_state());

        let pinned = plan(&range(10), &Bounds::Range { min: 1, max: 20 }, 0, 1, Snapshot::Pinned { id: "scn:42".into() }, Dialect::Oracle)
            .unwrap();
        assert!(pinned.notes.is_empty(), "a pinned read needs no warning");
        assert!(pinned.snapshot.describes_one_state());
    }

    #[test]
    fn an_unsupported_connector_refuses_with_the_reason() {
        // Criterion 5: fail clearly rather than silently doing something unsafe.
        let e = check_supported("src.rest", &range(10)).unwrap_err();
        assert!(e.contains("does not support chunked extraction"), "{e}");
        assert!(e.contains("silently drop or duplicate"), "{e}");
        assert!(check_supported("src.postgres", &range(10)).is_ok());
    }

    #[test]
    fn the_probe_counts_nulls_as_well_as_bounds() {
        let sql = probe_sql(&range(10), "public.companies").unwrap();
        assert!(sql.contains("MIN(company_id)") && sql.contains("MAX(company_id)"), "{sql}");
        assert!(sql.to_uppercase().contains("NULL"), "{sql}");
        // And it refuses the same column names planning does.
        assert!(probe_sql(&Strategy::Range { column: "a; DROP".into(), chunk_size: 1 }, "t").is_err());
    }
}
