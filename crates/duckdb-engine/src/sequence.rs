//! #326: an ordered delta chain, and whether the next link may be applied.
//!
//! Some feeds are a set of independent objects and some are a chain. For a
//! chain, discovering `D20260903.zip` is not permission to apply it: if
//! `D20260902.zip` never arrived, applying the third delta produces a dataset
//! that is structurally valid, reports success, and is WRONG. Nothing in the
//! pipeline notices, because nothing was ever told the objects were ordered.
//!
//! ## This module is the generator, not the executor
//!
//! A sequence is a third slice generator, after a partition (#295) and a chunk
//! (#306) - not a third ledger and not a second scheduler. What lives here is
//! what a slice IS: parsing a key out of an object name, and saying which key
//! must come before which. Whether a slice may run is one predicate on the
//! ledger, where the concurrency and retry rules already are.
//!
//! ## The ledger is the only record of how far the chain got
//!
//! The obvious design is an accepted-position pointer plus the slices, and it
//! is wrong: two records that must agree, whose interesting failures are
//! exactly the disagreements - a retry that moves the ledger and not the
//! pointer, an operator edit that moves the pointer past a slice that never
//! ran. There is no pointer here. The position is derived by walking the
//! slices, so it cannot disagree with them.
//!
//! ## Parsing is total, or it is a lie
//!
//! Every object that reaches the generator must produce exactly one key or a
//! structured refusal. A skip is not a safe default here, it is the failure
//! mode: given
//!
//! ```text
//! D20260901.zip
//! D202609O2.zip   <- a capital O where a zero belongs
//! D20260903.zip
//! ```
//!
//! a parser that skips what it cannot read reports a gap at 2026-09-02 while
//! the file is sitting right there under a malformed name. The gap report would
//! be true and the diagnosis it invites - "chase the publisher" - would be
//! wrong. So a name that does not parse is an error against the object, naming
//! the object.
//!
//! ## An open head is not a gap
//!
//! A chain is always missing its next element; that is what being at the head
//! means. Two states that look identical to a pointer and are completely
//! different to an operator:
//!
//! - nothing after the last success has been seen -> `waiting_for_next`, and
//!   the chain is healthy as far as continuity can tell. Whether the publisher
//!   is LATE is a freshness question (#304), not a continuity one, and this
//!   module deliberately does not answer it.
//! - a later object exists -> the hole is proven, immediately and without any
//!   grace period, because something that comes after it is already here.

use serde::{Deserialize, Serialize};

use crate::partition::Cadence;

/// How keys are ordered, and therefore what "the next one" means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Order {
    /// Calendar keys at a fixed cadence. Reuses the partition cadence rather
    /// than declaring a second one, so `2026-02-28`'s successor is decided in
    /// one place for the whole engine.
    Date {
        #[serde(default = "day")]
        cadence: Cadence,
    },
    /// Monotonic integers, step one. A registry that numbers its publications
    /// 101, 102, 103 has a gap at 102 whether or not it also has a schedule.
    Integer,
}

fn day() -> Cadence {
    Cadence::Day
}

impl Order {
    /// The key that must immediately precede `key`'s successor.
    pub fn next(self, key: &str) -> Option<String> {
        match self {
            Order::Date { cadence } => crate::partition::next_key(key, cadence),
            Order::Integer => key.trim().parse::<i64>().ok()?.checked_add(1).map(|n| n.to_string()),
        }
    }

    /// Sort order over canonical keys.
    ///
    /// Not string order: `9` sorts after `10` as text, which would put an
    /// integer chain in the wrong sequence and invent gaps that are not there.
    /// Date keys are ISO and do compare as text, which is why they are stored
    /// that way.
    fn rank(self, key: &str) -> Option<Rank> {
        match self {
            Order::Date { .. } => Some(Rank::Date(key.to_string())),
            Order::Integer => key.trim().parse::<i64>().ok().map(Rank::Int),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Rank {
    Date(String),
    Int(i64),
}

/// A declared ordered feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SequenceDef {
    /// How an object's leaf name spells its key: `D{date:YYYYMMDD}.KBO.zip`.
    pub pattern: String,
    pub order: Order,
    /// Whether a missing predecessor BLOCKS, or is only reported.
    ///
    /// Opt-in, and deliberately separate from declaring the sequence at all. A
    /// plain watermark over an append-only stream is allowed to have gaps -
    /// that is what a watermark means - and only the operator knows whether
    /// this particular feed promises not to. Off, the chain is still parsed and
    /// gaps are still reported; nothing is prevented.
    #[serde(default)]
    pub require_continuity: bool,
    /// Where this generation of the chain starts: the last key that is taken as
    /// already applied. `2026-01-01`, or `123456`, or the key of an accepted
    /// full snapshot.
    ///
    /// Absent, the chain starts at the lowest key observed, whose predecessor
    /// is then nothing. That is the workable default and it is strictly weaker:
    /// if the listing is missing the FIRST delta, an implicit baseline cannot
    /// know, and the chain begins one late while looking contiguous. An
    /// explicit baseline is the only way to state where the chain should have
    /// begun.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<String>,
    /// Which generation this is - the id of the full snapshot that reset it.
    ///
    /// A feed that republishes a full snapshot starts a new chain. The old
    /// epoch's slices stay for provenance and stop blocking, which is the
    /// difference between an operator-recoverable system and one that needs a
    /// state file edited by hand after every permanently missed delta.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<String>,
}

/// An object that produced a key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Observed {
    pub key: String,
    pub uri: String,
}

/// An object that could not produce exactly one key.
///
/// Machine-readable on purpose: the point of the totality contract is that a
/// malformed name is diagnosable without reading a log, so the code, the object
/// and the pattern it was measured against all travel together.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Refusal {
    /// `pattern_mismatch`, `invalid_date`, `invalid_integer`,
    /// `duplicate_sequence_key`.
    ///
    /// Four codes rather than one `invalid_sequence_key`, because the operator
    /// response differs: a mismatch is usually a discovery filter that is too
    /// wide, an invalid date is usually a typo in one publication, and a
    /// duplicate is two objects claiming to be the same delta - which is the
    /// only one of the four that is a correctness emergency.
    pub code: &'static str,
    pub uri: String,
    pub expected_pattern: String,
    pub detail: String,
}

/// What the selected objects turned out to be.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Reading {
    /// Ascending by key.
    pub items: Vec<Observed>,
    pub refusals: Vec<Refusal>,
}

/// One link of the chain: a key, and what must come before it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Link {
    pub key: String,
    /// The key that must have succeeded first. `None` only for the first link
    /// of an epoch, whose predecessor is the baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<String>,
    /// The object, when one was observed. Absent means the chain requires this
    /// key and nothing published it - the hole itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

impl Link {
    /// Whether this link is a hole rather than a publication.
    pub fn is_missing(&self) -> bool {
        self.uri.is_none()
    }
}

/// Why the chain cannot proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// A required key was never published, and a later one was.
    SequenceGap,
    /// A required key was published and its slice has not succeeded.
    PredecessorFailed,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Reason::SequenceGap => "sequence_gap",
            Reason::PredecessorFailed => "predecessor_failed",
        }
    }
}

/// Where the chain has got to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Verdict {
    #[serde(flatten)]
    pub state: Status,
    /// The highest key applied contiguously from the baseline. `None` means
    /// nothing has been applied in this epoch yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Status {
    /// Every observed key has been applied, contiguously.
    Complete,
    /// The chain is healthy and not finished: the next required key is not
    /// applied yet, and nothing after it proves a hole.
    #[serde(rename_all = "camelCase")]
    WaitingForNext {
        expected: String,
        /// Whether that key has actually been published.
        ///
        /// The distinction an operator needs and a position pointer cannot
        /// make: `false` means the publisher has not released it, which is a
        /// freshness question (#304); `true` means it is here and Duckle has
        /// not applied it yet, which is ordinary pending work. Reporting both
        /// as "waiting" would send someone to chase a publisher who has
        /// already delivered.
        observed: bool,
    },
    /// A later key exists, so the hole is proven.
    #[serde(rename_all = "camelCase")]
    Blocked {
        expected: String,
        next_observed: String,
        reason: Reason,
    },
}

/// A pattern with exactly one placeholder.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Compiled {
    prefix: String,
    suffix: String,
    slot: Slot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Slot {
    /// `{date:YYYYMMDD}` - a format of `YYYY`/`MM`/`DD`/`HH` tokens and
    /// literals.
    Date(String),
    /// `{seq}` - digits.
    Seq,
}

/// Read a pattern.
///
/// Exactly one placeholder is required, and more than one is refused rather
/// than resolved: two variables in a name make it ambiguous which one orders
/// the chain, and guessing would put the sequence silently in the wrong order.
fn compile(pattern: &str) -> Result<Compiled, String> {
    let open = pattern.find('{');
    let close = pattern.find('}');
    let (open, close) = match (open, close) {
        (Some(o), Some(c)) if c > o => (o, c),
        _ => {
            return Err(format!(
                "the sequence pattern {pattern:?} has no placeholder. It needs exactly one \
                 {{date:YYYYMMDD}} or {{seq}}, which is the part that orders the chain."
            ))
        }
    };
    let rest = &pattern[close + 1..];
    if rest.contains('{') {
        return Err(format!(
            "the sequence pattern {pattern:?} has more than one placeholder. Exactly one orders \
             the chain; with two it is not defined which."
        ));
    }
    let inner = &pattern[open + 1..close];
    let slot = match inner {
        "seq" => Slot::Seq,
        _ => match inner.strip_prefix("date:") {
            Some(fmt) if !fmt.is_empty() => Slot::Date(fmt.to_string()),
            _ => {
                return Err(format!(
                    "the sequence placeholder {{{inner}}} is not one I know. Use {{seq}} for an \
                     integer or {{date:YYYYMMDD}} for a date."
                ))
            }
        },
    };
    Ok(Compiled {
        prefix: pattern[..open].to_string(),
        suffix: rest.to_string(),
        slot,
    })
}

/// The object's leaf name - what the pattern describes.
fn leaf(uri: &str) -> &str {
    let cut = uri.rfind(['/', '\\']).map(|i| i + 1).unwrap_or(0);
    &uri[cut..]
}

/// Pull the key text out of a name, or say it did not match.
fn capture<'a>(c: &Compiled, name: &'a str) -> Option<&'a str> {
    let text = name.strip_prefix(&c.prefix)?.strip_suffix(&c.suffix)?;
    (!text.is_empty()).then_some(text)
}

/// `YYYYMMDD` against `20260901` - fixed-width fields and literals.
///
/// Written out rather than translated into a strftime, because the interesting
/// part is the refusal: a scanner that knows it wanted two digits and got `O2`
/// can say so, and a format-string parse can only say the whole thing failed.
fn read_date(text: &str, fmt: &str, cadence: Cadence) -> Result<String, String> {
    let (mut year, mut month, mut day, mut hour) = (None, 1u32, 1u32, 0u32);
    let (f, t) = (fmt.as_bytes(), text.as_bytes());
    let (mut i, mut j) = (0usize, 0usize);
    while i < f.len() {
        let token = [(&b"YYYY"[..], 4u32), (&b"MM"[..], 2), (&b"DD"[..], 2), (&b"HH"[..], 2)]
            .into_iter()
            .find(|(tok, _)| f[i..].starts_with(tok));
        let Some((tok, width)) = token else {
            // A literal in the format must be a literal in the text.
            if j >= t.len() || t[j] != f[i] {
                return Err(format!(
                    "{text:?} does not match the date format {fmt:?} at character {}",
                    j + 1
                ));
            }
            i += 1;
            j += 1;
            continue;
        };
        let width = width as usize;
        if j + width > t.len() {
            return Err(format!("{text:?} is too short for the date format {fmt:?}"));
        }
        let field = std::str::from_utf8(&t[j..j + width])
            .map_err(|_| format!("{text:?} is not text at character {}", j + 1))?;
        let value: u32 = field.parse().map_err(|_| {
            format!("{field:?} in {text:?} is not a number, and {fmt:?} wants one there")
        })?;
        match tok {
            b"YYYY" => year = Some(value as i32),
            b"MM" => month = value,
            b"DD" => day = value,
            _ => hour = value,
        }
        i += tok.len();
        j += width;
    }
    if j != t.len() {
        return Err(format!("{text:?} has characters the date format {fmt:?} does not describe"));
    }
    let year = year.ok_or_else(|| format!("the date format {fmt:?} has no YYYY, so it names no year"))?;
    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| format!("{year:04}-{month:02}-{day:02} is not a date"))?;
    crate::partition::key_of(date, hour, cadence)
        .ok_or_else(|| format!("{text:?} does not name a {cadence:?} slice"))
}

fn read_int(text: &str) -> Result<String, String> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("{text:?} is not a whole number"));
    }
    // Canonical decimal, so `007` and `7` are the same link rather than two.
    text.parse::<i64>()
        .map(|n| n.to_string())
        .map_err(|_| format!("{text:?} does not fit in a sequence number"))
}

/// Read every selected object: a key each, or a refusal each.
///
/// Objects are assumed already filtered by discovery. Louis's framing on the
/// issue, and it is what makes totality affordable: unrelated files in the same
/// directory are removed by the include/exclude the listing already has, so
/// everything that reaches here is CLAIMED to be part of the sequence, and a
/// name that then does not parse is a real problem rather than a neighbour.
pub fn read(def: &SequenceDef, uris: &[String]) -> Result<Reading, String> {
    let c = compile(&def.pattern)?;
    let mut out = Reading::default();
    // key -> the uri that claimed it, to catch two objects claiming one delta.
    let mut claimed: std::collections::BTreeMap<String, String> = Default::default();
    for uri in uris {
        let name = leaf(uri);
        let Some(text) = capture(&c, name) else {
            out.refusals.push(Refusal {
                code: "pattern_mismatch",
                uri: uri.clone(),
                expected_pattern: def.pattern.clone(),
                detail: format!("{name:?} does not match the sequence pattern"),
            });
            continue;
        };
        let parsed = match (&c.slot, def.order) {
            (Slot::Date(fmt), Order::Date { cadence }) => read_date(text, fmt, cadence),
            (Slot::Seq, Order::Integer) => read_int(text),
            (Slot::Date(_), Order::Integer) => Err(
                "the pattern reads a date and the sequence is ordered by integer. Use {seq}, or \
                 order the sequence by date."
                    .to_string(),
            ),
            (Slot::Seq, Order::Date { .. }) => Err(
                "the pattern reads an integer and the sequence is ordered by date. Use \
                 {date:YYYYMMDD}, or order the sequence by integer."
                    .to_string(),
            ),
        };
        let key = match parsed {
            Ok(k) => k,
            Err(detail) => {
                out.refusals.push(Refusal {
                    code: match (&c.slot, def.order) {
                        (Slot::Date(_), Order::Date { .. }) => "invalid_date",
                        (Slot::Seq, Order::Integer) => "invalid_integer",
                        // A pattern that cannot produce the declared order is a
                        // definition error, not a bad object; it is reported
                        // per object because that is where it is noticed.
                        _ => "pattern_mismatch",
                    },
                    uri: uri.clone(),
                    expected_pattern: def.pattern.clone(),
                    detail,
                });
                continue;
            }
        };
        match claimed.get(&key) {
            // The same listing returned twice is not a conflict.
            Some(first) if first == uri => continue,
            Some(first) => {
                out.refusals.push(Refusal {
                    code: "duplicate_sequence_key",
                    uri: uri.clone(),
                    expected_pattern: def.pattern.clone(),
                    detail: format!("{key} is already claimed by {first}"),
                });
                continue;
            }
            None => {
                claimed.insert(key.clone(), uri.clone());
                out.items.push(Observed { key, uri: uri.clone() });
            }
        }
    }
    // Ascending by VALUE, so an integer chain is not ordered as text.
    let mut ranked: Vec<(Rank, Observed)> = Vec::with_capacity(out.items.len());
    for item in std::mem::take(&mut out.items) {
        let rank = def
            .order
            .rank(&item.key)
            .ok_or_else(|| format!("{} is not orderable, which should be impossible", item.key))?;
        ranked.push((rank, item));
    }
    ranked.sort_by(|a, b| a.0.cmp(&b.0));
    out.items = ranked.into_iter().map(|(_, i)| i).collect();
    Ok(out)
}

/// How far a chain may be expanded before it is called a definition error.
///
/// A baseline a million links behind the first observed object is not a chain
/// with a large gap, it is a wrong baseline, and expanding it would build a
/// vector until the process died. The same guard `partition::generate` uses,
/// for the same reason.
pub const MAX_LINKS: usize = 200_000;

/// Every link from the baseline up to the highest observed key.
///
/// Missing keys are MATERIALISED as links with no object, because a hole has to
/// be nameable to be reported: "expected 2026-09-02" is only sayable if the
/// walk produced 2026-09-02 as a thing that ought to exist.
///
/// Keys below the baseline are dropped. That is the epoch rule: a full snapshot
/// resets the chain, and the deltas it superseded must stop blocking without
/// being deleted from history.
pub fn chain(def: &SequenceDef, items: &[Observed]) -> Result<Vec<Link>, String> {
    let Some(last) = items.last() else {
        return Ok(Vec::new());
    };
    let top = def
        .order
        .rank(&last.key)
        .ok_or_else(|| format!("{} is not orderable", last.key))?;

    // The first key this epoch requires: the successor of the baseline, or the
    // lowest thing actually seen.
    let (start, mut predecessor) = match &def.baseline {
        Some(base) => {
            let base = base.trim();
            let next = def.order.next(base).ok_or_else(|| {
                format!("the sequence baseline {base:?} is not a key this order can advance")
            })?;
            (next, Some(base.to_string()))
        }
        None => (items[0].key.clone(), None),
    };

    let observed: std::collections::BTreeMap<&str, &str> =
        items.iter().map(|i| (i.key.as_str(), i.uri.as_str())).collect();

    let mut out = Vec::new();
    let mut key = start;
    loop {
        let rank = def
            .order
            .rank(&key)
            .ok_or_else(|| format!("{key} is not orderable"))?;
        if rank > top {
            break;
        }
        out.push(Link {
            key: key.clone(),
            predecessor: predecessor.clone(),
            uri: observed.get(key.as_str()).map(|u| u.to_string()),
        });
        if out.len() >= MAX_LINKS {
            return Err(format!(
                "the chain from {start:?} to {} is longer than {MAX_LINKS} links. That is a \
                 baseline far behind the data rather than a gap; set the baseline to where this \
                 generation of the chain actually starts.",
                last.key,
                start = out[0].key
            ));
        }
        predecessor = Some(key.clone());
        key = def
            .order
            .next(&key)
            .ok_or_else(|| format!("{key} has no successor, so the chain cannot be walked"))?;
    }
    Ok(out)
}

/// Where the chain has got to, given which keys have succeeded.
///
/// `succeeded` answers "has the slice for this key been applied". It is a
/// closure rather than a set so the caller can consult the ledger directly -
/// there is no second copy of the position to fall out of step with.
pub fn verdict(links: &[Link], succeeded: impl Fn(&str) -> bool) -> Verdict {
    // The position is the contiguous run of successes from the start, which is
    // derived rather than stored: it cannot disagree with the slices because it
    // IS the slices.
    let applied = links.iter().take_while(|l| succeeded(&l.key)).count();
    let position = applied.checked_sub(1).map(|i| links[i].key.clone());

    let Some(next) = links.get(applied) else {
        return Verdict { state: Status::Complete, position };
    };
    // Anything after the first unapplied link proves the chain is incomplete.
    // Only an OBSERVED later object proves it; a materialised hole is another
    // way of saying the same key is missing.
    let later = links[applied + 1..].iter().find(|l| !l.is_missing());
    let state = match later {
        // Nothing after it, so nothing proves a hole. Either the publisher has
        // not released it, or it is here and has not run - `observed` says
        // which, and they are different problems.
        None => Status::WaitingForNext {
            expected: next.key.clone(),
            observed: !next.is_missing(),
        },
        Some(observed) => Status::Blocked {
            expected: next.key.clone(),
            next_observed: observed.key.clone(),
            reason: match next.is_missing() {
                true => Reason::SequenceGap,
                false => Reason::PredecessorFailed,
            },
        },
    };
    Verdict { state, position }
}

/// The sequence definition a pipeline document declares, if any.
pub fn of(doc: &serde_json::Value) -> Option<SequenceDef> {
    serde_json::from_value(doc.get("sequence")?.clone()).ok()
}

/// Turn a chain into ledger slices.
///
/// One slice per PUBLISHED link. A hole gets no slice, because there is nothing
/// to run: it is represented by the requirement its successor carries, which is
/// what makes an absent predecessor block instead of quietly passing. The hole
/// is still nameable - [`verdict`] reports it - it just is not a unit of work.
///
/// `requires` is only set when the definition asked for continuity. Without it
/// the same links are planned and nothing blocks: a plain feed is allowed to
/// have gaps, and `xf.incremental` keeps the semantics it has.
pub fn slices(
    def: &SequenceDef,
    pipeline: &str,
    release: Option<&str>,
    links: &[Link],
) -> Vec<crate::backfill::PartitionRun> {
    use crate::backfill::{occurrence_id, PartitionRun, State};
    links
        .iter()
        .filter(|l| !l.is_missing())
        .map(|l| {
            let mut params = std::collections::BTreeMap::new();
            params.insert("sequence_key".to_string(), l.key.clone());
            if let Some(uri) = &l.uri {
                params.insert("sequence_object".to_string(), uri.clone());
            }
            // Provenance, per acceptance criterion 6: the key, the key that had
            // to come first, and the object. The producing run and the
            // resulting position are the ledger's own, already.
            if let Some(prev) = &l.predecessor {
                params.insert("sequence_previous".to_string(), prev.clone());
            }
            if let Some(epoch) = &def.epoch {
                params.insert("sequence_epoch".to_string(), epoch.clone());
            }
            PartitionRun {
                occurrence: Some(occurrence_id(
                    pipeline,
                    &format!("sequence:{}:{}", def.epoch.as_deref().unwrap_or(""), l.key),
                    release,
                    None,
                )),
                key: l.key.clone(),
                state: State::Requested,
                run_id: None,
                attempts: 0,
                error: None,
                finished_at: None,
                params,
                predicate: None,
                artifact: None,
                // "predecessor succeeded OR predecessor == the accepted
                // baseline". The baseline has no slice - it is what the epoch
                // starts FROM, already applied - so requiring it would block
                // the first link of every chain forever. Dropping the
                // requirement is how the second half of the rule is expressed.
                requires: def
                    .require_continuity
                    .then(|| l.predecessor.clone())
                    .flatten()
                    .filter(|prev| Some(prev.as_str()) != def.baseline.as_deref()),
                source_uri: l.uri.clone(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dated(pattern: &str) -> SequenceDef {
        SequenceDef {
            pattern: pattern.to_string(),
            order: Order::Date { cadence: Cadence::Day },
            require_continuity: true,
            baseline: None,
            epoch: None,
        }
    }

    fn ints(pattern: &str) -> SequenceDef {
        SequenceDef { order: Order::Integer, ..dated(pattern) }
    }

    fn uris(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| format!("s3://reg/{n}")).collect()
    }

    fn keys(r: &Reading) -> Vec<&str> {
        r.items.iter().map(|i| i.key.as_str()).collect()
    }

    #[test]
    fn a_dated_chain_reads_in_order() {
        let def = dated("D{date:YYYYMMDD}.KBO.zip");
        let r = read(&def, &uris(&["D20260903.KBO.zip", "D20260901.KBO.zip", "D20260902.KBO.zip"]))
            .expect("a reading");
        assert_eq!(keys(&r), ["2026-09-01", "2026-09-02", "2026-09-03"]);
        assert!(r.refusals.is_empty(), "{:?}", r.refusals);
    }

    /// The failure this whole module exists to prevent.
    #[test]
    fn a_malformed_name_is_an_error_and_not_a_gap() {
        let def = dated("D{date:YYYYMMDD}.zip");
        // A capital O where a zero belongs.
        let r = read(&def, &uris(&["D20260901.zip", "D202609O2.zip", "D20260903.zip"])).unwrap();
        assert_eq!(keys(&r), ["2026-09-01", "2026-09-03"]);
        assert_eq!(r.refusals.len(), 1, "the typo must be reported, not skipped");
        let bad = &r.refusals[0];
        assert_eq!(bad.code, "invalid_date");
        assert!(bad.uri.ends_with("D202609O2.zip"), "it names the object: {}", bad.uri);
        assert_eq!(bad.expected_pattern, "D{date:YYYYMMDD}.zip");
        assert!(bad.detail.contains("O2"), "it names what failed: {}", bad.detail);
    }

    #[test]
    fn every_way_an_object_can_fail_is_named() {
        let def = dated("D{date:YYYYMMDD}.zip");
        let r = read(
            &def,
            &[
                "s3://reg/D20260901.zip".to_string(),
                "s3://reg/notes.txt".to_string(),
                "s3://reg/D20260230.zip".to_string(),
                // A second object claiming a delta that is already claimed.
                "s3://reg/copy/D20260901.zip".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(keys(&r), ["2026-09-01"]);
        let codes: Vec<&str> = r.refusals.iter().map(|f| f.code).collect();
        assert_eq!(codes, ["pattern_mismatch", "invalid_date", "duplicate_sequence_key"]);
        assert!(
            r.refusals[1].detail.contains("2026-02-30"),
            "February has no 30th, and it says so: {}",
            r.refusals[1].detail
        );
    }

    #[test]
    fn the_same_object_listed_twice_is_not_a_duplicate() {
        let def = dated("D{date:YYYYMMDD}.zip");
        let same = "s3://reg/D20260901.zip".to_string();
        let r = read(&def, &[same.clone(), same]).unwrap();
        assert_eq!(keys(&r), ["2026-09-01"]);
        assert!(r.refusals.is_empty(), "a repeated listing is not a conflict: {:?}", r.refusals);
    }

    #[test]
    fn integers_order_by_value_and_not_as_text() {
        let def = ints("f-{seq}.json");
        let r = read(&def, &uris(&["f-10.json", "f-9.json", "f-11.json"])).unwrap();
        assert_eq!(keys(&r), ["9", "10", "11"], "9 comes before 10");
        // Leading zeros name the same link, so they are a duplicate.
        let r = read(&def, &uris(&["f-007.json", "f-7.json"])).unwrap();
        assert_eq!(keys(&r), ["7"]);
        assert_eq!(r.refusals[0].code, "duplicate_sequence_key");
    }

    #[test]
    fn a_pattern_must_have_exactly_one_placeholder() {
        assert!(read(&dated("D.zip"), &[]).is_err(), "no placeholder");
        assert!(read(&dated("{date:YYYY}-{seq}.zip"), &[]).is_err(), "two placeholders");
        assert!(read(&dated("D{when}.zip"), &[]).is_err(), "an unknown placeholder");
        assert!(read(&dated("D{date:YYYYMMDD}.zip"), &[]).is_ok());
    }

    #[test]
    fn a_pattern_that_disagrees_with_the_order_is_refused_per_object() {
        let def = SequenceDef { order: Order::Integer, ..dated("D{date:YYYYMMDD}.zip") };
        let r = read(&def, &uris(&["D20260901.zip"])).unwrap();
        assert!(r.items.is_empty());
        assert_eq!(r.refusals[0].code, "pattern_mismatch");
        assert!(r.refusals[0].detail.contains("{seq}"), "{}", r.refusals[0].detail);
    }

    #[test]
    fn a_hole_is_materialised_so_it_can_be_named() {
        let def = dated("D{date:YYYYMMDD}.zip");
        let r = read(&def, &uris(&["D20260901.zip", "D20260903.zip"])).unwrap();
        let links = chain(&def, &r.items).unwrap();
        assert_eq!(
            links.iter().map(|l| l.key.as_str()).collect::<Vec<_>>(),
            ["2026-09-01", "2026-09-02", "2026-09-03"]
        );
        assert!(links[1].is_missing(), "the 2nd was never published");
        assert_eq!(links[2].predecessor.as_deref(), Some("2026-09-02"));
        // With no declared baseline the first link starts the epoch.
        assert_eq!(links[0].predecessor, None);
    }

    #[test]
    fn a_baseline_starts_the_chain_after_it() {
        let def = SequenceDef { baseline: Some("2026-08-31".into()), ..dated("D{date:YYYYMMDD}.zip") };
        let r = read(&def, &uris(&["D20260901.zip", "D20260902.zip"])).unwrap();
        let links = chain(&def, &r.items).unwrap();
        assert_eq!(links[0].key, "2026-09-01");
        assert_eq!(
            links[0].predecessor.as_deref(),
            Some("2026-08-31"),
            "the first link of an epoch answers to the baseline"
        );
    }

    /// Acceptance criterion 5: a new full snapshot resets the chain, and the
    /// gap in the old generation stops blocking.
    #[test]
    fn a_new_baseline_drops_the_superseded_deltas() {
        let def = SequenceDef {
            baseline: Some("2026-09-03".into()),
            epoch: Some("F2".into()),
            ..dated("D{date:YYYYMMDD}.zip")
        };
        // D02 is still in the listing and is still missing. It is before the
        // new baseline, so it is not this epoch's problem.
        let r = read(&def, &uris(&["D20260901.zip", "D20260903.zip", "D20260904.zip"])).unwrap();
        let links = chain(&def, &r.items).unwrap();
        assert_eq!(links.len(), 1, "only what follows F2: {links:?}");
        assert_eq!(links[0].key, "2026-09-04");
        assert!(matches!(verdict(&links, |_| false).state, Status::WaitingForNext { .. }));
    }

    #[test]
    fn an_open_head_is_healthy_and_a_proven_hole_is_not() {
        let def = dated("D{date:YYYYMMDD}.zip");

        // Nothing after the last success -> waiting, not blocked.
        let r = read(&def, &uris(&["D20260901.zip"])).unwrap();
        let links = chain(&def, &r.items).unwrap();
        let v = verdict(&links, |k| k == "2026-09-01");
        assert_eq!(v.position.as_deref(), Some("2026-09-01"));
        assert_eq!(v.state, Status::Complete, "everything observed is applied");

        // A later object proves the hole immediately, with no grace period.
        let r = read(&def, &uris(&["D20260901.zip", "D20260903.zip"])).unwrap();
        let links = chain(&def, &r.items).unwrap();
        let v = verdict(&links, |k| k == "2026-09-01");
        assert_eq!(v.position.as_deref(), Some("2026-09-01"));
        assert_eq!(
            v.state,
            Status::Blocked {
                expected: "2026-09-02".into(),
                next_observed: "2026-09-03".into(),
                reason: Reason::SequenceGap,
            }
        );
    }

    /// The issue's own worked example: D02 exists and failed, so D03 waits.
    #[test]
    fn a_failed_predecessor_blocks_and_a_retry_unblocks() {
        let def = dated("D{date:YYYYMMDD}.zip");
        let r = read(&def, &uris(&["D20260901.zip", "D20260902.zip", "D20260903.zip"])).unwrap();
        let links = chain(&def, &r.items).unwrap();

        let done = ["2026-09-01"];
        let v = verdict(&links, |k| done.contains(&k));
        assert_eq!(v.position.as_deref(), Some("2026-09-01"), "the position stays at D01");
        assert_eq!(
            v.state,
            Status::Blocked {
                expected: "2026-09-02".into(),
                next_observed: "2026-09-03".into(),
                // D02 is present, so this is a failure and not a missing file.
                reason: Reason::PredecessorFailed,
            }
        );

        // Retrying D02 successfully makes D03 eligible - acceptance criterion 4.
        let done = ["2026-09-01", "2026-09-02"];
        let v = verdict(&links, |k| done.contains(&k));
        assert_eq!(v.position.as_deref(), Some("2026-09-02"));
        assert_eq!(
            v.state,
            Status::WaitingForNext { expected: "2026-09-03".into(), observed: true },
            "D03 is here and has not run - not a publisher problem"
        );
    }

    /// Acceptance criterion 3, and the one property that must not be got wrong.
    ///
    /// A later slice succeeding does NOT move the position past a predecessor
    /// that did not. The distinction is contiguity, not a count: a position
    /// derived by counting successes reads D01 ok, D02 failed, D03 ok as
    /// "two applied" and puts the position on D02 - a key that never ran -
    /// which is exactly the "advance past a missing predecessor" this issue is
    /// about. Every other test here happens to have its successes in a prefix,
    /// where counting and contiguity agree, so this is the only one that can
    /// tell them apart.
    #[test]
    fn a_later_success_does_not_carry_the_position_over_a_hole() {
        let def = dated("D{date:YYYYMMDD}.zip");
        let r = read(&def, &uris(&["D20260901.zip", "D20260902.zip", "D20260903.zip"])).unwrap();
        let links = chain(&def, &r.items).unwrap();

        // D02 failed and D03 somehow succeeded anyway.
        let done = ["2026-09-01", "2026-09-03"];
        let v = verdict(&links, |k| done.contains(&k));
        assert_eq!(
            v.position.as_deref(),
            Some("2026-09-01"),
            "the chain is only applied as far as it is unbroken"
        );
        assert_eq!(
            v.state,
            Status::Blocked {
                expected: "2026-09-02".into(),
                next_observed: "2026-09-03".into(),
                reason: Reason::PredecessorFailed,
            }
        );
    }

    #[test]
    fn an_integer_gap_is_proven_without_any_schedule() {
        let def = ints("f-{seq}.json");
        let r = read(&def, &uris(&["f-101.json", "f-103.json"])).unwrap();
        let links = chain(&def, &r.items).unwrap();
        let v = verdict(&links, |k| k == "101");
        assert_eq!(
            v.state,
            Status::Blocked {
                expected: "102".into(),
                next_observed: "103".into(),
                reason: Reason::SequenceGap,
            },
            "101 succeeded, 103 exists, so 102 is absent - no cadence needed to know that"
        );
    }

    #[test]
    fn nothing_observed_is_an_empty_chain_rather_than_an_error() {
        let def = dated("D{date:YYYYMMDD}.zip");
        let links = chain(&def, &[]).unwrap();
        assert!(links.is_empty());
        assert_eq!(verdict(&links, |_| false).state, Status::Complete);
    }

    #[test]
    fn a_baseline_far_behind_the_data_is_a_definition_error() {
        let def = SequenceDef { baseline: Some("1".into()), ..ints("f-{seq}.json") };
        let err = chain(&def, &[Observed { key: "900000".into(), uri: "f-900000.json".into() }])
            .expect_err("a chain that long is a wrong baseline");
        assert!(err.contains("baseline"), "it says what to fix: {err}");
    }

    /// The report the issue asks for, as JSON.
    ///
    /// ```text
    /// sequence status: blocked
    /// expected: 2026-09-02
    /// next observed: 2026-09-03
    /// reason: sequence_gap
    /// ```
    #[test]
    fn the_verdict_serialises_as_the_report_the_issue_asks_for() {
        let def = dated("D{date:YYYYMMDD}.zip");
        let r = read(&def, &uris(&["D20260901.zip", "D20260903.zip"])).unwrap();
        let links = chain(&def, &r.items).unwrap();
        let v = verdict(&links, |k| k == "2026-09-01");
        assert_eq!(
            serde_json::to_value(&v).unwrap(),
            serde_json::json!({
                "status": "blocked",
                "expected": "2026-09-02",
                "nextObserved": "2026-09-03",
                "reason": "sequence_gap",
                "position": "2026-09-01",
            })
        );
        // And it reads back, so a console can hold one.
        assert_eq!(serde_json::from_value::<Verdict>(serde_json::to_value(&v).unwrap()).unwrap(), v);
    }

    /// Build a ledger the way a real chain would, for the claim rule.
    fn ledger(def: &SequenceDef, names: &[&str]) -> crate::backfill::Backfill {
        let r = read(def, &uris(names)).expect("a reading");
        let links = chain(def, &r.items).expect("a chain");
        crate::backfill::Backfill {
            id: "b1".into(),
            pipeline: "reg".into(),
            pipeline_path: "reg.json".into(),
            created_at: "2026-09-04T00:00:00Z".into(),
            release_id: None,
            max_concurrent: 4,
            pid: None,
            kind: crate::backfill::Kind::Sequence,
            chunk_node: None,
            staging: None,
            epoch: def.epoch.clone(),
            partitions: slices(def, "reg", None, &links),
        }
    }

    fn at(plan: &crate::backfill::Backfill, key: &str) -> usize {
        plan.partitions.iter().position(|p| p.key == key).unwrap_or_else(|| panic!("no {key}"))
    }

    /// Acceptance criteria 2 and 3, as the ledger rule Louis specified.
    #[test]
    fn a_link_is_not_claimable_until_its_predecessor_has_succeeded() {
        use crate::backfill::State;
        let def = dated("D{date:YYYYMMDD}.zip");
        let mut plan = ledger(&def, &["D20260901.zip", "D20260902.zip", "D20260903.zip"]);

        // Nothing has run. Only the head of the chain may be claimed, even
        // though all three are requested and workers are free.
        assert!(plan.claimable(at(&plan, "2026-09-01")));
        assert!(!plan.claimable(at(&plan, "2026-09-02")));
        assert!(!plan.claimable(at(&plan, "2026-09-03")));
        assert_eq!(plan.claimable_count(), 1, "a chain is not a fan-out");

        // D01 lands -> D02 opens, D03 still waits.
        let i = at(&plan, "2026-09-01");
        plan.partitions[i].state = State::Succeeded;
        assert!(plan.claimable(at(&plan, "2026-09-02")));
        assert!(!plan.claimable(at(&plan, "2026-09-03")));

        // D02 FAILS -> D03 stays shut, and the reason names the retry.
        let i = at(&plan, "2026-09-02");
        plan.partitions[i].state = State::Failed;
        let three = at(&plan, "2026-09-03");
        assert!(!plan.claimable(three));
        assert_eq!(
            plan.blocked_reason(three).as_deref(),
            Some("waiting for 2026-09-02, which has not succeeded")
        );

        // Acceptance criterion 4: retry D02, it succeeds, D03 is eligible.
        plan.retry_open(None);
        let i = at(&plan, "2026-09-02");
        plan.partitions[i].state = State::Succeeded;
        assert!(plan.claimable(three), "a successful retry unblocks what followed");
    }

    /// A hole has no slice, so the rule has to block on a key that is absent.
    #[test]
    fn a_never_published_predecessor_blocks_and_says_so() {
        let def = dated("D{date:YYYYMMDD}.zip");
        let mut plan = ledger(&def, &["D20260901.zip", "D20260903.zip"]);
        assert_eq!(plan.partitions.len(), 2, "the hole is not a unit of work");

        let i = at(&plan, "2026-09-01");
        plan.partitions[i].state = crate::backfill::State::Succeeded;
        let three = at(&plan, "2026-09-03");
        assert!(!plan.claimable(three), "2026-09-02 was never published");
        assert_eq!(
            plan.blocked_reason(three).as_deref(),
            Some("waiting for 2026-09-02, which was never published"),
            "a hole and a failure are different conversations"
        );
    }

    /// The first link of an epoch answers to the baseline, which has no slice.
    #[test]
    fn the_first_link_after_a_baseline_is_claimable() {
        let def = SequenceDef {
            baseline: Some("2026-08-31".into()),
            ..dated("D{date:YYYYMMDD}.zip")
        };
        let plan = ledger(&def, &["D20260901.zip", "D20260902.zip"]);
        let first = at(&plan, "2026-09-01");
        assert_eq!(
            plan.partitions[first].requires, None,
            "requiring the baseline would block every chain forever"
        );
        assert!(plan.claimable(first));
        assert!(!plan.claimable(at(&plan, "2026-09-02")));
    }

    /// Continuity is opt-in: without it, the same links plan and nothing blocks.
    #[test]
    fn without_continuity_nothing_is_ordered() {
        let def = SequenceDef { require_continuity: false, ..dated("D{date:YYYYMMDD}.zip") };
        let plan = ledger(&def, &["D20260901.zip", "D20260903.zip"]);
        assert_eq!(plan.claimable_count(), 2, "a plain feed may have gaps");
        assert!(plan.partitions.iter().all(|p| p.requires.is_none()));
    }

    /// Provenance, acceptance criterion 6.
    #[test]
    fn a_slice_carries_its_key_predecessor_object_and_epoch() {
        let def = SequenceDef {
            baseline: Some("2026-08-31".into()),
            epoch: Some("F2".into()),
            ..dated("D{date:YYYYMMDD}.zip")
        };
        let plan = ledger(&def, &["D20260901.zip", "D20260902.zip"]);
        let p = &plan.partitions[at(&plan, "2026-09-02")];
        assert_eq!(p.params["sequence_key"], "2026-09-02");
        assert_eq!(p.params["sequence_previous"], "2026-09-01");
        assert_eq!(p.params["sequence_epoch"], "F2");
        assert!(p.params["sequence_object"].ends_with("D20260902.zip"));
        assert_eq!(p.source_uri.as_deref(), Some("s3://reg/D20260902.zip"));
        // Two epochs of the same key are different work, so a new snapshot does
        // not collide with what the old generation already did.
        let other = SequenceDef { epoch: Some("F3".into()), ..def.clone() };
        let plan2 = ledger(&other, &["D20260901.zip", "D20260902.zip"]);
        assert_ne!(
            plan.partitions[0].occurrence, plan2.partitions[0].occurrence,
            "the epoch is part of what a slice IS"
        );
    }

    #[test]
    fn a_definition_is_read_off_the_document() {
        let doc = serde_json::json!({
            "sequence": {
                "pattern": "D{date:YYYYMMDD}.KBO.zip",
                "order": { "type": "date", "cadence": "day" },
                "requireContinuity": true,
                "baseline": "2026-08-31"
            }
        });
        let def = of(&doc).expect("a definition");
        assert!(def.require_continuity);
        assert_eq!(def.baseline.as_deref(), Some("2026-08-31"));
        assert_eq!(def.order, Order::Date { cadence: Cadence::Day });
        assert!(of(&serde_json::json!({ "nodes": [] })).is_none());
    }
}
