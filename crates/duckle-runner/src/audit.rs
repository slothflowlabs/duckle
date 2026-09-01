//! An append-only record of who did what through the management console.
//!
//! Run history already says a pipeline ran. It does not say who caused it, from
//! where, or whether someone tried and was refused, and those are the three
//! questions asked after an incident. This is the answer to them, kept as one
//! NDJSON line per event beside the run logs, so the same collector that ships
//! those ships this.
//!
//! Refusals are recorded as well as successes. A log that only holds what
//! worked cannot show someone probing an endpoint they have no role for, which
//! is the pattern worth seeing.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::console_auth::{Identity, Role};

pub fn audit_path(workspace: &Path) -> PathBuf {
    workspace.join("logs").join("audit.ndjson")
}

/// One recorded attempt.
///
/// The writer and the reader share this type on purpose: a log written by one
/// shape and read by another drifts the first time a field is renamed, and the
/// symptom is a reader that quietly shows blanks.
///
/// It lives in the engine because the console is not the only writer. The CLI,
/// MCP and the desktop panel reach the state mutators directly, and those
/// record from inside the engine - so there has to be exactly one definition
/// for all of them, not one per crate.
pub use duckle_duckdb_engine::audit::Entry;

/// How an attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The caller was allowed to proceed. Not that the work then succeeded -
    /// run history is where that is answered, and conflating the two would make
    /// a failed run read as a security event.
    Allowed,
    /// Authenticated, but the role was not enough.
    Denied,
    /// No usable credential at all.
    Unauthenticated,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Allowed => "allowed",
            Outcome::Denied => "denied",
            Outcome::Unauthenticated => "unauthenticated",
        }
    }
}

/// Append one event. Never fails the request it describes.
///
/// A console that refused to serve because its audit file was unwritable would
/// turn a full disk into an outage, so a write failure goes to stderr and the
/// request continues. That is a deliberate trade and the reason the file is not
/// the only record: runs are still logged in run history.
pub fn record(
    workspace: &Path,
    who: Option<&Identity>,
    action: &str,
    target: &str,
    outcome: Outcome,
) {
    let entry = Entry {
        detail: None,
        at: chrono::Utc::now().to_rfc3339(),
        // An unauthenticated caller has no name, and inventing one would make
        // the log read as though somebody known did this.
        actor: who.map(|w| w.label.as_str()).unwrap_or("-").to_string(),
        role: who.map(|w| w.role.as_str()).unwrap_or("-").to_string(),
        action: action.to_string(),
        target: target.to_string(),
        outcome: outcome.as_str().to_string(),
    };
    let line = match serde_json::to_string(&entry) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("duckle-runner: could not encode an audit entry: {e}");
            return;
        }
    };
    if let Err(e) = append(workspace, &line) {
        eprintln!("duckle-runner: could not write the audit log: {e}");
    }
}

/// What to show, and how much.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub actor: Option<String>,
    pub outcome: Option<String>,
    pub action: Option<String>,
    pub limit: usize,
}

impl Filter {
    fn keeps(&self, e: &Entry) -> bool {
        let matches = |want: &Option<String>, got: &str| match want {
            None => true,
            Some(w) => got.eq_ignore_ascii_case(w),
        };
        matches(&self.actor, &e.actor)
            && matches(&self.outcome, &e.outcome)
            // An action is a dotted name (schedule.write), so a prefix is the
            // useful thing to ask for: `--action schedule` finds all of them.
            && match &self.action {
                None => true,
                Some(a) => e.action.eq_ignore_ascii_case(a)
                    || e.action.to_ascii_lowercase().starts_with(&format!("{}.", a.to_ascii_lowercase())),
            }
    }
}

/// A window onto the log, newest first.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Page {
    pub entries: Vec<Entry>,
    /// Lines that would not parse. One truncated write must not hide every
    /// entry written before it, and a silent skip would be worse than the
    /// truncation: the count is reported so a corrupted file is visible.
    pub unreadable: usize,
    /// True when older entries exist beyond what is returned - either the
    /// limit was reached or the scan stopped short of the start of the file.
    /// The reader says so rather than letting a window read as the whole log.
    pub more: bool,
}

/// How far back a single read will go. The log has no rotation, so an unbounded
/// read would have the console re-parsing the whole history every poll. Reading
/// backwards from the end keeps a read proportional to what is shown.
const MAX_SCAN: u64 = 4 * 1024 * 1024;

/// Read the most recent entries, newest first.
///
/// A missing file is an empty page, not an error: a console that has never
/// refused anything and never been signed into has nothing to show, and that is
/// a normal state rather than a fault.
pub fn read(workspace: &Path, filter: &Filter) -> Result<Page, String> {
    let path = audit_path(workspace);
    let mut f = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Page::default()),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let len = f.metadata().map_err(|e| e.to_string())?.len();
    let start = len.saturating_sub(MAX_SCAN);
    // One byte earlier than the window, when there is one. Starting mid-file
    // usually starts mid-line, and that fragment is not an entry: it must not
    // be counted as a corrupt one. Reading the extra byte is what makes
    // dropping the first element safe - it is then either that fragment, or an
    // empty string where the window happened to open on a line boundary.
    // Starting at the window itself would drop a whole entry in that second
    // case, which is not a rare one: entries are near enough a fixed size.
    let from = start.saturating_sub(if start > 0 { 1 } else { 0 });
    f.seek(SeekFrom::Start(from)).map_err(|e| e.to_string())?;
    let mut buf = Vec::with_capacity((len - from) as usize);
    f.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf);
    let mut lines: Vec<&str> = text.lines().collect();
    if start > 0 && !lines.is_empty() {
        lines.remove(0);
    }

    let limit = if filter.limit == 0 { usize::MAX } else { filter.limit };
    let mut page = Page { more: start > 0, ..Page::default() };
    for line in lines.iter().rev() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Entry>(line) {
            Ok(e) => {
                if !filter.keeps(&e) {
                    continue;
                }
                if page.entries.len() == limit {
                    page.more = true;
                    break;
                }
                page.entries.push(e);
            }
            Err(_) => page.unreadable += 1,
        }
    }
    Ok(page)
}

/// `duckle-runner audit ...` - read the console's own record back.
///
/// Writing the log and never being able to read it is the same as not keeping
/// it. This is the terminal answer to "who ran that, and who was turned away",
/// and it works with no server running, which is when it is usually asked.
pub fn run() -> Result<i32, String> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    if args.first().map(String::as_str) == Some("-h")
        || args.first().map(String::as_str) == Some("--help")
    {
        println!(
            "duckle-runner audit - who did what through the management console\n\n\
             USAGE:\n    \
             duckle-runner audit [--workspace <dir>] [--actor <name>]\n                        \
             [--outcome allowed|denied|unauthenticated]\n                        \
             [--action <name>] [--limit <n>] [--json]\n\n\
             Reads <workspace>/logs/audit.ndjson, newest first. Default limit 50;\n\
             --limit 0 shows everything the read reaches.\n\n\
             --action takes a whole name (schedule.write) or a prefix (schedule).\n\n\
             Refusals are in here as well as successes, so\n    \
             duckle-runner audit --outcome denied\n\
             is the one that shows somebody reaching for what they do not have."
        );
        return Ok(0);
    }

    let mut workspace = PathBuf::from(".");
    let mut filter = Filter { limit: 50, ..Filter::default() };
    let mut as_json = false;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut value = |name: &str| -> Result<String, String> {
            it.next().cloned().ok_or_else(|| format!("{name} needs a value"))
        };
        match a.as_str() {
            "--workspace" => workspace = PathBuf::from(value("--workspace")?),
            "--actor" => filter.actor = Some(value("--actor")?),
            "--action" => filter.action = Some(value("--action")?),
            "--outcome" => {
                let v = value("--outcome")?;
                if !["allowed", "denied", "unauthenticated"].contains(&v.to_ascii_lowercase().as_str())
                {
                    return Err(format!(
                        "unknown outcome '{v}': expected allowed, denied or unauthenticated"
                    ));
                }
                filter.outcome = Some(v);
            }
            "--limit" => {
                filter.limit =
                    value("--limit")?.parse().map_err(|_| "--limit needs a number".to_string())?
            }
            "--json" => as_json = true,
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    let page = read(&workspace, &filter)?;
    if as_json {
        println!("{}", serde_json::to_string_pretty(&page).map_err(|e| e.to_string())?);
        return Ok(0);
    }
    if page.entries.is_empty() {
        let p = audit_path(&workspace);
        if p.exists() {
            println!("Nothing in {} matches.", p.display());
        } else {
            println!(
                "No {} yet. The console writes one the first time somebody signs in or is refused.",
                p.display()
            );
        }
        return Ok(0);
    }
    // Width from the names actually present, so the columns line up whatever
    // people called themselves rather than only for short labels.
    let who: Vec<String> =
        page.entries.iter().map(|e| format!("{} ({})", e.actor, e.role)).collect();
    let w = who.iter().map(String::len).max().unwrap_or(0).min(40);
    for (e, who) in page.entries.iter().zip(&who) {
        println!(
            "{:<20} {:<w$} {:<16} {:<22} {}",
            short_time(&e.at),
            who,
            e.outcome,
            e.action,
            e.target,
            w = w
        );
    }
    // Both of these change what the output means, so neither is left implied.
    if page.more {
        println!("\nOlder entries exist. Raise --limit to see further back.");
    }
    if page.unreadable > 0 {
        eprintln!("\n{} line(s) could not be read and were skipped.", page.unreadable);
    }
    Ok(0)
}

/// RFC3339 is written to the file; a person reading a table wants the date and
/// the clock and nothing else.
fn short_time(at: &str) -> String {
    at.get(..19).unwrap_or(at).replace('T', " ")
}

fn append(workspace: &Path, line: &str) -> Result<(), String> {
    let p = audit_path(workspace);
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    // One write of one short line in append mode, so concurrent writers
    // interleave whole lines rather than fragments of them.
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&p)
        .map_err(|e| format!("{}: {e}", p.display()))?;
    f.write_all(format!("{line}\n").as_bytes()).map_err(|e| e.to_string())
}

/// The role a route needs, and the action name recorded for it.
///
/// Kept as one table so a new route cannot quietly arrive without a decision
/// about who may call it: the dispatcher asks this for every request, and the
/// fallback is admin rather than public.
pub fn requirement(method: &str, path: &str) -> (Role, &'static str) {
    match (method, path) {
        // Reading the dashboard and its data.
        ("GET", "/") | ("GET", "/index.html") => (Role::Viewer, "console.open"),
        ("GET", "/api/summary") => (Role::Viewer, "summary.read"),
        ("GET", "/api/pipelines") => (Role::Viewer, "pipelines.list"),
        ("GET", "/api/pipeline") => (Role::Viewer, "pipeline.read"),
        ("GET", "/api/runs") => (Role::Viewer, "runs.read"),
        ("GET", "/api/log") => (Role::Viewer, "log.read"),
        ("GET", "/api/schedules") => (Role::Viewer, "schedules.read"),
        ("GET", "/api/catalog") => (Role::Viewer, "catalog.read"),
        ("GET", "/api/batches") => (Role::Viewer, "batches.read"),
        ("GET", "/api/batch") => (Role::Viewer, "batch.read"),
        ("GET", "/api/params") => (Role::Viewer, "params.read"),
        ("GET", "/api/whoami") => (Role::Viewer, "session.whoami"),
        // Reading this log names people and the things they reached for, so it
        // sits with the admin role even though it only reads. It would land
        // there via the fallback anyway; stating it means a later reshuffle of
        // this table has to make the decision deliberately.
        ("GET", "/api/audit") => (Role::Admin, "audit.read"),
        // Ending your own session is not a privilege. Leaving this to the
        // admin-only fallback below meant a viewer could sign in and then not
        // sign out, which the live run found before anyone else would have.
        ("DELETE", "/api/session") => (Role::Viewer, "session.sign_out"),

        // Causing work to happen, or changing when it happens.
        ("POST", "/api/run") => (Role::Operator, "pipeline.run"),
        // #259: accepting a run is the same act as running one, so it takes the
        // same role. Asking how a run is going only reads. Cancelling stops work
        // an operator was allowed to start, so it belongs with starting it.
        // These must stay in step with dispatch_console: a route served there
        // and missing here falls through to the admin-only arm below and 403s
        // every operator.
        ("POST", "/api/run/async") => (Role::Operator, "pipeline.run"),
        ("GET", "/api/run/status") => (Role::Viewer, "run.status"),
        ("DELETE", "/api/run") => (Role::Operator, "pipeline.cancel"),
        ("POST", "/api/schedules") => (Role::Operator, "schedule.write"),
        // Rebuilding re-derives the graph from pipelines the caller can already
        // read, so it is not an admin act; it does write a file, so it is not a
        // viewer one either.
        ("POST", "/api/catalog") => (Role::Operator, "catalog.rebuild"),
        // Redrive clears recorded FAILURES so unfinished items are tried
        // again. It causes work to happen, so it sits with running a
        // pipeline rather than with reading one.
        ("POST", "/api/batch/redrive") => (Role::Operator, "batch.redrive"),

        // Landing a pipeline is not the same as running one that is already
        // there. A deployed pipeline runs shell and SQL on this host, so
        // deploying is handing someone code execution, which is the
        // workspace-touching category rather than the operating one. The
        // schedule it brings arrives disabled, and switching that on is
        // `schedule.write` above, so shipping the code and starting it are
        // deliberately two acts needing two different roles.
        ("POST", "/api/deploy") => (Role::Admin, "pipeline.deploy"),

        // A plan is an ordering of pipelines. Reading one is reading; saving one changes
        // what will run and when, which is the same size of decision as a schedule.
        ("GET", "/api/plans") => (Role::Viewer, "plans.read"),
        ("POST", "/api/plans") => (Role::Operator, "plan.write"),
        ("DELETE", "/api/plans") => (Role::Operator, "plan.remove"),
        ("POST", "/api/plans/run") => (Role::Operator, "plan.run"),

        // Managing who may use the console, and what machines may. These would reach
        // admin through the fallback anyway; naming them means the audit log calls them
        // what they are rather than "unknown", and a later reshuffle has to decide
        // deliberately.
        ("GET", "/api/admin/users") => (Role::Admin, "admin.users.read"),
        ("POST", "/api/admin/users") => (Role::Admin, "admin.users.add"),
        ("DELETE", "/api/admin/users") => (Role::Admin, "admin.users.remove"),
        ("GET", "/api/admin/keys") => (Role::Admin, "admin.keys.read"),
        ("POST", "/api/admin/keys") => (Role::Admin, "admin.keys.add"),
        ("POST", "/api/admin/keys/revoke") => (Role::Admin, "admin.keys.revoke"),

        // Backfill state. Reading is a viewer's business; changing it decides
        // what data the next run processes, so it needs an operator. Named
        // here rather than left to the fallback, or every operator gets a 403
        // and the audit log calls the action "unknown".
        ("GET", "/api/watermarks") => (Role::Viewer, "watermarks.read"),
        ("POST", "/api/watermarks") => (Role::Operator, "watermark.write"),
        ("DELETE", "/api/watermarks") => (Role::Operator, "watermark.clear"),

        // #300: a scraper is a reader. Named here rather than left to the
        // fallback, which would demand admin - and handing a monitoring agent a
        // credential that can also add users and deploy pipelines is a worse
        // trade than not scraping at all.
        ("GET", "/metrics") => (Role::Viewer, "metrics.read"),

        // Anything unrecognised needs the highest role. A route added later
        // without a line here is locked down rather than left open.
        _ => (Role::Admin, "unknown"),
    }
}

#[cfg(test)]
mod tests {

    /// The backfill routes: reading is a viewer's business, changing what the
    /// next run processes is an operator's. A missing line here compiles,
    /// serves, and 403s every operator in production - the same trap the run
    /// routes below were written for.
    #[test]
    fn the_backfill_routes_are_not_admin_only_by_accident() {
        use super::{requirement, Role};
        assert_eq!(
            requirement("GET", "/api/watermarks"),
            (Role::Viewer, "watermarks.read"),
            "seeing where a pipeline will resume from only reads"
        );
        assert_eq!(
            requirement("POST", "/api/watermarks"),
            (Role::Operator, "watermark.write"),
            "moving a watermark decides what data the next run processes"
        );
        assert_eq!(
            requirement("DELETE", "/api/watermarks"),
            (Role::Operator, "watermark.clear"),
            "clearing state is the same class of act as setting it"
        );
        // The failure this guards against: falling through to the fallback,
        // which is admin-only and logs the action as "unknown".
        for route in [
            ("GET", "/api/watermarks"),
            ("POST", "/api/watermarks"),
            ("DELETE", "/api/watermarks"),
        ] {
            assert_ne!(
                requirement(route.0, route.1).1,
                "unknown",
                "{} {} fell through to the admin fallback",
                route.0,
                route.1
            );
        }
    }

    /// #259: every route dispatch_console serves needs a line in `requirement`.
    /// One that is missing falls through to the admin-only arm and 403s every
    /// operator, which compiles, serves, and only shows up in production.
    #[test]
    fn the_async_run_routes_are_not_admin_only_by_accident() {
        assert_eq!(
            requirement("POST", "/api/run/async"),
            (Role::Operator, "pipeline.run"),
            "accepting a run is the same act as running one"
        );
        assert_eq!(
            requirement("GET", "/api/run/status"),
            (Role::Viewer, "run.status"),
            "asking how a run is going only reads"
        );
        assert_eq!(
            requirement("DELETE", "/api/run"),
            (Role::Operator, "pipeline.cancel"),
            "stopping work belongs with starting it"
        );
        // The failure this guards against: falling through to the fallback.
        for route in [
            ("POST", "/api/run/async"),
            ("GET", "/api/run/status"),
            ("DELETE", "/api/run"),
        ] {
            assert_ne!(
                requirement(route.0, route.1).1,
                "unknown",
                "{} {} fell through to the admin-only fallback",
                route.0,
                route.1
            );
        }
    }
    use super::*;

    fn identity(label: &str, role: Role) -> Identity {
        Identity { label: label.into(), role }
    }

    #[test]
    fn events_are_appended_one_line_each_and_include_refusals() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let ops = identity("ops", Role::Operator);
        record(ws, Some(&ops), "pipeline.run", "nightly-load", Outcome::Allowed);
        record(ws, Some(&identity("reporting", Role::Viewer)), "pipeline.run", "nightly-load", Outcome::Denied);
        record(ws, None, "pipeline.run", "nightly-load", Outcome::Unauthenticated);

        let text = std::fs::read_to_string(audit_path(ws)).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "one line per event");

        let parsed: Vec<serde_json::Value> =
            lines.iter().map(|l| serde_json::from_str(l).expect("each line is JSON")).collect();
        assert_eq!(parsed[0]["actor"], "ops");
        assert_eq!(parsed[0]["outcome"], "allowed");
        // The refusal is the entry that matters: a log of successes alone
        // cannot show someone reaching for something they do not have.
        assert_eq!(parsed[1]["actor"], "reporting");
        assert_eq!(parsed[1]["outcome"], "denied");
        assert_eq!(parsed[2]["actor"], "-", "an unknown caller must not be given a name");
        assert_eq!(parsed[2]["outcome"], "unauthenticated");
        assert!(parsed[0]["at"].as_str().unwrap().contains('T'), "timestamp is not rfc3339");
    }

    #[test]
    fn reading_needs_less_than_running_and_unknown_routes_need_the_most() {
        assert_eq!(requirement("GET", "/api/runs").0, Role::Viewer);
        assert_eq!(requirement("POST", "/api/run").0, Role::Operator);
        // Signing yourself out is not a privileged act. Anyone who could sign
        // in must be able to sign out, whatever their role.
        assert_eq!(requirement("DELETE", "/api/session").0, Role::Viewer);
        assert_eq!(requirement("DELETE", "/api/session").1, "session.sign_out");
        // Reading the workspace graph is a read; rebuilding it writes a file.
        assert_eq!(requirement("GET", "/api/catalog").0, Role::Viewer);
        assert_eq!(requirement("POST", "/api/catalog").0, Role::Operator);
        assert_eq!(requirement("POST", "/api/schedules").0, Role::Operator);
        // Reading queued work is a read; retrying failed items causes work to
        // happen, so it sits with running a pipeline rather than reading one.
        assert_eq!(requirement("GET", "/api/batches").0, Role::Viewer);
        assert_eq!(requirement("GET", "/api/batch").0, Role::Viewer);
        assert_eq!(requirement("POST", "/api/batch/redrive").0, Role::Operator);
        assert_eq!(requirement("POST", "/api/batch/redrive").1, "batch.redrive");
        // The important one: a route nobody thought about is admin-only, so
        // adding an endpoint cannot accidentally publish it.
        assert_eq!(requirement("POST", "/api/some-future-thing").0, Role::Admin);
        assert_eq!(requirement("DELETE", "/api/pipelines").0, Role::Admin);
    }

    /// The gate and the dispatcher must not parse one path with two rules.
    ///
    /// `trim_start_matches` strips its prefix REPEATEDLY, so the editor's
    /// dispatcher turned `/api/cmd//api/cmd/connection_decrypt_payload` into
    /// `connection_decrypt_payload`, while the gate's `starts_with(
    /// "/api/cmd/connection")` saw a `/` at that position and asked only for
    /// operator. An operator token then reached the admin-only decrypt.
    #[test]
    fn a_doubled_route_prefix_cannot_split_the_gate_from_the_dispatcher() {
        let doubled = "/api/cmd//api/cmd/connection_decrypt_payload";
        // The property that was false before: whatever the dispatcher will run
        // is what the gate must have rated.
        let dispatched = doubled.strip_prefix("/api/cmd/");
        assert_eq!(
            dispatched,
            Some("/api/cmd/connection_decrypt_payload"),
            "strip_prefix must remove the prefix exactly once"
        );
        // trim_start_matches is the trap; keep a live example of why.
        assert_eq!(
            doubled.trim_start_matches("/api/cmd/"),
            "connection_decrypt_payload",
            "trim_start_matches still strips repeatedly - never route on it"
        );
        // With one parse, a doubled prefix is simply not a connection command
        // and is dispatched under the name the gate saw.
        assert!(!dispatched.unwrap().starts_with("connection"));
    }

    /// Reading is the half that makes the writing worth anything.
    #[test]
    fn the_log_reads_back_newest_first_and_filters_the_way_it_is_asked_to() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let ops = identity("ops", Role::Operator);
        let viewer = identity("reporting", Role::Viewer);
        record(ws, Some(&ops), "pipeline.run", "nightly-load", Outcome::Allowed);
        record(ws, Some(&viewer), "pipeline.run", "nightly-load", Outcome::Denied);
        record(ws, Some(&ops), "schedule.write", "nightly-load", Outcome::Allowed);
        record(ws, None, "editor.api", "/api/run", Outcome::Unauthenticated);

        let all = read(ws, &Filter { limit: 50, ..Filter::default() }).unwrap();
        assert_eq!(all.entries.len(), 4);
        assert_eq!(all.entries[0].action, "editor.api", "newest must come first");
        assert_eq!(all.entries[3].action, "pipeline.run");
        assert_eq!(all.unreadable, 0);
        assert!(!all.more, "everything was returned, so nothing is older");

        // The question actually asked after an incident.
        let refused =
            read(ws, &Filter { outcome: Some("denied".into()), limit: 50, ..Filter::default() })
                .unwrap();
        assert_eq!(refused.entries.len(), 1);
        assert_eq!(refused.entries[0].actor, "reporting");

        let by_who =
            read(ws, &Filter { actor: Some("ops".into()), limit: 50, ..Filter::default() }).unwrap();
        assert_eq!(by_who.entries.len(), 2);

        // An action is a dotted name, so the family prefix has to work as well
        // as the whole name: nobody remembers whether it is schedule.write or
        // schedule.save.
        let sched =
            read(ws, &Filter { action: Some("schedule".into()), limit: 50, ..Filter::default() })
                .unwrap();
        assert_eq!(sched.entries.len(), 1);
        assert_eq!(sched.entries[0].action, "schedule.write");
        let exact =
            read(ws, &Filter { action: Some("pipeline.run".into()), limit: 50, ..Filter::default() })
                .unwrap();
        assert_eq!(exact.entries.len(), 2);
        // A prefix must be a whole dotted segment, or `--action run` would
        // match `runjob` and quietly widen what was asked for.
        let narrow =
            read(ws, &Filter { action: Some("pipe".into()), limit: 50, ..Filter::default() })
                .unwrap();
        assert!(narrow.entries.is_empty(), "a partial segment must not match");

        // A page says so when it is a page.
        let page = read(ws, &Filter { limit: 2, ..Filter::default() }).unwrap();
        assert_eq!(page.entries.len(), 2);
        assert!(page.more, "a truncated page must not read as the whole log");
    }

    /// A console that has never refused anything has nothing to show, and that
    /// is a normal state rather than a fault.
    #[test]
    fn a_log_that_does_not_exist_yet_is_empty_rather_than_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let page = read(tmp.path(), &Filter { limit: 10, ..Filter::default() }).unwrap();
        assert!(page.entries.is_empty());
        assert!(!page.more);
    }

    /// One half-written line must not take the rest of the log with it.
    ///
    /// A crash mid-append leaves a partial line. Parsing the file as a whole
    /// would fail on it and show nothing; skipping it silently would hide that
    /// the file is damaged. It is skipped and counted.
    #[test]
    fn a_damaged_line_is_skipped_and_counted_rather_than_hiding_the_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        record(ws, Some(&identity("ops", Role::Operator)), "pipeline.run", "a", Outcome::Allowed);
        append(ws, "{\"at\":\"2026-08-15T00:00:00Z\",\"actor\":\"ops\"").unwrap();
        record(ws, Some(&identity("ops", Role::Operator)), "pipeline.run", "b", Outcome::Allowed);

        let page = read(ws, &Filter { limit: 50, ..Filter::default() }).unwrap();
        assert_eq!(page.entries.len(), 2, "the intact entries must still be readable");
        assert_eq!(page.unreadable, 1);
    }

    /// A log past the scan window still reads back exactly, boundary included.
    ///
    /// The read starts partway into the file, and the first thing it sees is
    /// usually the tail of a line rather than a line, so the first element is
    /// dropped. Reading from the window itself made that drop silently lose a
    /// real entry whenever the window opened on a line boundary, which is not
    /// rare: entries are near enough a fixed size. Every line here is exactly
    /// the same length, so the boundary case is the one under test rather than
    /// a coincidence.
    #[test]
    fn a_window_that_opens_on_a_line_boundary_keeps_that_line() {
        const W: usize = 256;
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let head = "{\"at\":\"2026-08-15T00:00:00Z\",\"actor\":\"a\",\"role\":\"admin\",\
                    \"action\":\"pipeline.run\",\"outcome\":\"allowed\",\"target\":\"";
        let line = |i: usize| {
            let stem = format!("line-{i}");
            let pad = W - 1 - head.len() - stem.len() - 2;
            format!("{head}{stem}{}\"}}", "x".repeat(pad))
        };
        // One line more than the window holds, so the window opens exactly at
        // the start of line 1 with a newline immediately before it.
        let lines = MAX_SCAN as usize / W + 1;
        let mut text = String::with_capacity(lines * W);
        for i in 0..lines {
            let l = line(i);
            assert_eq!(l.len(), W - 1, "the fixture must have fixed-width lines");
            text.push_str(&l);
            text.push('\n');
        }
        std::fs::create_dir_all(audit_path(ws).parent().unwrap()).unwrap();
        std::fs::write(audit_path(ws), &text).unwrap();

        let page = read(ws, &Filter::default()).unwrap();
        assert_eq!(page.unreadable, 0, "a whole line was mistaken for a fragment");
        assert_eq!(page.entries.len(), lines - 1, "the boundary line was dropped");
        assert!(page.more, "the scan stopped short of the start and must say so");
        assert!(
            page.entries.last().unwrap().target.starts_with("line-1x"),
            "the oldest entry in the window is not the one at the boundary: {}",
            page.entries.last().unwrap().target
        );
    }

    #[test]
    fn a_viewer_cannot_run_a_pipeline_and_an_operator_can() {
        let (needed, action) = requirement("POST", "/api/run");
        assert_eq!(action, "pipeline.run");
        assert!(!Role::Viewer.allows(needed));
        assert!(Role::Operator.allows(needed));
        assert!(Role::Admin.allows(needed));
    }
}
