//! `duckle-runner baseline` - see and re-base what `qa.baseline` treats as
//! normal (#281).
//!
//! The operation that matters here is `accept`. A source can legitimately
//! change shape, and when it does the accepted history describes a world that
//! no longer exists: every run fails from then on. Without a deliberate way to
//! say "this is the new normal", the only options left are removing the node or
//! widening its thresholds until they mean nothing - and the second one leaves
//! a check that looks present and is not.

use std::path::{Path, PathBuf};

pub const BASELINE_USAGE: &str = "\
duckle-runner baseline <list|inspect|accept|clear> [flags]

See and re-base what qa.baseline treats as normal.

  list                       every node with a baseline
  inspect                    the accepted median against the last run
  accept                     make the last measured profile the new normal
  clear                      forget the accepted history

  --workspace DIR   the workspace (default: .)
  --pipeline NAME   which pipeline           (inspect / accept / clear)
  --node ID         which qa.baseline node   (inspect / accept / clear)
  --history N       how many profiles to keep on accept (default: 10)
  --json            machine-readable output

`accept` promotes what the LAST RUN measured; it does not invent a number, so a
node no run has measured yet has nothing to accept. Run the pipeline, look at
`inspect`, then accept. Both accept and clear are recorded in the audit log with
the value they replaced.
";

struct Opts {
    verb: String,
    workspace: PathBuf,
    pipeline: Option<String>,
    node: Option<String>,
    history: usize,
    json: bool,
}

fn parse(argv: &[String]) -> Result<Opts, String> {
    let mut o = Opts {
        verb: String::new(),
        workspace: PathBuf::from("."),
        pipeline: None,
        node: None,
        history: 10,
        json: false,
    };
    let mut i = 0;
    while i < argv.len() {
        let a = argv[i].as_str();
        let mut take = |what: &str| -> Result<String, String> {
            i += 1;
            argv.get(i).cloned().ok_or_else(|| format!("{what} needs a value"))
        };
        match a {
            "--workspace" => o.workspace = PathBuf::from(take("--workspace")?),
            "--pipeline" => o.pipeline = Some(take("--pipeline")?),
            "--node" => o.node = Some(take("--node")?),
            "--history" => {
                let v = take("--history")?;
                o.history =
                    v.parse().map_err(|_| format!("--history wants a number, got {v}"))?;
            }
            "--json" => o.json = true,
            other if other.starts_with('-') => return Err(format!("unknown flag {other}")),
            other if o.verb.is_empty() => o.verb = other.to_string(),
            other => return Err(format!("unexpected argument {other}")),
        }
        i += 1;
    }
    Ok(o)
}

/// Both halves of the subject, or a message naming the one that is missing.
fn subject(o: &Opts) -> Result<(String, String), String> {
    match (&o.pipeline, &o.node) {
        (Some(p), Some(n)) => Ok((p.clone(), n.clone())),
        _ => Err(
            "this needs --pipeline and --node. `baseline list` shows what is available.".into(),
        ),
    }
}

pub fn run() -> Result<i32, String> {
    let argv: Vec<String> = std::env::args().skip(2).collect();
    if argv.is_empty() || argv.iter().any(|a| a == "--help" || a == "-h") {
        println!("{}", BASELINE_USAGE);
        return Ok(0);
    }
    let o = parse(&argv)?;
    let ws: &Path = &o.workspace;
    use duckle_duckdb_engine::baseline;

    match o.verb.as_str() {
        "list" => {
            let rows = baseline::list(ws);
            if o.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({ "baselines": rows }))
                        .unwrap_or_default()
                );
                return Ok(0);
            }
            if rows.is_empty() {
                println!("no baselines under {}", ws.display());
                return Ok(0);
            }
            for r in &rows {
                println!(
                    "  {:24} {:20} {:>3} accepted  last {:10} {}",
                    r.pipeline,
                    r.node,
                    r.accepted,
                    r.observed_status.as_deref().unwrap_or("-"),
                    if r.pending { "(unaccepted profile waiting)" } else { "" }
                );
            }
            Ok(0)
        }
        "inspect" => {
            let (p, n) = subject(&o)?;
            let view = baseline::inspect(ws, &p, &n);
            if o.json {
                println!("{}", serde_json::to_string_pretty(&view).unwrap_or_default());
                return Ok(0);
            }
            println!(
                "{}/{}: {} accepted profile(s), last run {}",
                view.status.pipeline,
                view.status.node,
                view.status.accepted,
                view.status.observed_status.as_deref().unwrap_or("never measured")
            );
            for v in &view.violations {
                println!("  ! {v}");
            }
            println!("  {:28} {:>16} {:>16} {:>9}", "metric", "baseline", "last run", "change");
            for m in &view.metrics {
                println!(
                    "  {:28} {:>16} {:>16} {:>8}",
                    m.metric,
                    m.baseline.map(|v| format!("{v}")).unwrap_or_else(|| "-".into()),
                    m.observed.map(|v| format!("{v}")).unwrap_or_else(|| "-".into()),
                    m.change_pct.map(|v| format!("{v:+.1}%")).unwrap_or_else(|| "-".into()),
                );
            }
            if view.status.pending {
                println!("\nThere is a measured profile that is not accepted.");
                println!(
                    "Accept it with: duckle-runner baseline accept --pipeline {} --node {}",
                    view.status.pipeline, view.status.node
                );
            }
            Ok(0)
        }
        "accept" => {
            let (p, n) = subject(&o)?;
            let after = baseline::accept(ws, &p, &n, o.history).map_err(|e| e.to_string())?;
            if o.json {
                println!("{}", serde_json::to_string_pretty(&after).unwrap_or_default());
            } else {
                println!(
                    "{p}/{n}: the last measured profile is now the baseline ({} accepted).",
                    after.status.accepted
                );
                println!("Recorded in the audit log with the value it replaced.");
            }
            Ok(0)
        }
        "clear" => {
            let (p, n) = subject(&o)?;
            let dropped = baseline::clear(ws, &p, &n).map_err(|e| e.to_string())?;
            if o.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({ "dropped": dropped }))
                        .unwrap_or_default()
                );
            } else {
                println!(
                    "{p}/{n}: {dropped} accepted profile(s) forgotten. The next run starts the \
                     history over and cannot fail against a baseline that no longer exists."
                );
            }
            Ok(0)
        }
        other => Err(format!(
            "unknown baseline verb {other:?} - expected list, inspect, accept or clear"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(s: &[&str]) -> Vec<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn flags_parse_in_any_order() {
        let o = parse(&argv(&["accept", "--pipeline", "orders", "--node", "q", "--json"])).unwrap();
        assert_eq!(o.verb, "accept");
        assert_eq!(o.pipeline.as_deref(), Some("orders"));
        assert_eq!(o.node.as_deref(), Some("q"));
        assert!(o.json);
        let o = parse(&argv(&["--json", "list"])).unwrap();
        assert_eq!(o.verb, "list");
        assert_eq!(o.history, 10, "a default that matches the node's own default");
    }

    #[test]
    fn a_flag_with_no_value_is_an_error_not_a_silent_default() {
        assert!(parse(&argv(&["accept", "--pipeline"])).is_err());
        assert!(parse(&argv(&["accept", "--history", "lots"])).is_err());
        assert!(parse(&argv(&["list", "--nonsense"])).is_err());
    }

    /// Naming half a subject is a mistake worth catching, not a wildcard: a
    /// missing --node must not mean "every node in the pipeline".
    #[test]
    fn half_a_subject_is_refused() {
        let o = parse(&argv(&["accept", "--pipeline", "orders"])).unwrap();
        let err = subject(&o).unwrap_err();
        assert!(err.contains("--pipeline and --node"), "{err}");
    }
}
