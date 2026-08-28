//! `duckle-runner checkpoint` - see and prune the results a stage has already
//! paid for (#252).
//!
//! An item checkpoint is the durable record of work that cost money, so it
//! needs the same two operations any store of that kind does: say what is in
//! it, and bound how much of it is kept. Pruning is explicit rather than
//! automatic because the entries ARE the paid results - dropping one silently
//! is the same as buying it again.

use std::path::{Path, PathBuf};

pub const CHECKPOINT_USAGE: &str = "\
duckle-runner checkpoint <status|prune> [flags]

See and bound the results a stage has already paid for.

  status                     what each stage has checkpointed
  prune                      drop entries, oldest first

  --workspace DIR   the workspace (default: .)
  --retain-days N   prune: drop anything older than N days
  --max-bytes N     prune: then drop oldest until under N bytes
  --json            machine-readable output

An entry holds the OUTPUT of one item, not just the fact that it succeeded, so
a rerun can rebuild the row without calling the API again. Dropping one means
the item is bought again on the next run, which is why nothing is pruned unless
you ask for it.
";

struct Opts {
    verb: String,
    workspace: PathBuf,
    retain_days: Option<u64>,
    max_bytes: Option<u64>,
    json: bool,
}

fn parse(argv: &[String]) -> Result<Opts, String> {
    let mut o = Opts {
        verb: String::new(),
        workspace: PathBuf::from("."),
        retain_days: None,
        max_bytes: None,
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
            "--retain-days" => {
                let v = take("--retain-days")?;
                o.retain_days =
                    Some(v.parse().map_err(|_| format!("--retain-days wants a number, got {v}"))?);
            }
            "--max-bytes" => {
                let v = take("--max-bytes")?;
                o.max_bytes =
                    Some(v.parse().map_err(|_| format!("--max-bytes wants a number, got {v}"))?);
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

pub fn run() -> Result<i32, String> {
    let argv: Vec<String> = std::env::args().skip(2).collect();
    if argv.is_empty() || argv.iter().any(|a| a == "--help" || a == "-h") {
        println!("{}", CHECKPOINT_USAGE);
        return Ok(0);
    }
    let o = parse(&argv)?;
    let ws: &Path = &o.workspace;
    use duckle_duckdb_engine::checkpoint;

    match o.verb.as_str() {
        "status" => {
            let rows = checkpoint::statuses(ws);
            if o.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "workspace": ws.display().to_string(),
                        "checkpoints": rows,
                    }))
                    .unwrap_or_default()
                );
                return Ok(0);
            }
            if rows.is_empty() {
                println!("no checkpoints under {}", ws.display());
                return Ok(0);
            }
            let total: u64 = rows.iter().map(|r| r.bytes).sum();
            for r in &rows {
                println!(
                    "  {:24} {:24} {:>9} item(s)  {:>10}  {}",
                    r.pipeline,
                    r.node,
                    r.entries,
                    human(r.bytes),
                    r.newest.as_deref().unwrap_or("-")
                );
            }
            println!("{} stage(s), {} in total", rows.len(), human(total));
            Ok(0)
        }
        "prune" => {
            if o.retain_days.is_none() && o.max_bytes.is_none() {
                // Refusing beats guessing: these entries are paid results, and
                // a default retention nobody chose would delete them.
                return Err(
                    "prune needs --retain-days or --max-bytes. These entries are results that \
                     were already paid for, so nothing is dropped on a default nobody chose."
                        .into(),
                );
            }
            let removed = checkpoint::prune(ws, o.retain_days, o.max_bytes)
                .map_err(|e| e.to_string())?;
            if o.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({ "removed": removed }))
                        .unwrap_or_default()
                );
            } else {
                println!(
                    "{removed} entr{} dropped; they will be recomputed on the next run.",
                    if removed == 1 { "y" } else { "ies" }
                );
            }
            Ok(0)
        }
        other => Err(format!(
            "unknown checkpoint verb {other:?} - expected status or prune"
        )),
    }
}

fn human(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
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
        let o = parse(&argv(&["prune", "--retain-days", "30", "--json"])).unwrap();
        assert_eq!(o.verb, "prune");
        assert_eq!(o.retain_days, Some(30));
        assert!(o.json);
        let o = parse(&argv(&["--json", "status"])).unwrap();
        assert_eq!(o.verb, "status");
    }

    #[test]
    fn a_flag_with_no_value_is_an_error_not_a_silent_default() {
        assert!(parse(&argv(&["prune", "--retain-days"])).is_err());
        assert!(parse(&argv(&["prune", "--max-bytes", "lots"])).is_err());
        assert!(parse(&argv(&["status", "--nonsense"])).is_err());
    }
}
