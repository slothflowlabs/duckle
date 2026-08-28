//! `duckle-runner cache` - see and drop the stage outputs kept for reuse
//! (#252, slice 1).
//!
//! The counterpart to `duckle-runner checkpoint`, and deliberately less
//! careful than it. A checkpoint entry is a result that was already paid for,
//! so pruning it costs money and needs an explicit retention. Everything here
//! can be recomputed, so clearing it costs only time and needs no ceremony.

use std::path::{Path, PathBuf};

pub const CACHE_USAGE: &str = "\
duckle-runner cache <list|clear> [flags]

See and drop the stage outputs kept for reuse.

  list                       what is cached, by pipeline and node
  clear                      drop cached outputs

  --workspace DIR   the workspace (default: .)
  --pipeline NAME   clear: only this pipeline
  --json            machine-readable output

A cached output is served on the next run only when the node's settings and the
rows arriving from upstream are both unchanged, so a stale entry cannot be
returned by a changed pipeline. Clearing is safe at any time: everything here
can be recomputed. To distrust the cache for one run without dropping it, run
with --no-cache.
";

struct Opts {
    verb: String,
    workspace: PathBuf,
    pipeline: Option<String>,
    json: bool,
}

fn parse(argv: &[String]) -> Result<Opts, String> {
    let mut o = Opts {
        verb: String::new(),
        workspace: PathBuf::from("."),
        pipeline: None,
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
        println!("{}", CACHE_USAGE);
        return Ok(0);
    }
    let o = parse(&argv)?;
    let ws: &Path = &o.workspace;
    use duckle_duckdb_engine::outcache;

    match o.verb.as_str() {
        "list" => {
            let rows = outcache::entries(ws);
            if o.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "workspace": ws.display().to_string(),
                        "cached": rows,
                    }))
                    .unwrap_or_default()
                );
                return Ok(0);
            }
            if rows.is_empty() {
                println!("nothing cached under {}", ws.display());
                return Ok(0);
            }
            let total: u64 = rows.iter().map(|r| r.bytes).sum();
            for r in &rows {
                println!(
                    "  {:24} {:24} {:>4} file(s)  {:>10}",
                    r.pipeline,
                    r.node,
                    r.files,
                    human(r.bytes)
                );
            }
            println!("{} stage(s), {} in total", rows.len(), human(total));
            Ok(0)
        }
        "clear" => {
            let removed = outcache::clear(ws, o.pipeline.as_deref());
            if o.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({ "removed": removed }))
                        .unwrap_or_default()
                );
            } else {
                println!(
                    "{removed} cached output{} dropped; the next run recomputes them.",
                    if removed == 1 { "" } else { "s" }
                );
            }
            Ok(0)
        }
        other => Err(format!("unknown cache verb {other:?} - expected list or clear")),
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
        let o = parse(&argv(&["clear", "--pipeline", "nightly", "--json"])).unwrap();
        assert_eq!(o.verb, "clear");
        assert_eq!(o.pipeline.as_deref(), Some("nightly"));
        assert!(o.json);
        let o = parse(&argv(&["--json", "list"])).unwrap();
        assert_eq!(o.verb, "list");
    }

    /// A flag that swallowed the next word would clear the wrong thing, so a
    /// missing value is an error rather than a default.
    #[test]
    fn a_flag_with_no_value_is_an_error_not_a_silent_default() {
        assert!(parse(&argv(&["clear", "--pipeline"])).is_err());
        assert!(parse(&argv(&["list", "--nonsense"])).is_err());
    }
}
