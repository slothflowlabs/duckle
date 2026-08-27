//! `duckle-runner backfill` - inspect and edit the saved state a pipeline
//! resumes from, without opening the desktop app.
//!
//! Production deployments are headless, so a replay should not require getting
//! at the server's workspace through a GUI. The operations themselves are
//! engine-side already (`duckle_duckdb_engine::watermark`); this is the
//! headless surface for them, and `--json` makes it usable from CI and from an
//! agent.
//!
//! Five node kinds keep state in the same folder, and only two of them resume
//! from a value a person can write down. The other three - a Kafka resume
//! offset, a spool byte position, a tumbling window's buffer pointer - are
//! listed and can be CLEARED, but `set` on them is refused by the engine
//! rather than silently destroying what they were holding.

use std::path::{Path, PathBuf};

pub const BACKFILL_USAGE: &str = "\
duckle-runner backfill <list|set|clear> [flags]

Inspect and edit the state a pipeline resumes from.

  list                        show every node's saved state
  set    --node ID --value V  set an incremental watermark
         [--type SQLTYPE]     defaults to VARCHAR
  set    --node ID --snapshot N   set a DuckLake CDC snapshot id
  clear  --node ID            remove a node's state, so it starts over

  --pipeline FILE   the pipeline, for its name and workspace
  --name NAME       override the pipeline name (default: the file stem)
  --workspace DIR   override the workspace
  --json            machine-readable output

Only `incremental` and `snapshot` state can be set by hand. A Kafka resume
point, a spool position or a tumbling window's buffer have no single value
that means the same thing to the node that wrote them, so `set` refuses and
`clear` is the way to start those over.

Clearing is not always a full reload. A Kafka node with startFrom `latest`
SKIPS everything already in the topic when it has no saved offset, so clearing
it moves past that backlog rather than replaying it.
";

struct Opts {
    verb: String,
    pipeline: Option<PathBuf>,
    workspace: Option<PathBuf>,
    name: Option<String>,
    node: Option<String>,
    value: Option<String>,
    value_type: Option<String>,
    snapshot: Option<u64>,
    json: bool,
}

fn parse(argv: &[String]) -> Result<Opts, String> {
    let mut o = Opts {
        verb: String::new(),
        pipeline: None,
        workspace: None,
        name: None,
        node: None,
        value: None,
        value_type: None,
        snapshot: None,
        json: false,
    };
    let mut i = 0;
    while i < argv.len() {
        let a = argv[i].as_str();
        let mut take = |what: &str| -> Result<String, String> {
            i += 1;
            argv.get(i)
                .cloned()
                .ok_or_else(|| format!("{} needs a value", what))
        };
        match a {
            "--pipeline" => o.pipeline = Some(PathBuf::from(take("--pipeline")?)),
            "--workspace" => o.workspace = Some(PathBuf::from(take("--workspace")?)),
            "--name" => o.name = Some(take("--name")?),
            "--node" => o.node = Some(take("--node")?),
            "--value" => o.value = Some(take("--value")?),
            "--type" => o.value_type = Some(take("--type")?),
            "--snapshot" => {
                let v = take("--snapshot")?;
                o.snapshot = Some(
                    v.parse()
                        .map_err(|_| format!("--snapshot wants a number, got {v}"))?,
                );
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

/// Workspace and pipeline name, resolved the same way a run resolves them, so
/// what this reports is what a run actually reads.
fn resolve(o: &Opts) -> Result<(PathBuf, String), String> {
    let workspace = o
        .workspace
        .clone()
        .or_else(|| o.pipeline.as_deref().and_then(Path::parent).map(Path::to_path_buf))
        .or_else(|| std::env::var("DUCKLE_WORKSPACE").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("."));
    let name = match (&o.name, &o.pipeline) {
        (Some(n), _) => n.clone(),
        (None, Some(p)) => p
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .ok_or_else(|| "cannot read a pipeline name from that path".to_string())?,
        (None, None) => {
            return Err("need --pipeline (or --name) to know whose state to look at".into())
        }
    };
    Ok((workspace, name))
}

pub fn run() -> Result<i32, String> {
    let argv: Vec<String> = std::env::args().skip(2).collect();
    if argv.is_empty() || argv.iter().any(|a| a == "--help" || a == "-h") {
        println!("{}", BACKFILL_USAGE);
        return Ok(0);
    }
    let o = parse(&argv)?;
    let (workspace, name) = resolve(&o)?;
    use duckle_duckdb_engine::watermark;

    match o.verb.as_str() {
        "list" => {
            let entries = watermark::list(&workspace, &name);
            if o.json {
                let payload = serde_json::json!({
                    "pipeline": name,
                    "workspace": workspace.display().to_string(),
                    "entries": entries,
                });
                println!("{}", serde_json::to_string_pretty(&payload).unwrap_or_default());
            } else if entries.is_empty() {
                println!("(no saved state for '{}' under {})", name, workspace.display());
            } else {
                println!("saved state for '{}':", name);
                for e in &entries {
                    let ty = e
                        .value_type
                        .as_deref()
                        .map(|t| format!(" ({})", t))
                        .unwrap_or_default();
                    // Say which rows the operator can act on, rather than
                    // letting them find out by having a set refused.
                    let editable = if e.editable { "" } else { "   [clear only]" };
                    println!("  {:24} {:12} {}{}{}", e.node_id, e.kind, e.value, ty, editable);
                }
            }
            Ok(0)
        }
        "set" => {
            let node = o
                .node
                .as_deref()
                .ok_or_else(|| "set needs --node".to_string())?;
            match (&o.value, o.snapshot) {
                (Some(v), None) => {
                    watermark::set_incremental(
                        &workspace,
                        &name,
                        node,
                        v,
                        o.value_type.as_deref(),
                    )
                    .map_err(|e| format!("set {}: {}", node, e))?;
                    report(
                        o.json,
                        serde_json::json!({ "set": node, "value": v, "type": o.value_type.as_deref().unwrap_or("VARCHAR") }),
                        &format!(
                            "set {} = {} ({})",
                            node,
                            v,
                            o.value_type.as_deref().unwrap_or("VARCHAR")
                        ),
                    );
                }
                (None, Some(id)) => {
                    watermark::set_snapshot(&workspace, &name, node, id)
                        .map_err(|e| format!("set {}: {}", node, e))?;
                    report(
                        o.json,
                        serde_json::json!({ "set": node, "snapshot_id": id }),
                        &format!("set {} = snapshot {}", node, id),
                    );
                }
                (Some(_), Some(_)) => {
                    return Err("give --value or --snapshot, not both".into());
                }
                (None, None) => return Err("set needs --value or --snapshot".into()),
            }
            Ok(0)
        }
        "clear" => {
            let node = o
                .node
                .as_deref()
                .ok_or_else(|| "clear needs --node".to_string())?;
            watermark::clear(&workspace, &name, node)
                .map_err(|e| format!("clear {}: {}", node, e))?;
            report(
                o.json,
                serde_json::json!({ "cleared": node }),
                &format!("cleared {}", node),
            );
            Ok(0)
        }
        other => Err(format!(
            "unknown backfill verb {:?} - expected list, set or clear",
            other
        )),
    }
}

fn report(json: bool, payload: serde_json::Value, human: &str) {
    if json {
        println!("{}", serde_json::to_string_pretty(&payload).unwrap_or_default());
    } else {
        println!("{}", human);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(s: &[&str]) -> Vec<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn a_verb_and_its_flags_parse_in_any_order() {
        let o = parse(&argv(&["set", "--node", "n1", "--value", "2026-01-01", "--json"])).unwrap();
        assert_eq!(o.verb, "set");
        assert_eq!(o.node.as_deref(), Some("n1"));
        assert_eq!(o.value.as_deref(), Some("2026-01-01"));
        assert!(o.json);
        // Flags before the verb work too, because people type them that way.
        let o = parse(&argv(&["--json", "list"])).unwrap();
        assert_eq!(o.verb, "list");
        assert!(o.json);
    }

    #[test]
    fn a_flag_with_no_value_is_an_error_not_a_silent_default() {
        assert!(parse(&argv(&["set", "--node"])).is_err());
        assert!(parse(&argv(&["set", "--snapshot", "not-a-number"])).is_err());
        assert!(parse(&argv(&["list", "--nonsense"])).is_err());
    }

    /// The name decides which folder is read, so getting it wrong reports
    /// somebody else's state as this pipeline's.
    #[test]
    fn the_name_comes_from_the_pipeline_file_stem_unless_overridden() {
        let o = parse(&argv(&["list", "--pipeline", "/tmp/ws/daily-load.json"])).unwrap();
        let (ws, name) = resolve(&o).unwrap();
        assert_eq!(name, "daily-load");
        assert_eq!(ws, PathBuf::from("/tmp/ws"), "workspace defaults to the file's folder");

        let o = parse(&argv(&["list", "--pipeline", "/tmp/ws/daily-load.json", "--name", "other"]))
            .unwrap();
        assert_eq!(resolve(&o).unwrap().1, "other");
    }

    #[test]
    fn without_a_pipeline_or_name_it_says_so_rather_than_guessing() {
        let o = parse(&argv(&["list"])).unwrap();
        assert!(resolve(&o).is_err(), "guessing a name would report the wrong state");
    }
}
