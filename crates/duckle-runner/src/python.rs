//! `duckle-runner python` - prepare and inspect the workspace's Python
//! environment (#246).
//!
//! `prepare` is a separate command on purpose. Resolving dependencies during a
//! pipeline run would turn a missing package into a mid-run download, which is
//! what an air-gapped box, a scheduled job and a signed run all cannot have.
//! Preparing is a provisioning step: run it once, in CI or at deploy time, and
//! the run itself fetches nothing.

use std::path::{Path, PathBuf};

pub const PYTHON_USAGE: &str = "\
duckle-runner python <check|prepare> [flags]

Prepare and inspect the workspace's Python environment.

  check                      what is installed, and whether it matches uv.lock
  prepare                    build .venv from pyproject.toml + uv.lock (needs uv)

  --workspace DIR   the workspace (default: .)
  --json            machine-readable output

check exits 1 when the environment is not the one the lock describes, so CI can
gate on it. A package the lock names but that is not installed is reported and
does not fail: a lock resolves for every platform, so something absent here may
simply not apply here.

With no uv.lock in the workspace, there is nothing to check against and check
just reports what is installed.
";

struct Opts {
    verb: String,
    workspace: PathBuf,
    json: bool,
}

fn parse(argv: &[String]) -> Result<Opts, String> {
    let mut o = Opts {
        verb: String::new(),
        workspace: PathBuf::from("."),
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
        println!("{}", PYTHON_USAGE);
        return Ok(0);
    }
    let o = parse(&argv)?;
    let ws: &Path = &o.workspace;
    use duckle_duckdb_engine::pyenv;

    match o.verb.as_str() {
        "check" => {
            let env = pyenv::inspect(ws);
            let drifts = pyenv::drift(&env);
            let failing = drifts.iter().filter(|d| d.is_failure()).count();
            if o.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "workspace": ws.display().to_string(),
                        "environment": env,
                        "drift": drifts,
                        "matches_lock": failing == 0,
                    }))
                    .unwrap_or_default()
                );
                return Ok(if failing == 0 { 0 } else { 1 });
            }
            match env.interpreter.as_deref() {
                Some(p) => println!("interpreter      : {p}"),
                None => println!("interpreter      : none in this workspace (.venv not found)"),
            }
            println!(
                "python           : {}",
                env.python_version.as_deref().unwrap_or("unknown")
            );
            println!("platform         : {}", env.platform);
            println!(
                "uv.lock          : {}",
                env.lock_sha256.as_deref().unwrap_or("none")
            );
            println!(
                "environment hash : {}",
                env.environment_hash.as_deref().unwrap_or("none")
            );
            println!("installed        : {} package(s)", env.installed.len());
            if env.lock_sha256.is_some() {
                println!("locked           : {} package(s)", env.locked.len());
            }
            if drifts.is_empty() {
                if env.lock_sha256.is_some() {
                    println!("\nthe environment matches uv.lock");
                }
                return Ok(0);
            }
            println!("\ndifferences from uv.lock:");
            println!("{}", pyenv::describe(&drifts));
            if failing == 0 {
                // Only absent packages, which a per-platform resolution
                // explains. Worth saying, not worth failing on.
                println!("\nnothing installed contradicts the lock");
                return Ok(0);
            }
            println!(
                "\n{failing} package(s) contradict the lock. Run `duckle-runner python prepare` \
                 to rebuild the environment from it."
            );
            Ok(1)
        }
        "prepare" => {
            if !ws.join("uv.lock").is_file() {
                return Err(format!(
                    "no uv.lock in {}. Create one with `uv lock` and commit it - without a lock \
                     there is nothing to reproduce.",
                    ws.display()
                ));
            }
            // `uv sync` builds .venv to match the lock exactly, including
            // removing what the lock does not name. That last part is the
            // reason to shell out to uv rather than to pip: an environment
            // with an extra package in it is not the locked environment.
            let out = std::process::Command::new("uv")
                .arg("sync")
                .arg("--frozen")
                .current_dir(ws)
                .output()
                .map_err(|e| {
                    format!(
                        "cannot run uv: {e}. Install it from https://docs.astral.sh/uv/ - Duckle \
                         does not install dependencies during a pipeline run, so this is the step \
                         that has to happen first."
                    )
                })?;
            let stderr = String::from_utf8_lossy(&out.stderr);
            if !out.status.success() {
                return Err(format!("uv sync --frozen failed:\n{}", stderr.trim()));
            }
            print!("{}", stderr);
            let env = pyenv::inspect(ws);
            println!(
                "prepared: {} package(s), environment hash {}",
                env.installed.len(),
                env.environment_hash.as_deref().unwrap_or("none")
            );
            Ok(0)
        }
        other => Err(format!("unknown python verb {other:?} - expected check or prepare")),
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
        let o = parse(&argv(&["check", "--workspace", "ws", "--json"])).unwrap();
        assert_eq!(o.verb, "check");
        assert_eq!(o.workspace, PathBuf::from("ws"));
        assert!(o.json);
        let o = parse(&argv(&["--json", "prepare"])).unwrap();
        assert_eq!(o.verb, "prepare");
    }

    #[test]
    fn a_flag_with_no_value_is_an_error_not_a_silent_default() {
        assert!(parse(&argv(&["check", "--workspace"])).is_err());
        assert!(parse(&argv(&["check", "--nonsense"])).is_err());
    }
}
