//! `duckle-runner xsd` - inspect and explicitly accept parser contracts (#315).

use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "\
duckle-runner xsd <list|accept> [flags]

Inspect or explicitly accept resolved XSD parser contracts.

  list                         show accepted contracts
  accept --uri URI --fingerprint SHA256
                               accept exactly this observed contract

  --workspace DIR              workspace (default: .)
  --reason TEXT                why the contract was accepted
  --json                       machine-readable output

`accept` records the actor, time, old and new fingerprints, and reason in the
workspace audit log. The fingerprint must be the exact SHA-256 reported by the
failed run; acceptance never happens implicitly during execution.
";

struct Args {
    verb: String,
    workspace: PathBuf,
    uri: Option<String>,
    fingerprint: Option<String>,
    reason: Option<String>,
    json: bool,
}

fn next_value(
    it: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<String, String> {
    it.next().ok_or_else(|| format!("{name} needs a value"))
}

fn parse(argv: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut args = Args {
        verb: String::new(),
        workspace: PathBuf::from("."),
        uri: None,
        fingerprint: None,
        reason: None,
        json: false,
    };
    let mut it = argv;
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--workspace" => args.workspace = next_value(&mut it, "--workspace")?.into(),
            "--uri" => args.uri = Some(next_value(&mut it, "--uri")?),
            "--fingerprint" => args.fingerprint = Some(next_value(&mut it, "--fingerprint")?),
            "--reason" => args.reason = Some(next_value(&mut it, "--reason")?),
            "--json" => args.json = true,
            "-h" | "--help" => return Err(String::new()),
            other if other.starts_with('-') => return Err(format!("unknown flag {other}")),
            other if args.verb.is_empty() => args.verb = other.to_string(),
            other => return Err(format!("unexpected argument {other}")),
        }
    }
    Ok(args)
}

fn valid_fingerprint(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

pub fn run() -> ExitCode {
    let args = match parse(std::env::args().skip(2)) {
        Ok(args) => args,
        Err(e) if e.is_empty() => {
            println!("{USAGE}");
            return ExitCode::from(0);
        }
        Err(e) => {
            eprintln!("duckle-runner xsd: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let path = duckle_duckdb_engine::xsd_contract::path(&args.workspace);
    match args.verb.as_str() {
        "list" => match duckle_duckdb_engine::xsd_contract::list(&path) {
            Ok(entries) => {
                if args.json {
                    let rows: Vec<_> = entries
                        .iter()
                        .map(|(uri, fingerprint)| {
                            serde_json::json!({
                                "uri": uri,
                                "fingerprint": fingerprint
                            })
                        })
                        .collect();
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&rows).unwrap_or_default()
                    );
                } else if entries.is_empty() {
                    println!("no accepted XSD contracts under {}", path.display());
                } else {
                    for (uri, fingerprint) in entries {
                        println!("{uri} {fingerprint}");
                    }
                }
                ExitCode::from(0)
            }
            Err(e) => {
                eprintln!("duckle-runner xsd list: {e}");
                ExitCode::from(2)
            }
        },
        "accept" => {
            let Some(uri) = args
                .uri
                .as_deref()
                .filter(|v| !v.is_empty() && !v.chars().any(char::is_whitespace))
            else {
                eprintln!(
                    "duckle-runner xsd accept: --uri is required and may not contain whitespace"
                );
                return ExitCode::from(2);
            };
            let Some(fingerprint) = args.fingerprint.as_deref().filter(|v| valid_fingerprint(v))
            else {
                eprintln!("duckle-runner xsd accept: --fingerprint must be a 64-character SHA-256 hex digest");
                return ExitCode::from(2);
            };
            let reason = args.reason.as_deref().unwrap_or("no reason supplied");
            if reason.bytes().any(|b| b == b'\r' || b == b'\n') {
                eprintln!("duckle-runner xsd accept: --reason may not contain newlines");
                return ExitCode::from(2);
            }
            let previous = match duckle_duckdb_engine::xsd_contract::accept(&path, uri, fingerprint)
            {
                Ok(previous) => previous,
                Err(e) => {
                    eprintln!("duckle-runner xsd accept: {e}");
                    return ExitCode::from(2);
                }
            };
            duckle_duckdb_engine::audit::note(
                &args.workspace,
                "xsd.contract.accept",
                uri,
                Some(format!(
                    "{} -> {}; reason: {}",
                    previous.as_deref().unwrap_or("none"),
                    fingerprint,
                    reason
                )),
            );
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "uri": uri,
                        "previous": previous,
                        "accepted": fingerprint,
                        "reason": reason
                    }))
                    .unwrap_or_default()
                );
            } else {
                println!("{uri}: accepted {fingerprint}");
                println!(
                    "Recorded the acceptance in {}",
                    duckle_duckdb_engine::audit::audit_path(&args.workspace).display()
                );
            }
            ExitCode::from(0)
        }
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprints_are_exact_sha256_values() {
        assert!(valid_fingerprint(&"a".repeat(64)));
        assert!(!valid_fingerprint("short"));
        assert!(!valid_fingerprint(&"g".repeat(64)));
    }

    #[test]
    fn flags_can_follow_the_verb_in_any_order() {
        let args = parse(
            [
                "accept",
                "--fingerprint",
                &"a".repeat(64),
                "--uri",
                "schema.xsd",
            ]
            .into_iter()
            .map(String::from),
        )
        .unwrap();
        assert_eq!(args.verb, "accept");
        assert_eq!(args.uri.as_deref(), Some("schema.xsd"));
    }
}
