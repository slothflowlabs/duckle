//! #313: the `capabilities` command.
//!
//! Printing only. The registry itself lives in the engine, so the CLI, the MCP
//! server and anything else that asks get one answer rather than two: a second
//! derivation is a second opinion about what a component supports, and the one
//! that drifts is always the one nobody is looking at.

use duckle_duckdb_engine::capabilities::{all, all_in, Capabilities};
use std::process::ExitCode;

pub fn run() -> ExitCode {
    let mut json_out = false;
    let mut kind_filter: Option<String> = None;
    let mut it = std::env::args().skip(2);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--json" => json_out = true,
            "--kind" => kind_filter = it.next(),
            other => {
                eprintln!(
                    "duckle-runner capabilities: unknown argument {other}. \
                     Use --json and --kind source|sink|transform|quality|control|custom."
                );
                return ExitCode::from(2);
            }
        }
    }
    let mut caps = all();
    if let Some(k) = &kind_filter {
        caps.retain(|c| &c.kind == k);
    }
    if caps.is_empty() {
        eprintln!("duckle-runner capabilities: no components matched");
        return ExitCode::from(2);
    }
    if json_out {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schemaVersion": crate::report::SCHEMA_VERSION,
                "command": "capabilities",
                "count": caps.len(),
                "components": caps,
            }))
            .unwrap_or_default()
        );
    } else {
        println!(
            "{:<26} {:<10} {:<5} {:<5} {:<5} {:<5} {}",
            "component", "kind", "sql", "incr", "push", "rej", "write modes"
        );
        for c in &caps {
            let y = |b: bool| if b { "yes" } else { "-" };
            println!(
                "{:<26} {:<10} {:<5} {:<5} {:<5} {:<5} {}",
                c.component,
                c.kind,
                y(c.custom_sql),
                y(c.incremental),
                y(c.pushdown),
                y(c.reject_output),
                c.write_modes.join("/")
            );
        }
        println!("\n{} component(s)", caps.len());
    }
    ExitCode::from(0)
}

#[cfg(test)]
mod readme_counts {
    use super::*;

    /// The README is a second list, so it must not be an independent one.
    ///
    /// #313's premise, checked rather than argued: "hand-maintained
    /// README/roadmap lists can drift from the code". They had. The README
    /// claimed 113 sources, 130 transforms and 73 sinks against a catalog with
    /// 119, 144 and 72 - two counts stale upward and one stale DOWNWARD, so it
    /// was not simply lagging behind additions, it was unmaintained.
    ///
    /// The prose and the groupings stay hand-written, because they are
    /// editorial. Only the numbers are derived, because only the numbers have a
    /// right answer.
    const README: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../README.md"));

    fn available(kind: &str) -> usize {
        all().iter().filter(|c| c.kind == kind && c.availability == "available").count()
    }

    /// `**113 sources available today.**` -> 113
    fn claimed(noun: &str) -> usize {
        let needle = format!(" {noun} available today");
        let line = README
            .lines()
            .find(|l| l.contains(&needle))
            .unwrap_or_else(|| panic!("the README no longer states how many {noun} are available"));
        line.chars()
            .skip_while(|c| !c.is_ascii_digit())
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse()
            .unwrap_or_else(|_| panic!("no number in {line:?}"))
    }

    #[test]
    fn the_readme_counts_match_the_catalog() {
        for (noun, kind) in [("sources", "source"), ("transforms", "transform"), ("sinks", "sink")] {
            assert_eq!(
                claimed(noun),
                available(kind),
                "the README says {} {noun} are available and the catalog has {}. Update the \
                 README, in the same change as whatever added or removed the component.",
                claimed(noun),
                available(kind)
            );
        }
    }
}

