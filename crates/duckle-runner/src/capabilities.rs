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
    let mut markdown = false;
    let mut workspace: Option<std::path::PathBuf> = None;
    let mut kind_filter: Option<String> = None;
    let mut it = std::env::args().skip(2);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--json" => json_out = true,
            "--markdown" => markdown = true,
            "--workspace" => workspace = it.next().map(std::path::PathBuf::from),
            "--kind" => kind_filter = it.next(),
            other => {
                eprintln!(
                    "duckle-runner capabilities: unknown argument {other}. \
                     Use --json, --markdown, --workspace DIR, or                      --kind source|sink|transform|quality|control|custom."
                );
                return ExitCode::from(2);
            }
        }
    }
    let mut caps = match &workspace {
        Some(ws) => all_in(ws),
        None => all(),
    };
    if markdown {
        // The matrices #313 asks for, from the registry, so the document cannot
        // disagree with the thing it documents.
        print!("{}", matrices(&caps));
        return ExitCode::from(0);
    }
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


/// The matrices #313 asks for, rendered from the registry.
///
/// Generated rather than written, because the issue's whole complaint is that a
/// second, independent list drifts. Everything here is a projection of the same
/// records the JSON carries; nothing is stated that the registry does not know.
fn matrices(caps: &[Capabilities]) -> String {
    let mut out = String::new();
    fn avail<'a>(caps: &'a [Capabilities], k: &'a str) -> impl Iterator<Item = &'a Capabilities> {
        caps.iter().filter(move |c| c.kind == k && c.availability == "available")
    }
    let yes = |b: bool| if b { "yes" } else { "-" };
    let list = |v: &[String]| match v.is_empty() {
        true => "-".to_string(),
        false => v.join(", "),
    };

    out.push_str("# Component capability matrix\n\n");
    out.push_str(
        "Generated by `duckle-runner capabilities --markdown`. Do not edit by hand: this file \
         is regenerated and diffed in CI, so an edit here is reverted and a component change \
         that is not exported fails the build.\n\n",
    );
    out.push_str(&format!(
        "{} components, {} available.\n",
        caps.len(),
        caps.iter().filter(|c| c.availability == "available").count()
    ));

    out.push_str("\n## Sources\n\nWhat a source can be asked to do.\n\n");
    out.push_str("| Component | Custom SQL | Incremental | Pushdown | Chunking | Dialect | Rejects |\n");
    out.push_str("|---|---|---|---|---|---|---|\n");
    for c in avail(caps, "source") {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} |\n",
            c.component,
            yes(c.custom_sql),
            yes(c.incremental),
            yes(c.pushdown),
            list(&c.chunking),
            c.dialect.as_deref().unwrap_or("-"),
            yes(c.reject_output),
        ));
    }

    out.push_str("\n## Sinks\n\nHow a sink writes, and what it does with rows it cannot.\n\n");
    out.push_str("| Component | Write modes | Rejects | Artifact I/O |\n|---|---|---|---|\n");
    for c in avail(caps, "sink") {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} |\n",
            c.component,
            list(&c.write_modes),
            yes(c.reject_output),
            yes(c.artifact_io),
        ));
    }

    out.push_str(
        "\n## Authentication\n\nComponents that take credentials. A saved connection keeps them \
         out of the pipeline file; an inline credential does not.\n\n",
    );
    out.push_str("| Component | Kind | Saved connection | Inline credentials |\n|---|---|---|---|\n");
    let mut auth: Vec<&Capabilities> = caps
        .iter()
        .filter(|c| c.availability == "available" && (c.credentials || c.connection_ref))
        .collect();
    auth.sort_by(|a, b| a.component.cmp(&b.component));
    for c in auth {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} |\n",
            c.component,
            c.kind,
            yes(c.connection_ref),
            yes(c.credentials)
        ));
    }

    out.push_str(
        "\n## Runtime dependencies\n\nDuckDB extensions a component's prelude loads. These are \
         what an offline bundle must embed, which is why the bundler asks the engine for them \
         rather than keeping its own list.\n\n",
    );
    out.push_str("| Component | Extensions |\n|---|---|\n");
    let mut ext: Vec<&Capabilities> = caps
        .iter()
        .filter(|c| c.availability == "available" && !c.extensions.is_empty())
        .collect();
    ext.sort_by(|a, b| a.component.cmp(&b.component));
    for c in ext {
        out.push_str(&format!("| `{}` | {} |\n", c.component, list(&c.extensions)));
    }

    out.push_str(
        "\n## Execution side effects\n\nWhat running a component does beyond producing rows. \
         Each is read from the engine function that already decides it, so this table cannot \
         disagree with what the policy enforces. Only components with a side effect are \
         listed; the rest have none.\n\n",
    );
    out.push_str("| Component | Advances durable state | Runs a process |\n|---|---|---|\n");
    let mut effects: Vec<&Capabilities> = caps
        .iter()
        .filter(|c| c.availability == "available" && (c.advances_state || c.executes_process))
        .collect();
    effects.sort_by(|a, b| a.component.cmp(&b.component));
    for c in effects {
        out.push_str(&format!(
            "| `{}` | {} | {} |\n",
            c.component,
            yes(c.advances_state),
            yes(c.executes_process)
        ));
    }
    out
}
