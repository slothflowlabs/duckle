//! The swarm: any number of agents, in six stages, turn a goal into a tested
//! pipeline.
//!
//! `duckle-mcp swarm` reads one request line on stdin and writes one event per
//! line on stdout until the run ends. Every agent works only through this
//! server's own tools, called in this process exactly as an MCP client's call
//! would be, so the swarm can do nothing an agent connected over MCP could not.
//! The desktop app and the web editor start this process and relay its events
//! to the swarm view. A "stop" line on stdin, or stdin closing, stops it at the
//! next step.
//!
//! The stages run in order: Discover reads the data, Design plans the pipeline
//! and the quality gates that will prove it, Build writes it, Test runs it,
//! Repair fixes what the tests found (and Test runs again, up to two rounds),
//! and Review reports to the person. A stage holds any number of agents.
//! Discover and Review only read, so their agents work side by side; in the
//! other stages they take turns, each starting from the notes of every agent
//! before it.

use crate::llm::{self, Delta, Model, Stop, ToolOutput, ToolSpec};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub const DEFAULT_CLAUDE: &str = "claude-opus-5-5";
/// Model calls one agent may make before it must hand off.
const MAX_TURNS: usize = 16;
/// Empty turns (no text, no tool call) in a row an agent is asked to redo before
/// its work ends anyway.
const SILENT_RETRIES: usize = 2;
/// Turns in a row cut off at the output limit an agent is asked to redo. Past
/// that the model is looping, not working, and the agent fails.
const CUT_RETRIES: usize = 2;
/// Times the Repair stage may run before the run is reported as failing.
const FIX_ROUNDS: usize = 2;
/// Agents of a side-by-side stage working at the same moment. A stage may
/// hold any number; the rest start as the ones before them finish.
const AT_ONCE: usize = 6;
/// The longest tool answer an agent reads. A catalog listing can run far
/// past this; the agent is told to narrow it.
const RESULT_LIMIT: usize = 24_000;
const HAND_OFF: &str = "hand_off";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stage {
    Discover,
    Design,
    Build,
    Test,
    Repair,
    Review,
}

impl Stage {
    pub fn id(self) -> &'static str {
        match self {
            Stage::Discover => "discover",
            Stage::Design => "design",
            Stage::Build => "build",
            Stage::Test => "test",
            Stage::Repair => "repair",
            Stage::Review => "review",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Stage::Discover => "Discover",
            Stage::Design => "Design",
            Stage::Build => "Build",
            Stage::Test => "Test",
            Stage::Repair => "Repair",
            Stage::Review => "Review",
        }
    }

    /// The Duckle tools this stage's agents may call, besides hand_off.
    fn tools(self) -> &'static [&'static str] {
        match self {
            Stage::Discover => &[
                "list_workspace_files",
                "inspect_source",
                "list_connections",
                "list_pipelines",
                "read_pipeline",
            ],
            Stage::Design => &[
                "list_components",
                "component_capabilities",
                "get_component_schema",
                "inspect_source",
            ],
            Stage::Build => &[
                "get_component_schema",
                "list_components",
                "create_pipeline",
                "update_pipeline",
                "validate_pipeline",
                "check_node_sql",
                "read_pipeline",
            ],
            Stage::Test => &[
                "run_pipeline",
                "inspect_source",
                "read_pipeline",
                "read_run_logs",
                "verify_pipeline",
            ],
            Stage::Repair => &[
                "read_pipeline",
                "get_component_schema",
                "update_pipeline",
                "validate_pipeline",
                "check_node_sql",
                "run_pipeline",
                "inspect_source",
                "read_run_logs",
            ],
            Stage::Review => &[
                "read_pipeline",
                "verify_pipeline",
                "pipeline_lineage",
                "trust_report",
                "inspect_source",
            ],
        }
    }

    /// Its agents give a pass or fail verdict.
    fn judges(self) -> bool {
        matches!(self, Stage::Test | Stage::Review)
    }

    /// Its agents only read, so they can work at the same time.
    fn side_by_side(self) -> bool {
        matches!(self, Stage::Discover | Stage::Review)
    }

    fn duty(self) -> &'static str {
        match self {
            Stage::Discover => {
                "Find the data the goal needs and describe it exactly. Start with \
                 list_workspace_files, then inspect each file the goal could use with \
                 inspect_source. For every file you report, give its ${workspace} path, its \
                 format, each column with its type, a sample value or two, and what a builder \
                 must watch for: nulls, date formats, numbers stored as text, duplicate keys. If \
                 the data the goal names is not there, say so plainly."
            }
            Stage::Design => {
                "Design the pipeline from the notes before you. Find components with \
                 list_components and read the schema of every one you use with \
                 get_component_schema; use only the property keys and ports it lists. Then hand \
                 off:\n\
                 1. The nodes in flow order, each with an id, its component id, a short label, \
                 and every property with its exact key and value.\n\
                 2. The edges, naming the port each one leaves from and arrives at.\n\
                 3. Two to four quality gates built into the pipeline, so the proof runs every \
                 time it does: for example qa.unique or qa.notnull on the output key, qa.expect \
                 or qa.assert for a rule the result must meet, qa.reconcile to prove a total \
                 matches the input. Set each to fail the run when it does not hold.\n\
                 Do not write files."
            }
            Stage::Build => {
                "Build the design with create_pipeline, named for the goal in a few words. Lay \
                 it out left to right in flow order: x = 80, 380, 680 and so on, y = 200; a \
                 second input sits above at y = 40, and a gate that branches off sits below at \
                 y = 360. When create_pipeline reports an error, correct the design and call it \
                 again. If an agent before you already wrote the pipeline, change it with \
                 update_pipeline instead of writing it again. Finish with validate_pipeline \
                 passing, then hand off what you built and anything you changed from the \
                 design, and why."
            }
            Stage::Test => {
                "Prove the pipeline works. Run it with run_pipeline. If the run fails, report \
                 the failing node and its error. If it succeeds, check every quality gate's \
                 result and the row counts, then read the output file it wrote with \
                 inspect_source and confirm it holds what the goal asked for. Hand off with \
                 verdict \"pass\" only if the run succeeded, every gate held and the output is \
                 right. Otherwise give verdict \"fail\" and list each failure: the check, what \
                 was expected, what came back, and the node most likely at fault."
            }
            Stage::Repair => {
                "The tests reported failures. Read the pipeline, find each cause, and fix it \
                 with the smallest update_pipeline patch (nodes merge by id, so one property can \
                 change alone). Use check_node_sql on SQL, and run_pipeline with target to look \
                 at one node's rows. Do not weaken or remove a quality gate to make it pass: fix \
                 the data flow it guards. Hand off exactly what you changed and why."
            }
            Stage::Review => {
                "Review the finished pipeline for the person who set the goal. Use \
                 verify_pipeline and pipeline_lineage, and trust_report for anything that looks \
                 like personal data. Hand off a short verdict addressed to them: what the \
                 pipeline does, where its output is, the evidence that it works (rows, gates \
                 that held), and any risk worth knowing. Verdict \"pass\" if it meets the goal."
            }
        }
    }
}

/// One member of the crew.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSpec {
    pub id: String,
    pub name: String,
    pub stage: Stage,
    /// Instructions of its own, on top of its stage's duty.
    #[serde(default)]
    pub brief: String,
}

/// The crew when the request names none: one agent per stage.
pub fn default_roster() -> Vec<AgentSpec> {
    [
        ("scout", "Scout", Stage::Discover),
        ("architect", "Architect", Stage::Design),
        ("builder", "Builder", Stage::Build),
        ("tester", "Tester", Stage::Test),
        ("fixer", "Fixer", Stage::Repair),
        ("reviewer", "Reviewer", Stage::Review),
    ]
    .into_iter()
    .map(|(id, name, stage)| AgentSpec {
        id: id.into(),
        name: name.into(),
        stage,
        brief: String::new(),
    })
    .collect()
}

/// The crew a request names, tidied, or why it cannot run. Any number of
/// agents, but one must be in the Build stage: nobody else writes the
/// pipeline.
pub fn checked_roster(agents: &[AgentSpec]) -> Result<Vec<AgentSpec>, String> {
    if agents.is_empty() {
        return Ok(default_roster());
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(agents.len());
    for a in agents {
        let id = a.id.trim();
        if id.is_empty()
            || id.len() > 40
            || !id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(format!(
                "Agent id '{}' must be 1 to 40 letters, digits, - or _.",
                a.id
            ));
        }
        if !seen.insert(id.to_string()) {
            return Err(format!("Two agents share the id '{id}'."));
        }
        let name = a.name.trim();
        if name.is_empty() {
            return Err(format!("Agent '{id}' has no name."));
        }
        out.push(AgentSpec {
            id: id.to_string(),
            name: name.chars().take(60).collect(),
            stage: a.stage,
            brief: a.brief.trim().chars().take(4000).collect(),
        });
    }
    if !out.iter().any(|a| a.stage == Stage::Build) {
        return Err(
            "The crew needs an agent in the Build stage: nobody else writes the pipeline.".into(),
        );
    }
    Ok(out)
}

/// What the swarm view sends to start a run.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub goal: String,
    pub workspace: String,
    #[serde(default)]
    pub duckdb: Option<String>,
    pub provider: Provider,
    /// The crew. Empty: one agent per stage.
    #[serde(default)]
    pub agents: Vec<AgentSpec>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Provider {
    /// "anthropic-account", "anthropic-key" or "openai-compatible".
    pub kind: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    /// A key typed into the swarm view, for this run only.
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
}

/// Keys the environment may hold, each tied to the one endpoint it is for.
///
/// A key from the environment is only ever sent to its own provider. A
/// request naming any other endpoint must bring its own key: otherwise anyone
/// able to start a swarm on a shared server could send its keys anywhere.
const ENV_KEYS: &[(&str, &str)] = &[
    ("OPENAI_API_KEY", "api.openai.com"),
    ("HF_TOKEN", "router.huggingface.co"),
    ("OPENROUTER_API_KEY", "openrouter.ai"),
    ("GROQ_API_KEY", "api.groq.com"),
    ("MISTRAL_API_KEY", "api.mistral.ai"),
    ("DEEPSEEK_API_KEY", "api.deepseek.com"),
    ("GEMINI_API_KEY", "generativelanguage.googleapis.com"),
    ("TOGETHER_API_KEY", "api.together.xyz"),
    ("XAI_API_KEY", "api.x.ai"),
];

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn clean(v: &Option<String>) -> Option<String> {
    v.as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// The key from the environment that may go to this endpoint, if any.
fn env_key_for(base_url: &str) -> Option<String> {
    let host = llm::host_of(base_url);
    ENV_KEYS
        .iter()
        .find(|(_, h)| *h == host)
        .and_then(|(name, _)| env_value(name))
}

/// A model for one agent's conversation, and how to name it in the swarm view.
pub fn model_for(p: &Provider) -> Result<(Box<dyn Model + Send>, String), String> {
    let typed = clean(&p.api_key);
    let model = clean(&p.model);
    let base = clean(&p.base_url).map(|b| b.trim_end_matches('/').to_string());
    match p.kind.as_str() {
        "anthropic-account" | "anthropic-key" => {
            let account = p.kind == "anthropic-account";
            let model = model.unwrap_or_else(|| DEFAULT_CLAUDE.to_string());
            // A credential the person did not type here (their sign-in, a key
            // from the environment) goes only to Anthropic, or to a stub on
            // this machine.
            let base_url = match base {
                None => llm::ANTHROPIC_BASE.to_string(),
                Some(b) if llm::is_loopback(&b) || (typed.is_some() && !account) => b,
                Some(_) => {
                    return Err(
                        "A custom Anthropic endpoint needs an API key typed in for it.".into(),
                    )
                }
            };
            let auth = if account {
                llm::AnthropicAuth::Account(llm::find_ant().ok_or(
                    "To use your Anthropic account, install Anthropic's CLI (ant) and run `ant auth login`.",
                )?)
            } else {
                llm::AnthropicAuth::Key(typed.or_else(|| env_value("ANTHROPIC_API_KEY")).ok_or(
                    "No Anthropic API key: paste one, or set ANTHROPIC_API_KEY where Duckle runs.",
                )?)
            };
            let label = format!(
                "{model} \u{00b7} {}",
                if account {
                    "your Anthropic account"
                } else {
                    "Anthropic API key"
                }
            );
            Ok((
                Box::new(llm::Anthropic {
                    base_url,
                    model,
                    auth,
                    effort: clean(&p.effort),
                }),
                label,
            ))
        }
        "openai-compatible" => {
            let base_url =
                base.ok_or("Give the endpoint's base URL, such as https://api.openai.com/v1.")?;
            let model = model.ok_or("Name the model to use.")?;
            let api_key = typed.or_else(|| env_key_for(&base_url));
            let label = format!("{model} \u{00b7} {}", llm::host_of(&base_url));
            Ok((
                Box::new(llm::OpenAiCompatible {
                    base_url,
                    model,
                    api_key,
                    usage_refused: false,
                    cap_refused: false,
                }),
                label,
            ))
        }
        other => Err(format!("Unknown provider '{other}'.")),
    }
}

/// What the swarm view needs to offer each way of reaching a model.
pub fn status() -> Value {
    let ant = llm::find_ant();
    let signed_in = ant.as_deref().is_some_and(llm::ant_signed_in);
    let mut keys = Map::new();
    keys.insert(
        "ANTHROPIC_API_KEY".into(),
        json!(env_value("ANTHROPIC_API_KEY").is_some()),
    );
    for (name, _) in ENV_KEYS {
        keys.insert((*name).into(), json!(env_value(name).is_some()));
    }
    json!({
        "anthropicAccount": { "installed": ant.is_some(), "signedIn": signed_in },
        "keys": keys,
        "defaultModel": DEFAULT_CLAUDE,
    })
}

/// A tool call: (answer text, is an error). Agents working at once share it.
pub type ToolFn<'a> = dyn Fn(&str, Value) -> (String, bool) + Sync + 'a;
/// Where events go. Agents working at once share it.
pub type Emit<'a> = dyn Fn(Value) + Sync + 'a;
/// A fresh conversation with the model: one per agent.
pub type MakeModel<'a> = dyn Fn() -> Result<Box<dyn Model + Send>, String> + Sync + 'a;

/// Call one of this server's tools in-process, as tools/call does.
pub fn call_tool(name: &str, args: Value) -> (String, bool) {
    match crate::tools::call_tool(json!({ "name": name, "arguments": args })) {
        Ok(v) => (
            v.pointer("/content/0/text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            v.get("isError").and_then(Value::as_bool).unwrap_or(false),
        ),
        Err((_, message)) => (message, true),
    }
}

/// Everything one run shares between its agents.
struct Ctx<'a> {
    goal: &'a str,
    workspace: PathBuf,
    duckdb: Option<String>,
    pipeline_id: String,
    pipeline_file: PathBuf,
    catalog: Vec<Value>,
    roster: Vec<AgentSpec>,
    tools: &'a ToolFn<'a>,
    emit: &'a Emit<'a>,
    make_model: &'a MakeModel<'a>,
    cancel: &'a AtomicBool,
    /// Tools change process-wide settings (run_pipeline puts the workspace
    /// and the DuckDB binary in the environment), so one runs at a time.
    tool_lock: Mutex<()>,
    tool_calls: AtomicU64,
    input_tokens: AtomicU64,
    output_tokens: AtomicU64,
}

impl Ctx<'_> {
    fn crew(&self, stage: Stage) -> Vec<&AgentSpec> {
        self.roster.iter().filter(|a| a.stage == stage).collect()
    }
}

#[derive(Clone)]
struct Note {
    name: String,
    stage: Stage,
    summary: String,
}

struct AgentOut {
    summary: String,
    verdict: Option<bool>,
}

/// An id for the pipeline the swarm builds, from the goal's words, that no
/// pipeline in the workspace has yet: the swarm never writes over one it did
/// not make.
pub fn pipeline_id_for(goal: &str, pipelines_dir: &std::path::Path) -> String {
    const SKIP: &[&str] = &[
        "a", "an", "the", "and", "or", "to", "of", "in", "into", "from", "for", "with", "by", "on",
        "at", "then", "that", "it", "its", "my", "our", "each", "per", "as", "is", "be", "all",
        "me", "i", "we", "please", "build", "make", "create", "pipeline",
    ];
    let words: Vec<String> = goal
        .split(|c: char| !c.is_ascii_alphanumeric())
        .map(|w| w.to_ascii_lowercase())
        .filter(|w| !w.is_empty() && !SKIP.contains(&w.as_str()))
        .take(4)
        .collect();
    let base: String = if words.is_empty() {
        "swarm_pipeline".to_string()
    } else {
        format!("swarm_{}", words.join("_"))
            .chars()
            .take(48)
            .collect()
    };
    let free = |id: &str| !pipelines_dir.join(format!("{id}.json")).exists();
    if free(&base) {
        return base;
    }
    (2..)
        .map(|n| format!("{base}_{n}"))
        .find(|id| free(id))
        .unwrap_or(base)
}

/// Run the whole swarm. Returns the `finished` event, also emitted.
pub fn run(
    req: &Request,
    make_model: &MakeModel,
    label: &str,
    tools: &ToolFn,
    emit: &Emit,
    cancel: &AtomicBool,
) -> Value {
    let started = Instant::now();
    let refuse = |message: String| {
        emit(json!({ "type": "error", "message": message }));
        let done = json!({ "type": "finished", "ok": false, "summary": message });
        emit(done.clone());
        done
    };
    let workspace = PathBuf::from(req.workspace.trim());
    if req.goal.trim().is_empty() {
        return refuse("Describe what the pipeline should do.".into());
    }
    if !workspace.is_dir() {
        return refuse(format!("No workspace folder at {}.", workspace.display()));
    }
    let roster = match checked_roster(&req.agents) {
        Ok(r) => r,
        Err(e) => return refuse(e),
    };
    let pipelines = workspace.join("pipelines");
    let pipeline_id = pipeline_id_for(&req.goal, &pipelines);
    let ctx = Ctx {
        goal: req.goal.trim(),
        pipeline_file: pipelines.join(format!("{pipeline_id}.json")),
        workspace,
        duckdb: clean(&req.duckdb),
        pipeline_id,
        catalog: crate::tools::list_tools()
            .as_array()
            .cloned()
            .unwrap_or_default(),
        roster,
        tools,
        emit,
        make_model,
        cancel,
        tool_lock: Mutex::new(()),
        tool_calls: AtomicU64::new(0),
        input_tokens: AtomicU64::new(0),
        output_tokens: AtomicU64::new(0),
    };
    emit(json!({
        "type": "started",
        "goal": ctx.goal,
        "pipelineId": ctx.pipeline_id,
        "model": label,
        "agents": ctx.roster.iter().map(|a| json!({ "id": a.id, "name": a.name, "stage": a.stage.id() })).collect::<Vec<_>>(),
    }));

    let mut notes: Vec<Note> = Vec::new();
    let mut last: Vec<String> = Vec::new();
    let mut failure: Option<String> = None;

    for stage in [Stage::Discover, Stage::Design, Stage::Build] {
        let crew = ctx.crew(stage);
        if crew.is_empty() {
            continue;
        }
        if let Err(e) = run_stage(&ctx, stage, &crew, &mut notes, &mut last) {
            failure = Some(e);
            break;
        }
    }
    if failure.is_none() && !ctx.pipeline_file.is_file() {
        failure = Some("The Build stage finished without writing the pipeline.".into());
    }

    let testers = ctx.crew(Stage::Test);
    let fixers = ctx.crew(Stage::Repair);
    let mut tested = false;
    let mut test_summary = String::new();
    if failure.is_none() && !testers.is_empty() {
        let mut rounds = 0;
        loop {
            match run_stage(&ctx, Stage::Test, &testers, &mut notes, &mut last) {
                Ok(outs) => {
                    tested = outs.iter().all(|o| o.verdict == Some(true));
                    test_summary = outs.last().map(|o| o.summary.clone()).unwrap_or_default();
                }
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
            if tested || rounds == FIX_ROUNDS || fixers.is_empty() {
                break;
            }
            rounds += 1;
            if let Err(e) = run_stage(&ctx, Stage::Repair, &fixers, &mut notes, &mut last) {
                failure = Some(e);
                break;
            }
        }
    }

    let reviewers = ctx.crew(Stage::Review);
    let mut reviewed = true;
    let mut review_summary = String::new();
    if failure.is_none() && !reviewers.is_empty() {
        match run_stage(&ctx, Stage::Review, &reviewers, &mut notes, &mut last) {
            Ok(outs) => {
                reviewed = outs.iter().all(|o| o.verdict == Some(true));
                review_summary = if outs.len() == 1 {
                    outs[0].summary.clone()
                } else {
                    reviewers
                        .iter()
                        .zip(&outs)
                        .map(|(a, o)| format!("{}: {}", a.name, o.summary))
                        .collect::<Vec<_>>()
                        .join("\n\n")
                };
            }
            Err(e) => failure = Some(e),
        }
    }

    let ok = failure.is_none() && tested && reviewed;
    let summary = match &failure {
        Some(e) => e.clone(),
        None if !review_summary.is_empty() => review_summary,
        None if testers.is_empty() => "Built, but not tested: the crew has no Test agent.".into(),
        None => test_summary,
    };
    let done = json!({
        "type": "finished",
        "ok": ok,
        "tested": tested,
        "reviewed": failure.is_none() && reviewed && !reviewers.is_empty(),
        "stopped": failure.as_deref() == Some(llm::CANCELLED),
        "summary": summary,
        "pipelineId": ctx.pipeline_id,
        "pipelineWritten": ctx.pipeline_file.is_file(),
        "agents": ctx.roster.len(),
        "durationMs": started.elapsed().as_millis() as u64,
        "toolCalls": ctx.tool_calls.load(Ordering::Relaxed),
        "inputTokens": ctx.input_tokens.load(Ordering::Relaxed),
        "outputTokens": ctx.output_tokens.load(Ordering::Relaxed),
    });
    emit(done.clone());
    done
}

/// One stage: its agents side by side or in turn, their results in crew
/// order. Every agent's notes join `notes`; `last` becomes the agents a
/// hand-off to the next stage leaves from.
fn run_stage(
    ctx: &Ctx,
    stage: Stage,
    crew: &[&AgentSpec],
    notes: &mut Vec<Note>,
    last: &mut Vec<String>,
) -> Result<Vec<AgentOut>, String> {
    (ctx.emit)(json!({
        "type": "stage",
        "stage": stage.id(),
        "agents": crew.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(),
    }));
    let handoff = |from: &str, to: &str| {
        (ctx.emit)(json!({ "type": "handoff", "from": from, "to": to }));
    };
    let mut outs = Vec::with_capacity(crew.len());
    if stage.side_by_side() && crew.len() > 1 {
        for a in crew {
            for from in last.iter() {
                handoff(from, &a.id);
            }
        }
        // Each starts from what was known before the stage, not from its
        // neighbours' notes: they work at the same time.
        let before = notes.clone();
        let mut results: Vec<Result<AgentOut, String>> = Vec::with_capacity(crew.len());
        for wave in crew.chunks(AT_ONCE) {
            if ctx.cancel.load(Ordering::Relaxed) {
                break;
            }
            let done: Vec<Result<AgentOut, String>> = std::thread::scope(|s| {
                let running: Vec<_> = wave
                    .iter()
                    .map(|a| {
                        let before = &before;
                        s.spawn(move || agent(ctx, a, before))
                    })
                    .collect();
                running
                    .into_iter()
                    .map(|h| {
                        h.join()
                            .unwrap_or_else(|_| Err("An agent stopped unexpectedly.".into()))
                    })
                    .collect()
            });
            results.extend(done);
        }
        if results.len() < crew.len() {
            return Err(llm::CANCELLED.to_string());
        }
        let mut first_error = None;
        for (a, result) in crew.iter().zip(results) {
            match result {
                Ok(o) => {
                    notes.push(Note {
                        name: a.name.clone(),
                        stage,
                        summary: o.summary.clone(),
                    });
                    outs.push(o);
                }
                Err(e) => {
                    first_error.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }
        *last = crew.iter().map(|a| a.id.clone()).collect();
    } else {
        for a in crew {
            for from in last.iter() {
                handoff(from, &a.id);
            }
            let o = agent(ctx, a, notes)?;
            notes.push(Note {
                name: a.name.clone(),
                stage,
                summary: o.summary.clone(),
            });
            *last = vec![a.id.clone()];
            outs.push(o);
        }
    }
    Ok(outs)
}

fn agent(ctx: &Ctx, a: &AgentSpec, notes: &[Note]) -> Result<AgentOut, String> {
    (ctx.emit)(json!({ "type": "agent", "agent": a.id, "state": "working" }));
    let out = if ctx.cancel.load(Ordering::Relaxed) {
        Err(llm::CANCELLED.to_string())
    } else {
        (ctx.make_model)().and_then(|mut model| work(ctx, a, notes, model.as_mut()))
    };
    match &out {
        Ok(o) => (ctx.emit)(json!({
            "type": "agent",
            "agent": a.id,
            "state": "done",
            "summary": o.summary,
            "verdict": o.verdict.map(|pass| if pass { "pass" } else { "fail" }),
        })),
        Err(e) => {
            (ctx.emit)(json!({ "type": "agent", "agent": a.id, "state": "failed", "summary": e }))
        }
    }
    out
}

fn work(
    ctx: &Ctx,
    a: &AgentSpec,
    notes: &[Note],
    model: &mut dyn Model,
) -> Result<AgentOut, String> {
    let system = system_prompt(a, &ctx.roster, &ctx.pipeline_id);
    let specs = specs_for(a.stage, &ctx.catalog);
    let mut history = vec![llm::user_message(&kickoff(ctx.goal, notes))];
    let mut last_text = String::new();
    let (mut silent, mut cut) = (0, 0);
    let emit = ctx.emit;
    for _ in 0..MAX_TURNS {
        if ctx.cancel.load(Ordering::Relaxed) {
            return Err(llm::CANCELLED.to_string());
        }
        let turn = {
            let mut on = |d: Delta| {
                let (kind, key, text) = match d {
                    Delta::Text(t) => ("text", "text", t),
                    Delta::Thinking(t) => ("thinking", "text", t),
                    Delta::Retry(why) => ("retry", "reason", why),
                };
                emit(json!({ "type": kind, "agent": a.id, key: text }));
            };
            model.turn(&system, &specs, &mut history, &mut on, ctx.cancel)?
        };
        let input = ctx
            .input_tokens
            .fetch_add(turn.input_tokens, Ordering::Relaxed)
            + turn.input_tokens;
        let output = ctx
            .output_tokens
            .fetch_add(turn.output_tokens, Ordering::Relaxed)
            + turn.output_tokens;
        emit(json!({
            "type": "usage",
            "agent": a.id,
            "inputTokens": input,
            "outputTokens": output,
        }));
        if !turn.text.trim().is_empty() {
            last_text = turn.text.trim().to_string();
        }
        match &turn.stop {
            Stop::Refused(category) if category.is_empty() => {
                return Err("The model declined to go on.".into())
            }
            Stop::Refused(category) => {
                return Err(format!("The model declined to go on ({category})."))
            }
            Stop::Truncated => {
                // Cut off: its tool calls may be incomplete, so none of them run.
                cut += 1;
                if cut > CUT_RETRIES {
                    return Err(format!(
                        "The model ran past its output limit {cut} turns in a row."
                    ));
                }
                history.pop();
                history.push(llm::user_message(
                    "Your last reply was cut off at the output limit. Take a smaller step: fewer \
                     nodes per call, or change one part with update_pipeline.",
                ));
                continue;
            }
            Stop::Done | Stop::Tools => {}
        }
        if turn.calls.is_empty() && turn.text.trim().is_empty() && silent < SILENT_RETRIES {
            // Nothing at all is not a hand-off: a local model can spend a turn on
            // reasoning alone, or write a call its server could not parse and dropped.
            // The dropped ones are long (a whole pipeline), so ask for smaller steps.
            silent += 1;
            history.pop();
            history.push(llm::user_message(
                "Your last reply had no text and no tool call, so nothing happened. If you \
                 meant to call a tool, the call did not arrive; long calls are the ones that \
                 get lost. Send it again as valid JSON, in smaller steps: a pipeline's first \
                 nodes with create_pipeline, the rest with update_pipeline. If your part is \
                 done, call hand_off.",
            ));
            continue;
        }
        if turn.calls.is_empty() {
            // Finished without handing off: what it said is its hand-off.
            return Ok(AgentOut {
                summary: last_text,
                verdict: None,
            });
        }
        (silent, cut) = (0, 0);
        let mut outputs = Vec::new();
        let mut handed: Option<AgentOut> = None;
        for call in &turn.calls {
            if call.name == HAND_OFF {
                let input = call.input.clone().unwrap_or_default();
                let summary = input
                    .get("summary")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| last_text.clone());
                let verdict = input
                    .get("verdict")
                    .and_then(Value::as_str)
                    .map(|v| v.trim().eq_ignore_ascii_case("pass"));
                handed = Some(AgentOut { summary, verdict });
                outputs.push(ToolOutput {
                    id: call.id.clone(),
                    text: "Handed off.".into(),
                    is_error: false,
                });
                continue;
            }
            let (text, is_error) = match &call.input {
                Err(why) => (
                    format!(
                        "INVALID_JSON: {why}. Send the call again with complete, valid arguments."
                    ),
                    true,
                ),
                Ok(_) if !a.stage.tools().contains(&call.name.as_str()) => (
                    format!(
                        "{} is not one of your tools in the {} stage.",
                        call.name,
                        a.stage.title()
                    ),
                    true,
                ),
                Ok(input) => {
                    if ctx.cancel.load(Ordering::Relaxed) {
                        return Err(llm::CANCELLED.to_string());
                    }
                    run_tool(ctx, a, &call.id, &call.name, input)
                }
            };
            outputs.push(ToolOutput {
                id: call.id.clone(),
                text: clip(&text),
                is_error,
            });
        }
        if let Some(out) = handed {
            if a.stage.judges() && out.verdict.is_none() {
                // A judge that gives no verdict has not passed anything.
                return Ok(AgentOut {
                    verdict: Some(false),
                    ..out
                });
            }
            return Ok(out);
        }
        model.tool_results(&mut history, &outputs);
    }
    Err(format!(
        "{} used all {MAX_TURNS} turns without finishing.",
        a.name
    ))
}

fn run_tool(ctx: &Ctx, a: &AgentSpec, id: &str, name: &str, input: &Value) -> (String, bool) {
    let args = scoped_args(name, input.clone(), ctx);
    ctx.tool_calls.fetch_add(1, Ordering::Relaxed);
    // What the agent asked for, not the paths filled in for it: the view (and
    // a screen recording of it) should not show the folder layout.
    (ctx.emit)(json!({ "type": "tool", "agent": a.id, "id": id, "name": name, "input": input }));
    let started = Instant::now();
    let (text, is_error) = {
        let _one_at_a_time = ctx.tool_lock.lock().unwrap_or_else(|p| p.into_inner());
        (ctx.tools)(name, args)
    };
    (ctx.emit)(json!({
        "type": "toolResult",
        "agent": a.id,
        "id": id,
        "name": name,
        "ok": !is_error,
        "ms": started.elapsed().as_millis() as u64,
        "preview": preview(&text),
    }));
    if !is_error && matches!(name, "create_pipeline" | "update_pipeline") {
        if let Some(doc) = read_doc(&ctx.pipeline_file) {
            let title = doc
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(&ctx.pipeline_id)
                .to_string();
            (ctx.emit)(
                json!({ "type": "pipeline", "id": ctx.pipeline_id, "name": title, "doc": doc }),
            );
        }
    }
    if name == "run_pipeline" {
        let result: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        if is_error || !result.is_object() {
            (ctx.emit)(json!({ "type": "run", "status": "error", "error": preview(&text) }));
        } else {
            (ctx.emit)(json!({
                "type": "run",
                "status": result.get("status").cloned().unwrap_or(Value::Null),
                "durationMs": result.get("duration_ms").cloned().unwrap_or(Value::Null),
                "nodes": result.get("nodes").cloned().unwrap_or(Value::Null),
                "error": result.get("error").cloned().unwrap_or(Value::Null),
            }));
        }
    }
    (text, is_error)
}

fn read_doc(path: &std::path::Path) -> Option<Value> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(duckle_duckdb_engine::format::strip_bom(&text)).ok()
}

/// Tools that act on a pipeline act on the swarm's own: an agent cannot read
/// past it into another, run another, or write anywhere else.
const ON_THE_PIPELINE: &[&str] = &[
    "run_pipeline",
    "validate_pipeline",
    "verify_pipeline",
    "pipeline_lineage",
    "check_node_sql",
    "trust_report",
];

/// The arguments the tool really gets: the workspace, the DuckDB binary and the
/// swarm's own pipeline filled in, and nothing an agent wrote that would point
/// a tool somewhere else.
fn scoped_args(name: &str, input: Value, ctx: &Ctx) -> Value {
    let mut args = match input {
        Value::Object(m) => m,
        _ => Map::new(),
    };
    let props = ctx
        .catalog
        .iter()
        .find(|t| t.get("name").and_then(Value::as_str) == Some(name))
        .and_then(|t| t.pointer("/inputSchema/properties"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let ws = ctx.workspace.to_string_lossy().to_string();
    let file = ctx.pipeline_file.to_string_lossy().to_string();
    if props.contains_key("workspace") {
        args.insert("workspace".into(), json!(ws));
    }
    if props.contains_key("duckdb") {
        match &ctx.duckdb {
            Some(d) => args.insert("duckdb".into(), json!(d)),
            None => args.remove("duckdb"),
        };
    }
    for key in ["directory", "logDir", "gpu"] {
        args.remove(key);
    }
    match name {
        "create_pipeline" => {
            args.insert("id".into(), json!(ctx.pipeline_id));
            args.insert("overwrite".into(), json!(true));
            args.entry("name").or_insert_with(|| json!(ctx.pipeline_id));
        }
        "update_pipeline" => {
            args.remove("path");
            args.insert("id".into(), json!(ctx.pipeline_id));
        }
        n if ON_THE_PIPELINE.contains(&n) => {
            args.remove("pipeline");
            args.insert("path".into(), json!(file));
        }
        "read_pipeline" => {
            args.entry("path").or_insert_with(|| json!(file));
        }
        "list_pipelines" => {
            args.insert(
                "directory".into(),
                json!(ctx.workspace.join("pipelines").to_string_lossy()),
            );
        }
        "read_run_logs" => {
            args.entry("pipelineName")
                .or_insert_with(|| json!(ctx.pipeline_id));
        }
        _ => {}
    }
    Value::Object(args)
}

/// A tool's schema as an agent sees it: without the arguments the swarm fills
/// in, so it neither wastes words on them nor tries to point them elsewhere.
fn visible_schema(name: &str, schema: &Value) -> Value {
    let mut hidden = vec!["workspace", "duckdb", "directory", "logDir", "gpu"];
    match name {
        "create_pipeline" => hidden.extend(["id", "overwrite"]),
        "update_pipeline" => hidden.extend(["id", "path"]),
        "list_pipelines" => hidden.push("directory"),
        n if ON_THE_PIPELINE.contains(&n) => hidden.extend(["pipeline", "path"]),
        _ => {}
    }
    let mut schema = schema.clone();
    if let Some(props) = schema.get_mut("properties").and_then(Value::as_object_mut) {
        for key in &hidden {
            props.remove(*key);
        }
    }
    if let Some(required) = schema.get_mut("required").and_then(Value::as_array_mut) {
        required.retain(|k| !k.as_str().is_some_and(|k| hidden.contains(&k)));
    }
    schema
}

fn specs_for(stage: Stage, catalog: &[Value]) -> Vec<ToolSpec> {
    let mut specs: Vec<ToolSpec> = stage
        .tools()
        .iter()
        .filter_map(|name| {
            let t = catalog
                .iter()
                .find(|t| t.get("name").and_then(Value::as_str) == Some(*name))?;
            Some(ToolSpec {
                name: (*name).to_string(),
                description: t
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                schema: visible_schema(name, t.get("inputSchema").unwrap_or(&json!({}))),
            })
        })
        .collect();
    let mut props = json!({
        "summary": {
            "type": "string",
            "description": "Notes for the next agent: exact names, paths, columns and values, what you did, and anything unresolved."
        }
    });
    let mut required = vec!["summary"];
    if stage.judges() {
        props["verdict"] = json!({ "type": "string", "enum": ["pass", "fail"] });
        required.push("verdict");
    }
    specs.push(ToolSpec {
        name: HAND_OFF.to_string(),
        description: "Finish your part and pass your findings on. Call it once, last.".to_string(),
        schema: json!({ "type": "object", "properties": props, "required": required }),
    });
    specs
}

/// Up to this many crew members are named to each agent; past it, a count.
const CREW_NAMED: usize = 40;

fn system_prompt(a: &AgentSpec, roster: &[AgentSpec], pipeline_id: &str) -> String {
    let format = match a.stage {
        Stage::Design | Stage::Build | Stage::Repair => {
            "\n\nPipeline format: {\"name\": \"...\", \"nodes\": [...], \"edges\": [...]}.\n\
             - node: {\"id\": \"n1\", \"type\": \"source\", \"position\": {\"x\": 80, \"y\": 200}, \"data\": \
             {\"label\": \"Orders\", \"componentId\": \"src.csv\", \"properties\": {...}}}. The type is \
             \"source\" for src.*, \"sink\" for snk.*, \"control\" for ctl.*, and \"transform\" for \
             everything else, quality gates included.\n\
             - edge: {\"id\": \"e1\", \"source\": \"n1\", \"target\": \"n2\", \"sourceHandle\": \"main\", \
             \"targetHandle\": \"main\", \"data\": {\"connectionType\": \"main\"}}. The handles are the \
             ports get_component_schema lists; a second input such as a join's arrives on its own port."
        }
        _ => "",
    };
    let mut crew: Vec<String> = roster
        .iter()
        .take(CREW_NAMED)
        .map(|m| format!("{} ({})", m.name, m.stage.title()))
        .collect();
    if roster.len() > CREW_NAMED {
        crew.push(format!("and {} more", roster.len() - CREW_NAMED));
    }
    let beside = roster
        .iter()
        .filter(|m| m.stage == a.stage && m.id != a.id)
        .count();
    let beside = if beside == 0 {
        String::new()
    } else if a.stage.side_by_side() {
        format!(
            "\n{beside} other agent(s) work this stage beside you, each with its own brief: cover yours."
        )
    } else {
        format!(
            "\n{beside} other agent(s) share this stage and take turns with you: build on their notes."
        )
    };
    let brief = if a.brief.is_empty() {
        String::new()
    } else {
        format!("\n\nYour own brief, on top of that:\n{}", a.brief)
    };
    format!(
        "You are {name}, one of {count} agents in Duckle's agent swarm. The swarm turns one goal \
         into a tested data pipeline in six stages, each agent handing its findings on: Discover \
         reads the data, Design plans the pipeline and its quality gates, Build writes it, Test \
         runs it, Repair fixes what the tests found, and Review reports to the person. Duckle \
         pipelines are JSON graphs of components that compile to DuckDB SQL.\n\n\
         The crew: {crew}.\n\n\
         This run:\n\
         - The pipeline is \"{pipeline_id}\". Every tool that reads, writes, checks or runs a pipeline \
         works on it, so you never pass its path.\n\
         - In a pipeline, refer to a file in the workspace as ${{workspace}}/<path>, for example \
         ${{workspace}}/data/orders.csv. Write outputs under ${{workspace}}/output/.\n\
         - Tools already know the workspace and the DuckDB binary.\n\n\
         How to work:\n\
         - Act through your tools, and look things up rather than assume them: a component id or \
         property key comes from list_components or get_component_schema, a column name from \
         inspect_source.\n\
         - Keep secrets out of pipelines; reference them as ${{ENV:NAME}}.\n\
         - Be brief between tool calls: a sentence on what you are doing is enough.\n\
         - When your part is done, call hand_off once, with notes the next agent can act on.\n\n\
         Your stage, {stage}:\n{duty}{beside}{brief}{format}",
        name = a.name,
        count = roster.len(),
        crew = crew.join(", "),
        stage = a.stage.title(),
        duty = a.stage.duty(),
    )
}

fn kickoff(goal: &str, notes: &[Note]) -> String {
    let mut text = format!("Goal: {}\n", goal.trim());
    if !notes.is_empty() {
        text.push_str("\nNotes from the agents before you, oldest first:\n");
        for note in notes {
            text.push_str(&format!(
                "\n[{}, {}]\n{}\n",
                note.name,
                note.stage.title(),
                note.summary
            ));
        }
    }
    text.push_str("\nDo your part now.");
    text
}

fn clip(text: &str) -> String {
    if text.len() <= RESULT_LIMIT {
        return text.to_string();
    }
    let mut end = RESULT_LIMIT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n... (cut at {RESULT_LIMIT} of {} characters; ask for less, e.g. list_components with kind or query)",
        &text[..end],
        text.len()
    )
}

fn preview(text: &str) -> String {
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= 280 {
        flat
    } else {
        format!("{}...", flat.chars().take(280).collect::<String>())
    }
}

/// `duckle-mcp swarm`: one request line in, one event per line out.
/// `duckle-mcp swarm --status`: what the swarm view can offer, as JSON.
pub fn main(args: &[String]) -> std::process::ExitCode {
    let stdout = std::io::stdout();
    if args.iter().any(|a| a == "--status") {
        let mut out = stdout.lock();
        let _ = writeln!(out, "{}", status());
        return std::process::ExitCode::SUCCESS;
    }
    let stdin = std::io::stdin();
    let mut line = String::new();
    if stdin.lock().read_line(&mut line).is_err() || line.trim().is_empty() {
        eprintln!("duckle-mcp swarm: expected one JSON request line on stdin");
        return std::process::ExitCode::from(2);
    }
    // Agents working at once write events from their own threads; each line
    // goes out whole.
    let emit = |v: Value| {
        let mut out = stdout.lock();
        let _ = writeln!(out, "{v}");
        let _ = out.flush();
    };
    let req: Request = match serde_json::from_str(&line) {
        Ok(r) => r,
        Err(e) => {
            emit(json!({ "type": "error", "message": format!("bad request: {e}") }));
            emit(
                json!({ "type": "finished", "ok": false, "summary": format!("bad request: {e}") }),
            );
            return std::process::ExitCode::from(2);
        }
    };
    // The rest of stdin is the stop signal: a line saying "stop", or the
    // parent going away.
    let cancel = Arc::new(AtomicBool::new(false));
    {
        let cancel = Arc::clone(&cancel);
        std::thread::spawn(move || {
            let mut rest = String::new();
            loop {
                rest.clear();
                match std::io::stdin().lock().read_line(&mut rest) {
                    Ok(0) | Err(_) => break,
                    Ok(_) if rest.trim() == "stop" => break,
                    Ok(_) => {}
                }
            }
            cancel.store(true, Ordering::Relaxed);
        });
    }
    if let Some(d) = clean(&req.duckdb) {
        std::env::set_var("DUCKLE_DUCKDB_BIN", d);
    }
    // Checked once up front, so a missing key is one clear error rather than
    // one per agent.
    let label = match model_for(&req.provider) {
        Ok((_, label)) => label,
        Err(message) => {
            emit(json!({ "type": "error", "message": message }));
            emit(json!({ "type": "finished", "ok": false, "summary": message }));
            return std::process::ExitCode::from(1);
        }
    };
    let make = || model_for(&req.provider).map(|(m, _)| m);
    let done = run(&req, &make, &label, &call_tool, &emit, &cancel);
    if done.get("ok").and_then(Value::as_bool) == Some(true) {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::{ToolCall, Turn};
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::AtomicUsize;

    type Calls = Vec<(String, Value)>;

    /// A model that plays back a script per agent (found by the name its
    /// system prompt gives it), and records what each agent was given. It
    /// can pause in each turn, so agents meant to work at once overlap.
    struct Script {
        turns: Mutex<HashMap<String, VecDeque<(String, Calls)>>>,
        seen: Mutex<Vec<Seen>>,
        in_flight: AtomicUsize,
        most_at_once: AtomicUsize,
        pause_ms: u64,
        /// The run's stop flag, which the script can press itself...
        cancel: AtomicBool,
        /// ...during this turn of the whole run (counting from 1).
        stop_at_turn: AtomicUsize,
        turn_count: AtomicUsize,
    }

    #[derive(Clone)]
    struct Seen {
        agent: String,
        system: String,
        tools: Vec<String>,
        history: Vec<Value>,
    }

    fn script(turns: Vec<(&str, &str, Vec<(&str, Value)>)>, pause_ms: u64) -> Arc<Script> {
        let mut by_agent: HashMap<String, VecDeque<(String, Calls)>> = HashMap::new();
        for (agent, text, calls) in turns {
            by_agent.entry(agent.to_string()).or_default().push_back((
                text.to_string(),
                calls.into_iter().map(|(n, a)| (n.to_string(), a)).collect(),
            ));
        }
        Arc::new(Script {
            turns: Mutex::new(by_agent),
            seen: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            most_at_once: AtomicUsize::new(0),
            pause_ms,
            cancel: AtomicBool::new(false),
            stop_at_turn: AtomicUsize::new(0),
            turn_count: AtomicUsize::new(0),
        })
    }

    impl Script {
        fn seen(&self) -> Vec<Seen> {
            self.seen.lock().unwrap().clone()
        }
        fn seen_by(&self, agent: &str) -> Vec<Seen> {
            self.seen()
                .into_iter()
                .filter(|s| s.agent == agent)
                .collect()
        }
    }

    struct Scripted(Arc<Script>);

    impl Model for Scripted {
        fn turn(
            &mut self,
            system: &str,
            tools: &[ToolSpec],
            history: &mut Vec<Value>,
            on: &mut dyn FnMut(Delta),
            _cancel: &AtomicBool,
        ) -> Result<Turn, String> {
            let s = &self.0;
            let agent = system
                .strip_prefix("You are ")
                .and_then(|r| r.split(", one of").next())
                .unwrap_or("?")
                .to_string();
            s.seen.lock().unwrap().push(Seen {
                agent: agent.clone(),
                system: system.to_string(),
                tools: tools.iter().map(|t| t.name.clone()).collect(),
                history: history.clone(),
            });
            let turn = s.turn_count.fetch_add(1, Ordering::SeqCst) + 1;
            if turn == s.stop_at_turn.load(Ordering::SeqCst) {
                s.cancel.store(true, Ordering::SeqCst);
            }
            let now = s.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            s.most_at_once.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(s.pause_ms));
            s.in_flight.fetch_sub(1, Ordering::SeqCst);
            let (text, calls) = s
                .turns
                .lock()
                .unwrap()
                .get_mut(&agent)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| format!("the script has no turn left for {agent}"))?;
            on(Delta::Text(&text));
            let calls: Vec<ToolCall> = calls
                .into_iter()
                .enumerate()
                .map(|(i, (name, input))| ToolCall {
                    id: format!("{agent}_{}_{i}", history.len()),
                    name,
                    input: Ok(input),
                })
                .collect();
            history.push(json!({ "role": "assistant", "content": text, "calls": calls.len() }));
            // Markers in the script for the endings a real model can give.
            let stop = match text.as_str() {
                "<cut>" => Stop::Truncated,
                "<declined>" => Stop::Refused("cyber".into()),
                _ if calls.is_empty() => Stop::Done,
                _ => Stop::Tools,
            };
            Ok(Turn {
                stop,
                text,
                calls,
                input_tokens: 10,
                output_tokens: 5,
            })
        }

        fn tool_results(&self, history: &mut Vec<Value>, results: &[ToolOutput]) {
            for r in results {
                history.push(
                    json!({ "role": "tool", "id": r.id, "text": r.text, "error": r.is_error }),
                );
            }
        }
    }

    fn hand(summary: &str) -> (&'static str, Value) {
        ("hand_off", json!({ "summary": summary }))
    }

    fn verdict(summary: &str, v: &str) -> (&'static str, Value) {
        ("hand_off", json!({ "summary": summary, "verdict": v }))
    }

    fn workspace() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "duckle-swarm-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(dir.join("pipelines")).unwrap();
        dir
    }

    fn custom(id: &str, name: &str, stage: Stage, brief: &str) -> AgentSpec {
        AgentSpec {
            id: id.into(),
            name: name.into(),
            stage,
            brief: brief.into(),
        }
    }

    /// Runs the swarm with a scripted model and stub tools. The stub writes the
    /// pipeline file when create_pipeline is called, as the real tool does, and
    /// records every call with the arguments it really received.
    fn play(
        ws: &std::path::Path,
        script: &Arc<Script>,
        agents: Vec<AgentSpec>,
        cancel: bool,
        answer: impl Fn(&str, &Value) -> (String, bool) + Sync,
    ) -> (Value, Vec<Value>, Calls) {
        let req = Request {
            goal: "Total revenue per customer from orders.csv".into(),
            workspace: ws.to_string_lossy().into(),
            duckdb: Some("/opt/duckdb".into()),
            provider: Provider::default(),
            agents,
        };
        let events = Mutex::new(Vec::new());
        let calls = Mutex::new(Vec::new());
        let dir = ws.join("pipelines");
        let tools = |name: &str, args: Value| {
            calls.lock().unwrap().push((name.to_string(), args.clone()));
            if name == "create_pipeline" {
                let id = args["id"].as_str().unwrap().to_string();
                std::fs::write(dir.join(format!("{id}.json")), args["pipeline"].to_string())
                    .unwrap();
            }
            answer(name, &args)
        };
        let make = || -> Result<Box<dyn Model + Send>, String> {
            Ok(Box::new(Scripted(Arc::clone(script))))
        };
        let emit = |e: Value| events.lock().unwrap().push(e);
        if cancel {
            script.cancel.store(true, Ordering::SeqCst);
        }
        let done = run(&req, &make, "test model", &tools, &emit, &script.cancel);
        (
            done,
            events.into_inner().unwrap(),
            calls.into_inner().unwrap(),
        )
    }

    #[test]
    fn a_stop_between_waves_of_a_big_review_stage_does_not_pass_the_mission() {
        let ws = workspace();
        // Seven reviewers: the first wave holds six, the seventh waits for a
        // free slot. The stop lands as the first wave's last turn is taken,
        // so that wave finishes and the seventh never starts.
        let mut crew = default_roster();
        let critics: Vec<String> = (0..6).map(|i| format!("Critic {i}")).collect();
        for (i, name) in critics.iter().enumerate() {
            crew.push(custom(&format!("c{i}"), name, Stage::Review, ""));
        }
        let mut turns = vec![
            ("Scout", "", vec![hand("data")]),
            ("Architect", "", vec![hand("design")]),
            (
                "Builder",
                "",
                vec![(
                    "create_pipeline",
                    json!({ "name": "R", "pipeline": pipeline() }),
                )],
            ),
            ("Builder", "", vec![hand("built")]),
            ("Tester", "", vec![verdict("ok", "pass")]),
            ("Reviewer", "", vec![verdict("ok", "pass")]),
        ];
        for name in &critics {
            turns.push((name.as_str(), "", vec![verdict("ok", "pass")]));
        }
        let s = script(turns, 30);
        // Scout, Architect, two Builder turns, Tester, then six reviewers.
        s.stop_at_turn.store(11, Ordering::SeqCst);
        let (done, _, _) = play(&ws, &s, crew, false, ok);
        assert_eq!(done["ok"], json!(false), "{done}");
        assert_eq!(done["stopped"], json!(true));
        assert_eq!(s.seen().len(), 11, "the seventh reviewer must not start");
        let _ = std::fs::remove_dir_all(ws);
    }

    fn pipeline() -> Value {
        json!({ "name": "Revenue", "nodes": [{ "id": "n1" }], "edges": [] })
    }

    fn working(events: &[Value]) -> Vec<String> {
        events
            .iter()
            .filter(|e| e["type"] == "agent" && e["state"] == "working")
            .map(|e| e["agent"].as_str().unwrap().to_string())
            .collect()
    }

    fn ok(_: &str, _: &Value) -> (String, bool) {
        ("{}".into(), false)
    }

    #[test]
    fn the_default_crew_runs_each_stage_in_order_and_reports_a_passing_pipeline() {
        let ws = workspace();
        let s = script(
            vec![
                (
                    "Scout",
                    "Looking.",
                    vec![("list_workspace_files", json!({}))],
                ),
                ("Scout", "", vec![hand("orders.csv: customer_id, amount")]),
                (
                    "Architect",
                    "",
                    vec![hand("src.csv -> xf.groupby -> qa.unique -> snk.parquet")],
                ),
                (
                    "Builder",
                    "Building.",
                    vec![(
                        "create_pipeline",
                        json!({ "name": "Revenue", "pipeline": pipeline() }),
                    )],
                ),
                ("Builder", "", vec![hand("built as designed")]),
                ("Tester", "", vec![("run_pipeline", json!({}))]),
                ("Tester", "", vec![verdict("ran, 3 gates held", "pass")]),
                (
                    "Reviewer",
                    "",
                    vec![verdict(
                        "Writes output/revenue.parquet; 412 customers.",
                        "pass",
                    )],
                ),
            ],
            0,
        );
        let (done, events, calls) = play(&ws, &s, vec![], false, |name, _| match name {
            "run_pipeline" => (
                json!({ "status": "ok", "duration_ms": 12, "nodes": { "n1": { "rows": 412 } } })
                    .to_string(),
                false,
            ),
            _ => ("{}".into(), false),
        });

        assert_eq!(done["ok"], json!(true), "{done}");
        assert_eq!(
            done["summary"],
            json!("Writes output/revenue.parquet; 412 customers.")
        );
        assert_eq!(done["toolCalls"], json!(3));
        assert_eq!(done["agents"], json!(6));
        assert_eq!(
            working(&events),
            ["scout", "architect", "builder", "tester", "reviewer"],
            "no Repair when the tests pass"
        );
        let stages: Vec<&str> = events
            .iter()
            .filter(|e| e["type"] == "stage")
            .map(|e| e["stage"].as_str().unwrap())
            .collect();
        assert_eq!(stages, ["discover", "design", "build", "test", "review"]);

        // Each agent starts from the notes of the ones before it...
        let tester = &s.seen_by("Tester")[0];
        let kickoff = tester.history[0]["content"].as_str().unwrap();
        assert!(
            kickoff.contains("[Architect, Design]\nsrc.csv -> xf.groupby"),
            "{kickoff}"
        );
        // ...and sees only its stage's tools, plus hand_off.
        assert_eq!(
            tester.tools,
            [
                "run_pipeline",
                "inspect_source",
                "read_pipeline",
                "read_run_logs",
                "verify_pipeline",
                "hand_off"
            ]
        );

        // The canvas gets the pipeline the moment it is written, and the run's rows.
        let pipeline_event = events
            .iter()
            .find(|e| e["type"] == "pipeline")
            .expect("a pipeline event");
        assert_eq!(pipeline_event["doc"], pipeline());
        let run_event = events
            .iter()
            .find(|e| e["type"] == "run")
            .expect("a run event");
        assert_eq!(run_event["nodes"]["n1"]["rows"], json!(412));
        assert!(calls.iter().any(|(n, _)| n == "run_pipeline"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn any_number_of_agents_can_join_and_the_reading_stages_work_side_by_side() {
        let ws = workspace();
        let mut crew = default_roster();
        crew.push(custom(
            "profiler",
            "Profiler",
            Stage::Discover,
            "Profile nulls and ranges.",
        ));
        crew.push(custom(
            "pii",
            "PII Guardian",
            Stage::Review,
            "Fail if raw personal data reaches the sink.",
        ));
        crew.push(custom(
            "perf",
            "Performance Critic",
            Stage::Review,
            "Look for expensive steps.",
        ));
        let s = script(
            vec![
                (
                    "Scout",
                    "",
                    vec![hand("orders.csv has customer_id, amount")],
                ),
                ("Profiler", "", vec![hand("amount: 0 nulls, 3..480")]),
                ("Architect", "", vec![hand("design")]),
                (
                    "Builder",
                    "",
                    vec![(
                        "create_pipeline",
                        json!({ "name": "R", "pipeline": pipeline() }),
                    )],
                ),
                ("Builder", "", vec![hand("built")]),
                ("Tester", "", vec![verdict("gates held", "pass")]),
                ("Reviewer", "", vec![verdict("meets the goal", "pass")]),
                (
                    "PII Guardian",
                    "",
                    vec![verdict("no personal data", "pass")],
                ),
                (
                    "Performance Critic",
                    "",
                    vec![verdict("one pass over the file", "pass")],
                ),
            ],
            80,
        );
        let (done, events, _) = play(&ws, &s, crew, false, ok);
        assert_eq!(done["ok"], json!(true), "{done}");
        assert_eq!(done["agents"], json!(9));
        let mut ran = working(&events);
        ran.sort();
        assert_eq!(
            ran,
            [
                "architect",
                "builder",
                "perf",
                "pii",
                "profiler",
                "reviewer",
                "scout",
                "tester"
            ]
        );
        assert!(
            s.most_at_once.load(Ordering::SeqCst) >= 3,
            "the three reviewers should overlap, most at once: {}",
            s.most_at_once.load(Ordering::SeqCst)
        );

        // A custom agent's own brief, and its stage's duty, reach its prompt.
        let profiler = &s.seen_by("Profiler")[0];
        assert!(profiler.system.contains("Profile nulls and ranges."));
        assert!(profiler.system.contains("Your stage, Discover"));
        assert!(profiler
            .system
            .contains("1 other agent(s) work this stage beside you"));
        assert!(profiler.system.contains("one of 9 agents"));
        // Working side by side, the Scout and the Profiler start from the same
        // notes; the Architect after them reads both.
        let architect = &s.seen_by("Architect")[0];
        let notes = architect.history[0]["content"].as_str().unwrap();
        assert!(
            notes.contains("[Scout, Discover]") && notes.contains("[Profiler, Discover]"),
            "{notes}"
        );
        // The Tester's hand-off fans out to every reviewer.
        for to in ["reviewer", "pii", "perf"] {
            assert!(
                events
                    .iter()
                    .any(|e| e["type"] == "handoff" && e["from"] == "tester" && e["to"] == to),
                "no hand-off to {to}"
            );
        }
        // Every reviewer's words reach the person.
        let summary = done["summary"].as_str().unwrap();
        assert!(
            summary.contains("PII Guardian: no personal data"),
            "{summary}"
        );
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn every_tester_must_pass_before_the_repair_stage_is_skipped() {
        let ws = workspace();
        let mut crew = default_roster();
        crew.push(custom(
            "edge",
            "Edge-case Tester",
            Stage::Test,
            "Try null keys and duplicates.",
        ));
        let s = script(
            vec![
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![hand("design")]),
                (
                    "Builder",
                    "",
                    vec![(
                        "create_pipeline",
                        json!({ "name": "R", "pipeline": pipeline() }),
                    )],
                ),
                ("Builder", "", vec![hand("built")]),
                ("Tester", "", vec![verdict("main path ok", "pass")]),
                (
                    "Edge-case Tester",
                    "",
                    vec![verdict("a null key slips through", "fail")],
                ),
                ("Fixer", "", vec![hand("added qa.notnull on the key")]),
                ("Tester", "", vec![verdict("main path ok", "pass")]),
                (
                    "Edge-case Tester",
                    "",
                    vec![verdict("null keys refused", "pass")],
                ),
                ("Reviewer", "", vec![verdict("good", "pass")]),
            ],
            0,
        );
        let (done, events, _) = play(&ws, &s, crew, false, ok);
        assert_eq!(done["ok"], json!(true), "{done}");
        assert_eq!(
            working(&events),
            [
                "scout",
                "architect",
                "builder",
                "tester",
                "edge",
                "fixer",
                "tester",
                "edge",
                "reviewer"
            ],
            "one failing tester must send the Repair stage in"
        );
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn one_failing_reviewer_fails_the_run() {
        let ws = workspace();
        let mut crew = default_roster();
        crew.push(custom(
            "pii",
            "PII Guardian",
            Stage::Review,
            "Fail if raw personal data reaches the sink.",
        ));
        let s = script(
            vec![
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![hand("design")]),
                (
                    "Builder",
                    "",
                    vec![(
                        "create_pipeline",
                        json!({ "name": "R", "pipeline": pipeline() }),
                    )],
                ),
                ("Builder", "", vec![hand("built")]),
                ("Tester", "", vec![verdict("gates held", "pass")]),
                ("Reviewer", "", vec![verdict("meets the goal", "pass")]),
                (
                    "PII Guardian",
                    "",
                    vec![verdict("email reaches the sink unmasked", "fail")],
                ),
            ],
            0,
        );
        let (done, _, _) = play(&ws, &s, crew, false, ok);
        assert_eq!(done["ok"], json!(false));
        assert_eq!(done["tested"], json!(true));
        assert_eq!(done["reviewed"], json!(false));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_crew_is_refused_without_a_builder_or_with_two_agents_of_one_id() {
        let ws = workspace();
        let s = script(vec![], 0);
        let no_builder = default_roster()
            .into_iter()
            .filter(|a| a.stage != Stage::Build)
            .collect();
        let (done, events, _) = play(&ws, &s, no_builder, false, ok);
        assert_eq!(done["ok"], json!(false));
        assert!(
            done["summary"].as_str().unwrap().contains("Build stage"),
            "{done}"
        );
        assert!(events.iter().any(|e| e["type"] == "error"));
        assert!(s.seen().is_empty(), "nothing ran");

        let mut twice = default_roster();
        twice.push(custom("scout", "Second Scout", Stage::Discover, ""));
        let (done, _, _) = play(&ws, &s, twice, false, ok);
        assert!(
            done["summary"]
                .as_str()
                .unwrap()
                .contains("share the id 'scout'"),
            "{done}"
        );
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_failing_test_sends_the_repair_stage_in_and_the_tests_back() {
        let ws = workspace();
        let s = script(
            vec![
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![hand("design")]),
                (
                    "Builder",
                    "",
                    vec![(
                        "create_pipeline",
                        json!({ "name": "R", "pipeline": pipeline() }),
                    )],
                ),
                ("Builder", "", vec![hand("built")]),
                (
                    "Tester",
                    "",
                    vec![verdict("qa.unique failed: 3 duplicate customer_id", "fail")],
                ),
                (
                    "Fixer",
                    "",
                    vec![(
                        "update_pipeline",
                        json!({ "patch": { "nodes": [{ "id": "n2" }] } }),
                    )],
                ),
                (
                    "Fixer",
                    "",
                    vec![hand("grouped by customer_id before the gate")],
                ),
                ("Tester", "", vec![verdict("all gates hold", "pass")]),
                ("Reviewer", "", vec![verdict("good", "pass")]),
            ],
            0,
        );
        let (done, events, _) = play(&ws, &s, vec![], false, ok);
        assert_eq!(done["ok"], json!(true));
        assert_eq!(
            working(&events),
            [
                "scout",
                "architect",
                "builder",
                "tester",
                "fixer",
                "tester",
                "reviewer"
            ]
        );
        assert!(events
            .iter()
            .any(|e| e["type"] == "handoff" && e["from"] == "fixer" && e["to"] == "tester"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_pipeline_that_never_passes_is_reported_as_failing_after_two_repairs() {
        let ws = workspace();
        let fail = || verdict("still wrong", "fail");
        let s = script(
            vec![
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![hand("design")]),
                (
                    "Builder",
                    "",
                    vec![(
                        "create_pipeline",
                        json!({ "name": "R", "pipeline": pipeline() }),
                    )],
                ),
                ("Builder", "", vec![hand("built")]),
                ("Tester", "", vec![fail()]),
                ("Fixer", "", vec![hand("fix 1")]),
                ("Tester", "", vec![fail()]),
                ("Fixer", "", vec![hand("fix 2")]),
                ("Tester", "", vec![fail()]),
                (
                    "Reviewer",
                    "",
                    vec![verdict("does not meet the goal", "fail")],
                ),
            ],
            0,
        );
        let (done, events, _) = play(&ws, &s, vec![], false, ok);
        assert_eq!(done["ok"], json!(false));
        assert_eq!(done["tested"], json!(false));
        let fixers = working(&events).iter().filter(|a| *a == "fixer").count();
        assert_eq!(fixers, FIX_ROUNDS);
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn tools_act_on_the_swarms_own_pipeline_whatever_the_agent_names() {
        let ws = workspace();
        let s = script(
            vec![
                (
                    "Scout",
                    "",
                    vec![
                        ("read_pipeline", json!({ "path": "elsewhere.json" })),
                        hand("data"),
                    ],
                ),
                ("Architect", "", vec![hand("design")]),
                (
                    "Builder",
                    "",
                    vec![(
                        "create_pipeline",
                        json!({ "name": "R", "id": "payroll", "directory": "C:/Windows", "workspace": "D:/other", "pipeline": pipeline() }),
                    )],
                ),
                (
                    "Builder",
                    "",
                    vec![(
                        "update_pipeline",
                        json!({ "path": "D:/other/pipelines/payroll.json", "patch": {} }),
                    )],
                ),
                ("Builder", "", vec![hand("built")]),
                (
                    "Tester",
                    "",
                    vec![(
                        "run_pipeline",
                        json!({ "pipeline": { "nodes": [] }, "duckdb": "evil.exe" }),
                    )],
                ),
                ("Tester", "", vec![verdict("ok", "pass")]),
                ("Reviewer", "", vec![verdict("ok", "pass")]),
            ],
            0,
        );
        let (_, events, calls) = play(&ws, &s, vec![], false, ok);
        let id = events[0]["pipelineId"].as_str().unwrap().to_string();
        assert_eq!(id, "swarm_total_revenue_customer_orders");
        let file = ws
            .join("pipelines")
            .join(format!("{id}.json"))
            .to_string_lossy()
            .to_string();
        let ws_text = ws.to_string_lossy().to_string();
        let args = |n: &str| {
            calls
                .iter()
                .find(|(name, _)| name == n)
                .map(|(_, a)| a.clone())
                .unwrap()
        };

        // Reading another pipeline for reference is allowed: that is a read.
        assert_eq!(args("read_pipeline")["path"], json!("elsewhere.json"));
        let create = args("create_pipeline");
        assert_eq!(create["id"], json!(id));
        assert_eq!(create["workspace"], json!(ws_text));
        assert!(create.get("directory").is_none());
        let update = args("update_pipeline");
        assert!(update.get("path").is_none());
        assert_eq!(update["id"], json!(id));
        let run = args("run_pipeline");
        assert_eq!(run["path"], json!(file));
        assert!(run.get("pipeline").is_none());
        assert_eq!(run["duckdb"], json!("/opt/duckdb"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn an_agent_cannot_call_a_tool_outside_its_stage() {
        let ws = workspace();
        let s = script(
            vec![
                ("Scout", "", vec![("run_pipeline", json!({}))]),
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![hand("design")]),
                ("Builder", "", vec![hand("forgot to build")]),
            ],
            0,
        );
        let (done, _, calls) = play(&ws, &s, vec![], false, ok);
        assert!(
            calls.is_empty(),
            "the Scout may not run pipelines: {calls:?}"
        );
        // The refusal went back to the model as an error it can read.
        let reply = s.seen_by("Scout")[1].history.last().unwrap().clone();
        assert_eq!(reply["error"], json!(true));
        assert!(reply["text"]
            .as_str()
            .unwrap()
            .contains("not one of your tools"));
        assert_eq!(done["ok"], json!(false));
        assert_eq!(
            done["summary"],
            json!("The Build stage finished without writing the pipeline.")
        );
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_judge_that_hands_off_without_a_verdict_has_not_passed() {
        let ws = workspace();
        let s = script(
            vec![
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![hand("design")]),
                (
                    "Builder",
                    "",
                    vec![(
                        "create_pipeline",
                        json!({ "name": "R", "pipeline": pipeline() }),
                    )],
                ),
                ("Builder", "", vec![hand("built")]),
                ("Tester", "", vec![hand("looks fine")]),
                ("Fixer", "", vec![hand("fixed nothing")]),
                ("Tester", "", vec![verdict("ok", "pass")]),
                ("Reviewer", "", vec![verdict("ok", "pass")]),
            ],
            0,
        );
        let (done, events, _) = play(&ws, &s, vec![], false, ok);
        assert_eq!(done["ok"], json!(true));
        let first_test = events
            .iter()
            .find(|e| e["type"] == "agent" && e["agent"] == "tester" && e["state"] == "done")
            .unwrap();
        assert_eq!(first_test["verdict"], json!("fail"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_turn_cut_off_at_the_output_limit_runs_none_of_its_calls_and_is_asked_again() {
        let ws = workspace();
        let s = script(
            vec![
                ("Scout", "<cut>", vec![("list_workspace_files", json!({}))]),
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![hand("design")]),
                ("Builder", "", vec![hand("no build")]),
            ],
            0,
        );
        let (_, _, calls) = play(&ws, &s, vec![], false, ok);
        assert!(calls.is_empty(), "a cut-off call ran: {calls:?}");
        // The cut-off turn is gone from the conversation, and the agent is told why.
        let retried = &s.seen_by("Scout")[1].history;
        assert_eq!(retried.len(), 2, "{retried:?}");
        assert!(retried[1]["content"]
            .as_str()
            .unwrap()
            .contains("cut off at the output limit"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn an_empty_turn_is_not_a_hand_off_and_the_agent_is_asked_again() {
        // A local model can end a turn with nothing at all: its reasoning spent, or a
        // tool call its server could not parse and dropped.
        let ws = workspace();
        let s = script(
            vec![
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![]),
                ("Architect", "", vec![hand("design")]),
                ("Builder", "", vec![hand("no build")]),
            ],
            0,
        );
        let (_, events, _) = play(&ws, &s, vec![], false, ok);
        // The empty turn is gone from the conversation, and the agent is told what happened,
        // and that a long call is the kind that gets lost.
        let retried = &s.seen_by("Architect")[1].history;
        assert_eq!(retried.len(), 2, "{retried:?}");
        let told = retried[1]["content"].as_str().unwrap();
        assert!(told.contains("no text and no tool call"), "{told}");
        assert!(told.contains("smaller steps"), "{told}");
        assert!(events.iter().any(|e| e["type"] == "agent"
            && e["agent"] == "architect"
            && e["state"] == "done"
            && e["summary"] == "design"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn an_agent_that_stays_silent_is_asked_twice_then_let_go() {
        let ws = workspace();
        let s = script(
            vec![
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![]),
                ("Architect", "", vec![]),
                ("Architect", "", vec![]),
                ("Builder", "", vec![hand("no build")]),
            ],
            0,
        );
        let (done, _, _) = play(&ws, &s, vec![], false, ok);
        assert_eq!(
            s.seen_by("Architect").len(),
            3,
            "asked twice more, then let go"
        );
        assert_eq!(
            done["summary"],
            json!("The Build stage finished without writing the pipeline.")
        );
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn only_silence_in_a_row_runs_out_of_retries() {
        // A builder working in small steps can lose a call now and then; a turn that does
        // something in between starts the count again.
        let ws = workspace();
        let s = script(
            vec![
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![]),
                ("Architect", "", vec![]),
                ("Architect", "", vec![("list_components", json!({}))]),
                ("Architect", "", vec![]),
                ("Architect", "", vec![]),
                ("Architect", "", vec![hand("design")]),
                ("Builder", "", vec![hand("no build")]),
            ],
            0,
        );
        let (_, events, _) = play(&ws, &s, vec![], false, ok);
        assert_eq!(s.seen_by("Architect").len(), 6);
        assert!(events.iter().any(|e| e["type"] == "agent"
            && e["agent"] == "architect"
            && e["state"] == "done"
            && e["summary"] == "design"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_model_that_keeps_running_past_its_output_limit_fails_the_agent() {
        // Cut off at the cap three turns in a row: the model is looping, not working.
        let ws = workspace();
        let s = script(
            vec![
                ("Scout", "<cut>", vec![]),
                ("Scout", "<cut>", vec![]),
                ("Scout", "<cut>", vec![]),
            ],
            0,
        );
        let (done, events, _) = play(&ws, &s, vec![], false, ok);
        assert_eq!(s.seen_by("Scout").len(), 3, "asked twice more, then failed");
        assert_eq!(done["ok"], json!(false));
        assert!(
            done["summary"].as_str().unwrap().contains("output limit"),
            "{done}"
        );
        assert!(events
            .iter()
            .any(|e| e["type"] == "agent" && e["agent"] == "scout" && e["state"] == "failed"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn only_cut_off_turns_in_a_row_run_out_of_retries() {
        let ws = workspace();
        let s = script(
            vec![
                ("Scout", "<cut>", vec![]),
                ("Scout", "", vec![("list_workspace_files", json!({}))]),
                ("Scout", "<cut>", vec![]),
                ("Scout", "<cut>", vec![]),
                ("Scout", "", vec![hand("data")]),
                ("Architect", "", vec![hand("design")]),
                ("Builder", "", vec![hand("no build")]),
            ],
            0,
        );
        let (_, events, _) = play(&ws, &s, vec![], false, ok);
        assert!(events.iter().any(|e| e["type"] == "agent"
            && e["agent"] == "scout"
            && e["state"] == "done"
            && e["summary"] == "data"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_declined_turn_fails_the_agent_and_ends_the_run() {
        let ws = workspace();
        let s = script(vec![("Scout", "<declined>", vec![])], 0);
        let (done, events, _) = play(&ws, &s, vec![], false, ok);
        assert_eq!(done["ok"], json!(false));
        assert_eq!(
            done["summary"],
            json!("The model declined to go on (cyber).")
        );
        assert!(events
            .iter()
            .any(|e| e["type"] == "agent" && e["agent"] == "scout" && e["state"] == "failed"));
        assert_eq!(s.seen().len(), 1, "nothing ran after the decline");
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn stopping_ends_the_run_before_the_next_step() {
        let ws = workspace();
        let s = script(vec![("Scout", "", vec![hand("data")])], 0);
        let (done, _, _) = play(&ws, &s, vec![], true, ok);
        assert_eq!(done["ok"], json!(false));
        assert_eq!(done["stopped"], json!(true));
        assert!(s.seen().is_empty(), "no model call after a stop");

        // A side-by-side stage stops too, and says so rather than passing.
        let mut crew = default_roster();
        crew.push(custom("profiler", "Profiler", Stage::Discover, ""));
        let (done, _, _) = play(&ws, &s, crew, true, ok);
        assert_eq!(done["stopped"], json!(true));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn the_pipeline_id_comes_from_the_goal_and_never_names_an_existing_file() {
        let ws = workspace();
        let dir = ws.join("pipelines");
        assert_eq!(
            pipeline_id_for("Build a pipeline: orders.csv -> revenue per region!", &dir),
            "swarm_orders_csv_revenue_region"
        );
        std::fs::write(dir.join("swarm_orders_csv_revenue_region.json"), "{}").unwrap();
        assert_eq!(
            pipeline_id_for("orders csv revenue region", &dir),
            "swarm_orders_csv_revenue_region_2"
        );
        assert_eq!(pipeline_id_for("!!!", &dir), "swarm_pipeline");
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn an_environment_key_only_goes_to_its_own_provider() {
        std::env::set_var("HF_TOKEN", "hf_secret");
        assert_eq!(
            env_key_for("https://router.huggingface.co/v1").as_deref(),
            Some("hf_secret")
        );
        assert_eq!(
            env_key_for("https://ROUTER.huggingface.co:443/v1").as_deref(),
            Some("hf_secret")
        );
        assert_eq!(env_key_for("https://attacker.example/v1"), None);
        assert_eq!(
            env_key_for("https://router.huggingface.co.attacker.example/v1"),
            None
        );
        let keyless = Provider {
            kind: "openai-compatible".into(),
            model: Some("m".into()),
            base_url: Some("https://attacker.example/v1".into()),
            ..Provider::default()
        };
        assert!(model_for(&keyless).is_ok(), "a keyless endpoint is allowed");

        std::env::set_var("ANTHROPIC_API_KEY", "sk-env");
        let claude = |base: Option<&str>, key: Option<&str>| Provider {
            kind: "anthropic-key".into(),
            base_url: base.map(str::to_string),
            api_key: key.map(str::to_string),
            ..Provider::default()
        };
        assert!(model_for(&claude(None, None)).is_ok());
        let refused = model_for(&claude(Some("https://attacker.example"), None))
            .err()
            .unwrap();
        assert!(refused.contains("typed in"), "{refused}");
        assert!(model_for(&claude(Some("https://gateway.example"), Some("typed"))).is_ok());
        assert!(
            model_for(&claude(Some("http://127.0.0.1:9"), None)).is_ok(),
            "a stub on this machine"
        );
    }

    #[test]
    fn hidden_arguments_are_not_offered_to_the_model() {
        let catalog = crate::tools::list_tools().as_array().cloned().unwrap();
        let specs = specs_for(Stage::Build, &catalog);
        let create = specs.iter().find(|s| s.name == "create_pipeline").unwrap();
        let props = create.schema["properties"].as_object().unwrap();
        assert!(props.contains_key("pipeline") && props.contains_key("name"));
        for gone in ["workspace", "directory", "id", "overwrite"] {
            assert!(!props.contains_key(gone), "{gone} offered");
        }
        // Every tool a stage names exists in the catalog.
        for stage in [
            Stage::Discover,
            Stage::Design,
            Stage::Build,
            Stage::Test,
            Stage::Repair,
            Stage::Review,
        ] {
            let specs = specs_for(stage, &catalog);
            assert_eq!(
                specs.len(),
                stage.tools().len() + 1,
                "{stage:?} names a tool that does not exist"
            );
        }
    }
}
