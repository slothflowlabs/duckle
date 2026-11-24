//! The swarm: six specialist agents that turn a goal into a tested pipeline.
//!
//! `duckle-mcp swarm` reads one request line on stdin and writes one event per
//! line on stdout until the run ends. Every agent works only through this
//! server's own tools, called in this process exactly as an MCP client's call
//! would be, so the swarm can do nothing an agent connected over MCP could not.
//! The desktop app and the web editor start this process and relay its events
//! to the swarm view. Closing its stdin stops it at the next step.
//!
//! The agents run in turn, each starting from the notes of the ones before:
//! Scout reads the data, Architect designs the pipeline and the quality gates
//! that will prove it, Builder writes it, Tester runs it, Fixer repairs what
//! the Tester found (up to two rounds), and Reviewer reports to the person.

use crate::llm::{self, Delta, Model, Stop, ToolOutput, ToolSpec};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub const DEFAULT_CLAUDE: &str = "claude-opus-5-5";
/// Model calls one agent may make before it must hand off.
const MAX_TURNS: usize = 16;
/// Times the Fixer may repair before the run is reported as failing.
const FIX_ROUNDS: usize = 2;
/// The longest tool answer an agent reads. A catalog listing can run far
/// past this; the agent is told to narrow it.
const RESULT_LIMIT: usize = 24_000;
const HAND_OFF: &str = "hand_off";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Scout,
    Architect,
    Builder,
    Tester,
    Fixer,
    Reviewer,
}

impl Role {
    pub const ALL: [Role; 6] = [
        Role::Scout,
        Role::Architect,
        Role::Builder,
        Role::Tester,
        Role::Fixer,
        Role::Reviewer,
    ];

    pub fn id(self) -> &'static str {
        match self {
            Role::Scout => "scout",
            Role::Architect => "architect",
            Role::Builder => "builder",
            Role::Tester => "tester",
            Role::Fixer => "fixer",
            Role::Reviewer => "reviewer",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Role::Scout => "Scout",
            Role::Architect => "Architect",
            Role::Builder => "Builder",
            Role::Tester => "Tester",
            Role::Fixer => "Fixer",
            Role::Reviewer => "Reviewer",
        }
    }

    /// The Duckle tools this role may call, besides hand_off.
    fn tools(self) -> &'static [&'static str] {
        match self {
            Role::Scout => &[
                "list_workspace_files",
                "inspect_source",
                "list_connections",
                "list_pipelines",
                "read_pipeline",
            ],
            Role::Architect => &[
                "list_components",
                "component_capabilities",
                "get_component_schema",
                "inspect_source",
            ],
            Role::Builder => &[
                "get_component_schema",
                "list_components",
                "create_pipeline",
                "update_pipeline",
                "validate_pipeline",
                "check_node_sql",
                "read_pipeline",
            ],
            Role::Tester => &[
                "run_pipeline",
                "inspect_source",
                "read_pipeline",
                "read_run_logs",
                "verify_pipeline",
            ],
            Role::Fixer => &[
                "read_pipeline",
                "get_component_schema",
                "update_pipeline",
                "validate_pipeline",
                "check_node_sql",
                "run_pipeline",
                "inspect_source",
                "read_run_logs",
            ],
            Role::Reviewer => &[
                "read_pipeline",
                "verify_pipeline",
                "pipeline_lineage",
                "trust_report",
                "inspect_source",
            ],
        }
    }

    fn judges(self) -> bool {
        matches!(self, Role::Tester | Role::Reviewer)
    }

    fn brief(self) -> &'static str {
        match self {
            Role::Scout => {
                "Find the data the goal needs and describe it exactly. Start with list_workspace_files, \
                 then inspect each file the goal could use with inspect_source. For every file you \
                 report, give its ${workspace} path, its format, each column with its type, a sample \
                 value or two, and what a builder must watch for: nulls, date formats, numbers stored \
                 as text, duplicate keys. If the data the goal names is not there, say so plainly."
            }
            Role::Architect => {
                "Design the pipeline from the Scout's findings. Find components with list_components \
                 and read the schema of every one you use with get_component_schema; use only the \
                 property keys and ports it lists. Then hand off:\n\
                 1. The nodes in flow order, each with an id, its component id, a short label, and \
                 every property with its exact key and value.\n\
                 2. The edges, naming the port each one leaves from and arrives at.\n\
                 3. Two to four quality gates built into the pipeline, so the proof runs every time \
                 it does: for example qa.unique or qa.notnull on the output key, qa.expect or \
                 qa.assert for a rule the result must meet, qa.reconcile to prove a total matches \
                 the input. Set each to fail the run when it does not hold.\n\
                 Do not write files."
            }
            Role::Builder => {
                "Build the Architect's design with create_pipeline, named for the goal in a few words. \
                 Lay it out left to right in flow order: x = 80, 380, 680 and so on, y = 200; a \
                 second input sits above at y = 40, and a gate that branches off sits below at \
                 y = 360. When create_pipeline reports an error, correct the design and call it \
                 again. Finish with validate_pipeline passing, then hand off what you built and \
                 anything you changed from the design, and why."
            }
            Role::Tester => {
                "Prove the pipeline works. Run it with run_pipeline. If the run fails, report the \
                 failing node and its error. If it succeeds, check every quality gate's result and \
                 the row counts, then read the output file it wrote with inspect_source and confirm \
                 it holds what the goal asked for. Hand off with verdict \"pass\" only if the run \
                 succeeded, every gate held and the output is right. Otherwise give verdict \"fail\" \
                 and list each failure: the check, what was expected, what came back, and the node \
                 most likely at fault."
            }
            Role::Fixer => {
                "The Tester reported failures. Read the pipeline, find each cause, and fix it with the \
                 smallest update_pipeline patch (nodes merge by id, so one property can change \
                 alone). Use check_node_sql on SQL, and run_pipeline with target to look at one \
                 node's rows. Do not weaken or remove a quality gate to make it pass: fix the data \
                 flow it guards. Hand off exactly what you changed and why."
            }
            Role::Reviewer => {
                "Review the finished pipeline for the person who set the goal. Use verify_pipeline \
                 and pipeline_lineage, and trust_report for anything that looks like personal data. \
                 Hand off a short verdict addressed to them: what the pipeline does, where its \
                 output is, the evidence that it works (rows, gates that held), and any risk worth \
                 knowing. Verdict \"pass\" if it meets the goal."
            }
        }
    }
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

/// The model a request asks for, and how to name it in the swarm view.
pub fn model_for(p: &Provider) -> Result<(Box<dyn Model>, String), String> {
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

/// A tool call the swarm makes: (answer text, is an error).
pub type ToolFn<'a> = dyn FnMut(&str, Value) -> (String, bool) + 'a;

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

struct Ctx {
    workspace: PathBuf,
    duckdb: Option<String>,
    pipeline_id: String,
    pipeline_file: PathBuf,
    catalog: Vec<Value>,
    tool_calls: u64,
    input_tokens: u64,
    output_tokens: u64,
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
    model: &mut dyn Model,
    label: &str,
    tools: &mut ToolFn,
    emit: &mut dyn FnMut(Value),
    cancel: &AtomicBool,
) -> Value {
    let started = Instant::now();
    let workspace = PathBuf::from(req.workspace.trim());
    if req.goal.trim().is_empty() || !workspace.is_dir() {
        let message = if req.goal.trim().is_empty() {
            "Describe what the pipeline should do.".to_string()
        } else {
            format!("No workspace folder at {}.", workspace.display())
        };
        emit(json!({ "type": "error", "message": message }));
        let done = json!({ "type": "finished", "ok": false, "summary": message });
        emit(done.clone());
        return done;
    }
    let pipelines = workspace.join("pipelines");
    let pipeline_id = pipeline_id_for(&req.goal, &pipelines);
    let mut ctx = Ctx {
        pipeline_file: pipelines.join(format!("{pipeline_id}.json")),
        workspace,
        duckdb: clean(&req.duckdb),
        pipeline_id,
        catalog: crate::tools::list_tools()
            .as_array()
            .cloned()
            .unwrap_or_default(),
        tool_calls: 0,
        input_tokens: 0,
        output_tokens: 0,
    };
    emit(json!({
        "type": "started",
        "goal": req.goal.trim(),
        "pipelineId": ctx.pipeline_id,
        "model": label,
        "roles": Role::ALL.iter().map(|r| r.id()).collect::<Vec<_>>(),
    }));

    let mut notes: Vec<(Role, String)> = Vec::new();
    let mut previous: Option<Role> = None;
    let mut failure: Option<String> = None;
    let mut tested = false;
    let mut review: Option<AgentOut> = None;

    let mut step = |role: Role,
                    ctx: &mut Ctx,
                    notes: &mut Vec<(Role, String)>,
                    previous: &mut Option<Role>,
                    tools: &mut ToolFn,
                    emit: &mut dyn FnMut(Value)|
     -> Result<AgentOut, String> {
        if let Some(from) = previous.replace(role) {
            emit(json!({ "type": "handoff", "from": from.id(), "to": role.id() }));
        }
        let out = agent(role, &req.goal, ctx, notes, model, tools, emit, cancel)?;
        notes.push((role, out.summary.clone()));
        Ok(AgentOut {
            summary: out.summary,
            verdict: out.verdict,
        })
    };

    for role in [Role::Scout, Role::Architect, Role::Builder] {
        if let Err(e) = step(role, &mut ctx, &mut notes, &mut previous, tools, emit) {
            failure = Some(e);
            break;
        }
    }
    if failure.is_none() && !ctx.pipeline_file.is_file() {
        failure = Some("The Builder finished without writing the pipeline.".into());
    }
    if failure.is_none() {
        let mut rounds = 0;
        loop {
            match step(
                Role::Tester,
                &mut ctx,
                &mut notes,
                &mut previous,
                tools,
                emit,
            ) {
                Ok(out) => tested = out.verdict == Some(true),
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
            if tested || rounds == FIX_ROUNDS {
                break;
            }
            rounds += 1;
            if let Err(e) = step(
                Role::Fixer,
                &mut ctx,
                &mut notes,
                &mut previous,
                tools,
                emit,
            ) {
                failure = Some(e);
                break;
            }
        }
    }
    if failure.is_none() {
        match step(
            Role::Reviewer,
            &mut ctx,
            &mut notes,
            &mut previous,
            tools,
            emit,
        ) {
            Ok(out) => review = Some(out),
            Err(e) => failure = Some(e),
        }
    }

    let ok = failure.is_none() && tested;
    let summary = match (&failure, &review) {
        (Some(e), _) => e.clone(),
        (None, Some(r)) => r.summary.clone(),
        (None, None) => String::new(),
    };
    let done = json!({
        "type": "finished",
        "ok": ok,
        "tested": tested,
        "stopped": failure.as_deref() == Some(llm::CANCELLED),
        "summary": summary,
        "pipelineId": ctx.pipeline_id,
        "pipelineWritten": ctx.pipeline_file.is_file(),
        "durationMs": started.elapsed().as_millis() as u64,
        "toolCalls": ctx.tool_calls,
        "inputTokens": ctx.input_tokens,
        "outputTokens": ctx.output_tokens,
    });
    emit(done.clone());
    done
}

#[allow(clippy::too_many_arguments)]
fn agent(
    role: Role,
    goal: &str,
    ctx: &mut Ctx,
    notes: &[(Role, String)],
    model: &mut dyn Model,
    tools: &mut ToolFn,
    emit: &mut dyn FnMut(Value),
    cancel: &AtomicBool,
) -> Result<AgentOut, String> {
    emit(json!({ "type": "agent", "role": role.id(), "state": "working" }));
    let out = work(role, goal, ctx, notes, model, tools, emit, cancel);
    match &out {
        Ok(o) => emit(json!({
            "type": "agent",
            "role": role.id(),
            "state": "done",
            "summary": o.summary,
            "verdict": o.verdict.map(|pass| if pass { "pass" } else { "fail" }),
        })),
        Err(e) => {
            emit(json!({ "type": "agent", "role": role.id(), "state": "failed", "summary": e }))
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn work(
    role: Role,
    goal: &str,
    ctx: &mut Ctx,
    notes: &[(Role, String)],
    model: &mut dyn Model,
    tools: &mut ToolFn,
    emit: &mut dyn FnMut(Value),
    cancel: &AtomicBool,
) -> Result<AgentOut, String> {
    let system = system_prompt(role, &ctx.pipeline_id);
    let specs = specs_for(role, &ctx.catalog);
    let mut history = vec![llm::user_message(&kickoff(goal, notes))];
    let mut last_text = String::new();
    for _ in 0..MAX_TURNS {
        if cancel.load(Ordering::Relaxed) {
            return Err(llm::CANCELLED.to_string());
        }
        let turn = {
            let mut on = |d: Delta| {
                let (kind, key, text) = match d {
                    Delta::Text(t) => ("text", "text", t),
                    Delta::Thinking(t) => ("thinking", "text", t),
                    Delta::Retry(why) => ("retry", "reason", why),
                };
                emit(json!({ "type": kind, "role": role.id(), key: text }));
            };
            model.turn(&system, &specs, &mut history, &mut on, cancel)?
        };
        ctx.input_tokens += turn.input_tokens;
        ctx.output_tokens += turn.output_tokens;
        emit(json!({
            "type": "usage",
            "role": role.id(),
            "inputTokens": ctx.input_tokens,
            "outputTokens": ctx.output_tokens,
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
                history.pop();
                history.push(llm::user_message(
                    "Your last reply was cut off at the output limit. Take a smaller step: fewer \
                     nodes per call, or change one part with update_pipeline.",
                ));
                continue;
            }
            Stop::Done | Stop::Tools => {}
        }
        if turn.calls.is_empty() {
            // Finished without handing off: what it said is its hand-off.
            return Ok(AgentOut {
                summary: last_text,
                verdict: None,
            });
        }
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
                Ok(_) if !role.tools().contains(&call.name.as_str()) => {
                    (format!("{} is not one of your tools.", call.name), true)
                }
                Ok(input) => {
                    if cancel.load(Ordering::Relaxed) {
                        return Err(llm::CANCELLED.to_string());
                    }
                    run_tool(role, &call.id, &call.name, input, ctx, tools, emit)
                }
            };
            outputs.push(ToolOutput {
                id: call.id.clone(),
                text: clip(&text),
                is_error,
            });
        }
        if let Some(out) = handed {
            if role.judges() && out.verdict.is_none() {
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
        "The {} used all {MAX_TURNS} turns without finishing.",
        role.title()
    ))
}

fn run_tool(
    role: Role,
    id: &str,
    name: &str,
    input: &Value,
    ctx: &mut Ctx,
    tools: &mut ToolFn,
    emit: &mut dyn FnMut(Value),
) -> (String, bool) {
    let args = scoped_args(name, input.clone(), ctx);
    ctx.tool_calls += 1;
    // What the agent asked for, not the paths filled in for it: the view (and
    // a screen recording of it) should not show the folder layout.
    emit(json!({ "type": "tool", "role": role.id(), "id": id, "name": name, "input": input }));
    let started = Instant::now();
    let (text, is_error) = tools(name, args);
    emit(json!({
        "type": "toolResult",
        "role": role.id(),
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
            emit(json!({ "type": "pipeline", "id": ctx.pipeline_id, "name": title, "doc": doc }));
        }
    }
    if name == "run_pipeline" {
        let result: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        if is_error || !result.is_object() {
            emit(json!({ "type": "run", "status": "error", "error": preview(&text) }));
        } else {
            emit(json!({
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

fn specs_for(role: Role, catalog: &[Value]) -> Vec<ToolSpec> {
    let mut specs: Vec<ToolSpec> = role
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
    if role.judges() {
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

fn system_prompt(role: Role, pipeline_id: &str) -> String {
    let format = match role {
        Role::Architect | Role::Builder | Role::Fixer => {
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
    format!(
        "You are the {title} in Duckle's agent swarm. Six specialists, the Scout, Architect, Builder, \
         Tester, Fixer and Reviewer, turn one goal into a tested data pipeline, each handing its \
         findings to the next. Duckle pipelines are JSON graphs of components that compile to DuckDB \
         SQL.\n\n\
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
         Your part, as the {title}:\n{brief}{format}",
        title = role.title(),
        brief = role.brief(),
    )
}

fn kickoff(goal: &str, notes: &[(Role, String)]) -> String {
    let mut text = format!("Goal: {}\n", goal.trim());
    if !notes.is_empty() {
        text.push_str("\nNotes from the agents before you, oldest first:\n");
        for (role, note) in notes {
            text.push_str(&format!("\n[{}]\n{}\n", role.title(), note));
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
    let emit_line = |v: Value| {
        let mut out = stdout.lock();
        let _ = writeln!(out, "{v}");
        let _ = out.flush();
    };
    let req: Request = match serde_json::from_str(&line) {
        Ok(r) => r,
        Err(e) => {
            emit_line(json!({ "type": "error", "message": format!("bad request: {e}") }));
            emit_line(
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
    let (mut model, label) = match model_for(&req.provider) {
        Ok(m) => m,
        Err(message) => {
            emit_line(json!({ "type": "error", "message": message }));
            emit_line(json!({ "type": "finished", "ok": false, "summary": message }));
            return std::process::ExitCode::from(1);
        }
    };
    let mut tools = call_tool;
    let mut emit = emit_line;
    let done = run(&req, model.as_mut(), &label, &mut tools, &mut emit, &cancel);
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
    use std::collections::VecDeque;

    /// A model that plays back a script: each turn is (role, text, calls).
    /// It records the system prompt, tools and conversation it was given.
    struct Scripted {
        turns: VecDeque<(String, Vec<(String, Value)>)>,
        seen: Vec<(String, Vec<String>, Vec<Value>)>,
    }

    impl Scripted {
        fn new(turns: Vec<(&str, Vec<(&str, Value)>)>) -> Self {
            Scripted {
                turns: turns
                    .into_iter()
                    .map(|(t, calls)| {
                        (
                            t.to_string(),
                            calls.into_iter().map(|(n, a)| (n.to_string(), a)).collect(),
                        )
                    })
                    .collect(),
                seen: Vec::new(),
            }
        }
    }

    impl Model for Scripted {
        fn turn(
            &mut self,
            system: &str,
            tools: &[ToolSpec],
            history: &mut Vec<Value>,
            on: &mut dyn FnMut(Delta),
            _cancel: &AtomicBool,
        ) -> Result<Turn, String> {
            self.seen.push((
                system.to_string(),
                tools.iter().map(|t| t.name.clone()).collect(),
                history.clone(),
            ));
            let (text, calls) = self.turns.pop_front().ok_or("the script ran out")?;
            on(Delta::Text(&text));
            let calls: Vec<ToolCall> = calls
                .into_iter()
                .enumerate()
                .map(|(i, (name, input))| ToolCall {
                    id: format!("c{}_{i}", self.seen.len()),
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

    fn request(ws: &std::path::Path) -> Request {
        Request {
            goal: "Total revenue per customer from orders.csv".into(),
            workspace: ws.to_string_lossy().into(),
            duckdb: Some("/opt/duckdb".into()),
            provider: Provider::default(),
        }
    }

    /// Runs the swarm with a scripted model and stub tools. The stub writes the
    /// pipeline file when create_pipeline is called, as the real tool does, and
    /// records every call with the arguments it really received.
    fn play(
        ws: &std::path::Path,
        model: &mut Scripted,
        answer: impl Fn(&str, &Value) -> (String, bool),
    ) -> (Value, Vec<Value>, Vec<(String, Value)>) {
        let req = request(ws);
        let mut events = Vec::new();
        let mut calls = Vec::new();
        let file = ws.join("pipelines");
        let mut tools = |name: &str, args: Value| {
            calls.push((name.to_string(), args.clone()));
            if name == "create_pipeline" {
                let id = args["id"].as_str().unwrap().to_string();
                std::fs::write(
                    file.join(format!("{id}.json")),
                    args["pipeline"].to_string(),
                )
                .unwrap();
            }
            answer(name, &args)
        };
        let done = run(
            &req,
            model,
            "test model",
            &mut tools,
            &mut |e| events.push(e),
            &AtomicBool::new(false),
        );
        (done, events, calls)
    }

    fn pipeline() -> Value {
        json!({ "name": "Revenue", "nodes": [{ "id": "n1" }], "edges": [] })
    }

    #[test]
    fn the_swarm_runs_each_specialist_in_turn_and_reports_a_passing_pipeline() {
        let ws = workspace();
        let mut model = Scripted::new(vec![
            ("Looking.", vec![("list_workspace_files", json!({}))]),
            ("", vec![hand("orders.csv: customer_id, amount")]),
            (
                "",
                vec![hand("src.csv -> xf.groupby -> qa.unique -> snk.parquet")],
            ),
            (
                "Building.",
                vec![(
                    "create_pipeline",
                    json!({ "name": "Revenue", "pipeline": pipeline() }),
                )],
            ),
            ("", vec![hand("built as designed")]),
            ("", vec![("run_pipeline", json!({}))]),
            ("", vec![verdict("ran, 3 gates held", "pass")]),
            (
                "",
                vec![verdict(
                    "Writes output/revenue.parquet; 412 customers.",
                    "pass",
                )],
            ),
        ]);
        let (done, events, calls) = play(&ws, &mut model, |name, _| match name {
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
        let order: Vec<&str> = events
            .iter()
            .filter(|e| e["type"] == "agent" && e["state"] == "working")
            .map(|e| e["role"].as_str().unwrap())
            .collect();
        assert_eq!(
            order,
            ["scout", "architect", "builder", "tester", "reviewer"],
            "no Fixer when the Tester passes"
        );

        // Each agent starts from the notes of the ones before it.
        let tester_prompt = &model.seen[5].2[0]["content"];
        assert!(
            tester_prompt
                .as_str()
                .unwrap()
                .contains("[Architect]\nsrc.csv -> xf.groupby"),
            "{tester_prompt}"
        );
        // ...and sees only its own tools, plus hand_off.
        assert_eq!(
            model.seen[5].1,
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
    fn a_failing_test_sends_the_fixer_in_and_the_tester_back() {
        let ws = workspace();
        let mut model = Scripted::new(vec![
            ("", vec![hand("data")]),
            ("", vec![hand("design")]),
            (
                "",
                vec![(
                    "create_pipeline",
                    json!({ "name": "R", "pipeline": pipeline() }),
                )],
            ),
            ("", vec![hand("built")]),
            (
                "",
                vec![verdict("qa.unique failed: 3 duplicate customer_id", "fail")],
            ),
            (
                "",
                vec![(
                    "update_pipeline",
                    json!({ "patch": { "nodes": [{ "id": "n2" }] } }),
                )],
            ),
            ("", vec![hand("grouped by customer_id before the gate")]),
            ("", vec![verdict("all gates hold", "pass")]),
            ("", vec![verdict("good", "pass")]),
        ]);
        let (done, events, _) = play(&ws, &mut model, |_, _| ("{}".into(), false));
        assert_eq!(done["ok"], json!(true));
        let order: Vec<&str> = events
            .iter()
            .filter(|e| e["type"] == "agent" && e["state"] == "working")
            .map(|e| e["role"].as_str().unwrap())
            .collect();
        assert_eq!(
            order,
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
        let handoffs: Vec<(String, String)> = events
            .iter()
            .filter(|e| e["type"] == "handoff")
            .map(|e| {
                (
                    e["from"].as_str().unwrap().into(),
                    e["to"].as_str().unwrap().into(),
                )
            })
            .collect();
        assert!(handoffs.contains(&("fixer".into(), "tester".into())));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_pipeline_that_never_passes_is_reported_as_failing_after_two_repairs() {
        let ws = workspace();
        let fail = || verdict("still wrong", "fail");
        let mut model = Scripted::new(vec![
            ("", vec![hand("data")]),
            ("", vec![hand("design")]),
            (
                "",
                vec![(
                    "create_pipeline",
                    json!({ "name": "R", "pipeline": pipeline() }),
                )],
            ),
            ("", vec![hand("built")]),
            ("", vec![fail()]),
            ("", vec![hand("fix 1")]),
            ("", vec![fail()]),
            ("", vec![hand("fix 2")]),
            ("", vec![fail()]),
            ("", vec![verdict("does not meet the goal", "fail")]),
        ]);
        let (done, events, _) = play(&ws, &mut model, |_, _| ("{}".into(), false));
        assert_eq!(done["ok"], json!(false));
        assert_eq!(done["tested"], json!(false));
        let fixers = events
            .iter()
            .filter(|e| e["type"] == "agent" && e["role"] == "fixer" && e["state"] == "working")
            .count();
        assert_eq!(fixers, FIX_ROUNDS);
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn tools_act_on_the_swarms_own_pipeline_whatever_the_agent_names() {
        let ws = workspace();
        let mut model = Scripted::new(vec![
            (
                "",
                vec![
                    ("read_pipeline", json!({ "path": "elsewhere.json" })),
                    hand("data"),
                ],
            ),
            ("", vec![hand("design")]),
            (
                "",
                vec![(
                    "create_pipeline",
                    json!({ "name": "R", "id": "payroll", "directory": "C:/Windows", "workspace": "D:/other", "pipeline": pipeline() }),
                )],
            ),
            (
                "",
                vec![(
                    "update_pipeline",
                    json!({ "path": "D:/other/pipelines/payroll.json", "patch": {} }),
                )],
            ),
            ("", vec![hand("built")]),
            (
                "",
                vec![(
                    "run_pipeline",
                    json!({ "pipeline": { "nodes": [] }, "duckdb": "evil.exe" }),
                )],
            ),
            ("", vec![verdict("ok", "pass")]),
            ("", vec![verdict("ok", "pass")]),
        ]);
        let (_, events, calls) = play(&ws, &mut model, |_, _| ("{}".into(), false));
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

        // The Scout may read another pipeline for reference: that is a read.
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
    fn an_agent_cannot_call_a_tool_outside_its_role() {
        let ws = workspace();
        let mut model = Scripted::new(vec![
            ("", vec![("run_pipeline", json!({}))]),
            ("", vec![hand("data")]),
            ("", vec![hand("design")]),
            ("", vec![hand("forgot to build")]),
        ]);
        let (done, _, calls) = play(&ws, &mut model, |_, _| ("{}".into(), false));
        assert!(
            calls.is_empty(),
            "the Scout may not run pipelines: {calls:?}"
        );
        // The refusal went back to the model as an error it can read.
        let reply = model.seen[1].2.last().unwrap();
        assert_eq!(reply["error"], json!(true));
        assert!(reply["text"]
            .as_str()
            .unwrap()
            .contains("not one of your tools"));
        assert_eq!(done["ok"], json!(false));
        assert_eq!(
            done["summary"],
            json!("The Builder finished without writing the pipeline.")
        );
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_judge_that_hands_off_without_a_verdict_has_not_passed() {
        let ws = workspace();
        let mut model = Scripted::new(vec![
            ("", vec![hand("data")]),
            ("", vec![hand("design")]),
            (
                "",
                vec![(
                    "create_pipeline",
                    json!({ "name": "R", "pipeline": pipeline() }),
                )],
            ),
            ("", vec![hand("built")]),
            ("", vec![hand("looks fine")]),
            ("", vec![hand("fixed nothing")]),
            ("", vec![verdict("ok", "pass")]),
            ("", vec![verdict("ok", "pass")]),
        ]);
        let (done, events, _) = play(&ws, &mut model, |_, _| ("{}".into(), false));
        assert_eq!(done["ok"], json!(true));
        let first_test = events
            .iter()
            .find(|e| e["type"] == "agent" && e["role"] == "tester" && e["state"] == "done")
            .unwrap();
        assert_eq!(first_test["verdict"], json!("fail"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_turn_cut_off_at_the_output_limit_runs_none_of_its_calls_and_is_asked_again() {
        let ws = workspace();
        let mut model = Scripted::new(vec![
            ("<cut>", vec![("list_workspace_files", json!({}))]),
            ("", vec![hand("data")]),
            ("", vec![hand("design")]),
            ("", vec![hand("no build")]),
        ]);
        let (_, _, calls) = play(&ws, &mut model, |_, _| ("{}".into(), false));
        assert!(calls.is_empty(), "a cut-off call ran: {calls:?}");
        // The cut-off turn is gone from the conversation, and the agent is told why.
        let retried = &model.seen[1].2;
        assert_eq!(retried.len(), 2, "{retried:?}");
        assert!(retried[1]["content"]
            .as_str()
            .unwrap()
            .contains("cut off at the output limit"));
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn a_declined_turn_fails_the_agent_and_ends_the_run() {
        let ws = workspace();
        let mut model = Scripted::new(vec![("<declined>", vec![])]);
        let (done, events, _) = play(&ws, &mut model, |_, _| ("{}".into(), false));
        assert_eq!(done["ok"], json!(false));
        assert_eq!(
            done["summary"],
            json!("The model declined to go on (cyber).")
        );
        assert!(events
            .iter()
            .any(|e| e["type"] == "agent" && e["role"] == "scout" && e["state"] == "failed"));
        assert_eq!(model.seen.len(), 1, "nothing ran after the decline");
        let _ = std::fs::remove_dir_all(ws);
    }

    #[test]
    fn stopping_ends_the_run_before_the_next_step() {
        let ws = workspace();
        let mut model = Scripted::new(vec![("", vec![hand("data")])]);
        let req = request(&ws);
        let mut events = Vec::new();
        let cancel = AtomicBool::new(true);
        let mut tools = |_: &str, _: Value| ("{}".to_string(), false);
        let done = run(
            &req,
            &mut model,
            "m",
            &mut tools,
            &mut |e| events.push(e),
            &cancel,
        );
        assert_eq!(done["ok"], json!(false));
        assert_eq!(done["stopped"], json!(true));
        assert!(model.seen.is_empty(), "no model call after a stop");
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
        let specs = specs_for(Role::Builder, &catalog);
        let create = specs.iter().find(|s| s.name == "create_pipeline").unwrap();
        let props = create.schema["properties"].as_object().unwrap();
        assert!(props.contains_key("pipeline") && props.contains_key("name"));
        for gone in ["workspace", "directory", "id", "overwrite"] {
            assert!(!props.contains_key(gone), "{gone} offered");
        }
        // Every tool a role names exists in the catalog.
        for role in Role::ALL {
            let specs = specs_for(role, &catalog);
            assert_eq!(
                specs.len(),
                role.tools().len() + 1,
                "{role:?} names a tool that does not exist"
            );
        }
    }
}
