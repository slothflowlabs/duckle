//! Plans: several pipelines, in an order somebody chose.
//!
//! A schedule runs one pipeline on a clock. A plan runs many, in steps: everything inside
//! a step goes at once, and the next step waits for the one before it to finish. That is
//! the shape most nightly loads already have, written down instead of being three schedules
//! set a few minutes apart and hoped over.
//!
//! The engine can already do this with `ctl.runpipeline` and `ctl.parallelize`, and a plan
//! could have compiled to those. It does not, for one reason: a compiled plan is a single
//! run, and what an operator wants when a nightly load fails at 3am is to see *which*
//! pipeline failed, in the run history, next to every other run. So a plan drives the
//! ordinary run path once per pipeline, and what it adds is the ordering and a record of
//! the whole attempt.
//!
//! Execution takes the runner as a closure. Deciding what runs next is the part worth
//! testing, and it should be testable without a DuckDB binary, a workspace or a clock.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::runlock;

pub fn plans_path(workspace: &Path) -> PathBuf {
    workspace.join("plans.json")
}

/// One group of pipelines that may run at the same time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Step {
    /// What this stage of the plan is for, in the operator's words.
    #[serde(default)]
    pub name: String,
    /// Pipeline files, relative to the workspace. They have no order between them: that
    /// is what putting them in the same step means.
    pub pipelines: Vec<String>,
    /// This step is allowed to fail without stopping the plan.
    ///
    /// `stopOnFailure` is a property of the whole plan, but a real sequence mixes the two:
    /// the load must stop the run, while writing an audit row or sorting yesterday's files
    /// should not. Without a per-step say, such a plan has to choose between abandoning the
    /// run on a housekeeping step and carrying on past a failed load.
    ///
    /// Absent means "follow the plan". Setting it does not make the step's failure
    /// invisible: the step is still recorded as failed and the plan still ends up failed.
    /// It only decides whether the steps after it run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continue_on_failure: Option<bool>,
    /// #317: parameter values for each pipeline in this step, keyed the way
    /// `pipelines` names it. Each pipeline's own contract checks its own values,
    /// so two pipelines with different parameters can share a step - which one
    /// plan-wide set could not do, since a declared contract refuses a name it
    /// does not declare.
    ///
    /// Absent means "this save did not say": the values already stored for the
    /// step's pipelines are kept (see [`keep_unmentioned_values`]). Empty clears.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<BTreeMap<String, BTreeMap<String, String>>>,
}

impl Step {
    /// The values this step gives `pipeline`, however either of them spelled it.
    pub fn values_for(&self, pipeline: &str) -> BTreeMap<String, String> {
        let Some(params) = &self.params else {
            return BTreeMap::new();
        };
        if let Some(values) = params.get(pipeline) {
            return values.clone();
        }
        let id = step_pipeline_id(pipeline);
        params
            .iter()
            .find(|(named, _)| step_pipeline_id(named) == id)
            .map(|(_, values)| values.clone())
            .unwrap_or_default()
    }
}

/// Several pipelines, in an order somebody chose.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub id: String,
    #[serde(default)]
    pub name: String,
    pub steps: Vec<Step>,
    /// Whether a failed pipeline stops the plan.
    ///
    /// Defaults to stopping. A plan is an order, and carrying on past a failed step means
    /// running the next one against data the failed one was supposed to produce.
    #[serde(default = "default_true")]
    pub stop_on_failure: bool,
}

fn default_true() -> bool {
    true
}

impl Plan {
    /// Why this plan could not run, if it could not.
    ///
    /// Checked before saving as well as before running, so a plan that cannot work is
    /// refused at the point somebody wrote it rather than at 3am.
    pub fn problems(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.id.trim().is_empty() {
            out.push("a plan needs an id".into());
        }
        if self.steps.is_empty() {
            out.push("a plan needs at least one step".into());
        }
        for (i, step) in self.steps.iter().enumerate() {
            if step.pipelines.is_empty() {
                out.push(format!("step {} has no pipelines in it", i + 1));
            }
            for p in &step.pipelines {
                if p.trim().is_empty() {
                    out.push(format!("step {} names an empty pipeline", i + 1));
                }
            }
        }
        // The same pipeline twice in one step would run it twice at once, against itself.
        for (i, step) in self.steps.iter().enumerate() {
            let mut seen = std::collections::HashSet::new();
            for p in &step.pipelines {
                if !seen.insert(p) {
                    out.push(format!("step {} runs {} twice at the same time", i + 1, p));
                }
            }
        }
        // #317: values for a pipeline the step does not run would never be used -
        // a typo, or a pipeline taken out and its values left behind.
        for (i, step) in self.steps.iter().enumerate() {
            for named in step.params.iter().flat_map(|p| p.keys()) {
                let id = step_pipeline_id(named);
                if !step.pipelines.iter().any(|p| step_pipeline_id(p) == id) {
                    out.push(format!(
                        "step {} has parameter values for {}, which it does not run",
                        i + 1,
                        named
                    ));
                }
            }
        }
        out
    }
}

/// #317: step values the pipelines' own contracts would refuse, found when the
/// plan is saved rather than when a scheduled run fails in the night.
///
/// The same check a run makes, on the same boundary - except a missing required
/// value, which a run started by hand can still supply. Reads only the pipelines
/// that are given values.
pub fn contract_problems(workspace: &Path, plan: &Plan) -> Vec<String> {
    let mut out = Vec::new();
    for (i, step) in plan.steps.iter().enumerate() {
        for (named, values) in step.params.iter().flatten() {
            if values.is_empty() {
                continue;
            }
            let file = workspace.join(step_pipeline_path(workspace, named));
            let doc = std::fs::read_to_string(&file)
                .map_err(|e| e.to_string())
                .and_then(|text| {
                    serde_json::from_str::<crate::PipelineDoc>(crate::format::strip_bom(&text))
                        .map_err(|e| e.to_string())
                });
            let doc = match doc {
                Ok(doc) => doc,
                Err(e) => {
                    out.push(format!(
                        "step {}: cannot read {} to check its values: {}",
                        i + 1,
                        named,
                        e
                    ));
                    continue;
                }
            };
            let values: std::collections::HashMap<String, String> =
                values.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            if let Err(errors) = crate::context::validate_params(&doc, &values) {
                for e in errors.into_iter().filter(|e| e.code != "param:missing") {
                    out.push(format!("step {}, {}: {}", i + 1, named, e.message));
                }
            }
        }
    }
    out
}

/// #317: a step that says nothing about values keeps the ones stored for its
/// pipelines, however the stored plan grouped them.
///
/// The console's plan form does not show parameter values, and saving it must
/// not wipe what the desktop editor or an API client bound. An empty set still
/// clears, and a pipeline taken out of the step takes its values with it.
pub fn carry_values(plan: &mut Plan, stored: Option<&Plan>) {
    let Some(stored) = stored else { return };
    let mut by_pipeline: BTreeMap<&str, &BTreeMap<String, String>> = BTreeMap::new();
    for (named, values) in stored.steps.iter().flat_map(|s| s.params.iter().flatten()) {
        by_pipeline.entry(step_pipeline_id(named)).or_insert(values);
    }
    for step in plan.steps.iter_mut().filter(|s| s.params.is_none()) {
        let carried: BTreeMap<String, BTreeMap<String, String>> = step
            .pipelines
            .iter()
            .filter_map(|p| by_pipeline.get(step_pipeline_id(p)).map(|v| (p.clone(), (*v).clone())))
            .collect();
        if !carried.is_empty() {
            step.params = Some(carried);
        }
    }
}

/// Everything a save does before a plan is written, in one place so the desktop
/// app, the console's form and the web editor cannot disagree (#317): the
/// structural check, the stored values kept for a form that does not show them,
/// and those values against each pipeline's own contract.
pub fn prepare_for_save(workspace: &Path, plan: &mut Plan) -> Result<(), String> {
    let problems = plan.problems();
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    keep_unmentioned_values(workspace, plan)?;
    let problems = contract_problems(workspace, plan);
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    Ok(())
}

/// [`carry_values`] against the plan as it is stored now.
pub fn keep_unmentioned_values(workspace: &Path, plan: &mut Plan) -> Result<(), String> {
    if plan.steps.iter().all(|s| s.params.is_some()) {
        return Ok(());
    }
    let stored = load(workspace)?.into_iter().find(|p| p.id == plan.id);
    carry_values(plan, stored.as_ref());
    Ok(())
}

/// The bare pipeline id a plan step names, however the step was spelled.
///
/// One `plans.json` is read by two products that identify a pipeline differently. The
/// console works in workspace-relative files (`pipelines/orders.json`), because that is what
/// its run API takes. The desktop app and the engine work in bare ids (`orders`), because
/// that is what [`crate::context::resolve_workspace`] takes - it builds
/// `<workspace>/pipelines/<id>.json` itself.
///
/// Neither spelling is wrong, but a reader that understands only one turns a plan authored
/// in the other product into a plan that fails on every step. So both readers normalise
/// here, and both writers emit [`step_pipeline_file`]: tolerant readers, consistent writers.
pub fn step_pipeline_id(step: &str) -> &str {
    let s = step.trim();
    let s = s
        .strip_prefix("pipelines/")
        .or_else(|| s.strip_prefix("pipelines\\"))
        .unwrap_or(s);
    s.strip_suffix(".json").unwrap_or(s)
}

/// The workspace-relative file a plan step names, however the step was spelled.
///
/// Derived from [`step_pipeline_id`] so the two can never disagree about what a step means.
pub fn step_pipeline_file(step: &str) -> String {
    format!("pipelines/{}.json", step_pipeline_id(step))
}

/// The workspace-relative file a plan step runs, in this workspace.
///
/// A step names a file the console's plan form offered (`report.json`,
/// `pipelines/report.json`) or a bare id the desktop editor wrote (`report`, meaning
/// `pipelines/report.json`). Sending every spelling to `pipelines/` sent a step naming a
/// pipeline anywhere else - which is where a deploy puts it - to a file that does not
/// exist. A named file inside the workspace is run as named; anything else keeps the
/// [`step_pipeline_file`] meaning.
pub fn step_pipeline_path(workspace: &Path, step: &str) -> String {
    let named = step.trim().replace('\\', "/");
    let inside = Path::new(&named).components().all(|c| matches!(c, std::path::Component::Normal(_)));
    if inside && named.ends_with(".json") && workspace.join(&named).is_file() {
        return named;
    }
    step_pipeline_file(step)
}

/// What became of one pipeline in a plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PipelineOutcome {
    pub pipeline: String,
    /// "ok", "failed", or "skipped" when an earlier step stopped the plan.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// What became of one step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StepOutcome {
    pub name: String,
    pub pipelines: Vec<PipelineOutcome>,
}

/// What became of one attempt at a plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PlanRun {
    pub plan_id: String,
    /// "ok" when everything ran, "failed" when anything did.
    pub status: String,
    pub steps: Vec<StepOutcome>,
}

impl PlanRun {
    pub fn failed(&self) -> bool {
        self.status == "failed"
    }
}

/// Run a plan, one pipeline at a time through `run`.
///
/// `run` is whatever actually executes a pipeline and says whether it worked. Passing it in
/// is what lets the ordering be tested on its own: which pipelines are attempted, in what
/// order, and what happens to the rest of the plan when one of them fails, are decisions
/// that should not need a database to check.
///
/// Pipelines within a step are handed over together and in order, which is the contract the
/// caller sees. Whether the caller actually runs them at the same time is its business:
/// this decides what may overlap, not how.
/// Each pipeline is handed the values its step binds for it (#317), so every
/// caller has to decide what to do with them - a runner that ignored them would
/// be the same gap as a surface that skipped the parameter boundary.
pub fn execute<F>(plan: &Plan, mut run: F) -> PlanRun
where
    F: FnMut(&str, &BTreeMap<String, String>) -> Result<(), String>,
{
    let mut out = PlanRun {
        plan_id: plan.id.clone(),
        status: "ok".to_string(),
        steps: Vec::new(),
    };
    let mut stopped = false;

    for step in &plan.steps {
        let mut results = Vec::new();
        for pipeline in &step.pipelines {
            if stopped {
                // Recorded rather than dropped. A plan that reports four pipelines when it
                // has six hides the two nobody looked at.
                results.push(PipelineOutcome {
                    pipeline: pipeline.clone(),
                    status: "skipped".into(),
                    error: None,
                });
                continue;
            }
            match run(pipeline, &step.values_for(pipeline)) {
                Ok(()) => results.push(PipelineOutcome {
                    pipeline: pipeline.clone(),
                    status: "ok".into(),
                    error: None,
                }),
                Err(e) => {
                    out.status = "failed".into();
                    results.push(PipelineOutcome {
                        pipeline: pipeline.clone(),
                        status: "failed".into(),
                        error: Some(e),
                    });
                }
            }
        }
        // A failure stops the NEXT step, not the rest of this one: things in one step were
        // declared independent, so the others were always going to run anyway.
        let soft = step.continue_on_failure.unwrap_or(false);
        if plan.stop_on_failure && !soft && results.iter().any(|r| r.status == "failed") {
            stopped = true;
        }
        out.steps.push(StepOutcome {
            name: step.name.clone(),
            pipelines: results,
        });
    }
    out
}

/// The store as it is on disk right now.
///
/// A missing file is an empty list. A file that exists and will not parse is an error,
/// because treating a corrupt store as empty is how a plan silently stops running.
pub fn load(workspace: &Path) -> Result<Vec<Plan>, String> {
    let p = plans_path(workspace);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", p.display()))
}

/// Apply a change to the store and persist it, as one exclusive step.
pub fn update<F>(workspace: &Path, f: F) -> Result<Vec<Plan>, String>
where
    F: FnOnce(&mut Vec<Plan>),
{
    let _guard = runlock::lock_store(workspace, "plans")?;
    let mut list = load(workspace)?;
    f(&mut list);
    let body = serde_json::to_string_pretty(&list).map_err(|e| e.to_string())?;
    let path = plans_path(workspace);
    // Through a temporary file and a rename, so a reader never sees half a store.
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, body).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("install {}: {e}", path.display()))?;
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(stop: bool) -> Plan {
        Plan {
            id: "nightly".into(),
            name: "Nightly load".into(),
            stop_on_failure: stop,
            steps: vec![
                Step { name: "Extract".into(), pipelines: vec!["orders".into(), "customers".into()], continue_on_failure: None, params: None },
                Step { name: "Transform".into(), pipelines: vec!["dbt".into()], continue_on_failure: None, params: None },
                Step { name: "Publish".into(), pipelines: vec!["export".into()], continue_on_failure: None, params: None },
            ],
        }
    }

    #[test]
    fn a_plan_runs_its_steps_in_order() {
        let mut seen = Vec::new();
        let out = execute(&plan(true), |p, _| {
            seen.push(p.to_string());
            Ok(())
        });
        assert_eq!(seen, ["orders", "customers", "dbt", "export"]);
        assert_eq!(out.status, "ok");
        assert_eq!(out.steps.len(), 3);
    }

    /// The reason a plan is not three schedules a few minutes apart: a step that did not
    /// work must not let the next one run against data it was supposed to produce.
    #[test]
    fn a_failure_stops_the_steps_after_it() {
        let mut seen = Vec::new();
        let out = execute(&plan(true), |p, _| {
            seen.push(p.to_string());
            if p == "customers" {
                return Err("connection refused".into());
            }
            Ok(())
        });

        assert_eq!(seen, ["orders", "customers"], "nothing after the failed step should run");
        assert!(out.failed());
        let statuses: Vec<&str> = out
            .steps
            .iter()
            .flat_map(|s| s.pipelines.iter())
            .map(|p| p.status.as_str())
            .collect();
        // Everything is accounted for, including what was never attempted.
        assert_eq!(statuses, ["ok", "failed", "skipped", "skipped"]);
    }

    /// Things in one step were declared independent, so a failure in one does not cancel
    /// its siblings: they were always going to run at the same time as it.
    #[test]
    fn a_failure_does_not_cancel_the_rest_of_its_own_step() {
        let mut seen = Vec::new();
        execute(&plan(true), |p, _| {
            seen.push(p.to_string());
            if p == "orders" {
                return Err("nope".into());
            }
            Ok(())
        });
        assert_eq!(seen, ["orders", "customers"], "the sibling should still be attempted");
    }

    #[test]
    fn a_step_can_be_allowed_to_fail_without_stopping_the_plan() {
        // A real sequence mixes the two. Here the middle step is housekeeping:
        // its failure must not abandon the publish that follows, while a failure
        // in either of the others still stops the plan.
        let mut p = plan(true);
        p.steps[1].continue_on_failure = Some(true);

        let mut seen = Vec::new();
        let out = execute(&p, |name, _| {
            seen.push(name.to_string());
            if name == "dbt" { Err("boom".into()) } else { Ok(()) }
        });

        assert_eq!(
            seen,
            vec!["orders", "customers", "dbt", "export"],
            "the step after a soft failure must still run"
        );
        // Allowed to fail is not the same as pretended to have worked.
        assert_eq!(out.status, "failed", "the plan still reports the failure");
        assert_eq!(out.steps[1].pipelines[0].status, "failed");
        assert_eq!(out.steps[2].pipelines[0].status, "ok");
    }

    #[test]
    fn a_hard_step_still_stops_the_plan_when_another_is_soft() {
        // The flag is per step, not a plan-wide switch by another name.
        let mut p = plan(true);
        p.steps[1].continue_on_failure = Some(true);

        let mut seen = Vec::new();
        let out = execute(&p, |name, _| {
            seen.push(name.to_string());
            if name == "orders" { Err("boom".into()) } else { Ok(()) }
        });

        assert!(!seen.contains(&"dbt".to_string()), "a hard failure stops what follows: {:?}", seen);
        assert_eq!(out.status, "failed");
        assert_eq!(out.steps[1].pipelines[0].status, "skipped");
    }

    #[test]
    fn a_plan_can_be_told_to_carry_on() {
        let mut seen = Vec::new();
        let out = execute(&plan(false), |p, _| {
            seen.push(p.to_string());
            if p == "customers" {
                return Err("nope".into());
            }
            Ok(())
        });
        assert_eq!(seen, ["orders", "customers", "dbt", "export"]);
        assert!(out.failed(), "carrying on does not make a failed plan a good one");
    }

    /// #317: each pipeline in a step is handed its OWN values, so two pipelines
    /// with different contracts can share a step. One plan-wide set would hand
    /// every pipeline every name, and a declared contract refuses a name it does
    /// not declare.
    #[test]
    fn each_pipeline_in_a_step_is_handed_its_own_values() {
        let mut p = plan(true);
        p.steps[0].params = Some(BTreeMap::from([
            ("orders".to_string(), BTreeMap::from([("region".to_string(), "BE".to_string())])),
            ("customers".to_string(), BTreeMap::from([("level".to_string(), "full".to_string())])),
        ]));
        let mut seen: Vec<(String, BTreeMap<String, String>)> = Vec::new();
        let out = execute(&p, |pipeline, values| {
            seen.push((pipeline.to_string(), values.clone()));
            Ok(())
        });
        assert_eq!(out.status, "ok");
        assert_eq!(seen[0].0, "orders");
        assert_eq!(seen[0].1.get("region").map(String::as_str), Some("BE"));
        assert!(!seen[0].1.contains_key("level"), "orders was handed customers' values: {seen:?}");
        assert_eq!(seen[1].1.get("level").map(String::as_str), Some("full"));
        assert!(seen[2].1.is_empty() && seen[3].1.is_empty(), "no values, none handed: {seen:?}");
    }

    /// A step may name a pipeline as a bare id or as a workspace file, and its
    /// values are found either way - the same tolerance the step itself gets.
    #[test]
    fn values_are_found_however_the_step_spells_the_pipeline() {
        let mut p = plan(true);
        p.steps[1].pipelines = vec!["pipelines/dbt.json".into()];
        p.steps[1].params = Some(BTreeMap::from([(
            "dbt".to_string(),
            BTreeMap::from([("target".to_string(), "prod".to_string())]),
        )]));
        let mut handed = BTreeMap::new();
        execute(&p, |pipeline, values| {
            if pipeline.contains("dbt") {
                handed = values.clone();
            }
            Ok(())
        });
        assert_eq!(handed.get("target").map(String::as_str), Some("prod"));
    }

    /// Values for a pipeline the step does not run would never be used: a typo
    /// or a stale edit, refused before the plan is saved rather than ignored at
    /// run time.
    #[test]
    fn values_for_a_pipeline_the_step_does_not_run_are_refused() {
        let mut p = plan(true);
        p.steps[0].params = Some(BTreeMap::from([(
            "ordrs".to_string(),
            BTreeMap::from([("region".to_string(), "BE".to_string())]),
        )]));
        let problems = p.problems();
        assert!(problems.iter().any(|m| m.contains("ordrs")), "{problems:?}");
        p.steps[0].params = Some(BTreeMap::from([(
            "orders".to_string(),
            BTreeMap::from([("region".to_string(), "BE".to_string())]),
        )]));
        assert!(p.problems().is_empty(), "{:?}", p.problems());
    }

    /// The console's plan form does not show values, and saving it must not wipe
    /// them. A step that says nothing keeps what is stored for its pipelines; an
    /// empty set clears; a pipeline taken out of the step takes its values with it.
    #[test]
    fn a_save_that_says_nothing_about_values_keeps_the_stored_ones() {
        let mut stored = plan(true);
        stored.steps[0].params = Some(BTreeMap::from([
            ("orders".to_string(), BTreeMap::from([("region".to_string(), "BE".to_string())])),
            ("customers".to_string(), BTreeMap::from([("level".to_string(), "full".to_string())])),
        ]));

        // The console's form: the same steps, no values, customers moved out.
        let mut saved = plan(true);
        saved.steps[0].pipelines = vec!["pipelines/orders.json".into()];
        carry_values(&mut saved, Some(&stored));
        assert_eq!(saved.steps[0].values_for("orders").get("region").map(String::as_str), Some("BE"));
        assert!(saved.problems().is_empty(), "a carried value must fit the step: {:?}", saved.problems());
        assert!(saved.steps[0].values_for("customers").is_empty(), "customers left the step");

        // An editor that says "no values" means it.
        let mut cleared = plan(true);
        cleared.steps[0].params = Some(BTreeMap::new());
        carry_values(&mut cleared, Some(&stored));
        assert!(cleared.steps[0].values_for("orders").is_empty());
    }

    /// #317: a value the pipeline's own contract refuses is refused when the plan
    /// is saved - the same check the run makes, on the same boundary. A missing
    /// required value is not, since a run started by hand can still supply it.
    #[test]
    fn step_values_are_checked_against_each_pipelines_contract() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines").join("orders.json"),
            r#"{"parameters":{"region":{"type":"string","enum":["eu","us"]},"day":{"type":"date","required":true}},"nodes":[],"edges":[]}"#,
        )
        .unwrap();
        let with = |values: &[(&str, &str)]| {
            let mut p = plan(true);
            p.steps[0].params = Some(BTreeMap::from([(
                "pipelines/orders.json".to_string(),
                values.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            )]));
            contract_problems(ws, &p)
        };
        let typo = with(&[("regin", "eu")]);
        assert!(typo.iter().any(|m| m.contains("regin")), "an unknown name: {typo:?}");
        let off_list = with(&[("region", "mars")]);
        assert!(off_list.iter().any(|m| m.contains("mars") || m.contains("region")), "{off_list:?}");
        assert!(with(&[("region", "us")]).is_empty(), "a good value, day still to come: {:?}", with(&[("region", "us")]));
    }

    /// The three parts of a save, together and in order: a stored value is kept
    /// by a save that does not mention it, and a value the contract refuses is
    /// refused - whichever of the two surfaces wrote it.
    #[test]
    fn a_save_keeps_stored_values_and_refuses_what_the_contract_refuses() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join("pipelines")).unwrap();
        std::fs::write(
            ws.join("pipelines").join("orders.json"),
            r#"{"parameters":{"region":{"type":"string","enum":["eu","us"]}},"nodes":[],"edges":[]}"#,
        )
        .unwrap();
        let mut stored = plan(true);
        stored.steps[0].params = Some(BTreeMap::from([(
            "orders".to_string(),
            BTreeMap::from([("region".to_string(), "us".to_string())]),
        )]));
        update(ws, |list| list.push(stored)).unwrap();

        let mut from_console = plan(true);
        prepare_for_save(ws, &mut from_console).expect("a save without values is fine");
        assert_eq!(from_console.steps[0].values_for("orders").get("region").map(String::as_str), Some("us"));

        let mut typo = plan(true);
        typo.steps[0].params = Some(BTreeMap::from([(
            "orders".to_string(),
            BTreeMap::from([("region".to_string(), "mars".to_string())]),
        )]));
        let e = prepare_for_save(ws, &mut typo).expect_err("mars is not a region");
        assert!(e.contains("region"), "{e}");
    }

    #[test]
    fn a_plan_that_cannot_work_says_so_before_it_is_saved() {
        let empty = Plan { id: "".into(), name: "".into(), steps: vec![], stop_on_failure: true };
        let problems = empty.problems();
        assert!(problems.iter().any(|p| p.contains("id")));
        assert!(problems.iter().any(|p| p.contains("at least one step")));

        let dupe = Plan {
            id: "x".into(),
            name: String::new(),
            stop_on_failure: true,
            steps: vec![Step { name: "s".into(), pipelines: vec!["a".into(), "a".into()], continue_on_failure: None, params: None }],
        };
        assert!(
            dupe.problems().iter().any(|p| p.contains("twice")),
            "the same pipeline twice in one step would run against itself"
        );

        assert!(plan(true).problems().is_empty(), "a sound plan has nothing to report");
    }

    #[test]
    fn the_store_survives_a_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        assert!(load(ws).unwrap().is_empty(), "a fresh workspace has no plans");

        update(ws, |list| list.push(plan(true))).unwrap();
        let back = load(ws).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0], plan(true));
    }

    /// The console and the desktop app spell a step differently, and one file is read by
    /// both. A reader that understands only its own spelling fails on every step of a plan
    /// the other product wrote.
    #[test]
    fn a_step_means_the_same_pipeline_however_it_was_spelled() {
        for spelling in ["orders", "orders.json", "pipelines/orders.json", "pipelines/orders"] {
            assert_eq!(step_pipeline_id(spelling), "orders", "spelled {spelling}");
            assert_eq!(step_pipeline_file(spelling), "pipelines/orders.json");
        }
        // Written on Windows, where a hand-edited path may carry backslashes.
        assert_eq!(step_pipeline_id("pipelines\\orders.json"), "orders");
        // A name that merely contains the word survives intact: only a leading directory
        // and a trailing extension are structure, the rest is somebody's pipeline name.
        assert_eq!(step_pipeline_id("pipelines-archive"), "pipelines-archive");
        assert_eq!(step_pipeline_id("orders.json.json"), "orders.json");
    }

    /// A deploy puts a pipeline at the workspace root, and the console's form offers it by
    /// that file; sent to `pipelines/`, no plan could run it. Every other spelling keeps the
    /// meaning it had.
    #[test]
    fn a_step_naming_a_file_in_the_workspace_runs_that_file() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join("report.json"), "{}").unwrap();
        std::fs::write(tmp.path().join("outside.json"), "{}").unwrap();

        assert_eq!(step_pipeline_path(&ws, "report.json"), "report.json", "deployed to the root");
        for spelling in ["orders", "orders.json", "pipelines/orders.json"] {
            assert_eq!(step_pipeline_path(&ws, spelling), "pipelines/orders.json", "spelled {spelling}");
        }
        assert_eq!(step_pipeline_path(&ws, "report"), "pipelines/report.json", "a bare id still means pipelines/");
        assert_ne!(step_pipeline_path(&ws, "../outside.json"), "../outside.json", "a step never names a file outside");
    }

    /// An older store written before a field existed must still load, or upgrading breaks
    /// every plan somebody already wrote.
    #[test]
    fn a_plan_without_the_newer_fields_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::write(
            plans_path(ws),
            r#"[{"id":"old","steps":[{"pipelines":["a"]}]}]"#,
        )
        .unwrap();

        let back = load(ws).expect("an older plan should still load");
        assert_eq!(back[0].id, "old");
        assert!(back[0].stop_on_failure, "stopping is the default, not carrying on");
        assert_eq!(back[0].steps[0].name, "");
    }
}
