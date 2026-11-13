//! The CLI surfaces that compile a pipeline without running it expand its
//! sub-flows as they compile, so each has to find a sub-flow the way a run does.

use std::path::{Path, PathBuf};

/// Keeps rows with amount >= 10, between a Sub-flow input and output.
const SUBFLOW: &str = r#"{"nodes":[
     {"id":"in","type":"transform","position":{"x":0,"y":0},
      "data":{"label":"In","componentId":"ctl.subflow.in","properties":{}}},
     {"id":"keep","type":"transform","position":{"x":200,"y":0},
      "data":{"label":"Keep","componentId":"xf.filter","properties":{"predicate":"amount >= 10"}}},
     {"id":"out","type":"transform","position":{"x":400,"y":0},
      "data":{"label":"Out","componentId":"ctl.subflow.out","properties":{}}}],
   "edges":[{"id":"a","source":"in","target":"keep"},{"id":"b","source":"keep","target":"out"}]}"#;

/// `ws/pipelines`, holding the sub-flow as `p_keep.json`.
fn workspace_with_a_subflow(ws: &Path) -> PathBuf {
    let pipelines = ws.join("pipelines");
    std::fs::create_dir_all(&pipelines).unwrap();
    std::fs::write(pipelines.join("p_keep.json"), SUBFLOW).unwrap();
    pipelines
}

/// A pipeline calling `p_keep` by its id, as the editor saves it, writing `sink`.
fn caller(dir: &Path, name: &str, sink: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(
        &path,
        format!(
            r#"{{"nodes":[
                 {{"id":"s","type":"source","position":{{"x":0,"y":0}},
                   "data":{{"label":"In","componentId":"src.csv","properties":{{"path":"rows.csv","hasHeader":true}}}}}},
                 {{"id":"call","type":"transform","position":{{"x":200,"y":0}},
                   "data":{{"label":"Sub-flow","componentId":"ctl.subflow","properties":{{"subflowRef":"p_keep"}}}}}},
                 {{"id":"k","type":"sink","position":{{"x":400,"y":0}},
                   "data":{{"label":"Out","componentId":"snk.csv","properties":{{"path":"{sink}","hasHeader":true}}}}}}],
               "edges":[{{"id":"e1","source":"s","target":"call"}},{{"id":"e2","source":"call","target":"k"}}]}}"#
        ),
    )
    .unwrap();
    path
}

fn runner(args: &[&std::ffi::OsStr]) -> (bool, String) {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
        .args(args)
        .env_remove("DUCKLE_WORKSPACE")
        .output()
        .expect("the runner starts");
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

/// The editor saves a Sub-flow node's `subflowRef` as a pipeline id, and a run
/// finds that id in its workspace. validate is the gate the CI templates run:
/// unless it finds the id the same way, every pipeline that calls a sub-flow
/// fails CI while running perfectly. Checked with `--workspace` and without it,
/// where the workspace is the pipeline file's folder, as for a run.
#[test]
fn a_pipeline_calling_a_subflow_by_its_id_validates() {
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    let caller = caller(&workspace_with_a_subflow(ws), "p_caller.json", "out.csv");

    let (ok, text) = runner(&["validate".as_ref(), caller.as_os_str()]);
    assert!(ok, "a pipeline calling a sub-flow by its id must validate:\n{text}");
    let (ok, text) =
        runner(&["validate".as_ref(), "--workspace".as_ref(), ws.as_os_str(), caller.as_os_str()]);
    assert!(ok, "the same, given --workspace:\n{text}");
}

/// A sub-flow is itself a pipeline in `pipelines/`, so `validate pipelines/*.json`
/// reads it too. With no sample on its input it is still a sound pipeline: it
/// is fed by whatever calls it.
#[test]
fn a_subflow_validates_on_its_own() {
    let tmp = tempfile::tempdir().unwrap();
    let sub = workspace_with_a_subflow(tmp.path()).join("p_keep.json");
    let (ok, text) = runner(&["validate".as_ref(), sub.as_os_str()]);
    assert!(ok, "a sub-flow with no sample must validate:\n{text}");
}

/// `review` compiles both versions to compare their plans, and a side that will
/// not compile reads as an unchanged plan. A caller whose sub-flow could not be
/// found answered "plan changed: no" for a change to its own sink.
#[test]
fn a_review_sees_the_plan_change_of_a_pipeline_calling_a_subflow() {
    let tmp = tempfile::tempdir().unwrap();
    let pipelines = workspace_with_a_subflow(tmp.path());
    let before = caller(&pipelines, "before.json", "out.csv");
    let after = caller(&pipelines, "after.json", "elsewhere.csv");

    let (_, text) = runner(&[
        "review".as_ref(),
        "--before".as_ref(),
        before.as_os_str(),
        "--after".as_ref(),
        after.as_os_str(),
    ]);
    assert!(
        text.contains("plan changed: yes"),
        "the sink moved, so the compiled plan did:\n{text}"
    );
    assert!(!text.contains("sub-pipeline"), "a side could not find its sub-flow:\n{text}");
}
