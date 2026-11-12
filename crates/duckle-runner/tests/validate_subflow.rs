//! `validate` expands a sub-flow as the pipeline compiles, so it has to find a
//! sub-flow the way a run does.

/// The editor saves a Sub-flow node's `subflowRef` as a pipeline id, and a run
/// finds that id in its workspace. validate is the gate the CI templates run:
/// unless it finds the id the same way, every pipeline that calls a sub-flow
/// fails CI while running perfectly. Checked with `--workspace` and without it,
/// where the workspace is the pipeline file's folder, as for a run.
#[test]
fn a_pipeline_calling_a_subflow_by_its_id_validates() {
    let tmp = tempfile::tempdir().unwrap();
    let ws = tmp.path();
    let pipelines = ws.join("pipelines");
    std::fs::create_dir_all(&pipelines).unwrap();
    std::fs::write(
        pipelines.join("p_keep.json"),
        r#"{"nodes":[
             {"id":"in","type":"transform","position":{"x":0,"y":0},
              "data":{"label":"In","componentId":"ctl.subflow.in","properties":{}}},
             {"id":"keep","type":"transform","position":{"x":200,"y":0},
              "data":{"label":"Keep","componentId":"xf.filter","properties":{"predicate":"amount >= 10"}}},
             {"id":"out","type":"transform","position":{"x":400,"y":0},
              "data":{"label":"Out","componentId":"ctl.subflow.out","properties":{}}}],
           "edges":[{"id":"a","source":"in","target":"keep"},{"id":"b","source":"keep","target":"out"}]}"#,
    )
    .unwrap();
    let caller = pipelines.join("p_caller.json");
    std::fs::write(
        &caller,
        r#"{"nodes":[
             {"id":"s","type":"source","position":{"x":0,"y":0},
              "data":{"label":"In","componentId":"src.csv","properties":{"path":"rows.csv","hasHeader":true}}},
             {"id":"call","type":"transform","position":{"x":200,"y":0},
              "data":{"label":"Sub-flow","componentId":"ctl.subflow","properties":{"subflowRef":"p_keep"}}},
             {"id":"k","type":"sink","position":{"x":400,"y":0},
              "data":{"label":"Out","componentId":"snk.csv","properties":{"path":"out.csv","hasHeader":true}}}],
           "edges":[{"id":"e1","source":"s","target":"call"},{"id":"e2","source":"call","target":"k"}]}"#,
    )
    .unwrap();

    let with_workspace = ["--workspace".to_string(), ws.display().to_string()];
    for extra in [&[][..], &with_workspace[..]] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_duckle-runner"))
            .arg("validate")
            .args(extra)
            .arg(&caller)
            .env_remove("DUCKLE_WORKSPACE")
            .output()
            .expect("the runner starts");
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(
            out.status.success(),
            "a pipeline calling a sub-flow by its id must validate ({extra:?}); \
             stdout: {stdout}\nstderr: {stderr}"
        );
    }
}
