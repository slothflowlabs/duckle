//! Planner unit tests (extracted from plan/mod.rs; gated via mod.rs).

    use super::*;

    fn pipeline_from_json(s: &str) -> PipelineDoc {
        serde_json::from_str(s).expect("valid pipeline JSON")
    }

    fn map_sql(doc: &PipelineDoc) -> String {
        compile(doc)
            .unwrap()
            .stages
            .iter()
            .find(|s| s.node_id == "m")
            .unwrap()
            .sql
            .clone()
    }

    #[test]
    fn map_with_lookups_emits_join_chain() {
        // Visual mapper: main CSV + two lookup CSVs, joined, with expressions
        // referencing each input and a filter referencing a lookup.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"o","position":{"x":0,"y":0},"data":{"label":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{"label":"cust","componentId":"src.csv","properties":{"path":"/tmp/c.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{"label":"region","componentId":"src.csv","properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{"label":"Map","componentId":"xf.map","properties":{
                  "lookups":[
                    {"port":"lookup_1","leftKey":"customer_id","rightKey":"cust_id","joinType":"left"},
                    {"port":"lookup_2","leftKey":"region_code","rightKey":"code","joinType":"inner"}
                  ],
                  "expressions":[
                    {"key":"order_id","value":"main.id"},
                    {"key":"customer_name","value":"lookup_1.name"},
                    {"key":"region_name","value":"lookup_2.label"},
                    {"key":"net","value":"main.amount * 1.08"}
                  ],
                  "filter":"lookup_2.active = true"
                }}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges":[
                {"id":"e1","source":"o","target":"m","data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"m","targetHandle":"lookup_1","data":{"connectionType":"lookup"}},
                {"id":"e3","source":"r","target":"m","targetHandle":"lookup_2","data":{"connectionType":"lookup"}},
                {"id":"e4","source":"m","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let sql = map_sql(&doc);
        assert!(sql.contains("LEFT JOIN \"c\" ON \"o\".\"customer_id\" = \"c\".\"cust_id\""), "left join: {}", sql);
        assert!(sql.contains("INNER JOIN \"r\" ON \"o\".\"region_code\" = \"r\".\"code\""), "inner join: {}", sql);
        assert!(sql.contains("\"o\".\"id\" AS \"order_id\""), "main expr: {}", sql);
        assert!(sql.contains("\"c\".\"name\" AS \"customer_name\""), "lookup_1 expr: {}", sql);
        assert!(sql.contains("\"o\".\"amount\" * 1.08 AS \"net\""), "arithmetic expr: {}", sql);
        assert!(sql.contains("WHERE \"r\".\"active\" = true"), "filter qualified: {}", sql);
    }

    #[test]
    fn map_without_lookups_is_unchanged() {
        // No lookups + no lookup refs: behaves like the original mapper.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"o","position":{"x":0,"y":0},"data":{"label":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{"label":"Map","componentId":"xf.map","properties":{
                  "expressions":[{"key":"net","value":"main.amount * 1.08"}]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges":[
                {"id":"e1","source":"o","target":"m","data":{"connectionType":"main"}},
                {"id":"e2","source":"m","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let sql = map_sql(&doc);
        assert!(sql.contains("amount * 1.08 AS \"net\""), "strip-prefix path: {}", sql);
        assert!(!sql.contains("JOIN"), "no join when no lookups: {}", sql);
    }

    #[test]
    fn map_unconfigured_lookup_ref_errors() {
        // Referencing lookup_1 without a lookups[] entry for it must error
        // clearly, not emit broken SQL.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"o","position":{"x":0,"y":0},"data":{"label":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{"label":"Map","componentId":"xf.map","properties":{
                  "expressions":[{"key":"x","value":"lookup_1.name"}]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges":[
                {"id":"e1","source":"o","target":"m","data":{"connectionType":"main"}},
                {"id":"e2","source":"m","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&doc).unwrap_err().to_string();
        assert!(err.contains("lookup_1") && err.contains("lookups"), "clear error: {}", err);
    }

    #[test]
    fn map_string_literal_with_dot_prefix_not_corrupted() {
        // A string literal containing 'main.' / 'lookup_1.' must be left
        // untouched by qualification (the qualifier is string-aware).
        let aliases: std::collections::BTreeMap<String, String> = [
            ("main".to_string(), "\"o\"".to_string()),
            ("lookup_1".to_string(), "\"c\"".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            qualify_port_refs("main.id || 'see lookup_1.x or main.y'", &aliases),
            "\"o\".\"id\" || 'see lookup_1.x or main.y'"
        );
        // Escaped quotes inside the literal don't end it early.
        assert_eq!(
            qualify_port_refs("'it''s main.x' || main.id", &aliases),
            "'it''s main.x' || \"o\".\"id\""
        );
    }

    #[test]
    fn cast_honors_on_error_try_vs_hard_cast() {
        // Default "Set to NULL" must emit TRY_CAST (bad values -> NULL);
        // "Fail pipeline" must emit a hard CAST. Previously onError was
        // ignored and the engine always emitted CAST, crashing the run on
        // dirty data even though the UI default promised NULLs.
        let try_doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"amount","targetType":"int64","onError":"null"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"c","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&try_doc).unwrap().stages.iter()
            .find(|s| s.node_id == "c").unwrap().sql.clone();
        assert!(sql.contains("TRY_CAST"), "default onError should TRY_CAST: {}", sql);

        let fail_doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"amount","targetType":"int64","onError":"fail"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"c","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&fail_doc).unwrap().stages.iter()
            .find(|s| s.node_id == "c").unwrap().sql.clone();
        assert!(sql.contains("CAST") && !sql.contains("TRY_CAST"),
            "onError=fail should hard CAST: {}", sql);
    }

    #[test]
    fn addcol_wraps_expression_in_declared_type() {
        // The Add-Column form's type selector must actually type the new
        // column (CAST the expression), not be cosmetic.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"a","position":{"x":0,"y":0},"data":{
                  "label":"Add","componentId":"xf.addcol",
                  "properties":{"name":"total","type":"int64","expression":"qty * price"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"a","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&doc).unwrap().stages.iter()
            .find(|s| s.node_id == "a").unwrap().sql.clone();
        assert!(sql.contains("CAST((qty * price) AS BIGINT)"),
            "addcol should cast expr to declared type: {}", sql);
    }

    #[test]
    fn downstream_ref_to_window_added_column_is_not_rejected() {
        // Regression: xf.rownum ADDS a column ("row_num"). A downstream
        // transform referencing that added column must NOT be falsely
        // rejected by the column-existence validator. Column-adding
        // transforms report "schema unknown" so downstream validation
        // is skipped rather than wrong. (Reported as "most transforms
        // erroneous" - the validator over-fired on column-adder chains.)
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/in.csv","hasHeader":true},
                  "schema":[{"name":"amount","type":"int64","nullable":true}]}},
                {"id":"rn","position":{"x":0,"y":0},"data":{
                  "label":"Row Number","componentId":"xf.rownum",
                  "properties":{"outputColumn":"row_num","orderBy":["amount"]}}},
                {"id":"d1","position":{"x":0,"y":0},"data":{
                  "label":"Distinct","componentId":"xf.distinct",
                  "properties":{"columns":["row_num"]}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"rn",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"rn","target":"d1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        // Must compile cleanly - the distinct on the rownum-added column
        // must not trip the validator.
        assert!(compile(&p).is_ok(), "rownum-added column must not be rejected downstream");
    }

    #[test]
    fn distinct_on_missing_column_errors_with_available_list() {
        // The genuine error case (issue screenshot): a customers CSV has
        // no order_id column, so xf.distinct on order_id must fail at
        // planner time with a message that lists the real columns.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/c.csv","hasHeader":true},
                  "schema":[
                    {"name":"Index","type":"int64","nullable":true},
                    {"name":"Customer Id","type":"string","nullable":true}
                  ]}},
                {"id":"d1","position":{"x":0,"y":0},"data":{
                  "label":"Distinct","componentId":"xf.distinct",
                  "properties":{"columns":["order_id"]}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"d1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).unwrap_err().to_string();
        assert!(err.contains("order_id"), "got: {}", err);
        assert!(
            err.contains("Available columns") && err.contains("Customer Id"),
            "error should list available columns, got: {}",
            err
        );
    }

    #[test]
    fn pure_sql_pipeline_marks_every_stage_batchable() {
        // CSV -> filter -> Parquet has no driver-based stages and no
        // ctl.* hooks, so every stage must report is_pure_sql() = true.
        // The batched executor uses exactly this predicate to decide
        // whether to collapse the pipeline into one CLI spawn.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/in.csv","hasHeader":true}}},
                {"id":"f1","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"x > 0"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Parquet","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"f1",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        assert_eq!(compiled.stages.len(), 3);
        for stage in &compiled.stages {
            assert!(
                stage.is_pure_sql(),
                "stage {} ({}) should be batchable",
                stage.node_id,
                stage.component_id
            );
        }
    }

    #[test]
    fn rest_source_pipeline_is_not_batchable() {
        // src.rest hits the Rust-side ureq driver mid-pipeline, so
        // its stage must report is_pure_sql() = false. Any single
        // false stage forces the per-stage execution path.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"REST","componentId":"src.rest",
                  "properties":{"url":"https://example.com/users",
                                "responsePath":"data"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"snk.csv",
                  "properties":{"path":"/tmp/out.csv"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let any_non_batchable = compiled.stages.iter().any(|s| !s.is_pure_sql());
        assert!(
            any_non_batchable,
            "src.rest pipeline must contain at least one non-pure stage"
        );
    }

    #[test]
    fn compiles_csv_filter_parquet() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/orders.csv","hasHeader":true}}},
                {"id":"f1","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"status = 'paid'"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Parquet","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"f1",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        assert_eq!(compiled.stages.len(), 3);
        assert_eq!(compiled.stages[0].node_id, "s1");
        assert!(compiled.stages[0]
            .sql
            .contains("read_csv_auto('/tmp/orders.csv'"));
        assert!(compiled.stages[1].sql.contains("WHERE status = 'paid'"));
        // Perf regression guard: a filter whose reject port is unwired must
        // compile to a lazy VIEW (so DuckDB pushes the predicate into the
        // source read) and must NOT materialize the rejected rows. The old
        // behaviour wrote every rejected row to a `__reject` table - on a
        // 10M-row source that dominated the whole run (~16s).
        assert!(
            compiled.stages[1].sql.contains("CREATE OR REPLACE VIEW \"f1\""),
            "unwired-reject filter must be a VIEW, got: {}",
            compiled.stages[1].sql
        );
        assert!(
            !compiled.stages[1].sql.contains("__reject"),
            "unwired-reject filter must not materialize a reject table, got: {}",
            compiled.stages[1].sql
        );
        assert_eq!(compiled.stages[2].kind, StageKind::Sink);
        assert!(compiled.stages[2]
            .sql
            .contains("TO '/tmp/out.parquet' (FORMAT PARQUET"));
    }

    #[test]
    fn filter_with_single_consumer_reject_is_a_lazy_view() {
        // When the reject port is consumed by exactly one downstream node,
        // it must be a lazy VIEW (inlined into that consumer), NOT a
        // materialized table. The old code always made reject a TABLE, which
        // wrote the entire rejected set to disk (8M rows on a 10M source)
        // even when its only consumer was a sink that would just COPY it.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/orders.csv","hasHeader":true}}},
                {"id":"f1","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"status = 'paid'"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Pass","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/pass.parquet"}}},
                {"id":"k2","position":{"x":0,"y":0},"data":{
                  "label":"Rejected","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/rej.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"f1",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f1","target":"k1",
                  "data":{"connectionType":"main"}},
                {"id":"e3","source":"f1","sourceHandle":"reject","target":"k2",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let filter = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "f1")
            .expect("filter stage");
        assert!(
            filter.sql.contains("CREATE OR REPLACE VIEW \"f1__reject\""),
            "single-consumer reject must be a lazy VIEW, got: {}",
            filter.sql
        );
        assert!(
            !filter.sql.contains("CREATE OR REPLACE TABLE \"f1__reject\""),
            "single-consumer reject must not materialize a table, got: {}",
            filter.sql
        );
        // The pass side is also single-consumer, so it stays a lazy view too.
        assert!(
            filter.sql.contains("CREATE OR REPLACE VIEW \"f1\""),
            "single-consumer pass must be a lazy VIEW, got: {}",
            filter.sql
        );
    }

    #[test]
    fn filter_with_multi_consumer_reject_materializes_table() {
        // 2+ consumers of the reject port -> materialize it once as a TABLE
        // so the body isn't re-evaluated per consumer.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/orders.csv","hasHeader":true}}},
                {"id":"f1","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"status = 'paid'"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"R1","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/r1.parquet"}}},
                {"id":"k2","position":{"x":0,"y":0},"data":{
                  "label":"R2","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/r2.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"f1",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f1","sourceHandle":"reject","target":"k1",
                  "data":{"connectionType":"main"}},
                {"id":"e3","source":"f1","sourceHandle":"reject","target":"k2",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let filter = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "f1")
            .expect("filter stage");
        assert!(
            filter.sql.contains("CREATE OR REPLACE TABLE \"f1__reject\""),
            "multi-consumer reject must materialize a table, got: {}",
            filter.sql
        );
    }

    #[test]
    fn source_feeding_reject_wired_filter_materializes_once() {
        // A source feeding a filter/validator whose reject port is wired is read
        // TWICE (the pass body and the reject body both `SELECT ... FROM src`).
        // It must materialize as a TABLE so an expensive source (read_csv_auto /
        // read_json_auto) is scanned once, not re-evaluated for each side
        // (darekdan: "the source will be processed twice").
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/orders.csv","hasHeader":true}}},
                {"id":"f1","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"status = 'paid'"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Pass","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/pass.parquet"}}},
                {"id":"k2","position":{"x":0,"y":0},"data":{
                  "label":"Rejected","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/rej.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"f1","data":{"connectionType":"main"}},
                {"id":"e2","source":"f1","target":"k1","data":{"connectionType":"main"}},
                {"id":"e3","source":"f1","sourceHandle":"reject","target":"k2","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "s1")
            .expect("source stage");
        assert!(
            src.sql.contains("CREATE OR REPLACE TABLE \"s1\""),
            "source feeding a reject-wired filter must materialize once as a TABLE, got: {}",
            src.sql
        );
    }

    #[test]
    fn materialize_memory_override_forces_table_for_single_consumer() {
        // materialize=memory forces a materialized run-db TABLE even when the
        // node has a single consumer (which would default to a lazy VIEW).
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/orders.csv","hasHeader":true,"materialize":"memory"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Out","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "s1")
            .expect("source stage");
        assert!(
            src.sql.contains("CREATE OR REPLACE TABLE \"s1\""),
            "materialize=memory must force a TABLE for a single consumer, got: {}",
            src.sql
        );
    }

    #[test]
    fn materialize_disk_streams_via_parquet() {
        // materialize=disk routes the stage through the COPY-to-parquet path
        // (read once, minimal RAM) instead of a run-db table insert.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/orders.csv","hasHeader":true,"materialize":"disk"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Out","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "s1")
            .expect("source stage");
        assert!(
            matches!(src.runtime.as_ref(), Some(RuntimeSpec::AttachParquetSource(_))),
            "materialize=disk must route through the parquet path, got sql: {}",
            src.sql
        );
    }

    #[test]
    fn materialize_view_duck_source_becomes_lazy_view() {
        // issue #76: an explicit View on a SINGLE-consumer ATTACH-backed duck
        // source becomes a real lazy VIEW over the live source (so a downstream
        // WHERE pushes down), with the duckle_src ATTACH kept (no DETACH) so the
        // view resolves in the batched downstream stage - NOT a CREATE TABLE and
        // NOT an eager parquet COPY. Controls: a 2-consumer View and a
        // 2-consumer auto both stay a materialized TABLE (scan once).
        let make = |materialize: &str, two: bool| {
            let mat = if materialize.is_empty() {
                String::new()
            } else {
                format!(",\"materialize\":\"{}\"", materialize)
            };
            let extra_node = if two {
                r#",{"id":"k2","position":{"x":0,"y":0},"data":{"label":"B","componentId":"snk.parquet","properties":{"path":"/tmp/b.parquet"}}}"#
            } else {
                ""
            };
            let extra_edge = if two {
                r#",{"id":"e2","source":"s1","target":"k2","data":{"connectionType":"main"}}"#
            } else {
                ""
            };
            pipeline_from_json(&format!(
                r#"{{"nodes":[
                    {{"id":"s1","position":{{"x":0,"y":0}},"data":{{"label":"Duck","componentId":"src.duckdb","properties":{{"database":"/tmp/src.duckdb","tableName":"orders"{}}}}}}},
                    {{"id":"k1","position":{{"x":0,"y":0}},"data":{{"label":"A","componentId":"snk.parquet","properties":{{"path":"/tmp/a.parquet"}}}}}}{}
                  ],"edges":[
                    {{"id":"e1","source":"s1","target":"k1","data":{{"connectionType":"main"}}}}{}
                  ]}}"#,
                mat, extra_node, extra_edge
            ))
        };
        // single-consumer View -> real lazy VIEW, ATTACH kept (no DETACH), pure SQL.
        let c = compile(&make("view", false)).unwrap();
        let s = c.stages.iter().find(|s| s.node_id == "s1").expect("src stage");
        assert!(s.sql.contains("CREATE OR REPLACE VIEW"), "view src must be a VIEW, got: {}", s.sql);
        assert!(!s.sql.contains("CREATE OR REPLACE TABLE"), "view src must not be a TABLE: {}", s.sql);
        assert!(!s.sql.contains("DETACH"), "view src must keep duckle_src attached: {}", s.sql);
        assert!(s.runtime.is_none(), "view src must stay pure-SQL (so the pipeline batches), got a runtime spec");
        // 2-consumer View -> materialized TABLE (scan once), not a re-scanned VIEW.
        let c2 = compile(&make("view", true)).unwrap();
        let s2 = c2.stages.iter().find(|s| s.node_id == "s1").expect("src stage");
        assert!(s2.sql.contains("CREATE OR REPLACE TABLE"), "multi-consumer view stays a TABLE: {}", s2.sql);
        // 2-consumer auto -> TABLE (no regression).
        let c3 = compile(&make("", true)).unwrap();
        let s3 = c3.stages.iter().find(|s| s.node_id == "s1").expect("src stage");
        assert!(s3.sql.contains("CREATE OR REPLACE TABLE"), "auto multi-consumer stays a TABLE: {}", s3.sql);
    }

    #[test]
    fn relational_source_infers_custom_sql_without_mode() {
        // issue #77: a filled SQL box wins even when the Read-mode dropdown is
        // left at its default (no "mode" prop), mirroring src.duckdb. A
        // table-only read still works; empty everything still errors loudly.
        use serde_json::json;
        let sql = build_relational_source(
            "src.ducklake",
            &json!({"path":"/tmp/x.ducklake","sql":"SELECT 1 AS a"}),
        )
        .unwrap();
        assert_eq!(sql, "(SELECT 1 AS a)");
        let mduck = build_relational_source("src.motherduck", &json!({"sql":"SELECT 2"})).unwrap();
        assert_eq!(mduck, "(SELECT 2)");
        let tbl = build_relational_source("src.quack", &json!({"tableName":"orders","schemaName":"main"})).unwrap();
        assert!(tbl.starts_with("SELECT * FROM"), "table read still works: {}", tbl);
        assert!(
            build_relational_source("src.quack", &json!({})).is_err(),
            "no table and no sql must still error"
        );
    }

    #[test]
    fn materialize_duckdb_temp_routes_to_duckdb_spec_without_path() {
        // materialize=duckdb persists the stage into a temp DuckDB file (no
        // user path).
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/orders.csv","hasHeader":true,"materialize":"duckdb"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Out","componentId":"snk.parquet","properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges": [{"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src = compiled.stages.iter().find(|s| s.node_id == "s1").unwrap();
        match src.runtime.as_ref() {
            Some(RuntimeSpec::MaterializeDuckDb(spec)) => {
                assert!(spec.output_path.is_none(), "temp target must have no path");
            }
            other => panic!("expected MaterializeDuckDb, got {:?}", other),
        }
    }

    #[test]
    fn materialize_duckdbfile_carries_path_and_requires_it() {
        // materialize=duckdbfile with a path persists into that .duckdb.
        let ok = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true,"materialize":"duckdbfile","materializePath":"/tmp/lake.duckdb"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Out","componentId":"snk.parquet","properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges": [{"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}]
            }"#,
        );
        let src = compile(&ok).unwrap();
        let st = src.stages.iter().find(|s| s.node_id == "s1").unwrap();
        match st.runtime.as_ref() {
            Some(RuntimeSpec::MaterializeDuckDb(spec)) => {
                assert_eq!(spec.output_path.as_deref(), Some("/tmp/lake.duckdb"));
            }
            other => panic!("expected MaterializeDuckDb with path, got {:?}", other),
        }
        // Without materializePath it fails loud (no silent temp fallback).
        let bad = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true,"materialize":"duckdbfile"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Out","componentId":"snk.parquet","properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges": [{"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}]
            }"#,
        );
        let err = compile(&bad).unwrap_err();
        assert!(
            err.to_string().contains("materializePath") || err.to_string().to_lowercase().contains("path"),
            "missing materializePath must fail loud, got: {:?}",
            err
        );
    }

    #[test]
    fn materialize_view_override_keeps_view_with_multiple_consumers() {
        // materialize=view forces a lazy VIEW even when 2+ consumers would
        // otherwise materialize it as a TABLE (per-node DUCKLE_FORCE_VIEWS).
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/orders.csv","hasHeader":true,"materialize":"view"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"A","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/a.parquet"}}},
                {"id":"k2","position":{"x":0,"y":0},"data":{
                  "label":"B","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/b.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}},
                {"id":"e2","source":"s1","target":"k2","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "s1")
            .expect("source stage");
        assert!(
            src.sql.contains("CREATE OR REPLACE VIEW \"s1\""),
            "materialize=view must keep a VIEW even with multiple consumers, got: {}",
            src.sql
        );
    }

    #[test]
    fn cdc_diff_requires_compare_columns() {
        // Regression (audit B3): without compareColumns, build_cdc_diff's
        // `updated` arm is empty so every changed row is tagged 'unchanged'
        // and dropped by rejectUnchanged. compile() must reject it.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"cur","position":{"x":0,"y":0},"data":{
                  "label":"cur","componentId":"src.csv",
                  "properties":{"path":"/tmp/cur.csv","hasHeader":true}}},
                {"id":"prev","position":{"x":0,"y":0},"data":{
                  "label":"prev","componentId":"src.csv",
                  "properties":{"path":"/tmp/prev.csv","hasHeader":true}}},
                {"id":"d","position":{"x":0,"y":0},"data":{
                  "label":"Diff","componentId":"xf.cdc.diff",
                  "properties":{"naturalKey":["id"]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"out","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/o.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"cur","target":"d","data":{"connectionType":"main"}},
                {"id":"e2","source":"prev","sourceHandle":"main","target":"d","targetHandle":"lookup","data":{"connectionType":"lookup"}},
                {"id":"e3","source":"d","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).expect_err("cdc.diff without compareColumns must fail");
        let msg = format!("{:?}", err);
        assert!(
            msg.contains("compare columns"),
            "error should name compare columns, got: {}",
            msg
        );
    }

    #[test]
    fn scd1_uses_union_all_by_name() {
        // Regression (audit B3): SCD1 retains unmatched-previous rows via
        // UNION ALL, which must align cur/prev by column NAME. Positional
        // UNION ALL silently swaps values when the two inputs present
        // columns in a different order.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"cur","position":{"x":0,"y":0},"data":{
                  "label":"cur","componentId":"src.csv",
                  "properties":{"path":"/tmp/cur.csv","hasHeader":true}}},
                {"id":"prev","position":{"x":0,"y":0},"data":{
                  "label":"prev","componentId":"src.csv",
                  "properties":{"path":"/tmp/prev.csv","hasHeader":true}}},
                {"id":"scd","position":{"x":0,"y":0},"data":{
                  "label":"SCD1","componentId":"xf.cdc.scd1",
                  "properties":{"naturalKey":["id"]}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"out","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/o.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"cur","target":"scd",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"prev","sourceHandle":"main","target":"scd","targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"scd","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let scd = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "scd")
            .expect("scd1 stage");
        assert!(
            scd.sql.contains("UNION ALL BY NAME"),
            "SCD1 must align by name, got: {}",
            scd.sql
        );
    }

    #[test]
    fn printf_escapes_stray_percent_but_keeps_specs() {
        // audit B5: a literal % not forming a spec must be doubled so
        // printf prints it; real conversion specs are preserved.
        assert_eq!(escape_stray_printf_percents("100% done"), "100%% done");
        assert_eq!(escape_stray_printf_percents("%s"), "%s");
        assert_eq!(escape_stray_printf_percents("%.2f"), "%.2f");
        assert_eq!(escape_stray_printf_percents("val %s (100%%)"), "val %s (100%%)");
        assert_eq!(escape_stray_printf_percents("50% off %d items"), "50%% off %d items");
        assert_eq!(escape_stray_printf_percents("no percents"), "no percents");
    }

    #[test]
    fn numeric_rejects_non_finite_argument() {
        // audit B5: 'inf'/'nan' as a numeric op argument bind as columns
        // in DuckDB -> confusing binder error. Reject at plan time.
        for bad in ["inf", "Infinity", "nan", "-inf"] {
            let p = pipeline_from_json(&format!(
                r#"{{
                  "nodes": [
                    {{"id":"s","position":{{"x":0,"y":0}},"data":{{
                      "label":"CSV","componentId":"src.csv",
                      "properties":{{"path":"/tmp/x.csv","hasHeader":true}}}}}},
                    {{"id":"n","position":{{"x":0,"y":0}},"data":{{
                      "label":"Pow","componentId":"xf.num.power",
                      "properties":{{"column":"v","argument":"{}"}}}}}},
                    {{"id":"k","position":{{"x":0,"y":0}},"data":{{
                      "label":"out","componentId":"snk.parquet",
                      "properties":{{"path":"/tmp/o.parquet"}}}}}}
                  ],
                  "edges": [
                    {{"id":"e1","source":"s","target":"n","data":{{"connectionType":"main"}}}},
                    {{"id":"e2","source":"n","target":"k","data":{{"connectionType":"main"}}}}
                  ]
                }}"#,
                bad
            ));
            assert!(
                compile(&p).is_err(),
                "numeric op with argument '{}' should be rejected",
                bad
            );
        }
    }

    #[test]
    fn addcol_typed_expr_defaults_to_try_cast() {
        // audit B5: a typed Add-Column should TRY_CAST by default so one
        // bad value nulls the cell instead of aborting the run.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"a","position":{"x":0,"y":0},"data":{
                  "label":"Add","componentId":"xf.addcol",
                  "properties":{"name":"n","type":"int64","expression":"v"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"out","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/o.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"a","data":{"connectionType":"main"}},
                {"id":"e2","source":"a","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let add = compiled.stages.iter().find(|s| s.node_id == "a").unwrap();
        assert!(
            add.sql.contains("TRY_CAST((v) AS BIGINT)"),
            "typed addcol should TRY_CAST by default, got: {}",
            add.sql
        );
    }

    #[test]
    fn qa_unique_tiebreak_makes_survivor_deterministic() {
        // audit B4: with a tieBreak prop, qa.unique's ROW_NUMBER gets an
        // ORDER BY so the kept duplicate is deterministic. Without it, no
        // ORDER BY (unchanged behavior).
        let with_tb = build_quality(
            &{
                let mut ni = NodeInputs::default();
                ni.ports.insert("main".into(), vec!["up".into()]);
                ni
            },
            &serde_json::json!({"columns": ["k"], "tieBreak": ["ts"]}),
            "qa.unique",
            false,
        )
        .unwrap();
        assert!(
            with_tb.contains("PARTITION BY \"k\" ORDER BY \"ts\""),
            "tieBreak should add ORDER BY, got: {}",
            with_tb
        );
        let without = build_quality(
            &{
                let mut ni = NodeInputs::default();
                ni.ports.insert("main".into(), vec!["up".into()]);
                ni
            },
            &serde_json::json!({"columns": ["k"]}),
            "qa.unique",
            false,
        )
        .unwrap();
        assert!(
            !without.contains("ORDER BY"),
            "no tieBreak should not add ORDER BY, got: {}",
            without
        );
    }

    #[test]
    fn skip_orderby_makes_offset_deterministic() {
        // audit B4: xf.skip with an orderBy prop emits ORDER BY before
        // OFFSET so the skipped slice is repeatable.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_take(&ni, &serde_json::json!({"count": 5, "orderBy": ["id"]}), TakeKind::Offset).unwrap();
        assert!(
            sql.contains("ORDER BY \"id\" OFFSET 5"),
            "skip with orderBy should sort before offset, got: {}",
            sql
        );
    }

    #[test]
    fn distinct_orderby_prop_replaces_order_by_all() {
        // audit B10: keyed DISTINCT defaults to ORDER BY ALL (deterministic
        // but a full sort, >100x slower). An `orderBy` prop sorts only the
        // keys + tiebreak columns; default is unchanged.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let default_sql = build_distinct(&ni, &serde_json::json!({"columns": ["status"]})).unwrap();
        assert!(
            default_sql.contains("ORDER BY ALL"),
            "default keyed distinct must keep ORDER BY ALL, got: {}",
            default_sql
        );
        let fast_sql = build_distinct(
            &ni,
            &serde_json::json!({"columns": ["status"], "orderBy": ["amount"]}),
        )
        .unwrap();
        assert!(
            fast_sql.contains("ORDER BY \"status\", \"amount\"") && !fast_sql.contains("ORDER BY ALL"),
            "orderBy prop must sort keys+tiebreak, not ALL, got: {}",
            fast_sql
        );
    }

    #[test]
    fn csv_declared_schema_overrides_autodetect() {
        // Regression for issue #3: when the user sets a column to
        // VARCHAR in the Schema panel (typical fix for dd/mm/yy dates
        // that DuckDB would otherwise misparse as yyyy-mm-dd), the
        // generated read_csv_auto must include `types = {...}` so
        // DuckDB uses the requested types instead of inferring them.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/dates.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false},
                    {"name":"event_date","type":"string","nullable":true}
                  ]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src_sql = &compiled.stages[0].sql;
        assert!(
            src_sql.contains("types = {"),
            "missing types= clause: {}",
            src_sql
        );
        assert!(
            src_sql.contains("'event_date': 'VARCHAR'"),
            "date column not forced to VARCHAR: {}",
            src_sql
        );
        assert!(
            src_sql.contains("'id': 'BIGINT'"),
            "int64 not mapped to BIGINT: {}",
            src_sql
        );
    }

    #[test]
    fn csv_date_format_passes_through_to_reader() {
        // Follow-up to #3: a user with dd/mm/yyyy dates can now keep
        // the column as a real DATE instead of forcing VARCHAR, by
        // setting the dateFormat prop. The generated SQL must include
        // dateformat='%d/%m/%Y' so DuckDB picks the right parser.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true,
                                "dateFormat":"%d/%m/%Y",
                                "timestampFormat":"%d/%m/%Y %H:%M:%S"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(sql.contains("dateformat='%d/%m/%Y'"), "missing dateformat: {}", sql);
        assert!(sql.contains("timestampformat='%d/%m/%Y %H:%M:%S'"), "missing timestampformat: {}", sql);
    }

    #[test]
    fn csv_per_column_format_wraps_with_try_strptime() {
        // Issue #10: two date/timestamp columns with DIFFERENT formats on
        // one read. Each is forced to VARCHAR in types= and re-parsed with
        // its own format via try_strptime inside a SELECT * REPLACE wrap.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true},
                  "schema":[
                    {"name":"d1","type":"date","format":"%d/%m/%Y"},
                    {"name":"ts","type":"timestamp","format":"%Y-%m-%d %H:%M:%S"}
                  ]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(sql.contains("SELECT * REPLACE ("), "missing REPLACE wrap: {}", sql);
        assert!(
            sql.contains("try_strptime(\"d1\", '%d/%m/%Y')::DATE AS \"d1\""),
            "missing d1 strptime: {}",
            sql
        );
        assert!(
            sql.contains("try_strptime(\"ts\", '%Y-%m-%d %H:%M:%S')::TIMESTAMP AS \"ts\""),
            "missing ts strptime: {}",
            sql
        );
        assert!(sql.contains("'d1': 'VARCHAR'"), "d1 not forced VARCHAR: {}", sql);
        assert!(sql.contains("'ts': 'VARCHAR'"), "ts not forced VARCHAR: {}", sql);
        assert!(sql.contains("FROM read_csv_auto("), "missing reader: {}", sql);
    }

    #[test]
    fn csv_date_column_without_format_keeps_native_type() {
        // A DATE column with no format (or empty format) must NOT trigger
        // the REPLACE wrap; its declared type goes straight into types=.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true},
                  "schema":[{"name":"d","type":"date","format":""}]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(!sql.contains("REPLACE ("), "should not wrap without format: {}", sql);
        assert!(sql.contains("'d': 'DATE'"), "date type not preserved: {}", sql);
    }

    #[test]
    fn csv_mixed_format_and_plain_columns() {
        // One formatted date column + one plain int column: only the date
        // is rewritten; the int keeps its type and is carried through *.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true},
                  "schema":[
                    {"name":"d","type":"date","format":"%d/%m/%Y"},
                    {"name":"n","type":"int64"}
                  ]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(sql.contains("SELECT * REPLACE ("), "missing REPLACE wrap: {}", sql);
        assert!(sql.contains("try_strptime(\"d\", '%d/%m/%Y')::DATE AS \"d\""), "missing d: {}", sql);
        assert!(!sql.contains("\"n\")") && !sql.contains("AS \"n\""), "n should not be rewritten: {}", sql);
        assert!(sql.contains("'n': 'BIGINT'"), "int type not preserved: {}", sql);
    }

    #[test]
    fn csv_per_column_format_quotes_identifier() {
        // A formatted date column whose name needs quoting: both the
        // try_strptime arg and the AS alias must be double-quoted.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true},
                  "schema":[{"name":"Order Date","type":"date","format":"%d/%m/%Y"}]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[0].sql;
        assert!(
            sql.contains("try_strptime(\"Order Date\", '%d/%m/%Y')::DATE AS \"Order Date\""),
            "identifier not quoted: {}",
            sql
        );
    }

    #[test]
    fn cast_referencing_unknown_column_errors_at_planner() {
        // When the upstream source has a declared schema (Autodetect
        // or hand-typed), downstream xf.cast that references a column
        // not in the schema errors at compile time instead of waiting
        // for DuckDB's runtime "column not found".
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false},
                    {"name":"name","type":"string","nullable":true}
                  ]}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"NAME","targetType":"VARCHAR"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("'NAME'"), "should name the bad column: {}", msg);
        assert!(
            msg.contains("did you mean 'name'"),
            "should suggest the case-insensitive match: {}",
            msg
        );
    }

    #[test]
    fn cast_referencing_truly_missing_column_errors_without_hint() {
        // No close match: error still surfaces but no "did you mean".
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false}
                  ]}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"price","targetType":"DOUBLE"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("'price'"), "should name the bad column: {}", msg);
        assert!(msg.contains("not found"), "should say not found: {}", msg);
    }

    #[test]
    fn fill_forward_with_unknown_column_errors() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false},
                    {"name":"reading","type":"float64","nullable":true},
                    {"name":"ts","type":"timestamp","nullable":false}
                  ]}},
                {"id":"f","position":{"x":0,"y":0},"data":{
                  "label":"Fill","componentId":"xf.fill_forward",
                  "properties":{"column":"Reading","orderBy":"ts"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"f",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("'Reading'"), "should name the bad column: {}", msg);
        assert!(
            msg.contains("did you mean 'reading'"),
            "should suggest the close match: {}",
            msg
        );
    }

    #[test]
    fn cast_with_valid_column_in_schema_compiles() {
        // The positive case: with a declared schema and a valid column
        // reference, compile succeeds and emits the cast SQL.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true},
                  "schema":[
                    {"name":"id","type":"int64","nullable":false},
                    {"name":"amount","type":"string","nullable":true}
                  ]}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"column":"amount","targetType":"DOUBLE"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).expect("should compile cleanly");
        let cast_sql = compiled.stages.iter().find(|s| s.node_id == "c").unwrap().sql.as_str();
        assert!(cast_sql.contains("CAST(\"amount\" AS DOUBLE)"), "wrong cast SQL: {}", cast_sql);
    }

    #[test]
    fn cast_with_all_empty_columns_errors_loudly() {
        // Used to silently emit `SELECT * FROM upstream` (no-op) when
        // every cast entry had an empty column - the user wondered
        // why their column type didn't change.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"casts":[
                    {"column":"","targetType":"INTEGER"},
                    {"column":"   ","targetType":"DOUBLE"}
                  ]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("Cast:"), "should mention Cast: {}", msg);
        assert!(msg.contains("no column name"), "should mention the empty-column gap: {}", msg);
    }

    #[test]
    fn cast_with_duplicate_columns_errors_loudly() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Cast","componentId":"xf.cast",
                  "properties":{"casts":[
                    {"column":"amount","targetType":"INTEGER"},
                    {"column":"amount","targetType":"DOUBLE"}
                  ]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err");
        let msg = format!("{:?}", err);
        assert!(msg.contains("'amount'"), "should name the duplicate column: {}", msg);
    }

    #[test]
    fn window_without_order_by_errors_clearly() {
        // xf.rank / xf.lead / xf.lag / etc. all need ORDER BY. DuckDB's
        // native error for missing ORDER BY arrives two stages later
        // and reads as "Binder Error: OVER clause requires ORDER BY";
        // we want a planner-side error mentioning the function name and
        // pointing at the right form field.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"Rank","componentId":"xf.rank",
                  "properties":{"partitionBy":["dept"]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"r",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).err().expect("expected an Err from missing ORDER BY");
        let msg = format!("{:?}", err);
        assert!(
            msg.to_lowercase().contains("order by"),
            "error should mention Order By: {}",
            msg
        );
        assert!(
            msg.contains("rank"),
            "error should mention the window function name: {}",
            msg
        );
    }

    #[test]
    fn union_uses_by_name_to_dodge_positional_silent_corruption() {
        // ETL users almost always expect by-name semantics. Standard SQL
        // UNION matches by position - reordering columns in one input
        // silently produces garbage with no error. DuckDB's UNION BY NAME
        // matches column names + pads missing columns with NULL.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"a","position":{"x":0,"y":0},"data":{
                  "label":"A","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"b","position":{"x":0,"y":0},"data":{
                  "label":"B","componentId":"src.csv",
                  "properties":{"path":"/tmp/b.csv","hasHeader":true}}},
                {"id":"u","position":{"x":0,"y":0},"data":{
                  "label":"Union","componentId":"xf.unionall","properties":{}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"a","target":"u",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"b","target":"u",
                  "data":{"connectionType":"main"}},
                {"id":"e3","source":"u","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let union_sql = compiled.stages.iter().find(|s| s.node_id == "u").unwrap().sql.as_str();
        assert!(union_sql.contains("UNION ALL BY NAME"), "expected BY NAME variant: {}", union_sql);
    }

    #[test]
    fn arr_contains_is_null_safe() {
        // list_contains(NULL_array, x) returns NULL. Without the COALESCE
        // shield, downstream WHERE _contains would silently drop the row.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"c","position":{"x":0,"y":0},"data":{
                  "label":"Contains","componentId":"xf.arr.contains",
                  "properties":{"column":"tags","value":"red"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"c",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"c","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = compiled.stages.iter().find(|s| s.node_id == "c").unwrap().sql.as_str();
        assert!(sql.contains("COALESCE(list_contains"), "missing COALESCE shield: {}", sql);
        assert!(sql.contains(", FALSE)"), "missing FALSE fallback: {}", sql);
    }

    #[test]
    fn join_with_same_key_name_uses_using_clause() {
        // When leftKey == rightKey, USING() dedupes the join column
        // and downstream `SELECT id FROM joined` is unambiguous.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"l","position":{"x":0,"y":0},"data":{
                  "label":"CSV L","componentId":"src.csv",
                  "properties":{"path":"/tmp/l.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"CSV R","componentId":"src.csv",
                  "properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"j","position":{"x":0,"y":0},"data":{
                  "label":"Join","componentId":"xf.join.inner",
                  "properties":{"leftKey":"customer_id","rightKey":"customer_id"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"l","target":"j",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"j",
                  "targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"j","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let join_sql = compiled.stages.iter().find(|s| s.node_id == "j").unwrap().sql.as_str();
        assert!(join_sql.contains("USING (\"customer_id\")"), "missing USING clause: {}", join_sql);
        assert!(!join_sql.contains("m.\"customer_id\" = r.\"customer_id\""), "should have used USING not ON: {}", join_sql);
    }

    #[test]
    fn join_with_different_key_names_excludes_right_key() {
        // Different key names: ON + EXCLUDE the right-side key so the
        // join column isn't duplicated in the output.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"l","position":{"x":0,"y":0},"data":{
                  "label":"CSV L","componentId":"src.csv",
                  "properties":{"path":"/tmp/l.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"CSV R","componentId":"src.csv",
                  "properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"j","position":{"x":0,"y":0},"data":{
                  "label":"Join","componentId":"xf.join.left",
                  "properties":{"leftKey":"customer_id","rightKey":"cust_id"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"l","target":"j",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"j",
                  "targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"j","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let join_sql = compiled.stages.iter().find(|s| s.node_id == "j").unwrap().sql.as_str();
        assert!(join_sql.contains("EXCLUDE (\"cust_id\")"), "missing EXCLUDE: {}", join_sql);
        assert!(join_sql.contains("m.\"customer_id\" = r.\"cust_id\""), "missing ON clause: {}", join_sql);
        assert!(join_sql.contains("LEFT JOIN"), "wrong kind: {}", join_sql);
    }

    #[test]
    fn join_composite_keys_two_columns() {
        // Composite keys via comma-separated input. Both sides must
        // have the same arity or compile fails loudly.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"l","position":{"x":0,"y":0},"data":{
                  "label":"CSV L","componentId":"src.csv",
                  "properties":{"path":"/tmp/l.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"CSV R","componentId":"src.csv",
                  "properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"j","position":{"x":0,"y":0},"data":{
                  "label":"Join","componentId":"xf.join.inner",
                  "properties":{"leftKey":"customer_id, order_date","rightKey":"customer_id, order_date"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"l","target":"j",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"j",
                  "targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"j","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let join_sql = compiled.stages.iter().find(|s| s.node_id == "j").unwrap().sql.as_str();
        assert!(
            join_sql.contains("USING (\"customer_id\", \"order_date\")"),
            "composite USING wrong: {}",
            join_sql
        );
    }

    #[test]
    fn semi_join_uses_exists_not_in() {
        // Anti-join was silently dropping all rows when the right side
        // had any NULL key, because `x NOT IN (subq with NULL)` evaluates
        // to UNKNOWN. NOT EXISTS doesn't have that quirk.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"l","position":{"x":0,"y":0},"data":{
                  "label":"CSV L","componentId":"src.csv",
                  "properties":{"path":"/tmp/l.csv","hasHeader":true}}},
                {"id":"r","position":{"x":0,"y":0},"data":{
                  "label":"CSV R","componentId":"src.csv",
                  "properties":{"path":"/tmp/r.csv","hasHeader":true}}},
                {"id":"j","position":{"x":0,"y":0},"data":{
                  "label":"Anti","componentId":"xf.anti",
                  "properties":{"leftKey":"id","rightKey":"id"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"l","target":"j",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"j",
                  "targetHandle":"lookup",
                  "data":{"connectionType":"lookup"}},
                {"id":"e3","source":"j","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let join_sql = compiled.stages.iter().find(|s| s.node_id == "j").unwrap().sql.as_str();
        assert!(join_sql.contains("NOT EXISTS"), "anti should use NOT EXISTS: {}", join_sql);
        assert!(!join_sql.contains("NOT IN"), "should not emit NOT IN: {}", join_sql);
    }

    #[test]
    fn row_hash_emits_concat_ws_with_casts() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"h","position":{"x":0,"y":0},"data":{
                  "label":"Hash","componentId":"xf.row_hash",
                  "properties":{"columns":["id","email","status"],"algorithm":"sha256","outputColumn":"fp"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"h",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"h","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("sha256("), "wrong algorithm: {}", sql);
        assert!(sql.contains("concat_ws('||'"), "wrong separator: {}", sql);
        assert!(sql.contains("CAST(\"id\" AS VARCHAR)"), "id not cast: {}", sql);
        assert!(sql.contains("CAST(\"email\" AS VARCHAR)"), "email not cast: {}", sql);
        assert!(sql.contains(" AS \"fp\""), "custom output column not honoured: {}", sql);
    }

    #[test]
    fn row_hash_default_algorithm_is_md5() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"h","position":{"x":0,"y":0},"data":{
                  "label":"Hash","componentId":"xf.row_hash",
                  "properties":{"columns":["id"]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"h",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"h","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("md5("), "default should be md5: {}", sql);
        assert!(sql.contains(" AS \"_row_hash\""), "default output column wrong: {}", sql);
    }

    #[test]
    fn audit_emits_selected_columns_only() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"a","position":{"x":0,"y":0},"data":{
                  "label":"Audit","componentId":"xf.audit",
                  "properties":{"loadedAt":true,"loadedDate":false,"source":"orders_etl","batchId":"2026-05-27"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"a",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"a","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("current_timestamp AS _loaded_at"), "loaded_at missing: {}", sql);
        assert!(!sql.contains("_loaded_date"), "loaded_date should be off: {}", sql);
        assert!(sql.contains("'orders_etl' AS _source"), "source literal missing: {}", sql);
        assert!(sql.contains("'2026-05-27' AS _batch_id"), "batch_id missing: {}", sql);
    }

    #[test]
    fn fill_constant_string_value_quoted() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"f","position":{"x":0,"y":0},"data":{
                  "label":"Fill","componentId":"xf.fill_constant",
                  "properties":{"column":"status","value":"unknown"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"f",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("COALESCE(\"status\", 'unknown')"), "string literal not quoted: {}", sql);
    }

    #[test]
    fn fill_constant_numeric_value_unquoted() {
        // Bare numbers (`0`, `-1.5`) pass through unquoted so DuckDB
        // sees a numeric literal and doesn't try to cast a string.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/x.csv","hasHeader":true}}},
                {"id":"f","position":{"x":0,"y":0},"data":{
                  "label":"Fill","componentId":"xf.fill_constant",
                  "properties":{"column":"qty","value":"0"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"f",
                  "data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"k",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sql = &compiled.stages[1].sql;
        assert!(sql.contains("COALESCE(\"qty\", 0)"), "numeric literal got quoted: {}", sql);
    }

    #[test]
    fn csv_without_declared_schema_uses_autodetect() {
        // Inverse check: no schema -> no columns clause, so DuckDB
        // falls back to its normal autodetect.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/d.csv","hasHeader":true}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        assert!(
            !compiled.stages[0].sql.contains("types = {"),
            "should not emit types clause without a declared schema: {}",
            compiled.stages[0].sql
        );
    }

    #[test]
    fn cloud_parquet_source_projects_declared_columns() {
        // audit B1: a cloud parquet source must honor the `columns`
        // projection like the local builder (delegation), not read SELECT *.
        let sql = build_cloud_source(
            "s3",
            &serde_json::json!({"format": "parquet", "path": "s3://b/k.parquet", "columns": "id, amount"}),
            None,
        )
        .unwrap();
        assert!(
            sql.contains("SELECT \"id\", \"amount\" FROM read_parquet('s3://b/k.parquet')"),
            "cloud parquet must project declared columns, got: {}",
            sql
        );
    }

    #[test]
    fn cloud_csv_source_threads_declared_schema() {
        // audit B1: a cloud CSV source must honor a Schema-panel declaration
        // via types= (issue #3 parity), not a bare read_csv_auto.
        let cols = vec![duckle_metadata::Column {
            name: "amt".into(),
            data_type: duckle_metadata::DataType::String,
            nullable: true,
            primary_key: None,
            format: None,
        }];
        let sql = build_cloud_source(
            "s3",
            &serde_json::json!({"format": "csv", "path": "s3://b/k.csv", "hasHeader": true}),
            Some(&cols),
        )
        .unwrap();
        assert!(
            sql.contains("types = {") && sql.contains("'amt': 'VARCHAR'"),
            "cloud csv must thread declared schema via types=, got: {}",
            sql
        );
    }

    #[test]
    fn csv_reject_and_split_partition_bad_rows() {
        // issue #15: a declared DATE column must yield a reject relation of the
        // rows that fail to parse (raw text), and a tolerant split main that
        // drops exactly those rows. The two predicates must be complementary.
        let cols = vec![duckle_metadata::Column {
            name: "order_date".into(),
            data_type: duckle_metadata::DataType::Date,
            nullable: true,
            primary_key: None,
            format: None,
        }];
        let props = serde_json::json!({"path": "orders.csv", "hasHeader": true});

        let reject = build_csv_reject_sql(&props, Some(&cols), false)
            .expect("a declared DATE column must produce a reject relation");
        // raw text read + present-but-unparseable predicate
        assert!(reject.contains("'order_date': 'VARCHAR'"), "reject reads raw text: {reject}");
        assert!(
            reject.contains("try_cast(\"order_date\" AS DATE) IS NULL")
                && reject.contains("\"order_date\" <> ''"),
            "reject keeps only present-but-unparseable values: {reject}"
        );

        let split = build_csv_source_split(&props, Some(&cols), false);
        // tolerant: casts back to the declared type and drops the failing rows
        assert!(
            split.contains("try_cast(\"order_date\" AS DATE) AS \"order_date\"")
                && split.contains("WHERE NOT ("),
            "split main casts + excludes the rejected rows: {split}"
        );

        // No declared schema (or all-text schema) => nothing to reject.
        assert!(build_csv_reject_sql(&props, None, false).is_none());
        let text_cols = vec![duckle_metadata::Column {
            name: "name".into(),
            data_type: duckle_metadata::DataType::String,
            nullable: true,
            primary_key: None,
            format: None,
        }];
        assert!(build_csv_reject_sql(&props, Some(&text_cols), false).is_none());
    }

    #[test]
    fn parquet_sink_forwards_row_group_size() {
        // issue-#16 perf report: the "Row group size" UI field was dropped by
        // build_parquet_sink, so DuckDB used its internal default. Forward it.
        let sql = build_parquet_sink(
            &serde_json::json!({"path": "out.parquet", "rowGroupSize": 1_000_000}),
            "input",
        );
        assert!(sql.contains("ROW_GROUP_SIZE 1000000"), "row group size not forwarded: {sql}");

        // A numeric string (forms sometimes serialize integers as strings).
        let sql_str = build_parquet_sink(
            &serde_json::json!({"path": "out.parquet", "rowGroupSize": "250000"}),
            "input",
        );
        assert!(sql_str.contains("ROW_GROUP_SIZE 250000"), "string row group size not forwarded: {sql_str}");

        // Absent or zero => omit it, leaving DuckDB's default.
        let sql_none = build_parquet_sink(&serde_json::json!({"path": "out.parquet"}), "input");
        assert!(!sql_none.contains("ROW_GROUP_SIZE"), "must not emit a default: {sql_none}");
        let sql_zero = build_parquet_sink(
            &serde_json::json!({"path": "out.parquet", "rowGroupSize": 0}),
            "input",
        );
        assert!(!sql_zero.contains("ROW_GROUP_SIZE"), "zero must be ignored: {sql_zero}");
    }

    #[test]
    fn parquet_sink_partition_guard() {
        // Partitioned write gets a fail-fast guard (default cap 10000).
        let guarded = build_parquet_sink(
            &serde_json::json!({"path": "out", "partitionBy": ["sender", "receiver"]}),
            "input",
        );
        assert!(guarded.contains("PARTITION_BY (\"sender\", \"receiver\")"), "{guarded}");
        assert!(
            guarded.contains("approx_count_distinct")
                && guarded.contains("> 10000")
                && guarded.contains("error("),
            "partitioned write must be guarded: {guarded}"
        );

        // maxPartitions = 0 disables the guard (explicit opt-out).
        let unlimited = build_parquet_sink(
            &serde_json::json!({"path": "out", "partitionBy": ["sender"], "maxPartitions": 0}),
            "input",
        );
        assert!(unlimited.contains("PARTITION_BY"), "{unlimited}");
        assert!(!unlimited.contains("error("), "cap 0 must skip the guard: {unlimited}");

        // No partitioning => no guard, plain source.
        let plain = build_parquet_sink(&serde_json::json!({"path": "out.parquet"}), "input");
        assert!(!plain.contains("approx_count_distinct") && !plain.contains("error("), "{plain}");
    }

    #[test]
    fn cloud_csv_sink_honors_options_but_not_partitionby() {
        // audit B1: a cloud CSV sink must honor delimiter/nullValue (ignored
        // before), but must NOT emit PARTITION_BY (unvalidated over httpfs).
        let sql = build_cloud_sink(
            &serde_json::json!({
                "format": "csv", "path": "s3://b/out.csv",
                "delimiter": "|", "nullValue": "NA", "partitionBy": "id"
            }),
            "v",
        )
        .unwrap();
        assert!(
            sql.contains("FORMAT CSV") && sql.contains("DELIM '|'") && sql.contains("NULLSTR 'NA'"),
            "cloud csv sink must honor options, got: {}",
            sql
        );
        assert!(
            !sql.contains("PARTITION_BY"),
            "cloud sink must not emit PARTITION_BY, got: {}",
            sql
        );
        assert!(sql.contains("'s3://b/out.csv'"), "must write to the cloud path, got: {}", sql);
    }

    #[test]
    fn cloud_source_rejects_avro_and_orc_formats() {
        // audit pass-3: the cloud reader has no Avro/ORC path; selecting either
        // used to fall through to read_csv_auto on the binary container. It must
        // now fail loud instead.
        for fmt in ["avro", "orc"] {
            let err = build_cloud_source(
                "s3",
                &serde_json::json!({"format": fmt, "path": format!("s3://b/k.{}", fmt)}),
                None,
            )
            .unwrap_err();
            assert!(
                err.to_string().to_lowercase().contains("not supported"),
                "cloud {} source should fail loud, got: {:?}",
                fmt,
                err
            );
        }
    }

    #[test]
    fn cloud_sink_rejects_avro_and_orc_formats() {
        // audit pass-3: no Avro/ORC writer exists; selecting either used to
        // silently write Parquet to the user's .avro/.orc path. Fail loud now.
        for fmt in ["avro", "orc"] {
            let err = build_cloud_sink(
                &serde_json::json!({"format": fmt, "path": format!("s3://b/out.{}", fmt)}),
                "v",
            )
            .unwrap_err();
            assert!(
                err.to_string().to_lowercase().contains("not supported"),
                "cloud {} sink should fail loud, got: {:?}",
                fmt,
                err
            );
        }
    }

    #[test]
    fn csv_windows_1252_encoding_is_remapped_to_cp1252() {
        // audit pass-3: DuckDB's CSV reader rejects the spelling "windows-1252"
        // (it wants CP1252); the UI/docs offer "Windows-1252", so the engine
        // must remap it rather than aborting the read.
        let sql = build_csv_source(
            &serde_json::json!({"path": "f.csv", "hasHeader": true, "encoding": "windows-1252"}),
            None,
        );
        assert!(sql.contains("encoding='CP1252'"), "windows-1252 must remap to CP1252, got: {}", sql);
        // latin-1 (a DuckDB-accepted spelling) passes through unchanged.
        let latin = build_csv_source(
            &serde_json::json!({"path": "f.csv", "hasHeader": true, "encoding": "latin-1"}),
            None,
        );
        assert!(latin.contains("encoding='latin-1'"), "latin-1 must pass through, got: {}", latin);
    }

    #[test]
    fn db_sink_unknown_mode_fails_loud_not_destructive_overwrite() {
        // audit pass-3: snk.sqlite/snk.duckdb used to DROP+CREATE for ANY
        // unrecognized mode, so a typo like "appnd" silently wiped the table.
        let err = build_sink_sql(
            "snk.sqlite",
            &serde_json::json!({"tableName": "t", "mode": "appnd"}),
            "v",
            &[],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("write mode") && err.to_string().contains("appnd"),
            "an unknown mode must fail loud, got: {:?}",
            err
        );
        // The explicit "overwrite" default is still the destructive recreate.
        let ok = build_sink_sql(
            "snk.sqlite",
            &serde_json::json!({"tableName": "t", "mode": "overwrite"}),
            "v",
            &[],
        )
        .unwrap();
        assert!(ok.contains("DROP TABLE IF EXISTS"), "overwrite stays a recreate, got: {}", ok);
    }

    #[test]
    fn relational_sink_append_creates_table_on_first_write() {
        // A MotherDuck (relational) sink in append mode used to emit a bare
        // INSERT INTO, which fails the first time the target doesn't exist
        // (e.g. appending ledger rows from a foreach). Append now creates the
        // table from the upstream's types before inserting, like truncate/upsert.
        let sql = build_sink_sql(
            "snk.motherduck",
            &serde_json::json!({"database": "my_db", "tableName": "process_ledger", "mode": "append"}),
            "v",
            &[],
        )
        .unwrap();
        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS") && sql.contains("LIMIT 0"),
            "append must create-if-missing from upstream types, got: {}",
            sql
        );
        assert!(sql.contains("INSERT INTO"), "append must still insert, got: {}", sql);
    }

    #[test]
    fn merge_mode_emits_partial_column_merge() {
        // Issue #39: merge updates only non-key columns the source carries and
        // inserts new rows; the key column is never in the UPDATE SET.
        let sql = build_sink_sql(
            "snk.duckdb",
            &serde_json::json!({"tableName": "t", "mode": "merge", "conflictColumns": ["k"]}),
            "v",
            &["k".to_string(), "a".to_string(), "b".to_string()],
        )
        .unwrap();
        assert!(sql.contains("MERGE INTO"), "got: {}", sql);
        assert!(sql.contains("ON (tgt.\"k\" = src.\"k\")"), "got: {}", sql);
        // The UPDATE SET lists exactly the non-key columns (the key is matched
        // on, never updated).
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET \"a\" = src.\"a\", \"b\" = src.\"b\" WHEN NOT MATCHED"),
            "UPDATE SET must list only the non-key columns, got: {}",
            sql
        );
        assert!(
            sql.contains("WHEN NOT MATCHED THEN INSERT (\"k\", \"a\", \"b\") VALUES (src.\"k\", src.\"a\", src.\"b\")"),
            "INSERT must list all source columns, got: {}",
            sql
        );
    }

    #[test]
    fn merge_mode_rejected_for_non_duckdb_target() {
        let err = build_sink_sql(
            "snk.postgres",
            &serde_json::json!({"tableName": "t", "mode": "merge", "conflictColumns": ["k"]}),
            "v",
            &["k".to_string(), "a".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("merge"), "got: {:?}", err);
    }

    #[test]
    fn merge_mode_needs_input_columns() {
        let err = build_sink_sql(
            "snk.duckdb",
            &serde_json::json!({"tableName": "t", "mode": "merge", "conflictColumns": ["k"]}),
            "v",
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains("input columns"), "got: {:?}", err);
    }

    #[test]
    fn db_sink_upsert_rejects_empty_conflict_columns() {
        // audit pass-3: conflictColumns=[""] used to pass the length-based
        // guard and emit a zero-length quoted identifier. The empty entry is
        // now dropped, so the "needs a conflict column" guard fires.
        let err = build_sink_sql(
            "snk.sqlite",
            &serde_json::json!({"tableName": "t", "mode": "upsert", "conflictColumns": ["", "  "]}),
            "v",
            &[],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("conflict column"),
            "blank conflict columns must be rejected, got: {:?}",
            err
        );
    }

    #[test]
    fn aggregate_missing_function_on_named_column_fails_loud() {
        // audit pass-3: {column: "amount"} with no function used to silently
        // become COUNT(amount); it must require an explicit function now.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let err = build_aggregate(
            &ni,
            &serde_json::json!({"groupBy": ["g"], "aggregations": [{"column": "amount", "output": "total"}]}),
            GroupMode::Plain,
        )
        .unwrap_err();
        assert!(err.contains("needs a function"), "named column without function must fail, got: {}", err);
        // A bare row count (column "*", no function) is still allowed as COUNT.
        let ok = build_aggregate(
            &ni,
            &serde_json::json!({"groupBy": ["g"], "aggregations": [{"column": "*", "output": "n"}]}),
            GroupMode::Plain,
        )
        .unwrap();
        assert!(ok.contains("COUNT(*)"), "count(*) default for '*' stays, got: {}", ok);
    }

    #[test]
    fn aggwin_with_order_by_pins_full_partition_frame() {
        // audit pass-3: an ORDER BY in the window without an explicit frame
        // silently becomes a running aggregate. xf.aggwin keeps a whole-
        // partition total on every row, so the full frame must be pinned.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_window_aggregate(
            &ni,
            &serde_json::json!({"function": "sum", "column": "amt", "partitionBy": ["region"], "orderBy": ["dt"]}),
        )
        .unwrap();
        assert!(
            sql.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
            "aggwin with orderBy must pin the full-partition frame, got: {}",
            sql
        );
    }

    #[test]
    fn kafka_offset_latest_maps_to_the_latest_sentinel() {
        // audit pass-3: the UI emits offset=latest/earliest; the engine reads
        // it onto its start_offset sentinel (-2 = latest, -1 = earliest).
        let p = pipeline_from_json(
            r#"{"nodes":[
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"Kafka","componentId":"src.kafka","properties":{"brokers":"b:9092","topic":"t","offset":"latest"}}},
                {"id":"o","position":{"x":1,"y":0},"data":{"label":"CSV","componentId":"snk.csv","properties":{"path":"/tmp/out.csv"}}}
            ],"edges":[
                {"id":"e","source":"k","target":"o","data":{"connectionType":"main"}}
            ]}"#,
        );
        let compiled = compile(&p).expect("kafka plan compiles");
        let spec = compiled
            .stages
            .iter()
            .find_map(|s| match s.runtime.as_ref() {
                Some(RuntimeSpec::KafkaSource(k)) => Some(k),
                _ => None,
            })
            .expect("kafka source spec");
        assert_eq!(spec.start_offset, -2, "offset=latest must map to the -2 sentinel");
    }

    #[test]
    fn csv_partial_declared_schema_uses_types_not_columns() {
        // Regression (audit B2): a Schema-panel declaration that covers only
        // SOME of a wider file's columns must emit `types = {...}` (name-
        // match, partial-ok), NOT `columns = {...}` (positional, requires
        // the full schema). The old `columns` emission made read_csv_auto
        // hard-fail with a sniffer arity error for the common "declare just
        // the column I care about" case.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/wide.csv","hasHeader":true},
                  "schema":[
                    {"name":"amt","type":"string","nullable":true}
                  ]}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src_sql = &compiled.stages[0].sql;
        assert!(
            src_sql.contains("types = {") && src_sql.contains("'amt': 'VARCHAR'"),
            "partial declaration must emit types= with the declared column: {}",
            src_sql
        );
        assert!(
            !src_sql.contains("columns = {"),
            "partial declaration must NOT emit columns= (positional, full-schema): {}",
            src_sql
        );
    }

    #[test]
    fn quack_source_emits_attach_with_secret() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"Quack","componentId":"src.quack",
                  "properties":{"host":"duck.example.com","port":9494,
                                "token":"super_secret","tableName":"orders"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"snk.csv",
                  "properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        // Single-consumer quack now materializes via the attach-parquet path,
        // so the ATTACH + secret live on the spec; concatenate spec.attach +
        // body to assert the same logic regardless of where it lands.
        let stage = &compiled.stages[0];
        let src_sql = match stage.runtime.as_ref() {
            Some(RuntimeSpec::AttachParquetSource(s)) => format!("{}{}", s.attach, s.body),
            _ => stage.sql.clone(),
        };
        assert!(
            src_sql.contains("CREATE OR REPLACE SECRET duckle_quack_secret"),
            "missing SECRET creation: {}",
            src_sql
        );
        assert!(src_sql.contains("TYPE QUACK"), "wrong SECRET type: {}", src_sql);
        assert!(src_sql.contains("'super_secret'"), "token not in SECRET: {}", src_sql);
        assert!(
            src_sql.contains("ATTACH 'quack:duck.example.com:9494'"),
            "wrong ATTACH URL: {}",
            src_sql
        );
        assert!(src_sql.contains("AS duckle_src"), "wrong alias: {}", src_sql);
        assert!(src_sql.contains("READ_ONLY"), "missing READ_ONLY: {}", src_sql);
        assert!(
            src_sql.contains("SELECT * FROM duckle_src"),
            "missing SELECT from alias: {}",
            src_sql
        );
    }

    #[test]
    fn quack_source_omits_secret_when_no_token() {
        // Unauthenticated test servers: leave the SECRET off entirely
        // rather than emitting an empty TOKEN clause.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"Quack","componentId":"src.quack",
                  "properties":{"host":"localhost","tableName":"t"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1",
                  "data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let stage = &compiled.stages[0];
        let src_sql = match stage.runtime.as_ref() {
            Some(RuntimeSpec::AttachParquetSource(s)) => format!("{}{}", s.attach, s.body),
            _ => stage.sql.clone(),
        };
        assert!(
            !src_sql.contains("CREATE OR REPLACE SECRET"),
            "should not emit empty SECRET: {}",
            src_sql
        );
        // Default port 9494 is appended when host has no explicit port.
        assert!(
            src_sql.contains("'quack:localhost:9494'"),
            "missing default port: {}",
            src_sql
        );
    }

    #[test]
    fn attach_parquet_source_keeps_fast_path_when_feeding_reject_wired_filter() {
        // Regression: a reject-wired filter reads its input twice, but for an
        // attach-parquet source (quack / postgres / ...) the rows are already
        // materialized once to a local parquet, so it must NOT be counted as two
        // consumers - it must keep the COPY-to-parquet fast path, not fall back
        // to a run-db table insert.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"Quack","componentId":"src.quack",
                  "properties":{"host":"localhost","tableName":"orders"}}},
                {"id":"f1","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"amount > 0"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"Pass","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/pass.parquet"}}},
                {"id":"k2","position":{"x":0,"y":0},"data":{
                  "label":"Rejected","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/rej.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"f1","data":{"connectionType":"main"}},
                {"id":"e2","source":"f1","target":"k1","data":{"connectionType":"main"}},
                {"id":"e3","source":"f1","sourceHandle":"reject","target":"k2","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let src = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "s1")
            .expect("source stage");
        assert!(
            matches!(src.runtime.as_ref(), Some(RuntimeSpec::AttachParquetSource(_))),
            "attach-parquet source feeding a reject-wired filter must keep the COPY-to-parquet fast path, got sql: {}",
            src.sql
        );
    }

    #[test]
    fn zip_arrays_to_table_pivots_to_real_columns() {
        // xf.zip: a row carrying a headings list + a list of row-arrays becomes
        // one output row per inner array with a real column per heading. It
        // explodes the values, aligns by position, and PIVOTs to columns. As a
        // data-driven PIVOT it must materialize as a TABLE, never a lazy VIEW.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"JSON","componentId":"src.json",
                  "properties":{"path":"/tmp/in.json"}}},
                {"id":"z1","position":{"x":0,"y":0},"data":{
                  "label":"Zip","componentId":"xf.zip",
                  "properties":{"headingsColumn":"headings","valuesColumn":"rows"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"snk.csv",
                  "properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"z1","data":{"connectionType":"main"}},
                {"id":"e2","source":"z1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let zip = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "z1")
            .expect("zip stage");
        assert!(zip.sql.contains("PIVOT"), "zip must emit a PIVOT: {}", zip.sql);
        assert!(
            zip.sql.contains("UNNEST(\"rows\")"),
            "zip must explode the values column: {}",
            zip.sql
        );
        assert!(
            zip.sql.contains("\"headings\""),
            "zip must reference the headings column: {}",
            zip.sql
        );
        assert!(
            zip.sql.contains("CREATE OR REPLACE TABLE \"z1\""),
            "a data-driven pivot must materialize as a TABLE: {}",
            zip.sql
        );
    }

    #[test]
    fn motherduck_inline_token_uses_set_not_query_param() {
        // Regression: an inline MotherDuck token must be applied via
        // SET motherduck_token, NOT as an `md:db?motherduck_token=...` query
        // param (which made MotherDuck treat the whole string as the db name).
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{
                  "label":"MD","componentId":"snk.motherduck",
                  "properties":{"database":"my_db","token":"SECRET_TOK","schemaName":"main","tableName":"orders","mode":"overwrite"}}}
              ],
              "edges": [
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let sink = compiled
            .stages
            .iter()
            .find(|s| s.node_id == "k1")
            .expect("sink stage");
        assert!(
            sink.sql.contains("SET motherduck_token='SECRET_TOK'"),
            "inline token must be applied via SET: {}",
            sink.sql
        );
        assert!(
            sink.sql.contains("ATTACH 'md:my_db'"),
            "must ATTACH md:db cleanly: {}",
            sink.sql
        );
        assert!(
            !sink.sql.contains("md:my_db?motherduck_token"),
            "must NOT use the broken query-param form: {}",
            sink.sql
        );
    }

    #[test]
    fn rejects_cycles() {
        let p = pipeline_from_json(
            r#"{
              "nodes":[
                {"id":"a","position":{"x":0,"y":0},"data":{"label":"A","componentId":"xf.filter","properties":{}}},
                {"id":"b","position":{"x":0,"y":0},"data":{"label":"B","componentId":"xf.filter","properties":{}}}
              ],
              "edges":[
                {"id":"e1","source":"a","target":"b","data":{"connectionType":"main"}},
                {"id":"e2","source":"b","target":"a","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        assert!(compile(&p).is_err());
    }

    #[test]
    fn excel_source_honors_declared_schema(){
        // Issue #25: read_xlsx has no type map, so a declared schema must be
        // applied as an all_varchar read + cast/project wrapper. No declared
        // schema -> unchanged read (all columns, auto-inferred).
        use duckle_metadata::{Column, DataType};
        let col = |name: &str, dt: DataType, fmt: Option<&str>| Column {
            name: name.into(),
            data_type: dt,
            nullable: true,
            primary_key: None,
            format: fmt.map(|s| s.to_string()),
        };
        let props = serde_json::json!({ "path": "/tmp/book.xlsx", "hasHeader": true });

        let plain = builders::build_excel_source(&props, None);
        assert!(
            plain.trim_start().starts_with("SELECT * FROM read_xlsx"),
            "plain read should be unchanged: {}",
            plain
        );
        assert!(!plain.contains("all_varchar"), "plain must not force all_varchar: {}", plain);

        // Keep id (BIGINT) + name (VARCHAR) + d (DATE w/ format); a file column
        // not in this list ("junk") is dropped by the projection.
        let declared = vec![
            col("id", DataType::Int64, None),
            col("name", DataType::String, None),
            col("d", DataType::Date, Some("%d/%m/%Y")),
        ];
        let typed = builders::build_excel_source(&props, Some(&declared));
        assert!(typed.contains("all_varchar = true"), "typed must read all_varchar: {}", typed);
        assert!(typed.contains("CAST(\"id\" AS BIGINT)"), "id cast missing: {}", typed);
        assert!(
            typed.contains("try_strptime(\"d\", '%d/%m/%Y')::DATE"),
            "date format parse missing: {}",
            typed
        );
        assert!(typed.contains("\"name\""), "name column missing: {}", typed);
        // Explicit projection over the inner read, not SELECT *.
        assert!(
            typed.contains("FROM (SELECT * FROM read_xlsx("),
            "should wrap the raw read: {}",
            typed
        );
        assert!(!typed.contains("junk"), "non-declared column leaked: {}", typed);
    }

    #[test]
    fn dbt_model_name_sanitized_consistently() {
        // The planner and the inline-project scaffolder must agree on the table
        // name, or the engine reads back a name dbt never created.
        assert_eq!(sanitize_dbt_model_name("my-model"), "my_model");
        assert_eq!(sanitize_dbt_model_name("test.model v2"), "test_model_v2");
        assert_eq!(sanitize_dbt_model_name("--weird--"), "weird");
        assert_eq!(sanitize_dbt_model_name(""), "duckle_model");
        assert_eq!(sanitize_dbt_model_name("ok_name"), "ok_name");
    }

    #[test]
    fn distinct_orderby_without_columns_errors() {
        // orderBy with no key columns would be silently dropped by a bare
        // DISTINCT - the planner must reject it instead.
        let doc = pipeline_from_json(
            r#"{"name":"t","nodes":[
                {"id":"s","type":"source","position":{"x":0,"y":0},"data":{"label":"s","componentId":"src.csv","properties":{"path":"x.csv"}}},
                {"id":"d","type":"transform","position":{"x":0,"y":0},"data":{"label":"d","componentId":"xf.distinct","properties":{"orderBy":["a"]}}},
                {"id":"k","type":"sink","position":{"x":0,"y":0},"data":{"label":"k","componentId":"snk.csv","properties":{"path":"o.csv"}}}
            ],"edges":[
                {"id":"e1","source":"s","target":"d","sourceHandle":"main","targetHandle":"main","data":{"connectionType":"main"}},
                {"id":"e2","source":"d","target":"k","sourceHandle":"main","targetHandle":"main","data":{"connectionType":"main"}}
            ]}"#,
        );
        let err = compile(&doc).unwrap_err();
        assert!(
            format!("{:?}", err).contains("orderBy"),
            "expected an orderBy validation error, got {:?}",
            err
        );
    }

    #[test]
    fn second_lookup_edge_rejected() {
        // A join reads one lookup via first_lookup(); a 2nd lookup edge would
        // be silently dropped, so the planner must reject it (not xf.map).
        let doc = pipeline_from_json(
            r#"{"name":"t","nodes":[
                {"id":"s","type":"source","position":{"x":0,"y":0},"data":{"label":"s","componentId":"src.csv","properties":{"path":"s.csv"}}},
                {"id":"r1","type":"source","position":{"x":0,"y":0},"data":{"label":"r1","componentId":"src.csv","properties":{"path":"r1.csv"}}},
                {"id":"r2","type":"source","position":{"x":0,"y":0},"data":{"label":"r2","componentId":"src.csv","properties":{"path":"r2.csv"}}},
                {"id":"j","type":"transform","position":{"x":0,"y":0},"data":{"label":"j","componentId":"xf.join.inner","properties":{"leftKey":"id","rightKey":"id"}}},
                {"id":"k","type":"sink","position":{"x":0,"y":0},"data":{"label":"k","componentId":"snk.csv","properties":{"path":"o.csv"}}}
            ],"edges":[
                {"id":"e1","source":"s","target":"j","sourceHandle":"main","targetHandle":"main","data":{"connectionType":"main"}},
                {"id":"e2","source":"r1","target":"j","sourceHandle":"main","targetHandle":"lookup","data":{"connectionType":"lookup"}},
                {"id":"e3","source":"r2","target":"j","sourceHandle":"main","targetHandle":"lookup","data":{"connectionType":"lookup"}},
                {"id":"e4","source":"j","target":"k","sourceHandle":"main","targetHandle":"main","data":{"connectionType":"main"}}
            ]}"#,
        );
        let err = compile(&doc).unwrap_err();
        assert!(
            format!("{:?}", err).contains("lookup"),
            "expected a lookup fan-in error, got {:?}",
            err
        );
    }

    #[test]
    fn dbt_exposes_all_main_inputs() {
        // xf.dbt is multi-main: both upstream tables should land in from_views
        // (exposed to dbt as var('duckle_inputs')), not just the first.
        let doc = pipeline_from_json(
            r#"{"name":"t","nodes":[
                {"id":"a","type":"source","position":{"x":0,"y":0},"data":{"label":"a","componentId":"src.csv","properties":{"path":"a.csv"}}},
                {"id":"b","type":"source","position":{"x":0,"y":0},"data":{"label":"b","componentId":"src.csv","properties":{"path":"b.csv"}}},
                {"id":"d","type":"transform","position":{"x":0,"y":0},"data":{"label":"d","componentId":"xf.dbt","properties":{"model":"SELECT 1 AS x","modelName":"m"}}}
            ],"edges":[
                {"id":"e1","source":"a","target":"d","sourceHandle":"main","targetHandle":"main","data":{"connectionType":"main"}},
                {"id":"e2","source":"b","target":"d","sourceHandle":"main","targetHandle":"main","data":{"connectionType":"main"}}
            ]}"#,
        );
        let stages = compile(&doc).unwrap().stages;
        let dbt = stages.iter().find(|s| s.node_id == "d").expect("dbt stage");
        match &dbt.runtime {
            Some(RuntimeSpec::Dbt(spec)) => {
                assert_eq!(spec.from_views.len(), 2, "both inputs expected: {:?}", spec.from_views);
                assert!(spec.from_views.contains(&"a".to_string()));
                assert!(spec.from_views.contains(&"b".to_string()));
            }
            other => panic!("expected a Dbt runtime spec, got {:?}", other),
        }
    }
