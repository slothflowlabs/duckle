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
    fn map_qualifies_columns_whose_names_contain_spaces() {
        // #214: with a lookup present the qualifier ran over double-quoted
        // identifiers as if they were bare text. `"main.col one"` came out as
        // `""o"."col" one"`, whose leading `""` DuckDB rejects as a zero-length
        // delimited identifier, and `main."col one"` was left unqualified so
        // DuckDB reported an unknown table `main`. Both forms must resolve, and
        // must agree with what the no-lookup path (strip_port_prefixes) yields.
        let aliases: std::collections::BTreeMap<String, String> = [
            ("main".to_string(), "\"o\"".to_string()),
            ("lookup_1".to_string(), "\"c\"".to_string()),
        ]
        .into_iter()
        .collect();

        // The canonical form the mapper UI now emits.
        assert_eq!(
            qualify_port_refs("main.\"col one\"", &aliases),
            "\"o\".\"col one\""
        );
        // The form saved by older pipelines, and what the issue reported.
        assert_eq!(
            qualify_port_refs("\"main.col one\"", &aliases),
            "\"o\".\"col one\""
        );
        assert_eq!(
            qualify_port_refs("\"lookup_1.col one\"", &aliases),
            "\"c\".\"col one\""
        );
        // An embedded escaped quote survives a round trip.
        assert_eq!(
            qualify_port_refs("main.\"od\"\"d\"", &aliases),
            "\"o\".\"od\"\"d\""
        );
    }

    #[test]
    fn map_quoting_fix_does_not_touch_expressions_or_foreign_identifiers() {
        // The dangerous over-fix is to quote everything after `main.` up to a
        // delimiter, which would swallow operators. These guard that only text
        // the user already delimited is ever treated as a column name.
        let aliases: std::collections::BTreeMap<String, String> = [
            ("main".to_string(), "\"o\"".to_string()),
            ("lookup_1".to_string(), "\"c\"".to_string()),
        ]
        .into_iter()
        .collect();

        assert_eq!(
            qualify_port_refs("main.amount * 1.08", &aliases),
            "\"o\".\"amount\" * 1.08"
        );
        assert_eq!(
            qualify_port_refs("main.a || main.b", &aliases),
            "\"o\".\"a\" || \"o\".\"b\""
        );
        assert_eq!(
            qualify_port_refs("UPPER(main.x)", &aliases),
            "UPPER(\"o\".\"x\")"
        );
        // Struct field access must keep working: only the first segment is a
        // column, the rest is DuckDB struct navigation.
        assert_eq!(
            qualify_port_refs("main.payload.id", &aliases),
            "\"o\".\"payload\".id"
        );
        // A quoted identifier that is not a port reference is copied verbatim.
        assert_eq!(
            qualify_port_refs("\"some other col\"", &aliases),
            "\"some other col\""
        );
        // An unknown prefix inside quotes is not a port reference either.
        assert_eq!(
            qualify_port_refs("\"notaport.col one\"", &aliases),
            "\"notaport.col one\""
        );
        // A double quote inside a string literal must not start an identifier.
        assert_eq!(
            qualify_port_refs("main.id || 'he said \"main.x\"'", &aliases),
            "\"o\".\"id\" || 'he said \"main.x\"'"
        );
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

    /// A responsePath that is not already a JSON Pointer must still find rows.
    ///
    /// The value goes to `Value::pointer`, which needs a leading `/` and
    /// returns None otherwise - and None is indistinguishable from "no rows",
    /// so `data` yielded an empty result with no error and nothing to debug.
    /// This repo's own example above used `data`, which is how long it went
    /// unnoticed.
    #[test]
    fn a_response_path_that_is_not_a_pointer_is_still_understood() {
        use crate::plan::builders::json_pointer_path;

        // The reported case, and the JSONPath spelling the field label invites.
        assert_eq!(json_pointer_path("data", true), "/data");
        assert_eq!(json_pointer_path("$.data[*]", true), "/data");
        assert_eq!(json_pointer_path("result.items", true), "/result/items");
        assert_eq!(json_pointer_path("$.result.items[0]", true), "/result/items");
        assert_eq!(json_pointer_path("  data  ", true), "/data");

        // Already a pointer: untouched, including a literal dot, which is a
        // legal pointer segment and therefore means what it says.
        assert_eq!(json_pointer_path("/data", true), "/data");
        assert_eq!(json_pointer_path("/d/results", true), "/d/results");
        assert_eq!(json_pointer_path("/a.b", true), "/a.b");

        // Empty stays empty: that means "the whole response is the row set".
        assert_eq!(json_pointer_path("", true), "");
        assert_eq!(json_pointer_path("   ", true), "");

        // XML uses the same property as an ELEMENT path, where dots are
        // legitimate characters, so it is left exactly as authored.
        assert_eq!(json_pointer_path("Envelope/Body", false), "Envelope/Body");
        assert_eq!(json_pointer_path("a.b", false), "a.b");
    }

    /// The legacy `jsonPath` field was offered by the form and read by nobody.
    #[test]
    fn the_legacy_json_path_field_is_honoured_rather_than_ignored() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{
                  "label":"REST","componentId":"src.rest",
                  "properties":{"url":"https://example.com/users",
                                "jsonPath":"$.data[*]"}}},
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
        let spec = compiled
            .stages
            .iter()
            .find_map(|s| match &s.runtime {
                Some(crate::plan::RuntimeSpec::RestSource(spec)) => Some(spec),
                _ => None,
            })
            .expect("the REST source did not compile");
        assert_eq!(
            spec.response_path, "/data",
            "a pipeline that set only the legacy field located no rows"
        );
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
    fn ducklake_custom_sql_resolves_catalog_schemas_via_search_path() {
        // #117: custom SQL on a ducklake source references the lake's OWN schemas
        // (e.g. data.weights) without the duckle_src prefix, exactly as the query
        // runs in the DuckLake CLI. The source must materialize once via
        // COPY-to-parquet with the attached catalog on the search_path (so the
        // unqualified names resolve), NOT a lazy VIEW or bare TABLE that would
        // re-resolve the names against the run database and fail with
        // "schema does not exist".
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"src","position":{"x":0,"y":0},"data":{
                  "label":"lake","componentId":"src.ducklake",
                  "properties":{"path":"x.ducklake","mode":"sql","sql":"SELECT * FROM data.weights"}}},
                {"id":"out","position":{"x":0,"y":0},"data":{
                  "label":"out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv"}}}
              ],
              "edges": [
                {"id":"e1","source":"src","target":"out","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let stage = compile(&p)
            .unwrap()
            .stages
            .into_iter()
            .find(|s| s.node_id == "src")
            .unwrap();
        assert!(
            !stage.attach_view,
            "custom-sql ducklake must not be a live VIEW (it would re-resolve names downstream)"
        );
        match stage.runtime {
            Some(RuntimeSpec::AttachParquetSource(spec)) => {
                assert!(
                    spec.attach.contains("SET search_path='duckle_src'"),
                    "custom-sql ducklake must put the attached catalog on the search_path, got: {}",
                    spec.attach
                );
                assert!(
                    spec.body.contains("data.weights"),
                    "the user's SQL is preserved verbatim, got: {}",
                    spec.body
                );
            }
            other => panic!(
                "expected an AttachParquetSource runtime for custom-sql ducklake, got: {:?}",
                other
            ),
        }
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
    fn duck_source_auto_single_consumer_is_a_live_view() {
        // #76 case 2: a single-consumer duck source on the DEFAULT Auto setting
        // becomes a live lazy VIEW (pushdown), not an eager materialize.
        let p = pipeline_from_json(
            r#"{"nodes":[
                {"id":"s1","position":{"x":0,"y":0},"data":{"label":"Duck","componentId":"src.duckdb","properties":{"database":"/tmp/src.duckdb","tableName":"orders"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{"label":"Out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv"}}}
              ],"edges":[
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}
              ]}"#,
        );
        let c = compile(&p).unwrap();
        let s = c.stages.iter().find(|s| s.node_id == "s1").unwrap();
        assert!(s.sql.contains("CREATE OR REPLACE VIEW"), "auto single-consumer must be a VIEW: {}", s.sql);
        assert!(!s.sql.contains("DETACH"), "view src keeps its alias attached: {}", s.sql);
        assert!(s.runtime.is_none(), "upgraded view src must be pure SQL");
    }

    #[test]
    fn two_duck_sources_each_stay_views_with_distinct_aliases() {
        // #76 case 3: two duck sources must each stay a live VIEW with its OWN
        // alias (duckle_src_<node>), instead of the second collapsing both to
        // tables on the shared duckle_src alias.
        let p = pipeline_from_json(
            r#"{"nodes":[
                {"id":"s1","position":{"x":0,"y":0},"data":{"label":"A","componentId":"src.duckdb","properties":{"database":"/tmp/a.duckdb","tableName":"t1"}}},
                {"id":"s2","position":{"x":0,"y":0},"data":{"label":"B","componentId":"src.duckdb","properties":{"database":"/tmp/b.duckdb","tableName":"t2"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{"label":"KA","componentId":"snk.csv","properties":{"path":"/tmp/a.csv"}}},
                {"id":"k2","position":{"x":0,"y":0},"data":{"label":"KB","componentId":"snk.csv","properties":{"path":"/tmp/b.csv"}}}
              ],"edges":[
                {"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}},
                {"id":"e2","source":"s2","target":"k2","data":{"connectionType":"main"}}
              ]}"#,
        );
        let c = compile(&p).unwrap();
        let a = c.stages.iter().find(|s| s.node_id == "s1").unwrap();
        let b = c.stages.iter().find(|s| s.node_id == "s2").unwrap();
        assert!(a.sql.contains("CREATE OR REPLACE VIEW") && b.sql.contains("CREATE OR REPLACE VIEW"),
            "both sources must be VIEWs:\nA: {}\nB: {}", a.sql, b.sql);
        assert!(a.sql.contains("AS duckle_src_s1"), "source A uses its own alias: {}", a.sql);
        assert!(b.sql.contains("AS duckle_src_s2"), "source B uses its own alias: {}", b.sql);
        assert!(!a.sql.contains("AS duckle_src_s2") && !b.sql.contains("AS duckle_src_s1"), "aliases must not cross");
        assert!(!a.sql.contains("DETACH") && !b.sql.contains("DETACH"), "both kept attached for the batched session");
    }

    #[test]
    fn merge_resolves_columns_through_a_schemaless_transform() {
        // #39: a transform (e.g. a sample) between the source and a `merge` sink
        // leaves the sink's own schema empty; the merge must still find its input
        // columns by walking back to the source's schema, not error out.
        let p = pipeline_from_json(
            r#"{"nodes":[
                {"id":"s1","position":{"x":0,"y":0},"data":{"label":"Src","componentId":"src.csv","properties":{"path":"/tmp/in.csv"},
                  "schema":[{"name":"id","type":"int64"},{"name":"amount","type":"int64"}]}},
                {"id":"t1","position":{"x":0,"y":0},"data":{"label":"Sort","componentId":"xf.sort","properties":{"orderBy":"id"}}},
                {"id":"k1","position":{"x":0,"y":0},"data":{"label":"Merge","componentId":"snk.duckdb","properties":{"database":"/tmp/t.duckdb","tableName":"orders","mode":"merge","conflictColumns":["id"]}}}
              ],"edges":[
                {"id":"e1","source":"s1","target":"t1","data":{"connectionType":"main"}},
                {"id":"e2","source":"t1","target":"k1","data":{"connectionType":"main"}}
              ]}"#,
        );
        let c = compile(&p).expect("merge through a schemaless transform must compile");
        let sink = c.stages.iter().find(|s| s.node_id == "k1").unwrap();
        assert!(sink.sql.contains("MERGE INTO"), "sink must build a MERGE: {}", sink.sql);
        assert!(sink.sql.contains("\"amount\" = src.\"amount\""), "merge must set the non-key source column: {}", sink.sql);
    }

    #[test]
    fn sqlserver_sink_bulk_uses_mssql_extension() {
        // #86: snk.sqlserver default (bulk) -> ATTACH via the mssql community
        // extension + pure-SQL CREATE/INSERT through duckle_dst (fast bulk load),
        // not the row-by-row driver.
        let mk = |bulk: &str| pipeline_from_json(&format!(
            r#"{{"nodes":[
                {{"id":"s","position":{{"x":0,"y":0}},"data":{{"label":"Src","componentId":"src.csv","properties":{{"path":"/tmp/in.csv"}}}}}},
                {{"id":"k","position":{{"x":0,"y":0}},"data":{{"label":"MSSQL","componentId":"snk.sqlserver","properties":{{"host":"h","database":"db","user":"u","password":"p","tableName":"orders"{}}}}}}}
              ],"edges":[{{"id":"e1","source":"s","target":"k","data":{{"connectionType":"main"}}}}]}}"#,
            bulk));
        // Default (no bulk prop) -> bulk path.
        let c = compile(&mk("")).unwrap();
        let k = c.stages.iter().find(|s| s.node_id == "k").unwrap();
        assert!(k.sql.contains("INSTALL mssql FROM community") && k.sql.contains("AS duckle_dst (TYPE mssql)"),
            "bulk sink must ATTACH via mssql: {}", k.sql);
        assert!(k.sql.contains("duckle_dst.\"dbo\".\"orders\""), "writes to dbo.orders via duckle_dst: {}", k.sql);
        assert!(k.runtime.is_none(), "bulk sink is pure SQL (no driver spec)");
        // bulk=false -> the tiberius driver runtime spec, empty stage SQL.
        let c2 = compile(&mk(r#","bulk":false"#)).unwrap();
        let k2 = c2.stages.iter().find(|s| s.node_id == "k").unwrap();
        assert!(matches!(k2.runtime.as_ref(), Some(RuntimeSpec::SqlserverSink(_))), "bulk=false uses the driver");
        assert!(!k2.sql.contains("mssql"), "driver path has no mssql ATTACH: {}", k2.sql);
    }

    #[test]
    fn sqlserver_bulk_honours_trust_and_batch() {
        // #86 follow-up: trustCert + batchSize now apply to the bulk (mssql
        // extension) path, not only the legacy driver.
        let mk = |extra: &str| pipeline_from_json(&format!(
            r#"{{"nodes":[
                {{"id":"s","position":{{"x":0,"y":0}},"data":{{"label":"S","componentId":"src.csv","properties":{{"path":"/tmp/in.csv"}}}}}},
                {{"id":"k","position":{{"x":0,"y":0}},"data":{{"label":"M","componentId":"snk.sqlserver","properties":{{"host":"h","database":"db","user":"u","password":"p","tableName":"t"{}}}}}}}
              ],"edges":[{{"id":"e1","source":"s","target":"k","data":{{"connectionType":"main"}}}}]}}"#, extra));
        let sql = |extra: &str| {
            let c = compile(&mk(extra)).unwrap();
            c.stages.iter().find(|s| s.node_id == "k").unwrap().sql.clone()
        };
        // Default: trust off (matches the legacy driver default), batch 1000.
        let d = sql("");
        assert!(!d.contains("TrustServerCertificate"), "trust off by default: {}", d);
        assert!(d.contains("SET mssql_insert_batch_size = 1000"), "default batch 1000: {}", d);
        // Trust on -> TrustServerCertificate in the connection string.
        assert!(sql(r#","trustCert":true"#).contains("TrustServerCertificate=true"));
        // batchSize honoured, and clamped to SQL Server's 1000 ceiling.
        assert!(sql(r#","batchSize":500"#).contains("SET mssql_insert_batch_size = 500"));
        assert!(sql(r#","batchSize":5000"#).contains("SET mssql_insert_batch_size = 1000"), "clamp to 1000");
    }

    #[test]
    fn partial_run_keeps_attach_source_materialized() {
        // #87: "Run from here" (compile_partial) must NOT upgrade an attach-backed
        // source to a live VIEW. Partial runs execute per-stage in separate
        // processes, so a `duckle_src` VIEW from the source stage would not exist
        // for the next stage ("Catalog duckle_src... does not exist"). Keep the
        // source a materialized TABLE that survives across stages.
        let p = pipeline_from_json(
            r#"{"nodes":[
                {"id":"s","position":{"x":0,"y":0},"data":{"label":"Duck","componentId":"src.duckdb","properties":{"database":"/tmp/src.duckdb","tableName":"orders"}}},
                {"id":"f","position":{"x":0,"y":0},"data":{"label":"F","componentId":"xf.filter","properties":{"predicate":"id > 0"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"Out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv"}}}
              ],"edges":[
                {"id":"e1","source":"s","target":"f","data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"k","data":{"connectionType":"main"}}
              ]}"#,
        );
        // Full run: the single-session executor can keep the source a live VIEW.
        let full = compile(&p).unwrap();
        let sf = full.stages.iter().find(|s| s.node_id == "s").unwrap();
        assert!(sf.sql.contains("CREATE OR REPLACE VIEW"), "full run upgrades source to a live VIEW: {}", sf.sql);
        // Partial run-to-here at the filter: per-stage, so source stays a TABLE.
        let part = compile_partial(&p, "f").unwrap();
        let sp = part.stages.iter().find(|s| s.node_id == "s").unwrap();
        assert!(sp.sql.contains("CREATE OR REPLACE TABLE"), "partial source must stay a TABLE: {}", sp.sql);
        assert!(!sp.sql.contains("CREATE OR REPLACE VIEW"), "partial source must NOT be a live VIEW: {}", sp.sql);
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
    fn ducklake_source_time_travel_read() {
        // Time-Travel Data Diff foundation: a DuckLake source can read a table
        // as of a past snapshot via AT (VERSION) / AT (TIMESTAMP).
        use serde_json::json;
        let v = build_relational_source("src.ducklake", &json!({"tableName":"orders","asOfVersion":3})).unwrap();
        assert!(v.contains("AT (VERSION => 3)"), "version read: {}", v);
        let vs = build_relational_source("src.ducklake", &json!({"tableName":"orders","asOfVersion":"5"})).unwrap();
        assert!(vs.contains("AT (VERSION => 5)"), "string version read: {}", vs);
        let t = build_relational_source("src.ducklake", &json!({"tableName":"orders","asOfTimestamp":"2026-01-01 00:00:00"})).unwrap();
        assert!(t.contains("AT (TIMESTAMP => '2026-01-01 00:00:00')"), "timestamp read: {}", t);
        let n = build_relational_source("src.ducklake", &json!({"tableName":"orders"})).unwrap();
        assert!(!n.contains(" AT ("), "no asOf must not add a clause: {}", n);
        // A plain relational source ignores asOf (AT VERSION is not valid there).
        let pg = build_relational_source("src.postgres", &json!({"tableName":"orders","asOfVersion":3})).unwrap();
        assert!(!pg.contains(" AT ("), "non-ducklake must ignore asOf: {}", pg);
    }

    #[test]
    fn source_select_postgres_autodetect_uses_attach_not_placeholder() {
        // #129: Postgres autodetect returned None -> the UI fell back to a
        // col_1/col_2/col_3 placeholder. source_select_for_format must build the
        // ATTACH-based SELECT for the relational families, same as the run path.
        use serde_json::json;
        let sel = source_select_for_format("postgres", &json!({"tableName": "orders"}))
            .expect("postgres autodetect select should be Some");
        assert!(sel.contains("duckle_src"), "reads via the attached catalog: {}", sel);
        assert!(sel.contains("orders"), "references the table: {}", sel);
        // mysql / motherduck / bigquery route through the same path.
        assert!(source_select_for_format("mysql", &json!({"tableName": "t"})).is_some());
        assert!(source_select_for_format("motherduck", &json!({"tableName": "t"})).is_some());
        // An unknown format still returns None (unchanged behavior).
        assert!(source_select_for_format("nope", &json!({})).is_none());
    }

    #[test]
    fn ducklake_diff_builds_change_feed_between_versions() {
        // src.ducklake.diff: the change feed between two explicit snapshots.
        use serde_json::json;
        let body = build_ducklake_diff(&json!({"schema":"main","table":"orders","fromVersion":2,"toVersion":5}));
        assert_eq!(body, "SELECT * FROM ducklake_table_changes('duckle_src', 'main', 'orders', 2, 5)");
        // string versions + default schema (main)
        let b2 = build_ducklake_diff(&json!({"table":"orders","fromVersion":"1","toVersion":"3"}));
        assert_eq!(b2, "SELECT * FROM ducklake_table_changes('duckle_src', 'main', 'orders', 1, 3)");
    }

    #[test]
    fn diffsummary_reduces_change_feed() {
        // xf.diffsummary: counts insert/delete/update_postimage from a change
        // feed into one summary row (added/removed/updated/total + text).
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_diffsummary(&ni, &serde_json::json!({})).unwrap();
        assert!(sql.contains("FILTER (WHERE \"change_type\" = 'insert') AS added"), "got: {}", sql);
        assert!(sql.contains("(added + removed + updated) AS total_changes"), "got: {}", sql);
        assert!(sql.contains("FROM \"up\""), "got: {}", sql);
        // configurable change column
        let custom = build_diffsummary(&ni, &serde_json::json!({"changeColumn": "op"})).unwrap();
        assert!(custom.contains("FILTER (WHERE \"op\" = 'delete') AS removed"), "got: {}", custom);
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
        assert!(
            fast_sql.trim_end().ends_with(", *"),
            "orderBy prop must append a trailing `, *` all-column tiebreaker for a deterministic survivor, got: {}",
            fast_sql
        );
    }

    #[test]
    fn setop_realigns_columns_by_name_without_invalid_syntax() {
        // INTERSECT/EXCEPT BY NAME is a parser error in DuckDB; realign later
        // legs to the first leg's columns via a 0-row UNION ALL BY NAME template
        // and join with the plain set operator.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["a".into(), "b".into()]);
        let sql = build_setop(&ni, "INTERSECT", &serde_json::json!({})).unwrap();
        assert!(!sql.contains("INTERSECT BY NAME"), "must not emit invalid INTERSECT BY NAME, got: {}", sql);
        assert!(sql.contains(" INTERSECT "), "must join legs with plain INTERSECT, got: {}", sql);
        assert!(sql.contains("WHERE false UNION ALL BY NAME"), "must realign later legs by name, got: {}", sql);
        let ex = build_setop(&ni, "EXCEPT", &serde_json::json!({})).unwrap();
        assert!(ex.contains(" EXCEPT ") && !ex.contains("EXCEPT BY NAME"), "got: {}", ex);
    }

    #[test]
    fn cast_per_column_format_uses_strptime() {
        // #10: each cast entry can carry its own strptime format so multiple
        // columns with different date formats parse independently.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_cast(
            &ni,
            &serde_json::json!({"casts": [
                {"column": "d1", "targetType": "date", "format": "%d/%m/%Y"},
                {"column": "ts", "targetType": "timestamp", "format": "%Y.%m.%d %H:%M:%S"},
                {"column": "amount", "targetType": "double"}
            ]}),
        )
        .unwrap();
        assert!(sql.contains("try_strptime(\"d1\", '%d/%m/%Y')::DATE AS \"d1\""), "got: {}", sql);
        assert!(
            sql.contains("try_strptime(\"ts\", '%Y.%m.%d %H:%M:%S')::TIMESTAMP AS \"ts\""),
            "got: {}",
            sql
        );
        assert!(sql.contains("TRY_CAST(\"amount\""), "non-date cast keeps TRY_CAST, got: {}", sql);
        // A date cast WITHOUT a format still uses TRY_CAST (no regression).
        let no_fmt =
            build_cast(&ni, &serde_json::json!({"casts":[{"column":"d","targetType":"date"}]})).unwrap();
        assert!(no_fmt.contains("TRY_CAST(\"d\" AS DATE)"), "got: {}", no_fmt);
    }

    #[test]
    fn mask_builds_replace_per_mode() {
        // qa.mask: SELECT * REPLACE with a per-column masking expression.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_mask(
            &ni,
            &serde_json::json!({"masks":[
                {"column":"ssn","mode":"partial","showLast":4},
                {"column":"email","mode":"hash","salt":"s7"},
                {"column":"name","mode":"null"},
                {"column":"note","mode":"constant","value":"X"}
            ]}),
        )
        .unwrap();
        assert!(sql.starts_with("SELECT * REPLACE ("), "got: {}", sql);
        assert!(sql.contains("right(CAST(\"ssn\" AS VARCHAR), 4)"), "got: {}", sql);
        assert!(sql.contains("md5('s7' || CAST(\"email\" AS VARCHAR)) AS \"email\""), "got: {}", sql);
        assert!(sql.contains("NULL AS \"name\""), "got: {}", sql);
        assert!(sql.contains("'X' AS \"note\""), "got: {}", sql);
        assert!(sql.contains("FROM \"up\""), "got: {}", sql);
        // hash without salt is unsalted md5; unknown mode is a loud error.
        let nosalt = build_mask(&ni, &serde_json::json!({"column":"x","mode":"hash"})).unwrap();
        assert!(nosalt.contains("md5(CAST(\"x\" AS VARCHAR))"), "got: {}", nosalt);
        assert!(build_mask(&ni, &serde_json::json!({"column":"x","mode":"bogus"})).is_err());
    }

    #[test]
    fn survivor_builds_golden_record_groupby() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let freq = build_survivor(&ni, &serde_json::json!({"groupBy":["id"],"rule":"most_frequent"})).unwrap();
        assert!(freq.contains("mode(COLUMNS(* EXCLUDE (\"id\")))"), "got: {}", freq);
        assert!(freq.contains("GROUP BY \"id\""), "got: {}", freq);
        let recent = build_survivor(&ni, &serde_json::json!({"groupBy":["id"],"rule":"most_recent","recencyColumn":"updated_at"})).unwrap();
        assert!(recent.contains("arg_max(COLUMNS(* EXCLUDE (\"id\")), \"updated_at\")"), "got: {}", recent);
        // most_recent without a recency column is a loud error; unknown rule too.
        assert!(build_survivor(&ni, &serde_json::json!({"groupBy":["id"],"rule":"most_recent"})).is_err());
        assert!(build_survivor(&ni, &serde_json::json!({"groupBy":["id"],"rule":"bogus"})).is_err());
        assert!(build_survivor(&ni, &serde_json::json!({"rule":"max"})).is_err());
    }

    #[test]
    fn block_joins_within_a_rule_and_keeps_each_pair_once() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_er_block(
            &ni,
            &serde_json::json!({
                "leftId": "id",
                "rules": { "postcode": "postcode, surname" },
                "carryColumns": ["name"],
            }),
        )
        .unwrap();
        // Every column in the rule must be equal, not just the first.
        assert!(sql.contains("l.\"postcode\" = r.\"postcode\""), "got: {}", sql);
        assert!(sql.contains("l.\"surname\" = r.\"surname\""), "got: {}", sql);
        // Self mode: each unordered pair once, and no record paired with itself.
        assert!(
            sql.contains("CAST(l.\"id\" AS VARCHAR) < CAST(r.\"id\" AS VARCHAR)"),
            "got: {}",
            sql
        );
        // The output contract qa.matchgroup reads by default.
        assert!(sql.contains("l.\"id\" AS id_a"), "got: {}", sql);
        assert!(sql.contains("r.\"id\" AS id_b"), "got: {}", sql);
        assert!(sql.contains("l.\"name\" AS \"a_name\""), "got: {}", sql);
        assert!(sql.contains("r.\"name\" AS \"b_name\""), "got: {}", sql);
        assert!(
            sql.contains("QUALIFY row_number() OVER (PARTITION BY id_a, id_b"),
            "got: {}",
            sql
        );

        // Link mode: two inputs, so the self-pair guard would throw away half
        // the legitimate pairs and must not be applied.
        let mut two = NodeInputs::default();
        two.ports.insert("main".into(), vec!["left".into()]);
        two.ports.insert("lookup".into(), vec!["right".into()]);
        let linked = build_er_block(
            &two,
            &serde_json::json!({ "leftId": "id", "rules": { "pc": "postcode" } }),
        )
        .unwrap();
        assert!(
            !linked.contains("AS VARCHAR) <"),
            "link mode must not drop half the pairs: {}",
            linked
        );
        assert!(linked.contains("FROM \"left\" l JOIN \"right\" r"), "got: {}", linked);

        // A missing id or no usable rule is a configuration error the user can
        // read, not empty SQL that silently produces nothing.
        assert!(build_er_block(&ni, &serde_json::json!({ "leftId": "id" })).is_err());
        assert!(build_er_block(&ni, &serde_json::json!({ "rules": { "a": "b" } })).is_err());
        assert!(build_er_block(
            &ni,
            &serde_json::json!({ "leftId": "id", "rules": { "empty": "  ,  " } })
        )
        .is_err());
        assert!(build_er_block(&NodeInputs::default(), &serde_json::json!({})).is_err());
    }

    #[test]
    fn matchgroup_builds_recursive_cluster_sql() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_matchgroup(&ni, &serde_json::json!({})).unwrap();
        assert!(sql.starts_with("WITH RECURSIVE"), "got: {}", sql);
        assert!(sql.contains("CAST(\"id_a\" AS VARCHAR) AS s, CAST(\"id_b\" AS VARCHAR) AS t"), "got: {}", sql);
        assert!(sql.contains("SELECT CAST(\"id_b\" AS VARCHAR), CAST(\"id_a\" AS VARCHAR)"), "got: {}", sql);
        assert!(sql.contains("SELECT id, MIN(rep) AS cluster_id FROM reach GROUP BY id"), "got: {}", sql);
        assert!(sql.contains("FROM \"up\""), "got: {}", sql);
        let custom = build_matchgroup(&ni, &serde_json::json!({"leftKey":"left rec","rightKey":"right rec"})).unwrap();
        assert!(custom.contains("CAST(\"left rec\" AS VARCHAR) AS s, CAST(\"right rec\" AS VARCHAR) AS t"), "got: {}", custom);
        assert!(build_matchgroup(&NodeInputs::default(), &serde_json::json!({})).is_err());
    }

    #[test]
    fn sample_adv_builds_using_sample_clause() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let seeded = build_sample_adv(&ni, &serde_json::json!({ "percent": 10, "method": "reservoir", "seed": 42 })).unwrap();
        assert_eq!(seeded, "SELECT * FROM \"up\" USING SAMPLE 10 PERCENT (reservoir, 42)", "got: {}", seeded);
        let no_seed = build_sample_adv(&ni, &serde_json::json!({ "percent": 5 })).unwrap();
        assert_eq!(no_seed, "SELECT * FROM \"up\" USING SAMPLE 5 PERCENT (reservoir)", "got: {}", no_seed);
        assert!(build_sample_adv(&ni, &serde_json::json!({})).is_err());
        assert!(build_sample_adv(&ni, &serde_json::json!({ "percent": 150 })).is_err());
        assert!(build_sample_adv(&ni, &serde_json::json!({ "percent": 10, "method": "bogus" })).is_err());
        assert!(build_sample_adv(&ni, &serde_json::json!({ "percent": "10; DROP TABLE x" })).is_err());
    }

    #[test]
    fn expect_builds_scorecard_per_rule() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_expect(
            &ni,
            &serde_json::json!({ "rules": [
                { "column": "email", "check": "not_null" },
                { "column": "amt",   "check": "in_range", "args": { "min": 0, "max": 10 } },
                { "column": "status","check": "in_set",   "args": ["paid", "pending"] },
                { "column": "code",  "check": "regex",    "args": "^[A-Z]+$" },
                { "column": "qty",   "check": "non_negative" },
                { "column": "id",    "check": "unique" }
            ]}),
        )
        .unwrap();
        assert!(sql.contains("AS pass_rate"), "got: {}", sql);
        assert!(sql.contains("(failed = 0) AS passed"), "got: {}", sql);
        assert!(sql.contains("COUNT(*) FILTER (WHERE NOT (\"email\" IS NOT NULL))"), "got: {}", sql);
        assert!(sql.contains("COUNT(*) FILTER (WHERE NOT (\"amt\" BETWEEN 0 AND 10))"), "got: {}", sql);
        assert!(sql.contains("\"status\" IN ('paid', 'pending')"), "got: {}", sql);
        assert!(sql.contains("regexp_full_match(CAST(\"code\" AS VARCHAR), '^[A-Z]+$')"), "got: {}", sql);
        assert!(sql.contains("COUNT(*) OVER (PARTITION BY \"id\") = 1"), "got: {}", sql);
        assert!(sql.contains("'in_set(status, 2 values)' AS expectation"), "got: {}", sql);
        assert!(sql.contains("FROM \"up\""), "got: {}", sql);
        assert!(build_expect(&ni, &serde_json::json!({ "rules": [] })).is_err());
        assert!(build_expect(&ni, &serde_json::json!({ "rules": [{ "column": "x", "check": "bogus" }] })).is_err());
        let kv = build_expect(&ni, &serde_json::json!({ "rules": { "amt": "in_range:0,10", "id": "unique" } })).unwrap();
        // The key-value form parses 0,10 as floats, so the SQL is BETWEEN 0.0 AND 10.0.
        assert!(kv.contains("\"amt\" BETWEEN 0.0 AND 10.0"), "got: {}", kv);
        assert!(kv.contains("COUNT(*) OVER (PARTITION BY \"id\") = 1"), "got: {}", kv);
    }

    #[test]
    fn refintegrity_builds_semi_and_anti_join() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["m1".into()]);
        ni.ports.insert("lookup".into(), vec!["r1".into()]);
        let props = serde_json::json!({"leftKey": "cust_id", "rightKey": "id"});
        let pass = build_refintegrity(&ni, &props, false).unwrap();
        assert_eq!(
            pass,
            "SELECT \"m1\".* FROM \"m1\" WHERE EXISTS (SELECT 1 FROM \"r1\" WHERE \"r1\".\"id\" = \"m1\".\"cust_id\")",
            "got: {}",
            pass
        );
        let rej = build_refintegrity(&ni, &props, true).unwrap();
        assert_eq!(
            rej,
            "SELECT \"m1\".* FROM \"m1\" WHERE NOT EXISTS (SELECT 1 FROM \"r1\" WHERE \"r1\".\"id\" = \"m1\".\"cust_id\")",
            "got: {}",
            rej
        );
        let mut no_ref = NodeInputs::default();
        no_ref.ports.insert("main".into(), vec!["m1".into()]);
        assert!(build_refintegrity(&no_ref, &props, false).is_err());
        assert!(build_refintegrity(&ni, &serde_json::json!({"rightKey": "id"}), false).is_err());
        assert!(build_refintegrity(&ni, &serde_json::json!({"leftKey": "cust_id"}), false).is_err());
    }

    #[test]
    fn profile_adv_builds_metric_value_long_form() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_profile_adv(&ni, &serde_json::json!({ "column": "email", "topN": 3 })).unwrap();
        assert!(sql.contains("SELECT CAST(\"email\" AS VARCHAR) AS v FROM \"up\""), "got: {}", sql);
        assert!(sql.contains("approx_count_distinct(v) AS distinct_n"), "got: {}", sql);
        assert!(sql.contains("regexp_full_match(v, '^-?[0-9]+$')"), "int pattern: {}", sql);
        assert!(sql.contains("GROUP BY v ORDER BY freq DESC, v LIMIT 3"), "top-n: {}", sql);
        assert!(sql.contains("SELECT \"metric\", \"value\", \"count\", \"pct\" FROM ("), "shape: {}", sql);
        let def = build_profile_adv(&ni, &serde_json::json!({ "column": "x" })).unwrap();
        assert!(def.contains("LIMIT 10"), "default top-n: {}", def);
        assert!(build_profile_adv(&ni, &serde_json::json!({ "column": "x", "topN": 99999 })).unwrap().contains("LIMIT 1000"));
        assert!(build_profile_adv(&ni, &serde_json::json!({})).is_err());
        assert!(build_profile_adv(&NodeInputs::default(), &serde_json::json!({ "column": "x" })).is_err());
    }

    #[test]
    fn link_builds_cross_join_over_two_inputs() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["m1".into()]);
        ni.ports.insert("lookup".into(), vec!["r1".into()]);
        let sql = build_record_link(&ni, &serde_json::json!({ "leftKey": "name", "rightKey": "company" })).unwrap();
        assert!(sql.contains("FROM \"m1\"") && sql.contains("FROM \"r1\""), "got: {}", sql);
        assert!(sql.contains("CROSS JOIN"), "got: {}", sql);
        assert!(sql.contains("jaro_winkler_similarity(a._key, b._key)"), "got: {}", sql);
        assert!(sql.contains(">= 0.85"), "got: {}", sql);
        assert!(sql.contains("a._show AS left_key") && sql.contains("b._show AS right_key"), "got: {}", sql);
        let lev = build_record_link(&ni, &serde_json::json!({"leftColumns":["first","last"],"rightColumns":["fname","lname"],"algorithm":"levenshtein","threshold":0.7})).unwrap();
        assert!(lev.contains("levenshtein(a._key, b._key)"), "got: {}", lev);
        assert!(lev.contains("concat_ws(' ', \"first\", \"last\")"), "got: {}", lev);
        let mut no_ref = NodeInputs::default();
        no_ref.ports.insert("main".into(), vec!["m1".into()]);
        assert!(build_record_link(&no_ref, &serde_json::json!({"leftKey":"a","rightKey":"b"})).is_err());
        assert!(build_record_link(&ni, &serde_json::json!({ "rightKey": "company" })).is_err());
    }

    #[test]
    fn reconcile_builds_full_outer_join_report() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["m1".into()]);
        ni.ports.insert("lookup".into(), vec!["r1".into()]);
        let sql = build_reconcile(&ni, &serde_json::json!({ "keyColumns": ["id"], "measureColumns": ["amount"] })).unwrap();
        assert!(sql.contains("FULL OUTER JOIN \"__r\" ON \"__m\".\"id\" IS NOT DISTINCT FROM \"__r\".\"id\""), "got: {}", sql);
        assert!(sql.contains("'source_rows' AS metric"), "got: {}", sql);
        assert!(sql.contains("'amount_difference', CAST((SELECT SUM(\"amount\") FROM \"__m\") AS DOUBLE) - CAST((SELECT SUM(\"amount\") FROM \"__r\") AS DOUBLE)"), "got: {}", sql);
        let composite = build_reconcile(&ni, &serde_json::json!({ "keyColumns": ["region", "id"] })).unwrap();
        assert!(composite.contains("\"__m\".\"region\" IS NOT DISTINCT FROM \"__r\".\"region\" AND \"__m\".\"id\" IS NOT DISTINCT FROM \"__r\".\"id\""), "got: {}", composite);
        assert!(!composite.contains("_source_sum"), "no measures means no sum metrics: {}", composite);
        assert!(build_reconcile(&ni, &serde_json::json!({ "measureColumns": ["amount"] })).is_err());
    }

    #[test]
    fn classify_builds_pii_report_sql() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let all = build_classify(&ni, &serde_json::json!({})).unwrap();
        assert!(all.contains("SELECT COLUMNS(*)::VARCHAR FROM \"up\""), "got: {}", all);
        assert!(all.contains("UNPIVOT INCLUDE NULLS (col_val FOR col_name IN (COLUMNS(*)))"), "got: {}", all);
        assert!(all.contains("n_email / NULLIF(sample_count, 0) AS r_email"), "got: {}", all);
        assert!(all.contains("GREATEST(COALESCE(r_email, 0)"), "got: {}", all);
        assert!(all.contains("CASE WHEN best_rate < 0.8 THEN 'text'"), "got: {}", all);
        assert!(all.contains("WHEN r_email = best_rate THEN 'email'"), "got: {}", all);
        assert!(all.contains("detected_type IN ('email', 'ssn', 'uuid', 'ipv4', 'credit_card', 'phone') AS is_pii"), "got: {}", all);
        let some = build_classify(&ni, &serde_json::json!({"columns": ["email", "ssn"]})).unwrap();
        assert!(some.contains("CAST(\"email\" AS VARCHAR) AS \"email\""), "got: {}", some);
        assert!(!some.contains("COLUMNS(*)::VARCHAR"), "explicit columns must not melt all: {}", some);
        let clamp = build_classify(&ni, &serde_json::json!({"threshold": 5})).unwrap();
        assert!(clamp.contains("CASE WHEN best_rate < 1 THEN 'text'"), "got: {}", clamp);
        assert!(build_classify(&NodeInputs::default(), &serde_json::json!({})).is_err());
    }

    #[test]
    fn rename_from_mapping_file_json_csv() {
        use std::io::Write;
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let dir = std::env::temp_dir().join(format!("duckle_rentest_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // JSON object form.
        let jpath = dir.join("map.json");
        std::fs::File::create(&jpath).unwrap().write_all(br#"{"a":"alpha","b":"beta"}"#).unwrap();
        let sql = build_rename(&ni, &serde_json::json!({ "mappingFile": jpath.to_string_lossy() })).unwrap();
        assert!(sql.contains("\"a\" AS \"alpha\"") && sql.contains("\"b\" AS \"beta\""), "got: {}", sql);
        // CSV form with a header row (skipped).
        let cpath = dir.join("map.csv");
        std::fs::File::create(&cpath).unwrap().write_all(b"old,new\nx,ex\ny,why\n").unwrap();
        let csv = build_rename(&ni, &serde_json::json!({ "mappingFile": cpath.to_string_lossy() })).unwrap();
        assert!(csv.contains("\"x\" AS \"ex\"") && csv.contains("\"y\" AS \"why\""), "got: {}", csv);
        // Missing file is a loud error.
        assert!(build_rename(&ni, &serde_json::json!({ "mappingFile": "/no/such/file.json" })).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_filter_that_ranks_rows_goes_where_ranking_is_allowed() {
        // A legacy job limits a loop by asking for a running sequence number and keeping
        // the rows below a bound. That reads as a window function, and SQL does not allow
        // one in WHERE at all - the step fails to bind, so the whole branch is lost for a
        // filter that was translated correctly and then put in the wrong clause.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{
                  "label":"Map","componentId":"xf.map",
                  "properties":{"expressions":{"A":"A"},
                                "filter":"(1 + (row_number() OVER () - 1) * 1) <= 5"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"m","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&doc).unwrap().stages.into_iter()
            .find(|s| s.node_id == "m").unwrap().sql;
        assert!(sql.contains("QUALIFY"), "got: {sql}");
        assert!(!sql.contains("WHERE (1 +"), "not in WHERE: {sql}");

        // An ordinary filter still goes where an ordinary filter goes.
        let plain = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{
                  "label":"Map","componentId":"xf.map",
                  "properties":{"expressions":{"A":"A"},"filter":"A > 5"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"m","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&plain).unwrap().stages.into_iter()
            .find(|s| s.node_id == "m").unwrap().sql;
        assert!(sql.contains("WHERE A > 5"), "got: {sql}");
        assert!(!sql.contains("QUALIFY"), "got: {sql}");

        // The word inside a piece of text is not a window function.
        let texty = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{
                  "label":"Map","componentId":"xf.map",
                  "properties":{"expressions":{"A":"A"},"filter":"A = 'left over (x)'"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"m","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&texty).unwrap().stages.into_iter()
            .find(|s| s.node_id == "m").unwrap().sql;
        assert!(!sql.contains("QUALIFY"), "got: {sql}");
    }

    #[test]
    fn a_join_key_that_is_an_expression_is_one_key_not_two() {
        // Keys are written as a comma-separated list, and a key can be an expression
        // rather than a bare column. Split on every comma, an expression that takes
        // more than one argument counts as several keys, so a perfectly good single-key
        // join is refused for having "2 vs 1" - and the count is the only thing wrong.
        assert_eq!(parse_key_list("a,b"), vec!["a", "b"]);
        assert_eq!(parse_key_list("right(File_Name, 5)"), vec!["right(File_Name, 5)"]);
        assert_eq!(
            parse_key_list("right(File_Name, 5), CODE"),
            vec!["right(File_Name, 5)", "CODE"]
        );
        // A comma inside text is not a separator either.
        assert_eq!(parse_key_list("replace(x, ',', '')"), vec!["replace(x, ',', '')"]);
        assert_eq!(parse_key_list("  a , , b "), vec!["a", "b"]);
    }

    #[test]
    fn a_run_variable_is_set_from_the_rows_that_reach_it() {
        // A job routinely works out a value while it runs - the date on the batch it
        // just read, the id it just wrote - and later steps ask for that value. The
        // static context cannot hold it, because nothing knows it until the run is
        // under way.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"v","position":{"x":0,"y":0},"data":{
                  "label":"Set","componentId":"ctl.setvar",
                  "properties":{"name":"batch_date","value":"max(TXNDATE)"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"v","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&doc).unwrap().stages.into_iter()
            .find(|s| s.node_id == "v").unwrap().sql;
        // Held in the run's own database rather than in the session, because a stage
        // can be a separate connection to the same file and session state does not
        // cross that boundary.
        assert!(sql.contains(r#"CREATE OR REPLACE TABLE "duckle_var__batch_date""#), "got: {sql}");
        assert!(sql.contains("(max(TXNDATE)) AS v"), "got: {sql}");
        assert!(sql.contains(r#"FROM "s""#), "it reads the rows wired into it: {sql}");
    }

    #[test]
    fn a_run_variable_with_nothing_wired_in_stands_on_its_own() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"v","position":{"x":0,"y":0},"data":{
                  "label":"Set","componentId":"ctl.setvar",
                  "properties":{"name":"today","value":"current_date"}}}
              ],
              "edges":[]
            }"#,
        );
        let sql = compile(&doc).unwrap().stages.into_iter()
            .find(|s| s.node_id == "v").unwrap().sql;
        assert!(sql.contains("(current_date) AS v"), "got: {sql}");
        assert!(!sql.contains(" FROM \""), "nothing to read from: {sql}");
    }

    #[test]
    fn a_run_variable_without_a_name_is_refused() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"v","position":{"x":0,"y":0},"data":{
                  "label":"Set","componentId":"ctl.setvar",
                  "properties":{"value":"current_date"}}}
              ],
              "edges":[]
            }"#,
        );
        let msg = compile(&doc).expect_err("a variable with no name is nothing").to_string();
        assert!(msg.contains("`name` is required"), "got: {msg}");
    }

    #[test]
    fn a_later_step_reads_the_run_variable_where_it_names_it() {
        // The three ways the name gets written. It stands for a VALUE, so where it is
        // spelled as a whole string literal - which is how a value is usually written
        // into a WHERE clause - the quotes come off with it. Left as text, the step
        // would compare its column against the eight characters of the placeholder.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"v","position":{"x":0,"y":0},"data":{
                  "label":"Set","componentId":"ctl.setvar",
                  "properties":{"name":"d","value":"max(TXNDATE)"}}},
                {"id":"q","position":{"x":0,"y":0},"data":{
                  "label":"SQL","componentId":"code.sql",
                  "properties":{"sql":"SELECT * FROM input WHERE a = '${d}' AND b = ${d} AND c = 'x${d}y'"}}}
              ],
              "edges":[
                {"id":"e1","source":"s","target":"v","data":{"connectionType":"main"}},
                {"id":"e2","source":"v","target":"q","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let sql = compile(&doc).unwrap().stages.into_iter()
            .find(|s| s.node_id == "q").unwrap().sql;
        let read = r#"(SELECT v FROM "duckle_var__d")"#;
        assert!(sql.contains(&format!("a = {read}")), "a whole literal loses its quotes: {sql}");
        assert!(sql.contains(&format!("b = {read}")), "standing on its own: {sql}");
        assert!(
            sql.contains(&format!("c = 'x' || {read} || 'y'")),
            "inside a longer literal the literal keeps its shape: {sql}"
        );
        assert!(!sql.contains("${d}"), "nothing is left as text: {sql}");
    }

    #[test]
    fn a_name_no_node_sets_is_left_alone() {
        // Only the names a node in this pipeline actually sets are read this way.
        // Anything else is somebody else's placeholder and is left exactly as it is.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"q","position":{"x":0,"y":0},"data":{
                  "label":"SQL","componentId":"code.sql",
                  "properties":{"sql":"SELECT * FROM input WHERE a = '${d}'"}}}
              ],
              "edges":[{"id":"e2","source":"s","target":"q","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&doc).unwrap().stages.into_iter()
            .find(|s| s.node_id == "q").unwrap().sql;
        assert!(sql.contains("'${d}'"), "got: {sql}");
    }

    #[test]
    fn quoting_off_is_said_to_the_reader_not_left_unsaid() {
        // "None" in the quote box means the file has no quoting: a lone double quote in
        // the middle of a line is data, not the start of a quoted field. Leaving the
        // argument out does not say that - it leaves the reader on its default, which is
        // the double quote - so a file whose text contains one was swallowed from that
        // point to the next one and came back with no rows at all.
        let off = build_csv_source(
            &serde_json::json!({ "path": "d.txt", "hasHeader": false, "quoteChar": "" }),
            None,
        );
        assert!(off.contains("quote=''"), "got: {}", off);
        // Said nothing at all, the reader keeps its own default: still nothing emitted.
        let unsaid = build_csv_source(
            &serde_json::json!({ "path": "d.txt", "hasHeader": false }),
            None,
        );
        assert!(!unsaid.contains("quote="), "got: {}", unsaid);
    }

    #[test]
    fn csv_extra_read_options_and_filename() {
        // #83: filename=true + a readOptions passthrough land in read_csv args.
        let sql = build_csv_source(
            &serde_json::json!({
                "path": "data/*.csv",
                "hasHeader": true,
                "filename": true,
                "readOptions": [{ "key": "union_by_name", "value": "true" }, { "key": "sample_size", "value": "-1" }]
            }),
            None,
        );
        assert!(sql.contains("filename=true"), "got: {}", sql);
        assert!(sql.contains("union_by_name=true"), "got: {}", sql);
        assert!(sql.contains("sample_size=-1"), "got: {}", sql);
        // Default: neither appears.
        let plain = build_csv_source(&serde_json::json!({ "path": "d.csv", "hasHeader": true }), None);
        assert!(!plain.contains("filename="), "got: {}", plain);
    }

    #[test]
    fn an_email_sink_with_nothing_wired_in_sends_one_fixed_message() {
        // A notification is not a row. Wiring an ordering link into a mail step
        // to say "tell someone we got here" is ordinary, and the sink used to
        // demand a main input, so it could only be reached by inventing a
        // one-row table to carry three constants.
        let base = r#"{
          "nodes": [
            {"id":"m","type":"sink","position":{"x":0,"y":0},
             "data":{"label":"m","componentId":"snk.email","properties":{
               "host":"smtp.example.com","fromAddress":"a@example.com"PROPS}}}
          ],
          "edges": []
        }"#;

        let with_to = base.replace(
            "PROPS",
            r#","to":"ops@example.com","subject":"done","body":"the load finished""#,
        );
        let doc: super::PipelineDoc = serde_json::from_str(&with_to).expect("parses");
        let c = super::compile(&doc).expect("a fixed message needs no upstream");
        assert_eq!(c.stages.len(), 1);

        // Without a recipient there is nothing to send and no rows to take one
        // from, so it is refused rather than silently sending nowhere.
        let without_to: super::PipelineDoc =
            serde_json::from_str(&base.replace("PROPS", "")).expect("parses");
        let err = super::compile(&without_to).expect_err("no recipient must be refused");
        let msg = format!("{err}");
        assert!(
            msg.contains("`to` is required"),
            "the error should say what is missing, got: {msg}"
        );
    }

    #[test]
    fn inline_rows_are_literals_not_identifiers() {
        // An inline row is data somebody typed. Emitting the values as
        // identifiers would let a stray one resolve against a table that
        // happens to exist, so they are written as literals and the NAMES are
        // the only part quoted as identifiers.
        let sql = build_inline_source(&serde_json::json!({
            "columns": [{ "key": "job", "value": "nightly" }, { "key": "n", "value": "1" }],
            "rowCount": 3
        }));
        assert!(sql.contains("'nightly' AS \"job\""), "got: {sql}");
        assert!(sql.contains("'1' AS \"n\""), "got: {sql}");
        assert!(sql.contains("FROM range(3)"), "rowCount repeats the row: {sql}");
        // A value carrying a quote must not end the literal.
        let hostile = build_inline_source(&serde_json::json!({
            "columns": [{ "key": "c", "value": "it's" }]
        }));
        assert!(hostile.contains("'it''s'"), "quote must be escaped: {hostile}");
        // Nothing declared yields no rows rather than a broken statement.
        assert!(build_inline_source(&serde_json::json!({})).contains("WHERE false"));
    }

    #[test]
    fn a_file_list_pointed_at_one_path_is_an_existence_test() {
        // Pointed at a single file the listing yields one row, or none. That is
        // what a job's file-exists check needs, and it needs no second component.
        let one = build_filelist_source(&serde_json::json!({ "path": "/data/in/today.csv" }));
        assert!(one.contains("glob('/data/in/today.csv')"), "got: {one}");
        // An explicit path wins over directory + pattern rather than being
        // silently combined with them into a path that names nothing.
        let both = build_filelist_source(&serde_json::json!({
            "path": "/data/in/today.csv", "directory": "/elsewhere", "pattern": "*.txt"
        }));
        assert!(both.contains("glob('/data/in/today.csv')"), "got: {both}");
        assert!(!both.contains("/elsewhere"), "got: {both}");
    }

    #[test]
    fn a_file_operation_refuses_what_it_cannot_do() {
        let node = |props: serde_json::Value| {
            let doc: super::PipelineDoc = serde_json::from_str(&format!(
                r#"{{"nodes":[{{"id":"f","type":"transform","position":{{"x":0,"y":0}},
                     "data":{{"label":"f","componentId":"ctl.file","properties":{}}}}}],"edges":[]}}"#,
                props
            ))
            .expect("parses");
            super::compile(&doc)
        };
        // A copy with nowhere to put it is refused, not quietly turned into a
        // no-op that reports success.
        let e = node(serde_json::json!({ "op": "copy", "source": "/a/x" }))
            .expect_err("copy without a destination must be refused");
        assert!(format!("{e}").contains("destination required"), "got: {e}");
        // Delete needs no destination.
        node(serde_json::json!({ "op": "delete", "source": "/a/x" }))
            .expect("delete needs only a source");
        // An unknown operation is refused rather than defaulting to something
        // that touches the filesystem.
        let e = node(serde_json::json!({ "op": "shred", "source": "/a/x", "destination": "/b/y" }))
            .expect_err("an unknown op must be refused");
        assert!(format!("{e}").contains("unknown op"), "got: {e}");
    }

    #[test]
    fn a_file_list_globs_the_directory_and_names_each_file() {
        let flat = build_filelist_source(&serde_json::json!({
            "directory": "/data/in/", "pattern": "*.csv"
        }));
        // The trailing separator must not double up.
        assert!(flat.contains("glob('/data/in/*.csv')"), "got: {flat}");
        assert!(flat.contains("parse_filename(file) AS filename"), "got: {flat}");
        let deep = build_filelist_source(&serde_json::json!({
            "directory": "/data/in", "pattern": "*.csv", "recursive": true
        }));
        assert!(deep.contains("glob('/data/in/**/*.csv')"), "got: {deep}");
        // No pattern lists everything rather than nothing.
        let all = build_filelist_source(&serde_json::json!({ "directory": "/data/in" }));
        assert!(all.contains("glob('/data/in/*')"), "got: {all}");
    }

    #[test]
    fn a_trigger_edge_orders_a_stage_without_wiring_it() {
        // A trigger says "after this". It constrains when a node runs; it does
        // not claim rows flow along it. Sorting on data edges alone meant the
        // canvas drew the link and the planner ignored it, so the two ends ran
        // in whatever order the sort happened to produce.
        let doc: super::PipelineDoc = serde_json::from_str(
            r#"{
              "nodes": [
                {"id":"a","type":"source","position":{"x":0,"y":0},
                 "data":{"label":"a","componentId":"code.sql",
                         "properties":{"sql":"CREATE OR REPLACE TABLE t1 AS SELECT 1 AS x"}}},
                {"id":"b","type":"transform","position":{"x":200,"y":0},
                 "data":{"label":"b","componentId":"code.sql",
                         "properties":{"sql":"CREATE OR REPLACE TABLE t2 AS SELECT 2 AS x"}}}
              ],
              "edges": [
                {"id":"e1","source":"a","target":"b",
                 "data":{"connectionType":"on-subjob-ok"}}
              ]
            }"#,
        )
        .expect("doc parses");

        let c = super::compile(&doc).expect("compiles");
        let order: Vec<&str> = c.stages.iter().map(|s| s.node_id.as_str()).collect();
        assert_eq!(
            order,
            vec!["a", "b"],
            "the trigger's target must be sequenced after its source, got {:?}",
            order
        );

        // Ordered, not wired: `b` reads no upstream relation, so the trigger did
        // not quietly become a data dependency.
        let b = c.stages.iter().find(|s| s.node_id == "b").expect("stage b");
        assert!(
            b.from.is_none(),
            "a trigger must not wire an input, got from={:?}",
            b.from
        );
    }

    #[test]
    fn csv_headerless_declared_schema_names_the_columns() {
        // A file whose rows carry a record-type discriminator in field 1 has no
        // header, so the reader must take its column names from the declared
        // schema. Without that the relation exposes DuckDB's positional
        // column00..columnNN and every downstream expression fails to bind.
        use duckle_metadata::{Column, DataType};
        let cols = vec![
            Column { name: "c00".into(), data_type: DataType::String, nullable: true, format: None, primary_key: None, tags: Vec::new() },
            Column { name: "c01".into(), data_type: DataType::String, nullable: true, format: None, primary_key: None, tags: Vec::new() },
        ];
        let sql = build_csv_source(
            &serde_json::json!({ "path": "d.txt", "hasHeader": false, "nullPadding": true,
                                 "ignoreErrors": true, "delimiter": "," }),
            Some(&cols),
        );
        // `types=` maps names that ALREADY exist in the file. A headerless file
        // has none, so DuckDB keeps column00..columnNN and the declared names
        // are silently discarded - with ignore_errors on, without even an error.
        // The names have to come from `columns=`, which both names and types.
        assert!(
            sql.contains("columns = {") && sql.contains("'c00'"),
            "a headerless declared schema must be emitted as columns=, got: {}",
            sql
        );
        assert!(
            sql.contains("auto_detect=false"),
            "columns= must disable the sniffer, got: {}",
            sql
        );
        assert!(!sql.contains("types = {"), "must not also emit types=, got: {}", sql);
    }

    #[test]
    fn csv_ignore_errors_and_null_padding() {
        // #98: first-class toggles surface read_csv ignore_errors / null_padding.
        let sql = build_csv_source(
            &serde_json::json!({
                "path": "d.csv",
                "hasHeader": true,
                "ignoreErrors": true,
                "nullPadding": true
            }),
            None,
        );
        assert!(sql.contains("ignore_errors=true"), "got: {}", sql);
        assert!(sql.contains("null_padding=true"), "got: {}", sql);
        // Default off: neither appears.
        let plain = build_csv_source(&serde_json::json!({ "path": "d.csv", "hasHeader": true }), None);
        assert!(!plain.contains("ignore_errors"), "got: {}", plain);
        assert!(!plain.contains("null_padding"), "got: {}", plain);
    }

    #[test]
    fn an_artifact_is_described_without_reading_it() {
        // #247. An artifact is a REFERENCE - uri, media type, size, hash - which is a
        // ROW, so it travels through the joins, filters and loops that already exist
        // rather than needing an edge type of its own.
        use crate::plan::builders::build_artifact_source;

        let plain = build_artifact_source(&serde_json::json!({ "path": "/docs", "glob": "*.pdf" }));
        assert!(plain.contains("read_blob("), "got: {plain}");
        assert!(plain.contains("/docs/*.pdf"), "got: {plain}");
        assert!(plain.contains("AS uri"), "got: {plain}");
        assert!(plain.contains("AS media_type"), "got: {plain}");
        assert!(plain.contains("AS size_bytes"), "got: {plain}");
        // Hashing reads every byte, which is what the issue says NOT to do to a large
        // model file, so it is off until asked for - the column is still there so the
        // shape does not change under anything downstream.
        assert!(plain.contains("CAST(NULL AS VARCHAR) AS sha256"), "got: {plain}");
        assert!(!plain.contains("sha256(content)"), "got: {plain}");

        let hashed = build_artifact_source(
            &serde_json::json!({ "path": "/docs", "glob": "*.pdf", "hash": true }),
        );
        assert!(hashed.contains("sha256(content) AS sha256"), "got: {hashed}");

        // Sub-folders only when asked.
        let deep = build_artifact_source(
            &serde_json::json!({ "path": "/docs", "glob": "*.pdf", "recursive": true }),
        );
        assert!(deep.contains("/docs/**/*.pdf"), "got: {deep}");
    }

    #[test]
    fn a_cached_stage_is_written_once_and_read_back_after() {
        // #252. Some stages are expensive and deterministic - a big download, an OCR
        // pass, an embedding run - and re-running them because something DOWNSTREAM
        // changed is the slowest part of working on a pipeline.
        //
        // Opt-in per node, deliberately. A cache that decides for itself when it is
        // still valid and gets it wrong serves stale data and says nothing, which is
        // worse than being slow.
        let doc = |cache: &str| {
            pipeline_from_json(&format!(
                r#"{{
                  "nodes": [
                    {{"id":"s","position":{{"x":0,"y":0}},"data":{{
                      "label":"CSV","componentId":"src.csv",
                      "properties":{{"path":"/tmp/a.csv","hasHeader":true{cache}}}}}}},
                    {{"id":"f","position":{{"x":0,"y":0}},"data":{{
                      "label":"Filter","componentId":"xf.filter",
                      "properties":{{"predicate":"amt > 1"}}}}}}
                  ],
                  "edges":[{{"id":"e","source":"s","target":"f","data":{{"connectionType":"main"}}}}]
                }}"#
            ))
        };
        // Driven with a folder of its own rather than an environment variable, so it
        // neither depends on how the suite was launched nor disturbs anything else.
        let tmp = tempfile::tempdir().unwrap();
        let sql_of = |d: &super::PipelineDoc, id: &str| {
            let mut stages = compile(d).unwrap().stages;
            crate::plan::apply_stage_cache_in(d, &mut stages, tmp.path());
            stages.into_iter().find(|s| s.node_id == id).unwrap().sql
        };

        // Not asked for, nothing changes at all.
        let plain = sql_of(&doc(""), "s");
        assert!(!plain.contains("COPY ("), "not asked for, nothing added: {plain}");

        // Asked for: the stage writes its output once and reads it back, so the node
        // still hands on a relation of the same name and nothing downstream can tell.
        let cached = sql_of(&doc(r#","cache":true"#), "s");
        assert!(cached.contains("COPY ("), "it has to materialise: {cached}");
        assert!(
            cached.contains("read_parquet("),
            "and be read back so the node keeps its shape: {cached}"
        );
        assert!(
            cached.contains("CREATE OR REPLACE VIEW \"s\""),
            "the relation keeps the node's name: {cached}"
        );

        // The key follows the SQL: change what the stage computes and it must not read
        // back the old answer.
        let other = pipeline_from_json(
            r#"{"nodes":[{"id":"s","position":{"x":0,"y":0},"data":{
                 "label":"CSV","componentId":"src.csv",
                 "properties":{"path":"/tmp/DIFFERENT.csv","hasHeader":true,"cache":true}}}],
               "edges":[]}"#,
        );
        let a = cached;
        let b = sql_of(&other, "s");
        // the file name the stage writes to, which carries the key
        let key = |s: &str| {
            let head = s.split(".parquet").next().unwrap_or("");
            head.rsplit('/').next().unwrap_or("").to_string()
        };
        assert_ne!(key(&a), key(&b), "a different read is a different key");

        // And in a real workspace it lands somewhere obvious and easy to throw away:
        // deleting the folder is the whole of "clear the cache".
        // DUCKLE_WORKSPACE is process-global; without this the remove_var below
        // reaches into whatever test is running alongside. It did: the SFTP
        // known-hosts tests read their file through this variable, and lost it
        // mid-test roughly one run in three.
        let _g = crate::util::workspace_env_guard();
        std::env::set_var("DUCKLE_WORKSPACE", tmp.path());
        let dir = crate::plan::cache_dir().expect("a workspace gives a cache folder");
        std::env::remove_var("DUCKLE_WORKSPACE");
        assert!(dir.ends_with(std::path::Path::new(".duckle").join("duckle_cache")), "{dir:?}");
    }

    #[test]
    fn a_check_told_to_fail_the_run_fails_the_run() {
        // Every quality check offers "On failure: reject / warn / fail". Only reject ever
        // happened - the setting was never read, so a gate configured to STOP a load let
        // it through and dropped the offending rows on the way. A run that was asked to
        // stop and did not is the worst of the three outcomes: it reports success.
        use crate::plan::builders::build_quality;
        let inputs = {
            let mut ni = crate::plan::graph::NodeInputs::default();
            ni.ports.insert("main".into(), vec!["up".into()]);
            ni
        };

        let fail = build_quality(
            &inputs,
            &serde_json::json!({ "columns": "amt", "onFail": "fail" }),
            "qa.notnull",
            false,
        )
        .unwrap();
        assert!(fail.contains("error("), "it has to raise: {fail}");
        assert!(fail.contains("qa.notnull"), "and say which check: {fail}");

        // reject is what it always did, and stays byte for byte what it was.
        let reject = build_quality(
            &inputs,
            &serde_json::json!({ "columns": "amt", "onFail": "reject" }),
            "qa.notnull",
            false,
        )
        .unwrap();
        let unset = build_quality(
            &inputs,
            &serde_json::json!({ "columns": "amt" }),
            "qa.notnull",
            false,
        )
        .unwrap();
        assert_eq!(reject, unset, "reject is the default, unchanged");
        assert!(!unset.contains("error("), "got: {unset}");

        // warn does not stop the run either, so it must not raise.
        let warn = build_quality(
            &inputs,
            &serde_json::json!({ "columns": "amt", "onFail": "warn" }),
            "qa.notnull",
            false,
        )
        .unwrap();
        assert!(!warn.contains("error("), "got: {warn}");

        // The reject PORT is unaffected whatever the setting says - it is what feeds a
        // dead-letter branch, and raising there would break the branch that exists to
        // catch these rows.
        let port = build_quality(
            &inputs,
            &serde_json::json!({ "columns": "amt", "onFail": "fail" }),
            "qa.notnull",
            true,
        )
        .unwrap();
        assert!(!port.contains("error("), "the reject port never raises: {port}");
    }

    #[test]
    fn the_geospatial_sink_writes_geoparquet_through_the_parquet_writer() {
        // #241. GeoParquet was asked for on the Geospatial sink, which writes through
        // GDAL. The bundled spatial extension has no GDAL Parquet driver - st_drivers()
        // lists none, and a COPY naming one writes NO FILE AT ALL and does not complain,
        // so offering it there would have been a silent no-op.
        //
        // DuckDB's own Parquet writer does write GeoParquet: the footer carries the
        // `geo` key and the geometry keeps its CRS. So the option exists where it was
        // looked for, and goes out through the writer that works.
        use crate::plan::builders::build_spatial_sink;
        let gp = build_spatial_sink(
            &serde_json::json!({ "path": "/out/f.parquet", "driver": "GeoParquet" }),
            "up",
        );
        assert!(gp.contains("FORMAT PARQUET"), "got: {gp}");
        assert!(!gp.contains("FORMAT GDAL"), "not through GDAL: {gp}");

        // Every other driver still goes through GDAL exactly as before.
        for d in ["GeoJSON", "GPKG", "ESRI Shapefile", "KML", "GPX"] {
            let sql = build_spatial_sink(
                &serde_json::json!({ "path": "/out/f", "driver": d }),
                "up",
            );
            assert!(sql.contains("FORMAT GDAL"), "{d}: {sql}");
            assert!(sql.contains(&format!("DRIVER '{d}'")), "{d}: {sql}");
        }
        // Unset is GeoJSON through GDAL, as it was.
        let dflt = build_spatial_sink(&serde_json::json!({ "path": "/out/f" }), "up");
        assert!(dflt.contains("FORMAT GDAL, DRIVER 'GeoJSON'"), "got: {dflt}");
    }

    #[test]
    fn json_flatten_is_a_setting_and_repeated_keys_can_keep_their_parent() {
        // #238. Two things, reported together.
        //
        // "Flatten nested objects" did nothing: the records branch always flattened all
        // the way down and nothing ever read the setting, so the only way to stop it was
        // to override the generated SQL.
        let off = build_json_source(&serde_json::json!({
            "path": "d.json", "recordsPath": "data", "flatten": false
        }));
        assert!(!off.contains("recursive"), "not asked for, not done: {off}");
        // Still a usable table: the records are rows and their own keys are columns,
        // with what is nested inside them left whole. Unnesting the list alone gives one
        // struct column named after the expression, which is not a table.
        assert!(off.contains("SELECT unnest(r) FROM (SELECT unnest("), "got: {off}");
        let on = build_json_source(&serde_json::json!({
            "path": "d.json", "recordsPath": "data", "flatten": true
        }));
        assert!(on.contains("recursive := true"), "got: {on}");
        // Unset stays as it was, so saved pipelines do not change under anyone.
        let unset = build_json_source(&serde_json::json!({
            "path": "d.json", "recordsPath": "data"
        }));
        assert!(unset.contains("recursive := true"), "got: {unset}");

        // And a document whose objects each carry an Id flattened to Id, Id_1, Id_2 -
        // names that say nothing about where they came from. Keeping the parent gives
        // Id, owner.Id, account.Id.
        let kept = build_json_source(&serde_json::json!({
            "path": "d.json", "recordsPath": "data", "keepParentNames": true
        }));
        assert!(kept.contains("keep_parent_names := true"), "got: {kept}");
        assert!(!unset.contains("keep_parent_names"), "off unless asked: {unset}");

        // With no records path there was no flattening step at all, so ticking the box
        // did nothing whatever - which is the half of the report that reads as "the
        // setting has no effect". Asked for, the whole row is flattened.
        let whole = build_json_source(&serde_json::json!({
            "path": "d.json", "flatten": true, "keepParentNames": true
        }));
        assert!(whole.contains("unnest("), "got: {whole}");
        assert!(whole.contains("recursive := true"), "got: {whole}");
        assert!(whole.contains("keep_parent_names := true"), "got: {whole}");
        // Not asked for, the read is a plain one - now carrying sample_size=-1,
        // because DuckDB's 20480-row default silently DROPS a column that first
        // appears later in a sparse document.
        let flat_off = build_json_source(&serde_json::json!({ "path": "d.json" }));
        assert_eq!(
            flat_off,
            "SELECT * FROM read_json_auto('d.json', maximum_object_size=104857600, sample_size=-1)"
        );
    }

    #[test]
    fn json_ignore_errors_and_format() {
        // #101: skip malformed records instead of aborting + wire the Format dropdown.
        let sql = build_json_source(&serde_json::json!({
            "path": "d.json", "format": "jsonl", "ignoreErrors": true
        }));
        assert!(sql.contains("ignore_errors=true"), "got: {}", sql);
        assert!(sql.contains("format='newline_delimited'"), "got: {}", sql);
        // The recordsPath branch carries the same args.
        let nested = build_json_source(&serde_json::json!({
            "path": "d.json", "recordsPath": "data", "ignoreErrors": true
        }));
        assert!(nested.contains("ignore_errors=true"), "got: {}", nested);
        // Default off: neither appears, auto format omitted.
        let plain = build_json_source(&serde_json::json!({ "path": "d.json" }));
        assert!(!plain.contains("ignore_errors"), "got: {}", plain);
        assert!(!plain.contains("format="), "got: {}", plain);
    }

    #[test]
    fn custom_sql_raw_mode_skips_input_wrapper() {
        // #102 item 3: rawSql=true emits the user SQL verbatim so a leading WITH
        // is not broken by the "WITH input AS (...)" wrapper.
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let raw = build_custom_sql(
            &ni,
            &serde_json::json!({
                "sql": "WITH c AS (SELECT * FROM \"up\") SELECT * FROM c",
                "rawSql": true
            }),
        )
        .unwrap();
        assert!(!raw.contains("WITH input AS"), "raw mode must not wrap: {}", raw);
        assert!(raw.starts_with("WITH c AS"), "raw SQL emitted verbatim: {}", raw);
        // Default (no rawSql) still wraps the upstream as `input`.
        let wrapped =
            build_custom_sql(&ni, &serde_json::json!({ "sql": "SELECT * FROM input" })).unwrap();
        assert!(
            wrapped.contains("WITH input AS (SELECT * FROM \"up\")"),
            "default wraps: {}",
            wrapped
        );
    }

    #[test]
    fn contract_builds_gated_passthrough() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_contract(
            &ni,
            &serde_json::json!({ "rules": [
                { "column": "email", "check": "not_null" },
                { "column": "amt",   "check": "in_range", "args": { "min": 0, "max": 10 } },
                { "column": "id",    "check": "unique" }
            ]}),
        )
        .unwrap();
        assert!(sql.contains("SELECT u.* FROM \"up\" u"), "got: {}", sql);
        assert!(sql.contains("WITH _duckle_contract AS MATERIALIZED"), "got: {}", sql);
        assert!(sql.contains("error('Data contract violated: '"), "got: {}", sql);
        assert!(sql.contains("COUNT(*) FILTER (WHERE NOT (\"amt\" BETWEEN 0 AND 10)) AS BIGINT) AS f1"), "got: {}", sql);
        assert!(sql.contains("COUNT(*) OVER (PARTITION BY \"id\") = 1"), "got: {}", sql);
        assert!(sql.contains("(f0 + f1 + f2) > 0"), "got: {}", sql);
        assert!(build_contract(&ni, &serde_json::json!({ "rules": [] })).is_err());
        assert!(build_contract(&NodeInputs::default(), &serde_json::json!({ "rules": [{ "column": "x", "check": "not_null" }] })).is_err());
    }

    #[test]
    fn surrogate_key_builds_hash_and_sequence() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let hash = build_surrogate_key(&ni, &serde_json::json!({"mode":"hash","keyColumns":["company","country"]})).unwrap();
        assert!(hash.contains("md5(concat_ws('||', CAST(\"company\" AS VARCHAR), CAST(\"country\" AS VARCHAR))) AS \"surrogate_key\""), "got: {}", hash);
        let sep = build_surrogate_key(&ni, &serde_json::json!({"mode":"hash","keyColumns":["id"],"separator":"-","outputColumn":"dim_key"})).unwrap();
        assert!(sep.contains("md5(concat_ws('-', CAST(\"id\" AS VARCHAR))) AS \"dim_key\""), "got: {}", sep);
        let seq = build_surrogate_key(&ni, &serde_json::json!({"mode":"sequence","keyColumns":["company","country"]})).unwrap();
        assert!(seq.contains("row_number() OVER (ORDER BY \"company\", \"country\") AS \"surrogate_key\""), "got: {}", seq);
        assert!(build_surrogate_key(&ni, &serde_json::json!({"mode":"hash"})).is_err());
        assert!(build_surrogate_key(&ni, &serde_json::json!({"mode":"bogus","keyColumns":["id"]})).is_err());
    }

    #[test]
    fn bucketize_labeled_bounds_mode() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        // bounds (array) -> labeled half-open ranges + NULL guard.
        let sql = build_bucketize(&ni, &serde_json::json!({"column":"age","bounds":[18,40,65]})).unwrap();
        assert!(sql.contains("CASE WHEN \"age\" IS NULL THEN NULL"), "got: {}", sql);
        assert!(sql.contains("< 18 THEN '<18'"), "got: {}", sql);
        assert!(sql.contains("< 40 THEN '18-40'"), "got: {}", sql);
        assert!(sql.contains("ELSE '>=65'"), "got: {}", sql);
        // comma-string bounds + custom labels.
        let lab = build_bucketize(&ni, &serde_json::json!({"column":"age","bounds":"18,65","labels":["minor","adult","senior"]})).unwrap();
        assert!(lab.contains("THEN 'minor'") && lab.contains("THEN 'adult'") && lab.contains("ELSE 'senior'"), "got: {}", lab);
        // wrong label count is a loud error; without bounds the equal-width path still needs low/high.
        assert!(build_bucketize(&ni, &serde_json::json!({"column":"age","bounds":[18],"labels":["a","b","c"]})).is_err());
        let eqw = build_bucketize(&ni, &serde_json::json!({"column":"age","low":0,"high":100,"buckets":4})).unwrap();
        assert!(eqw.contains("floor(") && !eqw.contains("'<"), "equal-width path unchanged: {}", eqw);
    }

    #[test]
    fn scd3_keeps_previous_value_in_sibling_column() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["c1".into()]);
        ni.ports.insert("lookup".into(), vec!["p1".into()]);
        let props = serde_json::json!({ "keyColumns": ["id"], "trackColumns": ["v"], "effectiveDateColumn": "effective_date" });
        let sql = build_scd3(&ni, &props).unwrap();
        assert_eq!(
            sql,
            "SELECT c.*, p.\"v\" AS \"previous_v\", CURRENT_TIMESTAMP AS \"effective_date\" FROM \"c1\" c LEFT JOIN \"p1\" p ON p.\"id\" = c.\"id\"",
            "got: {}", sql
        );
        let multi = build_scd3(&ni, &serde_json::json!({ "keyColumns": ["region", "id"], "trackColumns": ["name", "score"] })).unwrap();
        assert!(multi.contains("p.\"name\" AS \"previous_name\"") && multi.contains("p.\"score\" AS \"previous_score\""), "got: {}", multi);
        assert!(multi.contains("p.\"region\" = c.\"region\" AND p.\"id\" = c.\"id\""), "got: {}", multi);
        assert!(!multi.contains("CURRENT_TIMESTAMP"), "no effective date when unset: {}", multi);
        assert!(build_scd3(&ni, &serde_json::json!({ "trackColumns": ["v"] })).is_err());
        let mut no_prev = NodeInputs::default();
        no_prev.ports.insert("main".into(), vec!["c1".into()]);
        assert!(build_scd3(&no_prev, &props).is_err());
    }

    /// The SCD3 form and the SCD3 builder have to agree on the property names.
    ///
    /// They did not. The manifest declares `naturalKey` / `compareColumns`,
    /// which is what every other CDC builder reads, and `build_scd3` alone read
    /// `keyColumns` / `trackColumns`. So configuring SCD3 in the editor produced
    /// "SCD3 needs key columns" while looking at a filled-in Natural key field,
    /// and there was no way to fix it from the UI because the field the error
    /// asks for is not on the form.
    ///
    /// The older test passed throughout, because it asserted the spelling the
    /// code used rather than the one the form writes.
    #[test]
    fn scd3_reads_the_property_names_its_form_writes() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["c1".into()]);
        ni.ports.insert("lookup".into(), vec!["p1".into()]);

        // Exactly what the editor stores for this component.
        let declared = serde_json::json!({
            "naturalKey": ["id"],
            "compareColumns": ["v"],
        });
        let sql = build_scd3(&ni, &declared).expect(
            "the keys the form writes must build; anything else is a component              nobody can configure from the GUI",
        );
        assert!(
            sql.contains("p.\"v\" AS \"previous_v\""),
            "the tracked column has to come from compareColumns: {sql}"
        );
        assert!(
            sql.contains("p.\"id\" = c.\"id\""),
            "the join key has to come from naturalKey: {sql}"
        );

        // The engine's own older spelling keeps working, so a hand-written
        // pipeline that used it does not break.
        let legacy = serde_json::json!({ "keyColumns": ["id"], "trackColumns": ["v"] });
        assert!(build_scd3(&ni, &legacy).is_ok(), "the legacy spelling must stay accepted");
    }

    /// The Fixed-width form and the Fixed-width source have to agree.
    ///
    /// They did not. The form offers `columnWidths` ("10,20,8"), and the
    /// builder required `columns` as an array of {name,start,width} and
    /// nothing else, so configuring the node in the editor failed outright
    /// with "columns array required" and no field on the form could satisfy
    /// it. `columnWidths` appears nowhere but the manifest: no converter, no
    /// engine read.
    ///
    /// Widths are cumulative, so the Nth column starts after the previous
    /// ones. Names come from the declared schema when there is one, which is
    /// the same rule a headerless CSV already follows.
    #[test]
    fn fixedwidth_reads_the_widths_its_form_writes() {
        use duckle_metadata::{Column, DataType};

        let props = serde_json::json!({ "path": "/tmp/f.txt", "columnWidths": "10,5,8" });
        let sql = build_fixedwidth_source(&props, None).expect(
            "the key the form writes must build; anything else is a source \
             nobody can configure from the GUI",
        );
        // 1-based, cumulative: 1, then 1+10, then 1+10+5.
        assert!(sql.contains("substr(line, 1, 10)"), "first column: {sql}");
        assert!(sql.contains("substr(line, 11, 5)"), "second starts after the first: {sql}");
        assert!(sql.contains("substr(line, 16, 8)"), "third starts after both: {sql}");

        // With no declared schema the names are positional but usable.
        assert!(sql.contains("AS \"col1\""), "got: {sql}");

        // A declared schema names them, exactly like a headerless CSV.
        let declared = vec![
            Column { name: "id".into(), data_type: DataType::String, nullable: true, primary_key: None, format: None, tags: Vec::new() },
            Column { name: "code".into(), data_type: DataType::String, nullable: true, primary_key: None, format: None, tags: Vec::new() },
            Column { name: "amount".into(), data_type: DataType::String, nullable: true, primary_key: None, format: None, tags: Vec::new() },
        ];
        let named = build_fixedwidth_source(&props, Some(&declared)).unwrap();
        assert!(named.contains("AS \"id\""), "declared names must win: {named}");
        assert!(named.contains("AS \"amount\""), "got: {named}");

        // The explicit form keeps working unchanged.
        let explicit = serde_json::json!({
            "path": "/tmp/f.txt",
            "columns": [{ "name": "a", "start": 1, "width": 3 }],
        });
        let ex = build_fixedwidth_source(&explicit, None).unwrap();
        assert!(ex.contains("substr(line, 1, 3)") && ex.contains("AS \"a\""), "got: {ex}");

        // Neither form supplied is still an error, and it names both keys.
        let err = build_fixedwidth_source(&serde_json::json!({ "path": "/tmp/f.txt" }), None)
            .unwrap_err();
        assert!(err.contains("columnWidths"), "the error must name the form's key: {err}");
    }

    #[test]
    fn qa_outlier_emits_iqr_and_zscore_pass_reject_sql() {
        let mk = || { let mut ni = NodeInputs::default(); ni.ports.insert("main".into(), vec!["up".into()]); ni };
        let iqr_pass = build_outlier(&mk(), &serde_json::json!({"column": "amount", "method": "iqr"}), false).unwrap();
        assert!(iqr_pass.contains("quantile_cont") && iqr_pass.contains("1.5"), "got: {}", iqr_pass);
        assert!(iqr_pass.contains("\"amount\" IS NULL"), "got: {}", iqr_pass);
        assert!(iqr_pass.contains("EXCLUDE (__dq_q1, __dq_q3)"), "got: {}", iqr_pass);
        assert!(iqr_pass.contains("COALESCE") && !iqr_pass.contains("NOT COALESCE"), "got: {}", iqr_pass);
        let iqr_rej = build_outlier(&mk(), &serde_json::json!({"column": "amount", "method": "iqr"}), true).unwrap();
        assert!(iqr_rej.contains("NOT COALESCE"), "got: {}", iqr_rej);
        let z_pass = build_outlier(&mk(), &serde_json::json!({"column": "amount", "method": "zscore"}), false).unwrap();
        assert!(z_pass.contains("stddev_pop") && z_pass.contains("__dq_sd = 0") && z_pass.contains("<= 3"), "got: {}", z_pass);
        assert!(build_outlier(&mk(), &serde_json::json!({"method": "iqr"}), false).is_err());
        assert!(build_outlier(&mk(), &serde_json::json!({"column": "amount", "sensitivity": 0}), false).is_err());
    }

    #[test]
    fn sessionize_builds_gap_window_sql() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let sql = build_sessionize(&ni, &serde_json::json!({"partitionBy": ["user_id"], "orderBy": "ts", "gap": 30})).unwrap();
        assert!(sql.starts_with("WITH __flag AS ("), "got: {}", sql);
        assert!(sql.contains("epoch(CAST(\"ts\" AS TIMESTAMP)) - epoch(CAST(lag(\"ts\") OVER w AS TIMESTAMP))) > 1800"), "got: {}", sql);
        assert!(sql.contains("WINDOW w AS (PARTITION BY \"user_id\" ORDER BY \"ts\")"), "got: {}", sql);
        assert!(sql.contains("SUM(__new_sess) OVER (PARTITION BY \"user_id\" ORDER BY \"ts\") AS \"session_id\""), "got: {}", sql);
        assert!(sql.contains("ROW_NUMBER() OVER (PARTITION BY \"user_id\", \"session_id\" ORDER BY \"ts\") AS \"session_seq\""), "got: {}", sql);
        let hrs = build_sessionize(&ni, &serde_json::json!({ "orderBy": "ts", "gap": 1, "gapUnit": "hours", "emitSeq": false })).unwrap();
        assert!(hrs.contains("> 3600") && hrs.ends_with("SELECT * FROM __sid") && !hrs.contains("session_seq"), "got: {}", hrs);
        assert!(build_sessionize(&ni, &serde_json::json!({ "gap": 5 })).is_err());
        assert!(build_sessionize(&ni, &serde_json::json!({ "orderBy": "ts", "gap": 0 })).is_err());
        assert!(build_sessionize(&ni, &serde_json::json!({ "orderBy": "ts", "gap": 5, "gapUnit": "weeks" })).is_err());
    }

    #[test]
    fn freshness_builds_gate_and_report() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        let gate = build_freshness(&ni, &serde_json::json!({ "column": "ts", "maxAge": 24, "maxAgeUnit": "hours" })).unwrap();
        assert!(gate.contains("WITH _duckle_freshness AS MATERIALIZED") && gate.contains("SELECT u.* FROM \"up\" u"), "got: {}", gate);
        assert!(gate.contains("date_diff('hour', MAX(CAST(\"ts\" AS TIMESTAMP)), CURRENT_TIMESTAMP) <= 24 THEN 'ok'"), "got: {}", gate);
        assert!(gate.contains("error('Data is stale: '"), "got: {}", gate);
        let report = build_freshness(&ni, &serde_json::json!({ "column": "ts", "maxAge": 2, "maxAgeUnit": "days", "mode": "report" })).unwrap();
        assert!(report.contains("date_diff('day', MAX(CAST(\"ts\" AS TIMESTAMP)), CURRENT_TIMESTAMP) AS age_days"), "got: {}", report);
        assert!(report.contains("2 AS threshold_days") && report.contains("<= 2) AS is_fresh"), "got: {}", report);
        assert!(build_freshness(&ni, &serde_json::json!({ "maxAge": 1 })).is_err());
        assert!(build_freshness(&ni, &serde_json::json!({ "column": "ts" })).is_err());
        assert!(build_freshness(&ni, &serde_json::json!({ "column": "ts", "maxAge": 1, "mode": "bogus" })).is_err());
    }

    /// A driving input on `main` plus a second layer on `lookup_1`, which is
    /// how the canvas wires a two-input transform.
    fn spatial_join_inputs() -> crate::plan::graph::NodeInputs {
        let mut ni = crate::plan::graph::NodeInputs::default();
        ni.ports.insert("main".into(), vec!["left_layer".into()]);
        ni.ports.insert("lookup_1".into(), vec!["right_layer".into()]);
        ni
    }

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn spatial_join_supports_covers_and_covered_by() {
        // #220: Covers / CoveredBy differ from Contains / Within at the
        // boundary, which is exactly when they matter.
        use crate::plan::builders::build_spatial_join;
        let inputs = spatial_join_inputs();
        for (relation, expect) in [
            ("covers", "ST_Covers("),
            ("coveredby", "ST_CoveredBy("),
            ("contains", "ST_Contains("),
            ("intersects", "ST_Intersects("),
        ] {
            let props = serde_json::json!({
                "leftGeomColumn": "geom", "rightGeomColumn": "geom", "relation": relation
            });
            let sql = build_spatial_join(&inputs, &props).expect(relation);
            assert!(sql.contains(expect), "{relation}: {sql}");
        }
        // An unknown relation still falls back rather than emitting bad SQL.
        let props = serde_json::json!({
            "leftGeomColumn": "geom", "rightGeomColumn": "geom", "relation": "nonsense"
        });
        let sql = build_spatial_join(&inputs, &props).unwrap();
        assert!(sql.contains("ST_Intersects("), "fallback: {sql}");
    }

    #[test]
    fn spatial_join_guards_against_mismatched_crs() {
        // #219: joining across different CRS made every predicate false, so the
        // run succeeded and returned zero rows with no clue why. The guard is a
        // WHERE predicate rather than a select-list column on purpose: a
        // cross-joined column nothing reads can be optimised away, and a pruned
        // guard never fires.
        use crate::plan::builders::build_spatial_join;
        let props = serde_json::json!({
            "leftGeomColumn": "geom", "rightGeomColumn": "shape", "relation": "intersects"
        });
        let sql = build_spatial_join(&spatial_join_inputs(), &props).unwrap();
        assert!(sql.contains("regexp_extract(typeof("), "no CRS probe: {sql}");
        assert!(sql.contains("error("), "no error guard: {sql}");
        assert!(
            sql.contains("WHERE (SELECT __ok FROM __crs_guard)"),
            "guard must be a filter so it cannot be pruned: {sql}"
        );
        // Both column names must be probed, not just one.
        assert!(sql.contains("\"geom\"") && sql.contains("\"shape\""), "{sql}");
        // Only two KNOWN and DIFFERENT systems are an error; an unresolved CRS
        // stays permissive so existing pipelines keep running.
        assert!(sql.contains("__l <> '' AND __r <> '' AND __l <> __r"), "{sql}");
    }

    #[test]
    fn geo_clip_dissolves_the_clip_layer_and_keeps_attributes() {
        // #217. Two behaviours make this Clip rather than a spatial join:
        // the clip layer is dissolved first (otherwise one input feature
        // spanning three clip polygons comes back three times), and the input's
        // attributes survive with only the geometry column replaced.
        use crate::plan::builders::build_geo_clip;
        let props = serde_json::json!({ "geomColumn": "geom" });
        let sql = build_geo_clip(&spatial_join_inputs(), &props).expect("clip");
        assert!(sql.contains("ST_Union_Agg("), "clip layer must be dissolved: {sql}");
        assert!(sql.contains("ST_Intersection("), "{sql}");
        assert!(
            sql.contains("m.* REPLACE (ST_Intersection("),
            "attributes must be preserved with only geometry replaced: {sql}"
        );
        // Non-overlapping features are excluded, not returned with empty geometry.
        assert!(sql.contains("ST_Intersects("), "{sql}");
        // And the shared CRS guard applies.
        assert!(sql.contains("__crs_guard"), "{sql}");

        // A separate clip-layer column name is honoured.
        let props = serde_json::json!({ "geomColumn": "geom", "clipGeomColumn": "boundary" });
        let sql = build_geo_clip(&spatial_join_inputs(), &props).unwrap();
        assert!(sql.contains("\"boundary\""), "{sql}");

        // Both inputs are required, and the message says which is missing.
        let mut only_main = crate::plan::graph::NodeInputs::default();
        only_main.ports.insert("main".into(), vec!["a".into()]);
        let err = build_geo_clip(&only_main, &serde_json::json!({ "geomColumn": "geom" }))
            .unwrap_err();
        assert!(err.contains("clip layer"), "{err}");
    }

    #[test]
    fn geo_erase_dissolves_the_erase_layer_and_drops_emptied_rows() {
        // #218. Dissolving matters even more here: differencing against each
        // erase feature in turn would only ever remove the last one.
        use crate::plan::builders::build_geo_erase;
        let props = serde_json::json!({ "geomColumn": "geom" });
        let sql = build_geo_erase(&spatial_join_inputs(), &props).expect("erase");
        assert!(sql.contains("ST_Union_Agg("), "erase layer must be dissolved: {sql}");
        assert!(sql.contains("ST_Difference("), "{sql}");
        assert!(
            sql.contains("ST_IsEmpty("),
            "fully erased features must be dropped: {sql}"
        );
        assert!(sql.contains("__crs_guard"), "{sql}");
        // An empty erase layer must leave the input untouched rather than
        // NULL out every geometry.
        assert!(sql.contains("__e.__g IS NULL"), "{sql}");

        let err = build_geo_erase(
            &crate::plan::graph::NodeInputs::default(),
            &serde_json::json!({ "geomColumn": "geom" }),
        )
        .unwrap_err();
        assert!(err.contains("input layer"), "{err}");
    }

    #[test]
    fn ducklake_attach_carries_the_catalog_options_a_lake_needs() {
        // A Postgres-catalogued lake usually shares the database with other things, so
        // the catalog tables live in a schema of their own - and the schema is named by
        // METADATA_SCHEMA, which no property could reach. Nor could META_* parameters,
        // which is how a catalog is pointed at a stored secret instead of a DSN with a
        // password in it. Neither had a way through, so those lakes could not be attached
        // at all.
        use crate::plan::builders::ducklake_attach;
        let pg = ducklake_attach(
            &serde_json::json!({
                "path": "postgres:dbname=lake host=localhost",
                "dataPath": "s3://bucket/lake/",
                "metadataSchema": "ducklake_catalog",
                "attachOptions": [{ "key": "META_SECRET", "value": "postgres_secret" }]
            }),
            true,
        );
        assert!(pg.contains("METADATA_SCHEMA 'ducklake_catalog'"), "{pg}");
        assert!(pg.contains("META_SECRET 'postgres_secret'"), "{pg}");

        // A value with a quote in it is escaped, not passed through to end the string.
        let odd = ducklake_attach(
            &serde_json::json!({ "path": "postgres:dbname=l", "metadataSchema": "it's" }),
            false,
        );
        assert!(odd.contains("METADATA_SCHEMA 'it''s'"), "{odd}");

        // An option name is not a place to put SQL, so only a plain name is taken.
        let bad = ducklake_attach(
            &serde_json::json!({
                "path": "postgres:dbname=l",
                "attachOptions": [{ "key": "X'); DROP TABLE t; --", "value": "v" }]
            }),
            false,
        );
        assert!(!bad.contains("DROP TABLE"), "{bad}");

        // Saying none of it reproduces the previous output exactly.
        let plain = ducklake_attach(&serde_json::json!({ "path": "/lakes/a.ducklake" }), true);
        assert_eq!(
            plain,
            "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:/lakes/a.ducklake' \
             AS duckle_src (READ_ONLY); "
        );
    }

    #[test]
    fn ducklake_attach_emits_data_path_only_when_set() {
        // A postgres:/sqlite:/mysql: catalog carries no implied data location,
        // so DuckLake needs DATA_PATH. Omitting the property must reproduce the
        // previous output exactly, or every saved pipeline changes behaviour.
        use crate::plan::builders::ducklake_attach;
        let plain = ducklake_attach(&serde_json::json!({ "path": "/lakes/a.ducklake" }), true);
        assert!(plain.contains("ATTACH 'ducklake:/lakes/a.ducklake' AS duckle_src (READ_ONLY);"));
        assert!(!plain.contains("DATA_PATH"), "{plain}");

        let pg = ducklake_attach(
            &serde_json::json!({
                "path": "postgres:dbname=lake host=localhost",
                "dataPath": "s3://bucket/lake/"
            }),
            true,
        );
        assert!(
            pg.contains(
                "ATTACH 'ducklake:postgres:dbname=lake host=localhost' AS duckle_src \
                 (READ_ONLY, DATA_PATH 's3://bucket/lake/');"
            ),
            "{pg}"
        );
        // Write mode has no READ_ONLY, so DATA_PATH must still be parenthesised.
        let w = ducklake_attach(
            &serde_json::json!({ "path": "sqlite:m.db", "dataPath": "data_files/" }),
            false,
        );
        assert!(
            w.contains("AS duckle_dst (DATA_PATH 'data_files/');"),
            "{w}"
        );
        // Blank is treated as absent.
        let blank = ducklake_attach(
            &serde_json::json!({ "path": "/lakes/a.ducklake", "dataPath": "   " }),
            false,
        );
        assert!(!blank.contains("DATA_PATH"), "{blank}");
    }

    #[test]
    fn attach_prelude_loads_spatial_for_sql_template() {
        // #84: spatial loads on opt-in OR when the SQL references an ST_ function,
        // but not for unrelated SQL (and `list_` must not false-fire).
        let opt = attach_prelude("code.sql", &serde_json::json!({ "loadSpatial": true }));
        assert!(opt.contains("LOAD spatial"), "opt-in: {}", opt);
        let auto = attach_prelude("code.sqltemplate", &serde_json::json!({ "sql": "SELECT ST_Point(lon,lat) FROM input" }));
        assert!(auto.contains("LOAD spatial"), "auto-detect: {}", auto);
        let none = attach_prelude("code.sql", &serde_json::json!({ "sql": "SELECT list_value(a), first_name FROM input" }));
        assert!(!none.contains("spatial"), "must not false-fire on list_/first_: {}", none);
    }

    #[test]
    fn attach_prelude_loads_user_extensions_for_sql_template() {
        // #113: loadExtensions installs + loads each named extension. Accepts a
        // comma/space-separated string or a JSON array; names are sanitized so
        // they can never inject SQL; spatial is not loaded twice.
        let s = attach_prelude("code.sql", &serde_json::json!({ "loadExtensions": "h3, a5" }));
        assert!(s.contains("INSTALL h3; LOAD h3;"), "h3: {}", s);
        assert!(s.contains("INSTALL a5; LOAD a5;"), "a5: {}", s);
        let arr = attach_prelude("code.sqltemplate", &serde_json::json!({ "loadExtensions": ["h3", "h3", "INET!"] }));
        // Dedup + sanitize: "INET!" -> "inet", duplicate h3 collapsed.
        assert_eq!(arr.matches("LOAD h3;").count(), 1, "dedup: {}", arr);
        assert!(arr.contains("INSTALL inet; LOAD inet;"), "sanitized: {}", arr);
        // spatial requested via both loadSpatial and loadExtensions -> once only.
        let both = attach_prelude("code.sql", &serde_json::json!({ "loadSpatial": true, "loadExtensions": "spatial,h3" }));
        assert_eq!(both.matches("LOAD spatial;").count(), 1, "spatial once: {}", both);
        assert!(both.contains("LOAD h3;"), "h3 alongside spatial: {}", both);
    }

    #[test]
    fn partial_run_loads_spatial_into_geometry_consumers() {
        // #168: a partial ("Run from here") run executes each stage in its own
        // process. A snk.parquet reading a stored GEOMETRY column must LOAD
        // spatial itself, or DuckDB autoloads it only after binding the column
        // and reconstructs the CRS as WKT2 -> the GeoParquet V1 writer rejects
        // it ("only supports PROJJSON CRS definitions"). So every stage geometry
        // flows into must carry the spatial load, propagated from the source.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{"label":"Shp","componentId":"src.spatial","properties":{"path":"/tmp/cities.shp"}}},
                {"id":"f","position":{"x":0,"y":0},"data":{"label":"Filter","componentId":"xf.filter","properties":{"predicate":"1=1"}}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"Parquet","componentId":"snk.parquet","properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges":[
                {"id":"e1","source":"s","target":"f","data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile_partial(&doc, "k").unwrap();
        for id in ["s", "f", "k"] {
            let sql = &compiled.stages.iter().find(|s| s.node_id == id).unwrap().sql;
            assert!(sql.contains("LOAD spatial"), "stage {id} must load spatial: {sql}");
        }
        // The parquet sink itself has no spatial prelude, so the load can only
        // have come from the downstream-taint pass.
        let sink = &compiled.stages.iter().find(|s| s.node_id == "k").unwrap().sql;
        assert!(sink.contains("INSTALL spatial; LOAD spatial;"), "sink prelude: {sink}");
    }

    #[test]
    fn partial_run_does_not_load_spatial_without_geometry() {
        // Guard against a false positive: a plain CSV -> Parquet partial run must
        // NOT pull in the ~50 MB spatial extension.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{"label":"Csv","componentId":"src.csv","properties":{"path":"/tmp/in.csv","hasHeader":true}}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"Parquet","componentId":"snk.parquet","properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges":[{"id":"e1","source":"s","target":"k","data":{"connectionType":"main"}}]
            }"#,
        );
        let compiled = compile_partial(&doc, "k").unwrap();
        for st in &compiled.stages {
            assert!(!st.sql.contains("spatial"), "non-geo stage {} must not load spatial: {}", st.node_id, st.sql);
        }
    }

    #[test]
    fn cap_preview_query_wraps_per_dialect() {
        // #148: inspect caps a driver fetch with the dialect's own row limit.
        let q = "SELECT * FROM orders";
        assert_eq!(
            cap_preview_query("oracle", q, 100),
            "SELECT * FROM (SELECT * FROM orders) WHERE ROWNUM <= 100"
        );
        assert_eq!(
            cap_preview_query("sqlserver", q, 100),
            "SELECT TOP 100 * FROM (SELECT * FROM orders) q"
        );
        assert_eq!(
            cap_preview_query("teradata", q, 50),
            "SELECT TOP 50 * FROM (SELECT * FROM orders) q"
        );
        assert_eq!(
            cap_preview_query("clickhouse", q, 100),
            "SELECT * FROM (SELECT * FROM orders) LIMIT 100"
        );
        // A trailing semicolon must not break the derived-table wrap.
        assert_eq!(
            cap_preview_query("snowflake", "SELECT 1;", 10),
            "SELECT * FROM (SELECT 1) LIMIT 10"
        );
    }

    #[test]
    fn preview_source_query_prefers_query_then_table() {
        // #148: a user query is capped directly.
        let with_q = preview_source_query(
            "sqlserver",
            &serde_json::json!({ "query": "SELECT a FROM t" }),
            100,
        )
        .unwrap();
        assert_eq!(with_q, "SELECT TOP 100 * FROM (SELECT a FROM t) q");
        // Table-only rebuilds SELECT * FROM <table> with the dialect's quoting.
        let ss = preview_source_query(
            "sqlserver",
            &serde_json::json!({ "tableName": "orders", "schema": "sales" }),
            100,
        )
        .unwrap();
        assert_eq!(ss, "SELECT TOP 100 * FROM (SELECT * FROM [sales].[orders]) q");
        let ora =
            preview_source_query("oracle", &serde_json::json!({ "tableName": "ORDERS" }), 100)
                .unwrap();
        assert_eq!(
            ora,
            "SELECT * FROM (SELECT * FROM \"ORDERS\") WHERE ROWNUM <= 100"
        );
        // Nothing to cap: no query and no tableName.
        assert!(preview_source_query(
            "clickhouse",
            &serde_json::json!({ "endpoint": "http://x" }),
            100
        )
        .is_none());
    }

    #[test]
    fn text_template_renders_row_placeholders() {
        // #147: ${column} placeholders are filled from the row; missing keys and
        // nulls become empty; numbers keep their JSON form (no quotes).
        let row = serde_json::json!({ "location": "us", "temp": 21.5, "note": null });
        let line = crate::connectors::render_text_template(
            "weather,location=${location} temperature=${temp} ${note}",
            &row,
        );
        assert_eq!(line, "weather,location=us temperature=21.5 ");
        // An unknown placeholder yields empty, not the literal placeholder.
        assert_eq!(
            crate::connectors::render_text_template("x=${nope}", &row),
            "x="
        );
    }

    #[test]
    fn pure_sql_runs_verbatim_without_create_wrapper() {
        // #102 follow-up: pureSql=true emits the body verbatim - no `WITH input`
        // and no `CREATE OR REPLACE ... AS` wrapper - and the stage is flagged
        // as producing no output relation.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"o","position":{"x":0,"y":0},"data":{"label":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"m","position":{"x":0,"y":0},"data":{"label":"Pure","componentId":"code.sql","properties":{
                  "pureSql":true,
                  "sql":"CREATE OR REPLACE TABLE final AS SELECT * FROM \"o\""}}}
              ],
              "edges":[
                {"id":"e1","source":"o","target":"m","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let stage = compile(&doc).unwrap().stages.into_iter().find(|s| s.node_id == "m").unwrap();
        assert!(stage.no_output_relation, "pure SQL stage produces no node relation");
        assert!(!stage.sql.contains("CREATE OR REPLACE VIEW \"m\""), "no CTAS wrapper: {}", stage.sql);
        assert!(!stage.sql.contains("WITH input AS"), "no input wrapper: {}", stage.sql);
        assert!(stage.sql.contains("CREATE OR REPLACE TABLE final AS"), "verbatim body: {}", stage.sql);
    }

    #[test]
    fn node_alias_is_carried_and_must_be_unique() {
        // #102: a node alias becomes the stage's alias (exposed as a view by the
        // executor) so raw / pure SQL can reference upstream by a friendly name.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"n_123","position":{"x":0,"y":0},"data":{"label":"Orders","alias":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges":[]
            }"#,
        );
        let stage = compile(&doc).unwrap().stages.into_iter().find(|s| s.node_id == "n_123").unwrap();
        assert_eq!(stage.alias.as_deref(), Some("orders"), "alias carried to stage");
        // Two nodes sharing one alias must fail compilation up front.
        let dup = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"a","position":{"x":0,"y":0},"data":{"label":"A","alias":"shared","componentId":"src.csv","properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"b","position":{"x":0,"y":0},"data":{"label":"B","alias":"shared","componentId":"src.csv","properties":{"path":"/tmp/b.csv","hasHeader":true}}}
              ],
              "edges":[]
            }"#,
        );
        let err = compile(&dup).unwrap_err().to_string();
        assert!(err.contains("shared") && err.to_lowercase().contains("unique"), "dup alias errors: {}", err);
    }

    #[test]
    fn pure_sql_alias_is_used_by_downstream_consumer() {
        // #102 follow-up: a Pure SQL node with a custom SQL name (alias) registers
        // ONLY the relation its body created (the alias), never "<node_id>". A
        // downstream sink must therefore read the alias, not the raw node id -
        // otherwise it fails with "Upstream view '<node_id>' doesn't exist".
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"o","position":{"x":0,"y":0},"data":{"label":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"n_pure","position":{"x":0,"y":0},"data":{"label":"Filter","alias":"t_filter2","componentId":"code.sql","properties":{
                  "pureSql":true,
                  "sql":"create view t_filter2 as select * from \"o\""}}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"CSV out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges":[
                {"id":"e1","source":"o","target":"n_pure","data":{"connectionType":"main"}},
                {"id":"e2","source":"n_pure","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let sink = compile(&doc).unwrap().stages.into_iter().find(|s| s.node_id == "k").unwrap();
        assert!(sink.sql.contains("t_filter2"), "sink FROM must reference the alias: {}", sink.sql);
        assert!(!sink.sql.contains("n_pure"), "sink must not reference the raw pure-SQL node id: {}", sink.sql);
    }

    #[test]
    fn pure_sql_without_alias_still_uses_node_id() {
        // #102: a Pure SQL node with NO alias defaults to the node id, so the
        // user creates "<node_id>" themselves and downstream keeps reading it.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"o","position":{"x":0,"y":0},"data":{"label":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"n_pure","position":{"x":0,"y":0},"data":{"label":"Filter","componentId":"code.sql","properties":{
                  "pureSql":true,
                  "sql":"create or replace view \"n_pure\" as select * from \"o\""}}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"CSV out","componentId":"snk.csv","properties":{"path":"/tmp/out.csv","hasHeader":true}}}
              ],
              "edges":[
                {"id":"e1","source":"o","target":"n_pure","data":{"connectionType":"main"}},
                {"id":"e2","source":"n_pure","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let sink = compile(&doc).unwrap().stages.into_iter().find(|s| s.node_id == "k").unwrap();
        assert!(sink.sql.contains("n_pure"), "no alias -> downstream reads the node id: {}", sink.sql);
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
    fn csv_declared_schema_absent_from_file_falls_back_to_autodetect() {
        // #133: a stale/seeded declaration whose columns are not in the actual
        // file must NOT emit `types=` (which would raise DuckDB's COLUMN_TYPES
        // binder error); instead fall through to plain auto-detect.
        let path = std::env::temp_dir()
            .join(format!("duckle_csv133_absent_{}.csv", std::process::id()));
        std::fs::write(&path, "a,b,c\n1,2,3\n").unwrap();
        let path_s = path.to_string_lossy().replace('\\', "\\\\");
        let p = pipeline_from_json(&format!(
            r#"{{
              "nodes": [
                {{"id":"s1","position":{{"x":0,"y":0}},"data":{{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{{"path":"{path_s}","hasHeader":true}},
                  "schema":[
                    {{"name":"order_id","type":"int64","nullable":false}},
                    {{"name":"amount","type":"string","nullable":true}}
                  ]}}}},
                {{"id":"k1","position":{{"x":0,"y":0}},"data":{{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{{"path":"/tmp/out.csv","hasHeader":true}}}}}}
              ],
              "edges": [
                {{"id":"e1","source":"s1","target":"k1","data":{{"connectionType":"main"}}}}
              ]
            }}"#
        ));
        let compiled = compile(&p).unwrap();
        let src_sql = &compiled.stages[0].sql;
        assert!(
            !src_sql.contains("types = {"),
            "stale declaration should fall back to auto-detect: {}",
            src_sql
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn csv_declared_schema_partially_present_narrows_types() {
        // #133: when SOME declared columns exist in the file, keep only those in
        // `types=` and silently drop the absent ones (no binder error).
        let path = std::env::temp_dir()
            .join(format!("duckle_csv133_partial_{}.csv", std::process::id()));
        std::fs::write(&path, "id,event_date\n1,2026-01-01\n").unwrap();
        let path_s = path.to_string_lossy().replace('\\', "\\\\");
        let p = pipeline_from_json(&format!(
            r#"{{
              "nodes": [
                {{"id":"s1","position":{{"x":0,"y":0}},"data":{{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{{"path":"{path_s}","hasHeader":true}},
                  "schema":[
                    {{"name":"event_date","type":"string","nullable":true}},
                    {{"name":"missing_col","type":"int64","nullable":true}}
                  ]}}}},
                {{"id":"k1","position":{{"x":0,"y":0}},"data":{{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{{"path":"/tmp/out.csv","hasHeader":true}}}}}}
              ],
              "edges": [
                {{"id":"e1","source":"s1","target":"k1","data":{{"connectionType":"main"}}}}
              ]
            }}"#
        ));
        let compiled = compile(&p).unwrap();
        let src_sql = &compiled.stages[0].sql;
        assert!(
            src_sql.contains("'event_date': 'VARCHAR'"),
            "present column kept in types=: {}",
            src_sql
        );
        assert!(
            !src_sql.contains("missing_col"),
            "absent column dropped from types=: {}",
            src_sql
        );
        let _ = std::fs::remove_file(&path);
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
    fn join_multiple_keys_table_is_honored() {
        // #152: keys entered in the multi-column key table (multipleKeys) must
        // drive the join, not be silently dropped for a single key. Uses the
        // consolidated bare `xf.join` id with joinType, and no leftKey/rightKey.
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
                  "label":"Join","componentId":"xf.join",
                  "properties":{"joinType":"inner","leftKey":"","rightKey":"",
                    "multipleKeys":[{"key":"customer_id","value":"customer_id"},{"key":"order_date","value":"order_date"}]}}},
                {"id":"k","position":{"x":0,"y":0},"data":{
                  "label":"CSV out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges": [
                {"id":"e1","source":"l","target":"j","data":{"connectionType":"main"}},
                {"id":"e2","source":"r","target":"j","targetHandle":"lookup","data":{"connectionType":"lookup"}},
                {"id":"e3","source":"j","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let compiled = compile(&p).unwrap();
        let join_sql = compiled.stages.iter().find(|s| s.node_id == "j").unwrap().sql.as_str();
        assert!(
            join_sql.contains("USING (\"customer_id\", \"order_date\")"),
            "multipleKeys not honored (expected both keys in the join): {}",
            join_sql
        );
    }

    #[test]
    fn node_alias_emitted_into_compiled_plan() {
        // #154: the "SQL name" alias must appear in the COMPILED stage SQL (so it
        // shows in Plan view / SQL export and works in every execution path), not
        // only as a view the executor injects at run time.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"n_123","position":{"x":0,"y":0},"data":{"label":"Orders","alias":"orders","componentId":"src.csv","properties":{"path":"/tmp/o.csv","hasHeader":true}}}
              ],
              "edges":[]
            }"#,
        );
        let stage = compile(&doc).unwrap().stages.into_iter().find(|s| s.node_id == "n_123").unwrap();
        assert!(
            stage.sql.contains("CREATE OR REPLACE VIEW \"orders\" AS SELECT * FROM \"n_123\""),
            "alias view missing from compiled plan: {}",
            stage.sql
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
            tags: Vec::new(),
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
            tags: Vec::new(),
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
            tags: Vec::new(),
            name: "name".into(),
            data_type: duckle_metadata::DataType::String,
            nullable: true,
            primary_key: None,
            format: None,
        }];
        assert!(build_csv_reject_sql(&props, Some(&text_cols), false).is_none());
    }

    #[test]
    fn the_geospatial_source_reads_geoparquet_with_read_parquet() {
        // #241: ST_Read is GDAL-backed and the bundled spatial extension has no
        // GDAL Parquet driver, so a .geoparquet path fails with "Could not open
        // GDAL dataset" - the file is fine, that function just cannot open it.
        for path in ["/data/a.geoparquet", "/data/a.parquet", "s3://b/k.PARQUET", "/d/*.parquet"] {
            let sql = super::builders::build_spatial_source(
                &serde_json::json!({ "path": path }),
            );
            assert!(sql.contains("read_parquet"), "{path} -> {sql}");
            assert!(!sql.contains("ST_Read"), "{path} -> {sql}");
        }
    }

    #[test]
    fn every_other_geospatial_format_still_goes_through_st_read() {
        // ST_Read is what reads these, and routing them to read_parquet would
        // break every format the component was built for.
        for path in ["/d/a.geojson", "/d/a.shp", "/d/a.gpkg", "/d/a.kml", "/d/roads.gml"] {
            let sql = super::builders::build_spatial_source(
                &serde_json::json!({ "path": path }),
            );
            assert!(sql.contains("ST_Read"), "{path} -> {sql}");
            assert!(!sql.contains("read_parquet"), "{path} -> {sql}");
        }
    }

    #[test]
    fn parquet_sink_orders_by_hilbert_without_writing_the_bounds_out() {
        // #319. The issue proposes `CROSS JOIN bounds`, which puts the bbox in
        // the exported file as a column; a scalar subquery does not. And
        // ST_Extent_Agg(g)::BOX_2D, also from the issue, is rejected by DuckDB
        // 1.5.4 outright - ST_Extent(ST_Extent_Agg(g)) is the form that works.
        let sql = super::builders::build_parquet_sink(
            &serde_json::json!({ "path": "/lake/out.parquet", "hilbertColumn": "geom" }),
            "v",
        );
        assert!(sql.contains("ORDER BY ST_Hilbert(\"geom\""), "{sql}");
        assert!(sql.contains("ST_Extent(ST_Extent_Agg(\"geom\"))"), "{sql}");
        assert!(!sql.contains("CROSS JOIN"), "the bbox would become a column: {sql}");
        assert!(!sql.contains("BOX_2D"), "that cast does not work on 1.5.4: {sql}");
        // Spatial is loaded by the sink itself: geometry read back from a plain
        // Parquet file does not taint this stage, and ST_Hilbert would then
        // fail at write time, after the whole pipeline had run.
        assert!(sql.starts_with("INSTALL spatial; LOAD spatial;"), "{sql}");
    }

    #[test]
    fn a_parquet_sink_without_the_option_is_byte_for_byte_what_it_was() {
        let sql = super::builders::build_parquet_sink(
            &serde_json::json!({ "path": "/lake/out.parquet" }),
            "v",
        );
        assert_eq!(sql, "COPY (SELECT * FROM \"v\") TO '/lake/out.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')");
    }

    #[test]
    fn an_empty_hilbert_column_is_off_rather_than_an_error() {
        // A field cleared in the form arrives as "", and sorting by a column
        // called "" would fail the write.
        for value in ["", "   "] {
            let sql = super::builders::build_parquet_sink(
                &serde_json::json!({ "path": "/o.parquet", "hilbertColumn": value }),
                "v",
            );
            assert!(!sql.contains("ST_Hilbert"), "{value:?} -> {sql}");
        }
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
            "s3",
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
    fn minio_sink_composes_s3_url_from_bucket_and_key() {
        // #116: snk.minio / snk.r2 / snk.b2 are S3-compatible sinks that take
        // bucket + key (not a full URI); the planner must assemble s3://b/k and
        // route the COPY through the cloud sink builder (the endpoint itself
        // lives in the SECRET, so only the s3:// URL shows here).
        let sql = build_sink_sql(
            "snk.minio",
            &serde_json::json!({
                "bucket": "warehouse", "key": "out/orders.parquet"
            }),
            "v",
            &[],
            None,
        )
        .unwrap();
        assert!(
            sql.contains("'s3://warehouse/out/orders.parquet'"),
            "minio sink must COPY to the composed s3:// url, got: {}",
            sql
        );
        assert!(sql.contains("FORMAT PARQUET"), "extension picks parquet, got: {}", sql);
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
                "s3",
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("conflict column"),
            "blank conflict columns must be rejected, got: {:?}",
            err
        );
    }

    #[test]
    fn pyexpr_replaces_a_column_it_redefines() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        // Redefining an existing column used to emit `SELECT *, expr AS amount`,
        // which yields two columns named amount; the caller's name kept the old
        // value and the computed one landed as amount_1.
        let sql = build_pyexpr(
            &ni,
            &serde_json::json!({"columns": [{"name": "amount", "expr": "amount * 10"}]}),
        )
        .unwrap();
        assert!(
            sql.contains("c NOT IN ('amount')"),
            "the redefined name must be excluded from the star, got: {}",
            sql
        );
        assert!(
            !sql.starts_with("SELECT *,"),
            "the plain appending form is what duplicated the column, got: {}",
            sql
        );
        // Several columns are all excluded, and a quote in a name cannot break out.
        let sql = build_pyexpr(
            &ni,
            &serde_json::json!({"columns": [
                {"name": "a", "expr": "1"},
                {"name": "it's", "expr": "2"},
            ]}),
        )
        .unwrap();
        assert!(sql.contains("c NOT IN ('a', 'it''s')"), "got: {}", sql);
    }

    #[test]
    fn huggingface_source_builds_hf_url() {
        // Bare id + file -> hf://datasets/<repo>/<path>; DuckDB auto-detects format.
        assert_eq!(
            build_huggingface_source(&serde_json::json!({
                "repo": "stanfordnlp/imdb",
                "path": "plain_text/train-00000.parquet"
            })),
            "SELECT * FROM 'hf://datasets/stanfordnlp/imdb/plain_text/train-00000.parquet'"
        );
        // A revision maps to @rev; a stray datasets/ prefix and leading slashes
        // are normalised, and a glob path is preserved.
        assert_eq!(
            build_huggingface_source(&serde_json::json!({
                "repo": "datasets/ibm/duorc",
                "path": "/ParaphraseRC/*.parquet",
                "revision": "~parquet"
            })),
            "SELECT * FROM 'hf://datasets/ibm/duorc@~parquet/ParaphraseRC/*.parquet'"
        );
    }

    #[test]
    fn gdb_source_reads_a_named_layer() {
        // #205: a File Geodatabase reads one feature class via ST_Read(layer=).
        assert_eq!(
            build_gdb_source(&serde_json::json!({ "path": "C:/Data/My.gdb", "layer": "Roads" })),
            "SELECT * FROM ST_Read('C:/Data/My.gdb', layer='Roads')"
        );
        // No layer -> GDAL's default (first) layer, same shape as src.spatial.
        assert_eq!(
            build_gdb_source(&serde_json::json!({ "path": "C:/Data/My.gdb" })),
            "SELECT * FROM ST_Read('C:/Data/My.gdb')"
        );
    }

    #[test]
    fn huggingface_sink_compiles_with_normalised_repo_and_defaults() {
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"hf","position":{"x":0,"y":0},"data":{
                  "label":"HF","componentId":"snk.huggingface",
                  "properties":{"repo":"datasets/acme/widgets","token":"hf_secret"}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"hf","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let stage = compile(&p)
            .unwrap()
            .stages
            .into_iter()
            .find(|s| s.node_id == "hf")
            .unwrap();
        match stage.runtime {
            Some(RuntimeSpec::HuggingFaceSink(spec)) => {
                // the stray datasets/ prefix is normalised off the repo id
                assert_eq!(spec.repo, "acme/widgets");
                assert_eq!(spec.token, "hf_secret");
                // path + revision fall back to the documented defaults
                assert_eq!(spec.path, "data/train.parquet");
                assert_eq!(spec.revision, "main");
                assert_eq!(spec.commit_message, "Add data/train.parquet");
            }
            other => panic!("expected a HuggingFaceSink runtime, got: {:?}", other),
        }
    }

    #[test]
    fn huggingface_sink_requires_a_write_token() {
        // Unlike the read side there is no public write path, so a missing token
        // must fail at compile rather than silently no-op.
        let p = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/o.csv","hasHeader":true}}},
                {"id":"hf","position":{"x":0,"y":0},"data":{
                  "label":"HF","componentId":"snk.huggingface",
                  "properties":{"repo":"acme/widgets"}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"hf","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&p).unwrap_err();
        assert!(
            err.to_string().contains("token"),
            "snk.huggingface without a token must be rejected, got: {}",
            err
        );
    }

    #[test]
    fn driver_sink_upsert_rejects_missing_conflict_columns() {
        // The driver sinks (mongo / oracle / databricks / snowflake / sqlserver)
        // route through upsert_keys_from rather than build_sink_sql, and every
        // one of them reads "no keys" as "plain insert". Asking for upsert
        // without keys therefore appended the whole input again on each run and
        // still reported ok. It has to be refused instead.
        for props in [
            serde_json::json!({"mode": "upsert"}),
            serde_json::json!({"mode": "upsert", "conflictColumns": []}),
            serde_json::json!({"mode": "upsert", "conflictColumns": ["", "  "]}),
        ] {
            let err = upsert_keys_from(&props, "snk.mongodb").unwrap_err();
            assert!(
                err.to_string().contains("conflictColumns"),
                "upsert without usable keys must be rejected, got: {:?}",
                err
            );
        }
    }

    #[test]
    fn conflict_columns_accepts_a_bare_string() {
        // conflictColumns="id" instead of ["id"] used to parse as an empty
        // list, which silently downgraded the upsert to an insert.
        let keys = upsert_keys_from(
            &serde_json::json!({"mode": "upsert", "conflictColumns": "id"}),
            "snk.mongodb",
        )
        .unwrap();
        assert_eq!(keys, vec!["id".to_string()]);

        let keys = upsert_keys_from(
            &serde_json::json!({"mode": "upsert", "conflictColumns": "tenant , id"}),
            "snk.mongodb",
        )
        .unwrap();
        assert_eq!(keys, vec!["tenant".to_string(), "id".to_string()]);
    }

    #[test]
    fn mongo_write_mode_is_checked_against_what_the_sink_honours() {
        // "overwrite" is what snk.duckdb / snk.postgres / snk.csv call this, so
        // it is accepted as an alias rather than punished.
        assert_eq!(
            mongo_write_mode(&serde_json::json!({"mode": "overwrite"}), "snk.mongodb").unwrap(),
            "replace"
        );
        for m in ["insert", "replace", "upsert"] {
            assert_eq!(
                mongo_write_mode(&serde_json::json!({"mode": m}), "snk.mongodb").unwrap(),
                m
            );
        }
        // Unset stays the documented default.
        assert_eq!(
            mongo_write_mode(&serde_json::json!({}), "snk.mongodb").unwrap(),
            "insert"
        );
        // Anything else is a typo, and the sink would have silently appended.
        let err = mongo_write_mode(&serde_json::json!({"mode": "truncate"}), "snk.mongodb")
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown mode"),
            "an unhonoured mode must be rejected, got: {:?}",
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
            tags: Vec::new(),
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

    #[test]
    fn teradata_conn_string_friendly_dsn_and_raw() {
        use serde_json::json;
        // Friendly fields build a DRIVER=/DBCNAME= ODBC string with UID/PWD/
        // DATABASE and a UTF-8 charset; the default driver name is used when
        // none is given.
        let friendly = teradata_conn_string(&json!({
            "host": "tera.example.com",
            "user": "dbc",
            "password": "secret",
            "database": "sales"
        }))
        .unwrap();
        assert!(friendly.contains("DRIVER={Teradata Database ODBC Driver 17.20}"));
        assert!(friendly.contains("DBCNAME=tera.example.com"));
        assert!(friendly.contains("UID=dbc"));
        assert!(friendly.contains("PWD=secret"));
        assert!(friendly.contains("DATABASE=sales"));
        assert!(friendly.contains("CharacterSet=UTF8"));

        // A DSN takes the place of DRIVER/DBCNAME but still layers credentials.
        let dsn = teradata_conn_string(&json!({"dsn": "TeradataProd", "user": "dbc"})).unwrap();
        assert!(dsn.contains("DSN=TeradataProd"));
        assert!(dsn.contains("UID=dbc"));
        assert!(!dsn.contains("DRIVER="));

        // An explicit connectionString wins verbatim.
        let raw = teradata_conn_string(&json!({
            "connectionString": "DSN=Custom;UID=x;PWD=y",
            "host": "ignored"
        }))
        .unwrap();
        assert_eq!(raw, "DSN=Custom;UID=x;PWD=y");

        // No host / dsn / connectionString is a config error.
        assert!(teradata_conn_string(&json!({"user": "dbc"})).is_err());
    }

    #[test]
    fn quack_refuses_write_modes_duckdb_cannot_execute() {
        use serde_json::json;
        // A Quack-attached table is a streaming remote scan, not a base table.
        // Verified on the pinned DuckDB 1.5.4 against a live quack_serve:
        //   TRUNCATE / DELETE -> "Can only delete from base table"
        //   UPDATE            -> "Can only update base table"
        //   MERGE INTO        -> "Can only merge into base tables!"
        // These used to compile fine and blow up mid-run with a binder error
        // that named nothing the user had written, so they are refused here.
        let props = json!({
            "tableName": "t",
            "conflictColumns": ["id"],
            "mode": "upsert"
        });
        for mode in ["truncate", "upsert", "merge"] {
            let mut p = props.clone();
            p["mode"] = json!(mode);
            let err = build_relational_sink("snk.quack", &p, "up", &["id".into()])
                .expect_err(&format!("snk.quack must refuse '{}'", mode));
            let msg = err.to_string();
            assert!(
                msg.contains("not supported over the Quack protocol"),
                "{} should name Quack as the reason, got: {}",
                mode,
                msg
            );
            assert!(
                msg.contains("append") && msg.contains("overwrite"),
                "{} should name the modes that DO work, got: {}",
                mode,
                msg
            );
        }

        // The two that genuinely work must keep working.
        for mode in ["append", "overwrite"] {
            let mut p = props.clone();
            p["mode"] = json!(mode);
            assert!(
                build_relational_sink("snk.quack", &p, "up", &["id".into()]).is_ok(),
                "snk.quack must still allow '{}'",
                mode
            );
        }

        // MERGE is advertised through supports_merge; Quack must not be in it,
        // while the real DuckDB-native targets stay.
        assert!(!supports_merge("snk.quack"));
        for ok in ["snk.duckdb", "snk.sqlite", "snk.motherduck", "snk.ducklake"] {
            assert!(supports_merge(ok), "{} should still support MERGE", ok);
        }
    }

    #[test]
    fn a_warehouse_upsert_needs_no_second_write_mode_beside_it() {
        // An upsert is asked for by naming the key columns and setting mode to "upsert",
        // which is how it is documented. The truncate check reads `writeMode` and falls
        // back to `mode`, so it used to find "upsert" there and refuse it as an unknown
        // write mode - and the way through was to also set writeMode to "append", which
        // says nothing that mode has not already said.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{"label":"in","componentId":"src.csv","properties":{"path":"/tmp/in.csv"}}},
                {"id":"k1","position":{"x":1,"y":0},"data":{"label":"out","componentId":"snk.snowflake","properties":{
                  "account":"a","database":"d","schema":"s","warehouse":"w","username":"u","pat":"p",
                  "tableName":"T","mode":"upsert","conflictColumns":["id"]}}}
              ],
              "edges": [{"id":"e1","source":"s1","target":"k1","data":{"connectionType":"main"}}]
            }"#,
        );
        compile(&doc).expect("an upsert spelled the documented way compiles");
    }

    #[test]
    fn a_mapper_written_as_name_to_expression_actually_maps() {
        // The Map form writes its outputs either as a list of pairs or as one object from
        // output name to expression. Only the list was read, and a mapper whose outputs
        // did not parse falls back to passing its input straight through - so the object
        // form compiled to SELECT *, silently, and every expression in it was dropped
        // while the node still reported rows and still looked like it had run.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{"label":"in","componentId":"src.csv","properties":{"path":"/tmp/in.csv","hasHeader":true}}},
                {"id":"m","position":{"x":1,"y":0},"data":{"label":"Map","componentId":"xf.map","properties":{
                  "expressions": {"TOTAL": "AMT * 2", "NAME": "upper(N)"}}}},
                {"id":"k","position":{"x":2,"y":0},"data":{"label":"out","componentId":"snk.parquet","properties":{"path":"/tmp/o.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"m","data":{"connectionType":"main"}},
                {"id":"e2","source":"m","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let sql = map_sql(&doc);
        assert!(sql.contains("AMT * 2"), "the expression is applied: {sql}");
        assert!(sql.contains("upper(N)"), "all of them: {sql}");
        assert!(
            !sql.trim_start().to_uppercase().starts_with("SELECT * FROM"),
            "not a passthrough: {sql}"
        );
    }

    #[test]
    fn a_mapper_output_never_stands_in_for_the_input_column_it_is_named_after() {
        // A mapper's expressions read its INPUT. An output named after a column it also
        // reads used to shadow it: SQL resolves a name to a sibling output when the two
        // collide, so the expression saw the value being computed beside it rather than
        // the one that came in - and where the collision ran the other way it refused to
        // compile at all, on a job that was correct.
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{"label":"in","componentId":"src.csv","properties":{"path":"/tmp/in.csv","hasHeader":true}}},
                {"id":"m","position":{"x":1,"y":0},"data":{"label":"Map","componentId":"xf.map","properties":{
                  "expressions": {"BATCH_ID": "coalesce(BATCH_ID, 0)", "NEXT_ID": "BATCH_ID + 1"}}}},
                {"id":"k","position":{"x":2,"y":0},"data":{"label":"out","componentId":"snk.parquet","properties":{"path":"/tmp/o.parquet"}}}
              ],
              "edges": [
                {"id":"e1","source":"s","target":"m","data":{"connectionType":"main"}},
                {"id":"e2","source":"m","target":"k","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let sql = map_sql(&doc);
        // Both expressions read the input, so neither can be resolved against the other:
        // the names they are given are applied outside the scope that computes them.
        assert!(
            !sql.contains("AS \"BATCH_ID\" ,") && sql.matches("FROM").count() >= 2,
            "the outputs are named outside the query that reads the input: {sql}"
        );
        assert!(sql.contains("coalesce(BATCH_ID, 0)"), "{sql}");
        assert!(sql.contains("BATCH_ID + 1"), "{sql}");
    }
    // -----------------------------------------------------------------------
    // #274 - a publish group must REFUSE rather than shrink. Each of these is a
    // way a group of N quietly became a group of N-1 while still claiming the
    // tables publish together, which is worse than not offering the guarantee.
    // -----------------------------------------------------------------------

    fn grouped_lake_doc(extra_w2: &str, w2_path: &str) -> PipelineDoc {
        pipeline_from_json(&format!(
            r#"{{
              "nodes": [
                {{"id":"s1","position":{{"x":0,"y":0}},"data":{{"label":"a","componentId":"code.sql","properties":{{"sql":"SELECT 1 AS id"}}}}}},
                {{"id":"s2","position":{{"x":0,"y":0}},"data":{{"label":"b","componentId":"code.sql","properties":{{"sql":"SELECT 2 AS id"}}}}}},
                {{"id":"w1","position":{{"x":0,"y":0}},"data":{{"label":"dim","componentId":"snk.ducklake","properties":{{
                  "path":"/tmp/lake.duckdb","schemaName":"main","tableName":"dim","mode":"overwrite","publishGroup":"nightly"}}}}}},
                {{"id":"w2","position":{{"x":0,"y":0}},"data":{{"label":"fact",{extra}"componentId":"snk.ducklake","properties":{{
                  "path":"{path}","schemaName":"main","tableName":"fact","mode":"overwrite","publishGroup":"nightly"}}}}}}
              ],
              "edges":[
                {{"id":"e1","source":"s1","target":"w1","data":{{"connectionType":"main"}}}},
                {{"id":"e2","source":"s2","target":"w2","data":{{"connectionType":"main"}}}}
              ]
            }}"#,
            extra = extra_w2,
            path = w2_path
        ))
    }

    /// A group of two whose second member is disabled is a group of one. The
    /// planner has always honoured `disabled` silently, which here would mean
    /// publishing one table while the pipeline says two publish together.
    #[test]
    fn publish_group_refuses_a_disabled_member() {
        let doc = grouped_lake_doc("\"disabled\":true,", "/tmp/lake.duckdb");
        let err = compile(&doc).unwrap_err().to_string();
        assert!(
            err.contains("nightly") && err.contains("fact") && err.contains("disabled"),
            "must name the group, the member and why: {}",
            err
        );
    }

    /// Two catalogs cannot be committed together. One transaction reaches one
    /// database - there is no ordering of two commits that makes both land or
    /// neither, so offering the option at all would be a lie.
    #[test]
    fn publish_group_refuses_two_different_lakes() {
        let doc = grouped_lake_doc("", "/tmp/other-lake.duckdb");
        let err = compile(&doc).unwrap_err().to_string();
        assert!(
            err.contains("nightly") && err.contains("other-lake"),
            "must name the group and the second catalog: {}",
            err
        );
    }

    /// "Run from here" walks back along data edges only, so a partial run can
    /// contain one member of a group and not the other. Publishing the half it
    /// happens to reach is exactly the split the group exists to prevent.
    #[test]
    fn publish_group_refuses_a_partial_run_that_splits_it() {
        let doc = grouped_lake_doc("", "/tmp/lake.duckdb");
        // Running from w1 pulls in s1 and w1 - w2 is on a separate branch and
        // is left out entirely.
        let err = compile_partial(&doc, "w1").unwrap_err().to_string();
        assert!(
            err.contains("nightly") && err.contains("fact") && err.contains("not part of this run"),
            "must say the run does not contain the whole group: {}",
            err
        );
    }

    /// A member inside a ctl.parallelize branch runs as its own sub-pipeline,
    /// in its own process, and cannot join this run's transaction. The planner
    /// removes such nodes from the main plan, so the group would have committed
    /// without it and reported success.
    #[test]
    fn publish_group_refuses_a_member_inside_parallelize() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s1","position":{"x":0,"y":0},"data":{"label":"a","componentId":"code.sql","properties":{"sql":"SELECT 1 AS id"}}},
                {"id":"p","position":{"x":0,"y":0},"data":{"label":"fan","componentId":"ctl.parallelize","properties":{}}},
                {"id":"w1","position":{"x":0,"y":0},"data":{"label":"dim","componentId":"snk.ducklake","properties":{
                  "path":"/tmp/lake.duckdb","schemaName":"main","tableName":"dim","mode":"overwrite","publishGroup":"nightly"}}},
                {"id":"w2","position":{"x":0,"y":0},"data":{"label":"fact","componentId":"snk.ducklake","properties":{
                  "path":"/tmp/lake.duckdb","schemaName":"main","tableName":"fact","mode":"overwrite","publishGroup":"nightly"}}}
              ],
              "edges":[
                {"id":"e1","source":"s1","target":"p","data":{"connectionType":"main"}},
                {"id":"e2","source":"p","target":"w1","data":{"connectionType":"main"}},
                {"id":"e3","source":"p","target":"w2","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let err = compile(&doc).unwrap_err().to_string();
        assert!(
            err.contains("nightly") && err.contains("parallelize"),
            "must name the group and parallelize: {}",
            err
        );
    }


    /// #305: a durable output read INSTEAD of running the node that made it.
    #[test]
    fn a_bound_output_replaces_the_stage_and_keeps_its_name() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"f","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"amt > 1"}}},
                {"id":"out","position":{"x":0,"y":0},"data":{
                  "label":"Write","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges":[
                {"id":"e1","source":"s","target":"f","data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"out","data":{"connectionType":"main"}}
              ]
            }"#,
        );

        let bind = |pairs: &[(&str, &str)]| {
            let mut stages = compile(&doc).unwrap().stages;
            let map: std::collections::BTreeMap<String, String> =
                pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
            crate::plan::apply_output_bindings(&mut stages, &map);
            stages
        };

        // Nothing bound: the plan is exactly what it was.
        let before = compile(&doc).unwrap().stages;
        let plain = bind(&[]);
        assert_eq!(
            plain.iter().map(|s| s.sql.clone()).collect::<Vec<_>>(),
            before.iter().map(|s| s.sql.clone()).collect::<Vec<_>>(),
            "an ordinary run must be untouched"
        );

        // Bound: the source reads a parquet somebody else made, under its own
        // relation name, so nothing downstream can tell.
        let bound = bind(&[("s", "/ws/cache/p/s/K.parquet")]);
        let s = bound.iter().find(|x| x.node_id == "s").unwrap();
        assert!(
            s.sql.contains("CREATE OR REPLACE VIEW \"s\" AS SELECT * FROM read_parquet("),
            "{}",
            s.sql
        );
        assert!(s.sql.contains("/ws/cache/p/s/K.parquet"), "{}", s.sql);
        assert!(!s.sql.contains("read_csv"), "the source is not read again: {}", s.sql);
        assert!(s.runtime.is_none());
        assert!(!s.attach_view);

        // Downstream is not edited at all - it still reads "s" by name.
        let f = bound.iter().find(|x| x.node_id == "f").unwrap();
        let f_before = before.iter().find(|x| x.node_id == "f").unwrap();
        assert_eq!(f.sql, f_before.sql, "no consumer is rewritten");
    }

    /// A sink's effect is outside the run. Reading a file back neither repeats
    /// nor undoes it, so binding one would silently skip the write it exists to
    /// do.
    #[test]
    fn a_sink_is_never_bound() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"out","position":{"x":0,"y":0},"data":{
                  "label":"Write","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"out","data":{"connectionType":"main"}}]
            }"#,
        );
        let before = compile(&doc).unwrap().stages;
        let mut stages = compile(&doc).unwrap().stages;
        crate::plan::apply_output_bindings(
            &mut stages,
            &[("out".to_string(), "/ws/anything.parquet".to_string())].into_iter().collect(),
        );
        let out = stages.iter().find(|x| x.node_id == "out").unwrap();
        let out_before = before.iter().find(|x| x.node_id == "out").unwrap();
        assert_eq!(out.sql, out_before.sql, "the write must still happen");
    }

    #[test]
    fn a_quote_in_a_bound_path_cannot_break_out_of_the_literal() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [{"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}}],
              "edges":[]
            }"#,
        );
        let mut stages = compile(&doc).unwrap().stages;
        crate::plan::apply_output_bindings(
            &mut stages,
            &[("s".to_string(), "/ws/it's.parquet".to_string())].into_iter().collect(),
        );
        assert!(stages[0].sql.contains("it''s.parquet"), "{}", stages[0].sql);
    }

    /// The two traps binding has to clear, on a stage that actually has them.
    ///
    /// `src.csv` has neither, so asserting against a compiled CSV stage proves
    /// nothing - it was already true. This drives the pass directly instead.
    #[test]
    fn binding_clears_the_runtime_hook_and_the_attach() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [{"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}}],
              "edges":[]
            }"#,
        );
        let mut stages = compile(&doc).unwrap().stages;
        // A stage that would take the runtime branch and ignore its SQL, and
        // would ATTACH a database this run never opened.
        stages[0].runtime = Some(crate::plan::RuntimeSpec::InstallFallback("/tmp/x".into()));
        stages[0].attach_view = true;

        crate::plan::apply_output_bindings(
            &mut stages,
            &[("s".to_string(), "/ws/s.parquet".to_string())].into_iter().collect(),
        );
        assert!(
            stages[0].runtime.is_none(),
            "a runtime hook left in place makes the executor ignore the bound SQL entirely"
        );
        assert!(
            !stages[0].attach_view,
            "it must not ATTACH a source database this run never opened"
        );
        assert!(stages[0].sql.contains("read_parquet("), "{}", stages[0].sql);
    }

    /// #305: a skipped node is not staged at all.
    ///
    /// Without this the plan says "skip" and the stage runs anyway - which is
    /// exactly what happened the first time it was run end to end: the dry run
    /// reported `skip download` and the retry still read the CSV.
    #[test]
    fn a_skipped_node_is_not_staged() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"f","position":{"x":0,"y":0},"data":{
                  "label":"Filter","componentId":"xf.filter",
                  "properties":{"predicate":"amt > 1"}}},
                {"id":"out","position":{"x":0,"y":0},"data":{
                  "label":"Write","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges":[
                {"id":"e1","source":"s","target":"f","data":{"connectionType":"main"}},
                {"id":"e2","source":"f","target":"out","data":{"connectionType":"main"}}
              ]
            }"#,
        );
        let mut stages = compile(&doc).unwrap().stages;
        let before = stages.len();
        crate::plan::drop_stages(&mut stages, &["s".to_string()].into_iter().collect());
        assert_eq!(stages.len(), before - 1, "the skipped stage is gone");
        assert!(!stages.iter().any(|x| x.node_id == "s"));
        assert!(stages.iter().any(|x| x.node_id == "f"), "its consumer stays");
    }

    /// A sink is an end, so the backward walk always reaches it. Dropping one
    /// would silently skip the write it exists to do.
    #[test]
    fn a_sink_is_never_dropped() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"out","position":{"x":0,"y":0},"data":{
                  "label":"Write","componentId":"snk.parquet",
                  "properties":{"path":"/tmp/out.parquet"}}}
              ],
              "edges":[{"id":"e","source":"s","target":"out","data":{"connectionType":"main"}}]
            }"#,
        );
        let mut stages = compile(&doc).unwrap().stages;
        crate::plan::drop_stages(&mut stages, &["out".to_string()].into_iter().collect());
        assert!(stages.iter().any(|x| x.node_id == "out"), "the write must still happen");
    }

    /// A port the GUI wrote as a NUMBER must reach the connection.
    ///
    /// The integer field writes a JSON number and the port was read with
    /// `string_prop`, which is `as_str()` only - so every port typed into the
    /// panel parsed to None and fell through to the 31337 default. The user
    /// set 5000, Duckle dialled 31337, and nothing said why.
    #[test]
    fn a_port_typed_in_the_panel_is_not_discarded() {
        use crate::plan::builders::port_prop;
        // What the GUI actually stores.
        let gui = serde_json::json!({ "port": 5000 });
        assert_eq!(port_prop(&gui, "port"), Some(5000), "a JSON number is what the panel writes");

        // What a hand-written pipeline file and older saves spell.
        let text = serde_json::json!({ "port": "5000" });
        assert_eq!(port_prop(&text, "port"), Some(5000), "text must keep working");
        assert_eq!(port_prop(&serde_json::json!({ "port": " 5000 " }), "port"), Some(5000));

        // Absent and nonsense both fall through to the caller's default.
        assert_eq!(port_prop(&serde_json::json!({}), "port"), None);
        assert_eq!(port_prop(&serde_json::json!({ "port": "abc" }), "port"), None);
        // Out of range is not a port, and must not wrap to one.
        assert_eq!(port_prop(&serde_json::json!({ "port": 70000 }), "port"), None);
    }

    /// The GUI's "- column -" option writes column:"" and the builder emits
    /// COUNT("") - a quoted empty identifier, which is not valid SQL.
    #[test]
    fn a_group_by_count_with_no_column_is_not_broken_sql() {
        let doc = pipeline_from_json(
            r#"{
              "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{
                  "label":"CSV","componentId":"src.csv",
                  "properties":{"path":"/tmp/a.csv","hasHeader":true}}},
                {"id":"g","position":{"x":0,"y":0},"data":{
                  "label":"Group","componentId":"xf.groupby",
                  "properties":{"groupKeys":["k"],
                    "aggregations":[{"column":"","func":"count","output":"n"}]}}}
              ],
              "edges":[{"id":"e","source":"s","target":"g","data":{"connectionType":"main"}}]
            }"#,
        );
        let sql = compile(&doc).unwrap().stages.into_iter()
            .find(|s| s.node_id == "g").unwrap().sql;
        assert!(
            !sql.contains("count(\"\")") && !sql.contains("COUNT(\"\")"),
            "an empty column must not become a quoted empty identifier: {sql}"
        );
        assert!(sql.contains("COUNT(*)"), "it means COUNT(*), which is what the option says: {sql}");
    }

/// The PII contract's escape hatch, in the exact shape the Properties Panel
/// writes. The gate at plan/mod.rs:1102 has always told the operator to "set
/// contracts.allowPii=true" and, until the Advanced tab grew the field, nothing
/// in the interface could write it - so the way out of the refusal was to edit
/// the pipeline file by hand. Neither half had a test.
fn pii_doc(sink_props: &str) -> PipelineDoc {
    pipeline_from_json(&format!(
        r#"{{
          "nodes": [
            {{"id":"s","position":{{"x":0,"y":0}},"data":{{"label":"people","componentId":"src.csv",
              "properties":{{"path":"/tmp/people.csv","hasHeader":true,"contracts":{{"pii":["email"]}}}},
              "schema":[{{"name":"id","type":"int64"}},{{"name":"email","type":"string"}}]}}}},
            {{"id":"k","position":{{"x":0,"y":0}},"data":{{"label":"out","componentId":"snk.csv",
              "properties":{{"path":"/tmp/out.csv"{}}}}}}}
          ],
          "edges":[{{"id":"e1","source":"s","target":"k","data":{{"connectionType":"main"}}}}]
        }}"#,
        sink_props
    ))
}

#[test]
fn a_pii_column_reaching_a_sink_is_refused() {
    let err = compile(&pii_doc("")).expect_err("a tagged column reached a sink unmasked");
    let msg = err.to_string();
    assert!(msg.contains("tagged PII"), "the gate must be what refused it: {msg}");
    assert!(msg.contains("email"), "and it must name the column: {msg}");
}

#[test]
fn the_panels_allow_pii_shape_lifts_the_refusal() {
    // Nested under `contracts`, which is what setProperty('contracts.allowPii')
    // writes - a flat "contracts.allowPii" key would not be read at all.
    let plan = compile(&pii_doc(r#","contracts":{"allowPii":true}"#))
        .expect("the documented escape hatch has to actually compile");
    assert!(
        plan.stages.iter().any(|s| s.node_id == "k"),
        "and the sink still has to be planned"
    );
}

#[test]
fn a_flat_dotted_key_does_not_lift_the_refusal() {
    // The mistake the panel's dotted-key setter exists to prevent: writing the
    // literal property "contracts.allowPii" looks right in a pipeline file and
    // is read by nothing.
    let err = compile(&pii_doc(r#","contracts.allowPii":true"#))
        .expect_err("a flat key must not be mistaken for the contract");
    assert!(err.to_string().contains("tagged PII"), "{err}");
}

/// The Parquet half of the same delegation. The panel's S3 sink now offers
/// compression / level / version / row-group size and a CSV header toggle
/// because build_cloud_sink hands the props to the local builders unchanged;
/// only the CSV delimiter and null string had a test saying so.
#[test]
fn cloud_parquet_sink_honors_the_write_options_the_panel_offers() {
    let sql = build_cloud_sink(
        "s3",
        &serde_json::json!({
            "path": "s3://b/out.parquet",
            "compression": "zstd",
            "compressionLevel": 9,
            "parquetVersion": "v2",
            "rowGroupSize": 1000000
        }),
        "v",
    )
    .unwrap();
    assert!(sql.contains("COMPRESSION 'zstd'"), "{sql}");
    assert!(sql.contains("COMPRESSION_LEVEL 9"), "{sql}");
    assert!(sql.contains("PARQUET_VERSION V2"), "{sql}");
    assert!(sql.contains("ROW_GROUP_SIZE 1000000"), "{sql}");
}

#[test]
fn a_cloud_csv_sink_can_be_written_without_a_header() {
    let sql = build_cloud_sink(
        "s3",
        &serde_json::json!({"path": "s3://b/out.csv", "writeHeader": false}),
        "v",
    )
    .unwrap();
    assert!(sql.contains("HEADER false"), "the toggle has to reach the COPY: {sql}");
}

/// build_sort had no direct unit test. Everything asserted about it was
/// indirect - one end-to-end sortColumn run and a compile-only fixture that
/// never looks at the SQL - so every disagreement between its two branches
/// went unnoticed. These pin the whole surface before it is changed.
#[cfg(test)]
mod sorting {
    use super::*;

    fn sort_sql(props: serde_json::Value) -> String {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["up".into()]);
        build_sort(&ni, &props).expect("build_sort")
    }

    fn order_by(props: serde_json::Value) -> String {
        let sql = sort_sql(props);
        match sql.split_once("ORDER BY ") {
            Some((_, keys)) => keys.to_string(),
            None => panic!("no ORDER BY in: {sql}"),
        }
    }

    /// The two branches parsed `direction` differently: the orderBy element
    /// branch trims and lowercases, the single-column branch matched "desc"
    /// exactly. So "DESC" - which a hand-written pipeline, an import or an SDK
    /// will write - sorted ASCENDING, with no error and no warning.
    #[test]
    fn an_uppercase_direction_still_means_descending() {
        for spelling in ["DESC", "Desc", " desc", "desc "] {
            let keys = order_by(serde_json::json!({
                "sortColumn": "amount", "direction": spelling
            }));
            assert!(
                keys.starts_with("\"amount\" DESC"),
                "direction {spelling:?} must sort descending, got: {keys}"
            );
        }
    }

    /// columns_list accepts a bare string as a one-element list precisely
    /// because writing "id" instead of ["id"] is the obvious mistake. orderBy
    /// was the one reader that did not, and the node degraded to an unordered
    /// SELECT * in silence.
    #[test]
    fn a_bare_string_order_by_still_sorts() {
        assert_eq!(order_by(serde_json::json!({ "orderBy": "amount" })), "\"amount\" ASC");
    }

    /// The column name went into the SQL verbatim, so any name needing quotes
    /// produced a parse error rather than a sort.
    #[test]
    fn a_column_name_that_needs_quoting_is_quoted() {
        assert_eq!(order_by(serde_json::json!({ "orderBy": ["my col"] })), "\"my col\" ASC");
    }

    /// The documented workaround for multi-column sort, which the editor could
    /// not express: a trailing direction inside the string. It has to keep
    /// working, which is why the column cannot simply be quoted whole.
    #[test]
    fn a_trailing_direction_inside_the_string_is_understood() {
        assert_eq!(
            order_by(serde_json::json!({ "orderBy": ["amount DESC", "name asc"] })),
            "\"amount\" DESC, \"name\" ASC"
        );
    }

    /// nullsLast was read only in the single-column fallback, so a multi-column
    /// sort silently lost it.
    #[test]
    fn null_ordering_is_reachable_per_key() {
        assert_eq!(
            order_by(serde_json::json!({
                "orderBy": [
                    { "column": "a", "direction": "desc", "nullsLast": false },
                    { "column": "b", "nullsLast": true }
                ]
            })),
            "\"a\" DESC NULLS FIRST, \"b\" ASC NULLS LAST"
        );
    }

    /// A key that says nothing about nulls must emit no NULLS clause, or every
    /// pipeline already using orderBy changes its output ordering on upgrade.
    #[test]
    fn a_key_that_says_nothing_about_nulls_emits_no_clause() {
        assert_eq!(
            order_by(serde_json::json!({
                "orderBy": [{ "column": "a" }, { "column": "b", "direction": "desc" }]
            })),
            "\"a\" ASC, \"b\" DESC"
        );
    }

    /// The single-column form's existing semantics, unchanged.
    #[test]
    fn the_single_column_form_keeps_its_null_default() {
        assert_eq!(order_by(serde_json::json!({ "sortColumn": "c" })), "\"c\" ASC NULLS LAST");
        assert_eq!(
            order_by(serde_json::json!({ "sortColumn": "c", "nullsLast": false })),
            "\"c\" ASC NULLS FIRST"
        );
    }

    /// orderBy wins when both are present, and an unconfigured node is still a
    /// pass-through rather than an error.
    #[test]
    fn order_by_wins_and_an_empty_sort_is_a_pass_through() {
        assert_eq!(
            order_by(serde_json::json!({ "orderBy": ["a"], "sortColumn": "b" })),
            "\"a\" ASC"
        );
        assert_eq!(sort_sql(serde_json::json!({})), "SELECT * FROM \"up\"");
    }
}

/// The validator and build_sort have to agree about what a sort key IS, or one
/// half rejects a pipeline the other half compiles correctly.
#[cfg(test)]
mod sort_validation {
    use super::*;

    fn doc(sort_props: serde_json::Value) -> PipelineDoc {
        serde_json::from_value(serde_json::json!({
            "nodes": [
                {"id":"s","position":{"x":0,"y":0},"data":{"label":"in","componentId":"src.csv",
                  "properties":{"path":"/tmp/in.csv","hasHeader":true},
                  "schema":[{"name":"amount","type":"int64"},{"name":"name","type":"string"}]}},
                {"id":"t","position":{"x":0,"y":0},"data":{"label":"Sort","componentId":"xf.sort",
                  "properties": sort_props}},
                {"id":"k","position":{"x":0,"y":0},"data":{"label":"out","componentId":"snk.csv",
                  "properties":{"path":"/tmp/out.csv"}}}
            ],
            "edges":[
                {"id":"e1","source":"s","target":"t","data":{"connectionType":"main"}},
                {"id":"e2","source":"t","target":"k","data":{"connectionType":"main"}}
            ]
        }))
        .unwrap()
    }

    /// The documented multi-column workaround. build_sort has always understood
    /// it; the validator checked the whole string as a column name and refused
    /// the pipeline before build_sort ever saw it.
    #[test]
    fn a_trailing_direction_is_not_read_as_part_of_the_column_name() {
        let plan = compile(&doc(serde_json::json!({ "orderBy": ["amount DESC", "name"] })))
            .expect("the string form has to validate as well as compile");
        let sql = plan.stages.iter().find(|s| s.node_id == "t").expect("sort stage").sql.clone();
        assert!(sql.contains("ORDER BY \"amount\" DESC, \"name\" ASC"), "{sql}");
    }

    /// A real typo still has to be caught, or the check is worthless.
    #[test]
    fn a_misspelled_sort_column_is_still_refused() {
        let err = compile(&doc(serde_json::json!({ "orderBy": ["amonut DESC"] })))
            .expect_err("a column that does not exist must not compile");
        assert!(err.to_string().contains("amonut"), "it names the typo: {err}");
    }

    /// The legacy single-key form was never validated at all, so a typo in the
    /// editor's own Column field survived planning and failed inside DuckDB
    /// with a message about SQL rather than about the field.
    #[test]
    fn a_misspelled_single_sort_column_is_refused_too() {
        let err = compile(&doc(serde_json::json!({ "sortColumn": "amonut" })))
            .expect_err("the single-column form must be checked like every other");
        assert!(err.to_string().contains("amonut"), "{err}");
    }

    /// The bare-string form compiles, so it must validate.
    #[test]
    fn a_bare_string_order_by_is_validated_not_ignored() {
        compile(&doc(serde_json::json!({ "orderBy": "amount" }))).expect("valid");
        let err = compile(&doc(serde_json::json!({ "orderBy": "amonut" })))
            .expect_err("a typo in the bare-string form must be caught");
        assert!(err.to_string().contains("amonut"), "{err}");
    }
}

/// A GraphQL source has to be buildable from the form that draws it.
///
/// The arm requires `query` and reads `variables`, and synthApiSource declared
/// neither - it draws the REST `body` textarea, which this arm ignores because
/// it builds the body itself from query + variables. So every src.graphql,
/// src.linear and src.monday node the editor could produce failed at plan time
/// on "query required", and the only way to make one was to write the pipeline
/// file by hand.
#[cfg(test)]
mod graphql_source {
    use super::*;

    fn node(props: serde_json::Value) -> PipelineDoc {
        serde_json::from_value(serde_json::json!({
            "nodes": [
                {"id":"g","position":{"x":0,"y":0},
                 "data":{"label":"GQL","componentId":"src.graphql","properties": props}},
                {"id":"k","position":{"x":0,"y":0},
                 "data":{"label":"out","componentId":"snk.csv","properties":{"path":"/tmp/o.csv"}}}
            ],
            "edges":[{"id":"e1","source":"g","target":"k","data":{"connectionType":"main"}}]
        }))
        .unwrap()
    }

    /// The form's own keys, and nothing a hand-written file would add.
    #[test]
    fn the_fields_the_form_offers_are_enough_to_compile() {
        let plan = node(serde_json::json!({
            "url": "https://api.example.invalid/graphql",
            "query": "query { issues { nodes { id updatedAt } } }",
            "variables": "{\"first\": 50}",
            "responsePath": "/data/issues/nodes",
        }));
        let plan = compile(&plan).expect("a node built from the form must compile");
        assert!(plan.stages.iter().any(|s| s.node_id == "g"), "the source has to be planned");
    }

    /// And the query has to reach the request body, not just be accepted.
    #[test]
    fn the_query_and_variables_become_the_request_body() {
        let d = node(serde_json::json!({
            "url": "https://api.example.invalid/graphql",
            "query": "query Q { things { id } }",
            "variables": "{\"first\": 50}",
        }));
        let plan = compile(&d).expect("compiles");
        let stage = plan.stages.iter().find(|s| s.node_id == "g").expect("stage");
        let spec = stage.runtime.as_ref().expect("a GraphQL source runs a request");
        let body = match spec {
            crate::plan::RuntimeSpec::RestSource(r) => r.body.clone().unwrap_or_default(),
            other => panic!("expected a REST-backed source, got {other:?}"),
        };
        assert!(body.contains("query Q { things { id } }"), "the query is the body: {body}");
        assert!(body.contains("\"first\":50"), "and the variables are parsed JSON: {body}");
    }

    /// The failure this closes, kept so the message stays actionable if the
    /// form ever drops the field again.
    #[test]
    fn a_graphql_node_with_no_query_says_so() {
        let err = compile(&node(serde_json::json!({ "url": "https://x.invalid/graphql" })))
            .expect_err("a GraphQL request without a query is not a request");
        assert!(err.to_string().contains("query required"), "{err}");
    }
}

/// The join family declares a `reject` output port and nothing could fill it.
///
/// build_reject_sql is a per-component match and the joins were simply not in
/// it, so wiring the port failed the whole run with
/// `Table with name <node>__reject does not exist` - an internal name, for a
/// port the editor offers.
#[cfg(test)]
mod join_rejects {
    use super::*;

    fn reject(component: &str, props: serde_json::Value) -> Option<String> {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["l".into()]);
        ni.ports.insert("lookup".into(), vec!["r".into()]);
        build_reject_sql(component, &props, &ni, None).expect("reject sql")
    }

    /// An inner join DROPS the unmatched rows, which is exactly why someone
    /// wires the port: to find out which ones went.
    #[test]
    fn an_inner_join_rejects_the_rows_that_matched_nothing() {
        let sql = reject(
            "xf.join.inner",
            serde_json::json!({ "leftKey": "cust", "rightKey": "cust" }),
        )
        .expect("an inner join has unmatched rows to reject");
        assert_eq!(
            sql,
            "SELECT * FROM \"l\" m WHERE NOT EXISTS (SELECT 1 FROM \"r\" r WHERE m.\"cust\" = r.\"cust\")"
        );
    }

    /// Same question for a lookup and a semi join, which read the same keys.
    #[test]
    fn a_lookup_and_a_semi_join_answer_the_same_question() {
        for id in ["xf.join", "xf.lookup", "xf.lookup.outer", "xf.semi", "xf.semi.join"] {
            let sql = reject(id, serde_json::json!({ "leftKey": "a", "rightKey": "b" }))
                .unwrap_or_else(|| panic!("{id} declares a reject port and must fill it"));
            assert!(sql.contains("NOT EXISTS"), "{id}: {sql}");
            assert!(sql.contains("m.\"a\" = r.\"b\""), "{id}: {sql}");
        }
    }

    /// Composite keys ride the same construction as the join itself.
    #[test]
    fn a_composite_key_rejects_on_every_column() {
        let sql = reject(
            "xf.join.inner",
            serde_json::json!({ "leftKey": "a,b", "rightKey": "x,y" }),
        )
        .expect("sql");
        assert!(sql.contains("m.\"a\" = r.\"x\" AND m.\"b\" = r.\"y\""), "{sql}");
    }

    /// NOT EXISTS rather than NOT IN, for the reason build_semi already gives:
    /// a single NULL on the right makes `NOT IN` return UNKNOWN and silently
    /// reject every row. The reject stream is the last place to reintroduce it.
    #[test]
    fn the_reject_uses_not_exists_not_not_in() {
        let sql = reject("xf.join", serde_json::json!({ "leftKey": "a", "rightKey": "b" }))
            .expect("sql");
        assert!(!sql.contains("NOT IN"), "{sql}");
    }

    /// Without keys there is no way to say what "unmatched" means, and the
    /// message has to say that rather than produce an empty stream.
    #[test]
    fn a_join_with_no_keys_says_why_it_cannot_reject() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["l".into()]);
        ni.ports.insert("lookup".into(), vec!["r".into()]);
        let err = build_reject_sql("xf.join.inner", &serde_json::json!({}), &ni, None)
            .expect_err("no keys, no answer");
        assert!(err.contains("key"), "{err}");
    }
}

/// "Column match: by position" did nothing.
///
/// The four set operations declare a `matchBy` select, and build_union and
/// build_setop took no props at all - both hardcoded BY NAME. Someone whose
/// inputs are positionally aligned but differently NAMED picked "By position",
/// got a by-name union, and their columns were padded with NULLs into a wider
/// table instead of stacked. No error, wrong data.
#[cfg(test)]
mod set_operation_match_by {
    use super::*;

    fn two() -> NodeInputs {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["a".into(), "b".into()]);
        ni
    }

    fn by(v: &str) -> serde_json::Value {
        serde_json::json!({ "matchBy": v })
    }

    /// By name is the default and stays the default: an existing pipeline that
    /// never touched the control must emit exactly what it emitted before.
    #[test]
    fn by_name_remains_what_an_untouched_node_does() {
        for props in [serde_json::json!({}), by("name")] {
            let sql = build_union(&two(), true, &props).unwrap();
            assert!(sql.contains("UNION BY NAME"), "{sql}");
        }
        let all = build_union(&two(), false, &serde_json::json!({})).unwrap();
        assert!(all.contains("UNION ALL BY NAME"), "{all}");
    }

    /// The setting the form offers, which is the whole point.
    #[test]
    fn by_position_stacks_the_columns_as_they_come() {
        let sql = build_union(&two(), true, &by("position")).unwrap();
        assert!(sql.contains(" UNION "), "{sql}");
        assert!(!sql.contains("BY NAME"), "by position must not realign on names: {sql}");

        let all = build_union(&two(), false, &by("position")).unwrap();
        assert!(all.contains(" UNION ALL "), "{all}");
        assert!(!all.contains("BY NAME"), "{all}");
    }

    /// INTERSECT / EXCEPT realign later legs through a 0-row UNION ALL BY NAME
    /// template, because `INTERSECT BY NAME` is a parser error. By position
    /// there is nothing to realign, so the legs are compared as they stand.
    #[test]
    fn a_positional_intersect_drops_the_realignment_template() {
        for op in ["INTERSECT", "EXCEPT"] {
            let sql = build_setop(&two(), op, &by("position")).unwrap();
            assert!(sql.contains(&format!(" {op} ")), "{sql}");
            assert!(
                !sql.contains("WHERE false UNION ALL BY NAME"),
                "by position must not realign: {sql}"
            );
            assert!(!sql.contains(&format!("{op} BY NAME")), "still invalid syntax: {sql}");
        }
    }

    /// And by name it keeps realigning, which is the behaviour that guards
    /// against comparing the wrong columns.
    #[test]
    fn a_named_intersect_still_realigns() {
        let sql = build_setop(&two(), "INTERSECT", &by("name")).unwrap();
        assert!(sql.contains("WHERE false UNION ALL BY NAME"), "{sql}");
    }
}

/// The last three reject ports the join family advertised and could not fill.
///
/// xf.join.spatial can: unmatched is "no feature satisfied the predicate", the
/// same anti-join shape as a key join with ST_ in place of equality.
///
/// xf.anti and xf.join.cross cannot, structurally. An anti join's MAIN output
/// already IS the unmatched rows, so a reject port there would have to mean the
/// matched ones - a second meaning for the same word on the same canvas. A
/// cross join has no predicate, so nothing is ever unmatched. Their ports are
/// gone rather than filled.
#[cfg(test)]
mod spatial_and_the_ports_that_cannot_exist {
    use super::*;

    #[test]
    fn a_spatial_join_rejects_features_that_matched_nothing() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["l".into()]);
        ni.ports.insert("lookup".into(), vec!["r".into()]);
        let sql = build_reject_sql(
            "xf.join.spatial",
            &serde_json::json!({
                "leftGeomColumn": "geom",
                "rightGeomColumn": "shape",
                "relation": "within"
            }),
            &ni,
            None,
        )
        .expect("reject sql")
        .expect("a spatial join has unmatched features to reject");
        assert!(sql.contains("NOT EXISTS"), "{sql}");
        assert!(sql.contains("ST_Within"), "the reject must use the SAME predicate: {sql}");
        assert!(sql.contains("m.\"geom\"") && sql.contains("r.\"shape\""), "{sql}");
    }

    /// An unrecognised relation falls back to ST_Intersects in the join, so it
    /// has to fall back the same way here - or the two halves disagree about
    /// what "matched" meant.
    #[test]
    fn the_reject_falls_back_to_the_same_default_predicate() {
        let mut ni = NodeInputs::default();
        ni.ports.insert("main".into(), vec!["l".into()]);
        ni.ports.insert("lookup".into(), vec!["r".into()]);
        let sql = build_reject_sql(
            "xf.join.spatial",
            &serde_json::json!({ "leftGeomColumn": "g", "rightGeomColumn": "g" }),
            &ni,
            None,
        )
        .expect("ok")
        .expect("some sql");
        assert!(sql.contains("ST_Intersects"), "{sql}");
    }
}

/// A file sink must not silently overwrite when asked to do something else.
///
/// snk.parquet offered Append, snk.csv / json / jsonl / excel offered "Error if
/// exists", and NONE of those builders reads `mode` at all. An unrecognised
/// mode is not an error in a COPY - it is the default, and the default is
/// replace. Measured end to end before this guard: rows 1,2 written, a second
/// run with mode=append writing 3,4, and the file afterwards held ONLY 3,4.
/// The user asked to add to a dataset and destroyed it, with no error.
#[cfg(test)]
mod file_sink_modes {
    use super::*;

    fn sink(component: &str, mode: &str) -> Result<String, EngineError> {
        build_sink_sql(
            component,
            &serde_json::json!({ "path": "/tmp/out.dat", "mode": mode }),
            "v",
            &[],
            None,
        )
    }

    #[test]
    fn append_on_a_file_sink_is_refused_rather_than_silently_replacing() {
        let err = sink("snk.parquet", "append").expect_err("append must not plan as a replace");
        let msg = err.to_string();
        assert!(msg.contains("append"), "it names the mode: {msg}");
        assert!(msg.contains("replace") || msg.contains("overwrite"), "and what it would do: {msg}");
    }

    #[test]
    fn error_if_exists_is_refused_on_every_file_sink_that_offered_it() {
        for id in ["snk.csv", "snk.json", "snk.jsonl", "snk.parquet", "snk.excel"] {
            let err = sink(id, "error")
                .err()
                .unwrap_or_else(|| panic!("{id} accepted a mode it does not implement"));
            assert!(err.to_string().contains("error"), "{id}: {err}");
        }
    }

    /// Overwrite is what these sinks do, and an unset mode means the same, so
    /// both must keep planning exactly as before.
    #[test]
    fn overwrite_and_an_unset_mode_still_plan() {
        for id in ["snk.csv", "snk.json", "snk.parquet", "snk.excel"] {
            sink(id, "overwrite").unwrap_or_else(|e| panic!("{id} overwrite: {e}"));
            build_sink_sql(id, &serde_json::json!({ "path": "/tmp/o.dat" }), "v", &[], None)
                .unwrap_or_else(|e| panic!("{id} default: {e}"));
        }
    }
}
