//! Column-level lineage: resolve which source columns feed each projected
//! column of a SQL query, from DuckDB's `json_serialize_sql` AST.
//!
//! The AST walk is pure (input: the serialized-SQL JSON, output: lineage) so it
//! is unit-testable without the engine; `Engine::column_lineage` is the thin
//! wrapper that asks DuckDB to serialize the SQL and feeds the AST here. This is
//! the shared foundation the research flagged for impact analysis,
//! breaking-change data-diff, and data contracts - build the resolver once.

use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// A source column an output column is derived from. `table` is the reference's
/// qualifier as written (a table name or alias), if any - alias->real-table
/// resolution is a later refinement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSource {
    pub table: Option<String>,
    pub column: String,
}

/// One projected output column and the source columns that feed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputColumn {
    pub name: String,
    pub sources: Vec<ColumnSource>,
}

/// Resolve the lineage of every projected column from the value returned by
/// `json_serialize_sql(<query>)`. Returns an empty vec if the AST has no
/// SELECT node / select list (e.g. a non-SELECT statement).
pub fn lineage_from_serialized_sql(ast: &JsonValue) -> Vec<OutputColumn> {
    let node = ast
        .get("statements")
        .and_then(|s| s.as_array())
        .and_then(|a| a.first())
        .and_then(|s| s.get("node"));
    match node {
        Some(n) => select_node_lineage(n),
        None => Vec::new(),
    }
}

fn select_node_lineage(node: &JsonValue) -> Vec<OutputColumn> {
    let list = match node.get("select_list").and_then(|v| v.as_array()) {
        Some(l) => l,
        None => return Vec::new(),
    };
    list.iter()
        .enumerate()
        .map(|(i, item)| {
            let mut sources = Vec::new();
            collect_column_refs(item, &mut sources);
            OutputColumn {
                name: output_name(item, &sources, i),
                sources,
            }
        })
        .collect()
}

/// The name an item projects under: its explicit alias, else (for a bare column
/// reference) the column's own name, else a positional fallback. A star stands
/// for every column of its input, which the SQL alone cannot list, so it stays
/// `*` and traces to the `*` of its upstream rather than to a made-up `col1`.
fn output_name(item: &JsonValue, sources: &[ColumnSource], idx: usize) -> String {
    let alias = item.get("alias").and_then(|a| a.as_str()).unwrap_or("");
    if !alias.is_empty() {
        return alias.to_string();
    }
    if item.get("type").and_then(|t| t.as_str()) == Some("STAR") {
        return "*".to_string();
    }
    if item.get("type").and_then(|t| t.as_str()) == Some("COLUMN_REF") {
        if let Some(c) = sources.first() {
            return c.column.clone();
        }
    }
    format!("col{}", idx + 1)
}

/// Deep-walk an expression subtree and collect every COLUMN_REF. Walking the
/// whole subtree (rather than enumerating FUNCTION/operator/CASE/CAST/... node
/// types) makes this robust to arbitrarily nested expressions.
fn collect_column_refs(expr: &JsonValue, out: &mut Vec<ColumnSource>) {
    match expr {
        JsonValue::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("COLUMN_REF") {
                if let Some(names) = map.get("column_names").and_then(|n| n.as_array()) {
                    let parts: Vec<String> =
                        names.iter().filter_map(|n| n.as_str().map(String::from)).collect();
                    if let Some(column) = parts.last().cloned() {
                        let table = if parts.len() > 1 {
                            Some(parts[parts.len() - 2].clone())
                        } else {
                            None
                        };
                        let src = ColumnSource { table, column };
                        if !out.contains(&src) {
                            out.push(src);
                        }
                    }
                }
                // A COLUMN_REF has no child expressions to descend into.
                return;
            }
            for (_, v) in map {
                collect_column_refs(v, out);
            }
        }
        JsonValue::Array(arr) => {
            for v in arr {
                collect_column_refs(v, out);
            }
        }
        _ => {}
    }
}

// ---- Cross-stage stitching ---------------------------------------------
//
// A pipeline compiles to a DAG of stages; each stage's SQL has its own
// column lineage (above). Stitching chains those per-stage maps so a final
// output column traces back to the root source columns it actually came from.

/// A resolved origin: the column at the node where it enters the pipeline (a
/// source node with no upstream, or the deepest node the trace could reach).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RootColumn {
    pub node: String,
    pub column: String,
}

/// One node's lineage plus the upstream node ids feeding it, for stitching.
#[derive(Debug, Clone)]
pub struct NodeLineage {
    pub outputs: Vec<OutputColumn>,
    pub upstreams: Vec<String>,
}

/// Trace `column` of `node` back to its root source columns across the stage
/// graph. A node with no upstreams (a source) is a root; a column whose source
/// references an upstream node is followed into that node recursively.
pub fn resolve_roots(
    node: &str,
    column: &str,
    graph: &HashMap<String, NodeLineage>,
) -> Vec<RootColumn> {
    let mut roots = Vec::new();
    resolve_inner(node, column, graph, &mut Vec::new(), &mut roots);
    roots
}

fn resolve_inner(
    node: &str,
    column: &str,
    graph: &HashMap<String, NodeLineage>,
    visiting: &mut Vec<(String, String)>,
    roots: &mut Vec<RootColumn>,
) {
    let key = (node.to_string(), column.to_string());
    if visiting.contains(&key) {
        return; // cycle guard
    }
    let push_root = |roots: &mut Vec<RootColumn>, n: &str, c: &str| {
        let r = RootColumn { node: n.to_string(), column: c.to_string() };
        if !roots.contains(&r) {
            roots.push(r);
        }
    };
    let nl = match graph.get(node) {
        Some(n) => n,
        // Unknown node (e.g. a base table / source we don't have lineage for).
        None => return push_root(roots, node, column),
    };
    if nl.upstreams.is_empty() {
        return push_root(roots, node, column); // a source node is a root
    }
    let oc = nl.outputs.iter().find(|o| o.name == column);
    let oc = match oc {
        Some(o) if !o.sources.is_empty() => o,
        // No traceable derivation here. A single-input node with no lineage for
        // this column is a passthrough (a sink, ctl.* view, or SELECT *) - carry
        // the same column name into its upstream. Otherwise stop at this node.
        _ => {
            if nl.upstreams.len() == 1 {
                visiting.push(key);
                resolve_inner(&nl.upstreams[0], column, graph, visiting, roots);
                visiting.pop();
            } else {
                push_root(roots, node, column);
            }
            return;
        }
    };
    visiting.push(key);
    for src in &oc.sources {
        match pick_upstream(src, nl) {
            Some(u) => resolve_inner(&u, &src.column, graph, visiting, roots),
            None => push_root(roots, node, &src.column),
        }
    }
    visiting.pop();
}

/// Choose which upstream node a column reference came from: a matching table
/// qualifier (alias/id), else the sole upstream when there is exactly one.
fn pick_upstream(src: &ColumnSource, nl: &NodeLineage) -> Option<String> {
    if let Some(t) = &src.table {
        if let Some(u) = nl.upstreams.iter().find(|u| u.as_str() == t) {
            return Some(u.clone());
        }
    }
    if nl.upstreams.len() == 1 {
        return Some(nl.upstreams[0].clone());
    }
    None
}

/// Extract the SELECT body from a stage's compiled SQL. json_serialize_sql only
/// serializes SELECT statements, so strip a `CREATE [OR REPLACE] (VIEW|TABLE)
/// "name" AS ` wrapper (transform stages have no ATTACH prelude, so the first
/// ` AS ` is the CTAS separator). Returns None for non-query stages (COPY
/// sinks, PRAGMA-prefixed stages) - those are treated as passthrough.
pub fn select_body(sql: &str) -> Option<String> {
    let t = sql.trim().trim_end_matches(';').trim();
    let up = t.to_ascii_uppercase();
    if up.starts_with("SELECT") || up.starts_with("WITH") {
        return Some(t.to_string());
    }
    if up.starts_with("CREATE") {
        if let Some(idx) = t.find(" AS ") {
            let body = t[idx + 4..].trim();
            let bup = body.to_ascii_uppercase();
            if bup.starts_with("SELECT") || bup.starts_with("WITH") || body.starts_with('(') {
                return Some(body.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ast(select_list: JsonValue) -> JsonValue {
        json!({ "statements": [{ "node": { "select_list": select_list } }] })
    }

    #[test]
    fn resolves_passthrough_and_expression() {
        // SELECT a, b + c AS total
        let a = ast(json!([
            {"type":"COLUMN_REF","alias":"","column_names":["a"]},
            {"type":"FUNCTION","alias":"total","function_name":"+","children":[
                {"type":"COLUMN_REF","alias":"","column_names":["b"]},
                {"type":"COLUMN_REF","alias":"","column_names":["c"]}
            ]}
        ]));
        let lin = lineage_from_serialized_sql(&a);
        assert_eq!(lin.len(), 2);
        assert_eq!(lin[0].name, "a");
        assert_eq!(lin[0].sources, vec![ColumnSource { table: None, column: "a".into() }]);
        assert_eq!(lin[1].name, "total");
        assert_eq!(
            lin[1].sources,
            vec![
                ColumnSource { table: None, column: "b".into() },
                ColumnSource { table: None, column: "c".into() },
            ]
        );
    }

    #[test]
    fn resolves_aggregate_and_qualified_refs() {
        // SELECT region, sum(amount) AS total, c.name AS cust
        let a = ast(json!([
            {"type":"COLUMN_REF","alias":"","column_names":["region"]},
            {"type":"FUNCTION","alias":"total","function_name":"sum","children":[
                {"type":"COLUMN_REF","alias":"","column_names":["amount"]}
            ]},
            {"type":"COLUMN_REF","alias":"cust","column_names":["c","name"]}
        ]));
        let lin = lineage_from_serialized_sql(&a);
        assert_eq!(lin[0].name, "region");
        assert_eq!(lin[1].name, "total");
        assert_eq!(lin[1].sources, vec![ColumnSource { table: None, column: "amount".into() }]);
        assert_eq!(lin[2].name, "cust");
        assert_eq!(
            lin[2].sources,
            vec![ColumnSource { table: Some("c".into()), column: "name".into() }]
        );
    }

    #[test]
    fn a_star_is_every_column_not_one_called_col1() {
        // SELECT * cannot list the columns it stands for from the SQL alone.
        // Named col1, it traced a column that does not exist back to a source
        // column of the same false name, in a signed run manifest.
        let lin = lineage_from_serialized_sql(&ast(json!([
            {"type":"STAR","alias":"","relation_name":"","exclude_list":[]}
        ])));
        assert_eq!(lin.len(), 1);
        assert_eq!(lin[0].name, "*");
        let mut g: HashMap<String, NodeLineage> = HashMap::new();
        g.insert("s".into(), NodeLineage { outputs: vec![], upstreams: vec![] });
        g.insert("t".into(), NodeLineage { outputs: lin, upstreams: vec!["s".into()] });
        assert_eq!(resolve_roots("t", "*", &g), vec![RootColumn { node: "s".into(), column: "*".into() }]);
    }

    #[test]
    fn non_select_yields_empty() {
        assert!(lineage_from_serialized_sql(&json!({ "error": false, "statements": [] })).is_empty());
        assert!(lineage_from_serialized_sql(&json!({})).is_empty());
    }

    fn oc(name: &str, sources: &[(Option<&str>, &str)]) -> OutputColumn {
        OutputColumn {
            name: name.into(),
            sources: sources
                .iter()
                .map(|(t, c)| ColumnSource { table: t.map(String::from), column: (*c).into() })
                .collect(),
        }
    }

    #[test]
    fn stitches_lineage_across_stages_to_root_sources() {
        // s (source) -> t1: total = b + c, region passthrough -> t2: grand = total
        let mut g: HashMap<String, NodeLineage> = HashMap::new();
        g.insert("s".into(), NodeLineage { outputs: vec![], upstreams: vec![] });
        g.insert(
            "t1".into(),
            NodeLineage {
                outputs: vec![oc("total", &[(None, "b"), (None, "c")]), oc("region", &[(None, "region")])],
                upstreams: vec!["s".into()],
            },
        );
        g.insert(
            "t2".into(),
            NodeLineage {
                outputs: vec![oc("grand", &[(None, "total")]), oc("reg", &[(None, "region")])],
                upstreams: vec!["t1".into()],
            },
        );
        let mut grand = resolve_roots("t2", "grand", &g);
        grand.sort_by(|a, b| a.column.cmp(&b.column));
        assert_eq!(
            grand,
            vec![
                RootColumn { node: "s".into(), column: "b".into() },
                RootColumn { node: "s".into(), column: "c".into() },
            ]
        );
        assert_eq!(
            resolve_roots("t2", "reg", &g),
            vec![RootColumn { node: "s".into(), column: "region".into() }]
        );
    }

    #[test]
    fn stitch_join_uses_qualifier_to_pick_upstream() {
        // j joins a and b; id <- a.id, cust <- b.name (qualifier disambiguates).
        let mut g: HashMap<String, NodeLineage> = HashMap::new();
        g.insert("a".into(), NodeLineage { outputs: vec![], upstreams: vec![] });
        g.insert("b".into(), NodeLineage { outputs: vec![], upstreams: vec![] });
        g.insert(
            "j".into(),
            NodeLineage {
                outputs: vec![oc("id", &[(Some("a"), "id")]), oc("cust", &[(Some("b"), "name")])],
                upstreams: vec!["a".into(), "b".into()],
            },
        );
        assert_eq!(
            resolve_roots("j", "cust", &g),
            vec![RootColumn { node: "b".into(), column: "name".into() }]
        );
        assert_eq!(
            resolve_roots("j", "id", &g),
            vec![RootColumn { node: "a".into(), column: "id".into() }]
        );
    }

    #[test]
    fn select_body_strips_create_wrapper() {
        assert_eq!(
            select_body("CREATE OR REPLACE VIEW \"n\" AS SELECT a FROM t").as_deref(),
            Some("SELECT a FROM t")
        );
        assert_eq!(
            select_body("CREATE OR REPLACE TABLE \"n\" AS SELECT a FROM t;").as_deref(),
            Some("SELECT a FROM t")
        );
        assert_eq!(select_body("SELECT a FROM t").as_deref(), Some("SELECT a FROM t"));
        // A COPY sink (or other non-query) has no SELECT body.
        assert_eq!(select_body("COPY (SELECT * FROM t) TO 'x.csv'"), None);
    }
}
