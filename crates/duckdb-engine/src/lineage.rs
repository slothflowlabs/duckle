//! Column-level lineage: resolve which source columns feed each projected
//! column of a SQL query, from DuckDB's `json_serialize_sql` AST.
//!
//! The AST walk is pure (input: the serialized-SQL JSON, output: lineage) so it
//! is unit-testable without the engine; `Engine::column_lineage` is the thin
//! wrapper that asks DuckDB to serialize the SQL and feeds the AST here. This is
//! the shared foundation the research flagged for impact analysis,
//! breaking-change data-diff, and data contracts - build the resolver once.

use serde_json::Value as JsonValue;

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
/// reference) the column's own name, else a positional fallback.
fn output_name(item: &JsonValue, sources: &[ColumnSource], idx: usize) -> String {
    let alias = item.get("alias").and_then(|a| a.as_str()).unwrap_or("");
    if !alias.is_empty() {
        return alias.to_string();
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
    fn non_select_yields_empty() {
        assert!(lineage_from_serialized_sql(&json!({ "error": false, "statements": [] })).is_empty());
        assert!(lineage_from_serialized_sql(&json!({})).is_empty());
    }
}
