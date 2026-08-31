//! Duckle metadata.
//!
//! Pipeline documents, schemas, and lineage records. These types are the
//! authoritative shape of what a pipeline _is_; the canvas UI, the
//! workflow engine, and the runtime all serialize against them.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

// ---- Types ----

/// All the primitive cell types Duckle understands. Mirrors the
/// TypeScript `DataType` in the frontend so JSON round-trips cleanly.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    Date,
    Timestamp,
    Time,
    Decimal,
    Json,
    Binary,
    /// DuckDB 1.5 native GEOMETRY (spatial) type. serde snake_case serializes
    /// this to "geometry", matching the frontend DataType token (issue #151).
    Geometry,
}

impl DataType {
    /// Display name used over the wire and in user-facing UI.
    pub fn name(self) -> &'static str {
        match self {
            DataType::String => "string",
            DataType::Int32 => "int32",
            DataType::Int64 => "int64",
            DataType::Float32 => "float32",
            DataType::Float64 => "float64",
            DataType::Bool => "bool",
            DataType::Date => "date",
            DataType::Timestamp => "timestamp",
            DataType::Time => "time",
            DataType::Decimal => "decimal",
            DataType::Json => "json",
            DataType::Binary => "binary",
            DataType::Geometry => "geometry",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(default, rename = "primaryKey", skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<bool>,
    /// Optional per-column strptime format (e.g. `%d/%m/%Y`) for DATE /
    /// TIMESTAMP columns. Lets several date columns each parse with a
    /// different format on one read (issue #10). None = use the source's
    /// own / auto-detected parsing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// #301: what this column holds, for the surfaces people INSPECT.
    ///
    /// `pii` and `secret` are the ones the engine acts on; anything else is
    /// carried for a caller that knows what it means. Declared rather than
    /// inferred: a heuristic that masked `company_name` because it contains
    /// "name" would teach people to distrust the masking, and one that quietly
    /// failed to mask something would be worse.
    ///
    /// Tagging changes what a preview, log or API response SHOWS. It never
    /// changes what the pipeline writes - that is what `qa.mask` is for.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

fn default_true() -> bool {
    true
}

pub type Schema = Vec<Column>;

// ---- Pipeline document ----

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
pub struct Position {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PipelineNode {
    pub id: String,
    /// React Flow node `type` - usually one of `source` / `transform` / `sink`.
    #[serde(rename = "type", default)]
    pub flow_type: Option<String>,
    pub position: Position,
    pub data: NodeData,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeData {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<String>,
    #[serde(default, rename = "componentId", skip_serializing_if = "Option::is_none")]
    pub component_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    #[serde(default, rename = "sampleRows", skip_serializing_if = "Option::is_none")]
    pub sample_rows: Option<Vec<JsonValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
    /// Optional user-defined SQL name for this node's output relation. When set,
    /// the engine also exposes the node's output under this name (a view), so
    /// raw / pure SQL nodes can reference upstream by a friendly name instead of
    /// the auto-generated node id (#102). Edge wiring still keys off `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PipelineEdge {
    pub id: String,
    pub source: String,
    pub target: String,
    #[serde(default, rename = "sourceHandle", skip_serializing_if = "Option::is_none")]
    pub source_handle: Option<String>,
    #[serde(default, rename = "targetHandle", skip_serializing_if = "Option::is_none")]
    pub target_handle: Option<String>,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<EdgeData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EdgeData {
    #[serde(rename = "connectionType")]
    pub connection_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Pipeline {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub version: u32,
    pub nodes: Vec<PipelineNode>,
    pub edges: Vec<PipelineEdge>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

impl Pipeline {
    pub fn new(name: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            name: name.into(),
            version: 1,
            nodes: Vec::new(),
            edges: Vec::new(),
            created_at: Some(now),
            updated_at: Some(now),
        }
    }

    pub fn upstream_of<'a>(
        &'a self,
        node_id: &'a str,
    ) -> impl Iterator<Item = &'a PipelineEdge> + 'a {
        self.edges.iter().filter(move |e| e.target == node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_roundtrip_json() {
        let mut p = Pipeline::new("orders_etl");
        p.nodes.push(PipelineNode {
            id: "s1".into(),
            flow_type: Some("source".into()),
            position: Position { x: 60.0, y: 140.0 },
            data: NodeData {
                label: "CSV".into(),
                subtitle: Some("orders.csv".into()),
                component_id: Some("src.csv".into()),
                alias: None,
                properties: None,
                schema: Some(vec![Column {
                    name: "order_id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    primary_key: Some(true),
                    format: None,
                }]),
                sample_rows: None,
                disabled: None,
            },
        });
        let json = serde_json::to_string(&p).unwrap();
        let back: Pipeline = serde_json::from_str(&json).unwrap();
        assert_eq!(back.nodes.len(), 1);
        assert_eq!(back.nodes[0].data.label, "CSV");
    }

    #[test]
    fn data_type_serde_matches_frontend() {
        let v = serde_json::to_string(&DataType::Int64).unwrap();
        assert_eq!(v, "\"int64\"");
        let parsed: DataType = serde_json::from_str("\"timestamp\"").unwrap();
        assert_eq!(parsed, DataType::Timestamp);
    }

    #[test]
    fn column_format_round_trips_and_is_optional() {
        // Issue #10: optional per-column strptime format. A column WITH a
        // format deserializes and re-serializes the `format` key; a column
        // WITHOUT one carries None and omits the key (skip_serializing_if).
        let with: Column =
            serde_json::from_str(r#"{"name":"d","type":"date","nullable":true,"format":"%d/%m/%Y"}"#)
                .unwrap();
        assert_eq!(with.format.as_deref(), Some("%d/%m/%Y"));
        assert!(serde_json::to_string(&with).unwrap().contains("\"format\":\"%d/%m/%Y\""));

        let without: Column =
            serde_json::from_str(r#"{"name":"d","type":"date","nullable":true}"#).unwrap();
        assert_eq!(without.format, None);
        assert!(!serde_json::to_string(&without).unwrap().contains("format"));
    }
}
