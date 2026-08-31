//! Talend job (`.item`) importer.
//!
//! Reads the XML a Talend Studio job is stored as and produces a Duckle
//! pipeline. This is an interoperability reader for a file format: it parses
//! their data, never their code, and nothing here is derived from their
//! implementation.
//!
//! Coverage is deliberately the head of the distribution rather than the whole
//! catalogue. Talend ships 900+ components, but real jobs use a couple of
//! dozen: across a 44-job corpus only 16 distinct components appeared, and the
//! three hardest of them (the mapper, the child-job call, the parallel branch)
//! already have Duckle equivalents. Everything outside the table below is
//! reported as an unmapped node, never silently dropped, because a migration
//! that quietly loses a step is worse than one that refuses it.
//!
//! Three things deliberately do NOT convert:
//!   * Encrypted passwords (`enc:system.encryption.key.v1:...`). We cannot read
//!     them and would not want to bake them into a file if we could, so the
//!     property becomes an `${ENV:...}` placeholder and the run is reported.
//!   * Repository connections (`PROPERTY_TYPE=REPOSITORY`), where the host and
//!     credentials live in a separate repository item, not in the job. The job
//!     alone does not contain enough to connect.
//!   * Java expressions in mapper outputs (`TalendDate.getCurrentDate()`,
//!     `context.getProperty(..)`). A plain `Table.Column` reference maps to a
//!     column; anything else is reported for a human to translate.

use duckle_metadata::{EdgeData, NodeData, PipelineEdge, PipelineNode, Position};
use quick_xml::events::Event;
use quick_xml::Reader;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeMap;

/// Talend type of each column a mapper reads, keyed by `Table.Column` and by `Column`.
///
/// The file records it, which is what makes `new BigDecimal(x)` readable: the exact
/// constructor takes a string, the lossy one takes a double, and they do not agree.
type ColTypes = std::collections::BTreeMap<String, String>;

/// Which of a mapper's inputs each name belongs to, keyed by the name the file gives
/// the input. Only needed where the mapper looks something up: with more than one
/// relation in play, a bare column can sit in either and the reading is ambiguous.
type PortMap = std::collections::BTreeMap<String, String>;

/// A component Duckle could not translate, or a value it refused to guess.
#[derive(Debug, Clone, PartialEq)]
pub enum Warning {
    /// No Duckle equivalent for this Talend component.
    UnmappedComponent { node: String, component: String },
    /// Host/credentials live in a repository item, not in this job file.
    RepositoryConnection { node: String, component: String },
    /// Password is encrypted with a Studio key; emitted as a placeholder.
    EncryptedSecret { node: String, property: String, placeholder: String },
    /// A mapper output expression that is Java, not a column reference.
    JavaExpression { node: String, column: String, expression: String },
    /// A call whose child handed rows back to it, which a child pipeline cannot do.
    ChildReturnsRows { node: String },
    /// A Java body on a tJava/tJavaRow, which has to be ported by hand.
    ///
    /// `only_prints` when every statement is a print, so the body carries no rules. It
    /// still arrives with no SQL and still fails: the flag is there to triage a long
    /// list, not to let anything run.
    JavaBody { node: String, only_prints: bool },
    /// A Java body that set context values from the row it was given, carried over to
    /// nodes that set them once.
    ContextSetFromFirstRow { node: String, names: Vec<String> },
    /// The write action has no exact equivalent, so the nearest one was used.
    WriteActionApproximated { node: String, action: String, used: String },
    /// A SQL step that changes the database rather than returning rows.
    StatementNotQuery { node: String, verb: String },
    /// A link leaving a multi-output mapper that does not say which output it carries.
    MapperOutputUnnamed { node: String, target: String, outputs: Vec<String> },
}

impl std::fmt::Display for Warning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Warning::UnmappedComponent { node, component } => write!(
                f,
                "{node}: no Duckle equivalent for {component}; the node was imported as a \
                 placeholder and needs replacing by hand"
            ),
            Warning::RepositoryConnection { node, component } => write!(
                f,
                "{node} ({component}) uses a repository connection, so its host and credentials \
                 are not in this job file. Fill them in, or point the node at a saved connection"
            ),
            Warning::EncryptedSecret { node, property, placeholder } => write!(
                f,
                "{node}: {property} is encrypted with a Studio key and cannot be read. Set \
                 {placeholder} in the environment before running"
            ),
            Warning::WriteActionApproximated { node, action, used } => write!(
                f,
                "{node}: the legacy write action {action} amends rows that match the key and drops the rest. The nearest write mode here is '{used}', which also inserts the rows that do not match. Check that is what the table should hold"
            ),
            Warning::MapperOutputUnnamed { node, target, outputs } => write!(
                f,
                "{node}: the link to {target} does not say which of its outputs it \
                 carries ({}). It was attached to the first; check that is the one \
                 intended, because the outputs of a mapper are usually alternatives",
                outputs.join(", ")
            ),
            Warning::StatementNotQuery { node, verb } => write!(
                f,
                "{node}: this step runs a {verb}, which changes the database rather than \
                 returning rows. A SQL step here is read as a query and becomes a view, so \
                 the {verb} would not run. Move it into the sink that writes the table, or \
                 run it outside the pipeline"
            ),
            Warning::JavaExpression { node, column, expression } => write!(
                f,
                "{node}: output column {column} is computed by Java (`{expression}`), which does \
                 not translate. Rewrite it as a SQL expression"
            ),
            Warning::ChildReturnsRows { node } => write!(
                f,
                "{node}: the job it calls hands rows back to this one. A child pipeline \
                 runs for its side effects and returns nothing, so this node would stand \
                 in an empty relation. Have the child write a table this job reads, or \
                 fold the child's work in here"
            ),
            Warning::JavaBody { node, only_prints: true } => write!(
                f,
                "{node}: the Java body only prints, so it carries no rules to port. Drop the \
                 node, or replace it with a log"
            ),
            Warning::JavaBody { node, only_prints: false } => write!(
                f,
                "{node}: the Java body has to be rewritten as SQL before this job runs"
            ),
            Warning::ContextSetFromFirstRow { node, names } => write!(
                f,
                "{node}: sets {} from the row it is given. The Java ran once per row, so the last row decided what they held; a node sets them once, from the first row. The same thing for a single row, a different one for more",
                names.join(", ")
            ),
        }
    }
}

/// The result of reading one `.item` file.
#[derive(Debug, Clone)]
pub struct Import {
    /// Job name, taken from the file stem.
    pub name: String,
    pub nodes: Vec<PipelineNode>,
    pub edges: Vec<PipelineEdge>,
    /// Everything a human still has to resolve. Empty means a clean import.
    pub warnings: Vec<Warning>,
    /// Talend component name -> how many of them were seen.
    pub components: BTreeMap<String, usize>,
    /// Loop bodies lifted out of this job into pipelines of their own.
    ///
    /// A legacy job writes a loop's body inline, as the subjob hanging off the
    /// loop's iterate link. Duckle points a loop at a child pipeline instead, so
    /// the body has to become a file and the loop has to name it.
    pub children: Vec<Import>,
}

impl Import {
    /// Serialise to the same pipeline JSON the canvas and the runner read.
    pub fn to_pipeline_json(&self) -> JsonValue {
        serde_json::json!({
            "name": self.name,
            "nodes": self.nodes,
            "edges": self.edges,
        })
    }
}

/// One input of a mapper: what it is called, what it is matched on, and whether a row
/// with no match is dropped.
#[derive(Debug, Clone, Default)]
struct MapperInput {
    name: String,
    /// (its own column, the expression on the main side it is matched against)
    keys: Vec<(String, String)>,
    inner: bool,
}

/// One `<node>` as read from the file, before mapping.
struct RawNode {
    component: String,
    /// Talend's `UNIQUE_NAME`, e.g. `tDBInput_1`. Used as the Duckle node id
    /// because it is already unique within the job and appears verbatim in the
    /// `<connection>` elements.
    unique: String,
    params: BTreeMap<String, String>,
    /// Multi-row settings: parameter name -> rows, each row a field->value map.
    ///
    /// A `TABLE` parameter holds a list rather than a value - the key columns of
    /// a de-duplicate, a sort's criteria, a file mask list - and a reader that
    /// only sees flat name/value pairs finds nothing on them, which leaves the
    /// component unconfigured and failing validation for a setting that IS in
    /// the file.
    tables: BTreeMap<String, Vec<BTreeMap<String, String>>>,
    /// Declared type of each column, in the order the file lists them.
    ///
    /// A delimited file names its own columns and counts the rows to skip; the names are
    /// not in the file, so they have to come across with the node or the relation is
    /// named after a line of data.
    column_types: Vec<(String, String)>,
    /// The width a column declares, by name: (length, precision). A decimal
    /// declared with 9 decimal places is a different number from one rounded to 4.
    column_scale: std::collections::BTreeMap<String, (u32, u32)>,
    /// Column names the node declares on its main output.
    ///
    /// Some components take their output shape from the schema rather than from
    /// a parameter, so a reader that skips the metadata cannot configure them
    /// at all - the names are in the file, just not where parameters live.
    columns: Vec<String>,
    /// Mapper output expressions, keyed by output column.
    ///
    /// Flattened across every output the mapper writes, which is what a single-output
    /// mapper needs. [`mapper_outs`] keeps them apart for the rest.
    mapper_out: Vec<(String, String, String)>,
    /// The mapper's inputs, in the order the file lists them: the first carries the rows
    /// and the rest are looked up. Each keeps the columns it is matched on and whether
    /// the match is required.
    mapper_inputs: Vec<MapperInput>,
    /// Intermediate values the mapper names, in the order it computes them.
    ///
    /// They belong to the mapper: nothing outside it knows the name, so an output that
    /// uses one has to be given the value rather than the name.
    mapper_vars: Vec<(String, String)>,
    /// Talend type of each column the mapper reads, keyed by `Table.Column` and `Column`.
    mapper_types: ColTypes,
    /// The same expressions, grouped by the output that declares them, in file order.
    ///
    /// A mapper writes one relation per output and they are not variations on a theme:
    /// the two halves of a decision routinely share a column name and give it different
    /// expressions, so merging them silently answers one branch with the other's number.
    mapper_outs: Vec<(String, Vec<(String, String, String)>)>,
    /// The condition on an output, by output name, for the outputs that have one
    /// switched on. It decides which rows reach that output and no other.
    mapper_out_filters: Vec<(String, String)>,
    x: f64,
    y: f64,
}

/// Give a query's columns the names the job uses.
///
/// A database input names its own columns and takes whatever its query returns in that
/// order - the two disagree often enough, a query selecting a column the job calls
/// something else. Carried across with the query's own names, every step downstream
/// refers to a column that is not there.
///
/// The query is left exactly as written and the names are put on around it, so nothing
/// about what is fetched changes. Where the job names no columns there is nothing to
/// apply and the query stands alone.
fn named_by_schema(query: &str, columns: &[String]) -> String {
    let q = query.trim();
    if columns.is_empty() || q.is_empty() {
        return query.to_string();
    }
    // A node often declares the whole table and fetches part of it. Then the names it
    // declares cannot be laid over what comes back one for one, and the component matches
    // them up by name instead - so the query is left exactly as it is. Only where we can
    // see the query returns as many columns as the node names is the order the thing that
    // decides, and only then is anything put on around it.
    if selected_column_count(q) != Some(columns.len()) {
        return query.to_string();
    }
    let names = columns
        .iter()
        .map(|c| quote_sql_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT * FROM ({}) AS t({})", q.trim_end_matches(';'), names)
}

/// How many columns a query returns, when that can be seen from the query itself.
///
/// Only a plain select list is counted: `*` stands for however many the table has, and a
/// query assembled from something else cannot be read at all. None means "not knowable",
/// which is treated as "do not touch it".
fn selected_column_count(query: &str) -> Option<usize> {
    let q = query.trim().trim_end_matches(';');
    let rest = q.strip_prefix("select").or_else(|| q.strip_prefix("SELECT"))?;
    let rest = match rest.trim_start().strip_prefix("DISTINCT") {
        Some(r) => r,
        None => rest.trim_start().strip_prefix("distinct").unwrap_or(rest),
    };
    // The list runs to the FROM that closes it.
    let (mut depth, mut in_string, mut end) = (0i32, false, None);
    let bytes = rest.as_bytes();
    for (i, c) in rest.char_indices() {
        match c {
            '\'' if !(i > 0 && bytes[i - 1] == b'\\') => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth -= 1,
            _ if !in_string
                && depth == 0
                && rest[i..].len() >= 4
                && rest[i..i + 4].eq_ignore_ascii_case("from")
                && i > 0
                && bytes[i - 1].is_ascii_whitespace() =>
            {
                end = Some(i);
                break;
            }
            _ => {}
        }
    }
    let list = &rest[..end?];
    if list.contains('*') {
        return None;
    }
    let items = split_top_level(list, ",");
    (!items.is_empty()).then_some(items.len())
}

/// A column name as SQL writes one.
fn quote_sql_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The columns a delimited file declares, as a schema.
///
/// Only the reading components get one: a schema on a writer would pin the shape of what
/// it is handed rather than describe what it reads.
fn declared_schema(raw: &RawNode, component_id: &str) -> Option<duckle_metadata::Schema> {
    if !matches!(component_id, "src.csv") || raw.column_types.is_empty() {
        return None;
    }
    use duckle_metadata::{Column, DataType};
    Some(
        raw.column_types
            .iter()
            .map(|(name, ty)| Column {
                tags: Vec::new(),
                name: name.clone(),
                // The component reads a delimited file, so every field arrives as text
                // and the expressions that follow do their own conversion. Declaring a
                // narrower type here would make the read fail on a value the job itself
                // handles.
                data_type: match ty.as_str() {
                    "id_Date" => DataType::Date,
                    _ => DataType::String,
                },
                nullable: true,
                primary_key: None,
                format: None,
            })
            .collect(),
    )
}

/// Talend component -> (Duckle component id, React Flow node type).
///
/// `tDBInput`/`tDBOutput` are the modern generic forms; the concrete database
/// comes from the node's own `TYPE` parameter, so they are resolved separately
/// in [`map_component`].
/// Read a value out of the `PROPERTIES` blob a generic (tcomp) Talend component
/// carries.
///
/// Such a component keeps its whole configuration in one JSON document inside a
/// single `elementParameter`, so a reader that sees only flat name/value pairs
/// finds nothing on it at all: no account, no table, no query. On one corpus
/// that was every node of the largest connector family.
///
/// A value lives at `<path>.storedValue`. That is a bare scalar for most
/// properties, an object carrying `value` for booleans and numbers, and an
/// object carrying `name` for enums. Reading `value` on an enum yields nothing,
/// which silently drops exactly the settings worth importing - the
/// authentication type, the grant type - while looking like it worked.
fn tcomp_value(blob: &JsonValue, path: &str) -> Option<String> {
    // A value stored here is a context reference just as often as a literal, so it gets
    // the same rewrite a flat parameter does. Without it a connection's account and
    // warehouse arrive as the literal text "context.…" and nothing can resolve them.
    tcomp_stored(blob, path).map(|v| rewrite_context(&v).unwrap_or(v))
}

fn tcomp_stored(blob: &JsonValue, path: &str) -> Option<String> {
    let mut cur = blob;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    match cur.get("storedValue")? {
        JsonValue::String(s) if !s.is_empty() => Some(s.clone()),
        JsonValue::Bool(b) => Some(b.to_string()),
        JsonValue::Number(n) => Some(n.to_string()),
        // Enum before boolean: an enum object has `name` and no `value`, a
        // boolean has `value` and no `name`, so asking for `name` first reads
        // both correctly.
        JsonValue::Object(o) => match o.get("name").or_else(|| o.get("value")) {
            Some(JsonValue::String(s)) if !s.is_empty() => Some(s.clone()),
            Some(JsonValue::Bool(b)) => Some(b.to_string()),
            Some(JsonValue::Number(n)) => Some(n.to_string()),
            _ => None,
        },
        _ => None,
    }
}

/// The parsed `PROPERTIES` blob, if this node carries one.
fn tcomp_blob(raw: &RawNode) -> Option<JsonValue> {
    let text = raw.params.get("PROPERTIES")?;
    if text.len() < 2 {
        return None;
    }
    serde_json::from_str(text).ok()
}

fn static_map(component: &str) -> Option<(&'static str, &'static str)> {
    Some(match component {
        "tMysqlInput" => ("src.mysql", "source"),
        "tMysqlOutput" => ("snk.mysql", "sink"),
        "tOracleInput" => ("src.oracle", "source"),
        "tOracleOutput" => ("snk.oracle", "sink"),
        "tMSSqlInput" => ("src.sqlserver", "source"),
        "tMSSqlOutput" => ("snk.sqlserver", "sink"),
        "tPostgresqlInput" => ("src.postgres", "source"),
        "tPostgresqlOutput" => ("snk.postgres", "sink"),
        "tFileInputDelimited" => ("src.csv", "source"),
        "tFileOutputDelimited" => ("snk.csv", "sink"),
        "tFileInputExcel" => ("src.excel", "source"),
        "tFileOutputExcel" => ("snk.excel", "sink"),
        "tMap" => ("xf.map", "transform"),
        "tRunJob" => ("ctl.runjob", "transform"),
        "tUniqRow" => ("qa.unique", "transform"),
        // Markers, not work. Pre-job and post-job bracket a job, and Talend's
        // parallelize FANS SUBJOBS OUT rather than splitting rows - which is a
        // different thing from Duckle's ctl.parallelize, so mapping it there
        // asserted a row fan-out the job never had. All three exist to anchor
        // ordering links, which is what ctl.anchor is for.
        "tPrejob" | "tPostjob" | "tParallelize" => ("ctl.anchor", "transform"),
        // Opening and closing a shared connection is not work Duckle does: a
        // node resolves its own connection when it runs, so these mark a point
        // in the sequence and nothing else. Keeping them as anchors preserves
        // the ordering the job expressed through them.
        // A stopwatch measures how long a stretch of the job took. Duckle
        // records a duration for every stage already, so these mark a point in
        // the sequence and nothing else.
        "tChronometerStart" | "tChronometerStop" => ("ctl.anchor", "transform"),
        "tDBConnection" | "tDBClose" | "tSnowflakeConnection" | "tSnowflakeClose"
        | "tMysqlConnection" | "tMysqlClose" | "tOracleConnection" | "tOracleClose"
        | "tPostgresqlConnection" | "tPostgresqlClose" | "tMSSqlConnection"
        | "tMSSqlClose" => ("ctl.anchor", "transform"),
        // A log-catcher is a SOURCE of error rows, not a sink for them: what it
        // emits is mailed or written to a table downstream.
        "tLogCatcher" => ("src.runevents", "source"),
        // Both turn values into a row: one from constants, the other from the
        // iteration's current item, which ForEach exposes as ${ITER_ITEM_*}.
        "tFixedFlowInput" | "tIterateToFlow" => ("src.inline", "source"),
        "tFileList" => ("src.filelist", "source"),
        // A file-existence check is a listing of one path: one row, or none.
        "tFileExist" => ("src.filelist", "source"),
        // Turning a flow into an iteration IS the ForEach: each row becomes one
        // pass, and the row's fields are exposed to the child as ${ITER_ITEM_*}.
        "tFlowToIterate" => ("ctl.foreach", "transform"),
        "tFileCopy" | "tFileDelete" | "tFileArchive" => ("ctl.file", "transform"),
        // Components Duckle already has; these were placeholders only because
        // nobody had written the mapping line.
        "tSendMail" => ("snk.email", "sink"),
        "tLoop" => ("ctl.iterate", "transform"),
        "tSortRow" => ("xf.sort", "transform"),
        // Splitting one delimited column into named columns is Text to Columns.
        "tExtractDelimitedFields" => ("xf.text.tocolumns", "transform"),
        "tFileInputFullRow" => ("src.csv", "source"),
        // A raw statement against the connection, whichever family it is.
        "tDBRow" | "tSnowflakeRow" => ("code.sql", "transform"),
        // A Java body is business logic, and Duckle runs SQL. It cannot be
        // translated here - the proven reference implementation for this corpus
        // wrote a generic Java-to-SQL translator and abandoned it in favour of
        // porting the rules by hand.
        //
        // It maps to a custom-SQL node with NO sql, which fails validation and
        // says so. Mapping it to something that compiles - a log line, a
        // passthrough - would produce a pipeline that runs happily and silently
        // omits the rules, which is the worst outcome available: the shape looks
        // migrated and the numbers are wrong.
        "tJava" | "tJavaRow" => ("code.sql", "transform"),
        // A buffer exists to hand rows to whoever called this job, so it writes the file
        // the caller reads rather than a destination of its own.
        "tBufferOutput" => ("snk.parquet", "sink"),
        // Duckle already speaks Snowflake; these were arriving as placeholders
        // only because their configuration is in the tcomp PROPERTIES blob.
        "tSnowflakeInput" => ("src.snowflake", "source"),
        "tSnowflakeOutput" => ("snk.snowflake", "sink"),
        "tConvertType" => ("xf.cast", "transform"),
        // Passes rows through and prints them, which is what tLogRow does.
        "tLogRow" => ("xf.log", "transform"),
        // A raw SQL statement against the connection, whatever the family.
        "tMysqlRow" | "tOracleRow" | "tMSSqlRow" | "tPostgresqlRow" => ("code.sql", "transform"),
        // Talend's SCD components write a type-2 dimension.
        "tMysqlSCD" | "tOracleSCD" | "tMSSqlSCD" | "tPostgresqlSCD" => ("xf.cdc.scd2", "transform"),
        // A reusable sub-flow, invoked by name. Every built-in is spelled t
        // followed by a capital, so a name that is not is the project's own
        // sub-flow rather than a component nobody mapped - and calling another
        // pipeline is exactly what the run-job component does.
        other if is_subflow_name(other) => ("ctl.runjob", "transform"),
        _ => return None,
    })
}

/// True for a name that is not one of the built-in components.
///
/// The built-ins are all `t` followed by a capital letter. The port pseudo-nodes
/// a sub-flow's boundary produces are not invocations and must not be treated as
/// one, or a sub-flow would try to call itself.
fn is_subflow_name(name: &str) -> bool {
    if matches!(name, "INPUT" | "OUTPUT") {
        return false;
    }
    let mut c = name.chars();
    !matches!((c.next(), c.next()), (Some('t'), Some(second)) if second.is_ascii_uppercase())
}

/// Columns the component's own schema marks as keys.
///
/// A key-matched write needs to know what it matches on. The setting that names the
/// key explicitly is ignored by the component whenever it is told to use the schema's
/// keys instead, which is the usual configuration, so the schema is the reliable
/// source. It is stored as a JSON document inside the blob rather than as structure.
fn tcomp_key_columns(blob: &JsonValue) -> Vec<String> {
    let Some(raw) = tcomp_stored(blob, "table.main.schema") else {
        return Vec::new();
    };
    let Ok(schema) = serde_json::from_str::<JsonValue>(&raw) else {
        return Vec::new();
    };
    schema["fields"]
        .as_array()
        .map(|fields| {
            fields
                .iter()
                .filter(|f| f["talend.field.isKey"].as_str() == Some("true"))
                .filter_map(|f| f["name"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve the generic `tDBInput` / `tDBOutput` via the node's `TYPE` value.
fn map_component(raw: &RawNode) -> Option<(&'static str, &'static str)> {
    if let Some(hit) = static_map(&raw.component) {
        return Some(hit);
    }
    let family = raw.params.get("TYPE").map(|s| unquote(s).to_uppercase());
    let out = matches!(raw.component.as_str(), "tDBOutput");
    match (raw.component.as_str(), family.as_deref()) {
        ("tDBInput" | "tDBOutput", Some(fam)) => Some(match (fam, out) {
            ("MYSQL", false) => ("src.mysql", "source"),
            ("MYSQL", true) => ("snk.mysql", "sink"),
            ("ORACLE", false) => ("src.oracle", "source"),
            ("ORACLE", true) => ("snk.oracle", "sink"),
            ("MSSQL", false) => ("src.sqlserver", "source"),
            ("MSSQL", true) => ("snk.sqlserver", "sink"),
            ("POSTGRESQL", false) => ("src.postgres", "source"),
            ("POSTGRESQL", true) => ("snk.postgres", "sink"),
            _ => return None,
        }),
        _ => None,
    }
}

/// Talend stores parameter values as Java source, so a string literal arrives
/// wrapped in quotes. Strip one balanced pair; leave anything else (a bare
/// number, a `context.x` reference, an expression) untouched.
fn unquote(v: &str) -> String {
    let t = v.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// True for a value we must not copy into a pipeline file.
fn is_encrypted(v: &str) -> bool {
    v.trim_matches('"').starts_with("enc:")
}

/// The column of the loop's current row a value reads, if that is all it reads.
///
/// A row column is written `<flow>.<column>`; a component's own statistic has no dot and
/// is a different thing entirely, so only the dotted form is a row.
fn loop_row_column(v: &str) -> Option<&str> {
    let inner = v.split("globalMap.get(").nth(1)?;
    // Nothing may follow but the closing brackets, or this is part of a larger expression
    // and rewriting it alone would change what the whole says.
    let (key, rest) = inner.trim_start().strip_prefix('"')?.split_once('"')?;
    if !rest.trim_end_matches([')', ' ']).is_empty() {
        return None;
    }
    let (_, column) = key.split_once('.')?;
    let ok = !column.is_empty()
        && column.chars().all(|c| c.is_alphanumeric() || c == '_')
        && !column.contains('.');
    ok.then_some(column)
}

/// `context.foo` and `context.getProperty("foo")` become Duckle's `${foo}`, so
/// an imported job keeps using a context variable rather than freezing a value.
fn rewrite_context(v: &str) -> Option<String> {
    let t = v.trim();
    // A loop puts the row it is on where the steps inside it can reach it, by name. The
    // names are the loop's own and mean nothing here, so a value taken from the current
    // row - a file name, a query - arrived as the Java that would have fetched it, and
    // the step tried to use that text. The loop hands each column of the row to the work
    // it runs, so the column is named the way that work receives it.
    if let Some(column) = loop_row_column(t) {
        return Some(format!("${{ITER_ITEM_{}}}", column.to_uppercase()));
    }
    if let Some(rest) = t.strip_prefix("context.getProperty(") {
        let name = rest.trim_end_matches(')').trim().trim_matches('"');
        if !name.is_empty() {
            return Some(format!("${{{name}}}"));
        }
    }
    if let Some(name) = t.strip_prefix("context.") {
        let name = name.trim();
        if !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Some(format!("${{{name}}}"));
        }
    }
    // A path or a query is usually assembled from context values and literals joined with
    // +. It only reads one way when EVERY part is a literal or a context name; a row
    // reference or a call means the value is computed and must not be guessed at.
    //
    // A query field filled in as though it were a Java statement carries the terminator
    // as well. That is not part of the value, and leaving it on made the last piece stop
    // looking like a literal - so the whole query was carried across as Java and reached
    // the database still spelled `"UPDATE "+context...`.
    let t = t.strip_suffix(';').map(str::trim_end).unwrap_or(t);
    if t.contains('+') {
        let mut out = String::new();
        let (mut depth, mut in_string, mut start) = (0i32, false, 0usize);
        let mut parts: Vec<&str> = Vec::new();
        for (i, c) in t.char_indices() {
            match c {
                '"' => in_string = !in_string,
                _ if in_string => {}
                '(' => depth += 1,
                ')' => depth -= 1,
                '+' if depth == 0 => {
                    parts.push(&t[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        parts.push(&t[start..]);
        if parts.len() < 2 {
            return None;
        }
        for part in parts {
            let p = part.trim();
            if let Some(lit) = p.strip_prefix('"').and_then(|x| x.strip_suffix('"')) {
                if lit.contains('"') || lit.contains('\\') {
                    return None;
                }
                out.push_str(lit);
            } else if let Some(name) = p.strip_prefix("context.") {
                if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    return None;
                }
                out.push_str(&format!("${{{name}}}"));
            } else {
                return None;
            }
        }
        return Some(out);
    }
    None
}

/// Turn one Talend parameter into a Duckle property value, recording a warning
/// when the value cannot be carried across.
fn value_for(
    raw: &RawNode,
    key: &str,
    warnings: &mut Vec<Warning>,
) -> Option<JsonValue> {
    let raw_val = raw.params.get(key)?;
    if raw_val.trim().is_empty() {
        return None;
    }
    if is_encrypted(raw_val) {
        let placeholder = format!("${{ENV:{}_{}}}", raw.unique.to_uppercase(), key);
        warnings.push(Warning::EncryptedSecret {
            node: raw.unique.clone(),
            property: key.to_string(),
            placeholder: placeholder.clone(),
        });
        return Some(JsonValue::String(placeholder));
    }
    if let Some(ctx) = rewrite_context(raw_val) {
        return Some(JsonValue::String(ctx));
    }
    let v = unquote(raw_val);
    if v.is_empty() {
        return None;
    }
    Some(JsonValue::String(v))
}

/// Copy `(talend_key, duckle_key)` pairs into a property map.
fn copy_params(
    raw: &RawNode,
    pairs: &[(&str, &str)],
    props: &mut JsonMap<String, JsonValue>,
    warnings: &mut Vec<Warning>,
) {
    for (from, to) in pairs {
        if let Some(v) = value_for(raw, from, warnings) {
            props.insert((*to).to_string(), v);
        }
    }
}

/// Build the Duckle property map for one mapped node.
fn properties_for(
    raw: &RawNode,
    component_id: &str,
    context: &BTreeMap<String, String>,
    warnings: &mut Vec<Warning>,
) -> JsonMap<String, JsonValue> {
    let mut props = JsonMap::new();
    match component_id {
        "snk.parquet" if raw.component == "tBufferOutput" => {
            props.insert("path".into(), JsonValue::String(RETURN_FILE.into()));
            props.insert("mode".into(), JsonValue::String("overwrite".into()));
        }
        "code.sql" if raw.component.starts_with("tJava") => {
            let mut only_prints = false;
            // Keep the Java on the node so whoever writes the SQL can see what
            // it has to do, and leave `sql` empty so the node cannot be mistaken
            // for one that works.
            if let Some(code) = raw
                .params
                .get("CODE")
                .filter(|c| !c.trim().is_empty())
                .map(|c| unquote(c))
            {
                only_prints = java_body_only_prints(&code);
                props.insert("untranslatedSource".into(), JsonValue::String(code));
            }
            warnings.push(Warning::JavaBody { node: raw.unique.clone(), only_prints });
        }
        "code.sql" => {
            // The statement lives in the tcomp blob for a generic component and
            // in a flat parameter for a family-specific one, so try both before
            // giving up. Left unquoted: it is SQL, not a Java string literal.
            let sql = tcomp_blob(raw)
                .and_then(|b| tcomp_value(&b, "query"))
                .or_else(|| raw.params.get("QUERY").map(|v| unquote(v)))
                .or_else(|| raw.params.get("SQLQUERY").map(|v| unquote(v)));
            match sql.filter(|q| !q.trim().is_empty()) {
                Some(q) => {
                    // A SQL step returns rows and is compiled into a view. A statement
                    // that changes the database is not a query and cannot become one, so
                    // it would reach the database wrapped in CREATE VIEW and fail there.
                    // Say so at import instead, where it can still be dealt with.
                    if let Some(verb) = leading_statement_verb(&q) {
                        warnings.push(Warning::StatementNotQuery {
                            node: raw.unique.clone(),
                            verb,
                        });
                    }
                    props.insert("sql".into(), JsonValue::String(q));
                }
                None => warnings.push(Warning::RepositoryConnection {
                    node: raw.unique.clone(),
                    component: raw.component.clone(),
                }),
            }
        }
        "xf.text.tocolumns" => {
            copy_params(
                raw,
                &[("FIELD", "column"), ("FIELDSEPARATOR", "delimiter")],
                &mut props,
                warnings,
            );
            // The output names come from the node's declared schema rather than
            // a parameter: the split produces one column per declared field.
            if !raw.columns.is_empty() {
                props.insert(
                    "outputColumns".into(),
                    JsonValue::String(raw.columns.join(",")),
                );
            }
        }
        "qa.unique" => {
            // The key columns are a TABLE parameter: one row per column, with a
            // flag saying whether it takes part in the key. Reading only flat
            // parameters left this unset, so the node failed validation for a
            // setting that was in the file all along.
            let keys: Vec<JsonValue> = raw
                .tables
                .get("UNIQUE_KEY")
                .map(|rows| {
                    rows.iter()
                        .filter(|r| {
                            r.get("KEY_ATTRIBUTE")
                                .map(|v| v.eq_ignore_ascii_case("true"))
                                .unwrap_or(false)
                        })
                        .filter_map(|r| r.get("SCHEMA_COLUMN"))
                        .map(|c| JsonValue::String(unquote(c)))
                        .collect()
                })
                .unwrap_or_default();
            if !keys.is_empty() {
                props.insert("columns".into(), JsonValue::Array(keys));
            }
        }
        "ctl.iterate" => {
            // A counted loop runs from FROM to TO inclusive. Either bound may be
            // a context reference rather than a number, and a reference cannot
            // be turned into a count here, so it is passed through for the run
            // to resolve rather than guessed at.
            let num = |k: &str| -> Option<i64> {
                raw.params.get(k).and_then(|v| unquote(v).trim().parse().ok())
            };
            match (num("FROM"), num("TO")) {
                (Some(from), Some(to)) if to >= from => {
                    let step = num("STEP").filter(|s| *s > 0).unwrap_or(1);
                    let count = ((to - from) / step) + 1;
                    props.insert("count".into(), JsonValue::from(count));
                }
                _ => {
                    if let Some(to) = raw.params.get("TO") {
                        let name = unquote(to);
                        let name = name.strip_prefix("context.").unwrap_or(&name).to_string();
                        // The job carries its own context, so a bound written as
                        // context.NAME is resolvable here. Falling back to a
                        // placeholder leaves a pipeline that cannot run alone.
                        match context.get(&name).map(|v| unquote(v)) {
                            Some(v) if v.trim().parse::<i64>().is_ok() => {
                                props.insert("count".into(), JsonValue::String(v.trim().to_string()));
                            }
                            _ => {
                                props.insert(
                                    "count".into(),
                                    JsonValue::String(format!("${{{}}}", name)),
                                );
                            }
                        }
                    }
                    warnings.push(Warning::RepositoryConnection {
                        node: raw.unique.clone(),
                        component: "loop bound is a context value, not a number".into(),
                    });
                }
            }
        }
        "ctl.file" => {
            // Talend spells a move as "copy, then remove the source".
            let removing = raw
                .params
                .get("REMOVE_FILE")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            let op = if raw.component == "tFileArchive" {
                "archive"
            } else if raw.component == "tFileDelete" {
                "delete"
            } else if removing {
                "move"
            } else {
                "copy"
            };
            props.insert("op".into(), JsonValue::String(op.into()));
            copy_params(
                raw,
                &[
                    ("FILENAME", "source"),
                    ("DESTINATION", "destination"),
                    ("REPLACE_FILE", "overwrite"),
                    ("FAILON", "failOnError"),
                    // The archive component spells the same two differently.
                    ("SOURCE_FILE", "source"),
                    ("TARGET", "destination"),
                    ("OVERWRITE", "overwrite"),
                ],
                &mut props,
                warnings,
            );
        }
        "src.filelist" => copy_params(
            raw,
            &[
                ("DIRECTORY", "directory"),
                ("EXCLUDEFILEMASK", "exclude"),
                ("FILE_NAME", "path"),
            ],
            &mut props,
            warnings,
        ),
        "snk.email" => copy_params(
            raw,
            &[
                ("SMTP_HOST", "host"),
                ("SMTP_PORT", "port"),
                ("FROM", "fromAddress"),
                ("TO", "to"),
                ("SUBJECT", "subject"),
                ("MESSAGE", "body"),
                ("AUTH_USERNAME", "user"),
                ("AUTH_PASSWORD", "password"),
            ],
            &mut props,
            warnings,
        ),
        "src.snowflake" | "snk.snowflake" => {
            // A shared tSnowflakeConnection is mirrored into the node's own blob
            // under referencedComponent.reference, so read that first and fall
            // back to the node's inline connection.
            let blob = match tcomp_blob(raw) {
                Some(b) => b,
                None => {
                    warnings.push(Warning::RepositoryConnection {
                        node: raw.unique.clone(),
                        component: raw.component.clone(),
                    });
                    JsonValue::Null
                }
            };
            let pick = |leaf: &str| -> Option<String> {
                tcomp_value(&blob, &format!("connection.referencedComponent.reference.{leaf}"))
                    .or_else(|| tcomp_value(&blob, &format!("connection.{leaf}")))
            };
            for (leaf, prop) in [
                ("account", "account"),
                ("db", "database"),
                ("schemaName", "schema"),
                ("warehouse", "warehouse"),
                ("role", "role"),
                ("userPassword.userId", "username"),
            ] {
                if let Some(v) = pick(leaf) {
                    props.insert(prop.into(), JsonValue::String(v));
                }
            }
            if let Some(t) = tcomp_value(&blob, "table.tableName") {
                props.insert("tableName".into(), JsonValue::String(unquote(&t)));
            }
            // How the component writes. Without this the sink takes the default write
            // mode, which replaces the whole table - so an append became a replace, and
            // on a table several nodes write to, each one erased the one before it.
            if component_id == "snk.snowflake" {
                if let Some(action) = tcomp_stored(&blob, "outputAction") {
                    let action = action.trim().to_uppercase();
                    let mode = match action.as_str() {
                        "INSERT" => "append",
                        // Both match on a key. The legacy upsert amends the matching row
                        // and inserts the rest, which is what this mode does; the legacy
                        // update drops the rest instead, which no mode here does, so it
                        // is approximated and reported below.
                        "UPSERT" | "UPDATE" => "upsert",
                        // DELETE, and anything a later version adds, has no equivalent.
                        // Leaving the mode unset is the honest outcome: the node arrives
                        // unconfigured and is reported, rather than writing the wrong way.
                        _ => "",
                    };
                    if !mode.is_empty() {
                        props.insert("mode".into(), JsonValue::String(mode.into()));
                    }
                    if mode == "upsert" {
                        let keys = tcomp_key_columns(&blob);
                        if !keys.is_empty() {
                            props.insert(
                                "conflictColumns".into(),
                                JsonValue::String(keys.join(",")),
                            );
                        }
                    }
                    if action == "UPDATE" {
                        warnings.push(Warning::WriteActionApproximated {
                            node: raw.unique.clone(),
                            action,
                            used: mode.into(),
                        });
                    }
                }
            }
            if let Some(q) = tcomp_value(&blob, "query") {
                if component_id == "src.snowflake" && !q.trim().is_empty() {
                    props.insert(
                        "query".into(),
                        JsonValue::String(named_by_schema(&unquote(&q), &raw.columns)),
                    );
                }
            }
            // The password is Studio-encrypted and cannot be recovered here, so
            // name it as a placeholder rather than importing a value that would
            // fail at run time with no explanation.
            // The legacy component signs in with a user name and a password.
            // Duckle reaches Snowflake over the SQL API, which takes a token or
            // a key pair and has no password mode, so a password cannot be
            // carried across even if it were recoverable - and it is not, being
            // encrypted with a Studio key. Name the token the connection needs
            // and say why, rather than emitting a node that cannot authenticate.
            let placeholder = format!("${{ENV:{}_TOKEN}}", raw.unique.to_uppercase());
            props.insert("pat".into(), JsonValue::String(placeholder.clone()));
            warnings.push(Warning::EncryptedSecret {
                node: raw.unique.clone(),
                property: "pat".into(),
                placeholder,
            });
        }
        "src.mysql" | "snk.mysql" | "src.postgres" | "snk.postgres" => copy_params(
            raw,
            &[
                ("HOST", "host"),
                ("PORT", "port"),
                ("DBNAME", "database"),
                ("USER", "username"),
                ("PASS", "password"),
                ("TABLE", "tableName"),
            ],
            &mut props,
            warnings,
        ),
        "src.sqlserver" | "snk.sqlserver" => copy_params(
            raw,
            &[
                ("HOST", "host"),
                ("PORT", "port"),
                ("DBNAME", "database"),
                ("USER", "user"),
                ("PASS", "password"),
                ("DB_SCHEMA", "schema"),
                ("TABLE", "tableName"),
            ],
            &mut props,
            warnings,
        ),
        "src.oracle" | "snk.oracle" => {
            copy_params(
                raw,
                &[("USER", "user"), ("PASS", "password"), ("TABLE", "tableName")],
                &mut props,
                warnings,
            );
            // src.oracle wants one `connect` string rather than host/port/SID.
            let host = raw.params.get("HOST").map(|v| unquote(v)).unwrap_or_default();
            let port = raw.params.get("PORT").map(|v| unquote(v)).unwrap_or_default();
            let sid = raw
                .params
                .get("SID")
                .or_else(|| raw.params.get("SERVICE_NAME"))
                .or_else(|| raw.params.get("DBNAME"))
                .map(|v| unquote(v))
                .unwrap_or_default();
            if !host.is_empty() && !sid.is_empty() {
                let port = if port.is_empty() { "1521".to_string() } else { port };
                props.insert("connect".into(), JsonValue::String(format!("{host}:{port}/{sid}")));
            }
        }
        "src.csv" | "snk.csv" => {
            copy_params(raw, &[("FILENAME", "path")], &mut props, warnings);
            if let Some(sep) = value_for(raw, "FIELDSEPARATOR", warnings) {
                props.insert("delimiter".into(), sep);
            }
            // The whole-line component hands on each line as it stands - one field,
            // separators and all, for something further down to pick apart. Read as an
            // ordinary delimited file it is split on whatever the line happens to
            // contain, which is both the wrong shape and, against its single declared
            // column, a failure to read the file at all. So it is read with a separator
            // and a quote the text cannot hold.
            if raw.component == "tFileInputFullRow" {
                props.insert("delimiter".into(), JsonValue::String("\u{7}".to_string()));
                // And no quoting: the line is text, so a double quote standing in it is
                // a character like any other rather than the start of a quoted field.
                props.insert("quoteChar".into(), JsonValue::String(String::new()));
            }
            // The component counts header ROWS to skip and names its columns itself.
            // Read as "the first line is the header", the names come from whatever that
            // line happens to hold, so every column is renamed to a piece of data and
            // every expression downstream refers to something that is not there. Where
            // the node declares its columns they are the names, and the header count is
            // simply lines to skip. Where it declares none, the file's own header is the
            // only thing left to name them.
            let declared = !raw.column_types.is_empty();
            if let Some(h) = raw.params.get("HEADER") {
                let n: i64 = unquote(h).parse().unwrap_or(0);
                if declared {
                    props.insert("hasHeader".into(), JsonValue::Bool(false));
                    if n > 0 {
                        props.insert("skipLines".into(), JsonValue::from(n));
                    }
                } else {
                    props.insert("hasHeader".into(), JsonValue::Bool(n > 0));
                    if n > 1 {
                        props.insert("skipLines".into(), JsonValue::from(n - 1));
                    }
                }
            } else if declared {
                props.insert("hasHeader".into(), JsonValue::Bool(false));
            }
            // A writer says whether it puts the column names out as a first line. That is
            // its own setting, not the reader's count of lines to skip, so it is read on
            // its own - and it has to be, because the step that reads the file back is
            // told to skip a line either way. Written without the names, the line skipped
            // is a line of data.
            if let Some(h) = raw.params.get("INCLUDEHEADER") {
                props.insert("hasHeader".into(), JsonValue::Bool(unquote(h) == "true"));
            }
        }
        "src.inline" => {
            // The component hands one row of named values downstream - a batch id, a
            // file name, an error message. They are a TABLE parameter of column/value
            // pairs, and read as flat parameters there is nothing there at all: the node
            // produces no columns and every step that reads one fails on a name that is
            // not there.
            let mut cols = JsonMap::new();
            for (name, value) in row_value_pairs(raw) {
                if name.trim().is_empty() {
                    continue;
                }
                let v = rewrite_context(value).unwrap_or_else(|| unquote(value));
                cols.insert(name.trim().to_string(), JsonValue::String(v));
            }
            if !cols.is_empty() {
                props.insert("columns".into(), JsonValue::Object(cols));
            }
        }
        "src.excel" | "snk.excel" => {
            copy_params(raw, &[("FILENAME", "path")], &mut props, warnings);
        }
        "ctl.runjob" => {
            copy_params(raw, &[("PROCESS", "pipelineRef")], &mut props, warnings);
            // PROCESS holds the child's bare name, but pipelineRef is a path to the
            // child pipeline, and every child is written as `<name>.json`. Copying the
            // name verbatim left the reference pointing at nothing.
            let with_extension = match props.get("pipelineRef") {
                Some(JsonValue::String(n)) if !n.is_empty() && !n.ends_with(".json") => {
                    Some(format!("{n}.json"))
                }
                _ => None,
            };
            if let Some(path) = with_extension {
                props.insert("pipelineRef".into(), JsonValue::String(path));
            }
            // A sub-flow carries no PROCESS parameter: it IS the name, and the
            // importer writes it out under that name.
            if !props.contains_key("pipelineRef") && is_subflow_name(&raw.component) {
                props.insert(
                    "pipelineRef".into(),
                    JsonValue::String(format!("{}.json", raw.component)),
                );
            }
        }
        _ => {}
    }

    // A source with a hand-written query should carry it, whichever family.
    if component_id.starts_with("src.") {
        let query_key = if component_id == "src.mysql" || component_id == "src.postgres" {
            "sql"
        } else {
            "query"
        };
        if let Some(q) = value_for(raw, "QUERY", warnings) {
            let text = q.as_str().unwrap_or_default().trim().to_string();
            if !text.is_empty() {
                props.insert(query_key.to_string(), JsonValue::String(text));
                props.insert("mode".into(), JsonValue::String("query".into()));
            }
        }
    }
    props
}


/// The file a job writes its return rows to, and the calling job reads.
///
/// It travels to the child as an ordinary context substitution, so the child names it
/// without knowing where the parent put it.
pub const RETURN_FILE: &str = "${DUCKLE_RETURN}";

/// Where an imported project keeps the tables it produces for its own use.
///
/// One file for the whole project, not one per job, because a table written by one job
/// and read by another is the common case and each job runs in its own process.
pub const STAGING_DB: &str = "${workspace}/.duckle/staging.duckdb";

/// Serve every warehouse read the project can satisfy from its own output locally.
///
/// A job written against a warehouse uses it as working storage as well as a destination:
/// it writes a staging table, reads it back, joins it, writes it again. Every one of those
/// hops is billed, and none of them has to happen there - the rows were produced on this
/// machine and are being fetched back to be worked on here.
///
/// So a table this project both writes and reads is mirrored into a local file as it is
/// written, and the reads are pointed at the mirror. The warehouse write is left exactly
/// as it was, which is what makes this safe to do unasked: every table still lands where
/// it landed before, so nothing downstream of the project - a report, another tool, a
/// person - can tell the difference. Only the reads move.
///
/// A read moves only when the whole of it can: a query that also names a table this
/// project does not write still needs the warehouse to resolve that table, so it stays
/// there, and so does the table it reads, since a mirror would then be serving only part
/// of the query. Anything else - a query assembled at run time, a name that cannot be read
/// statically - is left alone rather than guessed at.
///
/// Returns the number of reads that moved.
pub fn route_reads_to_local_mirror(imports: &mut [&mut Import]) -> usize {
    // A job keeps its loop bodies as pipelines of their own, and the warehouse traffic is
    // as often inside one of those as it is in the job itself, so both passes walk the
    // whole tree rather than the top level.
    let mut written: std::collections::BTreeSet<String> = Default::default();
    let mut writers: std::collections::BTreeMap<String, usize> = Default::default();
    let mut reads: Vec<Vec<String>> = Vec::new();
    for im in imports.iter() {
        survey(im, &mut written, &mut writers, &mut reads);
    }
    if written.is_empty() {
        return 0;
    }

    // A table is worth mirroring when the project writes it and reads it back. It stops
    // being worth mirroring the moment one of its reads cannot move, because that read
    // would still go to the warehouse and would then be the only reader of a table the
    // rest of the project had stopped treating as remote. Dropping one table can strand
    // another read, so this settles rather than deciding in one pass.
    // The mirror is created by whichever write reaches it first and takes its shape from
    // that one. Where several steps write the same table they rarely carry the same
    // columns - one adds a field the others do not - and the next write then has nowhere
    // to put it. The warehouse table was made once, with room for all of them; a mirror
    // made from one write is not the same table, so it is not made.
    let written_twice: std::collections::BTreeSet<String> = written
        .iter()
        .filter(|t| writers.get(*t).copied().unwrap_or(0) > 1)
        .cloned()
        .collect();
    let mut local: std::collections::BTreeSet<String> = reads
        .iter()
        .flatten()
        .filter(|t| written.contains(*t) && !written_twice.contains(*t))
        .cloned()
        .collect();
    loop {
        let stranded: std::collections::BTreeSet<String> = reads
            .iter()
            .filter(|tables| {
                tables.iter().any(|t| local.contains(t))
                    && !tables.iter().all(|t| local.contains(t))
            })
            .flatten()
            .filter(|t| local.contains(*t))
            .cloned()
            .collect();
        if stranded.is_empty() {
            break;
        }
        local.retain(|t| !stranded.contains(t));
    }
    if local.is_empty() {
        return 0;
    }

    imports.iter_mut().map(|im| reroute(im, &local)).sum()
}

/// Collect what the project writes to the warehouse, and what each read of it names.
fn survey(
    im: &Import,
    written: &mut std::collections::BTreeSet<String>,
    writers: &mut std::collections::BTreeMap<String, usize>,
    reads: &mut Vec<Vec<String>>,
) {
    for n in &im.nodes {
        match n.data.component_id.as_deref() {
            Some("snk.snowflake") => {
                if let Some(t) = node_table(n) {
                    *writers.entry(t.clone()).or_insert(0) += 1;
                    written.insert(t);
                }
            }
            Some("src.snowflake") => {
                let tables = read_tables(n);
                if !tables.is_empty() {
                    reads.push(tables);
                }
            }
            _ => {}
        }
    }
    for c in &im.children {
        survey(c, written, writers, reads);
    }
}

/// Point every read of a mirrored table at the mirror, and fill the mirror as it is
/// written. Returns the number of reads that moved.
fn reroute(im: &mut Import, local: &std::collections::BTreeSet<String>) -> usize {
    let unordered = unordered_here(im, local);
    let mut moved = 0;
    // A lookup that moves has to be held until the mirror it now reads has been filled.
    // Its mapper already waits for the write, but the lookup itself waits for nothing, and
    // a mirror read too early is an absent table rather than a stale one.
    let mut held: Vec<(String, Vec<String>)> = Vec::new();
    let fed: std::collections::BTreeSet<String> =
        im.edges.iter().map(|e| e.target.clone()).collect();
    for n in im.nodes.iter_mut() {
        if n.data.component_id.as_deref() != Some("src.snowflake") {
            continue;
        }
        let tables = read_tables(n);
        if tables.is_empty() || !tables.iter().all(|t| local.contains(t)) {
            continue;
        }
        if !unordered.is_empty() && tables.iter().any(|t| unordered.contains(t)) {
            continue;
        }
        let node_id = n.id.clone();
        let is_lookup = !fed.contains(&node_id);
        let Some(props) = n.data.properties.as_mut().and_then(|p| p.as_object_mut()) else {
            continue;
        };
        // The local file is attached under a fixed alias, so a query that named the table
        // outright now qualifies it. A read that named only a table needs no query at all.
        let query = props
            .get("query")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        // The warehouse connection is no longer part of this node, and a token placeholder
        // left behind would keep asking for a secret nothing uses.
        props.retain(|k, _| {
            !matches!(
                k.as_str(),
                "account"
                    | "database"
                    | "schema"
                    | "warehouse"
                    | "role"
                    | "username"
                    | "pat"
                    | "query"
                    | "tableName"
            )
        });
        props.insert("database".into(), JsonValue::String(STAGING_DB.into()));
        if query.trim().is_empty() {
            props.insert("tableName".into(), JsonValue::String(tables[0].clone()));
        } else {
            props.insert("sql".into(), JsonValue::String(qualify_tables(&query, local)));
        }
        n.data.component_id = Some("src.duckdb".into());
        n.data.subtitle = Some("local mirror".into());
        if is_lookup {
            held.push((node_id, tables));
        }
        moved += 1;
    }

    // Mirror each write of a moved table, alongside the warehouse write it already does.
    let mirrors: Vec<PipelineNode> = im
        .nodes
        .iter()
        .filter(|n| n.data.component_id.as_deref() == Some("snk.snowflake"))
        .filter(|n| {
            node_table(n).is_some_and(|t| local.contains(&t) && !unordered.contains(&t))
        })
        .map(|n| {
            let mut copy = n.clone();
            copy.id = format!("{}__local", n.id);
            copy.data.label = format!("{} (local mirror)", n.data.label);
            copy.data.component_id = Some("snk.duckdb".into());
            copy.data.subtitle = Some("local mirror".into());
            copy.position.y += 90.0;
            if let Some(props) = copy.data.properties.as_mut().and_then(|p| p.as_object_mut()) {
                // How it writes carries over; where it wrote does not.
                props.retain(|k, _| matches!(k.as_str(), "tableName" | "mode" | "conflictColumns"));
                props.insert("database".into(), JsonValue::String(STAGING_DB.into()));
            }
            copy
        })
        .collect();
    for mirror in mirrors {
        let original = mirror.id.trim_end_matches("__local").to_string();
        let feeds: Vec<PipelineEdge> = im
            .edges
            .iter()
            .filter(|e| e.target == original)
            .map(|e| {
                let mut copy = e.clone();
                copy.id = format!("{}__local", e.id);
                copy.target = mirror.id.clone();
                copy
            })
            .collect();
        im.edges.extend(feeds);
        im.nodes.push(mirror);
    }

    // Hold each moved lookup until the mirrors of the tables it reads have been filled.
    for (read_id, tables) in held {
        let sources: Vec<String> = im
            .nodes
            .iter()
            .filter(|n| n.data.component_id.as_deref() == Some("snk.duckdb"))
            .filter(|n| node_table(n).is_some_and(|t| tables.contains(&t)))
            .map(|n| n.id.clone())
            .collect();
        for source in sources {
            // The mapper this lookup feeds is already behind the write, so this cannot
            // close a loop - but a graph is cheap to ask and a deadlock is not.
            if would_cycle(&im.edges, &source, &read_id) {
                continue;
            }
            im.edges.push(PipelineEdge {
                id: format!("await-{source}-{read_id}"),
                source,
                target: read_id.clone(),
                source_handle: Some("main".into()),
                target_handle: Some("main".into()),
                edge_type: None,
                data: Some(EdgeData {
                    connection_type: "on-subjob-ok".into(),
                    label: None,
                    condition: None,
                }),
            });
        }
    }

    for c in im.children.iter_mut() {
        moved += reroute(c, local);
    }
    moved
}

/// Tables this pipeline writes and reads with nothing ordering the two.
///
/// Reading the mirror is only the same as reading the warehouse if the mirror has been
/// filled by then. Within one pipeline that is a question the graph answers: the write has
/// to lead to the read, by rows or by one of the links that say what runs after what.
/// Where it does not, the read is left going to the warehouse - the difference being that
/// a warehouse table nothing wrote yet holds stale rows, while a local one is simply not
/// there, so guessing turns a quiet wrong answer into a failed run.
///
/// A write in a different pipeline is not this graph's business: whatever ordered it
/// before the warehouse read still orders it before the local one.
fn unordered_here(
    im: &Import,
    local: &std::collections::BTreeSet<String>,
) -> std::collections::BTreeSet<String> {
    let writes: Vec<(&str, String)> = im
        .nodes
        .iter()
        .filter(|n| n.data.component_id.as_deref() == Some("snk.snowflake"))
        .filter_map(|n| node_table(n).map(|t| (n.id.as_str(), t)))
        .filter(|(_, t)| local.contains(t))
        .collect();
    if writes.is_empty() {
        return Default::default();
    }
    let reads: Vec<(&str, Vec<String>)> = im
        .nodes
        .iter()
        .filter(|n| n.data.component_id.as_deref() == Some("src.snowflake"))
        .map(|n| (n.id.as_str(), read_tables(n)))
        .filter(|(_, t)| !t.is_empty())
        .collect();

    let mut out: std::collections::BTreeSet<String> = Default::default();
    for (write_id, table) in &writes {
        for (read_id, read_of) in &reads {
            if !read_of.contains(table) {
                continue;
            }
            if positions_of(im, read_id).iter().all(|at| reaches(im, write_id, at)) {
                continue;
            }
            out.insert(table.clone());
        }
    }
    out
}

/// Where a read sits in the order.
///
/// Normally that is the read itself. A mapper's second input is the exception: nothing
/// feeds it, so nothing can be shown to run before it and the question always answers
/// "nothing". Its real place is its mapper's, because that is when it is loaded - so a
/// write that lands before the mapper lands before the lookup too.
///
/// A read that feeds nothing keeps its own position; there is nowhere else to put it, and
/// it is not going to move anyway.
fn positions_of<'a>(im: &'a Import, read_id: &'a str) -> Vec<&'a str> {
    let fed = im.edges.iter().any(|e| e.target == read_id);
    if fed {
        return vec![read_id];
    }
    let consumers: Vec<&str> = im
        .edges
        .iter()
        .filter(|e| e.source == read_id)
        .map(|e| e.target.as_str())
        .collect();
    if consumers.is_empty() {
        vec![read_id]
    } else {
        consumers
    }
}

/// Whether one node leads to another, by rows or by an ordering link.
fn reaches(im: &Import, from: &str, to: &str) -> bool {
    let mut seen: std::collections::BTreeSet<&str> = Default::default();
    let mut stack = vec![from];
    while let Some(node) = stack.pop() {
        if node == to {
            return true;
        }
        if !seen.insert(node) {
            continue;
        }
        stack.extend(
            im.edges.iter().filter(|e| e.source == node).map(|e| e.target.as_str()),
        );
    }
    false
}

/// The table a warehouse node names outright, if it names one.
fn node_table(n: &PipelineNode) -> Option<String> {
    let t = n.data.properties.as_ref()?.get("tableName")?.as_str()?.trim();
    (!t.is_empty()).then(|| t.to_string())
}

/// The tables a warehouse read names - from its table setting, or from its query.
fn read_tables(n: &PipelineNode) -> Vec<String> {
    let Some(props) = n.data.properties.as_ref() else {
        return Vec::new();
    };
    let query = props.get("query").and_then(|v| v.as_str()).unwrap_or_default();
    if query.trim().is_empty() {
        return node_table(n).into_iter().collect();
    }
    tables_in_query(query)
}

/// Table names a query reads from.
///
/// Deliberately shallow: it reads the name after FROM and JOIN and nothing else. That is
/// enough to tell a query reading one staging table from one that also reaches for
/// something this project does not write, which is the only question being asked. A name
/// it cannot make sense of leaves the query where it is, which is the safe direction.
fn tables_in_query(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let words: Vec<&str> = sql.split_whitespace().collect();
    for (i, w) in words.iter().enumerate() {
        if !w.eq_ignore_ascii_case("from") && !w.eq_ignore_ascii_case("join") {
            continue;
        }
        let Some(next) = words.get(i + 1) else { continue };
        // A subquery or a derived table is not a plain name, so this read stays put.
        if next.starts_with('(') {
            return Vec::new();
        }
        let name = next.trim_end_matches([',', ';', ')']).trim();
        let plain = name
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '$' | '{' | '}' | '.' | '"'));
        if name.is_empty() || !plain {
            return Vec::new();
        }
        out.push(name.to_string());
    }
    out
}

/// Rewrite the name after FROM / JOIN so it reads from the attached local file.
fn qualify_tables(sql: &str, local: &std::collections::BTreeSet<String>) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let mut after_keyword = false;
    for part in sql.split_inclusive(char::is_whitespace) {
        let word = part.trim();
        let bare = word.trim_end_matches([',', ';', ')']);
        if after_keyword && local.contains(bare) {
            out.push_str(&part.replacen(bare, &format!("duckle_src.\"{bare}\""), 1));
        } else {
            out.push_str(part);
        }
        after_keyword = word.eq_ignore_ascii_case("from") || word.eq_ignore_ascii_case("join");
    }
    out
}


impl Import {
    /// Does this job hand rows back to whoever calls it?
    ///
    /// A lifted loop body counts: the rows still leave the job, just from further in.
    pub fn returns_rows(&self) -> bool {
        self.nodes.iter().any(|n| {
            n.data
                .properties
                .as_ref()
                .and_then(|p| p.get("path"))
                .and_then(|v| v.as_str())
                == Some(RETURN_FILE)
        }) || self.children.iter().any(Import::returns_rows)
    }

    /// Every node in this job and in the bodies lifted out of it.
    pub fn all_nodes(&self) -> Vec<&PipelineNode> {
        let mut out: Vec<&PipelineNode> = self.nodes.iter().collect();
        for c in &self.children {
            out.extend(c.all_nodes());
        }
        out
    }

    /// A reusable body a job calls into, rather than a job of its own.
    ///
    /// Its ports are wiring, not work, so it is meant to be spliced into its callers and
    /// is not runnable by itself.
    pub fn is_subflow_body(&self) -> bool {
        self.nodes.iter().any(|n| boundary_port(n).is_some())
    }
}

/// Is this node one of a body's boundary ports rather than work it does?
///
/// A port has no Duckle equivalent of its own, so it imports unmapped and keeps the
/// component name in its label.
fn boundary_port(node: &PipelineNode) -> Option<&'static str> {
    if node.data.component_id.is_some() {
        return None;
    }
    let label = node.data.label.as_str();
    if label.starts_with("INPUT") {
        Some("input")
    } else if label.starts_with("OUTPUT") {
        Some("output")
    } else {
        None
    }
}

/// Splice a reusable body into the job that calls it, replacing the call.
///
/// A child pipeline runs for its side effects and is handed no rows, so a call to a body
/// that takes an input could never work by reference. Inlining is also what the source
/// tool does when it generates the job, so the result is the shape the original had.
///
/// The body's nodes are prefixed with the call's id, because one job may call the same
/// body more than once and the second copy must not land on the first.
pub fn inline_subflow(parent: &mut Import, call_id: &str, body: &Import) -> Result<(), String> {
    if !parent.nodes.iter().any(|n| n.id == call_id) {
        return Err(format!("{call_id} is not a node in {}", parent.name));
    }
    let prefixed = |id: &str| format!("{call_id}__{id}");

    let ports: BTreeMap<&str, &'static str> = body
        .nodes
        .iter()
        .filter_map(|n| boundary_port(n).map(|kind| (n.id.as_str(), kind)))
        .collect();

    // Where the body starts, and which node feeds each named output.
    let mut entries: Vec<String> = Vec::new();
    let mut exits: BTreeMap<String, String> = BTreeMap::new();
    for e in &body.edges {
        match (ports.get(e.source.as_str()), ports.get(e.target.as_str())) {
            (Some(&"input"), None) => entries.push(prefixed(&e.target)),
            (None, Some(&"output")) => {
                exits.insert(e.target.clone(), prefixed(&e.source));
            }
            _ => {}
        }
    }

    // The body's own work, and the links between it.
    for n in body.nodes.iter().filter(|n| boundary_port(n).is_none()) {
        let mut copy = n.clone();
        copy.id = prefixed(&n.id);
        copy.data.label = copy.id.clone();
        parent.nodes.push(copy);
    }
    let mut next_edge = parent.edges.len();
    for e in body
        .edges
        .iter()
        .filter(|e| !ports.contains_key(e.source.as_str()) && !ports.contains_key(e.target.as_str()))
    {
        next_edge += 1;
        let mut copy = e.clone();
        copy.id = format!("e{next_edge}");
        copy.source = prefixed(&e.source);
        copy.target = prefixed(&e.target);
        parent.edges.push(copy);
    }

    // Re-point the caller's own links at the body, then drop the call itself.
    let single_exit = (exits.len() == 1).then(|| exits.values().next().cloned().unwrap());
    let mut rewired: Vec<PipelineEdge> = Vec::new();
    for e in std::mem::take(&mut parent.edges) {
        if e.target == call_id {
            for entry in &entries {
                let mut copy = e.clone();
                next_edge += 1;
                copy.id = format!("e{next_edge}");
                copy.target = entry.clone();
                rewired.push(copy);
            }
        } else if e.source == call_id {
            // The label names which output this link left by. With one output there is
            // nothing to choose between, so an unlabelled link still resolves.
            let port = e.data.as_ref().and_then(|d| d.label.clone());
            let from = port.and_then(|p| exits.get(&p).cloned()).or_else(|| single_exit.clone());
            let Some(from) = from else { continue };
            let mut copy = e.clone();
            copy.source = from;
            if let Some(d) = copy.data.as_mut() {
                d.label = None;
            }
            rewired.push(copy);
        } else {
            rewired.push(e);
        }
    }
    parent.edges = rewired;
    parent.nodes.retain(|n| n.id != call_id);
    // The splice folded the body in, so this call no longer stands in an empty relation.
    parent
        .warnings
        .retain(|w| !matches!(w, Warning::ChildReturnsRows { node } if node == call_id));
    for (component, n) in &body.components {
        *parent.components.entry(component.clone()).or_insert(0) += n;
    }
    // The ports were wiring and the splice resolved them, so reporting them as having no
    // equivalent would now be false. Everything else the body needs still needs it.
    parent.warnings.extend(
        body
            .warnings
            .iter()
            .filter(|w| !matches!(w, Warning::UnmappedComponent { node, .. } if ports.contains_key(node.as_str())))
            .cloned(),
    );
    Ok(())
}

/// Is every parenthesis in `s` closed in order?
fn balanced(s: &str) -> bool {
    let mut depth = 0i32;
    for c in s.chars() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

/// The arguments of `name(...)`, split on top-level commas, when the whole expression is
/// that one call.
fn call_args<'a>(e: &'a str, name: &str) -> Option<Vec<&'a str>> {
    let rest = e.strip_prefix(name)?.trim_start();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    if !balanced(inner) {
        return None;
    }
    let mut out = Vec::new();
    let (mut depth, mut start, mut in_string) = (0i32, 0usize, false);
    let bytes = inner.as_bytes();
    for (i, c) in inner.char_indices() {
        match c {
            // A comma inside a literal is part of the literal, not a separator between
            // arguments. Split on it and a call looks like it has more arguments than it
            // does, so the whole expression goes unread.
            '"' if !(i > 0 && bytes[i - 1] == b'\\') => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth -= 1,
            ',' if depth == 0 && !in_string => {
                out.push(&inner[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&inner[start..]);
    Some(out)
}

/// Does every statement in a Java body just print?
///
/// Such a body has no effect on the data, so it carries no rules to port. Anything else -
/// an assignment, a context write, a call - counts as a rule, because treating one as
/// harmless is how a pipeline ends up running happily while omitting the logic.
/// A Java body with its comments taken out.
fn java_source_without_comments(code: &str) -> Option<String> {
    let mut out = String::with_capacity(code.len());
    let mut rest = code;
    while let Some(start) = rest.find("/*") {
        out.push_str(&rest[..start]);
        match rest[start + 2..].find("*/") {
            Some(end) => rest = &rest[start + 2 + end + 2..],
            // An unclosed comment means the body is not readable at all.
            None => return None,
        }
    }
    out.push_str(rest);
    Some(
        out.lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// The context values a Java body sets, in the order it sets them.
///
/// `None` unless the body is nothing BUT such assignments. A body that decides
/// something, keeps a working value of its own, or prints, has rules in it that do not
/// survive being reduced to a list of assignments - and reading only the assignments
/// out of one would carry half of it over and quietly leave the rest behind, which is
/// worse than carrying none of it, because what is left looks finished.
fn context_assignments(code: &str) -> Option<Vec<(String, String)>> {
    let source = java_source_without_comments(code)?;
    // Anything that opens a block is control flow, and control flow is a rule.
    if source.contains('{') || source.contains('}') {
        return None;
    }
    let mut out = Vec::new();
    for stmt in split_top_level(&source, ";") {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        let rest = stmt.strip_prefix("context.")?;
        let (name, value) = rest.split_once('=')?;
        let name = name.trim();
        // `==` is a comparison, not an assignment, and a name is a plain word.
        if value.starts_with('=')
            || name.is_empty()
            || !name.chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            return None;
        }
        let value = value.trim();
        if value.is_empty() {
            return None;
        }
        out.push((name.to_string(), value.to_string()));
    }
    (!out.is_empty()).then_some(out)
}

fn java_body_only_prints(code: &str) -> bool {
    let without_block_comments = {
        let mut out = String::with_capacity(code.len());
        let mut rest = code;
        while let Some(start) = rest.find("/*") {
            out.push_str(&rest[..start]);
            match rest[start + 2..].find("*/") {
                Some(end) => rest = &rest[start + 2 + end + 2..],
                None => return false,
            }
        }
        out.push_str(rest);
        out
    };
    let source: String = without_block_comments
        .lines()
        .map(|l| l.split("//").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("
");
    let statements: Vec<&str> =
        source.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();
    !statements.is_empty()
        && statements
            .iter()
            .all(|s| s.starts_with("System.out.print") || s.starts_with("System.err.print"))
}

/// Split a Java conditional `cond ? a : b` at its own `?` and matching `:`.
///
/// A chain is right-associative, so the matching colon is the one that balances the
/// question marks after it rather than simply the next one.
fn split_ternary(e: &str) -> Option<(&str, &str, &str)> {
    let bytes = e.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut q = None;
    for (i, &c) in bytes.iter().enumerate() {
        match c {
            b'"' => in_string = !in_string,
            _ if in_string => {}
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'?' if depth == 0 => {
                q = Some(i);
                break;
            }
            _ => {}
        }
    }
    let q = q?;
    let (mut depth, mut pending, mut in_string) = (0i32, 0i32, false);
    for (i, &c) in bytes.iter().enumerate().skip(q + 1) {
        match c {
            b'"' => in_string = !in_string,
            _ if in_string => {}
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'?' if depth == 0 => pending += 1,
            b':' if depth == 0 => {
                if pending == 0 {
                    return Some((&e[..q], &e[q + 1..i], &e[i + 1..]));
                }
                pending -= 1;
            }
            _ => {}
        }
    }
    None
}

/// Translate a Java boolean to SQL. Only equality is read: an ordering would need its
/// sign checked against the comparison's contract, which is a guess we do not make.
fn java_condition_to_sql(cond: &str, types: &ColTypes, ports: &PortMap) -> Option<String> {
    let c = cond.trim();
    if c.is_empty() {
        return None;
    }
    if let Some(inner) = c.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
        if balanced(inner) {
            return java_condition_to_sql(inner, types, ports);
        }
    }

    // Tests joined together, loosest first so the reading nests the way Java does.
    for (token, op) in [("||", "OR"), ("&&", "AND")] {
        let parts = split_top_level(c, token);
        if parts.len() > 1 {
            let rendered = parts
                .iter()
                .map(|p| Some(format!("({})", java_condition_to_sql(p, types, ports)?)))
                .collect::<Option<Vec<_>>>()?;
            return Some(rendered.join(&format!(" {op} ")));
        }
    }
    if let Some(rest) = c.strip_prefix('!') {
        // `!=` is a comparison, not a negation.
        if !rest.starts_with('=') {
            return Some(format!("NOT ({})", java_condition_to_sql(rest, types, ports)?));
        }
    }

    // A comparison. The longer spellings come first so `<=` is not read as `<`.
    for (token, op) in [
        ("==", "="),
        ("!=", "<>"),
        ("<=", "<="),
        (">=", ">="),
        ("<", "<"),
        (">", ">"),
    ] {
        let parts = split_top_level(c, token);
        if parts.len() != 2 {
            continue;
        }
        let (lhs, rhs) = (parts[0], parts[1]);
        // `x.compareTo(y) == 0` is how Java spells `x = y` for an exact decimal.
        if let Some((recv, args)) = method_call(lhs.trim(), "compareTo") {
            if args.len() == 1 && rhs.trim() == "0" && matches!(op, "=" | "<>" | "<" | ">" | "<=" | ">=")
            {
                return Some(format!(
                    "{} {op} {}",
                    java_expr_to_sql(recv, types, ports)?,
                    java_expr_to_sql(args[0], types, ports)?
                ));
            }
        }
        return Some(format!(
            "{} {op} {}",
            java_expr_to_sql(lhs, types, ports)?,
            java_expr_to_sql(rhs, types, ports)?
        ));
    }

    // A test written as a method on the value.
    if let Some((recv, args)) = method_call(c, "equals") {
        if args.len() == 1 {
            return Some(format!(
                "{} = {}",
                java_expr_to_sql(recv, types, ports)?,
                java_expr_to_sql(args[0], types, ports)?
            ));
        }
    }
    if let Some((recv, args)) = method_call(c, "equalsIgnoreCase") {
        if args.len() == 1 {
            return Some(format!(
                "upper({}) = upper({})",
                java_expr_to_sql(recv, types, ports)?,
                java_expr_to_sql(args[0], types, ports)?
            ));
        }
    }
    if let Some((recv, args)) = method_call(c, "isEmpty") {
        if args.iter().all(|a| a.trim().is_empty()) {
            return Some(format!("{} = ''", java_expr_to_sql(recv, types, ports)?));
        }
    }
    for (name, sql_fn) in [
        ("startsWith", "starts_with"),
        ("endsWith", "ends_with"),
        ("contains", "contains"),
    ] {
        if let Some((recv, args)) = method_call(c, name) {
            if args.len() == 1 {
                return Some(format!(
                    "{sql_fn}({}, {})",
                    java_expr_to_sql(recv, types, ports)?,
                    java_expr_to_sql(args[0], types, ports)?
                ));
            }
        }
    }
    // A bare value used as a test is a boolean column, and reads as itself.
    java_expr_to_sql(c, types, ports)
}

/// Split on a token at the top level, outside any string or bracket.
fn split_top_level<'a>(e: &'a str, token: &str) -> Vec<&'a str> {
    let (mut depth, mut in_string) = (0i32, false);
    let mut parts = Vec::new();
    let mut start = 0usize;
    let bytes = e.as_bytes();
    let mut i = 0usize;
    while i < e.len() {
        if !e.is_char_boundary(i) {
            i += 1;
            continue;
        }
        let c = e[i..].chars().next().unwrap();
        match c {
            '"' if !(i > 0 && bytes[i - 1] == b'\\') => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth -= 1,
            _ if !in_string && depth == 0 && e[i..].starts_with(token) => {
                // `=` inside `==` / `<=` must not be taken for the shorter spelling.
                let before = i.checked_sub(1).map(|j| bytes[j]);
                let after = e.as_bytes().get(i + token.len()).copied();
                let glued = matches!(before, Some(b'=' | b'<' | b'>' | b'!'))
                    || matches!(after, Some(b'='))
                    || (token.len() == 1
                        && matches!(token.as_bytes()[0], b'<' | b'>')
                        && matches!(after, Some(b'=')));
                if !glued {
                    parts.push(&e[start..i]);
                    start = i + token.len();
                    i += token.len();
                    continue;
                }
            }
            _ => {}
        }
        i += c.len_utf8();
    }
    if parts.is_empty() {
        return vec![e];
    }
    parts.push(&e[start..]);
    parts.into_iter().map(str::trim).filter(|p| !p.is_empty()).collect()
}

/// An optionally signed decimal number, written out in full.
fn is_number(s: &str) -> bool {
    let t = s.strip_prefix('-').unwrap_or(s);
    !t.is_empty()
        && t.bytes().all(|b| b.is_ascii_digit() || b == b'.')
        && t.bytes().filter(|b| *b == b'.').count() <= 1
        && t.bytes().any(|b| b.is_ascii_digit())
}

/// Split `<receiver>.name(<args>)` when the whole expression is that one method call.
fn method_call<'a>(e: &'a str, name: &str) -> Option<(&'a str, Vec<&'a str>)> {
    let open = format!(".{name}(");
    let (mut depth, mut at) = (0i32, None);
    for (i, c) in e.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            '.' if depth == 0 && e[i..].starts_with(&open) => at = Some(i),
            _ => {}
        }
    }
    let i = at?;
    let args = call_args(&e[i + 1..], name)?;
    Some((&e[..i], args))
}

/// The sole argument of `name(...)`, when the whole expression is that one call.
fn single_arg<'a>(e: &'a str, name: &str) -> Option<&'a str> {
    let args = call_args(e, name)?;
    (args.len() == 1).then(|| args[0])
}

/// Translate one mapper output expression to SQL, or `None` when it needs a human.
///
/// Only forms with a single faithful SQL reading are translated. Arithmetic, branching
/// and anything whose index base is not established stay reported: guessing one of those
/// wrong produces a silently wrong number instead of a failure.
fn java_expr_to_sql(expr: &str, types: &ColTypes, ports: &PortMap) -> Option<String> {
    let e = expr.trim();
    if e.is_empty() {
        return None;
    }
    if e == "null" {
        return Some("NULL".to_string());
    }
    if e == "BigDecimal.ZERO" {
        return Some("0".to_string());
    }
    if e == "BigDecimal.ONE" {
        return Some("1".to_string());
    }
    if is_number(e) {
        return Some(e.to_string());
    }
    // A choice reads as a CASE, and a chain of them nests.
    if let Some((cond, yes, no)) = split_ternary(e) {
        return Some(format!(
            "CASE WHEN {} THEN {} ELSE {} END",
            java_condition_to_sql(cond, types, ports)?,
            java_expr_to_sql(yes, types, ports)?,
            java_expr_to_sql(no, types, ports)?
        ));
    }
    if let Some(inner) = e.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        // An escape would need Java's rules, so leave those to a human.
        if !inner.contains('"') && !inner.contains('\\') {
            return Some(format!("'{}'", inner.replace('\'', "''")));
        }
    }
    if let Some(inner) = e.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
        if balanced(inner) {
            return java_expr_to_sql(inner, types, ports);
        }
    }
    if let Some(head) = e.strip_suffix(".toString()") {
        return java_expr_to_sql(head, types, ports);
    }
    if let Some(arg) = single_arg(e, "Double.valueOf") {
        return Some(format!("TRY_CAST({} AS DOUBLE)", java_expr_to_sql(arg, types, ports)?));
    }
    if let Some(arg) = single_arg(e, "new BigDecimal") {
        let inner = arg.trim();
        // The double-valued form goes through a double in Java, so DOUBLE is faithful.
        if single_arg(inner, "Double.valueOf").is_some() {
            return java_expr_to_sql(inner, types, ports);
        }
        // Wrapping something already computed is a change of type, not of value: what it
        // wraps decides the number, and the wrapper only says how it is held. A bare
        // column is the exception below - there the wrapper is the only thing that would
        // say which reading was meant, and it does not say enough.
        if inner.contains('(') {
            if let Some(sql) = java_expr_to_sql(inner, types, ports) {
                return Some(format!("CAST({sql} AS DECIMAL(38,4))"));
            }
        }
        // A column whose declared type is a string goes through BigDecimal(String), the
        // exact constructor, so reading it as a fixed-point number is faithful rather
        // than a guess. The file records the type; it was only ever unavailable to a
        // reader that did not look. A double-valued column still goes through binary
        // floating point and is left refused.
        if let Some(kind) = types.get(inner.trim()) {
            if kind.eq_ignore_ascii_case("id_String") {
                return Some(format!("CAST({} AS DECIMAL(38,4))", java_expr_to_sql(inner, types, ports)?));
            }
        }
        // A quoted number is an exact decimal, and the literal already carries its own
        // scale, so writing it through keeps that rather than inventing a cast.
        if let Some(lit) = inner.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
            if is_number(lit) {
                return Some(lit.to_string());
            }
        }
        // `new BigDecimal(reference)` reads as an exact decimal or a double depending on
        // the reference's Java type, which the job file does not record.
        return None;
    }
    if let Some(arg) = single_arg(e, "StringHandling.TRIM") {
        return Some(format!("trim({})", java_expr_to_sql(arg, types, ports)?));
    }
    // The character helpers. SUBSTR takes a start and a length counted from 1, the same
    // as SQL, rather than Java's begin/end: a reference migration of this dialect renders
    // it verbatim as SUBSTR and its output matches the SQL reading on every row. The
    // counts must be plain integers, since a computed one would be a Java expression.
    for (name, sql_fn, arity) in [
        ("StringHandling.LEFT", "left", 2),
        ("StringHandling.RIGHT", "right", 2),
        ("StringHandling.SUBSTR", "substr", 3),
    ] {
        let Some(args) = call_args(e, name) else { continue };
        if args.len() != arity {
            return None;
        }
        let subject = java_expr_to_sql(args[0], types, ports)?;
        // How far to take is as often worked out as written down. Both count from the
        // same place in either language, so a count that is itself an expression reads
        // like any other; refusing those took the whole column with them.
        let counts = args[1..]
            .iter()
            .map(|a| java_expr_to_sql(a, types, ports))
            .collect::<Option<Vec<_>>>()?;
        return Some(format!("{sql_fn}({subject}, {})", counts.join(", ")));
    }
    // Sign-changing arithmetic on an exact decimal, which SQL does the same way.
    if let Some(recv) = e.strip_suffix(".negate()") {
        return Some(format!("-({})", java_expr_to_sql(recv, types, ports)?));
    }
    // `divide` and `setScale` name the scale they round to, so the rounding is not a
    // detail to be inferred from the target column: it is part of the expression.
    if let Some((recv, args)) = method_call(e, "divide") {
        let left = java_expr_to_sql(recv, types, ports)?;
        let right = java_expr_to_sql(args.first()?, types, ports)?;
        // Java throws on a zero divisor rather than returning NULL, but a pipeline that
        // stops on one row of bad data is worse than one that carries a NULL, and NULL
        // is what every other division here already yields.
        let quotient = format!("({left}) / NULLIF({right}, 0)");
        return match args.len() {
            1 => Some(quotient),
            // (divisor, scale, rounding-mode)
            3 if is_number(args[1].trim()) => {
                Some(format!("ROUND({quotient}, {})", args[1].trim()))
            }
            _ => None,
        };
    }
    if let Some((recv, args)) = method_call(e, "setScale") {
        if !args.is_empty() && is_number(args[0].trim()) {
            return Some(format!(
                "ROUND({}, {})",
                java_expr_to_sql(recv, types, ports)?,
                args[0].trim()
            ));
        }
        return None;
    }
    // A value the loop put aside, used inside a larger expression rather than standing
    // alone. Alone it is handled where settings are; here it is one operand among others.
    if let Some(column) = loop_row_column(e) {
        // Quoted, because here it is an operand in SQL and what the loop puts in its
        // place is text. Left bare, `right(FILE_NAME_AS_TEXT, 5)` reads the filled-in
        // name as the name of a COLUMN and the step fails to bind. Standing alone in a
        // path or a file name it is not quoted, and that is handled where settings are.
        return Some(format!("'${{ITER_ITEM_{}}}'", column.to_uppercase()));
    }
    // Dates. The tool writes its formats the Java way and SQL writes them another, so the
    // format is translated too rather than passed through to mean something else.
    if let Some(args) = call_args(e, "TalendDate.getCurrentDate") {
        if args.iter().all(|a| a.trim().is_empty()) {
            return Some("now()".into());
        }
    }
    if let Some(args) = call_args(e, "TalendDate.formatDate") {
        if args.len() == 2 {
            return Some(format!(
                "strftime({}, '{}')",
                java_expr_to_sql(args[1], types, ports)?,
                date_format_to_strftime(args[0])?
            ));
        }
    }
    if let Some(args) = call_args(e, "TalendDate.parseDate") {
        if args.len() == 2 {
            return Some(format!(
                "strptime({}, '{}')",
                java_expr_to_sql(args[1], types, ports)?,
                date_format_to_strftime(args[0])?
            ));
        }
    }
    if let Some(args) = call_args(e, "TalendDate.getDate") {
        if args.len() == 1 {
            return Some(format!("strftime(now(), '{}')", date_format_to_strftime(args[0])?));
        }
    }
    // A counter that starts somewhere and steps. It numbers the rows it is asked about,
    // which is what it does per run; the name it is given is the tool's way of keeping
    // two counters apart and has nothing to answer to here.
    if let Some(args) = call_args(e, "Numeric.sequence") {
        if args.len() == 3 {
            let start = java_expr_to_sql(args[1], types, ports)?;
            let step = java_expr_to_sql(args[2], types, ports)?;
            return Some(format!("({start} + (row_number() OVER () - 1) * {step})"));
        }
    }
    // The routines the tool ships, each with one reading in SQL.
    for (name, sql_fn) in [
        ("StringHandling.LEN", "length"),
        ("StringHandling.UPCASE", "upper"),
        ("StringHandling.DOWNCASE", "lower"),
    ] {
        if let Some(arg) = single_arg(e, name) {
            return Some(format!("{sql_fn}({})", java_expr_to_sql(arg, types, ports)?));
        }
    }
    if let Some(args) = call_args(e, "StringHandling.CHANGE") {
        if args.len() == 3 {
            return Some(format!(
                "regexp_replace({}, {}, {}, 'g')",
                java_expr_to_sql(args[0], types, ports)?,
                java_string_to_sql(args[1])?,
                java_string_to_sql(args[2])?
            ));
        }
    }
    if let Some(args) = call_args(e, "StringHandling.INDEX") {
        // Java counts from zero and answers -1 when absent; instr counts from one and
        // answers 0, so one subtraction says both.
        if args.len() == 2 {
            return Some(format!(
                "instr({}, {}) - 1",
                java_expr_to_sql(args[0], types, ports)?,
                java_expr_to_sql(args[1], types, ports)?
            ));
        }
    }
    if let Some(args) = call_args(e, "StringHandling.COUNT") {
        if args.len() == 2 {
            let subject = java_expr_to_sql(args[0], types, ports)?;
            let needle = java_expr_to_sql(args[1], types, ports)?;
            return Some(format!(
                "(length({subject}) - length(replace({subject}, {needle}, ''))) / \
                 nullif(length({needle}), 0)"
            ));
        }
    }
    // Arithmetic the tool spells as a routine over text.
    for (name, op) in [
        ("Mathematical.SMUL", "*"),
        ("Mathematical.SADD", "+"),
        ("Mathematical.SSUB", "-"),
        ("Mathematical.SDIV", "/"),
    ] {
        let Some(args) = call_args(e, name) else { continue };
        if args.len() != 2 {
            return None;
        }
        let left = java_expr_to_sql(args[0], types, ports)?;
        let right = java_expr_to_sql(args[1], types, ports)?;
        // The routine takes its operands as text and reads them as numbers, so the
        // reading is part of the translation. Tolerant, like the rest of the read.
        let divisor = matches!(op, "/");
        return Some(match divisor {
            true => format!(
                "TRY_CAST({left} AS DOUBLE) / nullif(TRY_CAST({right} AS DOUBLE), 0)"
            ),
            false => format!("TRY_CAST({left} AS DOUBLE) {op} TRY_CAST({right} AS DOUBLE)"),
        });
    }
    // Reading a number out of text, and the wrappers that only change the Java type.
    for (name, ty) in [
        ("Double.parseDouble", "DOUBLE"),
        ("Float.parseFloat", "DOUBLE"),
        ("Integer.parseInt", "BIGINT"),
        ("Long.parseLong", "BIGINT"),
    ] {
        if let Some(arg) = single_arg(e, name) {
            return Some(format!("TRY_CAST({} AS {ty})", java_expr_to_sql(arg, types, ports)?));
        }
    }
    for (suffix, ty) in [
        (".doubleValue()", "DOUBLE"),
        (".floatValue()", "DOUBLE"),
        (".intValue()", "INTEGER"),
        (".longValue()", "BIGINT"),
    ] {
        if let Some(recv) = e.strip_suffix(suffix) {
            return Some(format!("CAST({} AS {ty})", java_expr_to_sql(recv, types, ports)?));
        }
    }
    // Cutting a string on a separator and taking one piece. Java counts the pieces from
    // zero and a SQL list counts from one, so the piece asked for moves by one.
    if e.ends_with(']') {
        if let Some(open) = e.rfind('[') {
            let index: Option<i64> = e[open + 1..e.len() - 1].trim().parse().ok();
            // A piece named by anything but a number cannot be moved by one without
            // knowing what it is, so that is left unread rather than guessed at.
            if let Some(n) = index.filter(|n| *n >= 0) {
                if let Some((recv, args)) = method_call(e[..open].trim(), "split") {
                    if args.len() == 1 {
                        return Some(format!(
                            "str_split({}, {})[{}]",
                            java_expr_to_sql(recv, types, ports)?,
                            java_expr_to_sql(args[0], types, ports)?,
                            n + 1
                        ));
                    }
                }
            }
        }
    }
    // Finding and replacing on a plain string rather than a pattern.
    if let Some((recv, args)) = method_call(e, "indexOf") {
        if args.len() == 1 {
            return Some(format!(
                "instr({}, {}) - 1",
                java_expr_to_sql(recv, types, ports)?,
                java_expr_to_sql(args[0], types, ports)?
            ));
        }
    }
    if let Some((recv, args)) = method_call(e, "lastIndexOf") {
        if args.len() == 1 {
            let subject = java_expr_to_sql(recv, types, ports)?;
            let needle = java_expr_to_sql(args[0], types, ports)?;
            // Found from the back, then counted from the front; absent is -1 either way.
            return Some(format!(
                "CASE WHEN instr(reverse({subject}), reverse({needle})) = 0 THEN -1 ELSE \
                 length({subject}) - instr(reverse({subject}), reverse({needle})) - \
                 length({needle}) + 1 END"
            ));
        }
    }
    if let Some((recv, args)) = method_call(e, "replace") {
        if args.len() == 2 {
            return Some(format!(
                "replace({}, {}, {})",
                java_expr_to_sql(recv, types, ports)?,
                java_expr_to_sql(args[0], types, ports)?,
                java_expr_to_sql(args[1], types, ports)?
            ));
        }
    }
    // The ordinary things a mapper does to a field. Each has one reading in SQL, so
    // leaving them out only meant the whole expression around them went unread.
    for (name, sql_fn) in [
        ("toUpperCase", "upper"),
        ("toLowerCase", "lower"),
        ("length", "length"),
        ("trim", "trim"),
    ] {
        if let Some((recv, args)) = method_call(e, name) {
            // A call with no arguments still yields one, empty.
            if args.iter().all(|a| a.trim().is_empty()) {
                return Some(format!("{}({})", sql_fn, java_expr_to_sql(recv, types, ports)?));
            }
        }
    }
    // Java replaces on a regular expression and replaces every match.
    if let Some((recv, args)) = method_call(e, "replaceAll") {
        if args.len() == 2 {
            return Some(format!(
                "regexp_replace({}, {}, {}, 'g')",
                java_expr_to_sql(recv, types, ports)?,
                java_string_to_sql(args[0])?,
                java_string_to_sql(args[1])?
            ));
        }
    }
    // A static call, not a method on a value.
    if let Some(args) = call_args(e, "StringHandling.EREPLACE") {
        // The pattern is as often a setting as a literal, and either reads.
        if args.len() == 3 {
            return Some(format!(
                "regexp_replace({}, {}, {}, 'g')",
                java_expr_to_sql(args[0], types, ports)?,
                java_expr_to_sql(args[1], types, ports)?,
                java_expr_to_sql(args[2], types, ports)?
            ));
        }
    }
    // Java counts from zero and takes an end position; SQL counts from one and takes a
    // length. Written out rather than folded, so a bound that is itself an expression
    // still reads.
    if let Some((recv, args)) = method_call(e, "substring") {
        let subject = java_expr_to_sql(recv, types, ports)?;
        return match args.len() {
            1 => Some(format!("substr({}, {} + 1)", subject, java_expr_to_sql(args[0], types, ports)?)),
            2 => Some(format!(
                "substr({}, {} + 1, {} - {})",
                subject,
                java_expr_to_sql(args[0], types, ports)?,
                java_expr_to_sql(args[1], types, ports)?,
                java_expr_to_sql(args[0], types, ports)?
            )),
            _ => None,
        };
    }
    for (name, op) in [("multiply", "*"), ("subtract", "-"), ("add", "+")] {
        let Some((recv, args)) = method_call(e, name) else { continue };
        if args.len() != 1 {
            return None;
        }
        let (left, right) = (
            numeric_operand(recv, types, ports)?,
            numeric_operand(args[0], types, ports)?,
        );
        // A file leaves a charge blank when there is none, and adding a blank as UNKNOWN
        // makes the whole total unknown - one blank in a chain of five and the total is
        // gone, which is the figure that gets loaded. Counted as nothing, the total comes
        // out as the job it came from produces it.
        //
        // Multiplying is left alone: a blank there is not a nought, and saying it is
        // would turn a product into zero rather than leaving it unanswered.
        if op == "*" {
            return Some(format!("{left} {op} {right}"));
        }
        return Some(format!("COALESCE({left}, 0) {op} COALESCE({right}, 0)"));
    }
    if let Some(arg) = single_arg(e, "String.valueOf") {
        return Some(format!("CAST({} AS VARCHAR)", java_expr_to_sql(arg, types, ports)?));
    }
    // Java writes joining text and adding numbers the same way. SQL does not, and reads
    // the one written for text as arithmetic on it, which it refuses - so an expression
    // that only glues two fields together stops the whole step. Which was meant is
    // decided from what the pieces are: the file records the type of every column the
    // mapper reads, and a literal or a call that yields text says so itself. Where
    // nothing says, it is left to a person rather than guessed at.
    let parts = split_top_level_plus(e);
    if parts.len() > 1 {
        let textual = parts.iter().any(|p| yields_text(p, types));
        let numeric = parts.iter().all(|p| yields_number(p, types));
        if !textual && !numeric {
            return None;
        }
        let rendered = parts
            .iter()
            .map(|p| match textual {
                true => Some(format!("({})", java_expr_to_sql(p, types, ports)?)),
                false => numeric_operand(p, types, ports),
            })
            .collect::<Option<Vec<_>>>()?;
        let op = if textual { "||" } else { "+" };
        return Some(rendered.join(&format!(" {op} ")));
    }
    // The other signs, loosest first, so what sits either side of the looser one stays
    // together and the nesting comes out with the usual precedence. Only arithmetic is
    // written this way - joining text has its own sign, handled above - so there is
    // nothing to decide between.
    for op in ["-", "*", "/"] {
        let parts = split_top_level(e, op);
        // A leading sign is part of the number, not an operation with nothing on its
        // left; `split_top_level` drops the empty piece, so a single part says so.
        if parts.len() < 2 {
            continue;
        }
        let rendered = parts
            .iter()
            .map(|p| numeric_operand(p, types, ports))
            .collect::<Option<Vec<_>>>()?;
        return Some(rendered.join(&format!(" {op} ")));
    }
    // A context value is written the same way a column is. Read as a column it becomes a
    // reference to one of that name: where none exists the step fails, and where one does
    // it quietly answers with the row's own value instead of the setting.
    if e.starts_with("context.") {
        // Quoted, because it stands where a value stands. The run puts the setting in as
        // text, so unquoted it would read as the name of a column instead.
        return rewrite_context(e).map(|v| format!("'{v}'"));
    }
    // A mapper's own named value, still standing after the pass that puts those values
    // in place, was never defined. Reading it as a column of that name is the silent
    // wrong answer the pass exists to avoid, so it stops here.
    if e.starts_with("Var.") {
        return None;
    }
    // `Table.Column`, the only bare form that reads one way.
    let (table, column) = e.split_once('.')?;
    let ident = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_');
    if !(ident(table) && ident(column)) {
        return None;
    }
    // Where the mapper looks something up there is more than one relation in play and the
    // same column name can sit in either, so the input it came from is kept. With a
    // single input there is nothing to be ambiguous about and the name stands alone.
    Some(match ports.get(table) {
        Some(port) => format!("{port}.{column}"),
        None => column.to_string(),
    })
}

/// Whether a piece of SQL is just a column being named: `COL`, `main.COL`, `"COL"`.
fn is_plain_reference(sql: &str) -> bool {
    let s = sql.trim();
    if s.is_empty() {
        return false;
    }
    let part = s.rsplit_once('.').map(|(_, c)| c).unwrap_or(s);
    let part = part.trim_matches('"');
    !part.is_empty()
        && !part.eq_ignore_ascii_case("NULL")
        && part.chars().all(|c| c.is_alphanumeric() || c == '_')
        && !part.chars().next().is_some_and(|c| c.is_ascii_digit())
}

/// An output as the type the mapper says it is.
///
/// A delimited file arrives as text, so a column passed straight through arrives as text
/// too - and the next step that multiplies it has a number on one side and text on the
/// other. The mapper says what each of its outputs is, so it says so here rather than
/// leaving every later step to work it out again. Tolerant, like the rest of the read: a
/// value that will not parse becomes NULL rather than ending the run.
fn as_declared(sql: &str, declared: &str, width: Option<(u32, u32)>) -> String {
    // The SCALE the schema declares, and never its width. A rate declared with 9 decimal
    // places rounded to 4 is a different number, and money - so the scale is taken. The
    // width is NOT: the tool holds these as arbitrary-precision decimals and the declared
    // width is what the DATABASE column is, which real values routinely exceed. Held to
    // it, a 16-digit charge overflows a 13-digit column, and a cast that overflows gives
    // NULL - so the charge disappears, and every total built from it disappears too.
    // Never deeper than the default, which is what keeps this safe to take at all.
    // DuckDB's decimal stops at 38 digits and a product needs both scales, so a value
    // held at 9 decimal places overflows on the first multiplication and ends the run -
    // measured on a real job. A scale no deeper than the default cannot add an overflow
    // that was not already there, and it fixes the far more common case: a whole number
    // declared with no decimal places was arriving as 20251110.0000.
    //
    // The cost is that a rate declared with 9 places is still rounded to 4. That is a
    // real difference on money and it is not fixed here: it needs the arithmetic to stop
    // being fixed-point, which is a bigger decision than this.
    let decimal = match width {
        Some((_, scale)) if scale <= 4 => format!("DECIMAL(38,{scale})"),
        _ => "DECIMAL(38,4)".to_string(),
    };
    let ty = match declared {
        "id_BigDecimal" => decimal.as_str(),
        "id_Double" | "id_Float" => "DOUBLE",
        "id_Integer" | "id_Long" | "id_Short" => "BIGINT",
        _ => return sql.to_string(),
    };
    // Already said, or a literal that needs no saying.
    if sql.contains(&format!("AS {ty})")) || is_number(sql) {
        return sql.to_string();
    }
    // A column passed straight through is left exactly as it is. A file arrives as text
    // and leaves as text, arithmetic casts its own operands where it happens, and
    // retyping here only destroys whatever will not parse - on a real file the later
    // record types carry a different layout, so a place name sits in a column the first
    // layout calls a charge, and casting turns it into NULL for good.
    if is_plain_reference(sql) {
        return sql.to_string();
    }
    format!("TRY_CAST({sql} AS {ty})")
}

/// Put the value a mapper's named intermediate stands for in place of the name.
///
/// Textual, and in order, which is how the mapper computes them: each one may use the
/// ones before it. A name with no definition is left alone so it is reported as the
/// unreadable expression it is rather than quietly becoming a column reference.
fn inline_mapper_vars(expr: &str, vars: &[(String, String)]) -> String {
    if !expr.contains("Var.") {
        return expr.to_string();
    }
    let mut out = expr.to_string();
    // Later definitions first: a name can be a prefix of a longer one.
    for (name, body) in vars.iter().rev() {
        let needle = format!("Var.{name}");
        if !out.contains(&needle) {
            continue;
        }
        let mut rebuilt = String::with_capacity(out.len());
        let mut rest = out.as_str();
        while let Some(at) = rest.find(&needle) {
            let after = &rest[at + needle.len()..];
            // `Var.AB` must not match inside `Var.ABC`.
            let bounded = !after.starts_with(|c: char| c.is_alphanumeric() || c == '_');
            rebuilt.push_str(&rest[..at]);
            if bounded {
                rebuilt.push('(');
                rebuilt.push_str(body);
                rebuilt.push(')');
            } else {
                rebuilt.push_str(&needle);
            }
            rest = after;
        }
        rebuilt.push_str(rest);
        out = rebuilt;
    }
    out
}

/// Translate mapper output expressions. Anything without one faithful SQL reading is
/// reported rather than guessed at.
fn mapper_expressions(raw: &RawNode, job: &str, warnings: &mut Vec<Warning>) -> JsonValue {
    mapper_expressions_of(raw, &raw.mapper_out, job, warnings)
}

/// Translate one output's expressions. Anything without one faithful SQL reading is
/// reported rather than guessed at.
/// A mapper's own named values, resolved in the order it computes them, and which of its
/// inputs each name belongs to.
///
/// A name that stands for another name resolves too. With one input a name is
/// unambiguous; with more, it is kept qualified, because the same column can sit in
/// either relation.
fn mapper_vars_and_ports(raw: &RawNode) -> (Vec<(String, String)>, PortMap) {
    let ports: PortMap = match raw.mapper_inputs.len() > 1 {
        false => Default::default(),
        true => raw
            .mapper_inputs
            .iter()
            .enumerate()
            .map(|(at, t)| {
                let port = if at == 0 { "main".to_string() } else { format!("lookup_{at}") };
                (t.name.clone(), port)
            })
            .collect(),
    };
    let mut vars: Vec<(String, String)> = Vec::new();
    for (name, body) in &raw.mapper_vars {
        let resolved = inline_mapper_vars(body, &vars);
        vars.push((name.clone(), resolved));
    }
    (vars, ports)
}

/// The condition on one of a mapper's outputs, as SQL.
///
/// None means either that the output has no condition or that this one could not be
/// read. The two are not the same and the second is reported, because an unread
/// condition lets through every row it was there to hold back.
fn mapper_filter(raw: &RawNode, output: &str, warnings: &mut Vec<Warning>) -> Option<String> {
    let cond = raw.mapper_out_filters.iter().find(|(n, _)| n == output).map(|(_, c)| c)?;
    let (vars, ports) = mapper_vars_and_ports(raw);
    let c = inline_mapper_vars(cond.trim(), &vars);
    let c = c.trim();
    match java_condition_to_sql(c, &raw.mapper_types, &ports) {
        Some(sql) => Some(sql),
        None => {
            warnings.push(Warning::JavaExpression {
                node: raw.unique.clone(),
                column: format!("the condition on output {output}"),
                expression: c.to_string(),
            });
            None
        }
    }
}

fn mapper_expressions_of(
    raw: &RawNode,
    entries: &[(String, String, String)],
    job: &str,
    warnings: &mut Vec<Warning>,
) -> JsonValue {
    let mut out = JsonMap::new();
    let (vars, ports) = mapper_vars_and_ports(raw);
    for (col, expr, declared) in entries {
        // The job's own name is a value the tool supplies, and it is this one.
        let e = inline_mapper_vars(expr.trim(), &vars).replace("jobName", &format!("\"{job}\""));
        let e = e.trim();
        // An output can name a column and give it nothing to compute: the row carries the
        // column, and the column carries nothing. Leaving it out changes the shape the
        // mapper hands on, so every later step that names it fails to bind - a whole
        // branch lost for a column that was only ever empty. Nothing to compute is not a
        // reading we failed to find, so it is not reported either.
        if e.is_empty() || e.chars().all(|c| matches!(c, '(' | ')' | ' ')) {
            out.insert(
                col.clone(),
                JsonValue::String(as_declared("NULL", declared, raw.column_scale.get(col).copied())),
            );
            continue;
        }
        match java_expr_to_sql(e, &raw.mapper_types, &ports) {
            Some(c) => {
                out.insert(
                    col.clone(),
                    JsonValue::String(as_declared(&c, declared, raw.column_scale.get(col).copied())),
                );
            }
            None => warnings.push(Warning::JavaExpression {
                node: raw.unique.clone(),
                column: col.clone(),
                expression: e.to_string(),
            }),
        }
    }
    JsonValue::Object(out)
}

/// Read one Talend `.item` file.
pub fn import_item(xml: &str, job_name: &str) -> Result<Import, String> {
    let (raw_nodes, connections, context, subjob_heads) = parse(xml)?;

    let mut components: BTreeMap<String, usize> = BTreeMap::new();
    for n in &raw_nodes {
        *components.entry(n.component.clone()).or_default() += 1;
    }

    let mut warnings = Vec::new();
    let mut nodes = Vec::new();

    for raw in &raw_nodes {
        // A repository connection means the credentials are not in this file.
        if raw.params.get("PROPERTY:PROPERTY_TYPE").map(|s| s.as_str()) == Some("REPOSITORY") {
            warnings.push(Warning::RepositoryConnection {
                node: raw.unique.clone(),
                component: raw.component.clone(),
            });
        }

        let (component_id, flow_type) = match map_component(raw) {
            Some(hit) => hit,
            None => {
                warnings.push(Warning::UnmappedComponent {
                    node: raw.unique.clone(),
                    component: raw.component.clone(),
                });
                // Import it as a labelled placeholder so the shape of the job
                // survives and the gap is visible on the canvas.
                nodes.push(PipelineNode {
                    id: raw.unique.clone(),
                    flow_type: Some("transform".into()),
                    position: Position { x: raw.x, y: raw.y },
                    data: node_data(format!("{} (unmapped)", raw.component), None, None),
                });
                continue;
            }
        };

        let mut props = properties_for(raw, component_id, &context, &mut warnings);
        if component_id == "xf.map" {
            props.insert("expressions".into(), mapper_expressions(raw, job_name, &mut warnings));
            // With several outputs the node is split into one per output further on, and
            // each takes its own condition there. Setting one here would put the first
            // output's condition on all of them.
            if raw.mapper_outs.len() <= 1 {
                if let Some(f) = raw
                    .mapper_outs
                    .first()
                    .and_then(|(name, _)| mapper_filter(raw, name, &mut warnings))
                {
                    props.insert("filter".into(), JsonValue::String(f));
                }
            }
            // What the mapper looks up, and what it matches on. Wired in but not joined,
            // every column taken from a lookup refers to something that is not there.
            let joins: Vec<JsonValue> = raw
                .mapper_inputs
                .iter()
                .enumerate()
                .skip(1)
                .filter(|(_, t)| !t.keys.is_empty())
                .map(|(at, t)| {
                    // The main side of a match is an expression like any other - often a
                    // plain column, sometimes a trimmed one - so it is translated rather
                    // than cut out of the text. Cutting left the bracket of a call stuck
                    // to the column name, and matching on the column instead of the
                    // trimmed column is a different match.
                    let ports: PortMap = raw
                        .mapper_inputs
                        .iter()
                        .enumerate()
                        .map(|(at, t)| {
                            let port =
                                if at == 0 { "main".to_string() } else { format!("lookup_{at}") };
                            (t.name.clone(), port)
                        })
                        .collect();
                    // A key that cannot be read is left out rather than approximated.
                    // Falling back to whatever followed the last dot turned
                    // `row1.File_Name.split("_")[3]` into `split("_")[3]`: the column
                    // being split gone, the separator now a quoted NAME. A match decides
                    // which rows pair up, so an approximate one is a wrong answer that
                    // still runs. Left out, the job refuses to compile and says why.
                    let mut left: Vec<String> = Vec::new();
                    let mut unreadable: Vec<String> = Vec::new();
                    for (_, expr) in &t.keys {
                        match java_expr_to_sql(expr.trim(), &raw.mapper_types, &ports) {
                            Some(sql) => left.push(sql),
                            None => unreadable.push(expr.trim().to_string()),
                        }
                    }
                    let right: Vec<String> =
                        t.keys.iter().map(|(col, _)| col.clone()).collect();
                    (
                        at,
                        t.inner,
                        if unreadable.is_empty() { left.join(",") } else { String::new() },
                        right.join(","),
                        unreadable,
                    )
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|(at, inner, left, right, unreadable)| {
                    for expression in unreadable {
                        warnings.push(Warning::JavaExpression {
                            node: raw.unique.clone(),
                            column: format!("the key matching lookup_{at}"),
                            expression,
                        });
                    }
                    serde_json::json!({
                        "port": format!("lookup_{at}"),
                        "leftKey": left,
                        "rightKey": right,
                        "joinType": if inner { "inner" } else { "left" },
                    })
                })
                .collect();
            if !joins.is_empty() {
                props.insert("lookups".into(), JsonValue::Array(joins));
            }
        }

        let mut data = node_data(
            raw.unique.clone(),
            Some(component_id.into()),
            Some(JsonValue::Object(props)),
        );
        data.schema = declared_schema(raw, component_id);

        nodes.push(PipelineNode {
            id: raw.unique.clone(),
            flow_type: Some(flow_type.into()),
            position: Position { x: raw.x, y: raw.y },
            data,
        });
    }

    let known: std::collections::HashSet<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    // A join takes a second row-carrying input on its LOOKUP port, not a second
    // main. Sending both to `main` reads as two upstreams feeding one input,
    // which the planner refuses. Only the first row link into a node is main;
    // the rest are lookups, in the order the file lists them.
    let mut seen_main: std::collections::HashSet<&str> = Default::default();
    let mut target_port: std::collections::HashMap<usize, String> = Default::default();
    for (i, c) in connections.iter().enumerate() {
        if connection_type_for(c.connector.as_deref()) != "main" {
            continue;
        }
        if !known.contains(c.source.as_str()) || !known.contains(c.target.as_str()) {
            continue;
        }
        // A mapper says which of its inputs carries the rows and which are looked up, by
        // name, so the link is put on the port the file gives it. Numbering matters: two
        // lookups sharing one port means the second replaces the first.
        let named = raw_nodes
            .iter()
            .find(|r| r.unique == c.target && r.mapper_inputs.len() > 1)
            .and_then(|r| {
                let label = c.label.as_deref()?.trim();
                let at = r.mapper_inputs.iter().position(|t| t.name == label)?;
                Some(match at {
                    0 => "main".to_string(),
                    n => format!("lookup_{n}"),
                })
            });
        let port = match named {
            Some(p) => {
                if p == "main" {
                    seen_main.insert(c.target.as_str());
                }
                p
            }
            None if seen_main.insert(c.target.as_str()) => "main".to_string(),
            None => "lookup".to_string(),
        };
        target_port.insert(i, port);
    }
    let edges: Vec<PipelineEdge> = connections
        .iter()
        .enumerate()
        // Drop dangling links rather than emit an edge to a node that is not
        // in the file; a dangling edge fails to compile with a worse message.
        .filter(|(_, c)| known.contains(c.source.as_str()) && known.contains(c.target.as_str()))
        .map(|(i, c)| PipelineEdge {
            id: format!("e{}", i + 1),
            source: c.source.clone(),
            target: c.target.clone(),
            source_handle: Some("main".into()),
            target_handle: Some(
                target_port.get(&i).cloned().unwrap_or_else(|| "main".to_string()),
            ),
            edge_type: None,
            data: Some(EdgeData {
                connection_type: connection_type_for(c.connector.as_deref()).into(),
                // A component with several outputs names each port, and every one of them
                // is row-carrying, so every one types as main. Keep the name or three
                // different outputs arrive as three identical edges.
                label: c.connector.as_deref().and_then(named_port),
                condition: None,
            }),
        })
        .collect();

    let (nodes, edges) =
        split_multi_output_mappers(nodes, edges, &raw_nodes, &connections, job_name, &mut warnings);
    let (nodes, edges) = set_context_at_run_time(nodes, edges, &raw_nodes, &mut warnings);
    let (nodes, edges) = read_loop_rows_from_their_list(nodes, edges, &raw_nodes);
    let edges = rewire_parallel_joins(edges, &connections);
    let edges = anchor_subjob_links_at_their_end(edges);
    let edges = chain_declared_subjobs(edges, &subjob_heads, &connections);

    // A child pipeline is handed no rows and returns none, so a row link leaving a call
    // is a data path the source tool had and this one does not.
    let mut wants_rows: Vec<String> = Vec::new();
    for n in &nodes {
        if n.data.component_id.as_deref() != Some("ctl.runjob") {
            continue;
        }
        let returns_rows = edges.iter().any(|e| {
            e.source == n.id
                && e.data.as_ref().map(|d| d.connection_type.as_str()) == Some("main")
        });
        if returns_rows {
            wants_rows.push(n.id.clone());
        }
    }

    let mut nodes = nodes;
    for id in &wants_rows {
        if let Some(n) = nodes.iter_mut().find(|n| &n.id == id) {
            let props = n.data.properties.get_or_insert_with(|| JsonValue::Object(JsonMap::new()));
            if let Some(map) = props.as_object_mut() {
                map.insert("returnsRows".into(), JsonValue::Bool(true));
            }
        }
    }

    // A loop's body is inline in the source job; Duckle runs a child pipeline by
    // reference, so lift each body out and point its loop at the new file.
    let mut edges = edges;
    let mut warnings = warnings;
    let children = extract_loop_bodies(job_name, &mut nodes, &mut edges, &mut warnings);

    Ok(Import {
        name: job_name.to_string(),
        nodes,
        edges,
        warnings,
        components,
        children,
    })
}

/// `NodeData` has no `Default`, and an imported node only ever sets three of
/// its fields, so build it in one place rather than spelling out the rest twice.
fn node_data(label: String, component_id: Option<String>, properties: Option<JsonValue>) -> NodeData {
    NodeData {
        label,
        subtitle: None,
        component_id,
        properties,
        schema: None,
        sample_rows: None,
        disabled: None,
        alias: None,
    }
}

struct Conn {
    source: String,
    target: String,
    /// Talend's `connectorName`. It says whether a link carries rows or only
    /// ordering, and dropping it turned every trigger into a data dependency.
    connector: Option<String>,
    /// The connection's own name, which for a mapper output is the output it
    /// carries. Distinct from the connector kind.
    label: Option<String>,
}

/// Lift each loop's body out into a pipeline of its own and point the loop at it.
///
/// A legacy job expresses a loop by hanging its body off an iterate link inside
/// the same job. Duckle's loop components run a child pipeline by reference, so
/// an imported loop had no body to run and refused to compile for want of one.
///
/// Only a body that belongs solely to the loop is lifted. If any node in it is
/// also fed from outside the loop, moving it would silently cut the main flow,
/// so the loop is left alone and the job says so instead. Extracting the wrong
/// subgraph is worse than not extracting it.
/// Which nodes make up one loop's body.
///
/// The single definition of it. An ordering link must not cross a body's boundary, or the
/// body stops being liftable and the loop is left naming a file nobody wrote - and the
/// pass that adds those links and the pass that lifts the body have to agree about where
/// the boundary is. Working it out twice is how they came to disagree: a body also draws
/// in the sources its joins read, and a walk that only follows the flow forwards misses
/// them.
///
/// Empty when the loop has no body to speak of.
fn loop_body_members(
    loop_id: &str,
    edges: &[PipelineEdge],
) -> std::collections::HashSet<String> {
    // The body starts at whatever the loop's iterate link points to.
    let entries: Vec<String> = edges
        .iter()
        .filter(|e| {
            e.source == loop_id
                && e.data.as_ref().map(|d| d.connection_type.as_str()) == Some("iterate")
        })
        .map(|e| e.target.clone())
        .collect();
    let mut body: std::collections::HashSet<String> = Default::default();
    if entries.is_empty() {
        return body;
    }

    // Everything reachable from those entries, not passing back through the loop itself.
    let mut queue = entries;
    while let Some(id) = queue.pop() {
        if id == loop_id || !body.insert(id.clone()) {
            continue;
        }
        for e in edges.iter().filter(|e| e.source == id) {
            if e.target != loop_id {
                queue.push(e.target.clone());
            }
        }
    }

    // A join inside the loop reads its reference table from a source that sits outside
    // it. That source is part of the body's work, not of the main flow, so pull it in -
    // along with whatever feeds it - rather than refusing a loop whose only sin is having
    // a lookup.
    loop {
        let pull: Vec<String> = edges
            .iter()
            .filter(|e| body.contains(&e.target) && e.source != loop_id)
            .filter(|e| !body.contains(&e.source))
            .filter(|e| {
                // A mapper numbers its lookup ports, so the name is a prefix rather than
                // the whole of it. Matching the bare word missed every numbered one, and
                // a body that could not draw in the sources its joins read stopped being
                // liftable at all.
                e.data.as_ref().map(|d| d.connection_type.as_str()) == Some("lookup")
                    || e.target_handle.as_deref().is_some_and(|h| h.starts_with("lookup"))
            })
            .map(|e| e.source.clone())
            .collect();
        // Only if nothing outside the body still reads it: moving a source the main flow
        // also uses would cut the parent.
        let safe: Vec<String> = pull
            .into_iter()
            .filter(|src| {
                !edges
                    .iter()
                    .any(|e| e.source == *src && !body.contains(&e.target) && e.target != loop_id)
            })
            .collect();
        if safe.is_empty() {
            break;
        }
        for id in safe {
            let mut q = vec![id];
            while let Some(x) = q.pop() {
                if x == loop_id || !body.insert(x.clone()) {
                    continue;
                }
                for e in edges.iter().filter(|e| e.target == x) {
                    q.push(e.source.clone());
                }
            }
        }
    }
    body
}

/// The loops in a graph, in the order their nodes appear.
fn loop_nodes(nodes: &[PipelineNode]) -> Vec<String> {
    nodes
        .iter()
        .filter(|n| {
            matches!(
                n.data.component_id.as_deref(),
                Some("ctl.foreach") | Some("ctl.iterate")
            )
        })
        .map(|n| n.id.clone())
        .collect()
}

fn extract_loop_bodies(
    parent_name: &str,
    nodes: &mut Vec<PipelineNode>,
    edges: &mut Vec<PipelineEdge>,
    warnings: &mut Vec<Warning>,
) -> Vec<Import> {
    let mut children = Vec::new();
    for loop_id in loop_nodes(nodes) {
        let body = loop_body_members(&loop_id, edges);
        if body.is_empty() {
            continue;
        }

        // Refuse if anything in the body is still fed from outside it: that is a
        // step the main flow shares, and moving it would cut the parent.
        let fed_from_outside = edges.iter().any(|e| {
            body.contains(&e.target) && e.source != loop_id && !body.contains(&e.source)
        });
        if fed_from_outside {
            warnings.push(Warning::RepositoryConnection {
                node: loop_id.clone(),
                component: "loop body shared with the main flow".into(),
            });
            continue;
        }

        let child_name = format!("{}__{}", parent_name, loop_id);
        let mut child_nodes: Vec<PipelineNode> = nodes
            .iter()
            .filter(|n| body.contains(&n.id))
            .cloned()
            .collect();
        let mut child_edges: Vec<PipelineEdge> = edges
            .iter()
            .filter(|e| body.contains(&e.source) && body.contains(&e.target))
            .cloned()
            .collect();

        // A body can contain a loop of its own. Lift those too, and hand them
        // back alongside this one: names carry their whole ancestry, so they
        // stay distinct however deeply they nest.
        let nested = extract_loop_bodies(&child_name, &mut child_nodes, &mut child_edges, warnings);

        // The parent keeps the loop and loses the body.
        nodes.retain(|n| !body.contains(&n.id));
        edges.retain(|e| !body.contains(&e.source) && !body.contains(&e.target));

        // Point the loop at the file the body is about to become.
        if let Some(l) = nodes.iter_mut().find(|n| n.id == loop_id) {
            let props = l
                .data
                .properties
                .get_or_insert_with(|| JsonValue::Object(Default::default()));
            if let Some(map) = props.as_object_mut() {
                map.insert(
                    "pipelineRef".into(),
                    JsonValue::String(format!("{}.json", child_name)),
                );
            }
        }

        children.extend(nested);
        children.push(Import {
            name: child_name,
            nodes: child_nodes,
            edges: child_edges,
            warnings: Vec::new(),
            components: BTreeMap::new(),
            children: Vec::new(),
        });
    }
    children
}

/// Talend's connector name mapped to the edge vocabulary the canvas draws.
///
/// Talend links are not all data: most of them order the job rather than feed
/// it. Importing them all as `main` asserted a data dependency that the job
/// never had, which is both wrong on the canvas and wrong to the planner. The
/// row-carrying names become `main`; the rest keep their own meaning.
///
/// PARALLELIZE and SYNCHRONIZE have no exact counterpart. Both mean "after
/// this", so they import as `on-subjob-ok`: the ordering survives and the
/// parallelism does not, which is the honest half to keep. An unrecognised
/// name stays `main` rather than becoming a trigger nobody asked for.
/// The port's own name, when the connector carries one rather than a link type.
///
/// `FLOW` and `MAIN` are the ordinary single output and say nothing worth showing. The
/// rest of the recognised names become a connection type, which already carries their
/// meaning. A name like `OUTPUT_2` or `UNIQUE` is the one thing telling two row links out
/// of the same component apart.
fn named_port(connector: &str) -> Option<String> {
    let upper = connector.to_ascii_uppercase();
    let is_link_type = matches!(
        upper.as_str(),
        "FLOW"
            | "MAIN"
            | "ITERATE"
            | "RUN_IF"
            | "SUBJOB_OK"
            | "SUBJOB_ERROR"
            | "COMPONENT_OK"
            | "COMPONENT_ERROR"
            | "PARALLELIZE"
            | "SYNCHRONIZE"
    );
    (!is_link_type && !connector.trim().is_empty()).then(|| connector.to_string())
}

/// Give a mapper that writes several outputs one relation per output.
///
/// The outputs of a mapper are usually the branches of a decision - inbound and outbound,
/// one record type and the rest - and they routinely give the same column name different
/// expressions. Read as one set they overwrite each other and every reader downstream
/// gets whichever was parsed last, which is not a near miss: it is the other branch's
/// number, on a branch that still runs and still looks right.
///
/// The link leaving a mapper names the output it carries, so each one is wired to the
/// relation it actually reads. A link that names nothing recognisable stays on the first
/// output and is reported, since guessing which branch it meant is the whole problem.
/// Carry a Java body that only sets context values over to nodes that set them.
///
/// A job routinely works out a value in Java - the date on the batch it just read, a
/// code it just looked up - and later steps read it by name. There is a component for
/// exactly that, so such a body is carried over instead of being left for someone to
/// rewrite.
///
/// All of the body or none of it. A body with one statement that cannot be read stays
/// exactly as it was, because carrying half of it over leaves something that looks
/// finished and is not.
fn set_context_at_run_time(
    mut nodes: Vec<PipelineNode>,
    mut edges: Vec<PipelineEdge>,
    raw_nodes: &[RawNode],
    warnings: &mut Vec<Warning>,
) -> (Vec<PipelineNode>, Vec<PipelineEdge>) {
    for raw in raw_nodes.iter().filter(|r| r.component.starts_with("tJava")) {
        let Some(code) = raw.params.get("CODE").map(|c| unquote(c)) else { continue };
        let Some(pairs) = context_assignments(&code) else { continue };
        // Every value has to be readable as SQL, or the body is left alone.
        let mut translated: Vec<(String, String)> = Vec::new();
        for (name, value) in &pairs {
            match java_expr_to_sql(value, &Default::default(), &Default::default()) {
                Some(sql) => translated.push((name.clone(), sql)),
                None => {
                    translated.clear();
                    break;
                }
            }
        }
        if translated.is_empty() {
            continue;
        }
        let Some(at) = nodes.iter().position(|n| n.id == raw.unique) else { continue };
        let original = nodes[at].clone();

        let mut made: Vec<String> = Vec::new();
        for (offset, (name, value)) in translated.iter().enumerate() {
            let mut copy = original.clone();
            copy.id = format!("{}__{}", raw.unique, name);
            copy.data.label = copy.id.clone();
            copy.data.component_id = Some("ctl.setvar".into());
            copy.data.properties = Some(serde_json::json!({
                "name": name,
                "value": value,
            }));
            copy.position.y += 70.0 * offset as f64;
            made.push(copy.id.clone());
            nodes.push(copy);
        }
        nodes.remove(at);

        // What came in reaches the first, what left carries on from the last, and they
        // run in the order the body set them - a value can be built from an earlier one.
        let first = made[0].clone();
        let last = made[made.len() - 1].clone();
        for e in edges.iter_mut() {
            if e.target == raw.unique {
                e.target = first.clone();
            }
            if e.source == raw.unique {
                e.source = last.clone();
            }
        }
        for pair in made.windows(2) {
            edges.push(PipelineEdge {
                id: format!("{}__{}-to-{}", raw.unique, pair[0], pair[1]),
                source: pair[0].clone(),
                target: pair[1].clone(),
                source_handle: Some("main".into()),
                target_handle: Some("main".into()),
                edge_type: None,
                data: Some(EdgeData {
                    connection_type: "main".into(),
                    label: None,
                    condition: None,
                }),
            });
        }

        // It no longer has to be ported by hand, so it no longer says so.
        warnings.retain(|w| !matches!(w, Warning::JavaBody { node, .. } if node == &raw.unique));
        // A body that reads the row it is given ran once per row, so the last row
        // decided what the value ended up as. A node sets it once, from the first row.
        // The same thing for a single row, which is what these bodies are usually fed,
        // and a different thing for more - so it is said rather than assumed.
        let from_rows: Vec<String> = pairs
            .iter()
            .filter(|(_, java)| java.contains("input_row."))
            .map(|(name, _)| name.clone())
            .collect();
        if !from_rows.is_empty() {
            warnings.push(Warning::ContextSetFromFirstRow {
                node: raw.unique.clone(),
                names: from_rows,
            });
        }
    }
    (nodes, edges)
}

fn split_multi_output_mappers(
    mut nodes: Vec<PipelineNode>,
    mut edges: Vec<PipelineEdge>,
    raw_nodes: &[RawNode],
    connections: &[Conn],
    job: &str,
    warnings: &mut Vec<Warning>,
) -> (Vec<PipelineNode>, Vec<PipelineEdge>) {
    // The name a link carries is not the port it leaves by: a joblet's boundary is found
    // by the port, while which output a mapper link carries is the link's own name. Both
    // are on the connection, and reading one for the other loses the link entirely.
    let flow_name: std::collections::BTreeMap<(&str, &str), &str> = connections
        .iter()
        .filter_map(|c| {
            let l = c.label.as_deref()?.trim();
            (!l.is_empty()).then_some(((c.source.as_str(), c.target.as_str()), l))
        })
        .collect();
    for raw in raw_nodes.iter().filter(|r| r.mapper_outs.len() > 1) {
        let Some(at) = nodes.iter().position(|n| n.id == raw.unique) else { continue };
        let original = nodes[at].clone();

        // One node per output, each reading whatever the mapper read.
        let mut made: Vec<(String, String)> = Vec::new();
        for (name, entries) in &raw.mapper_outs {
            let mut copy = original.clone();
            copy.id = format!("{}__{}", raw.unique, name);
            copy.data.label = copy.id.clone();
            if let Some(props) = copy.data.properties.as_mut().and_then(|p| p.as_object_mut()) {
                props.insert(
                    "expressions".into(),
                    mapper_expressions_of(raw, entries, job, warnings),
                );
                match mapper_filter(raw, name, warnings) {
                    Some(f) => props.insert("filter".into(), JsonValue::String(f)),
                    None => props.remove("filter"),
                };
            }
            copy.position.y += 70.0 * made.len() as f64;
            made.push((name.clone(), copy.id.clone()));
            nodes.push(copy);
        }
        nodes.remove(at);

        let first = made[0].1.clone();
        let mut extra: Vec<PipelineEdge> = Vec::new();
        for e in edges.iter_mut() {
            if e.target == raw.unique {
                // Every output reads the same input, so the link is copied to each.
                for (_, id) in made.iter().skip(1) {
                    let mut copy = e.clone();
                    copy.id = format!("{}__{}", e.id, id);
                    copy.target = id.clone();
                    extra.push(copy);
                }
                e.target = first.clone();
            } else if e.source == raw.unique {
                let named = flow_name
                    .get(&(raw.unique.as_str(), e.target.as_str()))
                    .and_then(|l| made.iter().find(|(name, _)| name == l));
                match named {
                    Some((_, id)) => e.source = id.clone(),
                    None => {
                        warnings.push(Warning::MapperOutputUnnamed {
                            node: raw.unique.clone(),
                            target: e.target.clone(),
                            outputs: made.iter().map(|(n, _)| n.clone()).collect(),
                        });
                        e.source = first.clone();
                    }
                }
            }
        }
        edges.extend(extra);
    }
    (nodes, edges)
}

/// The column/value rows a row-producing component was given.
///
/// The fixed-row component calls the table VALUES and the loop-row one calls it MAPPING;
/// they hold the same thing, and reading only one name leaves the other with no columns.
fn row_value_pairs(raw: &RawNode) -> impl Iterator<Item = (&String, &String)> {
    ["VALUES", "MAPPING"]
        .into_iter()
        .filter_map(|k| raw.tables.get(k))
        .flatten()
        .filter_map(|row| Some((row.get("SCHEMA_COLUMN")?, row.get("VALUE")?)))
}

/// Read the row a file loop hands on from the list it loops.
///
/// Iterating a folder and turning the current file into a row is the most ordinary batch
/// shape there is. The row is described in terms of the loop's own variables - the current
/// path, the current name - which are not values this side of the move: read literally the
/// node produces no columns at all, and the step that reads one fails on a name that is
/// not there.
///
/// The list already yields a row per file, so the names are taken from it and the list
/// feeds the node. A row built from anything else is left as it was: it is a fixed row,
/// and its values are its own.
fn read_loop_rows_from_their_list(
    mut nodes: Vec<PipelineNode>,
    mut edges: Vec<PipelineEdge>,
    raw_nodes: &[RawNode],
) -> (Vec<PipelineNode>, Vec<PipelineEdge>) {
    for raw in raw_nodes.iter().filter(|r| r.component == "tIterateToFlow") {
        let mut source: Option<String> = None;
        let mut columns = JsonMap::new();
        let mut all_from_loop = true;
        for (name, value) in row_value_pairs(raw) {
            match loop_variable(value) {
                Some((list, part)) if source.as_deref().unwrap_or(&list) == list => {
                    source = Some(list);
                    columns.insert(name.trim().to_string(), JsonValue::String(part.into()));
                }
                _ => {
                    all_from_loop = false;
                    break;
                }
            }
        }
        let (Some(list), true) = (source, all_from_loop && !columns.is_empty()) else {
            continue;
        };
        let Some(node) = nodes.iter_mut().find(|n| n.id == raw.unique) else { continue };
        node.data.component_id = Some("xf.map".into());
        node.data.properties = Some(JsonValue::Object({
            let mut p = JsonMap::new();
            p.insert("expressions".into(), JsonValue::Object(columns));
            p
        }));

        // The loop link says which list it walks; the rows come the same way.
        // The loop link is already there and says which list it walks; what is missing
        // is the rows, which is a different kind of link between the same two nodes.
        if !edges.iter().any(|e| {
            e.source == list
                && e.target == raw.unique
                && e.data.as_ref().map(|d| d.connection_type.as_str()) == Some("main")
        }) {
            edges.push(PipelineEdge {
                id: format!("rows-{list}-{}", raw.unique),
                source: list,
                target: raw.unique.clone(),
                source_handle: Some("main".into()),
                target_handle: Some("main".into()),
                edge_type: None,
                data: Some(EdgeData {
                    connection_type: "main".into(),
                    label: None,
                    condition: None,
                }),
            });
        }
    }
    (nodes, edges)
}

/// `globalMap.get("<list>_CURRENT_FILE")` -> the list and the column that answers it.
fn loop_variable(value: &str) -> Option<(String, &'static str)> {
    let inner = value.split("globalMap.get(").nth(1)?;
    let key = inner.trim_start().trim_start_matches('"');
    let key = key.split('"').next()?;
    // The list yields the path and the name; the folder is the path without the name.
    // Its paths come back with forward slashes whatever the platform, so one separator
    // is enough to find where the name starts.
    for (suffix, column) in [
        ("_CURRENT_FILEPATH", "file"),
        ("_CURRENT_FILEDIRECTORY", "regexp_replace(file, '[^/]+$', '')"),
        ("_CURRENT_FILE", "filename"),
    ] {
        if let Some(list) = key.strip_suffix(suffix) {
            if !list.is_empty() {
                return Some((list.to_string(), column));
            }
        }
    }
    None
}

/// Whether adding `source -> target` would let something reach round to itself.
///
/// Checked per link rather than per pair of subjobs, because the link is drawn from the
/// END of a subjob: its head can sit safely behind the next subjob while something
/// further down it is already downstream of that subjob. A pipeline with a loop in it
/// cannot be ordered at all and is refused whole, so one ordering is never worth the job.
fn would_cycle(edges: &[PipelineEdge], source: &str, target: &str) -> bool {
    if source == target {
        return true;
    }
    let mut seen: std::collections::BTreeSet<String> = Default::default();
    let mut stack = vec![target.to_string()];
    while let Some(node) = stack.pop() {
        if node == source {
            return true;
        }
        if !seen.insert(node.clone()) {
            continue;
        }
        stack.extend(edges.iter().filter(|e| e.source == node).map(|e| e.target.clone()));
    }
    false
}

/// One side of an arithmetic expression, as a number.
///
/// A delimited file arrives as text and the numbers in it are numbers by declaration, not
/// by storage, so arithmetic on one says so. Tolerant on purpose: a field that does not
/// parse becomes NULL rather than ending the run, which is what the rest of the read
/// already does.
fn numeric_operand(e: &str, types: &ColTypes, ports: &PortMap) -> Option<String> {
    let sql = java_expr_to_sql(e, types, ports)?;
    let declared = column_type(e, types).is_some() && !is_number(e.trim());
    Some(match declared {
        true => format!("(TRY_CAST({sql} AS DECIMAL(38,4)))"),
        false => format!("({sql})"),
    })
}

/// Split on `+` at the top level, outside any string or bracket.
fn split_top_level_plus(e: &str) -> Vec<&str> {
    split_top_level(e, "+")
}


/// Whether a piece is text: a literal, a column the file records as one, or a call that
/// produces text whatever it was given.
fn yields_text(e: &str, types: &ColTypes) -> bool {
    let t = e.trim();
    if t.starts_with('"') && t.ends_with('"') && t.len() >= 2 {
        return true;
    }
    // The string helpers are text as a family, with three exceptions: the ones that
    // answer with a position or a count answer with a NUMBER whatever they were given.
    // Counted as text, `INDEX(name,"_")+2` reads as joining rather than adding, and what
    // wanted a position gets two digits stuck together.
    if t.starts_with("StringHandling.") && !starts_with_numeric_string_helper(t) {
        return true;
    }
    for marker in [
        ".replaceAll(",
        ".toUpperCase(",
        ".toLowerCase(",
        ".substring(",
        ".toString(",
        "String.valueOf(",
        ".trim(",
    ] {
        if t.contains(marker) {
            return true;
        }
    }
    // A setting arrives as text unless something says otherwise.
    if t.starts_with("context.") {
        return true;
    }
    column_type(t, types).is_some_and(|k| k.eq_ignore_ascii_case("id_String"))
}

/// The string helpers that answer with a number rather than with text.
fn starts_with_numeric_string_helper(t: &str) -> bool {
    ["StringHandling.INDEX", "StringHandling.LEN", "StringHandling.COUNT"]
        .iter()
        .any(|n| t.starts_with(n))
}

/// Whether a piece is a number: a literal, a column the file records as one, or a call
/// that answers with a number whatever it was given.
fn yields_number(e: &str, types: &ColTypes) -> bool {
    let t = e.trim();
    if is_number(t) || starts_with_numeric_string_helper(t) {
        return true;
    }
    column_type(t, types).is_some_and(|k| {
        ["id_BigDecimal", "id_Integer", "id_Long", "id_Short", "id_Double", "id_Float"]
            .iter()
            .any(|n| k.eq_ignore_ascii_case(n))
    })
}

/// The recorded type of a bare or table-qualified column reference.
fn column_type<'a>(e: &str, types: &'a ColTypes) -> Option<&'a String> {
    let t = e.trim();
    types.get(t).or_else(|| types.get(t.split_once('.').map(|(_, c)| c).unwrap_or(t)))
}

/// A date format written the Java way, as SQL writes it.
///
/// Passed through unchanged it would mean something else entirely - `%` is a literal in
/// one and an escape in the other - so only the pieces with one reading are translated
/// and anything else refuses, rather than producing a format that silently parses wrong.
fn date_format_to_strftime(literal: &str) -> Option<String> {
    let t = literal.trim();
    let pattern = t.strip_prefix('"')?.strip_suffix('"')?;
    let mut out = String::with_capacity(pattern.len());
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let run = chars[i..].iter().take_while(|x| **x == c).count();
        let code = match (c, run) {
            ('y', 4) => Some("%Y"),
            ('y', 2) => Some("%y"),
            ('M', 2) => Some("%m"),
            ('M', 3) => Some("%b"),
            ('d', 2) => Some("%d"),
            ('H', 2) => Some("%H"),
            ('m', 2) => Some("%M"),
            ('s', 2) => Some("%S"),
            ('S', 3) => Some("%f"),
            _ => None,
        };
        match code {
            Some(c) => out.push_str(c),
            None if !c.is_ascii_alphanumeric() && c != '%' => {
                // A separator stands for itself.
                out.extend(std::iter::repeat(c).take(run));
            }
            // A letter with no single reading, or a percent that would be read as an
            // escape: refuse rather than guess at the whole format.
            None => return None,
        }
        i += run;
    }
    Some(out)
}

/// A Java string literal as a SQL one. Anything else is refused: a pattern assembled at
/// run time is not a pattern this side of the move.
fn java_string_to_sql(arg: &str) -> Option<String> {
    let t = arg.trim();
    let inner = t.strip_prefix('"')?.strip_suffix('"')?;
    // Java's own escapes for a quote and a backslash, then SQL's for a quote.
    let unescaped = inner.replace("\\\"", "\"").replace("\\\\", "\\");
    Some(format!("'{}'", unescaped.replace('\'', "''")))
}

/// The verb of a SQL statement that changes the database, if this is one.
///
/// Only the leading word is read, and only the handful that a job actually carries. A
/// query that merely mentions one of these words further in is a query, and a WITH ...
/// SELECT is too - guessing more than the first word would refuse work that runs.
fn leading_statement_verb(sql: &str) -> Option<String> {
    let first = sql
        .trim_start()
        .lines()
        .find(|l| !l.trim().is_empty() && !l.trim_start().starts_with("--"))?
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()?
        .to_ascii_uppercase();
    matches!(
        first.as_str(),
        "UPDATE"
            | "INSERT"
            | "DELETE"
            | "MERGE"
            | "TRUNCATE"
            | "CREATE"
            | "DROP"
            | "ALTER"
            | "GRANT"
            | "CALL"
            // A block, and the statements that steer a session rather than ask it
            // anything. None of them returns rows either.
            | "BEGIN"
            | "DECLARE"
            | "EXECUTE"
            | "COMMIT"
            | "ROLLBACK"
            | "USE"
            | "SET"
    )
    .then_some(first)
}

/// Everything inside a loop's body.
///
/// The body becomes a pipeline of its own, which it can only do while nothing outside it
/// feeds it. So an ordering link must not reach into one, or across one: adding it would
/// leave the loop pointing at a body that could no longer be lifted, which costs the whole
/// child pipeline to gain an ordering that the loop already implies.
fn loop_body_nodes(
    edges: &[PipelineEdge],
    connections: &[Conn],
) -> std::collections::BTreeSet<String> {
    let mut inside: std::collections::BTreeSet<String> = Default::default();
    for c in connections
        .iter()
        .filter(|c| c.connector.as_deref().is_some_and(|k| k.eq_ignore_ascii_case("ITERATE")))
    {
        let mut stack = vec![c.target.clone()];
        while let Some(node) = stack.pop() {
            if node == c.source || !inside.insert(node.clone()) {
                continue;
            }
            stack.extend(
                edges.iter().filter(|e| e.source == node).map(|e| e.target.clone()),
            );
        }
    }
    inside
}

/// Make the work after a parallel join wait for the branches, not for the fork.
///
/// A job that forks into parallel branches and then carries on records the join as a link
/// from the node that forked - the same node the branches themselves hang off. Read
/// literally that makes the work after the join a third branch, free to run alongside the
/// two it exists to wait for, and everything downstream of it then sees whatever those
/// branches happened to have finished. The link is a join, so it is written as one: the
/// end of every branch feeds it.
///
/// What counts as a branch matters here. Branches converge - two of them will hand their
/// failures to the same error handler, and that handler is usually downstream of the join
/// as well. Following a branch to wherever it leads therefore walks straight past the join
/// and comes back round, and hanging the join off that would be a loop rather than a wait.
/// So a branch is only the part of itself that the work after the join cannot already
/// reach, and a fork with nothing left after that is left exactly as it was.
fn rewire_parallel_joins(mut edges: Vec<PipelineEdge>, connections: &[Conn]) -> Vec<PipelineEdge> {
    let is = |c: &Conn, name: &str| {
        c.connector.as_deref().is_some_and(|k| k.eq_ignore_ascii_case(name))
    };
    let joins: Vec<(String, String)> = connections
        .iter()
        .filter(|c| is(c, "SYNCHRONIZE"))
        .map(|c| (c.source.clone(), c.target.clone()))
        .collect();
    if joins.is_empty() {
        return edges;
    }

    let inside_loop = loop_body_nodes(&edges, connections);
    for (fork, after) in joins {
        if inside_loop.contains(&after) {
            continue;
        }
        let roots: Vec<String> = connections
            .iter()
            .filter(|c| is(c, "PARALLELIZE") && c.source == fork)
            .map(|c| c.target.clone())
            .collect();
        if roots.is_empty() {
            continue;
        }
        let successors = |from: &str, edges: &[PipelineEdge]| -> Vec<String> {
            edges.iter().filter(|e| e.source == from).map(|e| e.target.clone()).collect()
        };
        let reachable = |from: &str, edges: &[PipelineEdge]| -> std::collections::BTreeSet<String> {
            let mut seen: std::collections::BTreeSet<String> = Default::default();
            let mut stack = vec![from.to_string()];
            while let Some(node) = stack.pop() {
                if !seen.insert(node.clone()) {
                    continue;
                }
                stack.extend(successors(&node, edges));
            }
            seen
        };

        // Everything the work after the join already leads to is not part of a branch,
        // whatever a branch link says: it happens after the join, not before it.
        let downstream = reachable(&after, &edges);
        let mut ends: Vec<String> = Vec::new();
        for root in &roots {
            if downstream.contains(root) {
                continue;
            }
            let branch: std::collections::BTreeSet<String> = reachable(root, &edges)
                .into_iter()
                .filter(|n| !downstream.contains(n))
                .collect();
            for node in &branch {
                let leads_on = successors(node, &edges).iter().any(|t| branch.contains(t));
                if !leads_on && !ends.contains(node) {
                    ends.push(node.clone());
                }
            }
        }
        ends.retain(|e| !inside_loop.contains(e));
        if ends.is_empty() {
            continue;
        }

        edges.retain(|e| !(e.source == fork && e.target == after));
        for end in ends {
            if would_cycle(&edges, &end, &after) {
                continue;
            }
            edges.push(PipelineEdge {
                id: format!("join-{end}-{after}"),
                source: end,
                target: after.clone(),
                source_handle: Some("main".into()),
                target_handle: Some("main".into()),
                edge_type: None,
                data: Some(EdgeData {
                    connection_type: "on-subjob-ok".into(),
                    label: None,
                    condition: None,
                }),
            });
        }
    }
    edges
}

/// Make "after this subjob" wait for all of the subjob.
///
/// The link is written out of the component the subjob starts at, because that component
/// is how the tool names the subjob. Read as a link out of that one component it means
/// "after the first step" - a much weaker thing: everything the subjob went on to do,
/// including the file it wrote, is free to happen afterwards. A job that writes a file in
/// one subjob and reads it in the next then reads it before it exists, and reports
/// success either way.
///
/// So the link is moved to where the subjob ends. A subjob is what the rows flow through,
/// so its end is what nothing else reads from; a subjob that ends in several places has
/// the next one wait for all of them.
fn anchor_subjob_links_at_their_end(mut edges: Vec<PipelineEdge>) -> Vec<PipelineEdge> {
    let is_row = |e: &PipelineEdge| {
        matches!(
            e.data.as_ref().map(|d| d.connection_type.as_str()),
            Some("main") | Some("lookup")
        ) || matches!(
            e.data.as_ref().map(|d| d.connection_type.as_str()),
            Some(k) if k.starts_with("lookup")
        )
    };
    let waits: Vec<(usize, String, String)> = edges
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            e.data.as_ref().map(|d| d.connection_type.as_str()) == Some("on-subjob-ok")
        })
        .map(|(i, e)| (i, e.source.clone(), e.target.clone()))
        .collect();
    if waits.is_empty() {
        return edges;
    }

    let mut added: Vec<PipelineEdge> = Vec::new();
    let mut drop: std::collections::BTreeSet<usize> = Default::default();
    for (at, head, after) in waits {
        // What the rows flow through from here, without leaving by the link itself.
        let mut subjob: std::collections::BTreeSet<String> = Default::default();
        let mut stack = vec![head.clone()];
        while let Some(node) = stack.pop() {
            if node == after || !subjob.insert(node.clone()) {
                continue;
            }
            stack.extend(
                edges
                    .iter()
                    .filter(|e| e.source == node && is_row(e))
                    .map(|e| e.target.clone()),
            );
        }
        let ends: Vec<String> = subjob
            .iter()
            .filter(|n| {
                !edges
                    .iter()
                    .any(|e| &&e.source == n && is_row(e) && subjob.contains(&e.target))
            })
            .cloned()
            .collect();
        // A subjob of one component is already its own end.
        if ends.len() == 1 && ends[0] == head {
            continue;
        }
        let mut moved = false;
        for end in ends {
            if end == head || would_cycle(&edges, &end, &after) {
                continue;
            }
            added.push(PipelineEdge {
                id: format!("after-{end}-{after}"),
                source: end,
                target: after.clone(),
                source_handle: Some("main".into()),
                target_handle: Some("main".into()),
                edge_type: None,
                data: Some(EdgeData {
                    connection_type: "on-subjob-ok".into(),
                    label: None,
                    condition: None,
                }),
            });
            moved = true;
        }
        // The original link is kept unless the whole of the subjob now stands behind the
        // next one; dropping it otherwise would lose an ordering rather than sharpen it.
        if moved {
            drop.insert(at);
        }
    }
    let mut i = 0;
    edges.retain(|_| {
        let keep = !drop.contains(&i);
        i += 1;
        keep
    });
    edges.extend(added);
    edges
}

/// Keep the order subjobs are declared in, for the ones nothing else orders.
///
/// A job is mostly a list of subjobs that run one after another. Only some of them are
/// linked to each other; the rest are sequenced by nothing more than the order the file
/// lists them at the end, which is the order they were generated to run in. Read without
/// that, a job that writes a table in one subjob and reads it in the next looks like two
/// things that could happen in either order, and anything that has to prove the write
/// comes first cannot.
///
/// The exception is a parallel fork, whose branches are declared one after another like
/// everything else and are the one part of the job that genuinely does not run in that
/// order. Branches are left out, and so is any pair already ordered - and any pair whose
/// ordering would contradict a link that is already there.
fn chain_declared_subjobs(
    mut edges: Vec<PipelineEdge>,
    heads: &[String],
    connections: &[Conn],
) -> Vec<PipelineEdge> {
    if heads.len() < 2 {
        return edges;
    }
    let is = |c: &Conn, name: &str| {
        c.connector.as_deref().is_some_and(|k| k.eq_ignore_ascii_case(name))
    };
    let successors = |from: &str, edges: &[PipelineEdge]| -> Vec<String> {
        edges.iter().filter(|e| e.source == from).map(|e| e.target.clone()).collect()
    };
    let reachable = |from: &str, edges: &[PipelineEdge]| -> std::collections::BTreeSet<String> {
        let mut seen: std::collections::BTreeSet<String> = Default::default();
        let mut stack = vec![from.to_string()];
        while let Some(node) = stack.pop() {
            if !seen.insert(node.clone()) {
                continue;
            }
            stack.extend(successors(&node, edges));
        }
        seen
    };

    // Everything inside a parallel fork. Scoped the same way the join is: the part of a
    // branch that the work after the join cannot already reach.
    let mut parallel: std::collections::BTreeSet<String> = Default::default();
    for join in connections.iter().filter(|c| is(c, "SYNCHRONIZE")) {
        let downstream = reachable(&join.target, &edges);
        for root in connections.iter().filter(|c| is(c, "PARALLELIZE") && c.source == join.source)
        {
            for n in reachable(&root.target, &edges) {
                if !downstream.contains(&n) {
                    parallel.insert(n);
                }
            }
        }
    }
    // A fork with no join still starts its branches itself.
    for root in connections.iter().filter(|c| is(c, "PARALLELIZE")) {
        parallel.insert(root.target.clone());
    }
    // A loop's body is lifted into a pipeline of its own and cannot be fed from outside,
    // so it is left out of the chain at both ends.
    parallel.extend(loop_body_nodes(&edges, connections));

    let known: std::collections::BTreeSet<String> = edges
        .iter()
        .flat_map(|e| [e.source.clone(), e.target.clone()])
        .collect();
    let mut previous: Option<String> = None;
    for head in heads {
        // A head with no links of its own is still a subjob; it just has no edges to be
        // found in, so it is only skipped when it is part of a fork.
        if parallel.contains(head) {
            continue;
        }
        let Some(before) = previous.replace(head.clone()) else { continue };
        let onward = reachable(&before, &edges);
        if onward.contains(head) || reachable(head, &edges).contains(&before) {
            continue;
        }
        // Wait for the end of the previous subjob, not its start: a write sitting at the
        // end of it has to be behind the next subjob, and only its own tail is.
        let body: Vec<String> = onward.into_iter().filter(|n| !parallel.contains(n)).collect();
        let ends: Vec<String> = body
            .iter()
            .filter(|n| !successors(n, &edges).iter().any(|t| body.contains(t)))
            .cloned()
            .collect();
        for end in ends {
            if !(known.contains(&end) || end == *before) || would_cycle(&edges, &end, head) {
                continue;
            }
            edges.push(PipelineEdge {
                id: format!("order-{end}-{head}"),
                source: end,
                target: head.clone(),
                source_handle: Some("main".into()),
                target_handle: Some("main".into()),
                edge_type: None,
                data: Some(EdgeData {
                    connection_type: "on-subjob-ok".into(),
                    label: None,
                    condition: None,
                }),
            });
        }
    }
    edges
}

fn connection_type_for(connector: Option<&str>) -> &'static str {
    match connector.unwrap_or("").to_ascii_uppercase().as_str() {
        "ITERATE" => "iterate",
        "RUN_IF" => "run-if",
        "SUBJOB_OK" => "on-subjob-ok",
        "SUBJOB_ERROR" => "on-subjob-error",
        "COMPONENT_OK" => "on-component-ok",
        "COMPONENT_ERROR" => "on-component-error",
        "PARALLELIZE" | "SYNCHRONIZE" => "on-subjob-ok",
        _ => "main",
    }
}

/// Pull `<node>`, its `<elementParameter>`s, its mapper output entries, and the
/// `<connection>` list out of the job XML.
type Parsed = (Vec<RawNode>, Vec<Conn>, BTreeMap<String, String>, Vec<String>);

fn parse(xml: &str) -> Result<Parsed, String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut nodes: Vec<RawNode> = Vec::new();
    let mut conns: Vec<Conn> = Vec::new();
    let mut cur: Option<RawNode> = None;
    // Mapper entries are only outputs when we are inside <outputTables>.
    let mut in_output_table = false;
    let mut in_var_table = false;
    let mut input_table: Option<String> = None;
    let mut in_subjob = false;
    let mut subjob_heads: Vec<String> = Vec::new();
    // The TABLE parameter whose rows we are currently collecting, if any.
    let mut table_param: Option<String> = None;
    let mut in_flow_metadata = false;
    // The job's own context parameters. A bound or a table name written as
    // context.NAME is resolvable from here, and leaving it unresolved is what
    // stopped an imported loop from compiling on its own.
    let mut context: BTreeMap<String, String> = BTreeMap::new();
    let mut buf = Vec::new();

    loop {
        let ev = reader
            .read_event_into(&mut buf)
            .map_err(|e| format!("talend import: malformed XML at {}: {e}", reader.buffer_position()))?;
        match ev {
            Event::Eof => break,
            Event::Start(ref e) | Event::Empty(ref e) => {
                let name = e.local_name();
                let tag = name.as_ref().to_string();
                // Values must be unescaped: Talend stores Java string literals,
                // so the quotes arrive as `&quot;` and a raw read would leave
                // `&quot;localhost&quot;` where `localhost` belongs.
                let attr = |k: &str| -> Option<String> {
                    e.attributes().flatten().find_map(|a| {
                        (a.key.local_name().as_ref() == k).then(|| {
                            a.normalized_value(quick_xml::XmlVersion::Implicit1_0)
                                .map(|v| v.into_owned())
                                .unwrap_or_else(|_| a.value.to_string())
                        })
                    })
                };
                match tag.as_str() {
                    // A joblet writes its boundary ports as `jobletNodes`. They carry a
                    // component and a name like any other node and connections reference
                    // them, so reading only `node` drops the port and the link with it.
                    "node" | "jobletNodes" => {
                        if let Some(done) = cur.take() {
                            nodes.push(done);
                        }
                        cur = Some(RawNode {
                            component: attr("componentName").unwrap_or_default(),
                            unique: String::new(),
                            columns: Vec::new(),
                            column_types: Vec::new(),
                            column_scale: Default::default(),
                            tables: BTreeMap::new(),
                            params: BTreeMap::new(),
                            mapper_out: Vec::new(),
                            mapper_outs: Vec::new(),
                            mapper_out_filters: Vec::new(),
                            mapper_types: Default::default(),
                            mapper_vars: Vec::new(),
                            mapper_inputs: Vec::new(),
                            x: attr("posX").and_then(|v| v.parse().ok()).unwrap_or(0.0),
                            y: attr("posY").and_then(|v| v.parse().ok()).unwrap_or(0.0),
                        });
                        in_output_table = false;
                    }
                    // The file closes with one <subjob> per subjob, in the order they
                    // run. Nothing else records that order, and most subjobs have no link
                    // to each other at all.
                    "subjob" => in_subjob = true,
                    "elementParameter" if in_subjob => {
                        if attr("name").as_deref() == Some("UNIQUE_NAME") {
                            if let Some(v) = attr("value") {
                                subjob_heads.push(v);
                            }
                        }
                    }
                    "elementParameter" => {
                        if let (Some(n), Some(k)) = (cur.as_mut(), attr("name")) {
                            let v = attr("value").unwrap_or_default();
                            if k == "UNIQUE_NAME" {
                                n.unique = v.clone();
                            }
                            // A TABLE parameter carries its rows as the
                            // elementValue children that follow it.
                            if attr("field").as_deref() == Some("TABLE") {
                                table_param = Some(k.clone());
                                n.tables.entry(k.clone()).or_default();
                            } else {
                                table_param = None;
                            }
                            n.params.insert(k, v);
                        }
                    }
                    // One row per repeat of the first field seen: the ids run
                    // straight through the whole table rather than restarting.
                    "elementValue" => {
                        if let (Some(n), Some(tp)) = (cur.as_mut(), table_param.clone()) {
                            if let (Some(field), Some(v)) = (attr("elementRef"), attr("value")) {
                                let rows = n.tables.entry(tp).or_default();
                                let start_new = rows
                                    .last()
                                    .map(|r| r.contains_key(&field))
                                    .unwrap_or(true);
                                if start_new {
                                    rows.push(BTreeMap::new());
                                }
                                if let Some(r) = rows.last_mut() {
                                    r.insert(field, v);
                                }
                            }
                        }
                    }
                    // A node declares one schema per connector; the main output
                    // is the FLOW one. Reject and other connectors describe
                    // different shapes and must not be mixed into it.
                    "contextParameter" => {
                        if let (Some(k), Some(v)) = (attr("name"), attr("value")) {
                            // First definition wins: a job repeats its context
                            // once per environment and the default comes first.
                            context.entry(k).or_insert(v);
                        }
                    }
                    "metadata" => {
                        // The row-carrying schema is spelled FLOW by some components and
                        // MAIN by others; both describe what the node hands on. Reading
                        // only one left the other with no columns at all, so a read took
                        // its query's names rather than the ones the job uses. REJECT and
                        // the rest describe different shapes and stay out.
                        // A node can carry several schemas - what it hands on, what it
                        // rejects, what a second port carries. The row-carrying one is
                        // spelled FLOW by some components and MAIN by others, and a node
                        // with both would otherwise have the two read as one long list
                        // of columns that matches nothing it actually produces.
                        let row_carrying =
                            matches!(attr("connector").as_deref(), Some("FLOW") | Some("MAIN"));
                        let already = cur.as_ref().is_some_and(|n| !n.columns.is_empty());
                        in_flow_metadata = row_carrying && !already;
                    }
                    "column" => {
                        if in_flow_metadata {
                            if let (Some(n), Some(c)) = (cur.as_mut(), attr("name")) {
                                let ty = attr("type").unwrap_or_default();
                                // A decimal says how wide it is and how much of that is
                                // after the point. Both have to survive, or the value is
                                // rounded to whatever scale is assumed instead.
                                let width = |k: &str| -> Option<u32> {
                                    attr(k).and_then(|v| v.trim().parse::<i64>().ok())
                                        .filter(|v| *v >= 0)
                                        .map(|v| v as u32)
                                };
                                if let (Some(len), Some(prec)) = (width("length"), width("precision")) {
                                    if len > 0 && prec <= len && len <= 38 {
                                        n.column_scale.insert(c.clone(), (len, prec));
                                    }
                                }
                                n.columns.push(c.clone());
                                n.column_types.push((c, ty));
                            }
                        }
                    }
                    "inputTables" => {
                        in_output_table = false;
                        in_var_table = false;
                        if let Some(n) = cur.as_mut() {
                            input_table = attr("name");
                            n.mapper_inputs.push(MapperInput {
                                name: input_table.clone().unwrap_or_default(),
                                keys: Vec::new(),
                                inner: attr("innerJoin").as_deref() == Some("true"),
                            });
                        }
                    }
                    "outputTables" => {
                        in_output_table = true;
                        in_var_table = false;
                        input_table = None;
                        if let Some(n) = cur.as_mut() {
                            let name = attr("name").unwrap_or_else(|| {
                                format!("out{}", n.mapper_outs.len() + 1)
                            });
                            // The text of a condition is kept in the file whether it is
                            // in use or not, so the switch is what says to apply it.
                            if attr("activateExpressionFilter").as_deref() == Some("true") {
                                if let Some(f) =
                                    attr("expressionFilter").filter(|f| !f.trim().is_empty())
                                {
                                    n.mapper_out_filters.push((name.clone(), f));
                                }
                            }
                            n.mapper_outs.push((name, Vec::new()));
                        }
                    }
                    "varTables" => {
                        in_output_table = false;
                        in_var_table = true;
                        input_table = None;
                    }
                    "mapperTableEntries" => {
                        // An input entry records the column's Talend type, which is what
                        // says whether `new BigDecimal(x)` is the exact constructor or the
                        // lossy one. Without it the expression cannot be read at all.
                        if let (Some(n), Some(col), Some(ty)) =
                            (cur.as_mut(), attr("name"), attr("type"))
                        {
                            if !in_output_table {
                                if let Some(t) = input_table.as_deref() {
                                    n.mapper_types.insert(format!("{t}.{col}"), ty.clone());
                                }
                                // An input entry with an expression is a column this
                                // input is matched on; the expression is the main side.
                                if let Some(expr) = attr("expression") {
                                    if !expr.trim().is_empty() {
                                        if let Some(input) = n.mapper_inputs.last_mut() {
                                            input.keys.push((col.clone(), expr));
                                        }
                                    }
                                }
                                n.mapper_types.entry(col).or_insert(ty);
                            }
                        }
                        if in_var_table {
                            if let (Some(n), Some(col), Some(expr)) =
                                (cur.as_mut(), attr("name"), attr("expression"))
                            {
                                n.mapper_vars.push((col, expr));
                            }
                        }
                        if in_output_table {
                            if let (Some(n), Some(col)) = (cur.as_mut(), attr("name")) {
                                // A column with nothing to compute has no expression at
                                // all, not an empty one. It is still a column the row
                                // carries, so it is taken either way.
                                let expr = attr("expression").unwrap_or_default();
                                let ty = attr("type").unwrap_or_default();
                                n.mapper_out.push((col.clone(), expr.clone(), ty.clone()));
                                if let Some(last) = n.mapper_outs.last_mut() {
                                    last.1.push((col, expr, ty));
                                }
                            }
                        }
                    }
                    "connection" => {
                        if let (Some(s), Some(t)) = (attr("source"), attr("target")) {
                            conns.push(Conn {
                                source: s,
                                target: t,
                                connector: attr("connectorName"),
                                label: attr("label"),
                            });
                        }
                    }
                    _ => {}
                }
            }
            Event::End(ref e) => {
                let name = e.local_name();
                if name.as_ref() == "node" || name.as_ref() == "jobletNodes" {
                    if let Some(done) = cur.take() {
                        nodes.push(done);
                    }
                } else if name.as_ref() == "outputTables" {
                    in_output_table = false;
                } else if name.as_ref() == "subjob" {
                    in_subjob = false;
                }
            }
            _ => {}
        }
        buf.clear();
    }
    if let Some(done) = cur.take() {
        nodes.push(done);
    }

    // A node with no UNIQUE_NAME cannot be referenced by a connection; fall
    // back to the component name plus an index so the import still holds.
    for (i, n) in nodes.iter_mut().enumerate() {
        if n.unique.is_empty() {
            n.unique = format!("{}_{}", n.component, i + 1);
        }
    }
    Ok((nodes, conns, context, subjob_heads))
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_loop_body_moves_into_its_own_pipeline_and_the_loop_names_it() {
        // A legacy job writes a loop's body inline. Duckle runs a child pipeline
        // by reference, so the body has to become a file and the loop has to
        // name it, or the loop compiles to nothing to run.
        let mut nodes = vec![
            imported_node("root", "src.csv", 0.0, 0.0),
            imported_node("loop", "ctl.foreach", 1.0, 0.0),
            imported_node("body1", "xf.filter", 2.0, 0.0),
            imported_node("body2", "snk.parquet", 3.0, 0.0),
        ];
        let mut edges = vec![
            test_edge("e1", "root", "loop", "main"),
            test_edge("e2", "loop", "body1", "iterate"),
            test_edge("e3", "body1", "body2", "main"),
        ];
        let mut warnings = Vec::new();
        let kids = extract_loop_bodies("job", &mut nodes, &mut edges, &mut warnings);

        assert_eq!(kids.len(), 1, "one loop, one body");
        let kid = &kids[0];
        assert_eq!(kid.nodes.len(), 2, "the body moved wholesale");
        assert_eq!(kid.edges.len(), 1, "and kept its internal wiring");

        // The parent keeps the loop and loses the body.
        let left: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(left, vec!["root", "loop"]);
        assert_eq!(edges.len(), 1, "only the link into the loop remains");

        // And the loop names the file the body became.
        let r = nodes[1].data.properties.as_ref().unwrap()["pipelineRef"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(r, format!("{}.json", kid.name));
    }

    #[test]
    fn a_lookup_feeding_the_loop_body_travels_with_it() {
        // A join inside a loop reads its reference table from a source outside
        // it. That source is part of the body's work, not the main flow's, so
        // refusing to lift the loop over it would strand a whole job.
        let mut nodes = vec![
            imported_node("loop", "ctl.foreach", 0.0, 0.0),
            imported_node("join", "xf.map", 1.0, 0.0),
            imported_node("ref", "src.snowflake", 2.0, 0.0),
        ];
        let mut edges = vec![
            test_edge("e1", "loop", "join", "iterate"),
            test_edge("e2", "ref", "join", "lookup"),
        ];
        let mut warnings = Vec::new();
        let kids = extract_loop_bodies("job", &mut nodes, &mut edges, &mut warnings);

        assert_eq!(kids.len(), 1, "the loop should still lift");
        let ids: Vec<&str> = kids[0].nodes.iter().map(|n| n.id.as_str()).collect();
        assert!(ids.contains(&"join") && ids.contains(&"ref"),
                "the lookup source travels with the body, got {:?}", ids);
        assert_eq!(nodes.len(), 1, "only the loop stays behind");
    }

    #[test]
    fn a_lookup_the_main_flow_also_reads_is_left_where_it_is() {
        // Moving a source the parent still reads would cut the parent, so the
        // loop is refused rather than the reference stolen from under it.
        let mut nodes = vec![
            imported_node("loop", "ctl.foreach", 0.0, 0.0),
            imported_node("join", "xf.map", 1.0, 0.0),
            imported_node("ref", "src.snowflake", 2.0, 0.0),
            imported_node("other", "snk.csv", 3.0, 0.0),
        ];
        let mut edges = vec![
            test_edge("e1", "loop", "join", "iterate"),
            test_edge("e2", "ref", "join", "lookup"),
            test_edge("e3", "ref", "other", "main"),
        ];
        let mut warnings = Vec::new();
        let kids = extract_loop_bodies("job", &mut nodes, &mut edges, &mut warnings);
        assert!(kids.is_empty(), "nothing should have been lifted");
        assert_eq!(nodes.len(), 4, "the parent is untouched");
    }

    #[test]
    fn a_loop_body_shared_with_the_main_flow_is_left_alone() {
        // Moving a node that the main flow also feeds would silently cut the
        // parent. Refusing and saying so beats extracting the wrong subgraph.
        let mut nodes = vec![
            imported_node("root", "src.csv", 0.0, 0.0),
            imported_node("loop", "ctl.foreach", 1.0, 0.0),
            imported_node("shared", "xf.filter", 2.0, 0.0),
        ];
        let mut edges = vec![
            test_edge("e1", "loop", "shared", "iterate"),
            test_edge("e2", "root", "shared", "main"),
        ];
        let mut warnings = Vec::new();
        let kids = extract_loop_bodies("job", &mut nodes, &mut edges, &mut warnings);

        assert!(kids.is_empty(), "nothing should have been lifted");
        assert_eq!(nodes.len(), 3, "the parent is untouched");
        assert_eq!(warnings.len(), 1, "and the job says why");
        assert!(
            nodes[1].data.properties.as_ref().map_or(true, |p| p.get("pipelineRef").is_none()),
            "a loop with no lifted body must not name a file that was never written"
        );
    }

    fn imported_node(id: &str, component: &str, x: f64, y: f64) -> PipelineNode {
        PipelineNode {
            id: id.into(),
            flow_type: Some("transform".into()),
            position: Position { x, y },
            data: node_data(id.into(), Some(component.into()), None),
        }
    }

    fn test_edge(id: &str, from: &str, to: &str, kind: &str) -> PipelineEdge {
        PipelineEdge {
            id: id.into(),
            source: from.into(),
            target: to.into(),
            source_handle: Some("main".into()),
            target_handle: Some("main".into()),
            edge_type: None,
            data: Some(EdgeData {
                connection_type: kind.into(),
                label: None,
                condition: None,
            }),
        }
    }

    #[test]
    fn tcomp_reads_scalars_enums_and_booleans_from_the_properties_blob() {
        // A generic Talend component keeps its whole configuration in one JSON
        // document. Scalars sit at storedValue; booleans wrap it in an object
        // with `value`; enums wrap it in an object with `name` and NO `value`.
        // Reading `value` everywhere returns nothing for the enums, which drops
        // the auth type while looking like it worked.
        let blob: JsonValue = serde_json::from_str(
            r#"{
                "connection": {
                    "account":  { "storedValue": "acct-1" },
                    "db":       { "storedValue": "DB1" },
                    "autoCommit":         { "storedValue": { "@type": "b", "value": true } },
                    "authenticationType": { "storedValue": { "@type": "e", "name": "KEY_PAIR" } },
                    "loginTimeout":       { "storedValue": { "@type": "n", "value": 30 } },
                    "sharedConnectionName": { "storedValue": null },
                    "role": { "storedValue": "" },
                    "referencedComponent": {
                        "reference": { "warehouse": { "storedValue": "WH_SHARED" } }
                    }
                },
                "table": { "tableName": { "storedValue": "T1" } }
            }"#,
        )
        .unwrap();

        assert_eq!(tcomp_value(&blob, "connection.account").as_deref(), Some("acct-1"));
        assert_eq!(tcomp_value(&blob, "connection.db").as_deref(), Some("DB1"));
        assert_eq!(tcomp_value(&blob, "table.tableName").as_deref(), Some("T1"));
        // The enum: `name`, not `value`.
        assert_eq!(
            tcomp_value(&blob, "connection.authenticationType").as_deref(),
            Some("KEY_PAIR"),
            "an enum stores its token under name; reading value loses it"
        );
        // The boolean and the number still come back.
        assert_eq!(tcomp_value(&blob, "connection.autoCommit").as_deref(), Some("true"));
        assert_eq!(tcomp_value(&blob, "connection.loginTimeout").as_deref(), Some("30"));
        // A shared connection is mirrored under referencedComponent.reference.
        assert_eq!(
            tcomp_value(&blob, "connection.referencedComponent.reference.warehouse").as_deref(),
            Some("WH_SHARED")
        );
        // Absent, null and empty are all "not set", not an empty string that
        // would overwrite a default with nothing.
        assert_eq!(tcomp_value(&blob, "connection.sharedConnectionName"), None);
        assert_eq!(tcomp_value(&blob, "connection.role"), None);
        assert_eq!(tcomp_value(&blob, "connection.nosuch"), None);
        assert_eq!(tcomp_value(&blob, "nosuch.deep.path"), None);
    }

    #[test]
    fn a_trigger_link_does_not_import_as_a_data_edge() {
        // Talend orders a job with links that carry no rows. Importing them as
        // `main` asserted a data dependency the job never had: on one corpus
        // that turned 164 ordering links and 24 iterate links into data edges.
        assert_eq!(connection_type_for(Some("FLOW")), "main");
        assert_eq!(connection_type_for(Some("MAIN")), "main");
        assert_eq!(connection_type_for(Some("ITERATE")), "iterate");
        assert_eq!(connection_type_for(Some("RUN_IF")), "run-if");
        assert_eq!(connection_type_for(Some("SUBJOB_OK")), "on-subjob-ok");
        assert_eq!(connection_type_for(Some("SUBJOB_ERROR")), "on-subjob-error");
        assert_eq!(connection_type_for(Some("COMPONENT_OK")), "on-component-ok");
        assert_eq!(connection_type_for(Some("COMPONENT_ERROR")), "on-component-error");
        // No counterpart for these two: keep the ordering, lose the parallelism.
        assert_eq!(connection_type_for(Some("PARALLELIZE")), "on-subjob-ok");
        assert_eq!(connection_type_for(Some("SYNCHRONIZE")), "on-subjob-ok");
        // A named output port still carries rows, and so does an unknown name:
        // inventing a trigger from a name we do not recognise would silently
        // cut the flow.
        assert_eq!(connection_type_for(Some("OUTPUT_1")), "main");
        assert_eq!(connection_type_for(Some("UNIQUE")), "main");
        assert_eq!(connection_type_for(None), "main");
        assert_eq!(connection_type_for(Some("")), "main");
        // Talend has written these lowercase in older exports.
        assert_eq!(connection_type_for(Some("subjob_ok")), "on-subjob-ok");
    }
    use super::*;

    const JOB: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<talendfile:ProcessType xmlns:talendfile="platform:/resource/org.talend.model/model/TalendFile.xsd">
  <node componentName="tMSSqlInput" posX="100" posY="50">
    <elementParameter field="TEXT" name="UNIQUE_NAME" value="tDBInput_1"/>
    <elementParameter field="TECHNICAL" name="PROPERTY:PROPERTY_TYPE" value="REPOSITORY"/>
    <elementParameter field="TEXT" name="HOST" value="&quot;localhost&quot;"/>
    <elementParameter field="TEXT" name="PORT" value="&quot;1433&quot;"/>
    <elementParameter field="TEXT" name="DBNAME" value="&quot;AdventureWorks&quot;"/>
    <elementParameter field="TEXT" name="USER" value="&quot;sa&quot;"/>
    <elementParameter field="PASSWORD" name="PASS" value="enc:system.encryption.key.v1:AAAA"/>
    <elementParameter field="DBTABLE" name="TABLE" value="&quot;Location&quot;"/>
  </node>
  <node componentName="tMap" posX="300" posY="50">
    <elementParameter field="TEXT" name="UNIQUE_NAME" value="tMap_1"/>
    <outputTables name="dim_location">
      <mapperTableEntries name="LocationID" expression="Location.LocationID"/>
      <mapperTableEntries name="DI_Created_Date" expression="TalendDate.getCurrentDate()"/>
      <mapperTableEntries name="DI_Checksum" expression="Location.LocationID.hashCode()"/>
    </outputTables>
  </node>
  <node componentName="tSomethingExotic" posX="500" posY="50">
    <elementParameter field="TEXT" name="UNIQUE_NAME" value="tExotic_1"/>
  </node>
  <connection connectorName="FLOW" source="tDBInput_1" target="tMap_1" label="Location"/>
  <connection connectorName="FLOW" source="tMap_1" target="tExotic_1" label="out"/>
</talendfile:ProcessType>"#;

    #[test]
    fn maps_the_component_head_and_wires_the_flow() {
        let im = import_item(JOB, "dim_location").expect("parses");
        assert_eq!(im.nodes.len(), 3);
        assert_eq!(im.edges.len(), 2);
        let src = &im.nodes[0];
        assert_eq!(src.id, "tDBInput_1");
        assert_eq!(src.data.component_id.as_deref(), Some("src.sqlserver"));
        let p = src.data.properties.as_ref().unwrap();
        assert_eq!(p["host"], "localhost");
        assert_eq!(p["database"], "AdventureWorks");
        assert_eq!(p["tableName"], "Location");
    }

    #[test]
    fn an_encrypted_password_becomes_a_placeholder_not_a_guess() {
        let im = import_item(JOB, "j").unwrap();
        let p = im.nodes[0].data.properties.as_ref().unwrap();
        // Never the ciphertext, and never a blank that would look configured.
        assert_eq!(p["password"], "${ENV:TDBINPUT_1_PASS}");
        assert!(im
            .warnings
            .iter()
            .any(|w| matches!(w, Warning::EncryptedSecret { property, .. } if property == "PASS")));
    }

    #[test]
    fn a_repository_connection_is_reported_because_the_job_lacks_the_credentials() {
        let im = import_item(JOB, "j").unwrap();
        assert!(im
            .warnings
            .iter()
            .any(|w| matches!(w, Warning::RepositoryConnection { node, .. } if node == "tDBInput_1")));
    }

    #[test]
    fn an_unmapped_component_is_kept_as_a_placeholder_never_dropped() {
        let im = import_item(JOB, "j").unwrap();
        let exotic = im.nodes.iter().find(|n| n.id == "tExotic_1").expect("kept");
        assert_eq!(exotic.data.component_id, None);
        assert!(exotic.data.label.contains("unmapped"));
        assert!(im.warnings.iter().any(
            |w| matches!(w, Warning::UnmappedComponent { component, .. } if component == "tSomethingExotic")
        ));
    }

    #[test]
    fn column_refs_map_but_java_expressions_are_reported() {
        let im = import_item(JOB, "j").unwrap();
        let map = im.nodes.iter().find(|n| n.id == "tMap_1").unwrap();
        let exprs = &map.data.properties.as_ref().unwrap()["expressions"];
        assert_eq!(exprs["LocationID"], "LocationID");
        // Asking for the current time has one reading and now has it.
        assert_eq!(exprs["DI_Created_Date"], "now()");
        // A call with no such reading must not be silently carried across as if it
        // worked: the column is left out and the reason is reported.
        assert!(exprs.get("DI_Checksum").is_none());
        assert!(im.warnings.iter().any(
            |w| matches!(w, Warning::JavaExpression { column, .. } if column == "DI_Checksum")
        ));
    }

    #[test]
    fn context_variables_survive_as_duckle_placeholders() {
        assert_eq!(rewrite_context("context.myVar").as_deref(), Some("${myVar}"));
        assert_eq!(
            rewrite_context("context.getProperty(\"vJobPID\")").as_deref(),
            Some("${vJobPID}")
        );
        assert_eq!(rewrite_context("\"literal\""), None);
    }

    /// Run the importer over a real Talend workspace and report coverage.
    ///
    /// Opt-in, because it needs a Studio workspace on disk:
    ///   DUCKLE_TALEND_CORPUS=C:/Talend/workspace cargo test -p duckle-duckdb-engine \
    ///     --lib talend -- --ignored --nocapture
    ///
    /// Synthetic fixtures prove the mapping compiles; only a real corpus shows
    /// what fraction of actual jobs survive it.
    #[test]
    #[ignore = "needs a Talend workspace; set DUCKLE_TALEND_CORPUS"]
    fn real_corpus_coverage() {
        let root = match std::env::var("DUCKLE_TALEND_CORPUS") {
            Ok(v) => std::path::PathBuf::from(v),
            Err(_) => return,
        };
        let mut items = Vec::new();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&dir) else { continue };
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().and_then(|x| x.to_str()) == Some("item")
                    && p.to_string_lossy().contains("process")
                {
                    items.push(p);
                }
            }
        }
        assert!(!items.is_empty(), "no .item job files under the corpus root");

        let (mut jobs, mut nodes, mut mapped, mut failed) = (0usize, 0usize, 0usize, 0usize);
        let mut unmapped: BTreeMap<String, usize> = BTreeMap::new();
        for path in &items {
            let Ok(xml) = std::fs::read_to_string(path) else { continue };
            let name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
            match import_item(&xml, &name) {
                Ok(im) => {
                    jobs += 1;
                    nodes += im.nodes.len();
                    mapped += im.nodes.iter().filter(|n| n.data.component_id.is_some()).count();
                    for w in &im.warnings {
                        if let Warning::UnmappedComponent { component, .. } = w {
                            *unmapped.entry(component.clone()).or_default() += 1;
                        }
                    }
                }
                Err(e) => {
                    failed += 1;
                    eprintln!("  PARSE FAILED {}: {e}", path.display());
                }
            }
        }
        println!("\n  jobs parsed     : {jobs} of {} ({failed} failed)", items.len());
        println!("  nodes           : {nodes}");
        println!(
            "  mapped          : {mapped} ({:.1}%)",
            100.0 * mapped as f64 / nodes.max(1) as f64
        );
        println!("  unmapped kinds  : {unmapped:?}");
        assert_eq!(failed, 0, "every job in the corpus must at least parse");
    }

    #[test]
    fn plain_mapper_expressions_become_sql() {
        // Measured on a real corpus: 278 mapper expressions were reported as needing a
        // human, and the largest groups are a literal, a null, or a cast. Those have one
        // faithful SQL form each, so reporting them buries the ones that genuinely need
        // judgement.
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        assert_eq!(sql("null").as_deref(), Some("NULL"));
        assert_eq!(sql(r#""""#).as_deref(), Some("''"));
        assert_eq!(sql(r#""S""#).as_deref(), Some("'S'"));
        assert_eq!(sql("row1.AMOUNT").as_deref(), Some("AMOUNT"));
        assert_eq!(sql("row1.SPARE.toString()").as_deref(), Some("SPARE"));
        assert_eq!(sql("StringHandling.TRIM(row1.NAME)").as_deref(), Some("trim(NAME)"));
        // new BigDecimal(Double.valueOf(x)) goes through a double in Java, so DOUBLE is
        // the faithful intermediate rather than a guess at a decimal scale. The operand
        // is a row reference: a mapper's own named value is put in place before an
        // expression is read, so one still standing here was never defined.
        assert_eq!(
            sql("new BigDecimal(Double.valueOf(row1.RATE))").as_deref(),
            Some("TRY_CAST(RATE AS DOUBLE)")
        );
        assert_eq!(
            sql("new BigDecimal(Double.valueOf((row1.RATE)))").as_deref(),
            Some("TRY_CAST(RATE AS DOUBLE)")
        );
    }

    #[test]
    fn the_string_helpers_become_their_sql_equivalents() {
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        assert_eq!(sql("StringHandling.LEFT(row1.NID,2)").as_deref(), Some("left(NID, 2)"));
        assert_eq!(sql("StringHandling.RIGHT(row1.NID,2)").as_deref(), Some("right(NID, 2)"));
        // SUBSTR takes a start and a length from 1, matching SQL, rather than Java's
        // begin/end. Confirmed against a reference migration that renders it verbatim
        // as SUBSTR and whose output matches the SQL reading on every row.
        assert_eq!(
            sql("StringHandling.SUBSTR(row1.CODE,1,4)").as_deref(),
            Some("substr(CODE, 1, 4)")
        );
        // How far to take is as often worked out as written down, and refusing the whole
        // expression for that took the column with it.
        assert_eq!(
            sql("StringHandling.SUBSTR(row1.CODE,2,StringHandling.LEN(row1.CODE))").as_deref(),
            Some("substr(CODE, 2, length(CODE))")
        );
        assert_eq!(
            sql("StringHandling.LEFT(row1.CODE,StringHandling.LEN(row1.NID))").as_deref(),
            Some("left(CODE, length(NID))")
        );
        // and they compose with the cast, which is how they appear in practice
        assert_eq!(
            sql("new BigDecimal(Double.valueOf(StringHandling.LEFT(row1.D,4)))").as_deref(),
            Some("TRY_CAST(left(D, 4) AS DOUBLE)")
        );
        assert_eq!(
            sql("new BigDecimal(Double.valueOf(StringHandling.RIGHT(StringHandling.LEFT(row1.D,6),2)))").as_deref(),
            Some("TRY_CAST(right(left(D, 6), 2) AS DOUBLE)")
        );
    }

    #[test]
    fn a_string_helper_with_a_computed_length_is_still_reported() {
        // The count used to have to be a plain integer, on the grounds that anything else
        // could be a Java expression whose value we would be guessing at. It is only used
        // now when it has a faithful reading of its own, and where it has none the whole
        // expression still refuses - so the guess never happens either way, and a count
        // that is worked out rather than written down no longer costs the column.
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        assert_eq!(
            sql("StringHandling.LEFT(row1.NID,row1.N)").as_deref(),
            Some("left(NID, N)")
        );
        assert_eq!(
            sql("StringHandling.LEFT(row1.NID,row1.N.hashCode())"),
            None,
            "a count with no reading of its own still takes the expression with it"
        );
        assert_eq!(sql("StringHandling.LEFT(row1.NID)"), None, "wrong arity");
        assert_eq!(sql("StringHandling.SUBSTR(row1.CODE,1)"), None, "wrong arity");
    }

    #[test]
    fn a_java_body_that_only_prints_says_so() {
        // 73 bodies in one corpus is a long triage list, and 21 of them turned out to
        // carry no rules at all. Saying which is which costs nothing and does not make
        // any of them compile.
        let body = |code: &str| {
            let xml = format!(
                r#"<talendfile:ProcessType xmlns:talendfile="x">
                  <node componentName="tJava">
                    <elementParameter name="UNIQUE_NAME" value="j_1"/>
                    <elementParameter name="CODE" value="{code}"/>
                  </node></talendfile:ProcessType>"#
            );
            import_item(&xml, "j").unwrap().warnings
        };

        let prints = body("System.out.println(&quot;starting&quot;);");
        assert_eq!(prints.len(), 1);
        assert!(
            matches!(&prints[0], Warning::JavaBody { only_prints: true, .. }),
            "a body of prints carries no rules, got {:?}",
            prints[0]
        );

        let rules = body("output_row.total = input_row.a + input_row.b;");
        assert!(
            matches!(&rules[0], Warning::JavaBody { only_prints: false, .. }),
            "a body that assigns must not be called harmless, got {:?}",
            rules[0]
        );

        // a print AND an assignment is not a printing body
        let mixed = body("System.out.println(&quot;x&quot;); context.n = 1;");
        assert!(matches!(&mixed[0], Warning::JavaBody { only_prints: false, .. }));
    }

    #[test]
    fn a_printing_body_still_fails_to_compile() {
        // The whole point of the loud failure is that a body cannot quietly become a
        // pipeline that runs and omits the rules. Saying a body only prints must not
        // change that: it still arrives with no sql.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tJava">
            <elementParameter name="UNIQUE_NAME" value="j_1"/>
            <elementParameter name="CODE" value="System.out.println(&quot;hi&quot;);"/>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert_eq!(im.nodes[0].data.component_id.as_deref(), Some("code.sql"));
        let props = im.nodes[0].data.properties.as_ref().unwrap();
        assert!(props.get("sql").is_none(), "no sql, so it cannot run");
        assert!(props.get("untranslatedSource").is_some(), "the body is kept");
    }

    #[test]
    fn a_choice_becomes_a_case_expression() {
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        // compareTo(x) == 0 is numeric equality ignoring scale, which is what SQL = does.
        assert_eq!(
            sql(r#"row6.PCT.compareTo(new BigDecimal("100")) == 0 ? row6.A : row6.B"#).as_deref(),
            Some("CASE WHEN PCT = 100 THEN A ELSE B END")
        );
        assert_eq!(
            sql(r#"row7.PCT.compareTo(BigDecimal.ZERO) == 0? new BigDecimal("0.00"): row7.A"#).as_deref(),
            Some("CASE WHEN PCT = 0 THEN 0.00 ELSE A END")
        );
        // equals on a string is the same comparison, and a chain nests
        assert_eq!(
            sql(r#"m.d.equals("1")?"I":m.d.equals("2")?"O":"P""#).as_deref(),
            Some("CASE WHEN d = '1' THEN 'I' ELSE CASE WHEN d = '2' THEN 'O' ELSE 'P' END END")
        );
        // subtract, and a choice nested inside one. A missing side counts as nothing:
        // a file leaves a charge blank when there is none, and treating that as UNKNOWN
        // loses the whole total rather than the one charge.
        assert_eq!(
            sql("row6.A.subtract(row6.B)").as_deref(),
            Some("COALESCE((A), 0) - COALESCE((B), 0)")
        );
        assert_eq!(
            sql(r#"row6.T.subtract(row6.PCT.compareTo(new BigDecimal("100")) == 0 ? row6.A : row6.B)"#).as_deref(),
            Some("COALESCE((T), 0) - COALESCE((CASE WHEN PCT = 100 THEN A ELSE B END), 0)")
        );
        assert_eq!(
            sql("String.valueOf(row6.A)").as_deref(),
            Some("CAST(A AS VARCHAR)")
        );
    }

    #[test]
    fn a_choice_we_cannot_read_is_still_reported() {
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        assert_eq!(sql("a ? b : c"), None, "operands are not readable");
        // An ordering reads now: comparing the sign this returns against zero is how
        // Java spells the comparison itself, and it means the same thing whichever way
        // round the test is written.
        assert_eq!(
            sql("row6.PCT.compareTo(row6.X) > 0 ? row6.A : row6.B").as_deref(),
            Some("CASE WHEN PCT > X THEN A ELSE B END")
        );
        assert_eq!(sql("row6.A.compareTo(row6.B) == 1"), None, "not a boolean shape");
    }

    #[test]
    fn numeric_literals_and_exact_decimals_become_sql() {
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        // A bare number is a number.
        assert_eq!(sql("0").as_deref(), Some("0"));
        assert_eq!(sql("-1").as_deref(), Some("-1"));
        assert_eq!(
            sql("new BigDecimal(Double.valueOf(0))").as_deref(),
            Some("TRY_CAST(0 AS DOUBLE)")
        );
        // new BigDecimal("0.00") is an exact decimal, and the literal already carries the
        // scale, so writing it through keeps that without inventing a cast.
        assert_eq!(sql(r#"new BigDecimal("0")"#).as_deref(), Some("0"));
        assert_eq!(sql(r#"new BigDecimal("-1")"#).as_deref(), Some("-1"));
        assert_eq!(sql(r#"new BigDecimal("0.00")"#).as_deref(), Some("0.00"));
    }

    #[test]
    fn sign_changing_arithmetic_becomes_sql() {
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        assert_eq!(sql("row6.AMT.negate()").as_deref(), Some("-(AMT)"));
        assert_eq!(
            sql(r#"row6.AMT.multiply(new BigDecimal("-1"))"#).as_deref(),
            Some("(AMT) * (-1)")
        );
        // an operand we cannot read keeps the whole expression reported
        assert_eq!(sql("row6.AMT.multiply(somethingOdd(1,2))"), None);
    }

    #[test]
    fn a_mapper_expression_needing_judgement_is_still_reported() {
        // The point of translating the easy ones is that what remains is worth reading.
        // Anything with branching, arithmetic or an unverified index must keep warning:
        // guessing one of these wrong is a silent wrong number, not a failure.
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        assert_eq!(sql("jobName"), None, "a bare identifier is not a column");
        assert_eq!(sql("new BigDecimal(Var.ID)"), None, "exact decimal, not a double");
        assert_eq!(sql("new BigDecimal(row1.AMT)"), None, "exact decimal or double, unrecorded");
        assert_eq!(
            sql(r#"a.equals("1")?"I":"O""#),
            None,
            "the choice reads, but a bare `a` is not a column"
        );
        assert_eq!(sql(r#"TalendDate.parseDate("ddMMyyyy",Var.D)"#), None);
        assert_eq!(sql("f(a) + g(b)"), None, "not a single call");
    }

    fn caller_and_body() -> (Import, Import) {
        let caller = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="MY_BODY" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="MY_BODY_1"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="240" posY="10">
            <elementParameter name="UNIQUE_NAME" value="sink_a"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="240" posY="90">
            <elementParameter name="UNIQUE_NAME" value="sink_b"/>
            <elementParameter name="FILENAME" value="&quot;/data/b.csv&quot;"/>
          </node>
          <connection connectorName="FLOW" source="in_1" target="MY_BODY_1"/>
          <connection connectorName="OUTPUT_1" source="MY_BODY_1" target="sink_a"/>
          <connection connectorName="OUTPUT_2" source="MY_BODY_1" target="sink_b"/>
        </talendfile:ProcessType>"#;
        let body = r#"<xmi:XMI xmlns:xmi="x" xmlns:model="y"><model:JobletProcess>
          <jobletNodes componentName="INPUT" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="INPUT_1"/>
          </jobletNodes>
          <node componentName="tSortRow" posX="100" posY="10">
            <elementParameter name="UNIQUE_NAME" value="mid_1"/>
          </node>
          <jobletNodes componentName="OUTPUT" posX="200" posY="10">
            <elementParameter name="UNIQUE_NAME" value="OUTPUT_1"/>
          </jobletNodes>
          <jobletNodes componentName="OUTPUT" posX="200" posY="90">
            <elementParameter name="UNIQUE_NAME" value="OUTPUT_2"/>
          </jobletNodes>
          <connection connectorName="FLOW" source="INPUT_1" target="mid_1"/>
          <connection connectorName="OUTPUT_1" source="mid_1" target="OUTPUT_1"/>
          <connection connectorName="OUTPUT_2" source="mid_1" target="OUTPUT_2"/>
        </model:JobletProcess></xmi:XMI>"#;
        (import_item(caller, "caller").unwrap(), import_item(body, "MY_BODY").unwrap())
    }

    #[test]
    fn a_buffer_returns_its_rows_to_the_calling_job() {
        // A buffer exists to hand rows to whoever called this job. It becomes the return
        // sink, and the caller reads the same file.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tBufferOutput" posX="200" posY="10">
            <elementParameter name="UNIQUE_NAME" value="buf_1"/>
          </node>
          <connection connectorName="FLOW" source="in_1" target="buf_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "child").unwrap();
        let buf = im.nodes.iter().find(|n| n.id == "buf_1").expect("kept");
        assert_eq!(buf.data.component_id.as_deref(), Some("snk.parquet"));
        assert_eq!(
            buf.data.properties.as_ref().and_then(|p| p.get("path")).and_then(|v| v.as_str()),
            Some("${DUCKLE_RETURN}")
        );
        assert!(im.returns_rows(), "and the job says it returns rows");
        assert!(
            !im.warnings.iter().any(|w| matches!(
                w, Warning::UnmappedComponent { component, .. } if component == "tBufferOutput"
            )),
            "it has an equivalent now: {:?}",
            im.warnings
        );
    }

    #[test]
    fn a_call_that_wants_rows_says_so() {
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tRunJob" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="call_1"/>
            <elementParameter name="PROCESS" value="CHILD"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="200" posY="10">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <connection connectorName="FLOW" source="call_1" target="out_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let call = im.nodes.iter().find(|n| n.id == "call_1").unwrap();
        assert_eq!(
            call.data.properties.as_ref().and_then(|p| p.get("returnsRows")).and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn a_call_that_wants_rows_raises_nothing_on_its_own() {
        // Reading one file cannot say whether the child returns rows: that needs the child.
        // The call records what it wants and the bulk import checks the pair.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tRunJob" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="call_1"/>
            <elementParameter name="PROCESS" value="CHILD"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="200" posY="10">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <connection connectorName="FLOW" source="call_1" target="out_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert!(!im.warnings.iter().any(|w| matches!(w, Warning::ChildReturnsRows { .. })));
    }

    #[test]
    fn a_call_used_only_for_ordering_is_not_reported() {
        // Chaining jobs is the ordinary way to build a master job and carries no rows.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tRunJob" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="call_1"/>
            <elementParameter name="PROCESS" value="CHILD_A"/>
          </node>
          <node componentName="tRunJob" posX="200" posY="10">
            <elementParameter name="UNIQUE_NAME" value="call_2"/>
            <elementParameter name="PROCESS" value="CHILD_B"/>
          </node>
          <connection connectorName="SUBJOB_OK" source="call_1" target="call_2"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert!(
            !im.warnings.iter().any(|w| matches!(w, Warning::ChildReturnsRows { .. })),
            "ordering is not a data return: {:?}",
            im.warnings
        );
    }

    #[test]
    fn a_body_is_spliced_into_the_job_that_calls_it() {
        // A child pipeline runs for its side effects and is handed no rows, so a call to a
        // body that takes an input could never work. The body is inlined instead, which is
        // also what the source tool does when it generates the job.
        let (mut caller, body) = caller_and_body();
        inline_subflow(&mut caller, "MY_BODY_1", &body).expect("splices");

        let ids: Vec<&str> = caller.nodes.iter().map(|n| n.id.as_str()).collect();
        assert!(!ids.contains(&"MY_BODY_1"), "the call is replaced, not kept");
        assert!(!ids.iter().any(|i| i.contains("INPUT_1") || i.contains("OUTPUT_")),
                "the ports are wiring, not work: {ids:?}");
        assert!(ids.contains(&"MY_BODY_1__mid_1"), "the body's work arrives, got {ids:?}");

        let link = |from: &str, to: &str| {
            caller.edges.iter().any(|e| e.source == from && e.target == to)
        };
        assert!(link("in_1", "MY_BODY_1__mid_1"), "the caller's input drives the body");
        assert!(link("MY_BODY_1__mid_1", "sink_a"), "OUTPUT_1 reaches the sink it fed");
        assert!(link("MY_BODY_1__mid_1", "sink_b"), "and OUTPUT_2 reaches its own");
        assert!(
            !caller.warnings.iter().any(|w| matches!(w, Warning::ChildReturnsRows { .. })),
            "the splice folded the body in, so the call no longer stands in an empty relation"
        );
        assert!(
            !caller.warnings.iter().any(|w| matches!(
                w,
                Warning::UnmappedComponent { component, .. } if component == "INPUT" || component == "OUTPUT"
            )),
            "the splice resolved the ports, so reporting them would be false: {:?}",
            caller.warnings
        );
        assert_eq!(caller.edges.len(), 3, "no edge left dangling: {:?}",
                   caller.edges.iter().map(|e| (&e.source, &e.target)).collect::<Vec<_>>());
        assert!(caller.edges.iter().all(|e| {
            let ids: Vec<&str> = caller.nodes.iter().map(|n| n.id.as_str()).collect();
            ids.contains(&e.source.as_str()) && ids.contains(&e.target.as_str())
        }), "every edge names a node that exists");
    }

    #[test]
    fn splicing_the_same_body_twice_keeps_them_apart() {
        // Two calls to one body must not collide, or the second would silently land on the
        // first one's nodes.
        let (mut caller, body) = caller_and_body();
        let extra = PipelineNode {
            id: "MY_BODY_2".into(),
            flow_type: Some("transform".into()),
            position: Position { x: 120.0, y: 200.0 },
            data: node_data("MY_BODY_2".into(), Some("ctl.runjob".into()), None),
        };
        caller.nodes.push(extra);
        inline_subflow(&mut caller, "MY_BODY_1", &body).unwrap();
        inline_subflow(&mut caller, "MY_BODY_2", &body).unwrap();

        let ids: Vec<&str> = caller.nodes.iter().map(|n| n.id.as_str()).collect();
        assert!(ids.contains(&"MY_BODY_1__mid_1") && ids.contains(&"MY_BODY_2__mid_1"),
                "each call gets its own copy, got {ids:?}");
    }

    #[test]
    fn splicing_a_body_that_is_not_called_is_an_error() {
        let (mut caller, body) = caller_and_body();
        assert!(inline_subflow(&mut caller, "nope_1", &body).is_err());
    }

    #[test]
    fn a_path_built_by_concatenation_becomes_one_string() {
        // A path or a query is usually assembled from context values and literals joined
        // with +. Carried across verbatim it is Java, not a value, so the run has nothing
        // to resolve. Every part being a literal or a context name makes it one string.
        let j = |v: &str| rewrite_context(v);
        assert_eq!(j(r#"context.Root+context.grp+"/""#).as_deref(), Some("${Root}${grp}/"));
        assert_eq!(j(r#""/data/"+context.grp"#).as_deref(), Some("/data/${grp}"));
        assert_eq!(j("context.A").as_deref(), Some("${A}"), "the bare form still works");
        assert_eq!(
            j(r#""SELECT * FROM "+context.target_table+" WHERE x=1""#).as_deref(),
            Some("SELECT * FROM ${target_table} WHERE x=1")
        );
        // anything that is not a literal or a context name is left alone: a row reference
        // or a call means the value is computed, and guessing it would be wrong
        assert_eq!(j(r#""x"+row1.COL"#), None);
        assert_eq!(j(r#"context.A.substring(1)"#), None);
        assert_eq!(j(r#""a"+someMethod()"#), None);
        // A query field that was filled in as if it were a Java statement carries the
        // terminator too. It is not part of the value, and leaving it there made the
        // last piece stop looking like a literal, so the whole query stayed as Java.
        assert_eq!(
            j(r#""UPDATE "+context.target_tab+context.grp+" SET x = 1";"#).as_deref(),
            Some("UPDATE ${target_tab}${grp} SET x = 1")
        );
    }

    #[test]
    fn a_warehouse_write_keeps_the_action_it_was_configured_with() {
        // The legacy component records how it writes - append a row, or amend the row
        // that is already there - and the mode it lands on decides whether a re-run
        // adds rows or replaces them. Dropping it defaulted the write to a full-table
        // replace, so on a table with several writers each one erased the last.
        // One key column, one ordinary column, and the write action under test.
        let schema = r#"{"fields":[{"name":"REF_NO","talend.field.isKey":"true"},{"name":"AMT","talend.field.isKey":"false"}]}"#;
        let blob = |action: &str| {
            let json = serde_json::json!({
                "tableAction": {"storedValue": "NONE"},
                "outputAction": {"storedValue": action},
                "table": {
                    "tableName": {"storedValue": "T"},
                    "main": {"schema": {"storedValue": schema}},
                },
            });
            // The blob rides inside an XML attribute, so it arrives escaped.
            json.to_string().replace('&', "&amp;").replace('"', "&quot;")
        };
        let job = |action: &str| {
            let xml = format!(
                r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tSnowflakeOutput" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="PROPERTIES" value="{}"/>
          </node></talendfile:ProcessType>"#,
                blob(action)
            );
            import_item(&xml, "j").unwrap()
        };

        let ins = job("INSERT");
        let p = ins.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(
            p.get("mode").and_then(|v| v.as_str()),
            Some("append"),
            "an insert adds rows; it does not replace the table"
        );

        let ups = job("UPSERT");
        let p = ups.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(p.get("mode").and_then(|v| v.as_str()), Some("upsert"));
        assert_eq!(
            p.get("conflictColumns").and_then(|v| v.as_str()),
            Some("REF_NO"),
            "the key it matches on comes from the columns the schema marks as keys"
        );
    }

    #[test]
    fn an_update_only_write_is_reported_rather_than_quietly_widened() {
        // The legacy update amends rows that match and drops the rest. The nearest
        // mode here also inserts the rest, so the import is close but not equal -
        // and a difference that only shows up as extra rows in production is exactly
        // the kind that has to be said out loud at import time.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tSnowflakeOutput" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="PROPERTIES" value="{&quot;outputAction&quot;:{&quot;storedValue&quot;:&quot;UPDATE&quot;},&quot;table&quot;:{&quot;tableName&quot;:{&quot;storedValue&quot;:&quot;T&quot;}}}"/>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert!(
            im.warnings.iter().any(|w| matches!(
                w,
                Warning::WriteActionApproximated { node, action, .. }
                    if node == "out_1" && action == "UPDATE"
            )),
            "got {:?}",
            im.warnings
        );
    }

    #[test]
    fn work_after_a_parallel_join_waits_for_the_branches_it_joins() {
        // A job that forks into parallel branches and then carries on writes the join as a
        // link from the node that forked, not from the branches - so read literally, the
        // work after the join looks like a third branch and is free to run alongside the
        // two it is supposed to be waiting for. Everything downstream of the join then
        // reads whatever those branches happened to have finished writing.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tParallelize" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="fork_1"/>
          </node>
          <node componentName="tFileInputDelimited" posX="100" posY="10">
            <elementParameter name="UNIQUE_NAME" value="a_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="200" posY="10">
            <elementParameter name="UNIQUE_NAME" value="a_2"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.out&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="100" posY="90">
            <elementParameter name="UNIQUE_NAME" value="b_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/b.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="300" posY="50">
            <elementParameter name="UNIQUE_NAME" value="after_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/c.csv&quot;"/>
          </node>
          <connection connectorName="PARALLELIZE" source="fork_1" target="a_1"/>
          <connection connectorName="PARALLELIZE" source="fork_1" target="b_1"/>
          <connection connectorName="FLOW" source="a_1" target="a_2"/>
          <connection connectorName="SYNCHRONIZE" source="fork_1" target="after_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let into = |t: &str| {
            let mut v: Vec<&str> = im
                .edges
                .iter()
                .filter(|e| e.target == t)
                .map(|e| e.source.as_str())
                .collect();
            v.sort();
            v
        };
        assert_eq!(
            into("after_1"),
            vec!["a_2", "b_1"],
            "the work after the join waits for the end of every branch, not for the fork"
        );
    }

    #[test]
    fn subjobs_with_no_links_between_them_keep_the_order_they_are_declared_in() {
        // Most subjobs are not linked to each other at all: they simply run one after
        // another, in the order the file lists them at the end. Nothing in the graph said
        // so, which left a job that writes a table in one subjob and reads it in the next
        // looking like two things that could run in either order - and a reader that has
        // to prove the write happens first then cannot.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="a_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="90" posY="10">
            <elementParameter name="UNIQUE_NAME" value="a_2"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.out&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="10" posY="90">
            <elementParameter name="UNIQUE_NAME" value="b_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/b.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="10" posY="170">
            <elementParameter name="UNIQUE_NAME" value="c_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/c.csv&quot;"/>
          </node>
          <connection connectorName="FLOW" source="a_1" target="a_2"/>
          <subjob><elementParameter name="UNIQUE_NAME" value="a_1"/></subjob>
          <subjob><elementParameter name="UNIQUE_NAME" value="b_1"/></subjob>
          <subjob><elementParameter name="UNIQUE_NAME" value="c_1"/></subjob>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let into = |t: &str| {
            let mut v: Vec<&str> =
                im.edges.iter().filter(|e| e.target == t).map(|e| e.source.as_str()).collect();
            v.sort();
            v
        };
        assert_eq!(
            into("b_1"),
            vec!["a_2"],
            "the next subjob waits for the end of the one declared before it, not its start"
        );
        assert_eq!(into("c_1"), vec!["b_1"]);
    }

    #[test]
    fn subjobs_that_run_in_parallel_are_not_put_back_in_a_queue() {
        // Branches of a parallel fork are declared one after another like anything else,
        // and chaining them in that order would undo the only thing the fork is for.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tParallelize" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="fork_1"/>
          </node>
          <node componentName="tFileInputDelimited" posX="90" posY="10">
            <elementParameter name="UNIQUE_NAME" value="a_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="90" posY="90">
            <elementParameter name="UNIQUE_NAME" value="b_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/b.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="200" posY="50">
            <elementParameter name="UNIQUE_NAME" value="after_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/c.csv&quot;"/>
          </node>
          <connection connectorName="PARALLELIZE" source="fork_1" target="a_1"/>
          <connection connectorName="PARALLELIZE" source="fork_1" target="b_1"/>
          <connection connectorName="SYNCHRONIZE" source="fork_1" target="after_1"/>
          <subjob><elementParameter name="UNIQUE_NAME" value="fork_1"/></subjob>
          <subjob><elementParameter name="UNIQUE_NAME" value="a_1"/></subjob>
          <subjob><elementParameter name="UNIQUE_NAME" value="b_1"/></subjob>
          <subjob><elementParameter name="UNIQUE_NAME" value="after_1"/></subjob>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert!(
            !im.edges.iter().any(|e| e.source == "a_1" && e.target == "b_1"),
            "one branch must not be made to wait for the other: {:?}",
            im.edges.iter().map(|e| (&e.source, &e.target)).collect::<Vec<_>>()
        );
        let into_after: Vec<&str> =
            im.edges.iter().filter(|e| e.target == "after_1").map(|e| e.source.as_str()).collect();
        assert!(
            into_after.contains(&"a_1") && into_after.contains(&"b_1"),
            "and the join still waits for both: {into_after:?}"
        );
    }

    #[test]
    fn a_loop_body_still_lifts_when_the_job_declares_its_subjob_order() {
        // The body of a loop becomes a pipeline of its own, which it can only do while
        // nothing outside it feeds it. Keeping the declared order of subjobs put an edge
        // into the subjob that starts the loop, and if that edge lands on the body the
        // body stops being separable and the loop is left pointing at nothing.
        //
        // This goes through the whole import rather than calling the lifter directly:
        // the two features only meet here, which is exactly why hand-built nodes and
        // edges could not catch it.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="prev_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/prev.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="10" posY="90">
            <elementParameter name="UNIQUE_NAME" value="feed_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/feed.csv&quot;"/>
          </node>
          <node componentName="tFlowToIterate" posX="120" posY="90">
            <elementParameter name="UNIQUE_NAME" value="loop_1"/>
          </node>
          <node componentName="tFileInputDelimited" posX="220" posY="90">
            <elementParameter name="UNIQUE_NAME" value="body_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/each.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="320" posY="90">
            <elementParameter name="UNIQUE_NAME" value="body_2"/>
            <elementParameter name="FILENAME" value="&quot;/data/each.out&quot;"/>
          </node>
          <connection connectorName="FLOW" source="feed_1" target="loop_1"/>
          <connection connectorName="ITERATE" source="loop_1" target="body_1"/>
          <connection connectorName="FLOW" source="body_1" target="body_2"/>
          <subjob><elementParameter name="UNIQUE_NAME" value="prev_1"/></subjob>
          <subjob><elementParameter name="UNIQUE_NAME" value="body_1"/></subjob>
          <subjob><elementParameter name="UNIQUE_NAME" value="feed_1"/></subjob>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert_eq!(
            im.children.len(),
            1,
            "the body is still a pipeline of its own; warnings were {:?}",
            im.warnings
        );
        let body = &im.children[0];
        let mut ids: Vec<&str> = body.nodes.iter().map(|n| n.id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["body_1", "body_2"]);
        assert!(
            im.nodes
                .iter()
                .find(|n| n.id == "loop_1")
                .and_then(|n| n.data.properties.as_ref())
                .and_then(|p| p.get("pipelineRef"))
                .is_some(),
            "and the loop still names the file it became"
        );
    }

    #[test]
    fn an_ordering_link_is_never_drawn_where_it_would_close_a_loop() {
        // The link runs from the end of one subjob to the start of the next, and it is
        // the end that has to be checked: a subjob's head can be safely behind the next
        // subjob while something further down it is already downstream of that subjob.
        // Drawing it anyway produces a pipeline that cannot be ordered at all, which the
        // planner rejects outright - so the whole job is lost to gain one ordering.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="h1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="10" posY="90">
            <elementParameter name="UNIQUE_NAME" value="h2"/>
            <elementParameter name="FILENAME" value="&quot;/data/b.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="200" posY="50">
            <elementParameter name="UNIQUE_NAME" value="t1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.out&quot;"/>
          </node>
          <connection connectorName="COMPONENT_OK" source="h1" target="t1"/>
          <connection connectorName="COMPONENT_OK" source="h2" target="t1"/>
          <subjob><elementParameter name="UNIQUE_NAME" value="h1"/></subjob>
          <subjob><elementParameter name="UNIQUE_NAME" value="h2"/></subjob>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert!(
            !im.edges.iter().any(|e| e.source == "t1" && e.target == "h2"),
            "t1 already runs after h2, so it cannot also run before it: {:?}",
            im.edges.iter().map(|e| (&e.source, &e.target)).collect::<Vec<_>>()
        );

        // And the graph still has no way round to itself.
        let reachable = |from: &str| -> std::collections::BTreeSet<String> {
            let mut seen: std::collections::BTreeSet<String> = Default::default();
            let mut stack = vec![from.to_string()];
            while let Some(n) = stack.pop() {
                if !seen.insert(n.clone()) {
                    continue;
                }
                stack.extend(
                    im.edges.iter().filter(|e| e.source == n).map(|e| e.target.clone()),
                );
            }
            seen
        };
        for n in &im.nodes {
            let onward: Vec<String> = im
                .edges
                .iter()
                .filter(|e| e.source == n.id)
                .flat_map(|e| reachable(&e.target))
                .collect();
            assert!(!onward.contains(&n.id), "{} reaches itself", n.id);
        }
    }

    #[test]
    fn a_sql_step_that_changes_the_database_is_reported_not_imported_as_a_query() {
        // A SQL step returns rows and compiles into a view. A statement that changes the
        // database is not a query and cannot become one: carried across it reaches the
        // database wrapped in CREATE VIEW and fails there, which is a run-time surprise
        // for something that was knowable at import.
        let job = |query: &str| {
            let xml = format!(
                r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tDBRow" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="row_1"/>
            <elementParameter name="TYPE" value="SNOWFLAKE"/>
            <elementParameter name="QUERY" value="{query}"/>
          </node></talendfile:ProcessType>"#
            );
            import_item(&xml, "j").unwrap()
        };

        let changed = job("&quot;UPDATE t SET x = 1&quot;");
        assert!(
            changed.warnings.iter().any(|w| matches!(
                w,
                Warning::StatementNotQuery { node, verb } if node == "row_1" && verb == "UPDATE"
            )),
            "got {:?}",
            changed.warnings
        );

        // A query is left to get on with it, including one that opens with a CTE.
        for q in ["&quot;SELECT * FROM t&quot;", "&quot;WITH c AS (SELECT 1) SELECT * FROM c&quot;"] {
            let im = job(q);
            assert!(
                !im.warnings.iter().any(|w| matches!(w, Warning::StatementNotQuery { .. })),
                "{q} is a query: {:?}",
                im.warnings
            );
        }
    }

    #[test]
    fn an_output_keeps_the_condition_that_decides_which_rows_reach_it() {
        // A mapper output can carry a condition, and that is how a Talend job splits one
        // stream into branches: the same rows arrive at every output and each keeps only
        // the ones its condition holds for. Dropped, every branch keeps every row - so a
        // parse that should hand 550 rows one way and 8 another hands all 588 to both,
        // and the run still reports success.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <outputTables name="Wide" expressionFilter="row1.KIND.equals(&quot;6&quot;)" activateExpressionFilter="true">
              <mapperTableEntries name="A" expression="row1.A"/>
            </outputTables>
            <outputTables name="Narrow" expressionFilter="!row1.KIND.equals(&quot;6&quot;)" activateExpressionFilter="true">
              <mapperTableEntries name="A" expression="row1.A"/>
            </outputTables>
          </node>
          <node componentName="tFileOutputDelimited" posX="240" posY="10">
            <elementParameter name="UNIQUE_NAME" value="sink_w"/>
            <elementParameter name="FILENAME" value="&quot;/data/w.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="240" posY="90">
            <elementParameter name="UNIQUE_NAME" value="sink_n"/>
            <elementParameter name="FILENAME" value="&quot;/data/n.csv&quot;"/>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
          <connection connectorName="FLOW" source="m_1" target="sink_w" label="Wide"/>
          <connection connectorName="FLOW" source="m_1" target="sink_n" label="Narrow"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let filter = |id: &str| -> Option<String> {
            Some(
                im.nodes
                    .iter()
                    .find(|n| n.id == id)?
                    .data
                    .properties
                    .as_ref()?
                    .get("filter")?
                    .as_str()?
                    .to_string(),
            )
        };
        assert_eq!(filter("m_1__Wide").as_deref(), Some("KIND = '6'"));
        assert_eq!(filter("m_1__Narrow").as_deref(), Some("NOT (KIND = '6')"));
    }

    #[test]
    fn a_single_output_keeps_its_condition_too() {
        // The same condition on a mapper with one output. There is no branch to get
        // wrong here, just rows that should have been left behind and were not.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <outputTables name="out1" expressionFilter="!row1.NAME.endsWith(&quot;.gz&quot;)" activateExpressionFilter="true">
              <mapperTableEntries name="NAME" expression="row1.NAME"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let p = im.nodes.iter().find(|n| n.id == "m_1").unwrap().data.properties.as_ref().unwrap();
        assert_eq!(p["filter"].as_str(), Some("NOT (ends_with(NAME, '.gz'))"));
    }

    #[test]
    fn a_condition_left_off_is_not_a_condition() {
        // The file keeps the text of a condition that has been switched off. Applied, it
        // takes rows out that the job keeps.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <outputTables name="out1" expressionFilter="row1.NAME.equals(&quot;x&quot;)">
              <mapperTableEntries name="NAME" expression="row1.NAME"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let p = im.nodes.iter().find(|n| n.id == "m_1").unwrap().data.properties.as_ref().unwrap();
        assert!(p.get("filter").is_none(), "got: {:?}", p.get("filter"));
    }

    #[test]
    fn a_mapper_with_several_outputs_keeps_them_apart() {
        // A mapper can write several outputs, each with its own columns and its own
        // expression for a column they share. Read as one set they overwrite each other,
        // so every reader gets whichever was parsed last - and the outputs of a mapper
        // are usually the two halves of a decision, so that is not a near miss but the
        // wrong number, on a branch that still runs.
        //
        // Each output becomes a relation of its own, and the link that leaves the mapper
        // says by name which one it carries.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <outputTables name="Outbound">
              <mapperTableEntries name="SHARE" expression="row1.OUT_PCT"/>
              <mapperTableEntries name="ONLY_OUT" expression="row1.A"/>
            </outputTables>
            <outputTables name="Inbound">
              <mapperTableEntries name="SHARE" expression="row1.IN_PCT"/>
            </outputTables>
          </node>
          <node componentName="tFileOutputDelimited" posX="240" posY="10">
            <elementParameter name="UNIQUE_NAME" value="sink_out"/>
            <elementParameter name="FILENAME" value="&quot;/data/o.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="240" posY="90">
            <elementParameter name="UNIQUE_NAME" value="sink_in"/>
            <elementParameter name="FILENAME" value="&quot;/data/i.csv&quot;"/>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
          <connection connectorName="FLOW" source="m_1" target="sink_out" label="Outbound"/>
          <connection connectorName="FLOW" source="m_1" target="sink_in" label="Inbound"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();

        let expr = |id: &str, col: &str| -> Option<String> {
            im.nodes
                .iter()
                .find(|n| n.id == id)?
                .data
                .properties
                .as_ref()?
                .get("expressions")?
                .get(col)?
                .as_str()
                .map(str::to_string)
        };
        assert_eq!(
            expr("m_1__Outbound", "SHARE").as_deref(),
            Some("OUT_PCT"),
            "each output keeps its own expression for a shared column; nodes were {:?}",
            im.nodes.iter().map(|n| &n.id).collect::<Vec<_>>()
        );
        assert_eq!(expr("m_1__Inbound", "SHARE").as_deref(), Some("IN_PCT"));
        assert_eq!(
            expr("m_1__Inbound", "ONLY_OUT"),
            None,
            "and only the columns it actually declares"
        );

        let feeds = |target: &str| -> Vec<&str> {
            let mut v: Vec<&str> =
                im.edges.iter().filter(|e| e.target == target).map(|e| e.source.as_str()).collect();
            v.sort();
            v
        };
        assert_eq!(feeds("sink_out"), vec!["m_1__Outbound"], "the link carries the output it names");
        assert_eq!(feeds("sink_in"), vec!["m_1__Inbound"]);
        assert_eq!(feeds("m_1__Outbound"), vec!["in_1"], "both read the same input");
        assert_eq!(feeds("m_1__Inbound"), vec!["in_1"]);
        assert!(
            !im.nodes.iter().any(|n| n.id == "m_1"),
            "and the merged node is gone"
        );
    }

    #[test]
    fn an_exact_decimal_reads_when_the_file_records_the_column_type() {
        // `new BigDecimal(x)` has two constructors that disagree: the one taking a string
        // is exact, the one taking a double goes through binary floating point. Which
        // applies depends on the column's type - and the file records it, on the mapper's
        // own input table. It was refused for want of a type that was there all along.
        let job = |ty: &str| {
            let xml = format!(
                r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/d/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <inputTables name="Onboarded">
              <mapperTableEntries name="AMT" type="{ty}" nullable="true"/>
            </inputTables>
            <outputTables name="out">
              <mapperTableEntries name="TOTAL" expression="new BigDecimal(Onboarded.AMT)"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
        </talendfile:ProcessType>"#
            );
            import_item(&xml, "j").unwrap()
        };

        let exact = job("id_String");
        assert_eq!(
            exact.nodes.iter().find(|n| n.id == "m_1").unwrap().data.properties.as_ref()
                .unwrap()["expressions"]["TOTAL"].as_str(),
            Some("CAST(AMT AS DECIMAL(38,4))"),
            "a string column takes the exact constructor"
        );

        // A double-valued column still goes through binary floating point, and no type
        // recorded still means no reading at all.
        for ty in ["id_Double", "id_Float"] {
            let im = job(ty);
            assert!(
                im.warnings.iter().any(|w| matches!(w, Warning::JavaExpression { column, .. } if column == "TOTAL")),
                "{ty} is not exact and stays reported"
            );
        }
    }

    #[test]
    fn a_delimited_file_takes_its_column_names_from_the_schema_it_declares() {
        // The component counts header ROWS to skip and names its columns itself. Read as
        // "the first line is the header", the names come from whatever that line happens
        // to hold - so every column is renamed to a piece of data and every expression
        // downstream refers to something that is not there.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
            <elementParameter name="FIELDSEPARATOR" value="&quot;;&quot;"/>
            <elementParameter name="HEADER" value="1"/>
            <metadata connector="FLOW" name="in_1">
              <column name="record_type" type="id_String" nullable="true"/>
              <column name="amount" type="id_BigDecimal" nullable="true"/>
            </metadata>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let n = im.nodes.iter().find(|n| n.id == "in_1").unwrap();
        let p = n.data.properties.as_ref().unwrap();
        assert_eq!(p["delimiter"], ";");
        assert_eq!(
            p["hasHeader"], false,
            "the line it skips is not a header row to take names from"
        );
        assert_eq!(p["skipLines"], 1, "it is a line to skip");
        let schema = n.data.schema.as_ref().expect("the declared columns come across");
        assert_eq!(
            schema.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["record_type", "amount"]
        );
    }

    #[test]
    fn a_fixed_row_carries_the_values_it_was_given() {
        // The component exists to hand one row of named values downstream - a batch id, a
        // file name, an error message. They live in a TABLE parameter as column/value
        // pairs, and without them the node arrives configured with nothing: it produces
        // no columns, and every step that reads one fails on a name that is not there.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFixedFlowInput" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="row_1"/>
            <elementParameter field="TABLE" name="VALUES">
              <elementValue elementRef="SCHEMA_COLUMN" value="BATCH_ID"/>
              <elementValue elementRef="VALUE" value="context.batchID"/>
              <elementValue elementRef="SCHEMA_COLUMN" value="JOBNAME"/>
              <elementValue elementRef="VALUE" value="&quot;DAILY_LOAD&quot;"/>
            </elementParameter>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let p = im.nodes[0].data.properties.as_ref().unwrap();
        let cols = p.get("columns").and_then(|c| c.as_object()).expect("the row came across");
        assert_eq!(
            cols.get("BATCH_ID").and_then(|v| v.as_str()),
            Some("${batchID}"),
            "a context value stays one, so the run resolves it"
        );
        assert_eq!(
            cols.get("JOBNAME").and_then(|v| v.as_str()),
            Some("DAILY_LOAD"),
            "and a literal loses the quotes it was written with"
        );
    }

    #[test]
    fn the_row_a_file_loop_hands_on_is_read_from_the_list_it_loops() {
        // Iterating a folder and turning the current file into a row is the most ordinary
        // batch shape there is. The row is described as the loop's own variables, which
        // are not values this side of the move: read literally the node produces no
        // columns at all, and the step that reads one fails on a name that is not there.
        // The list already yields a row per file, so the names are taken from it.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileList" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="list_1"/>
            <elementParameter name="DIRECTORY" value="&quot;/data/in&quot;"/>
          </node>
          <node componentName="tIterateToFlow" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="row_1"/>
            <elementParameter field="TABLE" name="MAPPING">
              <elementValue elementRef="SCHEMA_COLUMN" value="File_Name_Path"/>
              <elementValue elementRef="VALUE" value="((String)globalMap.get(&quot;list_1_CURRENT_FILEPATH&quot;))"/>
              <elementValue elementRef="SCHEMA_COLUMN" value="File_Name"/>
              <elementValue elementRef="VALUE" value="((String)globalMap.get(&quot;list_1_CURRENT_FILE&quot;))"/>
            </elementParameter>
          </node>
          <connection connectorName="ITERATE" source="list_1" target="row_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let n = im.nodes.iter().find(|n| n.id == "row_1").unwrap();
        assert_eq!(
            n.data.component_id.as_deref(),
            Some("xf.map"),
            "it reads the list rather than inventing a row"
        );
        let ex = &n.data.properties.as_ref().unwrap()["expressions"];
        assert_eq!(ex["File_Name_Path"].as_str(), Some("file"));
        assert_eq!(ex["File_Name"].as_str(), Some("filename"));
        assert!(
            im.edges.iter().any(|e| e.source == "list_1"
                && e.target == "row_1"
                && e.data.as_ref().map(|d| d.connection_type.as_str()) == Some("main")),
            "and the list feeds it: {:?}",
            im.edges.iter().map(|e| (&e.source, &e.target)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_loop_row_column_is_read_as_the_value_the_loop_hands_the_child() {
        // A loop puts the row it is on where the steps inside it can reach it, by name.
        // Those names are the loop's own and mean nothing here, so a file name taken from
        // the current row arrived as the Java that would have fetched it and the step
        // tried to open a file called exactly that.
        let j = |v: &str| rewrite_context(v);
        assert_eq!(
            j(r#"((String)globalMap.get("out1.File_Name_Path"))"#).as_deref(),
            Some("${ITER_ITEM_FILE_NAME_PATH}")
        );
        assert_eq!(
            j(r#"globalMap.get("row2.SQLQUERY")"#).as_deref(),
            Some("${ITER_ITEM_SQLQUERY}")
        );
        // A component's own statistic is not a row column and is left alone.
        assert_eq!(j(r#"((String)globalMap.get("tFileList_1_CURRENT_FILE"))"#), None);
    }

    #[test]
    fn a_mapper_variable_is_worked_into_the_outputs_that_use_it() {
        // A mapper can name an intermediate value and use it in several outputs. The name
        // belongs to the mapper and nothing outside it knows the name, so an output that
        // used one referred to a column that does not exist - and the whole step failed
        // to bind, on a mapper that was otherwise translated.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/d/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <varTables name="Var">
              <mapperTableEntries name="TRIMMED" expression="StringHandling.TRIM(row1.CODE)"/>
              <mapperTableEntries name="DOUBLED" expression="Var.TRIMMED"/>
            </varTables>
            <outputTables name="out">
              <mapperTableEntries name="CODE" expression="Var.TRIMMED"/>
              <mapperTableEntries name="ALSO" expression="Var.DOUBLED"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let ex = &im.nodes.iter().find(|n| n.id == "m_1").unwrap()
            .data.properties.as_ref().unwrap()["expressions"];
        assert_eq!(
            ex["CODE"].as_str(),
            Some("trim(CODE)"),
            "the value the name stood for is worked into the output"
        );
        assert_eq!(
            ex["ALSO"].as_str(),
            Some("trim(CODE)"),
            "including through a name that stands for another name"
        );
    }

    #[test]
    fn the_ordinary_string_operations_read_as_sql() {
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        // Stripping quotes out of a field is the commonest thing a mapper does to a
        // delimited file, and it stopped the whole expression from being read.
        assert_eq!(
            sql(r#"row1.CODE.replaceAll("x","y")"#).as_deref(),
            Some("regexp_replace(CODE, 'x', 'y', 'g')")
        );
        assert_eq!(
            sql(r#"StringHandling.EREPLACE(row1.CODE,"x","y")"#).as_deref(),
            Some("regexp_replace(CODE, 'x', 'y', 'g')")
        );
        assert_eq!(sql("row1.NAME.toUpperCase()").as_deref(), Some("upper(NAME)"));
        assert_eq!(sql("row1.NAME.toLowerCase()").as_deref(), Some("lower(NAME)"));
        assert_eq!(sql("row1.NAME.length()").as_deref(), Some("length(NAME)"));
        // A comma inside a literal is part of the literal. Split on it, the call looked
        // like it had three arguments and the whole expression went unread.
        assert_eq!(
            sql(r#"row1.CODE.replaceAll("x",",")"#).as_deref(),
            Some("regexp_replace(CODE, 'x', ',', 'g')")
        );
        // Java counts from zero and takes an end; SQL counts from one and takes a length.
        assert_eq!(sql("row1.NAME.substring(2)").as_deref(), Some("substr(NAME, 2 + 1)"));
        assert_eq!(
            sql("row1.NAME.substring(2,5)").as_deref(),
            Some("substr(NAME, 2 + 1, 5 - 2)")
        );
    }

    #[test]
    fn work_that_waits_for_a_subjob_waits_for_all_of_it() {
        // "After this subjob" is written as a link out of the component the subjob starts
        // at. Read as a link out of that one component it means "after the first step",
        // which is a much weaker thing: everything the subjob went on to do - including
        // the file it wrote - was free to happen afterwards. A job that wrote a file in
        // one subjob and read it in the next then read it before it existed.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="head_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="90" posY="10">
            <elementParameter name="UNIQUE_NAME" value="mid_1"/>
            <outputTables name="o">
              <mapperTableEntries name="A" expression="row1.A"/>
            </outputTables>
          </node>
          <node componentName="tFileOutputDelimited" posX="170" posY="10">
            <elementParameter name="UNIQUE_NAME" value="write_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/mid.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="260" posY="10">
            <elementParameter name="UNIQUE_NAME" value="next_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/mid.csv&quot;"/>
          </node>
          <connection connectorName="FLOW" source="head_1" target="mid_1"/>
          <connection connectorName="FLOW" source="mid_1" target="write_1"/>
          <connection connectorName="SUBJOB_OK" source="head_1" target="next_1"/>
        </talendfile:ProcessType>"#;
        let im = im_edges(xml);
        assert!(
            im.iter().any(|(a, b)| a == "write_1" && b == "next_1"),
            "the next subjob waits for the end of this one, not its first step: {im:?}"
        );
        assert!(
            !im.iter().any(|(a, b)| a == "head_1" && b == "next_1"),
            "and no longer for the first step alone: {im:?}"
        );
    }

    #[cfg(test)]
    fn im_edges(xml: &str) -> Vec<(String, String)> {
        import_item(xml, "j")
            .unwrap()
            .edges
            .iter()
            .filter(|e| e.data.as_ref().map(|d| d.connection_type.as_str()) == Some("on-subjob-ok"))
            .map(|e| (e.source.clone(), e.target.clone()))
            .collect()
    }

    #[test]
    fn a_writer_that_puts_the_names_out_says_so() {
        // A sink says whether it writes the column names as a first line. Left unread,
        // the file goes out without them - and the step that reads it back was told to
        // skip a line, so it skips a line of data instead and the run is one row short
        // for a reason nothing reports.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileOutputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/o.csv&quot;"/>
            <elementParameter name="INCLUDEHEADER" value="true"/>
            <metadata connector="FLOW" name="out_1">
              <column name="A" type="id_String" nullable="true"/>
            </metadata>
          </node>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let p = im.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(p["hasHeader"], true);

        // And a writer that does not put them out still does not.
        let off = xml.replace(
            r#"<elementParameter name="INCLUDEHEADER" value="true"/>"#,
            r#"<elementParameter name="INCLUDEHEADER" value="false"/>"#,
        );
        let im = import_item(&off, "j").unwrap();
        let p = im.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(p["hasHeader"], false);
    }

    #[test]
    fn a_match_on_something_unreadable_is_not_guessed_at() {
        // The side of a match that could not be read used to fall back to whatever
        // followed the last dot. On `row1.File_Name.split("_")[3]` that is
        // `split("_")[3]`: the column being split is gone and the separator has become a
        // quoted NAME, so the step matched on a column called _ that does not exist.
        // A match is what decides which rows pair up, so a key that cannot be read is
        // left out and reported - the job then refuses to compile, which is the point.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="10" posY="90">
            <elementParameter name="UNIQUE_NAME" value="ref_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/ref.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <inputTables name="row1"/>
            <inputTables name="lk" innerJoin="true">
              <mapperTableEntries name="K" type="id_String" expression="row1.NAME.mysteryCall(7)"/>
            </inputTables>
            <outputTables name="out1">
              <mapperTableEntries name="K" expression="row1.NAME"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1" label="row1"/>
          <connection connectorName="FLOW" source="ref_1" target="m_1" label="lk"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let p = im.nodes.iter().find(|n| n.id == "m_1").unwrap().data.properties.as_ref().unwrap();
        let left = p
            .get("lookups")
            .and_then(|l| l.as_array())
            .and_then(|a| a.first())
            .and_then(|e| e.get("leftKey"))
            .and_then(|k| k.as_str())
            .unwrap_or("");
        assert!(
            !left.contains("mysteryCall"),
            "an unread call must not be handed on as a key: {left}"
        );
        assert!(left.is_empty(), "nothing is invented for it either: {left}");
        assert!(
            im.warnings
                .iter()
                .any(|w| matches!(w, Warning::JavaExpression { expression, .. }
                                  if expression.contains("mysteryCall"))),
            "and it is reported: {:?}",
            im.warnings
        );
    }

    #[test]
    fn adding_up_charges_counts_a_missing_one_as_nothing() {
        // A file leaves a charge blank when there is none. Adding a blank as UNKNOWN
        // makes the whole total unknown, so a row with five blank charges came out with
        // no total at all where the job it came from totals them to zero. One blank
        // charge is enough to lose the total, and the total is what gets loaded.
        //
        // Multiplying is left alone: a blank there is not a nought, and pretending it is
        // would turn a product into zero rather than leaving it unanswered.
        let mut types = ColTypes::new();
        types.insert("A".into(), "id_BigDecimal".into());
        types.insert("B".into(), "id_BigDecimal".into());
        let sql = |e: &str| java_expr_to_sql(e, &types, &Default::default());
        let added = sql("row1.A.add(row1.B)").unwrap();
        assert!(added.starts_with("COALESCE("), "got: {added}");
        assert_eq!(added.matches("COALESCE(").count(), 2, "both sides: {added}");
        assert!(added.contains(" + "), "got: {added}");

        let taken = sql("row1.A.subtract(row1.B)").unwrap();
        assert_eq!(taken.matches("COALESCE(").count(), 2, "both sides: {taken}");

        let times = sql("row1.A.multiply(row1.B)").unwrap();
        assert!(!times.contains("COALESCE("), "a blank is not a nought here: {times}");
    }

    #[test]
    fn a_column_passed_straight_through_is_not_retyped() {
        // A file arrives as text and leaves as text. Retyping a column on the way through
        // does not help anything - arithmetic casts its own operands where it happens -
        // and it destroys whatever will not parse. On a real file the later record types
        // carry a different layout, so a place name sits in a column the first layout
        // calls a charge: cast, it becomes NULL and the value is gone for good.
        //
        // A value the mapper COMPUTES is different: there the declared type is the only
        // thing that says what the result should be.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <outputTables name="out1">
              <mapperTableEntries name="THROUGH" expression="row1.CHARGE" type="id_BigDecimal"/>
              <mapperTableEntries name="COMPUTED" expression="row1.A.add(row1.B)" type="id_BigDecimal"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let p = im.nodes.iter().find(|n| n.id == "m_1").unwrap().data.properties.as_ref().unwrap();
        let ex = |c: &str| p["expressions"][c].as_str().unwrap_or("").to_string();
        assert_eq!(ex("THROUGH"), "CHARGE", "passed through untouched, got: {}", ex("THROUGH"));
        assert!(ex("COMPUTED").contains("DECIMAL"), "a computed value keeps its type: {}", ex("COMPUTED"));
    }

    #[test]
    fn a_decimal_keeps_the_scale_its_schema_declares() {
        // A settlement rate is declared with 9 decimal places. Given a fixed scale of 4
        // instead, every rate is silently ROUNDED on the way through - 1.106872543
        // arrives as 1.1069 - and a rate is money, so the whole run is wrong by a
        // rounding no one asked for and nothing reports.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <metadata connector="FLOW" name="m_1">
              <column name="RATE" type="id_BigDecimal" length="18" precision="9" nullable="true"/>
              <column name="AMOUNT" type="id_BigDecimal" length="12" precision="2" nullable="true"/>
              <column name="PLAIN" type="id_BigDecimal" nullable="true"/>
            </metadata>
            <outputTables name="out1">
              <mapperTableEntries name="RATE" expression="row1.RATE.add(row1.RATE)" type="id_BigDecimal"/>
              <mapperTableEntries name="AMOUNT" expression="row1.AMOUNT.add(row1.AMOUNT)" type="id_BigDecimal"/>
              <mapperTableEntries name="PLAIN" expression="row1.PLAIN.add(row1.PLAIN)" type="id_BigDecimal"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let p = im.nodes.iter().find(|n| n.id == "m_1").unwrap().data.properties.as_ref().unwrap();
        let ex = |c: &str| p["expressions"][c].as_str().unwrap_or("").to_string();
        // The declared WIDTH is never kept: a value wider than the database column it is
        // bound for still has to survive the journey, and held to 13 digits a 16-digit
        // charge casts to NULL and is simply gone.
        //
        // The declared SCALE is kept only where it is no deeper than the default. Deeper
        // than that and the first multiplication overflows DuckDB's 38-digit decimal and
        // ends the run, so a rate declared with 9 places is still rounded to 4.
        assert!(ex("RATE").contains("DECIMAL(38,4)"), "a deeper scale is not taken: {}", ex("RATE"));
        assert!(ex("AMOUNT").contains("DECIMAL(38,2)"), "got: {}", ex("AMOUNT"));
        // Nothing declared, so the previous fixed scale still stands.
        assert!(ex("PLAIN").contains("DECIMAL(38,4)"), "got: {}", ex("PLAIN"));
    }

    #[test]
    fn a_position_plus_a_number_is_addition_not_joining() {
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        // Java writes joining text and adding numbers the same way, so each side has to
        // say which it is. The string helpers were treated as text as a family, but the
        // ones that answer with a POSITION or a COUNT answer with a number - so
        // `INDEX(name,"_")+2` was read as joining and the SUBSTR around it was handed
        // "81" where it wanted 9, which SQL then refused outright.
        assert_eq!(
            sql(r#"StringHandling.INDEX(row1.A,"_")+2"#).as_deref(),
            Some("(instr(A, '_') - 1) + (2)")
        );
        assert_eq!(
            sql(r#"StringHandling.LEN(row1.A)+1"#).as_deref(),
            Some("(length(A)) + (1)")
        );
        // The ones that answer with text still join.
        assert_eq!(
            sql(r#"StringHandling.TRIM(row1.A) + "-""#).as_deref(),
            Some("(trim(A)) || ('-')")
        );
        assert_eq!(
            sql(r#"row1.A + "-" + row1.B"#).as_deref(),
            Some("(A) || ('-') || (B)")
        );
    }

    #[test]
    fn splitting_a_string_and_taking_a_piece_reads_as_that() {
        let sql = |e: &str| java_expr_to_sql(e, &Default::default(), &Default::default());
        // Java counts the pieces from zero and SQL counts a list from one, so the piece
        // asked for moves by one. Left untranslated the call lost the thing it was
        // splitting and the separator became a QUOTED NAME, so the step went looking for
        // a column called _ and the whole branch failed to bind.
        assert_eq!(
            sql(r#"row1.File_Name.split("_")[3]"#).as_deref(),
            Some(r#"str_split(File_Name, '_')[4]"#)
        );
        assert_eq!(
            sql(r#"row1.A.split("-")[0]"#).as_deref(),
            Some(r#"str_split(A, '-')[1]"#)
        );
        // A piece named by something other than a number cannot be moved by one without
        // knowing what it is, so it is refused rather than guessed at.
        assert_eq!(sql(r#"row1.A.split("_")[n]"#), None);
    }

    #[test]
    fn a_java_body_of_context_assignments_is_read_as_assignments() {
        let one = context_assignments("context.A = row1.X;");
        assert_eq!(one, Some(vec![("A".into(), "row1.X".into())]));

        // In order, because a later one can be built from an earlier one.
        let two = context_assignments("context.A = row1.X;\ncontext.B = context.A;");
        assert_eq!(
            two,
            Some(vec![
                ("A".into(), "row1.X".into()),
                ("B".into(), "context.A".into()),
            ])
        );

        // Comments are not statements.
        let commented = context_assignments("// set it\ncontext.A = row1.X; /* done */");
        assert_eq!(commented, Some(vec![("A".into(), "row1.X".into())]));

        // A semicolon inside text is not the end of a statement.
        let quoted = context_assignments(r#"context.A = "a;b";"#);
        assert_eq!(quoted, Some(vec![("A".into(), r#""a;b""#.into())]));
    }

    #[test]
    fn a_java_body_that_is_not_only_assignments_is_not_read_as_assignments() {
        // A body that decides something, keeps its own working value, or prints, is a
        // body with rules in it. Reading only the assignments out of one would drop the
        // rest and leave something that looks like it works.
        assert_eq!(context_assignments("if (x != null) { context.A = 0; }"), None);
        assert_eq!(context_assignments("String d = f();\ncontext.A = d;"), None);
        assert_eq!(context_assignments("System.out.println(\"hi\");"), None);
        assert_eq!(context_assignments("context.A = row1.X;\nfoo();"), None);
        assert_eq!(context_assignments(""), None);
        assert_eq!(context_assignments("   \n  "), None);
    }

    #[test]
    fn a_java_node_that_only_sets_context_becomes_nodes_that_set_them() {
        // A job works out a value in Java and later steps read it as a context name.
        // There is a component for exactly that now, so the body is carried over rather
        // than left for someone to rewrite by hand.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tJavaRow" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="jr_1"/>
            <elementParameter name="CODE" value="context.batch_date = input_row.TXNDATE;&#10;context.region = &quot;EU&quot;;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="240" posY="10">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/o.csv&quot;"/>
          </node>
          <connection connectorName="FLOW" source="in_1" target="jr_1"/>
          <connection connectorName="FLOW" source="jr_1" target="out_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let ids: Vec<&str> = im.nodes.iter().map(|n| n.id.as_str()).collect();
        assert!(!ids.contains(&"jr_1"), "the Java node is gone: {ids:?}");
        assert!(ids.contains(&"jr_1__batch_date"), "got: {ids:?}");
        assert!(ids.contains(&"jr_1__region"), "got: {ids:?}");

        let of = |id: &str| -> (String, String) {
            let n = im.nodes.iter().find(|n| n.id == id).unwrap();
            let p = n.data.properties.as_ref().unwrap();
            assert_eq!(n.data.component_id.as_deref(), Some("ctl.setvar"));
            (
                p["name"].as_str().unwrap().to_string(),
                p["value"].as_str().unwrap().to_string(),
            )
        };
        assert_eq!(of("jr_1__batch_date"), ("batch_date".into(), "TXNDATE".into()));
        assert_eq!(of("jr_1__region"), ("region".into(), "'EU'".into()));

        // Wired in order, with what came in reaching the first and what left the last.
        let edge = |from: &str, to: &str| {
            im.edges.iter().any(|e| e.source == from && e.target == to)
        };
        assert!(edge("in_1", "jr_1__batch_date"), "input reaches the first");
        assert!(edge("jr_1__batch_date", "jr_1__region"), "they run in order");
        assert!(edge("jr_1__region", "out_1"), "the last one carries rows on");
    }

    #[test]
    fn only_a_value_taken_from_the_row_is_called_out() {
        // The caveat is about WHICH row decided, so it belongs only on a value that came
        // from one. A fixed value is the same however many rows went past.
        let body = |code: &str| {
            let xml = format!(
                r#"<talendfile:ProcessType xmlns:talendfile="x">
              <node componentName="tFileInputDelimited" posX="10" posY="10">
                <elementParameter name="UNIQUE_NAME" value="in_1"/>
                <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
              </node>
              <node componentName="tJavaRow" posX="120" posY="10">
                <elementParameter name="UNIQUE_NAME" value="jr_1"/>
                <elementParameter name="CODE" value="{code}"/>
              </node>
              <connection connectorName="FLOW" source="in_1" target="jr_1"/>
            </talendfile:ProcessType>"#
            );
            let im = import_item(&xml, "j").unwrap();
            im.warnings
                .iter()
                .any(|w| matches!(w, Warning::ContextSetFromFirstRow { .. }))
        };
        assert!(body("context.a = input_row.X;"), "taken from the row");
        assert!(!body("context.a = &quot;EU&quot;;"), "a fixed value needs no caveat");
        assert!(!body("context.a = 7;"), "nor does a number");
    }

    #[test]
    fn a_java_node_that_does_more_than_set_context_is_left_as_it_was() {
        // One statement that cannot be read means the whole body stays for a person to
        // port. Carrying over the readable half would silently drop the other half.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tJavaRow" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="jr_1"/>
            <elementParameter name="CODE" value="context.a = input_row.X;&#10;String t = helper();"/>
          </node>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert_eq!(im.nodes[0].id, "jr_1");
        assert_eq!(im.nodes[0].data.component_id.as_deref(), Some("code.sql"));
        assert!(
            im.warnings.iter().any(|w| matches!(w, Warning::JavaBody { .. })),
            "it still has to be ported by hand: {:?}",
            im.warnings
        );
    }

    #[test]
    fn a_context_assignment_that_will_not_translate_leaves_the_body_alone() {
        // The body is only assignments, but one of them reads a component's own
        // statistic, which is not a value this tool can produce.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tJava" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="j_1"/>
            <elementParameter name="CODE" value="context.a = &quot;x&quot;;&#10;context.b = ((String)globalMap.get(&quot;tFileList_1_CURRENT_FILE&quot;));"/>
          </node>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert_eq!(im.nodes[0].id, "j_1", "nothing was replaced: {:?}", im.nodes[0].id);
        assert_eq!(im.nodes[0].data.component_id.as_deref(), Some("code.sql"));
    }

    #[test]
    fn a_whole_line_input_keeps_the_line_whole() {
        // This component hands on each line as it stands - one field, separators and all,
        // for something further down to pick apart. Read as an ordinary delimited file it
        // was split on whatever the line happened to contain, which is both the wrong
        // shape and, against a declared single column, a failure to read the file at all.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputFullRow" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="raw_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.txt&quot;"/>
            <elementParameter name="HEADER" value="1"/>
            <metadata connector="FLOW" name="raw_1">
              <column name="line" type="id_String" nullable="true"/>
            </metadata>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let p = im.nodes[0].data.properties.as_ref().unwrap();
        let sep = p["delimiter"].as_str().unwrap();
        assert!(
            sep.chars().all(|c| c.is_control()),
            "the separator is one the text cannot contain, got {sep:?}"
        );
        // Off, under the name the reader actually looks for. Named anything else the
        // setting is not seen at all and the reader keeps its default double quote,
        // which eats a file whose lines contain one.
        assert_eq!(
            p["quoteChar"].as_str(),
            Some(""),
            "and quoting is off too, or a line holding a quote is taken apart"
        );
        assert_eq!(p["hasHeader"], false);
        assert_eq!(p["skipLines"], 1);
    }

    #[test]
    fn joining_two_pieces_of_text_reads_as_joining_not_adding() {
        // Java writes joining text and adding numbers the same way. SQL does not, and
        // reads the one written for text as arithmetic on it, which it refuses - so an
        // expression that only glued two fields together stopped the whole step.
        let mut types = ColTypes::new();
        types.insert("A".into(), "id_String".into());
        types.insert("B".into(), "id_String".into());
        types.insert("N".into(), "id_BigDecimal".into());
        let sql = |e: &str| java_expr_to_sql(e, &types, &Default::default());

        assert_eq!(sql("row1.A + row1.B").as_deref(), Some("(A) || (B)"));
        assert_eq!(
            sql(r#"row1.A + "-" + row1.B"#).as_deref(),
            Some("(A) || ('-') || (B)"),
            "and a literal in the middle is text too"
        );
        assert_eq!(
            sql("StringHandling.TRIM(row1.A) + row1.B").as_deref(),
            Some("(trim(A)) || (B)"),
            "including when a piece has been worked on"
        );
        // Numbers are still added.
        assert_eq!(
            sql("row1.N + 1").as_deref(),
            Some("(TRY_CAST(N AS DECIMAL(38,4))) + (1)"),
            "a number read out of text says so before it is added"
        );
        // And where nothing says which it is, it is left to a person.
        let unknown = ColTypes::new();
        assert_eq!(java_expr_to_sql("row1.X + row1.Y", &unknown, &Default::default()), None);
    }

    #[test]
    fn a_context_value_in_a_mapper_stays_a_context_value() {
        // A mapper expression reads columns as `<flow>.<column>`, and a context value is
        // written the same way. Read as a column it becomes a reference to one of that
        // name: where none exists the step fails, and where one does it quietly answers
        // with the row's own value instead of the setting - which is worse.
        let types = ColTypes::new();
        let sql = |e: &str| java_expr_to_sql(e, &types, &Default::default());
        assert_eq!(sql("context.REGION_CODE").as_deref(), Some("'${REGION_CODE}'"));
        assert_eq!(
            sql(r#"context.getProperty("batch_no")"#).as_deref(),
            Some("'${batch_no}'")
        );
        assert_eq!(
            sql(r#""grp" + context.batch_no"#).as_deref(),
            Some("('grp') || ('${batch_no}')"),
            "and it joins with text like anything else"
        );
        // An ordinary column reference is still a column reference.
        assert_eq!(sql("row1.REGION_CODE").as_deref(), Some("REGION_CODE"));
    }

    #[test]
    fn a_condition_reads_the_way_it_is_written() {
        // Conditions in a mapper are ordinary Java: tests joined with and/or, negated,
        // compared. Only two shapes were understood, so a choice that turned on anything
        // else took the whole column with it - and a choice is the commonest thing a
        // mapper does.
        let types = ColTypes::new();
        let sql = |e: &str| java_expr_to_sql(e, &types, &Default::default());

        assert_eq!(
            sql(r#"row1.D.equals("2")||row1.D.equals("5") ? "S" : "P""#).as_deref(),
            Some("CASE WHEN (D = '2') OR (D = '5') THEN 'S' ELSE 'P' END")
        );
        assert_eq!(
            sql(r#"row1.A.equals("x") && row1.B.equals("y") ? 1 : 0"#).as_deref(),
            Some("CASE WHEN (A = 'x') AND (B = 'y') THEN 1 ELSE 0 END")
        );
        assert_eq!(
            sql(r#"!row1.A.equals("x") ? 1 : 0"#).as_deref(),
            Some("CASE WHEN NOT (A = 'x') THEN 1 ELSE 0 END")
        );
        assert_eq!(
            sql(r#"StringHandling.LEN(row1.A)==0 ? "z" : row1.A"#).as_deref(),
            Some("CASE WHEN length(A) = 0 THEN 'z' ELSE A END")
        );
        assert_eq!(
            sql(r#"row1.A.equalsIgnoreCase("x") ? 1 : 0"#).as_deref(),
            Some("CASE WHEN upper(A) = upper('x') THEN 1 ELSE 0 END")
        );
        assert_eq!(
            sql(r#"row1.A.startsWith("ST") ? 1 : 0"#).as_deref(),
            Some("CASE WHEN starts_with(A, 'ST') THEN 1 ELSE 0 END")
        );
        assert_eq!(
            sql(r#"row1.A.isEmpty() ? 1 : 0"#).as_deref(),
            Some("CASE WHEN A = '' THEN 1 ELSE 0 END")
        );
    }

    #[test]
    fn the_remaining_routines_read_as_sql() {
        let types = ColTypes::new();
        let sql = |e: &str| java_expr_to_sql(e, &types, &Default::default());

        // A value the loop put aside, used inside a larger expression rather than alone.
        // It is QUOTED here: the loop fills the name in as text before the run, and left
        // bare the step reads that text as the name of a column and fails to bind. Alone,
        // in a path or a file name, it is not quoted - that is handled where settings are.
        assert_eq!(
            sql(r#"StringHandling.RIGHT(((String)globalMap.get("out1.File_Name")),5)"#).as_deref(),
            Some("right('${ITER_ITEM_FILE_NAME}', 5)")
        );
        // Wrapping a number in an exact decimal is a change of type, not of value, so it
        // reads whenever what it wraps does.
        assert_eq!(
            sql(r#"new BigDecimal(String.valueOf(Mathematical.SMUL("-1",row1.AMT)))"#).as_deref(),
            Some("CAST(CAST(TRY_CAST('-1' AS DOUBLE) * TRY_CAST(AMT AS DOUBLE) AS VARCHAR) AS DECIMAL(38,4))")
        );
        // Dates.
        assert_eq!(sql("TalendDate.getCurrentDate()").as_deref(), Some("now()"));
        assert_eq!(
            sql(r#"TalendDate.formatDate("yyyyMMddHHmmss", TalendDate.getCurrentDate())"#).as_deref(),
            Some("strftime(now(), '%Y%m%d%H%M%S')")
        );
        assert_eq!(
            sql(r#"TalendDate.parseDate("ddMMyyyy", row1.D)"#).as_deref(),
            Some("strptime(D, '%d%m%Y')")
        );
        // A counter that starts somewhere and steps.
        assert_eq!(
            sql(r#"Numeric.sequence("s1",1,1)"#).as_deref(),
            Some("(1 + (row_number() OVER () - 1) * 1)")
        );
    }

    #[test]
    fn arithmetic_written_with_signs_reads_like_arithmetic() {
        // Only the method spellings of arithmetic were read. Written with signs - which
        // is how anyone writes a subtraction - the expression went unread, and with it
        // the column and everything downstream naming it.
        let mut types = ColTypes::new();
        types.insert("N".into(), "id_BigDecimal".into());
        let sql = |e: &str| java_expr_to_sql(e, &types, &Default::default());

        assert_eq!(
            sql("row1.N - row1.N").as_deref(),
            Some("(TRY_CAST(N AS DECIMAL(38,4))) - (TRY_CAST(N AS DECIMAL(38,4)))")
        );
        assert_eq!(sql("1 * 2").as_deref(), Some("(1) * (2)"));
        assert_eq!(sql("1 / 2").as_deref(), Some("(1) / (2)"));
        // Signs keep their usual precedence: the looser one splits first, so what is
        // left on either side of it stays together.
        assert_eq!(sql("1 - 2 * 3").as_deref(), Some("(1) - ((2) * (3))"));
        assert_eq!(sql("1 * 2 - 3").as_deref(), Some("((1) * (2)) - (3)"));
        // A negative number is a number, not a subtraction with nothing on its left.
        assert_eq!(sql("-1").as_deref(), Some("-1"));
        // Asking whether one string is inside another.
        assert_eq!(
            sql(r#"row1.A.contains("-") ? 1 : 0"#).as_deref(),
            Some("CASE WHEN contains(A, '-') THEN 1 ELSE 0 END")
        );
    }

    #[test]
    fn a_mapper_that_looks_something_up_says_what_it_joins_on() {
        // A mapper reads its main rows and looks the rest up. Which input is which, and
        // what they are matched on, is in the file - and without it the lookup was wired
        // in but never joined, so every column taken from it referred to something that
        // was not there and the step failed to bind.
        //
        // Two lookups also have to be told apart: sharing one port, the second one
        // replaces the first.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="main_1"/>
            <elementParameter name="FILENAME" value="&quot;/d/m.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="10" posY="90">
            <elementParameter name="UNIQUE_NAME" value="ref_1"/>
            <elementParameter name="FILENAME" value="&quot;/d/r.csv&quot;"/>
          </node>
          <node componentName="tFileInputDelimited" posX="10" posY="170">
            <elementParameter name="UNIQUE_NAME" value="ref_2"/>
            <elementParameter name="FILENAME" value="&quot;/d/s.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="140" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <inputTables name="rows">
              <mapperTableEntries name="CODE" type="id_String"/>
            </inputTables>
            <inputTables name="lk1" innerJoin="true">
              <mapperTableEntries name="CODE" expression="rows.CODE" type="id_String"/>
              <mapperTableEntries name="RATE" expression="" type="id_String"/>
            </inputTables>
            <inputTables name="lk2">
              <mapperTableEntries name="CODE" expression="rows.CODE" type="id_String"/>
            </inputTables>
            <outputTables name="o">
              <mapperTableEntries name="RATE" expression="lk1.RATE"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="main_1" target="m_1" label="rows"/>
          <connection connectorName="FLOW" source="ref_1" target="m_1" label="lk1"/>
          <connection connectorName="FLOW" source="ref_2" target="m_1" label="lk2"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let n = im.nodes.iter().find(|n| n.id == "m_1").unwrap();
        let lookups = n.data.properties.as_ref().unwrap()["lookups"].as_array().unwrap();
        assert_eq!(lookups.len(), 2, "one entry per input that is looked up");
        assert_eq!(lookups[0]["port"], "lookup_1");
        // The main side says which input it reads, because with a lookup joined there is
        // more than one place a column of that name could come from.
        assert_eq!(lookups[0]["leftKey"], "main.CODE");
        assert_eq!(lookups[0]["rightKey"], "CODE");
        assert_eq!(lookups[0]["joinType"], "inner", "the file says this one is an inner join");
        assert_eq!(lookups[1]["port"], "lookup_2");
        assert_eq!(lookups[1]["joinType"], "left", "and this one is not");

        let handle = |src: &str| {
            im.edges
                .iter()
                .find(|e| e.source == src && e.target == "m_1")
                .and_then(|e| e.target_handle.clone())
                .unwrap_or_default()
        };
        assert_eq!(handle("main_1"), "main");
        assert_eq!(handle("ref_1"), "lookup_1");
        assert_eq!(handle("ref_2"), "lookup_2", "the second lookup has a port of its own");
    }

    #[test]
    fn a_database_read_gives_its_columns_the_names_the_job_uses() {
        // A database input names its own columns and takes whatever its query returns in
        // that order. The two disagree often enough - a query selecting a column the job
        // calls something else - and carried across with the query's names, every step
        // downstream referred to a column that was not there.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tSnowflakeInput" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="read_1"/>
            <elementParameter name="PROPERTIES" value="{&quot;query&quot;:{&quot;storedValue&quot;:&quot;select A, B from T&quot;}}"/>
            <metadata connector="FLOW" name="read_1">
              <column name="ALPHA" type="id_String" nullable="true"/>
              <column name="BRAVO" type="id_String" nullable="true"/>
            </metadata>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let q = im.nodes[0].data.properties.as_ref().unwrap()["query"].as_str().unwrap();
        assert!(
            q.contains("\"ALPHA\"") && q.contains("\"BRAVO\""),
            "the columns arrive under the names the job uses: {q}"
        );
        assert!(q.contains("select A, B from T"), "and the query itself is untouched: {q}");
    }

    #[test]
    fn a_database_read_is_left_alone_when_it_fetches_fewer_columns_than_it_declares() {
        // A node often declares the whole table and fetches part of it. Then the names it
        // declares cannot be laid over what comes back one for one, and the component
        // matches them up by name instead - so the query is left exactly as it is.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tSnowflakeInput" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="read_1"/>
            <elementParameter name="PROPERTIES" value="{&quot;query&quot;:{&quot;storedValue&quot;:&quot;select ID, CODE from T&quot;}}"/>
            <metadata connector="MAIN" name="MAIN">
              <column name="ID" type="id_String" nullable="true"/>
              <column name="CODE" type="id_String" nullable="true"/>
              <column name="EXTRA" type="id_String" nullable="true"/>
            </metadata>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let q = im.nodes[0].data.properties.as_ref().unwrap()["query"].as_str().unwrap();
        assert_eq!(q, "select ID, CODE from T", "left exactly as written: {q}");
    }

    #[test]
    fn a_mapper_output_carries_the_type_its_schema_declares() {
        // A delimited file arrives as text, so a column passed straight through a mapper
        // arrives as text too, and the next step that multiplies it has a number on one
        // side and text on the other. That is fixed WHERE THE ARITHMETIC IS - every
        // operand a mapper multiplies is cast there, from the same declared types - and
        // not by retyping the column on its way through.
        //
        // Retyping on the way through looked equivalent and is not: it destroys whatever
        // does not parse. A real file carries a different layout for its later record
        // types, so a place name sits in a column the first layout calls a charge, and
        // the cast turned it into NULL for good. Only a COMPUTED value is typed here,
        // where the declared type is the one thing that says what the result should be.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/d/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <outputTables name="o">
              <mapperTableEntries name="RATE" expression="row1.RATE" type="id_BigDecimal"/>
              <mapperTableEntries name="NAME" expression="row1.NAME" type="id_String"/>
              <mapperTableEntries name="COUNT" expression="row1.C" type="id_Integer"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let ex = &im.nodes.iter().find(|n| n.id == "m_1").unwrap()
            .data.properties.as_ref().unwrap()["expressions"];
        assert_eq!(ex["RATE"].as_str(), Some("RATE"), "passed through, not retyped");
        assert_eq!(ex["COUNT"].as_str(), Some("C"), "passed through, not retyped");
        assert_eq!(ex["NAME"].as_str(), Some("NAME"), "text is left as it is");
    }

    #[test]
    fn an_output_column_with_nothing_to_compute_is_still_a_column() {
        // A mapper output can name a column and give it nothing: the row carries it, and
        // it carries nothing. Dropping it instead left the column out of the shape the
        // mapper hands on, so every later step that named it failed to bind - which is a
        // whole branch lost for a column that was only ever empty.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/d/in.csv&quot;"/>
          </node>
          <node componentName="tMap" posX="120" posY="10">
            <elementParameter name="UNIQUE_NAME" value="m_1"/>
            <outputTables name="o">
              <mapperTableEntries name="KEPT" expression="row1.A" type="id_String"/>
              <mapperTableEntries name="BLANK" type="id_String"/>
              <mapperTableEntries name="BLANK_NUM" type="id_BigDecimal"/>
            </outputTables>
          </node>
          <connection connectorName="FLOW" source="in_1" target="m_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let ex = &im.nodes.iter().find(|n| n.id == "m_1").unwrap()
            .data.properties.as_ref().unwrap()["expressions"];
        assert_eq!(ex["KEPT"].as_str(), Some("A"));
        assert_eq!(ex["BLANK"].as_str(), Some("NULL"), "named, and empty");
        assert_eq!(
            ex["BLANK_NUM"].as_str(),
            Some("TRY_CAST(NULL AS DECIMAL(38,4))"),
            "and still the type the mapper says it is"
        );
    }

    #[test]
    fn a_context_reference_in_the_tcomp_blob_resolves() {
        // A component whose configuration lives in the tcomp blob carries its connection
        // details as context references, exactly as a flat parameter does. Leaving them
        // raw meant the account, warehouse and schema arrived as the literal text
        // "context.connection_..." instead of a value the run could resolve.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tSnowflakeOutput" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="PROPERTIES" value="{&quot;connection&quot;:{&quot;account&quot;:{&quot;storedValue&quot;:&quot;context.conn_account&quot;},&quot;warehouse&quot;:{&quot;storedValue&quot;:&quot;context.conn_wh&quot;}},&quot;table&quot;:{&quot;tableName&quot;:{&quot;storedValue&quot;:&quot;T&quot;}}}"/>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        let p = im.nodes[0].data.properties.as_ref().unwrap();
        assert_eq!(p.get("account").and_then(|v| v.as_str()), Some("${conn_account}"));
        assert_eq!(p.get("warehouse").and_then(|v| v.as_str()), Some("${conn_wh}"));
        assert_eq!(
            p.get("tableName").or_else(|| p.get("table")).and_then(|v| v.as_str()),
            Some("T"),
            "a plain value is untouched"
        );
    }

    #[test]
    fn a_named_output_port_keeps_its_name() {
        // A body with several outputs links each one under its own port name. All of them
        // are row-carrying, so all of them type as main, and without the name three
        // different outputs arrive as three identical edges.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="200" posY="10">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="200" posY="80">
            <elementParameter name="UNIQUE_NAME" value="out_2"/>
            <elementParameter name="FILENAME" value="&quot;/data/b.csv&quot;"/>
          </node>
          <connection connectorName="OUTPUT_1" source="in_1" target="out_1"/>
          <connection connectorName="OUTPUT_2" source="in_1" target="out_2"/>
        </talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();

        let label = |t: &str| {
            im.edges
                .iter()
                .find(|e| e.target == t)
                .and_then(|e| e.data.as_ref())
                .and_then(|d| d.label.clone())
        };
        assert_eq!(label("out_1").as_deref(), Some("OUTPUT_1"));
        assert_eq!(label("out_2").as_deref(), Some("OUTPUT_2"));

        // a port that already has a meaning is not relabelled with its own type
        let plain = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tFileInputDelimited" posX="10" posY="10">
            <elementParameter name="UNIQUE_NAME" value="in_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/in.csv&quot;"/>
          </node>
          <node componentName="tFileOutputDelimited" posX="200" posY="10">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="FILENAME" value="&quot;/data/a.csv&quot;"/>
          </node>
          <connection connectorName="FLOW" source="in_1" target="out_1"/>
        </talendfile:ProcessType>"#;
        let im = import_item(plain, "j").unwrap();
        assert_eq!(
            im.edges[0].data.as_ref().and_then(|d| d.label.clone()),
            None,
            "FLOW is the ordinary case and needs no label"
        );
        assert_eq!(
            named_port("MAIN"),
            None,
            "MAIN is the ordinary row link too, and labelling it is noise"
        );
        assert_eq!(
            named_port("UNIQUE").as_deref(),
            Some("UNIQUE"),
            "but a port that says which of two row outputs this is must be kept"
        );
    }

    #[test]
    fn a_joblets_boundary_port_is_kept() {
        // A joblet writes its ports as <jobletNodes>, not <node>. Reading only <node>
        // dropped the port, and with it the link into the first component, which then
        // failed as "missing main input" and named the wrong node.
        let xml = r#"<xmi:XMI xmlns:xmi="x" xmlns:model="y">
          <model:JobletProcess>
            <jobletNodes componentName="INPUT" posX="10" posY="10">
              <elementParameter name="UNIQUE_NAME" value="INPUT_1"/>
            </jobletNodes>
            <node componentName="tFileOutputDelimited" posX="100" posY="10">
              <elementParameter name="UNIQUE_NAME" value="out_1"/>
              <elementParameter name="FILENAME" value="&quot;/data/out.csv&quot;"/>
            </node>
            <connection connectorName="FLOW" source="INPUT_1" target="out_1"/>
          </model:JobletProcess>
        </xmi:XMI>"#;
        let im = import_item(xml, "body").unwrap();

        assert!(
            im.nodes.iter().any(|n| n.id == "INPUT_1"),
            "the port must survive, got {:?}",
            im.nodes.iter().map(|n| &n.id).collect::<Vec<_>>()
        );
        assert_eq!(im.edges.len(), 1, "and so must the link it carries");
        assert!(
            im.warnings.iter().any(|w| matches!(
                w,
                Warning::UnmappedComponent { component, .. } if component == "INPUT"
            )),
            "a port Duckle cannot yet drive has to be reported, not dropped"
        );
    }

    #[test]
    fn a_child_job_reference_names_the_file_the_child_became() {
        // PROCESS holds the child's bare name, but pipelineRef is a path to the child
        // pipeline. Copying it verbatim left the reference dangling: measured on a real
        // corpus, 17 of 28 references resolved only once the extension was added.
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tRunJob">
            <elementParameter name="UNIQUE_NAME" value="call_1"/>
            <elementParameter name="PROCESS" value="CHILD_JOB"/>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "parent").unwrap();
        let r = im.nodes[0].data.properties.as_ref().unwrap()["pipelineRef"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(r, "CHILD_JOB.json", "the child is written under that name");
    }

    #[test]
    fn the_generic_db_node_resolves_through_its_type_parameter() {
        let xml = r#"<talendfile:ProcessType xmlns:talendfile="x">
          <node componentName="tDBOutput">
            <elementParameter name="UNIQUE_NAME" value="out_1"/>
            <elementParameter name="TYPE" value="MYSQL"/>
            <elementParameter name="TABLE" value="&quot;dim&quot;"/>
          </node></talendfile:ProcessType>"#;
        let im = import_item(xml, "j").unwrap();
        assert_eq!(im.nodes[0].data.component_id.as_deref(), Some("snk.mysql"));
    }
}
