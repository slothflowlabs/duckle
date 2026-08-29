//! #286: derive `src.xml`'s declared schema from a published XSD.
//!
//! Many government and registry feeds publish an official XSD beside the data.
//! Translating a deeply nested one into a declared schema by hand is
//! repetitive, and a typo in it is a silently mistyped column rather than an
//! error. The XSD already says what the feed contains, so read it.
//!
//! Two things this deliberately is not:
//!
//! - **Not a validator.** Nothing here checks a document against the XSD.
//!   Full-document validation on every production load is expensive and is not
//!   what the schema is wanted for; it is wanted for stable column types so the
//!   bounded Parquet path can skip per-batch inference.
//! - **Not a general XSD implementation.** Substitution groups, redefines and
//!   type derivation by extension across imports are not resolved. What is not
//!   understood is REFUSED rather than approximated, because a schema that is
//!   quietly missing half its columns costs more than one that would not build.
//!
//! The derived schema describes what Duckle's XML READER produces, which is not
//! quite what the XSD describes: attributes arrive as `@name`, a repeated child
//! arrives as an array and a nested child as an object. Deriving the abstract
//! XSD shape instead would produce declared types that fail to cast.

use std::collections::BTreeMap;

use duckle_metadata::{Column, DataType};

use crate::EngineError;

/// One child of a complex type, as declared.
#[derive(Debug, Clone)]
struct Child {
    name: String,
    /// The type name, with any namespace prefix kept - `xs:string` and a
    /// local `AddressType` are told apart by the prefix.
    type_name: Option<String>,
    /// `maxOccurs` greater than one, so the reader will produce an array.
    repeated: bool,
    /// `minOccurs="0"`, so the column can be absent.
    optional: bool,
    /// An inline `xs:complexType`, which has no name to look up.
    inline_complex: bool,
}

#[derive(Debug, Clone, Default)]
struct ComplexType {
    children: Vec<Child>,
    /// Attribute name -> type name.
    attributes: Vec<(String, Option<String>, bool)>,
}

#[derive(Debug, Default)]
struct Schema {
    /// Global element name -> its type name.
    elements: BTreeMap<String, Option<String>>,
    /// Global complex types by name.
    complex: BTreeMap<String, ComplexType>,
    /// Global simple types by name -> the built-in they restrict.
    simple: BTreeMap<String, Option<String>>,
}

/// Strip a namespace prefix: `xs:string` -> `string`, `AddressType` unchanged.
fn local(name: &str) -> &str {
    match name.rfind(':') {
        Some(i) => &name[i + 1..],
        None => name,
    }
}

/// Is this a built-in XML Schema type rather than one the document defines?
///
/// Judged by the local name against the built-in list, not by the prefix. A
/// document is free to bind the XML Schema namespace to any prefix, and several
/// real feeds use `xsd:` rather than `xs:`.
fn builtin(type_name: &str) -> Option<DataType> {
    Some(match local(type_name) {
        "string" | "normalizedString" | "token" | "NMTOKEN" | "Name" | "NCName" | "ID"
        | "IDREF" | "anyURI" | "language" | "duration" | "gYear" | "gMonth" | "gDay"
        | "gYearMonth" | "gMonthDay" | "QName" | "NOTATION" | "anySimpleType" => DataType::String,
        "boolean" => DataType::Bool,
        "byte" | "unsignedByte" | "short" | "unsignedShort" | "int" => DataType::Int32,
        "long" | "unsignedInt" | "unsignedLong" | "integer" | "nonNegativeInteger"
        | "positiveInteger" | "negativeInteger" | "nonPositiveInteger" => DataType::Int64,
        // xs:decimal is exact by definition, so it must not become a float.
        "decimal" => DataType::Decimal,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Date,
        "dateTime" => DataType::Timestamp,
        "time" => DataType::Time,
        "base64Binary" | "hexBinary" => DataType::Binary,
        _ => return None,
    })
}

/// Read the attributes of one start tag into a map.
fn attrs(e: &quick_xml::events::BytesStart) -> BTreeMap<String, String> {
    e.attributes()
        .flatten()
        .map(|a| {
            (
                a.key.as_ref().to_string(),
                a.unescape_value().map(|v| v.to_string()).unwrap_or_default(),
            )
        })
        .collect()
}

/// Parse an XSD into the little bit of it this needs.
///
/// One pass with an explicit stack rather than a recursive descent, because an
/// XSD is a stream of events and the interesting facts are all "which named
/// definition am I inside".
fn parse(xsd: &str) -> Result<Schema, EngineError> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_str(xsd);
    let config = reader.config_mut();
    config.trim_text(true);

    let mut schema = Schema::default();
    // The named complex type currently being filled, and the depth at which it
    // started so a nested inline type does not steal its children.
    let mut current: Option<(String, ComplexType)> = None;
    // The named simple type whose base has not been seen yet.
    let mut current_simple: Option<String> = None;
    let mut depth: i32 = 0;
    let mut current_depth: i32 = 0;
    // A global element's own inline complexType is stored under a private name.
    let mut buf = Vec::new();

    loop {
        let ev = reader
            .read_event_into(&mut buf)
            .map_err(|e| EngineError::Config(format!("xsd: cannot parse: {e}")))?;
        match ev {
            Event::Eof => break,
            Event::Start(ref e) | Event::Empty(ref e) => {
                let empty = matches!(ev, Event::Empty(_));
                let name = e.name().as_ref().to_string();
                let a = attrs(e);
                if !empty {
                    depth += 1;
                }
                match local(&name) {
                    // Phase 1 refuses these rather than deriving half a schema.
                    // A missing import means missing definitions, and a column
                    // list that quietly stops early is the failure mode this
                    // feature is supposed to remove.
                    "import" | "include" | "redefine" => {
                        let what = a
                            .get("schemaLocation")
                            .or_else(|| a.get("namespace"))
                            .cloned()
                            .unwrap_or_else(|| "(no schemaLocation)".into());
                        return Err(EngineError::Config(format!(
                            "xsd: this schema pulls in another one ({what}), and resolving \
                             imports is not supported yet. Deriving from it would silently \
                             produce a partial column list. Point at a self-contained schema, \
                             or declare the columns by hand."
                        )));
                    }
                    "element" => {
                        let el_name = a.get("name").cloned();
                        let ty = a.get("type").cloned();
                        match (&current, el_name) {
                            // A child inside the complex type being filled.
                            (Some(_), Some(n)) => {
                                let max = a.get("maxOccurs").map(String::as_str).unwrap_or("1");
                                let min = a.get("minOccurs").map(String::as_str).unwrap_or("1");
                                let child = Child {
                                    name: n,
                                    type_name: ty,
                                    repeated: max == "unbounded"
                                        || max.parse::<u32>().map(|v| v > 1).unwrap_or(false),
                                    optional: min == "0",
                                    inline_complex: false,
                                };
                                if let Some((_, ct)) = current.as_mut() {
                                    ct.children.push(child);
                                }
                            }
                            // A global element declaration.
                            (None, Some(n)) => {
                                schema.elements.insert(n.clone(), ty);
                                // Its inline complexType, if it has one, is
                                // stored under a name only this module uses.
                                if !empty {
                                    current = Some((inline_name(&n), ComplexType::default()));
                                    current_depth = depth;
                                    schema.elements.insert(n.clone(), {
                                        let existing = schema.elements.get(&n).cloned().flatten();
                                        existing.or_else(|| Some(inline_name(&n)))
                                    });
                                }
                            }
                            // A ref= child, or an element with no name.
                            _ => {
                                if let (Some(ct), Some(r)) =
                                    (current.as_mut(), a.get("ref").cloned())
                                {
                                    let max = a.get("maxOccurs").map(String::as_str).unwrap_or("1");
                                    let min = a.get("minOccurs").map(String::as_str).unwrap_or("1");
                                    ct.1.children.push(Child {
                                        name: local(&r).to_string(),
                                        // Resolved later against the global
                                        // element of the same name.
                                        type_name: None,
                                        repeated: max == "unbounded"
                                            || max.parse::<u32>().map(|v| v > 1).unwrap_or(false),
                                        optional: min == "0",
                                        inline_complex: false,
                                    });
                                }
                            }
                        }
                    }
                    "complexType" => {
                        if let Some(n) = a.get("name") {
                            current = Some((n.clone(), ComplexType::default()));
                            current_depth = depth;
                        }
                    }
                    "simpleType" => {
                        if let Some(n) = a.get("name") {
                            schema.simple.insert(n.clone(), None);
                            current_simple = Some(n.clone());
                        }
                    }
                    "restriction" | "extension" => {
                        // The base belongs to the simple type currently OPEN,
                        // held by name. It used to be written to
                        // `simple.iter_mut().last()`, and a BTreeMap iterates by
                        // KEY order rather than insertion order - so with two
                        // named types the second one's base landed on whichever
                        // sorted last, found it already set, and was dropped.
                        // The type that lost its base fell back to text, which
                        // for an xs:decimal is the money column this module
                        // exists to keep exact.
                        //
                        // Taken, not just read: only the restriction directly
                        // inside the named type counts, so an inline simpleType
                        // nested within it cannot overwrite the answer.
                        if let (Some(base), Some(name)) = (a.get("base"), current_simple.take()) {
                            schema.simple.insert(name, Some(base.clone()));
                        }
                    }
                    "attribute" => {
                        if let (Some(ct), Some(n)) = (current.as_mut(), a.get("name")) {
                            let optional =
                                a.get("use").map(|u| u != "required").unwrap_or(true);
                            ct.1.attributes.push((n.clone(), a.get("type").cloned(), optional));
                        }
                    }
                    _ => {}
                }
            }
            Event::End(ref e) => {
                let name = e.name().as_ref().to_string();
                depth -= 1;
                if matches!(local(&name), "complexType" | "element") && depth < current_depth {
                    if let Some((n, ct)) = current.take() {
                        schema.complex.insert(n, ct);
                    }
                    current_depth = 0;
                }
            }
            _ => {}
        }
        buf.clear();
    }
    if let Some((n, ct)) = current.take() {
        schema.complex.insert(n, ct);
    }
    Ok(schema)
}

/// The private name under which a global element's inline complexType is kept.
fn inline_name(element: &str) -> String {
    format!("#inline:{element}")
}

/// Follow a named simple type down to the built-in it restricts.
fn resolve_simple(schema: &Schema, type_name: &str) -> Option<DataType> {
    if let Some(t) = builtin(type_name) {
        return Some(t);
    }
    let mut seen = 0;
    let mut name = local(type_name).to_string();
    // Bounded, so a schema that defines a type in terms of itself cannot spin.
    while seen < 16 {
        let base = schema.simple.get(&name)?.clone()?;
        if let Some(t) = builtin(&base) {
            return Some(t);
        }
        name = local(&base).to_string();
        seen += 1;
    }
    None
}

/// The complex type of an element, looked up by the element's own name.
fn type_of_element<'a>(schema: &'a Schema, element: &str) -> Option<&'a ComplexType> {
    let ty = schema.elements.get(element).cloned().flatten()?;
    schema
        .complex
        .get(local(&ty))
        .or_else(|| schema.complex.get(&ty))
}

/// The complex type a child element resolves to, whether by its own `type=` or
/// by being a `ref=` to a global element.
fn type_of_child<'a>(schema: &'a Schema, child: &Child) -> Option<&'a ComplexType> {
    match &child.type_name {
        Some(t) => schema
            .complex
            .get(local(t))
            .or_else(|| schema.complex.get(t.as_str())),
        None => type_of_element(schema, &child.name),
    }
}

/// Derive the declared columns for the element at `row_path`.
///
/// `row_path` is the same slash-separated path `src.xml` already takes, and it
/// is walked from the schema's global element of the first segment.
pub fn derive(xsd: &str, row_path: &str) -> Result<Vec<Column>, EngineError> {
    let schema = parse(xsd)?;
    let segments: Vec<&str> = row_path
        .split('/')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(local)
        .collect();
    if segments.is_empty() {
        return Err(EngineError::Config(
            "xsd: needs a row path saying which element is one row (e.g. Root/Enterprises/Enterprise)"
                .into(),
        ));
    }

    // Walk down to the row element's type.
    let root = segments[0];
    let mut ct = type_of_element(&schema, root).ok_or_else(|| {
        EngineError::Config(format!(
            "xsd: the schema has no global element {root:?}. It declares: {}",
            names(&schema)
        ))
    })?;
    let mut walked = vec![root.to_string()];
    for seg in &segments[1..] {
        let child = ct
            .children
            .iter()
            .find(|c| local(&c.name) == *seg)
            .ok_or_else(|| {
                EngineError::Config(format!(
                    "xsd: {} has no child element {seg:?}. It has: {}",
                    walked.join("/"),
                    ct.children
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;
        ct = type_of_child(&schema, child).ok_or_else(|| {
            EngineError::Config(format!(
                "xsd: {}/{seg} is not a complex type in this schema, so it has no columns \
                 to derive. Point the row path at an element that contains fields.",
                walked.join("/")
            ))
        })?;
        walked.push(seg.to_string());
    }

    let mut out: Vec<Column> = Vec::new();
    // Attributes first, matching the order the reader inserts them, and named
    // the way the reader names them.
    for (name, ty, optional) in &ct.attributes {
        out.push(Column {
            name: format!("@{name}"),
            data_type: ty
                .as_deref()
                .and_then(|t| resolve_simple(&schema, t))
                .unwrap_or(DataType::String),
            nullable: *optional,
            primary_key: None,
            format: None,
        });
    }
    for child in &ct.children {
        // A repeated child arrives as a JSON array and a nested one as an
        // object. Declaring the scalar type the XSD gives would produce a cast
        // that fails on every row, so the honest declaration is the text of
        // what the reader actually produced.
        let complex = child.inline_complex || type_of_child(&schema, child).is_some();
        let data_type = if child.repeated || complex {
            DataType::String
        } else {
            child
                .type_name
                .as_deref()
                .and_then(|t| resolve_simple(&schema, t))
                .unwrap_or(DataType::String)
        };
        out.push(Column {
            name: child.name.clone(),
            data_type,
            // A repeated or nested child can be absent from a given row even
            // when the XSD requires it once, and the cast is a TRY_CAST anyway.
            nullable: child.optional || child.repeated || complex,
            primary_key: None,
            format: None,
        });
    }
    if out.is_empty() {
        return Err(EngineError::Config(format!(
            "xsd: {} has no fields to derive - no child elements and no attributes",
            walked.join("/")
        )));
    }
    Ok(out)
}

fn names(schema: &Schema) -> String {
    let mut v: Vec<&str> = schema
        .elements
        .keys()
        .map(String::as_str)
        .filter(|k| !k.starts_with("#inline:"))
        .collect();
    v.sort();
    if v.is_empty() {
        "nothing".to_string()
    } else {
        v.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CBE: &str = r#"<?xml version="1.0"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:simpleType name="EnterpriseNumber">
    <xs:restriction base="xs:string"/>
  </xs:simpleType>
  <xs:complexType name="AddressType">
    <xs:sequence>
      <xs:element name="Street" type="xs:string"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="EnterpriseType">
    <xs:sequence>
      <xs:element name="Number" type="EnterpriseNumber"/>
      <xs:element name="Employees" type="xs:int"/>
      <xs:element name="Turnover" type="xs:decimal" minOccurs="0"/>
      <xs:element name="StartDate" type="xs:date"/>
      <xs:element name="Active" type="xs:boolean"/>
      <xs:element name="Address" type="AddressType"/>
      <xs:element name="Activity" type="xs:string" maxOccurs="unbounded"/>
    </xs:sequence>
    <xs:attribute name="id" type="xs:long" use="required"/>
    <xs:attribute name="lang" type="xs:string"/>
  </xs:complexType>
  <xs:complexType name="EnterprisesType">
    <xs:sequence>
      <xs:element name="Enterprise" type="EnterpriseType" maxOccurs="unbounded"/>
    </xs:sequence>
  </xs:complexType>
  <xs:complexType name="RootType">
    <xs:sequence>
      <xs:element name="Enterprises" type="EnterprisesType"/>
    </xs:sequence>
  </xs:complexType>
  <xs:element name="Root" type="RootType"/>
</xs:schema>"#;

    fn col<'a>(cols: &'a [Column], name: &str) -> &'a Column {
        cols.iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no column {name} in {:?}", cols.iter().map(|c| &c.name).collect::<Vec<_>>()))
    }

    #[test]
    fn a_nested_row_path_reaches_the_right_element() {
        let cols = derive(CBE, "Root/Enterprises/Enterprise").unwrap();
        assert_eq!(col(&cols, "Employees").data_type, DataType::Int32);
        assert_eq!(col(&cols, "StartDate").data_type, DataType::Date);
        assert_eq!(col(&cols, "Active").data_type, DataType::Bool);
    }

    /// xs:decimal is exact by definition. Mapping it to a float would lose
    /// money, which is exactly what these feeds carry.
    #[test]
    fn decimal_does_not_become_a_float() {
        let cols = derive(CBE, "Root/Enterprises/Enterprise").unwrap();
        assert_eq!(col(&cols, "Turnover").data_type, DataType::Decimal);
    }

    /// A named simple type is followed down to the built-in it restricts,
    /// rather than falling back to text.
    #[test]
    fn a_named_simple_type_resolves_to_its_base() {
        let cols = derive(CBE, "Root/Enterprises/Enterprise").unwrap();
        assert_eq!(col(&cols, "Number").data_type, DataType::String);
    }

    /// The reader gives `@name` for attributes; declaring `id` would declare a
    /// column that never arrives.
    #[test]
    fn attributes_are_named_the_way_the_reader_names_them() {
        let cols = derive(CBE, "Root/Enterprises/Enterprise").unwrap();
        assert_eq!(col(&cols, "@id").data_type, DataType::Int64);
        assert!(!col(&cols, "@id").nullable, "use=required");
        assert!(col(&cols, "@lang").nullable, "no use= means optional");
    }

    /// A repeated child arrives as an array and a nested one as an object.
    /// Declaring the scalar type the XSD gives would fail to cast on every row.
    #[test]
    fn repeated_and_nested_children_are_declared_as_text() {
        let cols = derive(CBE, "Root/Enterprises/Enterprise").unwrap();
        assert_eq!(col(&cols, "Activity").data_type, DataType::String);
        assert!(col(&cols, "Activity").nullable);
        assert_eq!(col(&cols, "Address").data_type, DataType::String);
    }

    #[test]
    fn minoccurs_zero_is_nullable_and_the_default_is_not() {
        let cols = derive(CBE, "Root/Enterprises/Enterprise").unwrap();
        assert!(col(&cols, "Turnover").nullable);
        assert!(!col(&cols, "Employees").nullable);
    }

    /// Two named simple types, with the alphabetically LAST one declared FIRST.
    ///
    /// The base was recorded against `simple.iter_mut().last()`, and a BTreeMap
    /// iterates by KEY order rather than insertion order - so the second type's
    /// base landed on the first type (already set, so silently dropped) and the
    /// second stayed unresolved. It then fell back to text, which for a money
    /// column is exactly the loss `decimal_does_not_become_a_float` exists to
    /// prevent - and that test passes with this bug present, because it declares
    /// only ONE named simple type.
    #[test]
    fn a_second_named_simple_type_resolves_too() {
        let xsd = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:simpleType name="ZipCode">
            <xs:restriction base="xs:string"/>
          </xs:simpleType>
          <xs:simpleType name="Amount">
            <xs:restriction base="xs:decimal"/>
          </xs:simpleType>
          <xs:complexType name="T">
            <xs:sequence>
              <xs:element name="zip" type="ZipCode"/>
              <xs:element name="paid" type="Amount"/>
            </xs:sequence>
          </xs:complexType>
          <xs:element name="R" type="T"/>
        </xs:schema>"#;
        let cols = derive(xsd, "R").unwrap();
        assert_eq!(col(&cols, "zip").data_type, DataType::String);
        assert_eq!(
            col(&cols, "paid").data_type,
            DataType::Decimal,
            "the second named simple type must resolve to its base, not fall back to text"
        );
    }

    /// A wrong path is a mistake worth naming, with the alternatives, rather
    /// than an empty column list.
    #[test]
    fn a_path_the_schema_does_not_describe_says_what_it_does() {
        let e = derive(CBE, "Root/Enterprises/Company").unwrap_err().to_string();
        assert!(e.contains("Company"), "got: {e}");
        assert!(e.contains("Enterprise"), "must list what is there; got: {e}");

        let e = derive(CBE, "Nope").unwrap_err().to_string();
        assert!(e.contains("Root"), "must list the global elements; got: {e}");
    }

    /// A partial column list derived from half a schema is the failure this
    /// feature exists to remove, so an unresolved import stops rather than
    /// deriving what it can see.
    #[test]
    fn an_import_is_refused_rather_than_half_derived() {
        let xsd = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
          <xs:import namespace="urn:other" schemaLocation="other.xsd"/>
          <xs:element name="Root" type="xs:string"/>
        </xs:schema>"#;
        let e = derive(xsd, "Root").unwrap_err().to_string();
        assert!(e.contains("other.xsd"), "must name what it needed; got: {e}");
        assert!(e.contains("partial"), "must say why it refused; got: {e}");
    }

    /// A prefix other than `xs:` is common in real feeds and binds the same
    /// namespace.
    #[test]
    fn the_xsd_prefix_does_not_have_to_be_xs() {
        let xsd = r#"<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <xsd:complexType name="T">
            <xsd:sequence><xsd:element name="n" type="xsd:int"/></xsd:sequence>
          </xsd:complexType>
          <xsd:element name="R" type="T"/>
        </xsd:schema>"#;
        let cols = derive(xsd, "R").unwrap();
        assert_eq!(col(&cols, "n").data_type, DataType::Int32);
    }
}
