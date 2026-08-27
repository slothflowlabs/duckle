//! Clean-room reader for Qlik QVD files (issue #88).
//!
//! A QVD file is three sections, back to back:
//!   1. a UTF-8 XML header `<QvdTableHeader>...</QvdTableHeader>` (per-field
//!      metadata + NoOfRecords + RecordByteSize), terminated by `\r\n\0`;
//!   2. a per-field symbol table (the distinct values of each column, each value
//!      prefixed by a 1-byte type tag);
//!   3. the bit-stuffed record index: `NoOfRecords * RecordByteSize` bytes at the
//!      end of the file, each record packing one symbol index per field.
//!
//! No Qlik runtime or external/Python dependency: the format is decoded directly
//! from the public spec. Both the reader and writer are verified by round-trip
//! and cross-checked against the third-party pyqvd library.

use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

struct FieldMeta {
    name: String,
    /// Byte offset of this field's symbols within the symbol-table section.
    offset: usize,
    no_of_symbols: usize,
    /// Bit offset of this field within a record (counted from the record's LSB).
    bit_offset: usize,
    bit_width: usize,
    /// Added to the read index. The Qlik sentinel `-2` means the value is NULL.
    bias: i64,
}

/// Read a QVD file into one JSON object per record (column name -> value).
pub fn read_file(path: &Path) -> Result<Vec<Value>, String> {
    let data = std::fs::read(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let tag = b"</QvdTableHeader>";
    let end = find_sub(&data, tag).ok_or("not a QVD file: no </QvdTableHeader>")? + tag.len();
    let header =
        std::str::from_utf8(&data[..end]).map_err(|_| "QVD header is not valid UTF-8".to_string())?;
    let (nrec, rbs, fields) = parse_header(header)?;
    if fields.is_empty() {
        // A 0-column table (e.g. an empty result set) carries no rows.
        return Ok(Vec::new());
    }

    // Symbol table begins right after the header + its \r\n\0 terminator.
    let mut p = end;
    while p < data.len() && matches!(data[p], 0x0d | 0x0a | 0x00) {
        p += 1;
    }
    let symtab = p;

    if nrec == 0 || rbs == 0 {
        return Ok(Vec::new());
    }
    let idx_start = data
        .len()
        .checked_sub(nrec * rbs)
        .ok_or("QVD: record index runs past the file")?;
    if idx_start < symtab {
        return Err("QVD: malformed (record index overlaps the symbol table)".into());
    }

    // Decode each field's symbol list.
    let mut symbols: Vec<Vec<Value>> = Vec::with_capacity(fields.len());
    for f in &fields {
        symbols.push(read_symbols(&data, symtab + f.offset, f.no_of_symbols)?);
    }

    // Decode the bit-stuffed records.
    let mut rows = Vec::with_capacity(nrec);
    for r in 0..nrec {
        let rec = &data[idx_start + r * rbs..idx_start + (r + 1) * rbs];
        let mut obj = Map::with_capacity(fields.len());
        for (fi, f) in fields.iter().enumerate() {
            // Per cell: index = raw bits + Bias. Nullable fields carry Bias=-2
            // and store NULL rows as raw 0 (-> index -2); any index outside the
            // symbol range (the NULL sentinel, or an unused slot) reads as NULL.
            let idx = read_bits(rec, f.bit_offset, f.bit_width) as i64 + f.bias;
            let value = if idx >= 0 && (idx as usize) < symbols[fi].len() {
                symbols[fi][idx as usize].clone()
            } else {
                Value::Null
            };
            obj.insert(f.name.clone(), value);
        }
        rows.push(Value::Object(obj));
    }
    Ok(rows)
}

fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Read `bit_width` bits at `bit_offset` from a record. A record's bytes are one
/// little-endian integer (byte 0 holds bits 0..8, byte 1 holds bits 8..16, ...),
/// and fields are packed from the LSB up - matching how QVD writes the index.
fn read_bits(rec: &[u8], bit_offset: usize, bit_width: usize) -> u64 {
    let mut v: u64 = 0;
    let n = rec.len();
    for k in 0..bit_width.min(64) {
        let bit = bit_offset + k;
        let byte = bit / 8;
        if byte >= n {
            break;
        }
        let set = (rec[byte] >> (bit % 8)) & 1;
        v |= (set as u64) << k;
    }
    v
}

fn read_symbols(data: &[u8], mut i: usize, count: usize) -> Result<Vec<Value>, String> {
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let t = *data.get(i).ok_or("QVD: symbol table truncated")?;
        i += 1;
        let v = match t {
            1 => {
                let n = read_i32(data, i)?;
                i += 4;
                Value::from(n)
            }
            2 => {
                let d = read_f64(data, i)?;
                i += 8;
                json_num(d)
            }
            4 => {
                let (s, ni) = read_cstr(data, i)?;
                i = ni;
                Value::String(s)
            }
            // Dual (number + display string): keep the display string when it
            // carries one (e.g. formatted dates/money), else the raw number.
            5 => {
                let n = read_i32(data, i)?;
                i += 4;
                let (s, ni) = read_cstr(data, i)?;
                i = ni;
                if s.is_empty() { Value::from(n) } else { Value::String(s) }
            }
            6 => {
                let d = read_f64(data, i)?;
                i += 8;
                let (s, ni) = read_cstr(data, i)?;
                i = ni;
                if s.is_empty() { json_num(d) } else { Value::String(s) }
            }
            other => return Err(format!("QVD: unknown symbol type byte {}", other)),
        };
        out.push(v);
    }
    Ok(out)
}

fn read_i32(data: &[u8], i: usize) -> Result<i32, String> {
    let b = data.get(i..i + 4).ok_or("QVD: truncated int symbol")?;
    Ok(i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn read_f64(data: &[u8], i: usize) -> Result<f64, String> {
    let b = data.get(i..i + 8).ok_or("QVD: truncated double symbol")?;
    Ok(f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
}

fn read_cstr(data: &[u8], i: usize) -> Result<(String, usize), String> {
    let nul = data[i..]
        .iter()
        .position(|&b| b == 0)
        .ok_or("QVD: unterminated string symbol")?;
    let s = String::from_utf8_lossy(&data[i..i + nul]).into_owned();
    Ok((s, i + nul + 1))
}

fn json_num(d: f64) -> Value {
    Number::from_f64(d).map(Value::Number).unwrap_or(Value::Null)
}

/// Fold one text fragment into the header being parsed.
///
/// `FieldName` APPENDS the raw fragment: a name is built from however many
/// events the parser splits it into, and a column named with leading or
/// trailing spaces keeps them. The numeric fields trim and skip blanks,
/// because whitespace-only text sits between pretty-printed tags.
fn take_text(
    fields: &mut [FieldMeta],
    nrec: &mut usize,
    rbs: &mut usize,
    in_field: bool,
    cur: &str,
    raw: &str,
) {
    if in_field && cur == "FieldName" {
        if let Some(f) = fields.last_mut() {
            f.name.push_str(raw);
        }
        return;
    }
    let txt = raw.trim();
    if txt.is_empty() {
        return;
    }
    if in_field {
        if let Some(f) = fields.last_mut() {
            match cur {
                "Offset" => f.offset = txt.parse().unwrap_or(0),
                "NoOfSymbols" => f.no_of_symbols = txt.parse().unwrap_or(0),
                "BitOffset" => f.bit_offset = txt.parse().unwrap_or(0),
                "BitWidth" => f.bit_width = txt.parse().unwrap_or(0),
                "Bias" => f.bias = txt.parse().unwrap_or(0),
                _ => {}
            }
        }
    } else {
        match cur {
            "NoOfRecords" => *nrec = txt.parse().unwrap_or(0),
            "RecordByteSize" => *rbs = txt.parse().unwrap_or(0),
            _ => {}
        }
    }
}

fn parse_header(xml: &str) -> Result<(usize, usize, Vec<FieldMeta>), String> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;
    let mut reader = Reader::from_str(xml);
    let mut nrec = 0usize;
    let mut rbs = 0usize;
    let mut fields: Vec<FieldMeta> = Vec::new();
    let mut in_field = false;
    let mut cur = String::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = e.name().as_ref().to_string();
                if name == "QvdFieldHeader" {
                    in_field = true;
                    fields.push(FieldMeta {
                        name: String::new(),
                        offset: 0,
                        no_of_symbols: 0,
                        bit_offset: 0,
                        bit_width: 0,
                        bias: 0,
                    });
                }
                cur = name;
            }
            Ok(Event::End(e)) => {
                if e.name().as_ref() == "QvdFieldHeader" {
                    in_field = false;
                }
                cur.clear();
            }
            // quick-xml 0.41 replaced unescape() with xml_content(); 0.42 made
            // it infallible AND split entity references into their own event,
            // so `A &amp; B` arrives as three events. Both feed the same
            // accumulation or a field name would keep only its last fragment.
            Ok(Event::Text(t)) => {
                let raw = t.xml_content(quick_xml::XmlVersion::Implicit1_0);
                take_text(&mut fields, &mut nrec, &mut rbs, in_field, &cur, &raw);
            }
            Ok(Event::GeneralRef(r)) => {
                let text = crate::util::xml_entity_text(&r);
                take_text(&mut fields, &mut nrec, &mut rbs, in_field, &cur, &text);
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(format!("QVD header XML: {}", e)),
            _ => {}
        }
    }
    Ok((nrec, rbs, fields))
}

// ===================== writer (snk.qvd) =====================

/// One distinct symbol value, typed the way QVD stores it.
enum Sym {
    Int(i32),
    Double(f64),
    Str(String),
}

impl Sym {
    /// Dedup key: same type + same value share one symbol (int 3 and double 3.0
    /// are distinct symbols, as in Qlik).
    fn key(&self) -> String {
        match self {
            Sym::Int(i) => format!("i{}", i),
            Sym::Double(d) => format!("d{}", d.to_bits()),
            Sym::Str(s) => format!("s{}", s),
        }
    }
    fn emit(&self, out: &mut Vec<u8>) {
        match self {
            Sym::Int(i) => {
                out.push(1);
                out.extend_from_slice(&i.to_le_bytes());
            }
            Sym::Double(d) => {
                out.push(2);
                out.extend_from_slice(&d.to_le_bytes());
            }
            Sym::Str(s) => {
                out.push(4);
                out.extend_from_slice(s.as_bytes());
                out.push(0);
            }
        }
    }
    fn byte_len(&self) -> usize {
        match self {
            Sym::Int(_) => 5,
            Sym::Double(_) => 9,
            Sym::Str(s) => s.len() + 2,
        }
    }
}

/// Classify a JSON value as a QVD symbol. None means NULL (not stored). Integers
/// outside i32 are promoted to double (QVD's int symbol is a 32-bit int; this is
/// what Qlik does too - all numbers are doubles internally). Arrays/objects, which
/// QVD's flat model has no slot for, are stored as their JSON text.
fn classify(v: &Value) -> Option<Sym> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(Sym::Int(if *b { 1 } else { 0 })),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if (i32::MIN as i64..=i32::MAX as i64).contains(&i) {
                    Some(Sym::Int(i as i32))
                } else {
                    Some(Sym::Double(i as f64))
                }
            } else if let Some(u) = n.as_u64() {
                if u <= i32::MAX as u64 {
                    Some(Sym::Int(u as i32))
                } else {
                    Some(Sym::Double(u as f64))
                }
            } else {
                Some(Sym::Double(n.as_f64().unwrap_or(0.0)))
            }
        }
        Value::String(s) => Some(Sym::Str(s.clone())),
        other => Some(Sym::Str(other.to_string())),
    }
}

/// Bits needed to hold values 0..=max (0 for max 0).
fn bits_needed(max: u64) -> usize {
    if max == 0 {
        0
    } else {
        64 - max.leading_zeros() as usize
    }
}

struct WCol {
    name: String,
    symbols: Vec<Sym>,
    index_of: HashMap<String, usize>,
    has_null: bool,
    bit_offset: usize,
    bit_width: usize,
    bias: i64,
    offset: usize,
    length: usize,
}

/// Write `rows` (JSON objects) to a QVD file. `columns` fixes the column order;
/// if empty, columns are the union of row keys in first-seen order.
pub fn write_file(path: &Path, columns: &[String], rows: &[Value]) -> Result<(), String> {
    let cols: Vec<String> = if !columns.is_empty() {
        columns.to_vec()
    } else {
        let mut seen = Vec::new();
        let mut set = std::collections::HashSet::new();
        for r in rows {
            if let Some(o) = r.as_object() {
                for k in o.keys() {
                    if set.insert(k.clone()) {
                        seen.push(k.clone());
                    }
                }
            }
        }
        seen
    };

    let mut wcols: Vec<WCol> = cols
        .iter()
        .map(|c| WCol {
            name: c.clone(),
            symbols: Vec::new(),
            index_of: HashMap::new(),
            has_null: false,
            bit_offset: 0,
            bit_width: 0,
            bias: 0,
            offset: 0,
            length: 0,
        })
        .collect();

    // Build per-column symbol tables (distinct non-null values, first-seen order).
    for r in rows {
        let obj = r.as_object();
        for wc in wcols.iter_mut() {
            let v = obj.and_then(|o| o.get(&wc.name)).unwrap_or(&Value::Null);
            match classify(v) {
                None => wc.has_null = true,
                Some(sym) => {
                    let k = sym.key();
                    if !wc.index_of.contains_key(&k) {
                        wc.index_of.insert(k, wc.symbols.len());
                        wc.symbols.push(sym);
                    }
                }
            }
        }
    }

    // Geometry: bias, bit widths, byte offsets. Nullable columns use Qlik's
    // Bias=-2 (NULL row -> raw 0 -> index -2); real index k -> raw k+2.
    let mut bit_offset = 0usize;
    let mut sym_offset = 0usize;
    for wc in wcols.iter_mut() {
        wc.bias = if wc.has_null { -2 } else { 0 };
        let nsym = wc.symbols.len() as u64;
        let max_raw = if wc.has_null {
            if nsym == 0 { 0 } else { nsym + 1 }
        } else {
            nsym.saturating_sub(1)
        };
        wc.bit_width = bits_needed(max_raw).max(1);
        wc.bit_offset = bit_offset;
        bit_offset += wc.bit_width;
        wc.offset = sym_offset;
        wc.length = wc.symbols.iter().map(Sym::byte_len).sum();
        sym_offset += wc.length;
    }
    let rbs = (bit_offset + 7) / 8;
    let nrec = rows.len();

    let mut sym_bytes = Vec::with_capacity(sym_offset);
    for wc in &wcols {
        for s in &wc.symbols {
            s.emit(&mut sym_bytes);
        }
    }

    // Bit-pack the record index, little-endian within each record.
    let mut rec_bytes = vec![0u8; nrec * rbs];
    for (ri, r) in rows.iter().enumerate() {
        let obj = r.as_object();
        let base = ri * rbs;
        for wc in &wcols {
            let v = obj.and_then(|o| o.get(&wc.name)).unwrap_or(&Value::Null);
            let raw: u64 = match classify(v) {
                None => 0,
                Some(sym) => {
                    let idx = *wc.index_of.get(&sym.key()).unwrap_or(&0) as u64;
                    if wc.bias == -2 { idx + 2 } else { idx }
                }
            };
            for k in 0..wc.bit_width {
                if (raw >> k) & 1 == 1 {
                    let bit = wc.bit_offset + k;
                    rec_bytes[base + bit / 8] |= 1 << (bit % 8);
                }
            }
        }
    }

    let header = build_header(&wcols, nrec, rbs, sym_bytes.len(), rec_bytes.len());
    let mut out = Vec::with_capacity(header.len() + 3 + sym_bytes.len() + rec_bytes.len());
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(b"\r\n\0");
    out.extend_from_slice(&sym_bytes);
    out.extend_from_slice(&rec_bytes);

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create {}: {}", parent.display(), e))?;
        }
    }
    std::fs::write(path, &out).map_err(|e| format!("write {}: {}", path.display(), e))
}

fn col_tags(wc: &WCol) -> &'static str {
    // Always emit a <Tags> element, even when empty: Qlik QVDs always include
    // one, and some readers (e.g. pyqvd) iterate <Tags> unconditionally and
    // crash on its absence. Zero-symbol (all-null) and mixed-type columns get
    // an empty <Tags></Tags>.
    const EMPTY: &str = "      <Tags></Tags>\r\n";
    if wc.symbols.is_empty() {
        return EMPTY;
    }
    if wc.symbols.iter().all(|s| matches!(s, Sym::Int(_))) {
        "      <Tags>\r\n        <String>$numeric</String>\r\n        <String>$integer</String>\r\n      </Tags>\r\n"
    } else if wc.symbols.iter().all(|s| matches!(s, Sym::Int(_) | Sym::Double(_))) {
        "      <Tags>\r\n        <String>$numeric</String>\r\n      </Tags>\r\n"
    } else if wc.symbols.iter().all(|s| matches!(s, Sym::Str(_))) {
        "      <Tags>\r\n        <String>$text</String>\r\n      </Tags>\r\n"
    } else {
        EMPTY
    }
}

fn build_header(wcols: &[WCol], nrec: usize, rbs: usize, sym_len: usize, rec_len: usize) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n<QvdTableHeader>\r\n");
    s.push_str("  <QvBuildNo>50668</QvBuildNo>\r\n");
    s.push_str("  <CreatorDoc>duckle</CreatorDoc>\r\n");
    s.push_str(&format!("  <CreateUtcTime>{}</CreateUtcTime>\r\n", utc_now_iso()));
    s.push_str("  <SourceCreateUtcTime></SourceCreateUtcTime>\r\n");
    s.push_str("  <SourceFileUtcTime></SourceFileUtcTime>\r\n");
    s.push_str("  <StaleUtcTime></StaleUtcTime>\r\n");
    s.push_str("  <TableName>Duckle</TableName>\r\n");
    s.push_str("  <SourceFileSize>-1</SourceFileSize>\r\n");
    s.push_str("  <Fields>\r\n");
    for wc in wcols {
        s.push_str("    <QvdFieldHeader>\r\n");
        s.push_str(&format!("      <FieldName>{}</FieldName>\r\n", xml_escape(&wc.name)));
        s.push_str(&format!("      <BitOffset>{}</BitOffset>\r\n", wc.bit_offset));
        s.push_str(&format!("      <BitWidth>{}</BitWidth>\r\n", wc.bit_width));
        s.push_str(&format!("      <Bias>{}</Bias>\r\n", wc.bias));
        s.push_str("      <NumberFormat>\r\n        <Type>UNKNOWN</Type>\r\n        <nDec>0</nDec>\r\n        <UseThou>0</UseThou>\r\n        <Fmt></Fmt>\r\n        <Dec></Dec>\r\n        <Thou></Thou>\r\n      </NumberFormat>\r\n");
        s.push_str(&format!("      <NoOfSymbols>{}</NoOfSymbols>\r\n", wc.symbols.len()));
        s.push_str(&format!("      <Offset>{}</Offset>\r\n", wc.offset));
        s.push_str(&format!("      <Length>{}</Length>\r\n", wc.length));
        s.push_str("      <Comment></Comment>\r\n");
        s.push_str(col_tags(wc));
        s.push_str("    </QvdFieldHeader>\r\n");
    }
    s.push_str("  </Fields>\r\n");
    s.push_str("  <Compression></Compression>\r\n");
    s.push_str(&format!("  <RecordByteSize>{}</RecordByteSize>\r\n", rbs));
    s.push_str(&format!("  <NoOfRecords>{}</NoOfRecords>\r\n", nrec));
    s.push_str(&format!("  <Offset>{}</Offset>\r\n", sym_len));
    s.push_str(&format!("  <Length>{}</Length>\r\n", rec_len));
    s.push_str("  <Comment></Comment>\r\n");
    s.push_str("  <Lineage></Lineage>\r\n");
    s.push_str("</QvdTableHeader>");
    s
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn utc_now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as i64;
    let (y, m, d) = civil_from_days(secs.div_euclid(86400));
    let rem = secs.rem_euclid(86400);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        y,
        m,
        d,
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Days-since-Unix-epoch -> (year, month, day), UTC. Howard Hinnant's algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_pyqvd_fixture() {
        // Fixture written by pyqvd: 4 rows x {id, name, amount, active}.
        let path = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixture.qvd"));
        if !path.exists() {
            return; // fixture not present in this checkout; skip.
        }
        let rows = read_file(path).expect("read qvd");
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0]["name"], Value::String("Alice".into()));
        assert_eq!(rows[0]["id"], Value::from(1));
        assert_eq!(rows[2]["amount"], json_num(30.25));
        assert_eq!(rows[3]["name"], Value::String("Dave".into()));
    }

    #[test]
    fn writer_roundtrips_nulls_multibyte_and_mixed() {
        use serde_json::json;
        // 400 rows: forces RecordByteSize >= 2 (multi-byte records, exposes the
        // little-endian bug). Includes nulls (Bias=-2), an empty string, unicode,
        // a negative, and a mixed int/float column.
        let mut rows = Vec::new();
        for i in 0..400i64 {
            rows.push(json!({
                "id": i,
                "name": if i % 7 == 0 { Value::Null } else { json!(format!("n{}", i % 50)) },
                "amount": if i % 2 == 0 { json!(i as f64 / 4.0) } else { json!(i) },
                "tag": if i == 0 { json!("") } else { json!("café") },
            }));
        }
        let dir = std::env::temp_dir().join(format!("duckle_qvd_rt_{}", rows.len()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("rt.qvd");
        let cols: Vec<String> = vec!["id", "name", "amount", "tag"].into_iter().map(String::from).collect();
        write_file(&path, &cols, &rows).expect("write qvd");
        let back = read_file(&path).expect("read qvd");
        assert_eq!(back.len(), rows.len());
        for (orig, got) in rows.iter().zip(back.iter()) {
            // id: small int round-trips exactly.
            assert_eq!(got["id"], orig["id"]);
            // name: nulls preserved, strings preserved.
            assert_eq!(got["name"], orig["name"]);
            // tag: empty string + unicode preserved.
            assert_eq!(got["tag"], orig["tag"]);
            // amount: numeric value equal (int vs float typing may differ).
            let a = orig["amount"].as_f64().unwrap();
            let b = got["amount"].as_f64().unwrap();
            assert!((a - b).abs() < 1e-9, "amount {} != {}", a, b);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
