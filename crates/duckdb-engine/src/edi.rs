//! src.edi: X12 and EDIFACT interchanges, as trading partners exchange orders,
//! invoices and shipping notices, read as one row per segment. Each row carries
//! the envelope it sits in (interchange, group and message control numbers,
//! sender, receiver, message type) and its elements as lists of components, so
//! SQL picks out what a mapping needs: `elements[3][1]` is BEG03 of an 850.
//!
//! The delimiters come from the interchange itself: X12's ISA segment is fixed
//! width and names them in place, EDIFACT's optional UNA says them, with the
//! defaults when it is absent. EDIFACT's release character is resolved.

use crate::orc::orc_files;
use crate::*;

/// The separators one interchange uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Syntax {
    pub element: char,
    pub component: char,
    pub segment: char,
    /// EDIFACT's release (escape) character; X12 has none.
    pub release: Option<char>,
    pub x12: bool,
}

/// The syntax an interchange starting at `text` declares, and where its
/// segments start: after the UNA, which is not a segment.
pub(crate) fn syntax(text: &str) -> Result<(Syntax, usize), String> {
    if text.starts_with("ISA") {
        let chars: Vec<char> = text.chars().take(106).collect();
        if chars.len() < 106 {
            return Err("an ISA segment is 106 characters, and this one is cut short".into());
        }
        return Ok((Syntax { element: chars[3], component: chars[104], segment: chars[105], release: None, x12: true }, 0));
    }
    if let Some(una) = text.strip_prefix("UNA") {
        // UNA:+.? ' gives component, element, decimal, release, reserved, segment.
        let c: Vec<char> = una.chars().take(6).collect();
        if c.len() < 6 {
            return Err("a UNA segment names six characters, and this one is cut short".into());
        }
        let release = (c[3] != ' ').then_some(c[3]);
        let skip = 3 + c.iter().map(|ch| ch.len_utf8()).sum::<usize>();
        return Ok((Syntax { element: c[1], component: c[0], segment: c[5], release, x12: false }, skip));
    }
    if text.starts_with("UNB") {
        return Ok((Syntax { element: '+', component: ':', segment: '\'', release: Some('?'), x12: false }, 0));
    }
    Err("this is neither an X12 interchange (it would start ISA) nor an EDIFACT one (UNA or UNB)".into())
}

/// `text` split at each `sep` the release character does not escape, the
/// release characters left in for the next split to see.
fn split_escaped(text: &str, sep: char, release: Option<char>) -> Vec<String> {
    let (mut out, mut cur, mut chars) = (Vec::new(), String::new(), text.chars());
    while let Some(c) = chars.next() {
        if Some(c) == release {
            cur.push(c);
            if let Some(next) = chars.next() {
                cur.push(next);
            }
        } else if c == sep {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    out.push(cur);
    out
}

/// `text` with each release character dropped and what it escaped kept.
fn unescape(text: &str, release: Option<char>) -> String {
    let Some(r) = release else { return text.to_string() };
    let (mut out, mut chars) = (String::new(), text.chars());
    while let Some(c) = chars.next() {
        if c == r {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// One segment: its tag, and each element as its components.
pub(crate) fn segment(raw: &str, s: &Syntax) -> (String, Vec<Vec<String>>) {
    let mut parts = split_escaped(raw, s.element, s.release).into_iter();
    let tag = unescape(parts.next().unwrap_or_default().trim(), s.release);
    // ISA16 is the component separator itself, so ISA is not split on it.
    let whole = s.x12 && tag == "ISA";
    let elements = parts
        .map(|e| {
            if whole {
                vec![e]
            } else {
                split_escaped(&e, s.component, s.release).iter().map(|c| unescape(c, s.release)).collect()
            }
        })
        .collect();
    (tag, elements)
}

/// A file's text: UTF-8 when it is, Latin-1 (as EDIFACT's UNOC is) when not.
pub(crate) fn decode(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => bytes.iter().map(|&b| char::from(b)).collect(),
    }
}

/// Where a segment sits: the envelope it belongs to, as far as it is open.
#[derive(Debug, Default, Clone)]
struct Envelope {
    interchange: Option<String>,
    sender: Option<String>,
    receiver: Option<String>,
    group: Option<String>,
    message: Option<String>,
    message_type: Option<String>,
}

fn element(elements: &[Vec<String>], i: usize, c: usize) -> Option<String> {
    elements.get(i).and_then(|e| e.get(c)).map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

/// Every segment of every interchange in `text`, as rows. An interchange the
/// file ends before closing is an error: the file is cut short.
pub(crate) fn rows(file: &str, text: &str) -> Result<Vec<JsonValue>, String> {
    let mut out = Vec::new();
    let mut rest = text.trim_start_matches('\u{feff}').trim_start();
    let mut position = 0_u64;
    while !rest.is_empty() {
        let (s, skip) = syntax(rest)?;
        let body = &rest[skip..];
        let mut env = Envelope::default();
        let mut closed_at: Option<usize> = None;
        let mut offset = 0;
        for raw in split_escaped(body, s.segment, s.release) {
            offset += raw.len() + s.segment.len_utf8();
            let raw = raw.trim_matches(|c: char| c == '\r' || c == '\n' || c == ' ' || c == '\t');
            if raw.is_empty() {
                continue;
            }
            let (tag, elements) = segment(raw, &s);
            // The envelope opens on its header, so the header row carries it.
            match (s.x12, tag.as_str()) {
                (true, "ISA") => {
                    env = Envelope {
                        interchange: element(&elements, 12, 0),
                        sender: element(&elements, 5, 0),
                        receiver: element(&elements, 7, 0),
                        ..Default::default()
                    }
                }
                (true, "GS") => env.group = element(&elements, 5, 0),
                (true, "ST") => {
                    env.message = element(&elements, 1, 0);
                    env.message_type = element(&elements, 0, 0);
                }
                (false, "UNB") => {
                    env = Envelope {
                        interchange: element(&elements, 4, 0),
                        sender: element(&elements, 1, 0),
                        receiver: element(&elements, 2, 0),
                        ..Default::default()
                    }
                }
                (false, "UNG") => env.group = element(&elements, 4, 0),
                (false, "UNH") => {
                    env.message = element(&elements, 0, 0);
                    env.message_type = element(&elements, 1, 0);
                }
                _ => {}
            }
            position += 1;
            out.push(serde_json::json!({
                "file": file,
                "standard": if s.x12 { "X12" } else { "EDIFACT" },
                "interchange_control": env.interchange,
                "sender": env.sender,
                "receiver": env.receiver,
                "group_control": env.group,
                "message_control": env.message,
                "message_type": env.message_type,
                "position": position.to_string(),
                "segment": tag,
                "elements": serde_json::to_string(&elements).unwrap_or_default(),
            }));
            // And closes after its trailer, so the trailer row carries it too.
            match (s.x12, tag.as_str()) {
                (true, "SE") | (false, "UNT") => {
                    env.message = None;
                    env.message_type = None;
                }
                (true, "GE") | (false, "UNE") => env.group = None,
                (true, "IEA") | (false, "UNZ") => {
                    closed_at = Some(skip + offset);
                    break;
                }
                _ => {}
            }
        }
        let Some(end) = closed_at else {
            return Err(format!(
                "the interchange {} has no {} trailer: the file is cut short",
                env.interchange.as_deref().unwrap_or("at its start"),
                if s.x12 { "IEA" } else { "UNZ" }
            ));
        };
        rest = rest.get(end.min(rest.len())..).unwrap_or_default().trim_start();
    }
    Ok(out)
}

const COLUMNS: &str = "'file': 'VARCHAR', 'standard': 'VARCHAR', 'interchange_control': 'VARCHAR', 'sender': 'VARCHAR', \
    'receiver': 'VARCHAR', 'group_control': 'VARCHAR', 'message_control': 'VARCHAR', 'message_type': 'VARCHAR', \
    'position': 'VARCHAR', 'segment': 'VARCHAR', 'elements': 'VARCHAR'";
const SELECT_LIST: &str = "\"file\", \"standard\", \"interchange_control\", \"sender\", \"receiver\", \"group_control\", \
    \"message_control\", \"message_type\", CAST(\"position\" AS BIGINT) AS \"position\", \"segment\", \
    from_json(\"elements\", '[[\"VARCHAR\"]]') AS \"elements\"";

impl DuckdbEngine {
    /// src.edi: every interchange in every file the path names, a row a segment.
    pub(crate) fn run_edi_source(&self, db: &Path, spec: &plan::EdiSourceSpec) -> Result<String, EngineError> {
        let files = orc_files(&spec.path).map_err(|e| EngineError::Query(format!("edi: {e}")))?;
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let mut count = 0_usize;
        for file in &files {
            self.check_cancelled()?;
            let fail = |e: &dyn std::fmt::Display| EngineError::Query(format!("edi: {}: {e}", file.display()));
            let bytes = std::fs::read(file).map_err(|e| fail(&e))?;
            let name = file.to_string_lossy().replace('\\', "/");
            for row in rows(&name, &decode(&bytes)).map_err(|e| fail(&e))? {
                writer.write_row(&row)?;
                count += 1;
            }
        }
        writer.finalize_typed(&self.bin, db, &spec.node_id, COLUMNS, SELECT_LIST)?;
        Ok(format!("edi: read {count} segment(s) from {} file(s) into {}", files.len(), spec.node_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ISA: &str = "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *261119*1000*U*00401*000000001*0*P*>~";

    #[test]
    fn an_interchange_names_its_own_delimiters() {
        assert_eq!(ISA.len(), 106);
        let (s, skip) = syntax(ISA).unwrap();
        assert_eq!((s.element, s.component, s.segment, s.release, s.x12, skip), ('*', '>', '~', None, true, 0));
        let (s, skip) = syntax("UNA:+.? 'UNB+UNOC:3'").unwrap();
        assert_eq!((s.element, s.component, s.segment, s.release, s.x12, skip), ('+', ':', '\'', Some('?'), false, 9));
        let (s, _) = syntax("UNA|^.? !UNB^UNOC|3!").unwrap();
        assert_eq!((s.element, s.component, s.segment), ('^', '|', '!'), "a UNA can name any");
        let (s, skip) = syntax("UNB+UNOA:2").unwrap();
        assert_eq!((s.element, s.component, s.segment, s.release, skip), ('+', ':', '\'', Some('?'), 0), "the defaults");
        assert!(syntax("ISA*00*short~").is_err());
        assert!(syntax("id,name\n").unwrap_err().contains("neither"));
    }

    #[test]
    fn a_release_character_keeps_a_delimiter_as_text() {
        let s = syntax("UNB").unwrap().0;
        assert_eq!(segment("BGM+220+PO?+123+9", &s), ("BGM".to_string(), vec![vec!["220".to_string()], vec!["PO+123".to_string()], vec!["9".to_string()]]));
        assert_eq!(segment("DTM+137:2026?:11:102", &s).1, vec![vec!["137".to_string(), "2026:11".to_string(), "102".to_string()]]);
        assert_eq!(segment("FTX+AAI+++a??b", &s).1[3], vec!["a?b".to_string()], "a released release character");
    }

    #[test]
    fn a_file_that_is_not_utf8_reads_as_latin1() {
        assert_eq!(decode(b"Zo\xEB"), "Zo\u{eb}");
        assert_eq!(decode("Zo\u{eb}".as_bytes()), "Zo\u{eb}");
    }

    #[test]
    fn segments_carry_the_envelope_they_sit_in() {
        let text = format!("{ISA}\nGS*PO*SENDER*RECEIVER*20261119*1000*7*X*004010~\nST*850*0001~\nBEG*00*SA*PO-123**20261119~\nSE*3*0001~\nGE*1*7~\nIEA*1*000000001~\n");
        let got = rows("a.x12", &text).unwrap();
        let tags: Vec<&str> = got.iter().map(|r| r["segment"].as_str().unwrap()).collect();
        assert_eq!(tags, ["ISA", "GS", "ST", "BEG", "SE", "GE", "IEA"]);
        assert_eq!(got[3]["message_control"], "0001");
        assert_eq!(got[3]["message_type"], "850");
        assert_eq!(got[3]["group_control"], "7");
        assert_eq!(got[3]["interchange_control"], "000000001");
        assert_eq!(got[3]["sender"], "SENDER");
        assert_eq!(got[4]["message_control"], "0001", "the trailer sits in its message");
        assert_eq!(got[5]["message_control"], JsonValue::Null, "past it, no message");
        assert_eq!(got[3]["elements"], "[[\"00\"],[\"SA\"],[\"PO-123\"],[\"\"],[\"20261119\"]]");
        assert!(got[0]["elements"].as_str().unwrap().ends_with("[\"P\"],[\">\"]]"), "ISA16 is the separator, whole");
        assert!(rows("a.x12", &text.replace("IEA*1*000000001~\n", "")).unwrap_err().contains("cut short"));
    }
}
