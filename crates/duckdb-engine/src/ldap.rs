//! src.ldap: entries from an LDAP directory, OpenLDAP and Active Directory
//! among them, through the ldap3 crate on Duckle's own TLS. A search pages
//! through its results with the paged results control, so a directory's size
//! limit does not cut it short.

use crate::*;
use ldap3::adapters::{Adapter, EntriesOnly, PagedResults};
use ldap3::{LdapConn, LdapConnSettings, Scope, SearchEntry};
use std::time::Duration;

/// How long a connect, a bind, or any one reply to a search may take.
const WAIT: Duration = Duration::from_secs(120);

/// An Active Directory objectGUID as Windows shows it: its first three fields
/// are stored little-endian, so their bytes read backwards.
pub(crate) fn guid_text(b: &[u8]) -> Option<String> {
    let b: &[u8; 16] = b.try_into().ok()?;
    Some(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    ))
}

/// An objectSid as Windows shows it, S-1-5-21-...: a revision, a count of
/// sub-authorities, a 48-bit big-endian authority, then each sub-authority as
/// a 32-bit little-endian number.
pub(crate) fn sid_text(b: &[u8]) -> Option<String> {
    let (&revision, &count) = (b.first()?, b.get(1)?);
    if b.len() != 8 + 4 * usize::from(count) {
        return None;
    }
    let authority = b[2..8].iter().fold(0u64, |a, &x| (a << 8) | u64::from(x));
    let mut out = format!("S-{revision}-{authority}");
    for sub in b[8..].chunks_exact(4) {
        out.push_str(&format!("-{}", u32::from_le_bytes([sub[0], sub[1], sub[2], sub[3]])));
    }
    Some(out)
}

/// objectGUID and objectSid as Windows shows them; None for anything else.
fn windows_text(name: &str, bytes: &[u8]) -> Option<String> {
    if name.eq_ignore_ascii_case("objectGUID") {
        guid_text(bytes)
    } else if name.eq_ignore_ascii_case("objectSid") {
        sid_text(bytes)
    } else {
        None
    }
}

/// The values of attribute `name`, in any case, as text: objectGUID and
/// objectSid as Windows shows them, values that are not text as base64. None
/// when the entry has no such attribute.
pub(crate) fn values_of(entry: &SearchEntry, name: &str) -> Option<Vec<String>> {
    if let Some((key, values)) = entry.attrs.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)) {
        return Some(values.iter().map(|v| windows_text(key, v.as_bytes()).unwrap_or_else(|| v.clone())).collect());
    }
    let (key, values) = entry.bin_attrs.iter().find(|(k, _)| k.eq_ignore_ascii_case(name))?;
    Some(
        values
            .iter()
            .map(|v| {
                windows_text(key, v).unwrap_or_else(|| {
                    use base64::Engine as _;
                    base64::engine::general_purpose::STANDARD.encode(v)
                })
            })
            .collect(),
    )
}

/// An attribute Active Directory sent in ranges, as `member;range=0-1499`,
/// because it holds more values than one search returns.
pub(crate) fn ranged(entry: &SearchEntry) -> Option<&str> {
    entry.attrs.keys().chain(entry.bin_attrs.keys()).map(String::as_str).find(|k| k.to_ascii_lowercase().contains(";range="))
}

/// One entry as a row: its DN, then each attribute listed as a JSON list of
/// text, or, when none is listed, every attribute in one JSON object.
pub(crate) fn row_of(entry: &SearchEntry, attributes: &[String]) -> JsonValue {
    let mut row = serde_json::Map::new();
    row.insert("dn".into(), JsonValue::String(entry.dn.clone()));
    if attributes.is_empty() {
        let mut names: Vec<&String> = entry.attrs.keys().chain(entry.bin_attrs.keys()).collect();
        names.sort();
        let all: serde_json::Map<String, JsonValue> =
            names.into_iter().map(|n| (n.clone(), serde_json::json!(values_of(entry, n)))).collect();
        row.insert("attributes".into(), JsonValue::String(JsonValue::Object(all).to_string()));
    } else {
        for a in attributes {
            let values = values_of(entry, a).map(|v| JsonValue::String(serde_json::json!(v).to_string()));
            row.insert(a.clone(), values.unwrap_or(JsonValue::Null));
        }
    }
    JsonValue::Object(row)
}

/// One entry as a row, or why it cannot be one: an attribute Active Directory
/// sent in ranges, because it holds more values than one search returns, would
/// be read short.
pub(crate) fn entry_row(entry: &SearchEntry, attributes: &[String]) -> Result<JsonValue, String> {
    if let Some(attribute) = ranged(entry) {
        return Err(format!(
            "{} sent {attribute}: Active Directory sends an attribute in ranges when it holds more \
             values than one search returns, and this source does not read ranges. Leave that \
             attribute out of Attributes, or that entry out of the filter",
            entry.dn
        ));
    }
    Ok(row_of(entry, attributes))
}

/// read_json's columns, and the projection that types them: the DN, then a
/// list of text for each attribute listed, or every attribute as JSON.
pub(crate) fn shape(attributes: &[String]) -> (String, String) {
    if attributes.is_empty() {
        return (
            "'dn': 'VARCHAR', 'attributes': 'VARCHAR'".into(),
            "\"dn\", CAST(\"attributes\" AS JSON) AS \"attributes\"".into(),
        );
    }
    // The planner lets an attribute name hold only letters, digits, '-', '.' and ';'.
    let mut columns = vec!["'dn': 'VARCHAR'".to_string()];
    let mut select = vec!["\"dn\"".to_string()];
    for a in attributes {
        columns.push(format!("'{a}': 'VARCHAR'"));
        select.push(format!("from_json(\"{a}\", '[\"VARCHAR\"]') AS \"{a}\""));
    }
    (columns.join(", "), select.join(", "))
}

impl DuckdbEngine {
    /// src.ldap: bind, then search, a page at a time, writing each entry as a
    /// row. Referrals are left out, so a search from a domain's root in Active
    /// Directory reads the domain's own entries.
    pub(crate) fn run_ldap_source(&self, db: &Path, spec: &plan::LdapSourceSpec) -> Result<String, EngineError> {
        let fail = |what: &str, e: &dyn std::fmt::Display| EngineError::Query(format!("ldap {}: {what}: {e}", spec.url));
        let settings = LdapConnSettings::new()
            .set_conn_timeout(WAIT)
            .set_starttls(spec.start_tls)
            .set_config(std::sync::Arc::new(crate::tls::build_client_config()));
        let mut ldap = LdapConn::with_settings(settings, &spec.url).map_err(|e| fail("connect", &e))?;
        if let (Some(dn), Some(password)) = (&spec.bind_dn, &spec.password) {
            ldap.with_timeout(WAIT)
                .simple_bind(dn, password)
                .and_then(|r| r.success())
                .map_err(|e| fail(&format!("bind as {dn}"), &e))?;
        }
        let scope = match spec.scope.as_str() {
            "base" => Scope::Base,
            "one" => Scope::OneLevel,
            _ => Scope::Subtree,
        };
        let attrs: Vec<String> = if spec.attributes.is_empty() { vec!["*".into()] } else { spec.attributes.clone() };
        let adapters: Vec<Box<dyn Adapter<_, _>>> =
            vec![Box::new(EntriesOnly::new()), Box::new(PagedResults::new(spec.page_size as i32))];
        let mut writer = JsonLinesWriter::open(&spec.node_id)?;
        let mut count = 0usize;
        {
            let mut search = ldap
                .with_timeout(WAIT)
                .streaming_search_with(adapters, &spec.base_dn, scope, &spec.filter, attrs)
                .map_err(|e| fail("search", &e))?;
            while let Some(entry) = search.next().map_err(|e| fail("search", &e))? {
                self.check_cancelled()?;
                let entry = SearchEntry::construct(entry);
                let row = entry_row(&entry, &spec.attributes).map_err(|why| fail("search", &why))?;
                writer.write_row(&row)?;
                count += 1;
            }
            search.result().success().map_err(|e| fail("search", &e))?;
        }
        let _ = ldap.unbind();
        let (columns, select) = shape(&spec.attributes);
        writer.finalize_typed(&self.bin, db, &spec.node_id, &columns, &select)?;
        Ok(format!("ldap: read {count} entr{} into {}", if count == 1 { "y" } else { "ies" }, spec.node_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn entry(attrs: &[(&str, &[&str])], bin: &[(&str, &[&[u8]])]) -> SearchEntry {
        SearchEntry {
            dn: "cn=ada,dc=example,dc=org".into(),
            attrs: attrs.iter().map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect())).collect(),
            bin_attrs: bin.iter().map(|(k, v)| (k.to_string(), v.iter().map(|b| b.to_vec()).collect())).collect::<HashMap<_, _>>(),
        }
    }

    #[test]
    fn an_object_guid_reads_as_windows_shows_it() {
        let bytes = [0x3e, 0x9c, 0x1d, 0x8b, 0x6a, 0x1f, 0x4b, 0x4d, 0x9a, 0x2e, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06];
        assert_eq!(guid_text(&bytes).as_deref(), Some("8b1d9c3e-1f6a-4d4b-9a2e-010203040506"));
        assert_eq!(guid_text(&bytes[..15]), None);
    }

    #[test]
    fn an_object_sid_reads_as_windows_shows_it() {
        // BUILTIN\Administrators.
        let admins = [1, 2, 0, 0, 0, 0, 0, 5, 0x20, 0, 0, 0, 0x20, 2, 0, 0];
        assert_eq!(sid_text(&admins).as_deref(), Some("S-1-5-32-544"));
        assert_eq!(sid_text(&admins[..12]), None, "fewer sub-authorities than it counts");
    }

    #[test]
    fn values_are_text_base64_or_as_windows_shows_them() {
        let guid: &[u8] = &[0x3e, 0x9c, 0x1d, 0x8b, 0x6a, 0x1f, 0x4b, 0x4d, 0x9a, 0x2e, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06];
        let e = entry(&[("mail", &["a@x", "b@x"])], &[("jpegPhoto", &[&[0xFF, 0xD8, 0xFF, 0xE0]]), ("objectGUID", &[guid])]);
        assert_eq!(values_of(&e, "MAIL"), Some(vec!["a@x".to_string(), "b@x".to_string()]), "names in any case");
        assert_eq!(values_of(&e, "jpegPhoto"), Some(vec!["/9j/4A==".to_string()]));
        assert_eq!(values_of(&e, "objectguid"), Some(vec!["8b1d9c3e-1f6a-4d4b-9a2e-010203040506".to_string()]));
        assert_eq!(values_of(&e, "telephoneNumber"), None);
        // A GUID whose bytes all happen to be text still reads as a GUID.
        let e = entry(&[("objectGUID", &["ABCDEFGHIJKLMNOP"])], &[]);
        assert_eq!(values_of(&e, "objectGUID"), Some(vec!["44434241-4645-4847-494a-4b4c4d4e4f50".to_string()]));
    }

    #[test]
    fn a_row_has_the_attributes_listed_or_all_of_them() {
        let e = entry(&[("cn", &["ada"]), ("mail", &["a@x"])], &[]);
        let listed = row_of(&e, &["mail".to_string(), "title".to_string()]);
        assert_eq!(listed, serde_json::json!({"dn": "cn=ada,dc=example,dc=org", "mail": "[\"a@x\"]", "title": null}));
        let all = row_of(&e, &[]);
        assert_eq!(all["attributes"], serde_json::json!("{\"cn\":[\"ada\"],\"mail\":[\"a@x\"]}"));
    }

    #[test]
    fn an_entry_sent_in_ranges_is_refused_rather_than_read_short() {
        let group = entry(&[("cn", &["admins"]), ("member;range=0-1499", &["cn=a"])], &[]);
        let err = entry_row(&group, &["member".to_string()]).unwrap_err();
        assert!(err.contains("member;range=0-1499") && err.contains("cn=ada,dc=example,dc=org"), "{err}");
        assert!(entry_row(&group, &[]).is_err(), "with every attribute read, too");
        assert!(entry_row(&entry(&[("cn", &["admins"])], &[]), &[]).is_ok());
    }

    #[test]
    fn a_ranged_attribute_is_found() {
        assert_eq!(ranged(&entry(&[("member;range=0-1499", &["cn=a"])], &[])), Some("member;range=0-1499"));
        assert_eq!(ranged(&entry(&[("member", &["cn=a"])], &[])), None);
    }
}
