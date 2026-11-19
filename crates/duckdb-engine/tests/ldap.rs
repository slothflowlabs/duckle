//! src.ldap against a real OpenLDAP directory, when DUCKLE_LDAP_URL names one,
//! such as ldap://localhost:1389 for the osixia/openldap:1.5.0 image the CI job
//! starts. Its defaults hold: the domain example.org, so the base
//! dc=example,dc=org, and the admin cn=admin,dc=example,dc=org with the
//! password admin. Each test seeds an organizational unit of its own through
//! ldap3. DUCKLE_LDAP_CA, when set, is the directory's CA certificate, for the
//! StartTLS test.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use ldap3::LdapConn;
use serde_json::{json, Value};
use std::collections::HashSet;

const ADMIN: &str = "cn=admin,dc=example,dc=org";
const PASSWORD: &str = "admin";

fn engine() -> Option<DuckdbEngine> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok()?;
    let p = std::path::PathBuf::from(bin);
    p.exists().then(|| DuckdbEngine::new(p))
}

fn live() -> Option<String> {
    std::env::var("DUCKLE_LDAP_URL").ok().filter(|s| !s.is_empty())
}

macro_rules! ready_or_skip {
    () => {
        match (engine(), live()) {
            (Some(e), Some(url)) => (e, url),
            _ => {
                eprintln!("skipping: set DUCKLE_DUCKDB_BIN, and DUCKLE_LDAP_URL to a directory such as ldap://localhost:1389");
                return;
            }
        }
    };
}

fn unique() -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("{}", nanos % 1_000_000_000_000)
}

fn attr(name: &str, values: &[&[u8]]) -> (Vec<u8>, HashSet<Vec<u8>>) {
    (name.as_bytes().to_vec(), values.iter().map(|v| v.to_vec()).collect())
}

/// An organizational unit holding user1 to user25, user1 with two mail
/// addresses, a photo and a description that is not ASCII, and a nested unit
/// holding one more person. Its DN.
fn seed(url: &str) -> String {
    let ou = format!("ou=duckle-{},dc=example,dc=org", unique());
    let mut ldap = LdapConn::new(url).expect("connect to the directory");
    ldap.simple_bind(ADMIN, PASSWORD).unwrap().success().expect("bind as the admin");
    let unit = |name: &str| vec![attr("objectClass", &[b"organizationalUnit"]), attr("ou", &[name.as_bytes()])];
    ldap.add(&ou, unit(ou.split([',', '=']).nth(1).unwrap())).unwrap().success().unwrap();
    for i in 1..=25 {
        let (cn, mail) = (format!("user{i}"), format!("user{i}@example.org"));
        let mut attrs = vec![
            attr("objectClass", &[b"inetOrgPerson"]),
            attr("cn", &[cn.as_bytes()]),
            attr("sn", &[format!("Surname{i}").as_bytes()]),
        ];
        if i == 1 {
            attrs.push(attr("mail", &[mail.as_bytes(), b"u1@example.org"]));
            attrs.push(attr("jpegPhoto", &[&[0xFF, 0xD8, 0xFF, 0xE0]]));
            attrs.push(attr("description", &["Zo\u{eb}".as_bytes()]));
        } else {
            attrs.push(attr("mail", &[mail.as_bytes()]));
        }
        ldap.add(&format!("cn={cn},{ou}"), attrs).unwrap().success().unwrap();
    }
    let nested = format!("ou=nested,{ou}");
    ldap.add(&nested, unit("nested")).unwrap().success().unwrap();
    let deep = vec![attr("objectClass", &[b"inetOrgPerson"]), attr("cn", &[b"deep"]), attr("sn", &[b"Deep"])];
    ldap.add(&format!("cn=deep,{nested}"), deep).unwrap().success().unwrap();
    let _ = ldap.unbind();
    ou
}

fn node(id: &str, component: &str, props: Value) -> Value {
    json!({"id": id, "type": "source", "position": {"x": 0, "y": 0}, "data": {"label": id, "componentId": component, "properties": props}})
}

fn read(engine: &DuckdbEngine, props: Value, out: &str) -> RunResult {
    let doc: PipelineDoc = serde_json::from_value(json!({
        "nodes": [node("d", "src.ldap", props), node("k", "snk.parquet", json!({ "path": out }))],
        "edges": [{"id": "e1", "source": "d", "target": "k", "sourceHandle": "main", "targetHandle": "main", "data": {"connectionType": "main"}}],
    }))
    .unwrap();
    engine.execute_pipeline(&doc)
}

/// A search of `base` as the admin, with what `extra` changes.
fn search(url: &str, base: &str, extra: Value) -> Value {
    let mut props = json!({ "url": url, "bindDn": ADMIN, "password": PASSWORD, "baseDn": base });
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        props[k] = v;
    }
    props
}

fn duckdb_json(sql: &str) -> Vec<Value> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").expect("DUCKLE_DUCKDB_BIN set");
    let out = std::process::Command::new(bin).arg(":memory:").arg("-json").arg("-c").arg(sql).output().expect("run duckdb");
    serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
}

fn out_path(dir: &std::path::Path) -> String {
    dir.join("out.parquet").to_string_lossy().replace('\\', "/")
}

fn columns(out: &str) -> Vec<String> {
    duckdb_json(&format!("DESCRIBE SELECT * FROM read_parquet('{out}')"))
        .iter()
        .map(|c| format!("{} {}", c["column_name"].as_str().unwrap(), c["column_type"].as_str().unwrap()))
        .collect()
}

#[test]
fn ldap_reads_each_entry_with_the_attributes_listed_a_page_at_a_time() {
    let (engine, url) = ready_or_skip!();
    let ou = seed(&url);
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let props = json!({ "filter": "(objectClass=inetOrgPerson)", "attributes": "cn, mail\njpegPhoto, description", "pageSize": 10 });
    let r = read(&engine, search(&url, &ou, props), &out);
    assert_eq!(r.status, "ok", "src.ldap failed: {:?}", r.error);
    assert_eq!(columns(&out), ["dn VARCHAR", "cn VARCHAR[]", "mail VARCHAR[]", "jpegPhoto VARCHAR[]", "description VARCHAR[]"]);
    let rows = duckdb_json(&format!(
        "SELECT dn, cn, list_sort(mail) AS mail, jpegPhoto, description FROM read_parquet('{out}') ORDER BY dn"
    ));
    assert_eq!(rows.len(), 26, "every person below the unit, past two page breaks");
    let user = |cn: &str| rows.iter().find(|r| r["cn"] == json!([cn])).cloned().unwrap_or_default();
    assert_eq!(
        user("user1"),
        json!({"dn": format!("cn=user1,{ou}"), "cn": ["user1"], "mail": ["u1@example.org", "user1@example.org"],
               "jpegPhoto": ["/9j/4A=="], "description": ["Zo\u{eb}"]})
    );
    assert_eq!(user("user2")["jpegPhoto"], Value::Null, "an attribute the entry does not have");
}

#[test]
fn ldap_reads_every_attribute_as_json_when_none_is_listed() {
    let (engine, url) = ready_or_skip!();
    let ou = seed(&url);
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, search(&url, &format!("cn=user2,{ou}"), json!({ "scope": "base" })), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(columns(&out), ["dn VARCHAR", "attributes JSON"]);
    let rows = duckdb_json(&format!(
        "SELECT dn, CAST(attributes->'$.sn' AS VARCHAR) AS sn, CAST(attributes->'$.mail' AS VARCHAR) AS mail FROM read_parquet('{out}')"
    ));
    assert_eq!(rows, [json!({"dn": format!("cn=user2,{ou}"), "sn": "[\"Surname2\"]", "mail": "[\"user2@example.org\"]"})]);
}

#[test]
fn ldap_scope_one_reads_only_the_level_below_and_base_only_the_entry() {
    let (engine, url) = ready_or_skip!();
    let ou = seed(&url);
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let dns = |out: &str| -> Vec<String> {
        duckdb_json(&format!("SELECT dn FROM read_parquet('{out}') ORDER BY dn"))
            .iter()
            .map(|r| r["dn"].as_str().unwrap().to_string())
            .collect()
    };
    let r = read(&engine, search(&url, &ou, json!({ "scope": "one", "attributes": "cn" })), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let one = dns(&out);
    assert_eq!(one.len(), 26, "the 25 people and the nested unit");
    assert!(one.contains(&format!("ou=nested,{ou}")) && !one.contains(&format!("cn=deep,ou=nested,{ou}")), "{one:?}");
    let r = read(&engine, search(&url, &ou, json!({ "scope": "base", "attributes": "ou" })), &out);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(dns(&out), [ou]);
}

#[test]
fn ldap_reads_nothing_as_its_columns() {
    let (engine, url) = ready_or_skip!();
    let ou = seed(&url);
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let r = read(&engine, search(&url, &ou, json!({ "filter": "(cn=nobody)", "attributes": "cn" })), &out);
    assert_eq!(r.status, "ok", "a search that matches nothing is not an error: {:?}", r.error);
    assert_eq!(columns(&out), ["dn VARCHAR", "cn VARCHAR[]"]);
}

#[test]
fn ldap_says_when_the_base_is_not_in_the_directory() {
    let (engine, url) = ready_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let r = read(&engine, search(&url, "ou=nowhere,dc=example,dc=org", json!({ "attributes": "cn" })), &out_path(tmp.path()));
    assert_eq!(r.status, "error", "a base that is not there is not an empty result");
    let err = format!("{:?}", r.error);
    assert!(err.contains("noSuchObject"), "{err}");
}

#[test]
fn ldap_says_when_the_directory_refuses_the_bind() {
    let (engine, url) = ready_or_skip!();
    let tmp = tempfile::tempdir().unwrap();
    let props = search(&url, "dc=example,dc=org", json!({ "password": "not the password" }));
    let r = read(&engine, props, &out_path(tmp.path()));
    assert_eq!(r.status, "error");
    let err = format!("{:?}", r.error);
    assert!(err.contains("bind as cn=admin,dc=example,dc=org") && err.contains("invalidCredentials"), "{err}");
    assert!(!err.contains("not the password"), "no password in the message: {err}");
}

#[test]
fn ldap_starts_tls_with_the_roots_duckle_trusts() {
    let (engine, url) = ready_or_skip!();
    let Some(ca) = std::env::var("DUCKLE_LDAP_CA").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping: set DUCKLE_LDAP_CA to the directory's CA certificate");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let out = out_path(tmp.path());
    let props = search(&url, "dc=example,dc=org", json!({ "startTls": true, "scope": "base", "attributes": "dc" }));
    let refused = read(&engine, props.clone(), &out);
    assert_eq!(refused.status, "error", "a certificate from a CA Duckle does not trust");
    assert!(format!("{:?}", refused.error).contains("certificate"), "refused for its certificate: {:?}", refused.error);
    std::env::set_var("DUCKLE_CA_CERT", &ca);
    let r = read(&engine, props, &out);
    std::env::remove_var("DUCKLE_CA_CERT");
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(duckdb_json(&format!("SELECT dn FROM read_parquet('{out}')")), [json!({"dn": "dc=example,dc=org"})]);
}
