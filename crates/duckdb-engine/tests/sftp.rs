//! SFTP against a real OpenSSH server.
//!
//! The SSH client underneath the SFTP source and sink is russh, whose minor
//! releases change its API (0.63 changed the host-key callback), and the unit
//! tests never connect to a server. These run both nodes against OpenSSH,
//! which judges the client rather than agreeing with it, and check what
//! arrived on the server's disk rather than asking the code under test.
//!
//! Set DUCKLE_SFTP_HOST to run them, with DUCKLE_SFTP_PORT, DUCKLE_SFTP_USER,
//! DUCKLE_SFTP_PASSWORD, DUCKLE_SFTP_KEY (the path of an OpenSSH private key
//! the same user signs in with), DUCKLE_SFTP_FINGERPRINT (the server's host
//! key, as `ssh-keygen -lf` prints it) and DUCKLE_SFTP_DIR (a directory that
//! user can write). This process reads and writes the same directory to judge
//! the transfers, at DUCKLE_SFTP_LOCAL_DIR when the server mounts it at a
//! different path. The sftp-integration CI job sets all of it up.

use base64::Engine as _;
use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc, RunResult};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

struct Server {
    engine: DuckdbEngine,
    bin: String,
    host: String,
    port: u16,
    user: String,
    password: String,
    key: String,
    fingerprint: String,
    remote: String,
    local: PathBuf,
}

fn server() -> Option<Server> {
    let Some(host) = std::env::var("DUCKLE_SFTP_HOST")
        .ok()
        .filter(|h| !h.is_empty())
    else {
        eprintln!(
            "skipping: set DUCKLE_SFTP_HOST and the rest of DUCKLE_SFTP_* (see the file header)"
        );
        return None;
    };
    // A configured run checks everything or fails; it never skips a part, so
    // a CI job missing the DuckDB CLI fails here instead of passing empty.
    let var = |k: &str| std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"));
    let bin = var("DUCKLE_DUCKDB_BIN");
    assert!(
        Path::new(&bin).exists(),
        "DUCKLE_DUCKDB_BIN {bin} does not exist"
    );
    let remote = var("DUCKLE_SFTP_DIR");
    Some(Server {
        engine: DuckdbEngine::new(bin.clone().into()),
        bin,
        host,
        port: var("DUCKLE_SFTP_PORT")
            .parse()
            .expect("DUCKLE_SFTP_PORT is a port"),
        user: var("DUCKLE_SFTP_USER"),
        password: var("DUCKLE_SFTP_PASSWORD"),
        key: var("DUCKLE_SFTP_KEY"),
        fingerprint: var("DUCKLE_SFTP_FINGERPRINT"),
        local: PathBuf::from(
            std::env::var("DUCKLE_SFTP_LOCAL_DIR").unwrap_or_else(|_| remote.clone()),
        ),
        remote,
    })
}

impl Server {
    /// Both nodes' connection settings, signed in by password with the host key pinned.
    fn props(&self, extra: Value) -> Value {
        let mut p = json!({
            "protocol": "sftp", "host": self.host, "port": self.port, "user": self.user,
            "password": self.password, "hostFingerprint": self.fingerprint,
        });
        for (k, v) in extra.as_object().unwrap() {
            p[k] = v.clone();
        }
        p
    }

    /// A name in the shared directory that no other test, or earlier run, uses.
    fn name(&self, test: &str) -> String {
        format!("{}-{test}", std::process::id())
    }

    fn remote_path(&self, name: &str) -> String {
        format!("{}/{name}", self.remote.trim_end_matches('/'))
    }

    /// A fresh directory holding `files`, as the server would serve it.
    fn seed(&self, test: &str, files: &[(&str, &[u8])]) -> String {
        let name = self.name(test);
        let dir = self.local.join(&name);
        std::fs::create_dir_all(&dir).unwrap();
        for (file, bytes) in files {
            std::fs::write(dir.join(file), bytes).unwrap();
        }
        self.remote_path(&name)
    }

    /// Run `sql` and upload its rows through snk.ftp.
    fn upload(&self, sql: &str, to: Value) -> RunResult {
        self.engine.execute_pipeline(&doc(
            json!([
                node("s", "code.sql", json!({ "sql": sql })),
                node("w", "snk.ftp", to)
            ]),
            json!([{ "id": "e", "source": "s", "target": "w" }]),
        ))
    }

    /// Download through src.ftp, and return the rows it produced.
    fn download(&self, from: Value) -> (RunResult, Vec<Value>) {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp
            .path()
            .join("out.parquet")
            .to_string_lossy()
            .replace('\\', "/");
        let r = self.engine.execute_pipeline(&doc(
            json!([
                node("s", "src.ftp", from),
                node("k", "snk.parquet", json!({ "path": out }))
            ]),
            json!([{ "id": "e", "source": "s", "target": "k" }]),
        ));
        let rows = if r.status == "ok" {
            self.rows(&format!(
                "SELECT filename, size, content_b64 FROM '{out}' ORDER BY filename"
            ))
        } else {
            Vec::new()
        };
        (r, rows)
    }

    fn rows(&self, sql: &str) -> Vec<Value> {
        let out = std::process::Command::new(&self.bin)
            .args([":memory:", "-json", "-c", sql])
            .output()
            .expect("duckdb runs");
        serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_default()
    }
}

fn doc(nodes: Value, edges: Value) -> PipelineDoc {
    serde_json::from_value(json!({ "nodes": nodes, "edges": edges })).unwrap()
}

fn node(id: &str, component: &str, props: Value) -> Value {
    json!({ "id": id, "position": { "x": 0, "y": 0 },
            "data": { "label": id, "componentId": component, "properties": props } })
}

fn decode(row: &Value) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(row["content_b64"].as_str().expect("content_b64 is text"))
        .expect("content_b64 is base64")
}

/// Rows become a CSV file on the server, byte for byte.
#[test]
fn the_output_uploads_as_written() {
    let Some(s) = server() else { return };
    let name = format!("{}.csv", s.name("upload"));
    let r = s.upload(
        "SELECT * FROM (VALUES (1, 'Zoë'), (2, 'Ana')) t(id, name)",
        s.props(json!({ "remotePath": s.remote_path(&name) })),
    );
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let landed = std::fs::read_to_string(s.local.join(&name)).expect("the file is on the server");
    assert_eq!(landed.replace("\r\n", "\n"), "id,name\n1,Zoë\n2,Ana\n");
}

/// Each file a pattern matches reads as one row, signed in with a private key.
#[test]
fn a_file_on_the_server_reads_as_a_row() {
    let Some(s) = server() else { return };
    let dir = s.seed(
        "read",
        &[
            ("greeting.txt", b"hello from OpenSSH\n"),
            ("other.csv", b"x\n"),
        ],
    );
    let mut from =
        s.props(json!({ "directory": dir, "pattern": "*.txt", "privateKeyPath": s.key }));
    // Without a password to fall back on, only the key can sign in.
    from.as_object_mut().unwrap().remove("password");
    let (r, rows) = s.download(from);
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["filename"], "greeting.txt");
    assert_eq!(rows[0]["size"], 19);
    assert_eq!(decode(&rows[0]), b"hello from OpenSSH\n");
}

/// Files bigger than an SSH channel window, both ways. A transfer that large
/// only completes if the client handles window adjustments, which is where
/// russh 0.65 fixed its accounting.
#[test]
fn a_file_larger_than_the_ssh_window_arrives_whole() {
    let Some(s) = server() else { return };
    let name = format!("{}.csv", s.name("large"));
    let r = s.upload(
        "SELECT range AS id, md5(range::VARCHAR) AS h FROM range(150000)",
        s.props(json!({ "remotePath": s.remote_path(&name) })),
    );
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let landed = s.local.join(&name);
    let size = std::fs::metadata(&landed)
        .expect("the file is on the server")
        .len();
    assert!(
        size > 4 << 20,
        "{size} bytes is too small to cross a window"
    );
    let check = s.rows(&format!(
        "SELECT count(*) AS n, sum(id)::BIGINT AS total, count(DISTINCT h) AS distinct_h \
         FROM read_csv('{}')",
        landed.to_string_lossy().replace('\\', "/")
    ));
    assert_eq!(
        check,
        vec![json!({ "n": 150000, "total": 11249925000u64, "distinct_h": 150000 })]
    );

    // A seeded pseudo-random file, so a dropped or repeated chunk cannot pass.
    let mut x: u32 = 0x2545_f491;
    let bytes: Vec<u8> = (0..3 << 20)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            (x >> 24) as u8
        })
        .collect();
    let dir = s.seed("large", &[("blob.bin", &bytes)]);
    let (r, rows) = s.download(s.props(json!({ "directory": dir })));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(rows.len(), 1, "{} rows", rows.len());
    let got = decode(&rows[0]);
    assert!(
        got == bytes,
        "{} bytes arrived of {}",
        got.len(),
        bytes.len()
    );
}

/// A server whose key is not the pinned one is refused by both nodes, and
/// the refusal names the key OpenSSH actually presented.
#[test]
fn a_host_key_other_than_the_pinned_one_is_refused() {
    let Some(s) = server() else { return };
    let wrong = json!("SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    let dir = s.seed("pin", &[("a.txt", b"a")]);
    let (r, _) = s.download(s.props(json!({ "directory": dir, "hostFingerprint": wrong })));
    assert_ne!(r.status, "ok");
    let e = format!("{:?}", r.error);
    assert!(
        e.contains(&format!("presented host key {}", s.fingerprint)),
        "{e}"
    );

    let name = format!("{}.csv", s.name("pin"));
    let r = s.upload(
        "SELECT 1 AS id",
        s.props(json!({ "remotePath": s.remote_path(&name), "hostFingerprint": wrong })),
    );
    assert_ne!(r.status, "ok");
    assert!(
        format!("{:?}", r.error).contains("does not match the pinned fingerprint"),
        "{:?}",
        r.error
    );
    assert!(
        !s.local.join(&name).exists(),
        "a refused connection still wrote the file"
    );
}

/// A wrong password is refused as a sign-in failure, and the error does not
/// repeat it.
#[test]
fn a_wrong_password_is_refused() {
    let Some(s) = server() else { return };
    let dir = s.seed("password", &[("a.txt", b"a")]);
    let mut from = s.props(json!({ "directory": dir }));
    from["password"] = json!("not-the-password");
    let (r, _) = s.download(from);
    assert_ne!(r.status, "ok");
    let e = format!("{:?}", r.error);
    assert!(
        e.contains(&format!("authentication failed for user '{}'", s.user)),
        "{e}"
    );
    assert!(
        !e.contains("not-the-password"),
        "the password is in the error: {e}"
    );
}
