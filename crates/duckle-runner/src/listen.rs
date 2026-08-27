//! `duckle-runner listen` - keep a push source up, and spool what arrives.
//!
//! `src.webhook` and `src.websocket` collect inside a pipeline run: they bind
//! or connect, take N messages or time out, and stop. That is right for a
//! one-shot capture and wrong for anything continuous, because between runs
//! the port is closed and arriving requests are REFUSED. Under `follow` that
//! gap is every batch boundary.
//!
//! This is the other half. The listener stays up and appends each message to
//! an append-only NDJSON spool; a pipeline reads the spool with `src.spool`,
//! from wherever the last successful run stopped. Arrival is decoupled from
//! processing, so a slow batch, a failed batch or a restart costs nothing that
//! already arrived.
//!
//! Append-only and byte-offset is the whole trick: the reader never deletes,
//! the writer never rewrites, so there is no race between them and no
//! coordination to get wrong.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub struct ListenOptions {
    pub port: u16,
    pub spool: PathBuf,
    /// Only requests whose path starts with this are spooled. Anything else
    /// gets a 404, so a health check or a stray probe does not become a record.
    pub path_filter: Option<String>,
    /// Stop after this many messages. Bounds a test or a capture run.
    pub max_messages: Option<u64>,
    /// Bind address. Loopback by default: exposing a listener to a network is
    /// a deliberate act, not something to get by leaving a flag off.
    pub bind: String,
}

impl Default for ListenOptions {
    fn default() -> Self {
        Self {
            port: 0,
            spool: PathBuf::new(),
            path_filter: None,
            max_messages: None,
            bind: "127.0.0.1".to_string(),
        }
    }
}

/// One spool file, opened once and appended to under a lock.
///
/// Every record is written with a single `write_all` of a line that ends in
/// `\n`, and flushed before the response goes out. That ordering matters: a
/// 200 is a promise the record is durable, so it must not be sent while the
/// record is still in a buffer.
pub struct Spool {
    file: Mutex<std::fs::File>,
    path: PathBuf,
}

impl Spool {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("spool dir {}: {}", parent.display(), e))?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("spool {}: {}", path.display(), e))?;
        Ok(Self {
            file: Mutex::new(file),
            path: path.to_path_buf(),
        })
    }

    /// Append one record. The value is written as a single line, so a payload
    /// containing newlines cannot split into two records.
    pub fn append(&self, value: &serde_json::Value) -> Result<(), String> {
        let mut line = serde_json::to_string(value).map_err(|e| format!("serialize: {}", e))?;
        line.push('\n');
        let mut f = self.file.lock().unwrap_or_else(|e| e.into_inner());
        f.write_all(line.as_bytes())
            .map_err(|e| format!("append to {}: {}", self.path.display(), e))?;
        f.flush()
            .map_err(|e| format!("flush {}: {}", self.path.display(), e))
    }
}

/// Parse a request far enough to spool it: method, path, headers, body.
///
/// Deliberately minimal rather than a general HTTP server. It reads a
/// Content-Length body and nothing else - no chunked encoding, no keep-alive,
/// no pipelining - because the job is to catch a webhook POST and write it
/// down, and every feature beyond that is another way to be wrong.
pub fn read_request(stream: &mut TcpStream, max_body: usize) -> Result<Request, String> {
    let mut reader = BufReader::new(stream.try_clone().map_err(|e| e.to_string())?);
    let mut start = String::new();
    reader.read_line(&mut start).map_err(|e| e.to_string())?;
    let mut parts = start.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("/").to_string();

    let mut headers: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).map_err(|e| e.to_string())?;
        if n == 0 || line.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim().to_string();
            if k == "content-length" {
                content_length = v.parse().unwrap_or(0);
            }
            headers.insert(k, serde_json::Value::String(v));
        }
    }
    let want = content_length.min(max_body);
    let mut body = vec![0u8; want];
    if want > 0 {
        reader.read_exact(&mut body).map_err(|e| e.to_string())?;
    }
    Ok(Request {
        method,
        path,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: serde_json::Map<String, serde_json::Value>,
    pub body: String,
}

impl Request {
    /// The record written to the spool.
    ///
    /// A JSON body is embedded as JSON rather than as a string, so the
    /// pipeline can address its fields directly. A body that is not JSON is
    /// kept verbatim under `body` - dropping it because it did not parse would
    /// lose the thing the request was sent to deliver.
    pub fn to_record(&self, received_at: &str) -> serde_json::Value {
        let parsed: Option<serde_json::Value> = serde_json::from_str(&self.body).ok();
        let mut o = serde_json::Map::new();
        o.insert("received_at".into(), serde_json::Value::String(received_at.into()));
        o.insert("method".into(), serde_json::Value::String(self.method.clone()));
        o.insert("path".into(), serde_json::Value::String(self.path.clone()));
        o.insert("headers".into(), serde_json::Value::Object(self.headers.clone()));
        match parsed {
            Some(v) => o.insert("json".into(), v),
            None => o.insert("body".into(), serde_json::Value::String(self.body.clone())),
        };
        serde_json::Value::Object(o)
    }
}

/// Does this path count? An empty filter takes everything.
pub fn path_accepted(path: &str, filter: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(f) => path.starts_with(f),
    }
}

/// Bind and serve until stopped. Returns how many records were spooled.
pub fn run(opts: ListenOptions) -> Result<u64, String> {
    let spool = Spool::open(&opts.spool)?;
    let listener = TcpListener::bind((opts.bind.as_str(), opts.port))
        .map_err(|e| format!("bind {}:{}: {}", opts.bind, opts.port, e))?;
    let bound = listener
        .local_addr()
        .map_err(|e| e.to_string())?;
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;

    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = stop.clone();
        if ctrlc::set_handler(move || stop.store(true, Ordering::Relaxed)).is_err() {
            eprintln!("listen: no Ctrl-C handler; Ctrl-C will stop the process immediately");
        }
    }

    eprintln!("duckle-runner listen: http://{} -> {}", bound, opts.spool.display());
    if opts.bind == "127.0.0.1" {
        eprintln!("  Loopback only. Put a tunnel or a reverse proxy in front to take public traffic.");
    }
    eprintln!("  Read it with src.spool pointed at that file. Ctrl-C to stop.");

    let mut spooled = 0u64;
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _peer)) => {
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(10)));
                let req = match read_request(&mut stream, 8 * 1024 * 1024) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("listen: unreadable request: {e}");
                        let _ = respond(&mut stream, 400, "bad request");
                        continue;
                    }
                };
                if !path_accepted(&req.path, opts.path_filter.as_deref()) {
                    let _ = respond(&mut stream, 404, "not spooled");
                    continue;
                }
                let now = chrono::Utc::now().to_rfc3339();
                // Write BEFORE answering. A 200 says the record is durable, so
                // sending it first would tell a sender its data is safe when it
                // is not - and webhook senders do not retry a 200.
                match spool.append(&req.to_record(&now)) {
                    Ok(()) => {
                        spooled += 1;
                        let _ = respond(&mut stream, 200, "ok");
                    }
                    Err(e) => {
                        eprintln!("listen: could not spool: {e}");
                        // 500 so the sender retries, rather than dropping it.
                        let _ = respond(&mut stream, 500, "not spooled");
                    }
                }
                if let Some(max) = opts.max_messages {
                    if spooled >= max {
                        break;
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(e) => return Err(format!("accept: {e}")),
        }
    }
    eprintln!("listen: {} record(s) spooled to {}", spooled, opts.spool.display());
    Ok(spooled)
}

fn respond(stream: &mut TcpStream, code: u16, body: &str) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Internal Server Error",
    };
    let resp = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        code,
        reason,
        body.len(),
        body
    );
    stream.write_all(resp.as_bytes())?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_filter_only_takes_what_it_names() {
        assert!(path_accepted("/hooks/stripe", Some("/hooks")));
        assert!(path_accepted("/hooks", Some("/hooks")));
        assert!(!path_accepted("/health", Some("/hooks")));
        // No filter takes everything, including a health probe - which is why
        // the filter exists.
        assert!(path_accepted("/anything", None));
    }

    #[test]
    fn a_json_body_is_embedded_as_json_not_as_a_string() {
        let r = Request {
            method: "POST".into(),
            path: "/hooks".into(),
            headers: serde_json::Map::new(),
            body: r#"{"id":7,"kind":"charge"}"#.into(),
        };
        let rec = r.to_record("2026-08-27T00:00:00Z");
        assert_eq!(rec["json"]["id"], 7, "fields must be addressable by the pipeline");
        assert!(rec.get("body").is_none(), "a parsed body should not also be kept as text");
    }

    #[test]
    fn a_body_that_is_not_json_is_kept_verbatim() {
        let r = Request {
            method: "POST".into(),
            path: "/hooks".into(),
            headers: serde_json::Map::new(),
            body: "id=7&kind=charge".into(),
        };
        let rec = r.to_record("2026-08-27T00:00:00Z");
        assert_eq!(
            rec["body"], "id=7&kind=charge",
            "dropping it because it did not parse would lose the delivery"
        );
        assert!(rec.get("json").is_none());
    }

    #[test]
    fn a_record_is_one_line_even_when_the_payload_has_newlines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.ndjson");
        let s = Spool::open(&path).unwrap();
        s.append(&serde_json::json!({ "body": "line one\nline two\nline three" }))
            .unwrap();
        s.append(&serde_json::json!({ "body": "second record" })).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            text.lines().count(),
            2,
            "a newline inside a payload must not split one record into several: {text:?}"
        );
    }

    #[test]
    fn the_spool_is_appended_to_not_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.ndjson");
        Spool::open(&path)
            .unwrap()
            .append(&serde_json::json!({ "n": 1 }))
            .unwrap();
        // A second listener run must continue the file, not truncate it - the
        // reader's saved byte offset would otherwise point past the end.
        Spool::open(&path)
            .unwrap()
            .append(&serde_json::json!({ "n": 2 }))
            .unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 2, "reopening truncated the spool: {text:?}");
    }
}
