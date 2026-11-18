//! src.email against an IMAP server that runs inside the test.
//!
//! The server speaks just enough IMAP4rev1 for one read (greeting, CAPABILITY,
//! STARTTLS, LOGIN, SELECT, FETCH, LOGOUT) under a certificate from a CA minted
//! here. Duckle has to trust that CA through `DUCKLE_CA_CERT`, the way every
//! other TLS client it has does, which is what shows the connection runs on
//! Duckle's own TLS rather than on a copy bundled inside the IMAP library.
//!
//! `DUCKLE_CA_CERT` is set for this test binary only, which is its own
//! process, and the tests hold a lock so each sees the value it set.

use duckle_duckdb_engine::{DuckdbEngine, PipelineDoc};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::json;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};

static ENV: Mutex<()> = Mutex::new(());

const MESSAGE: &str = concat!(
    "From: Ada <ada@example.com>\r\n",
    "To: ops@example.com\r\n",
    "Subject: Quarterly numbers\r\n",
    "Date: Thu, 22 Oct 2026 09:00:00 +0000\r\n",
    "Message-ID: <q3@example.com>\r\n",
    "\r\n",
    "Revenue is up.\r\n",
);

fn engine() -> Option<DuckdbEngine> {
    let bin = std::env::var("DUCKLE_DUCKDB_BIN").ok()?;
    let p = std::path::PathBuf::from(bin);
    p.exists().then(|| DuckdbEngine::new(p))
}

/// A CA's certificate, and a server configured with a certificate it issued
/// for 127.0.0.1.
fn pki() -> (String, Arc<rustls::ServerConfig>) {
    let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca.distinguished_name.push(rcgen::DnType::CommonName, "Duckle test CA");
    let ca = rcgen::CertifiedIssuer::self_signed(ca, rcgen::KeyPair::generate().unwrap()).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()])
        .unwrap()
        .signed_by(&key, &ca)
        .unwrap();
    let config = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![leaf.der().clone()],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
    )
    .unwrap();
    (ca.pem(), Arc::new(config))
}

#[derive(Clone, Copy, PartialEq)]
enum Security {
    /// TLS from the first byte, as on port 993.
    Implicit,
    /// Plain text until the client asks for STARTTLS.
    StartTls,
    /// Plain text throughout, with no STARTTLS on offer.
    Plain,
}

/// Serve `connections` connections, one after another, and hand back every
/// command line received, each marked with whether it came under TLS.
fn serve(
    listener: TcpListener,
    config: Arc<rustls::ServerConfig>,
    security: Security,
    connections: usize,
) -> Arc<Mutex<Vec<String>>> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let seen = log.clone();
    std::thread::spawn(move || {
        for _ in 0..connections {
            let Ok((tcp, _)) = listener.accept() else { return };
            // A client that refuses the certificate ends the connection
            // mid-handshake. That is an outcome under test, not a server fault.
            let _ = serve_one(tcp, &config, security, &seen);
        }
    });
    log
}

fn serve_one(
    mut tcp: TcpStream,
    config: &Arc<rustls::ServerConfig>,
    security: Security,
    log: &Mutex<Vec<String>>,
) -> io::Result<()> {
    let tls = |tcp: TcpStream| -> io::Result<_> {
        let conn = rustls::ServerConnection::new(config.clone()).map_err(io::Error::other)?;
        Ok(rustls::StreamOwned::new(conn, tcp))
    };
    if security == Security::Implicit {
        let mut s = tls(tcp)?;
        say(&mut s, "* OK test server ready\r\n")?;
        return converse(&mut s, "tls", log);
    }
    say(&mut tcp, "* OK test server ready\r\n")?;
    loop {
        let line = hear(&mut tcp)?;
        log.lock().unwrap().push(format!("plain: {line}"));
        let (tag, command) = split(&line);
        match command.as_str() {
            "CAPABILITY" => {
                let caps = if security == Security::StartTls { "IMAP4rev1 STARTTLS" } else { "IMAP4rev1" };
                say(&mut tcp, &format!("* CAPABILITY {caps}\r\n{tag} OK CAPABILITY completed\r\n"))?;
            }
            "STARTTLS" if security == Security::StartTls => {
                say(&mut tcp, &format!("{tag} OK begin TLS now\r\n"))?;
                return converse(&mut tls(tcp)?, "tls", log);
            }
            _ => {
                if !answer(&mut tcp, &tag, &command)? {
                    return Ok(());
                }
            }
        }
    }
}

fn converse(s: &mut (impl Read + Write), via: &str, log: &Mutex<Vec<String>>) -> io::Result<()> {
    loop {
        let line = hear(s)?;
        log.lock().unwrap().push(format!("{via}: {line}"));
        let (tag, command) = split(&line);
        if command == "AUTHENTICATE" {
            authenticate(s, &tag, via, log)?;
            continue;
        }
        if !answer(s, &tag, &command)? {
            return Ok(());
        }
    }
}

/// XOAUTH2 as Gmail answers it: an empty challenge, the client's answer (kept,
/// decoded), then OK for the one token this server knows. Any other gets the
/// refusal as a JSON challenge, answered empty, then NO.
fn authenticate(s: &mut (impl Read + Write), tag: &str, via: &str, log: &Mutex<Vec<String>>) -> io::Result<()> {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    say(s, "+ \r\n")?;
    let answer = hear(s)?;
    let sasl = String::from_utf8_lossy(&b64.decode(answer.trim()).unwrap_or_default()).into_owned();
    log.lock().unwrap().push(format!("{via}: sasl {}", sasl.replace('\x01', "^A")));
    if sasl.contains("auth=Bearer good-token\x01") {
        return say(s, &format!("{tag} OK AUTHENTICATE completed\r\n"));
    }
    let refusal = b64.encode(r#"{"status":"400","schemes":"Bearer","scope":"https://mail.google.com/"}"#);
    say(s, &format!("+ {refusal}\r\n"))?;
    // The client must answer the refusal with an empty line before the NO.
    if !hear(s)?.is_empty() {
        return say(s, &format!("{tag} BAD the refusal takes an empty answer\r\n"));
    }
    say(s, &format!("{tag} NO [AUTHENTICATIONFAILED] Invalid credentials\r\n"))
}

/// An OAuth token endpoint on localhost that answers one request with `status`
/// and `body`, and keeps the form it was sent.
fn token_endpoint(status: u16, body: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://127.0.0.1:{}/token", listener.local_addr().unwrap().port());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let keep = seen.clone();
    std::thread::spawn(move || {
        let Ok((mut tcp, _)) = listener.accept() else { return };
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if tcp.read(&mut byte).unwrap_or(0) == 0 {
                return;
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
        let len: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let mut form = vec![0u8; len];
        let _ = tcp.read_exact(&mut form);
        keep.lock().unwrap().push(String::from_utf8_lossy(&form).into_owned());
        let reply = format!(
            "HTTP/1.1 {status} Answer\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = tcp.write_all(reply.as_bytes());
    });
    (url, seen)
}

fn oauth_email(port: u16, out: &str, auth: serde_json::Value) -> PipelineDoc {
    let mut props = json!({
        "host": "127.0.0.1", "port": port, "user": "ops@example.com", "mailbox": "INBOX", "authType": "oauth2", "clientId": "app",
    });
    for (k, v) in auth.as_object().cloned().unwrap_or_default() {
        props[k] = v;
    }
    serde_json::from_value(json!({
        "nodes": [
            { "id": "mail", "position": { "x": 0, "y": 0 },
              "data": { "label": "mail", "componentId": "src.email", "properties": props } },
            { "id": "out", "position": { "x": 0, "y": 0 },
              "data": { "label": "out", "componentId": "snk.csv", "properties": { "path": out, "hasHeader": true } } }
        ],
        "edges": [ { "id": "e", "source": "mail", "target": "out", "data": { "connectionType": "main" } } ]
    }))
    .unwrap()
}

/// The form a token request sent, as pairs.
fn form_of(sent: &Mutex<Vec<String>>) -> std::collections::HashMap<String, String> {
    let sent = sent.lock().unwrap();
    url::form_urlencoded::parse(sent.first().expect("a token request").as_bytes()).into_owned().collect()
}

/// A person's mailbox, signed in to with XOAUTH2: the refresh token traded for
/// an access token at the token endpoint, then the user and that token as the
/// server takes them, and no password sent at all.
#[test]
fn xoauth2_signs_in_with_the_token_a_refresh_token_gets() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let Some(engine) = engine() else { return };
    let (ca_pem, config) = pki();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let log = serve(listener, config, Security::StartTls, 1);
    let (token_url, sent) = token_endpoint(200, r#"{"access_token":"good-token","token_type":"Bearer","expires_in":3599}"#);
    let tmp = tempfile::tempdir().unwrap();
    let ca = tmp.path().join("ca.pem");
    std::fs::write(&ca, &ca_pem).unwrap();
    let out = tmp.path().join("mail.csv").to_string_lossy().replace('\\', "/");
    std::env::set_var("DUCKLE_CA_CERT", &ca);
    let r = engine.execute_pipeline(&oauth_email(port, &out, json!({
        "oauthProvider": "other", "tokenUrl": token_url, "clientSecret": "s3cret", "refreshToken": "r-1",
    })));
    std::env::remove_var("DUCKLE_CA_CERT");
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert!(std::fs::read_to_string(&out).unwrap().contains("Quarterly numbers"));
    let form = form_of(&sent);
    for (k, v) in [("grant_type", "refresh_token"), ("refresh_token", "r-1"), ("client_id", "app"), ("client_secret", "s3cret")] {
        assert_eq!(form.get(k).map(String::as_str), Some(v), "{k} in {form:?}");
    }
    let log = log.lock().unwrap();
    assert!(log.iter().any(|l| l == "tls: sasl user=ops@example.com^Aauth=Bearer good-token^A^A"), "{log:?}");
    assert!(!log.iter().any(|l| l.contains("LOGIN")), "{log:?}");
}

/// An app's access to Microsoft 365 mailboxes: no refresh token, so the client
/// credentials grant, for the scope Exchange Online's IMAP takes.
#[test]
fn xoauth2_signs_an_app_in_by_the_client_credentials_grant() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let Some(engine) = engine() else { return };
    let (ca_pem, config) = pki();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let _log = serve(listener, config, Security::StartTls, 1);
    let (token_url, sent) = token_endpoint(200, r#"{"access_token":"good-token","token_type":"Bearer","expires_in":3599}"#);
    let tmp = tempfile::tempdir().unwrap();
    let ca = tmp.path().join("ca.pem");
    std::fs::write(&ca, &ca_pem).unwrap();
    let out = tmp.path().join("mail.csv").to_string_lossy().replace('\\', "/");
    std::env::set_var("DUCKLE_CA_CERT", &ca);
    let r = engine.execute_pipeline(&oauth_email(port, &out, json!({
        "oauthProvider": "microsoft", "tenantId": "contoso", "tokenUrl": token_url, "clientSecret": "s3cret",
    })));
    std::env::remove_var("DUCKLE_CA_CERT");
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let form = form_of(&sent);
    assert_eq!(form.get("grant_type").map(String::as_str), Some("client_credentials"));
    assert_eq!(form.get("scope").map(String::as_str), Some("https://outlook.office365.com/.default"));
    assert!(!form.contains_key("refresh_token"), "{form:?}");
}

/// A refused sign-in says why, by the AADSTS line, without the trace ids after
/// it; nothing reaches the mail server.
#[test]
fn xoauth2_says_why_the_sign_in_was_refused() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let Some(engine) = engine() else { return };
    let (token_url, _sent) = token_endpoint(
        400,
        r#"{"error":"invalid_grant","error_description":"AADSTS70008: The provided authorization code or refresh token has expired.\r\nTrace ID: 0ab1\r\nTimestamp: 2026-11-18 09:00:00Z"}"#,
    );
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("mail.csv").to_string_lossy().replace('\\', "/");
    let r = engine.execute_pipeline(&oauth_email(1, &out, json!({
        "oauthProvider": "other", "tokenUrl": token_url, "refreshToken": "r-1",
    })));
    assert_eq!(r.status, "error");
    let e = r.error.unwrap_or_default();
    assert!(e.contains("AADSTS70008") && !e.contains("Trace ID"), "{e}");
}

/// A token the mail server refuses fails the run, with the server's reason.
#[test]
fn xoauth2_reports_the_mail_server_refusing_the_token() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let Some(engine) = engine() else { return };
    let (ca_pem, config) = pki();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let _log = serve(listener, config, Security::StartTls, 1);
    let (token_url, _sent) = token_endpoint(200, r#"{"access_token":"stale-token"}"#);
    let tmp = tempfile::tempdir().unwrap();
    let ca = tmp.path().join("ca.pem");
    std::fs::write(&ca, &ca_pem).unwrap();
    let out = tmp.path().join("mail.csv").to_string_lossy().replace('\\', "/");
    std::env::set_var("DUCKLE_CA_CERT", &ca);
    let r = engine.execute_pipeline(&oauth_email(port, &out, json!({
        "oauthProvider": "other", "tokenUrl": token_url, "refreshToken": "r-1",
    })));
    std::env::remove_var("DUCKLE_CA_CERT");
    assert_eq!(r.status, "error");
    let e = r.error.unwrap_or_default();
    assert!(e.contains("AUTHENTICATIONFAILED") || e.contains("Invalid credentials"), "{e}");
}

/// Answer one command. False once the client has logged out.
fn answer(s: &mut impl Write, tag: &str, command: &str) -> io::Result<bool> {
    let reply = match command {
        "CAPABILITY" => format!("* CAPABILITY IMAP4rev1\r\n{tag} OK CAPABILITY completed\r\n"),
        "LOGIN" => format!("{tag} OK LOGIN completed\r\n"),
        "SELECT" => format!(
            "* 1 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 7] UIDs valid\r\n{tag} OK [READ-WRITE] SELECT completed\r\n"
        ),
        "FETCH" => format!(
            "* 1 FETCH (UID 42 BODY[] {{{}}}\r\n{MESSAGE})\r\n{tag} OK FETCH completed\r\n",
            MESSAGE.len()
        ),
        "LOGOUT" => {
            say(s, &format!("* BYE logging out\r\n{tag} OK LOGOUT completed\r\n"))?;
            return Ok(false);
        }
        _ => format!("{tag} BAD unknown command\r\n"),
    };
    say(s, &reply)?;
    Ok(true)
}

fn say(s: &mut impl Write, text: &str) -> io::Result<()> {
    s.write_all(text.as_bytes())?;
    s.flush()
}

/// One CRLF-terminated line, read a byte at a time so nothing past it is
/// consumed: after STARTTLS the next byte belongs to the TLS handshake.
fn hear(s: &mut impl Read) -> io::Result<String> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    while !line.ends_with(b"\r\n") {
        if s.read(&mut byte)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        line.push(byte[0]);
    }
    line.truncate(line.len() - 2);
    Ok(String::from_utf8_lossy(&line).into_owned())
}

fn split(line: &str) -> (String, String) {
    let mut words = line.split(' ');
    let tag = words.next().unwrap_or_default().to_string();
    (tag, words.next().unwrap_or_default().to_ascii_uppercase())
}

fn email_to_csv(port: u16, out: &str) -> PipelineDoc {
    serde_json::from_value(json!({
        "nodes": [
            { "id": "mail", "position": { "x": 0, "y": 0 },
              "data": { "label": "mail", "componentId": "src.email", "properties": {
                  "host": "127.0.0.1", "port": port, "user": "ops",
                  "password": "not-a-real-password", "mailbox": "INBOX" } } },
            { "id": "out", "position": { "x": 0, "y": 0 },
              "data": { "label": "out", "componentId": "snk.csv",
                        "properties": { "path": out, "hasHeader": true } } }
        ],
        "edges": [ { "id": "e", "source": "mail", "target": "out", "data": { "connectionType": "main" } } ]
    }))
    .unwrap()
}

/// Run against `port` twice: refused without the CA, read with it.
fn refused_then_read(engine: &DuckdbEngine, port: u16, ca_pem: &str, log: &Mutex<Vec<String>>) {
    let tmp = tempfile::tempdir().unwrap();
    let ca = tmp.path().join("ca.pem");
    std::fs::write(&ca, ca_pem).unwrap();
    let out = tmp.path().join("mail.csv").to_string_lossy().replace('\\', "/");

    std::env::remove_var("DUCKLE_CA_CERT");
    let r = engine.execute_pipeline(&email_to_csv(port, &out));
    let error = r.error.unwrap_or_default();
    assert_eq!(r.status, "error", "a certificate from an unknown CA was accepted");
    assert!(error.contains("certificate"), "refused for the wrong reason: {error}");

    std::env::set_var("DUCKLE_CA_CERT", &ca);
    let r = engine.execute_pipeline(&email_to_csv(port, &out));
    std::env::remove_var("DUCKLE_CA_CERT");
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let csv = std::fs::read_to_string(Path::new(&out)).unwrap();
    for want in ["42", "ada@example.com", "ops@example.com", "Quarterly numbers", "Revenue is up."] {
        assert!(csv.contains(want), "{want} missing from\n{csv}");
    }
    let log = log.lock().unwrap();
    assert!(log.iter().any(|l| l.starts_with("tls: ") && l.contains("LOGIN")), "{log:?}");
    assert!(!log.iter().any(|l| l.starts_with("plain: ") && l.contains("LOGIN")), "{log:?}");
}

#[test]
fn a_starttls_server_is_trusted_through_duckle_ca_cert() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let Some(engine) = engine() else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN to run");
        return;
    };
    let (ca_pem, config) = pki();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let log = serve(listener, config, Security::StartTls, 2);
    refused_then_read(&engine, port, &ca_pem, &log);
}

/// Port 993 is the one port Duckle opens with TLS straight away, so this needs
/// that port. Where the OS reserves it (Linux, for an unprivileged user) the
/// test says so and stops.
#[test]
fn an_implicit_tls_server_on_993_is_trusted_through_duckle_ca_cert() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let Some(engine) = engine() else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN to run");
        return;
    };
    let listener = match TcpListener::bind("127.0.0.1:993") {
        Ok(l) => l,
        Err(e) => {
            eprintln!("skipping: cannot listen on 993 here ({e})");
            return;
        }
    };
    let (ca_pem, config) = pki();
    let log = serve(listener, config, Security::Implicit, 2);
    refused_then_read(&engine, 993, &ca_pem, &log);
}

#[test]
fn a_server_without_starttls_is_never_sent_the_password() {
    let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let Some(engine) = engine() else {
        eprintln!("skipping: set DUCKLE_DUCKDB_BIN to run");
        return;
    };
    let (_, config) = pki();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let log = serve(listener, config, Security::Plain, 1);
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("mail.csv").to_string_lossy().replace('\\', "/");

    let r = engine.execute_pipeline(&email_to_csv(port, &out));
    assert_eq!(r.status, "error", "read a mailbox over plain text");
    let log = log.lock().unwrap();
    assert!(!log.iter().any(|l| l.contains("LOGIN")), "the password went out in the clear: {log:?}");
}
