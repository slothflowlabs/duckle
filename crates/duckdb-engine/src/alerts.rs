//! Telling someone when a run fails, without every pipeline having to ask.
//!
//! Duckle already ships `snk.email` and `snk.rest`, but those are nodes inside
//! a pipeline: somebody has to remember to wire one into all of them, and a
//! node cannot fire when the pipeline dies before reaching it - which is
//! exactly the run you needed to hear about. A pipeline could fail at three in
//! the morning and nothing at all would say so.
//!
//! Rules live in `alerts.json` at the workspace root, authored and meant to be
//! committed alongside the pipelines, matching assets to owners the same way
//! `owners.json` does.
//!
//! # What this gets right on purpose
//!
//! **Repeat suppression.** A pipeline on a five-minute schedule that starts
//! failing would otherwise send 288 identical messages a day, and a channel
//! that noisy gets muted, which is worse than no alerting. Each rule has a
//! cooldown per pipeline and event.
//!
//! **The all-clear.** When a pipeline that was failing succeeds again, that is
//! its own event and it ignores the cooldown. Suppressing it would leave
//! someone believing an outage is ongoing hours after it ended, and it is the
//! message that lets people stop looking.
//!
//! **Never breaking the run.** Delivery happens after the run is recorded, is
//! time-bounded, and reports failures to stderr rather than propagating. An
//! unreachable Slack must not turn a successful load into a failed one.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::RunResult;

/// How long to wait on a channel before giving up. Short on purpose: this runs
/// after a pipeline has already finished, and nobody should wait on Slack.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);

/// What happened, from the point of view of someone who wants to be told.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Event {
    /// The run failed.
    Failure,
    /// The run succeeded, and the one before it did not. The all-clear.
    Recovery,
    /// The run succeeded. Rarely wanted; off unless asked for.
    Success,
}

impl Event {
    fn as_str(self) -> &'static str {
        match self {
            Event::Failure => "failure",
            Event::Recovery => "recovery",
            Event::Success => "success",
        }
    }
}

/// Where an alert goes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "channel", rename_all = "lowercase")]
pub enum Channel {
    /// A JSON POST. The payload carries a `text` field as well as the
    /// structured fields, because Slack, Teams and Discord all render `text`
    /// directly, so one shape covers them and anything home-grown too.
    Webhook {
        /// Supports `${ENV:NAME}`, since a webhook url is a credential and
        /// belongs in the environment rather than in a committed file.
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    // `rename_all` on the enum above renames the VARIANTS, not the fields
    // inside them, so without this the same rule object would mix
    // `cooldownMinutes` with `smtp_host` and quietly reject the spelling
    // everyone would reach for first.
    #[serde(rename_all = "camelCase")]
    Email {
        smtp_host: String,
        #[serde(default = "default_smtp_port")]
        smtp_port: u16,
        #[serde(default)]
        username: String,
        /// Supports `${ENV:NAME}`.
        #[serde(default)]
        password: String,
        from: String,
        to: Vec<String>,
    },
}

fn default_smtp_port() -> u16 {
    587
}

fn default_events() -> Vec<Event> {
    // Failure and its all-clear. Alerting on every success is how a channel
    // becomes unreadable, so it is opt-in.
    vec![Event::Failure, Event::Recovery]
}

fn default_cooldown() -> u64 {
    15
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertRule {
    /// Glob over pipeline ids. `*` covers the whole workspace.
    #[serde(rename = "match")]
    pub pattern: String,
    #[serde(default = "default_events")]
    pub on: Vec<Event>,
    /// Minutes before the same pipeline may raise the same event again.
    /// Recovery ignores this: an all-clear that arrives late is useless.
    #[serde(default = "default_cooldown")]
    pub cooldown_minutes: u64,
    #[serde(flatten)]
    pub channel: Channel,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Alerts {
    #[serde(default)]
    pub rules: Vec<AlertRule>,
}

impl Alerts {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

pub fn alerts_path(workspace: &Path) -> PathBuf {
    workspace.join("alerts.json")
}

/// Rules as authored, or an empty set when the workspace has none.
///
/// A file that will not parse is an error rather than "no rules", because
/// silently sending nothing is the failure mode alerting exists to prevent.
pub fn load(workspace: &Path) -> Result<Alerts, String> {
    let p = alerts_path(workspace);
    if !p.exists() {
        return Ok(Alerts::default());
    }
    let text = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
    if text.trim().is_empty() {
        return Ok(Alerts::default());
    }
    serde_json::from_str(&text).map_err(|e| format!("parse alerts.json: {e}"))
}

/// What has been sent and how each pipeline last finished.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlertState {
    /// Pipeline -> the status of its last recorded run, so a return to health
    /// can be told apart from an ordinary success.
    #[serde(default)]
    last_status: BTreeMap<String, String>,
    /// "pipeline|event" -> when it was last sent, for the cooldown.
    #[serde(default)]
    last_sent: BTreeMap<String, DateTime<Utc>>,
}

fn state_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("alert-state.json")
}

fn load_state(workspace: &Path) -> AlertState {
    // Unreadable state means "remember nothing", which errs towards sending an
    // extra message rather than swallowing one.
    std::fs::read_to_string(state_path(workspace))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

fn save_state(workspace: &Path, state: &AlertState) {
    let p = state_path(workspace);
    if let Some(dir) = p.parent() {
        if std::fs::create_dir_all(dir).is_err() {
            return;
        }
    }
    let Ok(body) = serde_json::to_string_pretty(state) else { return };
    // Temp file then rename, the same way schedules.json and catalog.json are
    // written. A plain write truncates first, and load_state treats an
    // unparseable file as "remember nothing" - so a reader landing in that
    // window silently reset every cooldown and last_status in the workspace,
    // which shows up as a burst of duplicate alerts and a missing all-clear.
    // A temp name of this writer's own, and no unlink before the rename: see
    // write_atomically in schedules.rs. One shared `alert-state.json.tmp` let
    // two runs finishing together write the same file, and removing the
    // destination first opened exactly the window this function exists to
    // close.
    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = p.with_extension(format!("json.{}.{seq}.tmp", std::process::id()));
    if std::fs::write(&tmp, body).is_err() {
        return;
    }
    if std::fs::rename(&tmp, &p).is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
}

/// Read, change and write the alert state as one exclusive step.
///
/// `notify` used to load the state, work, and save the whole file back with no
/// lock at all. Two runs finishing at the same moment - which the concurrency
/// permits make ordinary, not rare - each loaded the state before the other
/// wrote, so the second save dropped the first's keys entirely. What is lost is
/// `last_status`, and losing it means the next run of that pipeline classifies
/// against a stale status: an all-clear that never goes out, or a failure alert
/// sent a second time.
///
/// Delivery stays outside this. It waits up to ten seconds per channel, and
/// holding a workspace-wide lock across that would serialise every alert in
/// the workspace behind one unreachable webhook.
fn update_state<F>(workspace: &Path, change: F)
where
    F: FnOnce(&mut AlertState),
{
    let _guard = match crate::runlock::lock_store(workspace, "alert-state") {
        Ok(g) => Some(g),
        // Not being able to take the lock must not cost the update: an alert
        // that was delivered still has to be recorded, or it delivers again on
        // the next run. Falling through is the same behaviour as before the
        // lock existed.
        Err(e) => {
            eprintln!("duckle: {e}");
            None
        }
    };
    let mut state = load_state(workspace);
    change(&mut state);
    save_state(workspace, &state);
}

/// A stable name for a channel, so two rules pointing at different places do
/// not share one cooldown slot. Never the url or the password.
fn channel_id(channel: &Channel) -> String {
    match channel {
        Channel::Webhook { url, .. } => format!("webhook:{}", host_of(url)),
        Channel::Email { smtp_host, to, .. } => format!("email:{smtp_host}:{}", to.join(",")),
    }
}

/// Host and port only. A webhook url's path is the secret part - a Slack hook
/// is `https://hooks.slack.com/services/T.../B.../<token>` - so anything that
/// might be logged or persisted keeps the host and drops the rest.
fn host_of(url: &str) -> String {
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    after_scheme.split('/').next().unwrap_or("").to_string()
}

/// Remove anything secret from a transport error before it is logged.
fn redact_secrets(error: &str, channel: &Channel) -> String {
    let mut out = error.to_string();
    match channel {
        Channel::Webhook { url, .. } => {
            let resolved = resolve_env(url);
            if !resolved.is_empty() {
                out = out.replace(&resolved, &format!("{}/<redacted>", host_of(&resolved)));
            }
        }
        Channel::Email { password, .. } => {
            let resolved = resolve_env(password);
            if !resolved.is_empty() {
                out = out.replace(&resolved, "<redacted>");
            }
        }
    }
    out
}

/// One alert, as delivered.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub event: String,
    pub pipeline: String,
    pub status: String,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// Rendered summary. Slack, Teams and Discord all show this field as-is.
    pub text: String,
}

fn build_message(event: Event, pipeline: &str, result: &RunResult) -> Message {
    let seconds = result.duration_ms as f64 / 1000.0;
    let text = match event {
        Event::Failure => {
            let why = result.error.as_deref().unwrap_or("no error message");
            // One line, most important part first: people read alerts in a
            // notification preview, not in the channel.
            format!("Duckle: {pipeline} FAILED after {seconds:.1}s - {why}")
        }
        Event::Recovery => {
            format!("Duckle: {pipeline} recovered, succeeded in {seconds:.1}s")
        }
        Event::Success => format!("Duckle: {pipeline} succeeded in {seconds:.1}s"),
    };
    Message {
        event: event.as_str().to_string(),
        pipeline: pipeline.to_string(),
        status: result.status.clone(),
        duration_ms: result.duration_ms,
        error: result.error.clone(),
        category: result.category.clone(),
        text,
    }
}

/// Which event a run represents, given how the pipeline last finished.
fn classify(result: &RunResult, previous: Option<&str>) -> Event {
    // Anything that is not a clean "ok" counts as a failure. Erring this way
    // means an unrecognised status raises an alert rather than staying silent.
    if result.status != "ok" {
        return Event::Failure;
    }
    match previous {
        Some(prev) if prev != "ok" => Event::Recovery,
        _ => Event::Success,
    }
}

/// Tell whoever asked to be told about this run.
///
/// Called after the run has been recorded, and never returns an error: alerting
/// is downstream of the work, and a broken channel must not change the outcome
/// of a pipeline that already finished. Returns how many alerts were sent, for
/// tests and for the caller's own logging.
pub fn notify(workspace: &Path, pipeline_id: &str, result: &RunResult) -> usize {
    let rules = match load(workspace) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("duckle: {e}");
            return 0;
        }
    };
    if rules.is_empty() {
        return 0;
    }

    // A snapshot, to decide against. The commit at the end re-reads under a
    // lock and merges, so a run finishing at the same moment cannot drop this
    // one's bookkeeping. Deciding from a snapshot is safe because every key
    // here is scoped to one pipeline, and one pipeline cannot be running twice
    // at once: the run lock is what guarantees that.
    let state = load_state(workspace);
    let event = classify(result, state.last_status.get(pipeline_id).map(String::as_str));
    let now = Utc::now();
    let mut sent = 0usize;
    let mut delivered: Vec<String> = Vec::new();

    for rule in &rules.rules {
        if !glob::Pattern::new(&rule.pattern).map(|p| p.matches(pipeline_id)).unwrap_or(false) {
            continue;
        }
        if !rule.on.contains(&event) {
            continue;
        }
        // Keyed by RULE as well as pipeline and event. Keyed by pipeline and
        // event alone, two rules matching one pipeline - the ordinary "chat
        // channel and on-call email" setup - shared one cooldown, so whichever
        // rule was listed first delivered and the other never did.
        let key = format!("{pipeline_id}|{}|{}|{}", event.as_str(), rule.pattern, channel_id(&rule.channel));
        // An all-clear always goes out. Suppressing it would leave people
        // believing an outage is still running long after it ended.
        if event != Event::Recovery {
            if let Some(last) = state.last_sent.get(&key) {
                let elapsed = now.signed_duration_since(*last);
                if elapsed < chrono::Duration::minutes(rule.cooldown_minutes as i64) {
                    continue;
                }
            }
        }
        let message = build_message(event, pipeline_id, result);
        match deliver(&rule.channel, &message) {
            Ok(()) => {
                delivered.push(key);
                sent += 1;
            }
            // Reported, never propagated: see the function docs. The error is
            // scrubbed first, because a webhook url IS the credential and a
            // transport error quotes the url it failed to reach - which would
            // put the secret this file works to keep out of the repository
            // straight into the server log.
            Err(e) => eprintln!(
                "duckle: alert for {pipeline_id} could not be sent: {}",
                redact_secrets(&e, &rule.channel)
            ),
        }
    }

    let status = result.status.clone();
    let pipeline = pipeline_id.to_string();
    update_state(workspace, move |s| {
        for key in delivered {
            s.last_sent.insert(key, now);
        }
        s.last_status.insert(pipeline, status);
    });
    sent
}

/// Expand `${ENV:NAME}`, so a webhook url or password is not committed.
fn resolve_env(value: &str) -> String {
    let Ok(re) = regex::Regex::new(r"\$\{ENV:([^}]+)\}") else {
        return value.to_string();
    };
    re.replace_all(value, |caps: &regex::Captures| {
        std::env::var(caps[1].trim()).unwrap_or_else(|_| caps[0].to_string())
    })
    .into_owned()
}

fn deliver(channel: &Channel, message: &Message) -> Result<(), String> {
    match channel {
        Channel::Webhook { url, headers } => {
            let url = resolve_env(url);
            if url.contains("${ENV:") {
                return Err(format!("{url} still has an unset ${{ENV:...}} placeholder"));
            }
            // The shared agent carries the merged OS + bundled root store, so
            // an internal webhook behind a corporate CA works here too.
            let agent = crate::tls::http_agent();
            let mut req = agent
                .post(&url)
                .timeout(DELIVERY_TIMEOUT)
                .set("Content-Type", "application/json");
            for (k, v) in headers {
                req = req.set(k, &resolve_env(v));
            }
            req.send_json(serde_json::to_value(message).map_err(|e| e.to_string())?)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        Channel::Email { smtp_host, smtp_port, username, password, from, to } => {
            send_email(smtp_host, *smtp_port, username, password, from, to, message)
        }
    }
}

fn send_email(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    from: &str,
    to: &[String],
    message: &Message,
) -> Result<(), String> {
    use lettre::message::header::ContentType;
    use lettre::transport::smtp::authentication::Credentials;
    use lettre::{Message as Mail, SmtpTransport, Transport};

    if to.is_empty() {
        return Err("email alert has no recipients".into());
    }
    let subject = format!("[Duckle] {} {}", message.pipeline, message.event);
    let mut builder = Mail::builder()
        .from(from.parse().map_err(|e| format!("from address: {e}"))?)
        .subject(subject)
        .header(ContentType::TEXT_PLAIN);
    for addr in to {
        builder = builder.to(addr.parse().map_err(|e| format!("to address {addr}: {e}"))?);
    }
    // The body repeats the one-line summary and then the detail, so it is
    // readable in a notification preview as well as in full.
    let body = match &message.error {
        Some(err) => format!("{}\n\nPipeline: {}\nError:\n{err}\n", message.text, message.pipeline),
        None => format!("{}\n\nPipeline: {}\n", message.text, message.pipeline),
    };
    let mail = builder.body(body).map_err(|e| e.to_string())?;

    let mut transport = SmtpTransport::starttls_relay(host)
        .map_err(|e| e.to_string())?
        .port(port)
        .timeout(Some(DELIVERY_TIMEOUT));
    if !username.is_empty() {
        transport = transport
            .credentials(Credentials::new(username.to_string(), resolve_env(password)));
    }
    transport.build().send(&mail).map(|_| ()).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(status: &str, error: Option<&str>) -> RunResult {
        RunResult {
            status: status.into(),
            duration_ms: 1234,
            nodes: Default::default(),
            preview: Vec::new(),
            error: error.map(str::to_string),
            category: None,
            unchanged: false,
            artifacts: Vec::new(),
            artifacts_truncated: false,
        }
    }

    fn write_rules(ws: &Path, json: serde_json::Value) {
        std::fs::write(alerts_path(ws), json.to_string()).unwrap();
    }

    #[test]
    fn a_return_to_health_is_its_own_event() {
        // Without this a recovery is indistinguishable from any other success,
        // and nobody is ever told the outage ended.
        assert_eq!(classify(&result("error", Some("boom")), None), Event::Failure);
        assert_eq!(classify(&result("ok", None), None), Event::Success);
        assert_eq!(classify(&result("ok", None), Some("ok")), Event::Success);
        assert_eq!(classify(&result("ok", None), Some("error")), Event::Recovery);
        // An unrecognised status counts as a failure: staying silent about a
        // status nobody anticipated is the worse of the two mistakes.
        assert_eq!(classify(&result("weird", None), Some("ok")), Event::Failure);
    }

    #[test]
    fn the_default_is_failures_and_their_all_clear_but_not_every_success() {
        let rule: AlertRule = serde_json::from_value(serde_json::json!({
            "match": "*", "channel": "webhook", "url": "https://example.test/hook"
        }))
        .expect("a rule needs only a pattern and a channel");
        assert_eq!(rule.on, vec![Event::Failure, Event::Recovery]);
        assert!(!rule.on.contains(&Event::Success), "alerting on every success is unreadable");
        assert_eq!(rule.cooldown_minutes, 15);
    }

    #[test]
    fn a_missing_file_is_no_rules_and_a_broken_one_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        assert!(load(ws).unwrap().is_empty());
        // Silently treating a broken file as "no rules" would turn a typo into
        // an outage nobody hears about, which is the whole failure mode here.
        std::fs::write(alerts_path(ws), b"{ not json").unwrap();
        assert!(load(ws).is_err());
    }

    #[test]
    fn nothing_is_sent_when_the_workspace_has_no_rules() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(notify(tmp.path(), "nightly", &result("error", Some("boom"))), 0);
    }

    #[test]
    fn an_unreachable_channel_is_reported_but_never_raised() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        // Port 1 refuses immediately, so this does not wait on a timeout.
        write_rules(
            ws,
            serde_json::json!({ "rules": [
                { "match": "*", "channel": "webhook", "url": "http://127.0.0.1:1/hook" }
            ]}),
        );
        // The point is that this returns at all: alerting is downstream of the
        // work, and a broken channel must not change a finished run's outcome.
        assert_eq!(notify(ws, "nightly", &result("error", Some("boom"))), 0);
        // The status is still remembered, so the eventual recovery is spotted
        // even though the failure alert never landed.
        let state = load_state(ws);
        assert_eq!(state.last_status.get("nightly").map(String::as_str), Some("error"));
    }

    #[test]
    fn a_pipeline_no_rule_matches_is_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_rules(
            ws,
            serde_json::json!({ "rules": [
                { "match": "nightly-*", "channel": "webhook", "url": "http://127.0.0.1:1/hook" }
            ]}),
        );
        assert_eq!(notify(ws, "hourly-sync", &result("error", Some("boom"))), 0);
        // It is still tracked, so a rule added later starts from the truth.
        assert_eq!(
            load_state(ws).last_status.get("hourly-sync").map(String::as_str),
            Some("error")
        );
    }

    #[test]
    fn the_message_leads_with_what_went_wrong() {
        let msg = build_message(Event::Failure, "nightly-load", &result("error", Some("ORA-01017")));
        // Read in a notification preview, the first few words have to carry it.
        assert!(msg.text.starts_with("Duckle: nightly-load FAILED"), "{}", msg.text);
        assert!(msg.text.contains("ORA-01017"), "the cause was dropped: {}", msg.text);
        assert_eq!(msg.event, "failure");

        let ok = build_message(Event::Recovery, "nightly-load", &result("ok", None));
        assert!(ok.text.contains("recovered"), "{}", ok.text);
        assert!(ok.error.is_none());
    }

    /// A webhook receiver on a real socket, so delivery is exercised end to end
    /// rather than mocked. Returns its url and a handle to the bodies it saw.
    fn capture_webhook() -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use std::io::{Read, Write};
        use std::sync::{Arc, Mutex};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                // Read until the body is complete, which for these small
                // payloads is the first read that contains the header break.
                while let Ok(n) = stream.read(&mut tmp) {
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    let text = String::from_utf8_lossy(&buf);
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let len: usize = head
                            .lines()
                            .find_map(|l| {
                                l.strip_prefix("Content-Length: ")
                                    .or_else(|| l.strip_prefix("content-length: "))
                            })
                            .and_then(|v| v.trim().parse().ok())
                            .unwrap_or(0);
                        if body.len() >= len {
                            sink.lock().unwrap().push(body.to_string());
                            break;
                        }
                    }
                }
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
            }
        });
        (url, seen)
    }

    #[test]
    fn a_failure_is_delivered_then_suppressed_until_the_cooldown_passes() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (url, seen) = capture_webhook();
        write_rules(
            ws,
            serde_json::json!({ "rules": [
                { "match": "nightly-*", "channel": "webhook", "url": url, "cooldownMinutes": 60 }
            ]}),
        );

        assert_eq!(notify(ws, "nightly-load", &result("error", Some("ORA-01017"))), 1);
        // The second failure inside the window is the one that would have made
        // 288 messages a day out of a five-minute schedule.
        assert_eq!(notify(ws, "nightly-load", &result("error", Some("ORA-01017"))), 0);
        assert_eq!(notify(ws, "nightly-load", &result("error", Some("ORA-01017"))), 0);

        let bodies = seen.lock().unwrap();
        assert_eq!(bodies.len(), 1, "the cooldown did not suppress the repeats");
        let sent: serde_json::Value = serde_json::from_str(&bodies[0]).expect("valid JSON body");
        assert_eq!(sent["event"], "failure");
        assert_eq!(sent["pipeline"], "nightly-load");
        assert_eq!(sent["error"], "ORA-01017");
        // Slack, Teams and Discord all render this field, so it has to be there
        // and it has to lead with the pipeline and the verdict.
        assert!(
            sent["text"].as_str().unwrap().starts_with("Duckle: nightly-load FAILED"),
            "text was {}",
            sent["text"]
        );
    }

    #[test]
    fn the_all_clear_arrives_even_though_the_failure_is_still_in_cooldown() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (url, seen) = capture_webhook();
        write_rules(
            ws,
            serde_json::json!({ "rules": [
                { "match": "*", "channel": "webhook", "url": url, "cooldownMinutes": 600 }
            ]}),
        );

        assert_eq!(notify(ws, "nightly-load", &result("error", Some("boom"))), 1);
        assert_eq!(notify(ws, "nightly-load", &result("error", Some("boom"))), 0, "suppressed");
        // Ten hours of cooldown remain, and the recovery must still land: the
        // alternative is people believing the outage is ongoing.
        assert_eq!(notify(ws, "nightly-load", &result("ok", None)), 1);
        // ...and an ordinary success after that says nothing, because the
        // default subscription is failures and their all-clear.
        assert_eq!(notify(ws, "nightly-load", &result("ok", None)), 0);

        let bodies = seen.lock().unwrap();
        assert_eq!(bodies.len(), 2);
        let second: serde_json::Value = serde_json::from_str(&bodies[1]).unwrap();
        assert_eq!(second["event"], "recovery");
        assert!(second["text"].as_str().unwrap().contains("recovered"));
    }

    #[test]
    fn two_rules_matching_one_pipeline_both_deliver() {
        // The ordinary setup: failures to a chat channel and to on-call email.
        // A cooldown keyed only by pipeline and event let whichever rule was
        // listed first claim the slot, and the second never delivered at all.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let (first, seen_first) = capture_webhook();
        let (second, seen_second) = capture_webhook();
        write_rules(
            ws,
            serde_json::json!({ "rules": [
                { "match": "*", "channel": "webhook", "url": first,  "cooldownMinutes": 60 },
                { "match": "*", "channel": "webhook", "url": second, "cooldownMinutes": 60 }
            ]}),
        );

        assert_eq!(notify(ws, "nightly-load", &result("error", Some("boom"))), 2, "both rules");
        assert_eq!(seen_first.lock().unwrap().len(), 1);
        assert_eq!(seen_second.lock().unwrap().len(), 1, "the second rule never fired");

        // Each still has its own cooldown afterwards.
        assert_eq!(notify(ws, "nightly-load", &result("error", Some("boom"))), 0);
    }

    #[test]
    fn a_failed_delivery_never_logs_the_webhook_url() {
        // The url IS the credential - a Slack hook is a bearer token in a path -
        // and a transport error quotes the address it could not reach.
        let channel = Channel::Webhook {
            url: "http://127.0.0.1:1/services/T000/B000/SUPERSECRETTOKEN".into(),
            headers: Default::default(),
        };
        let msg = build_message(Event::Failure, "p", &result("error", Some("x")));
        let err = deliver(&channel, &msg).expect_err("port 1 refuses");
        let scrubbed = redact_secrets(&err, &channel);
        assert!(!scrubbed.contains("SUPERSECRETTOKEN"), "the token survived: {scrubbed}");
        // The host is kept, because an operator still has to know where it failed.
        assert!(scrubbed.contains("127.0.0.1:1"), "too much was removed: {scrubbed}");
    }

    #[test]
    fn state_is_never_left_truncated_for_a_reader_to_find() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let mut state = AlertState::default();
        state.last_status.insert("nightly".into(), "error".into());
        save_state(ws, &state);

        // A plain write truncates first, and load_state reads an unparseable
        // file as "remember nothing" - which silently resets every cooldown.
        // The temp-and-rename means the file is only ever the old or the new one.
        let text = std::fs::read_to_string(state_path(ws)).unwrap();
        assert!(!text.is_empty());
        assert_eq!(load_state(ws).last_status.get("nightly").map(String::as_str), Some("error"));
        assert!(!state_path(ws).with_extension("json.tmp").exists(), "temp file left behind");
    }

    #[test]
    fn an_unset_env_placeholder_is_refused_rather_than_posted_to_a_literal_url() {
        let channel = Channel::Webhook {
            url: "${ENV:DUCKLE_TEST_UNSET_HOOK}".into(),
            headers: Default::default(),
        };
        let msg = build_message(Event::Failure, "p", &result("error", Some("x")));
        let err = deliver(&channel, &msg).expect_err("must not post to the placeholder itself");
        assert!(err.contains("ENV:"), "unhelpful error: {err}");
    }

    /// Two runs finishing together must not drop each other's bookkeeping.
    ///
    /// The state was loaded, changed and written back with no lock, and the
    /// write covers the whole file. Two pipelines finishing at the same moment
    /// - ordinary, given the scheduler runs several at once - each loaded
    /// before the other wrote, so the second save erased the first's
    /// `last_status`. The next run of that pipeline then classified against a
    /// stale status: an all-clear that never goes out, or a failure alert sent
    /// twice.
    #[test]
    fn state_written_by_runs_finishing_together_keeps_every_pipeline() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        std::fs::create_dir_all(ws.join(".duckle")).unwrap();

        let threads: Vec<_> = (0..8)
            .map(|i| {
                let ws = ws.clone();
                std::thread::spawn(move || {
                    update_state(&ws, move |s| {
                        s.last_status.insert(format!("pipeline-{i}"), "error".into());
                    });
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        let after = load_state(&ws);
        assert_eq!(
            after.last_status.len(),
            8,
            "a concurrent write dropped another pipeline's last status: {:?}",
            after.last_status.keys().collect::<Vec<_>>()
        );
    }

    /// The state file is never absent, so no reader ever sees "remember
    /// nothing" and resets every cooldown in the workspace.
    #[test]
    fn the_alert_state_is_never_observably_missing_during_a_write() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().to_path_buf();
        update_state(&ws, |s| {
            s.last_status.insert("p".into(), "ok".into());
        });
        let p = state_path(&ws);
        assert!(p.exists());

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let missing = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reader = {
            let (p, stop, missing) = (p.clone(), stop.clone(), missing.clone());
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if !p.exists() {
                        missing.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            })
        };
        for i in 0..200 {
            update_state(&ws, move |s| {
                s.last_status.insert("p".into(), format!("run-{i}"));
            });
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader.join().unwrap();
        assert_eq!(
            missing.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the alert state was observably absent during a write"
        );
    }
}
