//! #310: sign in through an identity provider, without inventing an auth model.
//!
//! Everything a login needs already existed - ordered roles, sessions with a
//! TTL, HttpOnly cookies, logout, audit, and a break-glass admin token that
//! lives only in the process. What was missing was the protocol. So this adds
//! the OIDC authorization-code flow and nothing else: it ends by calling the
//! same `Console::sign_in` a password login calls, and every request after that
//! is authorised by the same `authorize()` as before.
//!
//! ## The decisions are pure, the I/O is not
//!
//! Everything that decides whether someone gets in - claim to role, token
//! validation, state and nonce and PKCE - is a function from values to values,
//! tested without a network. The parts that talk to the provider are mechanical
//! by comparison. Auth bugs live in the decisions.
//!
//! ## What it refuses to do
//!
//! **No reverse-proxy identity headers.** #310 asks that they not be trusted by
//! default, and the safest reading of "by default" is not to implement them:
//! any deployment where the proxy can be bypassed turns `X-Forwarded-User` into
//! an admin login.
//!
//! **No self-registration into a role.** A subject the mappings do not match
//! gets `defaultRole`, and if the config names none, it is refused. An identity
//! provider saying who someone is does not say what they may do here.
//!
//! **No provider tokens are stored.** The access token is used to nothing and
//! the ID token is validated and dropped; only the subject, a display name and
//! the mapped role survive into the session.

use crate::console_auth::Role;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long an in-flight login may take between the redirect and the callback.
///
/// Short on purpose: this is the window in which a stolen `state` is worth
/// something, and no human needs ten minutes to type a password.
const PENDING_TTL: Duration = Duration::from_secs(300);

/// One claim-to-role rule.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoleMapping {
    /// The claim to read, e.g. `groups`.
    pub claim: String,
    /// The value that must be present. A claim holding a list matches when any
    /// element equals this; a claim holding a string matches on equality.
    pub contains: String,
    pub role: Role,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub issuer: String,
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    /// Where the provider sends the browser back. Must match what is registered
    /// with the provider exactly.
    pub redirect_uri: String,
    #[serde(default = "default_scopes")]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub role_mappings: Vec<RoleMapping>,
    /// The role for a subject no mapping matched. Absent means refuse.
    ///
    /// #310 asks for "least-privileged or deny, per config", and absent means
    /// deny because that is the answer that cannot surprise anyone: a
    /// deployment that wants everyone in says so.
    #[serde(default)]
    pub default_role: Option<Role>,
}

fn default_scopes() -> Vec<String> {
    vec!["openid".into(), "profile".into(), "email".into()]
}

pub fn config_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("oidc.json")
}

/// The configuration, or `None` when this deployment has not asked for OIDC.
///
/// A file that exists and cannot be parsed is an error rather than `None`: a
/// typo in an auth config must not silently leave the server on local accounts
/// while an operator believes SSO is enforced.
pub fn load(workspace: &Path) -> Result<Option<Config>, String> {
    let path = config_path(workspace);
    let Ok(text) = std::fs::read_to_string(&path) else { return Ok(None) };
    let cfg: Config = serde_json::from_str(&text)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    if cfg.issuer.trim().is_empty() || cfg.client_id.trim().is_empty() {
        return Err(format!("{}: issuer and clientId are required", path.display()));
    }
    if !cfg.issuer.starts_with("https://") {
        // An http issuer means the discovery document, the JWKS and the token
        // exchange are all interceptible, which makes every check below
        // theatre.
        return Err(format!("{}: issuer must be https", path.display()));
    }
    Ok(Some(cfg))
}

/// The endpoints a provider advertises.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Endpoints {
    #[serde(rename = "authorization_endpoint")]
    pub authorization: String,
    #[serde(rename = "token_endpoint")]
    pub token: String,
    #[serde(rename = "jwks_uri")]
    pub jwks: String,
    /// The issuer the document claims to be for.
    pub issuer: String,
}

/// The cookie that ties a callback to the browser that started the login.
///
/// Without it the `state` is a bearer token that anyone holding can redeem:
/// an attacker starts a login, gets a valid callback URL for THEIR identity,
/// and induces a victim's browser to visit it - the victim is then signed in
/// as the attacker and whatever they do next happens in the attacker's account.
/// Single-use and a TTL stop replay; they do not stop that.
pub const LOGIN_COOKIE: &str = "duckle_oidc_login";

/// One login in flight, from the redirect until the callback.
#[derive(Debug, Clone)]
pub struct Pending {
    pub state: String,
    pub nonce: String,
    pub verifier: String,
    /// Held by the browser that started this login, in an HttpOnly cookie, and
    /// required back at the callback.
    pub browser: String,
    pub created: Instant,
}

/// Logins in flight, keyed by state.
///
/// In memory and not on disk: a restart losing an in-flight login costs the
/// user one click, while persisting it would leave the material for replaying a
/// callback lying around after the window closed.
#[derive(Default)]
pub struct PendingLogins {
    by_state: HashMap<String, Pending>,
}

impl PendingLogins {
    pub fn insert(&mut self, pending: Pending) {
        self.sweep();
        self.by_state.insert(pending.state.clone(), pending);
    }

    /// Take the login this callback belongs to, if it is still valid.
    ///
    /// Removed whether or not it turns out to be usable, so a `state` is good
    /// for exactly one callback: a replayed callback finds nothing, which is
    /// what makes the state a CSRF defence rather than a decoration.
    pub fn take(&mut self, state: &str, browser: Option<&str>) -> Option<Pending> {
        self.sweep();
        let pending = self.by_state.remove(state).filter(|p| p.created.elapsed() < PENDING_TTL)?;
        // The browser that finishes a login must be the one that started it.
        // Compared in constant time, because this is a secret and a timing
        // oracle on it would hand back the state's twin.
        let presented = browser.unwrap_or_default();
        let ok = presented.len() == pending.browser.len()
            && presented
                .bytes()
                .zip(pending.browser.bytes())
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0;
        ok.then_some(pending)
    }

    fn sweep(&mut self) {
        self.by_state.retain(|_, p| p.created.elapsed() < PENDING_TTL);
    }

    pub fn len(&self) -> usize {
        self.by_state.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_state.is_empty()
    }
}

/// URL-safe base64 without padding, which is what OAuth and JWT both use.
fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// 32 bytes of randomness as a URL-safe string.
fn random_token() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("the OS random source");
    b64(&bytes)
}

/// The PKCE challenge for a verifier: base64url(sha256(verifier)), method S256.
///
/// S256 and not `plain`: a `plain` challenge is the verifier, so anyone who can
/// see the authorization request can complete the exchange, which is the attack
/// PKCE exists to stop.
pub fn pkce_challenge(verifier: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(verifier.as_bytes());
    b64(&h.finalize())
}

/// Start a login: a fresh state, nonce and PKCE verifier, and the URL to send
/// the browser to.
pub fn begin(cfg: &Config, endpoints: &Endpoints) -> (String, Pending) {
    let pending = Pending {
        state: random_token(),
        nonce: random_token(),
        verifier: random_token(),
        browser: random_token(),
        created: Instant::now(),
    };
    let query = [
        ("response_type", "code".to_string()),
        ("client_id", cfg.client_id.clone()),
        ("redirect_uri", cfg.redirect_uri.clone()),
        ("scope", cfg.scopes.join(" ")),
        ("state", pending.state.clone()),
        ("nonce", pending.nonce.clone()),
        ("code_challenge", pkce_challenge(&pending.verifier)),
        ("code_challenge_method", "S256".to_string()),
    ]
    .iter()
    .map(|(k, v)| format!("{k}={}", urlencode(v)))
    .collect::<Vec<_>>()
    .join("&");
    let sep = if endpoints.authorization.contains('?') { '&' } else { '?' };
    (format!("{}{sep}{query}", endpoints.authorization), pending)
}

/// Percent-encode everything that is not unreserved, so a redirect URI with a
/// port and a path survives and nothing can inject another parameter.
fn urlencode(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for b in v.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Who signed in, once everything checked out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The provider's stable identifier.
    pub subject: String,
    /// The display name the provider gave, for a person reading the log.
    pub label: String,
    pub role: Role,
}

impl Identity {
    /// What the session stores, and therefore what every later audit line
    /// names as the actor.
    ///
    /// Subject first, and it is the part that identifies. A display name is
    /// self-service at most providers, so a session labelled with one lets a
    /// user pick their own actor string - including the label the break-glass
    /// admin runs under - and every action they take afterwards is recorded
    /// against it. The name is kept, after the subject, because a log nobody
    /// can read is its own problem; but it is never what identifies.
    pub fn actor(&self) -> String {
        match self.label.trim().is_empty() || self.label == self.subject {
            true => self.subject.clone(),
            false => format!("{} ({})", self.subject, self.label),
        }
    }
}

/// Map an ID token's claims to a Duckle role.
///
/// First matching rule wins, and the order is the order in the config, so the
/// result is a property of the file rather than of iteration order. A subject
/// no rule matches gets `defaultRole`, and if there is none, `None` - which the
/// caller must turn into a refusal.
pub fn map_role(claims: &Value, mappings: &[RoleMapping], default: Option<Role>) -> Option<Role> {
    for rule in mappings {
        let Some(value) = claims.get(&rule.claim) else { continue };
        let matched = match value {
            // A list claim - `groups: ["a","b"]` - matches on any element.
            Value::Array(items) => {
                items.iter().any(|i| i.as_str() == Some(rule.contains.as_str()))
            }
            // A single-valued claim matches on equality, never on substring:
            // `contains: "admin"` must not match a group called
            // "not-admin-really".
            Value::String(s) => s == &rule.contains,
            _ => false,
        };
        if matched {
            return Some(rule.role);
        }
    }
    default
}

/// Compare two issuer identifiers.
///
/// Trailing slash ignored on BOTH sides. It was being trimmed off the
/// configured value and not off the token's claim, so a provider whose issuer
/// ends in a slash - `https://tenant.auth0.com/`, which is how Auth0 spells it -
/// could never match, and no value an operator could put in the config would
/// help, because the trim made both spellings identical. It fails closed, which
/// is the right direction, but it fails closed for everyone forever.
fn same_issuer(a: &str, b: &str) -> bool {
    a.trim_end_matches('/') == b.trim_end_matches('/')
}

/// Everything about an ID token that can be checked without the network.
///
/// Signature verification happens before this, in [`verify`]; these are the
/// claims. Split out so each rule can be tested on its own - an auth check that
/// only exists inside a function that needs a live provider is one nobody
/// exercises.
pub fn check_claims(
    claims: &Value,
    issuer: &str,
    client_id: &str,
    nonce: &str,
    now_unix: i64,
) -> Result<(), String> {
    let s = |k: &str| claims.get(k).and_then(Value::as_str).unwrap_or_default();
    if !same_issuer(s("iss"), issuer) {
        return Err(format!("token issuer {:?} is not {issuer:?}", s("iss")));
    }
    // `aud` is a string or an array of strings, and this client must be in it -
    // otherwise a token minted for a different application of the same provider
    // would be accepted here.
    let audience_ok = match claims.get("aud") {
        Some(Value::String(a)) => a == client_id,
        Some(Value::Array(items)) => {
            items.iter().any(|i| i.as_str() == Some(client_id))
        }
        _ => false,
    };
    if !audience_ok {
        return Err("token audience does not include this client".into());
    }
    // The nonce ties the token to the authorization request this server
    // started; without it a token obtained elsewhere could be replayed here.
    if s("nonce") != nonce {
        return Err("token nonce does not match this login".into());
    }
    let exp = claims.get("exp").and_then(Value::as_i64).unwrap_or(0);
    if exp <= now_unix {
        return Err("token has expired".into());
    }
    if s("sub").is_empty() {
        return Err("token has no subject".into());
    }
    Ok(())
}

/// A display name, preferring what a person would recognise.
pub fn label_of(claims: &Value) -> String {
    for key in ["name", "preferred_username", "email"] {
        if let Some(v) = claims.get(key).and_then(Value::as_str) {
            if !v.trim().is_empty() {
                return v.to_string();
            }
        }
    }
    claims.get("sub").and_then(Value::as_str).unwrap_or("unknown").to_string()
}

/// One JWKS key, in the fields RS256 verification needs.
#[derive(Debug, Clone, Deserialize)]
struct Jwk {
    kid: Option<String>,
    #[serde(default)]
    alg: Option<String>,
    #[serde(default)]
    kty: String,
    #[serde(default)]
    n: String,
    #[serde(default)]
    e: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

/// A short-timeout agent for talking to the provider.
///
/// Through the shared transport so a corporate proxy and the merged trust store
/// apply here as everywhere else; a login that works in the browser and not in
/// the server is otherwise very hard to explain.
fn agent(timeout_secs: u64) -> ureq::Agent {
    duckle_duckdb_engine::tls::http_agent_with(&duckle_duckdb_engine::tls::HttpTransport {
        read_timeout_secs: Some(timeout_secs),
        connect_timeout_secs: Some(timeout_secs),
        ..Default::default()
    })
}

/// Fetch the provider's discovery document.
///
/// The issuer INSIDE the document must equal the issuer configured, which is
/// what stops a discovery URL pointing somewhere that then names a different
/// authority.
pub fn discover(issuer: &str) -> Result<Endpoints, String> {
    let url = format!("{}/.well-known/openid-configuration", issuer.trim_end_matches('/'));
    let body = agent(10)
        .get(&url)
        .call()
        .map_err(|e| format!("OIDC discovery {url}: {e}"))?
        .into_string()
        .map_err(|e| format!("OIDC discovery {url}: {e}"))?;
    let endpoints: Endpoints =
        serde_json::from_str(&body).map_err(|e| format!("OIDC discovery {url}: {e}"))?;
    if endpoints.issuer.trim_end_matches('/') != issuer.trim_end_matches('/') {
        return Err(format!(
            "OIDC discovery at {url} claims issuer {:?}, which is not {issuer:?}",
            endpoints.issuer
        ));
    }
    Ok(endpoints)
}

/// Exchange the authorization code for an ID token.
///
/// Returns the raw ID token; nothing here trusts it yet. The access token is
/// deliberately not read: #310 asks that provider tokens not be stored unless
/// needed, and nothing here needs one.
pub fn exchange(cfg: &Config, endpoints: &Endpoints, code: &str, verifier: &str) -> Result<String, String> {
    let mut form: Vec<(&str, &str)> = vec![
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", &cfg.redirect_uri),
        ("client_id", &cfg.client_id),
        ("code_verifier", verifier),
    ];
    if !cfg.client_secret.trim().is_empty() {
        form.push(("client_secret", &cfg.client_secret));
    }
    let body = agent(15)
        .post(&endpoints.token)
        .send_form(&form)
        .map_err(|e| format!("OIDC token exchange: {e}"))?
        .into_string()
        .map_err(|e| format!("OIDC token exchange: {e}"))?;
    let parsed: Value =
        serde_json::from_str(&body).map_err(|e| format!("OIDC token response: {e}"))?;
    parsed
        .get("id_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        // The error deliberately does not include the body: a token response
        // carries an access token, and an error string ends up in logs.
        .ok_or_else(|| "OIDC token response carried no id_token".to_string())
}

/// Verify an ID token's signature against the provider's JWKS, then its claims.
///
/// Signature first. Every claim below is attacker-controlled until the
/// signature says otherwise, so checking them on an unverified token would be
/// checking the attacker's own assertions.
pub fn verify(
    cfg: &Config,
    endpoints: &Endpoints,
    id_token: &str,
    nonce: &str,
    now_unix: i64,
) -> Result<Identity, String> {
    let header = jsonwebtoken::decode_header(id_token)
        .map_err(|e| format!("OIDC id_token header: {e}"))?;
    if !matches!(header.alg, jsonwebtoken::Algorithm::RS256) {
        // Only RS256, and never `none`: accepting the token's own choice of
        // algorithm is the oldest JWT vulnerability there is.
        return Err(format!("OIDC id_token algorithm {:?} is not supported", header.alg));
    }
    let body = agent(10)
        .get(&endpoints.jwks)
        .call()
        .map_err(|e| format!("OIDC jwks: {e}"))?
        .into_string()
        .map_err(|e| format!("OIDC jwks: {e}"))?;
    let jwks: Jwks = serde_json::from_str(&body).map_err(|e| format!("OIDC jwks: {e}"))?;
    let key = jwks
        .keys
        .iter()
        .filter(|k| k.kty == "RSA")
        .filter(|k| k.alg.as_deref().is_none_or(|a| a == "RS256"))
        // Match the key id when the token names one. A token with no kid is
        // tried against every RSA key, which is what a single-key provider
        // needs and costs nothing extra when it fails.
        .find(|k| match (&header.kid, &k.kid) {
            (Some(want), Some(have)) => want == have,
            (None, _) => true,
            _ => false,
        })
        .ok_or_else(|| "OIDC jwks has no usable RSA key for this token".to_string())?;

    let decoding = jsonwebtoken::DecodingKey::from_rsa_components(&key.n, &key.e)
        .map_err(|e| format!("OIDC jwks key: {e}"))?;
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
    // An exact set membership, so both spellings are offered - `check_claims`
    // below is what actually decides, and it compares them properly.
    let trimmed = cfg.issuer.trim_end_matches('/').to_string();
    validation.set_issuer(&[trimmed.clone(), format!("{trimmed}/")]);
    validation.set_audience(&[cfg.client_id.as_str()]);
    let data = jsonwebtoken::decode::<Value>(id_token, &decoding, &validation)
        .map_err(|e| format!("OIDC id_token is not valid: {e}"))?;

    // And again, here, rather than trusting the library's configuration to
    // have covered everything: the nonce is not something it knows about, and
    // the rest is cheap to re-state and easy to test on its own.
    check_claims(&data.claims, cfg.issuer.trim_end_matches('/'), &cfg.client_id, nonce, now_unix)?;

    let role = map_role(&data.claims, &cfg.role_mappings, cfg.default_role).ok_or_else(|| {
        "signed in, but no role mapping matched and no defaultRole is configured".to_string()
    })?;
    Ok(Identity {
        subject: data.claims.get("sub").and_then(Value::as_str).unwrap_or_default().to_string(),
        label: label_of(&data.claims),
        role,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mappings() -> Vec<RoleMapping> {
        vec![
            RoleMapping { claim: "groups".into(), contains: "data-admins".into(), role: Role::Admin },
            RoleMapping {
                claim: "groups".into(),
                contains: "data-operators".into(),
                role: Role::Operator,
            },
        ]
    }

    #[test]
    fn a_group_maps_to_its_role_and_the_first_rule_wins() {
        let claims = serde_json::json!({ "groups": ["data-operators", "data-admins"] });
        // Both match; the file's order decides, so the result is a property of
        // the config rather than of iteration order.
        assert_eq!(map_role(&claims, &mappings(), None), Some(Role::Admin));
    }

    #[test]
    fn an_unmapped_subject_is_refused_unless_a_default_says_otherwise() {
        let claims = serde_json::json!({ "groups": ["interns"] });
        assert_eq!(map_role(&claims, &mappings(), None), None, "no default means no login");
        assert_eq!(map_role(&claims, &mappings(), Some(Role::Viewer)), Some(Role::Viewer));
    }

    #[test]
    fn a_group_name_matches_whole_and_not_as_a_substring() {
        // "not-admins-really" containing "admins" must not become an admin.
        let claims = serde_json::json!({ "groups": ["not-data-admins-really"] });
        assert_eq!(map_role(&claims, &mappings(), None), None);
        let single = serde_json::json!({ "groups": "data-admins-shadow" });
        assert_eq!(map_role(&single, &mappings(), None), None);
    }

    #[test]
    fn a_missing_or_wrongly_typed_claim_matches_nothing() {
        for claims in [
            serde_json::json!({}),
            serde_json::json!({ "groups": 42 }),
            serde_json::json!({ "groups": null }),
            serde_json::json!({ "groups": { "nested": "data-admins" } }),
        ] {
            assert_eq!(map_role(&claims, &mappings(), None), None, "{claims}");
        }
    }

    fn good() -> Value {
        serde_json::json!({
            "iss": "https://idp.example.com",
            "aud": "duckle-console",
            "nonce": "N",
            "sub": "user-123",
            "exp": 2_000_000_000i64
        })
    }

    #[test]
    fn a_valid_token_passes_every_claim_check() {
        assert!(check_claims(&good(), "https://idp.example.com", "duckle-console", "N", 1_000).is_ok());
    }

    #[test]
    fn a_token_from_another_issuer_is_refused() {
        let mut c = good();
        c["iss"] = "https://evil.example.com".into();
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).is_err());
    }

    #[test]
    fn a_token_minted_for_another_client_is_refused() {
        // Same provider, different application: without this check any app in
        // the tenant could mint a token that signs someone in here.
        let mut c = good();
        c["aud"] = "some-other-app".into();
        let e = check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).unwrap_err();
        assert!(e.contains("audience"), "{e}");
        // An array audience is accepted only when this client is in it.
        c["aud"] = serde_json::json!(["some-other-app", "duckle-console"]);
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).is_ok());
        c["aud"] = serde_json::json!(["a", "b"]);
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).is_err());
    }

    #[test]
    fn a_token_for_a_different_login_is_refused() {
        let mut c = good();
        c["nonce"] = "someone-elses".into();
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).is_err());
        // And a token with no nonce at all cannot pass by omission.
        let mut c = good();
        c.as_object_mut().unwrap().remove("nonce");
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).is_err());
    }

    #[test]
    fn an_expired_token_is_refused_and_the_boundary_is_not_inclusive() {
        let mut c = good();
        c["exp"] = serde_json::json!(1_000i64);
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).is_err());
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 999).is_ok());
        // A token with no exp is expired, not eternal.
        let mut c = good();
        c.as_object_mut().unwrap().remove("exp");
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1).is_err());
    }

    #[test]
    fn a_token_with_no_subject_is_refused() {
        // The subject is what audit records; without one there is nobody to
        // record, and an empty string would collide across users.
        let mut c = good();
        c["sub"] = "".into();
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).is_err());
    }

    #[test]
    fn a_state_is_good_for_exactly_one_callback() {
        let mut logins = PendingLogins::default();
        let p = Pending {
            state: "S".into(),
            nonce: "N".into(),
            verifier: "V".into(),
            browser: "B".into(),
            created: Instant::now(),
        };
        logins.insert(p);
        assert!(logins.take("S", Some("B")).is_some());
        assert!(logins.take("S", Some("B")).is_none(), "a replayed callback must find nothing");
    }

    #[test]
    fn an_unknown_or_stale_state_is_refused() {
        let mut logins = PendingLogins::default();
        assert!(logins.take("never-issued", Some("B")).is_none());
        logins.insert(Pending {
            state: "old".into(),
            nonce: "N".into(),
            verifier: "V".into(),
            browser: "B".into(),
            created: Instant::now() - PENDING_TTL - Duration::from_secs(1),
        });
        assert!(logins.take("old", Some("B")).is_none(), "the window had closed");
        assert!(logins.is_empty(), "and it is not left lying around");
    }

    #[test]
    fn a_callback_from_a_different_browser_is_refused() {
        // Login CSRF / session fixation: an attacker starts a login, takes the
        // callback URL for THEIR identity, and gets a victim to visit it. The
        // victim would be signed in as the attacker, and whatever they did next
        // would happen in the attacker's account. Single-use and a TTL do not
        // stop that; binding the login to the browser does.
        let mut logins = PendingLogins::default();
        logins.insert(Pending {
            state: "S".into(),
            nonce: "N".into(),
            verifier: "V".into(),
            browser: "attacker-cookie".into(),
            created: Instant::now(),
        });
        assert!(logins.take("S", Some("victim-cookie")).is_none(), "another browser got in");
        assert!(logins.take("S", None).is_none(), "no cookie at all got in");
    }

    #[test]
    fn a_login_cookie_is_spent_even_when_the_browser_is_wrong() {
        // The state must not survive a failed attempt for a second try.
        let mut logins = PendingLogins::default();
        logins.insert(Pending {
            state: "S".into(),
            nonce: "N".into(),
            verifier: "V".into(),
            browser: "B".into(),
            created: Instant::now(),
        });
        assert!(logins.take("S", Some("wrong")).is_none());
        assert!(logins.take("S", Some("B")).is_none(), "it was still there to retry");
    }

    #[test]
    fn an_issuer_with_a_trailing_slash_matches_one_without() {
        // Auth0 spells its issuer "https://tenant.auth0.com/". The config side
        // was trimmed and the token's claim was not, so it could never match -
        // and no config value helped, because the trim made both spellings the
        // same expected string.
        let mut c = good();
        c["iss"] = "https://idp.example.com/".into();
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).is_ok());
        assert!(check_claims(&c, "https://idp.example.com/", "duckle-console", "N", 1_000).is_ok());
        // And a genuinely different issuer is still refused.
        c["iss"] = "https://idp.example.com.evil.test/".into();
        assert!(check_claims(&c, "https://idp.example.com", "duckle-console", "N", 1_000).is_err());
    }

    #[test]
    fn the_audited_actor_is_the_subject_and_not_a_name_the_user_can_pick() {
        // A display name is self-service at most providers. A session labelled
        // with one lets a user choose their own actor string - including the
        // label the break-glass admin runs under - and every action they take
        // afterwards is recorded against it.
        let forged = Identity {
            subject: "user-123".into(),
            label: "token".into(),
            role: Role::Operator,
        };
        let actor = forged.actor();
        assert!(actor.starts_with("user-123"), "{actor}");
        assert_ne!(actor, "token", "the break-glass admin's label was forgeable");
        // The name is kept for a person reading the log, after the subject.
        assert!(actor.contains("token"), "{actor}");
        // No name, no parenthetical.
        let plain = Identity {
            subject: "user-123".into(),
            label: "user-123".into(),
            role: Role::Viewer,
        };
        assert_eq!(plain.actor(), "user-123");
    }

    #[test]
    fn pkce_uses_s256_and_not_the_verifier_itself() {
        // A `plain` challenge IS the verifier, so anyone who can see the
        // authorization request can complete the exchange.
        let v = "a-verifier-value";
        let c = pkce_challenge(v);
        assert_ne!(c, v);
        assert_eq!(c, pkce_challenge(v), "and it is deterministic");
        // base64url, no padding.
        assert!(!c.contains('='), "{c}");
        assert!(!c.contains('+') && !c.contains('/'), "{c}");
    }

    #[test]
    fn every_login_gets_fresh_secrets() {
        let cfg = Config {
            issuer: "https://idp.example.com".into(),
            client_id: "duckle-console".into(),
            client_secret: String::new(),
            redirect_uri: "https://console.example.com/auth/oidc/callback".into(),
            scopes: default_scopes(),
            role_mappings: mappings(),
            default_role: None,
        };
        let endpoints = Endpoints {
            authorization: "https://idp.example.com/authorize".into(),
            token: "https://idp.example.com/token".into(),
            jwks: "https://idp.example.com/jwks".into(),
            issuer: "https://idp.example.com".into(),
        };
        let (url_a, a) = begin(&cfg, &endpoints);
        let (_, b) = begin(&cfg, &endpoints);
        assert_ne!(a.state, b.state);
        assert_ne!(a.nonce, b.nonce);
        assert_ne!(a.verifier, b.verifier);
        assert!(url_a.contains("code_challenge_method=S256"), "{url_a}");
        assert!(url_a.contains("response_type=code"), "{url_a}");
        // The verifier must never be in the URL - only its hash.
        assert!(!url_a.contains(&a.verifier), "the verifier leaked into the redirect");
        // And the redirect URI survives encoding intact.
        assert!(url_a.contains("redirect_uri=https%3A%2F%2Fconsole.example.com"), "{url_a}");
    }

    #[test]
    fn a_label_prefers_something_a_person_recognises() {
        assert_eq!(label_of(&serde_json::json!({ "name": "Ada", "email": "a@b.c" })), "Ada");
        assert_eq!(label_of(&serde_json::json!({ "email": "a@b.c" })), "a@b.c");
        assert_eq!(label_of(&serde_json::json!({ "sub": "u-1" })), "u-1");
    }

    #[test]
    fn an_http_issuer_is_refused() {
        // Everything below the transport - discovery, JWKS, the code exchange -
        // is interceptible without TLS, which makes every other check theatre.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join(".duckle")).unwrap();
        std::fs::write(
            config_path(tmp.path()),
            r#"{"issuer":"http://idp.example.com","clientId":"c","redirectUri":"https://x/cb"}"#,
        )
        .unwrap();
        let e = load(tmp.path()).unwrap_err();
        assert!(e.contains("https"), "{e}");
    }

    #[test]
    fn a_broken_config_is_an_error_and_not_silently_no_oidc() {
        // Falling back to "no OIDC" would leave the server on local accounts
        // while an operator believed SSO was enforced.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join(".duckle")).unwrap();
        std::fs::write(config_path(tmp.path()), "{ not json").unwrap();
        assert!(load(tmp.path()).is_err());
        // And no file at all is simply not configured.
        let empty = tempfile::tempdir().unwrap();
        assert!(load(empty.path()).unwrap().is_none());
    }
}
