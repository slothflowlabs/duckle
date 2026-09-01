//! Who may use the management console, and what they are allowed to do.
//!
//! The console can run any pipeline in the workspace, and a pipeline can run
//! shell and SQL, so reaching it is equivalent to running code on the host. It
//! used to have no authentication at all: binding it to `0.0.0.0` printed a
//! warning and then served anyone who connected.
//!
//! Loopback is left alone. A console on `127.0.0.1` is reachable only by
//! someone who is already on the machine, and requiring a token there would
//! break the ordinary local workflow to protect against an attacker who has
//! already won. Binding anywhere else now refuses to start without a
//! credential, so the exposure that mattered cannot happen silently.
//!
//! Roles are viewer, operator and admin. The split follows what an action can
//! destroy rather than which screen it lives on: reading is viewer, causing a
//! pipeline to run or changing when it runs is operator, and touching
//! credentials or the workspace itself is admin.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::auth_store::AuthStore;

/// What a caller is allowed to do. Ordered: each role includes the ones before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Read the dashboard, runs and logs. Changes nothing.
    Viewer,
    /// Everything a viewer can do, plus run a pipeline and set its schedule.
    Operator,
    /// Everything an operator can do, plus credentials, connections and the
    /// workspace itself.
    Admin,
}

impl Role {
    fn rank(self) -> u8 {
        match self {
            Role::Viewer => 0,
            Role::Operator => 1,
            Role::Admin => 2,
        }
    }

    /// Whether this role satisfies a requirement of `needed`.
    pub fn allows(self, needed: Role) -> bool {
        self.rank() >= needed.rank()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Operator => "operator",
            Role::Admin => "admin",
        }
    }
}


/// What a console is, which decides what it will answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Loopback with nothing configured. Everything is allowed, because reaching the
    /// socket means already being on the machine. This is the ordinary local case and it
    /// has never required setting anything up.
    Local,
    /// Reachable off this machine with no credentials yet. Only the setup route answers,
    /// and only until the window closes. Whoever claims it becomes the first admin.
    Unclaimed,
    /// Has credentials. Ordinary authorisation.
    Claimed,
}

/// How long an unclaimed console stays claimable.
///
/// Short on purpose. During this window anyone who can reach the port can take it, which
/// is the price of setup being a wizard rather than a command. A restart re-opens it, and
/// starting with a token or existing accounts never opens it at all.
pub const CLAIM_WINDOW_SECS: i64 = 15 * 60;

/// The caller behind one request, as far as the console can tell.
#[derive(Debug, Clone)]
pub struct Identity {
    /// Shown in the audit log. Never a token or any part of one.
    pub label: String,
    pub role: Role,
}

/// One console account, as stored. The token itself is never written down.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Account {
    label: String,
    role: Role,
    /// PHC-format Argon2id hash. A token may be chosen by a person, so it is
    /// treated as a password rather than as high-entropy key material.
    token_hash: String,
}

/// The console's authentication policy for one running server.
pub struct Console {
    /// Reloaded when the console is claimed, because that creates an account in a process
    /// that is already serving: a snapshot taken at startup would not contain the
    /// administrator who was just made, and they would be unable to sign in until a
    /// restart.
    accounts: Mutex<Vec<Account>>,
    /// True when nothing is configured and the bind is loopback, which is the
    /// unchanged local case: the caller is trusted because they are already on
    /// the machine.
    open: bool,
    /// The account made from `--token`, which exists only for this process and is in no
    /// store. Held separately so re-reading the stored accounts cannot drop it: doing so
    /// would sign out whoever started the server, the moment they added a colleague.
    ephemeral: Option<Account>,
    /// Which of the three states this console is in. Behind a lock for the same reason as
    /// the accounts: claiming moves it, and the running process has to notice.
    mode: Mutex<Mode>,
    /// When an unclaimed console started, so the window can close.
    opened_at: i64,
    /// Where the credential database lives.
    workspace: PathBuf,
    /// Accounts, sessions and API keys. Behind a mutex because a SQLite connection is Send
    /// but not Sync, and one console is shared by every request thread.
    store: Mutex<AuthStore>,
    /// sha256(token) -> identity for tokens already verified once, so an API
    /// client polling every few seconds does not pay for an Argon2 hash on
    /// every request. Only ever populated with tokens that verified.
    verified: Mutex<HashMap<[u8; 32], Identity>>,
}

/// Deliberately hand-written: a derived one would print the stored token
/// hashes, and the first place a struct like this gets printed is an error
/// message or a log line.
impl std::fmt::Debug for Console {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Console")
            .field("accounts", &self.accounts.lock().map(|a| a.len()).unwrap_or(0))
            .field("open", &self.open)
            .finish()
    }
}

pub fn accounts_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("console-users.json")
}

/// How long a session lasts before it has to be earned again. A working day, so nobody is
/// interrupted mid-shift and nobody holds a credential for a fortnight.
const SESSION_TTL_SECS: i64 = 12 * 60 * 60;

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Carry a previous version's `console-users.json` into the database, once.
///
/// Only when the database has no accounts, so this cannot resurrect an account that was
/// deliberately removed later. The file is then renamed rather than deleted: an upgrade
/// that quietly destroys the only copy of a credential store is not one anyone should have
/// to trust, and the rename also stops it being read again.
///
/// A file that will not parse is still an error, exactly as before. Treating it as "no
/// accounts" would silently reopen a console that someone had locked.
fn migrate_accounts_file(workspace: &Path, store: &AuthStore) -> Result<(), String> {
    let path = accounts_path(workspace);
    if !path.exists() || !store.accounts()?.is_empty() {
        return Ok(());
    }
    for a in read_accounts_file(workspace)? {
        store.add_account(&a.label, a.role, &a.token_hash)?;
    }
    let _ = std::fs::rename(&path, path.with_extension("json.migrated"));
    Ok(())
}


impl Console {
    /// Decide the policy for a server about to bind `host`.
    ///
    /// Returns an error, rather than a warning, when the console would be
    /// reachable off this machine with no way to tell callers apart. Refusing
    /// to start is the point: a warning is what this had before, and a warning
    /// that is printed once into a service log is not a control.
    pub fn configure(
        workspace: &Path,
        host: &str,
        token: Option<&str>,
    ) -> Result<Console, String> {
        let store = AuthStore::open(workspace)?;
        // An existing deployment has a console-users.json from a previous version. Carry
        // it across rather than orphaning it: the people holding those tokens are exactly
        // the ones an upgrade would otherwise lock out.
        migrate_accounts_file(workspace, &store)?;
        store.prune_sessions();

        let mut accounts: Vec<Account> = store
            .accounts()?
            .into_iter()
            .map(|(label, role, token_hash)| Account { label, role, token_hash })
            .collect();
        // A key is a credential too, so a console with keys and no user accounts has been
        // configured and must not be treated as though it had not been.
        let has_machine_credential = store.has_live_api_key();
        let mut ephemeral = None;
        if let Some(t) = token {
            if t.trim().is_empty() {
                return Err("--token was given but is empty".into());
            }
            ephemeral = Some(Account {
                label: "token".into(),
                role: Role::Admin,
                token_hash: hash_token(t)?,
            });
        }
        let unconfigured_before_token = accounts.is_empty() && !has_machine_credential;
        if let Some(e) = &ephemeral {
            accounts.push(e.clone());
        }
        let _ = unconfigured_before_token;
        let loopback = is_loopback(host);
        let unconfigured = accounts.is_empty() && !has_machine_credential;
        let mode = match (unconfigured, loopback) {
            (false, _) => Mode::Claimed,
            (true, true) => Mode::Local,
            // Reachable off the machine with nothing configured. It used to refuse here,
            // which is the correct answer when setup means a terminal and the wrong one
            // when setup means opening a page: there would be nothing to open.
            (true, false) => Mode::Unclaimed,
        };
        if mode == Mode::Unclaimed {
            eprintln!(
                "duckle-runner: NOT SET UP. This console is reachable on {host} and has no \
                 accounts, so for the next {} minutes anyone who can reach it can claim it \
                 and become its administrator. Open it now and finish setup, or stop it and \
                 start again with DUCKLE_CONSOLE_TOKEN set.",
                CLAIM_WINDOW_SECS / 60
            );
        }
        Ok(Console {
            ephemeral,
            mode: Mutex::new(mode),
            opened_at: now_secs(),
            open: unconfigured && loopback,
            accounts: Mutex::new(accounts),
            store: Mutex::new(store),
            workspace: workspace.to_path_buf(),
            verified: Mutex::new(HashMap::new()),
        })
    }



    /// Who has an account, without anything that could be replayed.
    pub fn list_accounts(&self) -> Result<Vec<(String, Role)>, String> {
        let store = self.store.lock().map_err(|_| "credential store busy")?;
        Ok(store.accounts()?.into_iter().map(|(l, r, _)| (l, r)).collect())
    }

    /// Add a person. Returns their token once; it is kept only as a hash.
    pub fn create_account(&self, label: &str, role: Role) -> Result<String, String> {
        let label = label.trim();
        if label.is_empty() {
            return Err("an account needs a name".into());
        }
        let token = add_account(&self.workspace, label, role)?;
        self.refresh_accounts();
        Ok(token)
    }

    /// Remove a person. Their sessions die with the account at the next check.
    pub fn remove_account(&self, label: &str) -> Result<bool, String> {
        let removed = {
            let store = self.store.lock().map_err(|_| "credential store busy")?;
            store.remove_account(label)?
        };
        self.refresh_accounts();
        Ok(removed)
    }

    /// Machine credentials, with enough to decide whether one is still in use.
    pub fn list_keys(&self) -> Result<Vec<crate::auth_store::ApiKeyInfo>, String> {
        let store = self.store.lock().map_err(|_| "credential store busy")?;
        store.api_keys()
    }

    /// Mint a machine credential. Returned once, kept as a hash.
    pub fn create_key(&self, label: &str, role: Role, expires_days: Option<i64>) -> Result<String, String> {
        let label = label.trim();
        if label.is_empty() {
            return Err("a key needs a name, so it can be recognised later".into());
        }
        let expires_at = expires_days.map(|d| crate::auth_store::now_secs() + d * 86_400);
        let store = self.store.lock().map_err(|_| "credential store busy")?;
        store.create_api_key(label, role, expires_at)
    }

    /// Revoke a machine credential. Takes effect on the next request, not the next restart.
    pub fn revoke_key(&self, label: &str) -> Result<usize, String> {
        let store = self.store.lock().map_err(|_| "credential store busy")?;
        store.revoke_api_key(label)
    }

    /// Re-read the accounts this process authorises against.
    ///
    /// Adding or removing one happens while the server is running, so a snapshot taken at
    /// startup would leave a new colleague unable to sign in, and a removed one still able
    /// to, until somebody restarted it.
    fn refresh_accounts(&self) {
        let fresh = self.store.lock().ok().and_then(|s| s.accounts().ok());
        if let (Some(fresh), Ok(mut held)) = (fresh, self.accounts.lock()) {
            let mut rebuilt: Vec<Account> = fresh
                .into_iter()
                .map(|(label, role, token_hash)| Account { label, role, token_hash })
                .collect();
            // The --token account lives only here, so a re-read has to restore it.
            if let Some(e) = &self.ephemeral {
                rebuilt.push(e.clone());
            }
            *held = rebuilt;
        }
        // A removed account must stop being admitted by the verified-token cache too.
        if let Ok(mut cache) = self.verified.lock() {
            cache.clear();
        }
    }

    /// Parse a role from what a form or an API client sent.
    pub fn role_from(name: &str) -> Option<Role> {
        match name.trim().to_ascii_lowercase().as_str() {
            "viewer" => Some(Role::Viewer),
            "operator" => Some(Role::Operator),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }

    /// Which state this console is in.
    pub fn mode(&self) -> Mode {
        self.mode.lock().map(|m| *m).unwrap_or(Mode::Claimed)
    }

    /// Whether an unclaimed console can still be claimed.
    ///
    /// False once the window has passed, and the console then answers nothing until it is
    /// restarted. Leaving it claimable forever would mean a box that was never finished
    /// stays open to whoever finds it.
    pub fn claimable(&self) -> bool {
        self.mode() == Mode::Unclaimed && now_secs() - self.opened_at < CLAIM_WINDOW_SECS
    }

    /// Take an unclaimed console: create its first administrator and close the window.
    ///
    /// Returns the new token once. Refuses if the console was already claimed, which is
    /// what makes this safe to expose without a credential: it works exactly once.
    pub fn claim(&self, label: &str) -> Result<String, String> {
        if self.mode() != Mode::Unclaimed {
            return Err("this console has already been set up".into());
        }
        if !self.claimable() {
            return Err(
                "the setup window has closed. Restart the server to open it again, or set \
                 DUCKLE_CONSOLE_TOKEN and set up from there"
                    .into(),
            );
        }
        let label = label.trim();
        if label.is_empty() {
            return Err("an administrator needs a name".into());
        }
        let store = AuthStore::open(&self.workspace)?;
        if !store.accounts()?.is_empty() {
            // Another request claimed it between the check above and here.
            return Err("this console has already been set up".into());
        }
        let token = add_account(&self.workspace, label, Role::Admin)?;
        // The process is already serving. Pick up the account that was just created and
        // leave the unclaimed state, or the person who set it up cannot use what they set
        // up until somebody restarts it.
        self.refresh_accounts();
        if let Ok(mut m) = self.mode.lock() {
            *m = Mode::Claimed;
        }
        Ok(token)
    }

    /// Whether this console admits anyone, which is only ever true on loopback
    /// with nothing configured.
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Who is calling, from the request's `Authorization` and `Cookie` headers.
    ///
    /// `None` means the request is unauthenticated and must be refused. An open
    /// console reports a local admin so callers need no special case.
    pub fn identify(&self, authorization: Option<&str>, cookie: Option<&str>) -> Option<Identity> {
        if self.open {
            return Some(Identity { label: "local".into(), role: Role::Admin });
        }
        // A session cookie first: it is the browser's normal path and costs an indexed
        // lookup rather than a password hash.
        if let Some(sid) = cookie.and_then(|c| cookie_value(c, SESSION_COOKIE)) {
            if let Ok(store) = self.store.lock() {
                if let Some((label, role)) = store.session(&sid) {
                    return Some(Identity { label, role });
                }
            }
        }
        let bearer = authorization?.strip_prefix("Bearer ")?.trim();
        // An API key is a hash lookup and an account token is an Argon2 verify, so the
        // cheap one goes first. Reading the key on every request is also what makes
        // revoking one take effect on a console that is already running: a revocation that
        // waited for a restart would be a promise rather than a control.
        if let Ok(store) = self.store.lock() {
            if let Some((label, role)) = store.api_key(bearer) {
                return Some(Identity { label, role });
            }
        }
        self.verify_token(bearer)
    }

    /// Exchange a token for a session id to put in a cookie.
    ///
    /// The browser holds the session id, not the token, so a stored cookie
    /// cannot be replayed against the API after the server restarts.
    pub fn sign_in(&self, token: &str) -> Option<(String, Identity)> {
        let identity = self.verify_token(token)?;
        let mut raw = [0u8; 32];
        getrandom::fill(&mut raw).ok()?;
        let sid = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
        if let Ok(store) = self.store.lock() {
            let _ = store.put_session(
                &sid,
                &identity.label,
                identity.role,
                now_secs() + SESSION_TTL_SECS,
            );
        }
        Some((sid, identity))
    }

    /// Mint a session for an identity another mechanism has already verified
    /// (#310).
    ///
    /// Deliberately separate from [`sign_in`], which verifies a token first.
    /// This one verifies NOTHING - the caller has, and the only caller is the
    /// OIDC callback, after a signature check, an issuer, audience, expiry and
    /// nonce check, and a role mapping that refuses a subject no rule matched.
    /// Naming it plainly is the point: a function that hands out a session for
    /// a label and a role should be obvious in a grep, not hidden behind a
    /// parameter on the ordinary path.
    pub fn sign_in_external(&self, label: &str, role: Role) -> String {
        let mut raw = [0u8; 32];
        // Not `ok()?` - a session id that is not random is not a session id,
        // and returning a short one would be worse than failing loudly.
        getrandom::fill(&mut raw).expect("the OS random source");
        let sid = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
        if let Ok(store) = self.store.lock() {
            let _ = store.put_session(&sid, label, role, now_secs() + SESSION_TTL_SECS);
        }
        sid
    }

    pub fn sign_out(&self, cookie: Option<&str>) {
        if let Some(sid) = cookie.and_then(|c| cookie_value(c, SESSION_COOKIE)) {
            if let Ok(store) = self.store.lock() {
                store.drop_session(&sid);
            }
        }
    }

    fn verify_token(&self, token: &str) -> Option<Identity> {
        if token.is_empty() {
            return None;
        }
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        if let Some(id) = self.verified.lock().ok()?.get(&digest) {
            return Some(id.clone());
        }
        // Every account is checked even after a match, so the time taken does
        // not reveal which account a token belongs to or how many exist.
        let mut found: Option<Identity> = None;
        let accounts = self.accounts.lock().ok()?.clone();
        for account in &accounts {
            let Ok(parsed) = PasswordHash::new(&account.token_hash) else {
                continue;
            };
            if Argon2::default().verify_password(token.as_bytes(), &parsed).is_ok() && found.is_none()
            {
                found = Some(Identity { label: account.label.clone(), role: account.role });
            }
        }
        let identity = found?;
        self.verified.lock().ok()?.insert(digest, identity.clone());
        Some(identity)
    }
}

pub const SESSION_COOKIE: &str = "duckle_console";

/// Pull one cookie's value out of a `Cookie:` header.
pub fn cookie_value(header: &str, name: &str) -> Option<String> {
    header.split(';').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k.trim() == name).then(|| v.trim().to_string())
    })
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]")
}

fn hash_token(token: &str) -> Result<String, String> {
    let mut salt_bytes = [0u8; 16];
    getrandom::fill(&mut salt_bytes).map_err(|e| format!("salt rng: {e}"))?;
    let salt = SaltString::encode_b64(&salt_bytes).map_err(|e| format!("salt: {e}"))?;
    Argon2::default()
        .hash_password(token.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("hash token: {e}"))
}

fn read_accounts_file(workspace: &Path) -> Result<Vec<Account>, String> {
    let p = accounts_path(workspace);
    if !p.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
    // A console-users file that will not parse is refused rather than treated
    // as "no accounts", because that would silently reopen the console.
    let accounts: Vec<Account> = serde_json::from_str(&text)
        .map_err(|e| format!("parse {}: {e}", p.display()))?;
    for a in &accounts {
        if PasswordHash::new(&a.token_hash).is_err() {
            return Err(format!(
                "account '{}' in {} has an unreadable tokenHash; expected an Argon2 PHC string",
                a.label,
                p.display()
            ));
        }
    }
    Ok(accounts)
}

/// Write an account file for `label` with a freshly generated token, returning
/// the token so the operator can copy it. It is not stored anywhere.
pub fn add_account(workspace: &Path, label: &str, role: Role) -> Result<String, String> {
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).map_err(|e| format!("token rng: {e}"))?;
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
    let store = AuthStore::open(workspace)?;
    // Carry any previous file across first, so adding the second account does not look
    // like the first one and silently drop everybody who was already there.
    migrate_accounts_file(workspace, &store)?;
    store.add_account(label, role, &hash_token(&token)?)?;
    Ok(token)
}

/// `duckle-runner console ...` - manage who may sign in.
///
/// This exists because a role model nobody can create accounts for is a role
/// model that ships switched off.
pub fn run() -> Result<i32, String> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let sub = args.first().map(String::as_str).unwrap_or("");
    if sub.is_empty() || sub == "-h" || sub == "--help" {
        println!(
            "duckle-runner console - console accounts and roles\n\n\
             USAGE:\n    \
             duckle-runner console add-user <label> --role <viewer|operator|admin> [--workspace <dir>]\n    \
             duckle-runner console list [--workspace <dir>]\n    \
             duckle-runner console key-add <label> --role <role> [--expires-days <n>]\n    \
             duckle-runner console key-list [--workspace <dir>]\n    \
             duckle-runner console key-revoke <label> [--workspace <dir>]\n\n\
             add-user prints a freshly generated token once. It is stored only as an\n\
             Argon2 hash, so it cannot be recovered later; generate a new one instead.\n\n\
             An API key is the same idea for a machine: it carries its own role, has no\n\
             browser session, and can be given an expiry. It is shown once and stored only\n\
             as a hash. key-list shows when each was last used, which is the question worth\n\
             asking before revoking one. Revoking marks the key rather than deleting it, so\n\
             one that turns up in a log afterwards can still be named.\n\n\
             ROLES:\n    \
             viewer     read the dashboard, runs and logs\n    \
             operator   also run pipelines and change schedules\n    \
             admin      also credentials, connections and the workspace itself"
        );
        return Ok(0);
    }

    let mut label = None;
    let mut role = None;
    let mut expires_days: Option<i64> = None;
    let mut workspace = PathBuf::from(".");
    let mut it = args.iter().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--role" => {
                role = Some(match it.next().map(String::as_str) {
                    Some("viewer") => Role::Viewer,
                    Some("operator") => Role::Operator,
                    Some("admin") => Role::Admin,
                    other => {
                        return Err(format!(
                            "--role must be viewer, operator or admin (got {})",
                            other.unwrap_or("nothing")
                        ))
                    }
                })
            }
            "--workspace" => {
                workspace = PathBuf::from(it.next().ok_or("--workspace needs a value")?)
            }
            "--expires-days" => {
                let raw = it.next().ok_or("--expires-days needs a value")?;
                let days: i64 = raw
                    .parse()
                    .map_err(|_| format!("--expires-days wants a number, got {raw}"))?;
                if days <= 0 {
                    return Err("--expires-days must be greater than zero".into());
                }
                expires_days = Some(days);
            }
            other if !other.starts_with("--") && label.is_none() => {
                label = Some(other.to_string())
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    match sub {
        "add-user" => {
            let label = label.ok_or("add-user needs a label, e.g. `console add-user ops`")?;
            let role = role.ok_or("add-user needs --role viewer|operator|admin")?;
            let token = add_account(&workspace, &label, role)?;
            println!("Added '{label}' as {}.", role.as_str());
            println!("\nToken (shown once, not stored):\n  {token}\n");
            println!("Use it as a header:  Authorization: Bearer {token}");
            println!("Accounts: {}", crate::auth_store::db_path(&workspace).display());
            Ok(0)
        }
        "list" => {
            let store = AuthStore::open(&workspace)?;
            migrate_accounts_file(&workspace, &store)?;
            let accounts = store.accounts()?;
            if accounts.is_empty() {
                println!("No console accounts in {}.", crate::auth_store::db_path(&workspace).display());
            }
            for (label, role, _) in &accounts {
                println!("{:<24} {}", label, role.as_str());
            }
            Ok(0)
        }
        "key-add" => {
            let label = label.ok_or("key-add needs a label, e.g. `key-add nightly-loader`")?;
            let role = role.ok_or("key-add needs --role viewer|operator|admin")?;
            let store = AuthStore::open(&workspace)?;
            let expires_at = expires_days.map(|d| crate::auth_store::now_secs() + d * 86_400);
            let key = store.create_api_key(&label, role, expires_at)?;
            println!("Added API key '{label}' as {}.", role.as_str());
            match expires_days {
                Some(d) => println!("Expires in {d} day(s)."),
                None => println!("No expiry. Revoke it when the machine is retired."),
            }
            println!();
            println!("Key (shown once, not stored):");
            println!("  {key}");
            println!();
            println!("Use it as a header:  Authorization: Bearer {key}");
            Ok(0)
        }
        "key-list" => {
            let store = AuthStore::open(&workspace)?;
            let keys = store.api_keys()?;
            if keys.is_empty() {
                println!("No API keys in {}.", crate::auth_store::db_path(&workspace).display());
            }
            let now = crate::auth_store::now_secs();
            for k in &keys {
                let state = if k.revoked {
                    "revoked"
                } else if k.expires_at.is_some_and(|e| e <= now) {
                    "expired"
                } else {
                    "live"
                };
                // The question before revoking anything is whether it is still in service.
                let used = match k.last_used_at {
                    Some(t) => format!("last used {}d ago", (now - t) / 86_400),
                    None => "never used".to_string(),
                };
                println!("{:<24} {:<9} {:<8} {}", k.label, k.role.as_str(), state, used);
            }
            Ok(0)
        }
        "key-revoke" => {
            let label = label.ok_or("key-revoke needs a label")?;
            let store = AuthStore::open(&workspace)?;
            match store.revoke_api_key(&label)? {
                0 => {
                    println!("No live API key called '{label}'.");
                    Ok(1)
                }
                n => {
                    println!("Revoked {n} key(s) called '{label}'. It stops working immediately.");
                    Ok(0)
                }
            }
        }
        other => Err(format!("unknown console command: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_off_this_machine_without_a_credential_is_unclaimed_never_open() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The case that matters: exposed to the network with no way to tell callers
        // apart. It once served everyone and printed a warning, then it refused to start,
        // and it now starts claimable so setup can happen in a browser. What must never
        // come back is the first of those.
        let console = Console::configure(ws, "0.0.0.0", None).expect("starts, to be claimed");
        assert_eq!(console.mode(), Mode::Unclaimed);
        assert!(!console.is_open(), "an unclaimed console must not admit anyone");
        assert!(console.identify(None, None).is_none(), "no credential was admitted");
        assert!(console.claimable(), "it should be claimable right after starting");

        // With a token it is configured, so there is no window at all.
        let console = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        assert_eq!(console.mode(), Mode::Claimed);
        assert!(!console.is_open());
        assert!(!console.claimable(), "a configured console is not claimable");

        // Loopback with nothing configured is unchanged: no token, no setup, no window.
        let local = Console::configure(ws, "127.0.0.1", None).expect("loopback starts");
        assert_eq!(local.mode(), Mode::Local);
        assert!(local.is_open(), "requiring a token on localhost breaks the local workflow");
        assert!(!local.claimable(), "there is nothing to claim on a machine you are sitting at");
    }

    /// Adding a colleague must not sign out the person adding them.
    ///
    /// The --token account exists only in the running process and is in no store, so
    /// re-reading the stored accounts after a change dropped it. The first thing an
    /// administrator does through the new screens is add someone, and the reward was being
    /// locked out of the console they had just been using.
    #[test]
    fn adding_an_account_does_not_sign_out_the_token_that_started_the_server() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let console = Console::configure(ws, "0.0.0.0", Some("starter-token")).expect("starts");
        assert!(
            console.identify(Some("Bearer starter-token"), None).is_some(),
            "the starting token should work before anything else happens"
        );

        console.create_account("bob", Role::Operator).expect("adds a colleague");

        assert!(
            console.identify(Some("Bearer starter-token"), None).is_some(),
            "adding an account signed out the token that started the server"
        );
        assert_eq!(console.list_accounts().unwrap().len(), 1, "bob is stored, the token is not");
    }

    /// The other half: a removed account stops being admitted straight away, rather than
    /// lingering in the verified-token cache until a restart.
    #[test]
    fn a_removed_account_stops_being_admitted_immediately() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let console = Console::configure(ws, "0.0.0.0", Some("starter-token")).expect("starts");
        let token = console.create_account("carol", Role::Operator).expect("adds");
        let bearer = format!("Bearer {token}");
        assert!(console.identify(Some(&bearer), None).is_some(), "carol can sign in");

        assert!(console.remove_account("carol").expect("removes"));
        assert!(
            console.identify(Some(&bearer), None).is_none(),
            "a removed account was still admitted from cache"
        );
    }

    /// Setting up in a browser means the first person to finish becomes the administrator.
    #[test]
    fn claiming_creates_the_first_administrator() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let console = Console::configure(ws, "0.0.0.0", None).expect("starts");

        let token = console.claim("sourav").expect("claim succeeds");
        let claimed = Console::configure(ws, "0.0.0.0", None).expect("starts");
        assert_eq!(claimed.mode(), Mode::Claimed, "claiming must configure it");

        let who = claimed
            .identify(Some(&format!("Bearer {token}")), None)
            .expect("the new administrator can sign in");
        assert_eq!(who.label, "sourav");
        assert_eq!(who.role, Role::Admin);
    }

    /// The reason claiming is safe to expose without a credential: it works exactly once.
    #[test]
    fn a_console_cannot_be_claimed_twice() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let console = Console::configure(ws, "0.0.0.0", None).expect("starts");
        console.claim("first").expect("first claim succeeds");

        let err = console.claim("second").expect_err("a second claim must be refused");
        assert!(err.contains("already been set up"), "unhelpful refusal: {err}");

        // And a fresh process sees a configured console, not a claimable one.
        let after = Console::configure(ws, "0.0.0.0", None).expect("starts");
        assert!(!after.claimable());
        assert!(after.claim("third").is_err(), "a claimed console stayed claimable");
    }

    /// A box someone started and walked away from must not stay open to whoever finds it.
    #[test]
    fn the_claim_window_closes() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let mut console = Console::configure(ws, "0.0.0.0", None).expect("starts");
        assert!(console.claimable());

        // Age it past the window, which is the only thing keeping it claimable.
        console.opened_at = now_secs() - CLAIM_WINDOW_SECS - 1;
        assert!(!console.claimable(), "the window should have closed");

        let err = console.claim("late").expect_err("claiming after the window must fail");
        assert!(err.contains("Restart"), "the refusal should say how to recover: {err}");
    }

    /// Sessions used to live only in the process, so every restart signed everyone out.
    /// On a rolling deployment that is every release, which is the difference between a
    /// console people keep open and one they avoid.
    #[test]
    fn a_signed_in_session_survives_a_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let first = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let (sid, who) = first.sign_in("s3cret-token").expect("token signs in");
        assert_eq!(who.role, Role::Admin);
        let cookie = format!("{SESSION_COOKIE}={sid}");
        drop(first);

        // Same workspace, new process.
        let restarted = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let after = restarted
            .identify(None, Some(&cookie))
            .expect("the session should outlive the process that minted it");
        assert_eq!(after.role, Role::Admin);
        assert_eq!(after.label, who.label);
    }

    /// The credential database sits in the workspace, which gets backed up and copied
    /// around, so it must not be a file of working credentials. Only a hash of the session
    /// id is written; the id itself exists in the browser and nowhere else.
    #[test]
    fn the_session_store_holds_nothing_that_can_be_replayed() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let console = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let (sid, _) = console.sign_in("s3cret-token").expect("signs in");

        let bytes = std::fs::read(crate::auth_store::db_path(ws)).expect("database written");
        let haystack = String::from_utf8_lossy(&bytes);
        assert!(!haystack.contains(&sid), "the session id itself was written down");
        assert!(!haystack.contains("s3cret-token"), "the token was written down");
    }

    /// A session that never expires is a credential that never expires.
    #[test]
    fn an_expired_session_is_refused_and_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        {
            let store = crate::auth_store::AuthStore::open(ws).expect("opens");
            store
                .put_session("stale-sid", "alice", Role::Admin, crate::auth_store::now_secs() - 1)
                .expect("stored");
        }

        let console = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        assert!(
            console.identify(None, Some(&format!("{SESSION_COOKIE}=stale-sid"))).is_none(),
            "an expired session was admitted"
        );
    }

    /// Running `serve` for operations and `web` for authoring against one workspace is a
    /// normal setup, and both keep sessions in the same file. Writing the whole map from a
    /// copy loaded at startup meant whichever signed in last erased the other's sessions.
    /// Raised by Dariusz Danielewski, who was right that a JSON file needs the same care
    /// here as anywhere else; the store this repository already uses for schedules takes a
    /// lock around read-modify-write, and this one now does too.
    #[test]
    fn two_processes_sharing_a_workspace_do_not_erase_each_others_sessions() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let ops = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let editor = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");

        let (a, _) = ops.sign_in("s3cret-token").expect("signs in");
        let (b, _) = editor.sign_in("s3cret-token").expect("signs in");

        // A third process reads what is actually on disk.
        let fresh = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        for (who, sid) in [("the ops console", &a), ("the editor", &b)] {
            assert!(
                fresh.identify(None, Some(&format!("{SESSION_COOKIE}={sid}"))).is_some(),
                "{who}'s session was erased by the other process"
            );
        }
    }

    /// Signing out has to mean signed out everywhere, not just in the process that
    /// happened to serve the request.
    #[test]
    fn signing_out_survives_a_restart_too() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let console = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        let (sid, _) = console.sign_in("s3cret-token").expect("signs in");
        let cookie = format!("{SESSION_COOKIE}={sid}");
        console.sign_out(Some(&cookie));
        drop(console);

        let restarted = Console::configure(ws, "0.0.0.0", Some("s3cret-token")).expect("starts");
        assert!(
            restarted.identify(None, Some(&cookie)).is_none(),
            "a signed-out session came back after a restart"
        );
    }

    /// An existing deployment has a console-users.json written by the previous version.
    /// Moving the store must carry those accounts across, not orphan them: the people who
    /// have those tokens are the ones who would be locked out.
    #[test]
    fn accounts_in_the_old_file_are_carried_into_the_database() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let token = "an-existing-operator-token";
        std::fs::create_dir_all(accounts_path(ws).parent().unwrap()).unwrap();
        std::fs::write(
            accounts_path(ws),
            serde_json::to_string(&vec![Account {
                label: "carol".into(),
                role: Role::Operator,
                token_hash: hash_token(token).unwrap(),
            }])
            .unwrap(),
        )
        .unwrap();

        let console = Console::configure(ws, "0.0.0.0", None).expect("starts on existing accounts");
        let who = console
            .identify(Some(&format!("Bearer {token}")), None)
            .expect("an existing token must keep working");
        assert_eq!(who.label, "carol");
        assert_eq!(who.role, Role::Operator);

        let store = crate::auth_store::AuthStore::open(ws).expect("opens");
        assert_eq!(store.accounts().unwrap().len(), 1, "the account should now live in the database");
    }

    /// The thing accounts never fitted: a machine has no browser, no session and nobody to
    /// rotate a password. It presents a key and gets the role that key was made with.
    #[test]
    fn an_api_key_identifies_the_machine_it_was_made_for() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let key = {
            let store = crate::auth_store::AuthStore::open(ws).expect("opens");
            store.create_api_key("nightly-loader", Role::Operator, None).expect("mints")
        };

        let console = Console::configure(ws, "0.0.0.0", Some("admin-token")).expect("starts");
        let who = console
            .identify(Some(&format!("Bearer {key}")), None)
            .expect("an API key should identify its machine");
        assert_eq!(who.label, "nightly-loader");
        assert_eq!(who.role, Role::Operator, "a key carries its own role, not the caller's");
    }

    /// Revoking has to take effect for a process that is already running, or revocation is
    /// a promise rather than a control.
    #[test]
    fn a_revoked_api_key_stops_identifying_without_a_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let store = crate::auth_store::AuthStore::open(ws).expect("opens");
        let key = store.create_api_key("ci", Role::Viewer, None).expect("mints");

        let console = Console::configure(ws, "0.0.0.0", Some("admin-token")).expect("starts");
        assert!(console.identify(Some(&format!("Bearer {key}")), None).is_some());

        store.revoke_api_key("ci").expect("revokes");
        assert!(
            console.identify(Some(&format!("Bearer {key}")), None).is_none(),
            "a revoked key was still admitted by a running console"
        );
    }

    #[test]
    fn only_the_right_token_identifies_a_caller() {
        let tmp = tempfile::tempdir().unwrap();
        let console =
            Console::configure(tmp.path(), "0.0.0.0", Some("s3cret-token")).expect("starts");

        assert!(console.identify(None, None).is_none(), "no credential was admitted");
        assert!(console.identify(Some("Bearer wrong"), None).is_none(), "wrong token admitted");
        assert!(console.identify(Some("s3cret-token"), None).is_none(), "raw header admitted");

        let who = console.identify(Some("Bearer s3cret-token"), None).expect("correct token");
        assert_eq!(who.role, Role::Admin, "--token is the operator's own credential");
    }

    #[test]
    fn a_session_cookie_stands_in_for_the_token() {
        let tmp = tempfile::tempdir().unwrap();
        let console =
            Console::configure(tmp.path(), "0.0.0.0", Some("s3cret-token")).expect("starts");

        assert!(console.sign_in("wrong").is_none(), "signed in with the wrong token");
        let (sid, who) = console.sign_in("s3cret-token").expect("sign in");
        assert_eq!(who.role, Role::Admin);
        // The session id is not the token, so a stolen cookie is not a token.
        assert_ne!(sid, "s3cret-token");

        let cookie = format!("{SESSION_COOKIE}={sid}");
        assert!(console.identify(None, Some(&cookie)).is_some(), "session not honoured");
        console.sign_out(Some(&cookie));
        assert!(console.identify(None, Some(&cookie)).is_none(), "sign-out left the session live");
    }

    #[test]
    fn accounts_carry_their_own_role_and_the_file_never_holds_the_token() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let token = add_account(ws, "reporting", Role::Viewer).expect("add account");
        let bytes = std::fs::read(crate::auth_store::db_path(ws)).expect("database written");
        let stored = String::from_utf8_lossy(&bytes);
        assert!(!stored.contains(&token), "the token itself was written to disk");
        assert!(stored.contains("argon2"), "expected an Argon2 hash");

        let console = Console::configure(ws, "0.0.0.0", None).expect("accounts are a credential");
        let who = console
            .identify(Some(&format!("Bearer {token}")), None)
            .expect("the generated token works");
        assert_eq!(who.role, Role::Viewer);
        assert_eq!(who.label, "reporting");
        assert!(!who.role.allows(Role::Operator), "a viewer must not be able to run pipelines");
    }

    #[test]
    fn roles_include_the_ones_below_them() {
        assert!(Role::Admin.allows(Role::Operator) && Role::Admin.allows(Role::Viewer));
        assert!(Role::Operator.allows(Role::Viewer));
        assert!(!Role::Operator.allows(Role::Admin));
        assert!(!Role::Viewer.allows(Role::Operator));
    }

    #[test]
    fn an_unreadable_accounts_file_stops_the_server_rather_than_opening_it() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(accounts_path(ws).parent().unwrap()).unwrap();
        std::fs::write(accounts_path(ws), b"{ not json").unwrap();
        // Treating a corrupt file as "no accounts" would silently drop the
        // console back to unauthenticated, which is the worst possible default.
        assert!(Console::configure(ws, "0.0.0.0", None).is_err());
        assert!(Console::configure(ws, "127.0.0.1", None).is_err());
    }
}
