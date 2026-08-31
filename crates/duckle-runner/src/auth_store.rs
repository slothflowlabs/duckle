//! Accounts, sessions and API keys, in SQLite.
//!
//! These three stores are read on every request and written by more than one process:
//! running `serve` for operations and `web` for authoring against one workspace is an
//! ordinary setup, and both touch the same records. JSON files handled that with a lock
//! around read-modify-write, which works and is exactly the job a transactional store
//! already does properly.
//!
//! Three kinds of credential, deliberately not one:
//!
//!   * an **account** belongs to a person. Its token may be chosen by a human, so it is
//!     hashed with Argon2 and treated as a password.
//!   * a **session** is what a browser holds after signing in. The row keys on a hash of
//!     the session id, so a copy of this database admits nobody.
//!   * an **API key** belongs to a machine. It is generated here with 256 bits of entropy,
//!     so SHA-256 is the right hash: there is no dictionary to defend against, and an
//!     integration polling every few seconds must not pay for Argon2 on every call.
//!
//! Every hash stored is of something the caller must already possess. Nothing in this file
//! can be replayed by reading it.

use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};

use crate::console_auth::Role;

/// Where the console keeps its credentials.
pub fn db_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("console.db")
}

/// One machine credential, as shown to a human. Never carries the key itself.
#[derive(Debug, Clone)]
pub struct ApiKeyInfo {
    pub label: String,
    pub role: Role,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub last_used_at: Option<i64>,
    pub revoked: bool,
}

impl ApiKeyInfo {
    /// Whether this key would be accepted right now.
    #[allow(dead_code)]
    pub fn is_live(&self, now: i64) -> bool {
        !self.revoked && self.expires_at.is_none_or(|e| e > now)
    }
}

pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// SHA-256, hex. Used for session ids and API keys, both of which this code generates with
/// 256 bits of entropy. Unsalted on purpose: a salt defends a low-entropy secret against a
/// dictionary, and there is no dictionary of random 256-bit values.
pub fn digest(secret: &str) -> String {
    let d: [u8; 32] = Sha256::digest(secret.as_bytes()).into();
    d.iter().map(|b| format!("{b:02x}")).collect()
}

fn role_of(s: &str) -> Role {
    match s {
        "admin" => Role::Admin,
        "operator" => Role::Operator,
        // An unrecognised role reads as the least privilege rather than the most. A
        // corrupted or hand-edited row must not become an admin.
        _ => Role::Viewer,
    }
}

/// The credential database for one workspace.
pub struct AuthStore {
    conn: Connection,
}

impl AuthStore {
    /// Open (creating if needed) and bring the schema up to date.
    pub fn open(workspace: &Path) -> Result<Self, String> {
        let path = db_path(workspace);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
        }
        let conn = Connection::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        // WAL so a reader is never blocked by the writer, and a busy timeout so two
        // processes contend rather than fail. This is the whole reason for using a
        // database here instead of a file and a lock.
        conn.pragma_update(None, "journal_mode", "WAL").map_err(|e| e.to_string())?;
        conn.busy_timeout(std::time::Duration::from_secs(5)).map_err(|e| e.to_string())?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    /// Open in memory. For tests, and for nothing else.
    #[cfg(test)]
    pub fn in_memory() -> Result<Self, String> {
        let conn = Connection::open_in_memory().map_err(|e| e.to_string())?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), String> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS accounts (
                     label       TEXT PRIMARY KEY,
                     role        TEXT NOT NULL,
                     token_hash  TEXT NOT NULL,
                     created_at  INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS sessions (
                     key         TEXT PRIMARY KEY,
                     label       TEXT NOT NULL,
                     role        TEXT NOT NULL,
                     expires_at  INTEGER NOT NULL
                 );
                 CREATE INDEX IF NOT EXISTS sessions_expiry ON sessions(expires_at);
                 CREATE TABLE IF NOT EXISTS api_keys (
                     key_hash     TEXT PRIMARY KEY,
                     label        TEXT NOT NULL,
                     role         TEXT NOT NULL,
                     created_at   INTEGER NOT NULL,
                     expires_at   INTEGER,
                     last_used_at INTEGER,
                     revoked      INTEGER NOT NULL DEFAULT 0
                 );",
            )
            .map_err(|e| format!("migrate: {e}"))
    }

    // ---- accounts ----------------------------------------------------------------

    pub fn add_account(&self, label: &str, role: Role, token_hash: &str) -> Result<(), String> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO accounts (label, role, token_hash, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![label, role.as_str(), token_hash, now_secs()],
            )
            .map(|_| ())
            .map_err(|e| format!("add account: {e}"))
    }

    pub fn accounts(&self) -> Result<Vec<(String, Role, String)>, String> {
        let mut q = self
            .conn
            .prepare("SELECT label, role, token_hash FROM accounts ORDER BY label")
            .map_err(|e| e.to_string())?;
        let rows = q
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    role_of(&r.get::<_, String>(1)?),
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
    }

    pub fn remove_account(&self, label: &str) -> Result<bool, String> {
        self.conn
            .execute("DELETE FROM accounts WHERE label = ?1", params![label])
            .map(|n| n > 0)
            .map_err(|e| format!("remove account: {e}"))
    }

    // ---- sessions ----------------------------------------------------------------

    pub fn put_session(&self, sid: &str, label: &str, role: Role, expires_at: i64) -> Result<(), String> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO sessions (key, label, role, expires_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![digest(sid), label, role.as_str(), expires_at],
            )
            .map(|_| ())
            .map_err(|e| format!("save session: {e}"))
    }

    /// Who a session id belongs to, if it is still live. Expired rows are dropped on the
    /// way past, so the table is pruned by use rather than by a sweeper.
    pub fn session(&self, sid: &str) -> Option<(String, Role)> {
        let key = digest(sid);
        let now = now_secs();
        let found: Option<(String, String, i64)> = self
            .conn
            .query_row(
                "SELECT label, role, expires_at FROM sessions WHERE key = ?1",
                params![key],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .ok()
            .flatten();
        match found {
            Some((label, role, expires_at)) if expires_at > now => Some((label, role_of(&role))),
            Some(_) => {
                let _ = self.conn.execute("DELETE FROM sessions WHERE key = ?1", params![key]);
                None
            }
            None => None,
        }
    }

    pub fn drop_session(&self, sid: &str) {
        let _ = self
            .conn
            .execute("DELETE FROM sessions WHERE key = ?1", params![digest(sid)]);
    }

    /// Remove every expired session. Cheap, indexed, and worth doing at startup.
    pub fn prune_sessions(&self) -> usize {
        self.conn
            .execute("DELETE FROM sessions WHERE expires_at <= ?1", params![now_secs()])
            .unwrap_or(0)
    }

    // ---- API keys ----------------------------------------------------------------

    /// Mint a machine credential and return it once.
    ///
    /// The caller is the only place it will ever exist in the clear: only its hash is
    /// stored, so a lost key is replaced rather than recovered.
    pub fn create_api_key(
        &self,
        label: &str,
        role: Role,
        expires_at: Option<i64>,
    ) -> Result<String, String> {
        let mut raw = [0u8; 32];
        getrandom::fill(&mut raw).map_err(|e| format!("random: {e}"))?;
        // A recognisable prefix so a leaked key is identifiable in a log or a scanner,
        // and so a human can tell it apart from a session id or an account token.
        let key = format!(
            "duckle_{}",
            base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, raw)
        );
        self.conn
            .execute(
                "INSERT INTO api_keys (key_hash, label, role, created_at, expires_at, revoked)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0)",
                params![digest(&key), label, role.as_str(), now_secs(), expires_at],
            )
            .map_err(|e| format!("create api key: {e}"))?;
        Ok(key)
    }

    /// Who an API key belongs to, if it is live. Records that it was used, which is how an
    /// operator finds out which keys are actually in service before revoking one.
    pub fn api_key(&self, presented: &str) -> Option<(String, Role)> {
        let hash = digest(presented);
        let now = now_secs();
        let found: Option<(String, String, Option<i64>, i64)> = self
            .conn
            .query_row(
                "SELECT label, role, expires_at, revoked FROM api_keys WHERE key_hash = ?1",
                params![hash],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .ok()
            .flatten();
        let (label, role, expires_at, revoked) = found?;
        if revoked != 0 || expires_at.is_some_and(|e| e <= now) {
            return None;
        }
        let _ = self.conn.execute(
            "UPDATE api_keys SET last_used_at = ?1 WHERE key_hash = ?2",
            params![now, hash],
        );
        Some((label, role_of(&role)))
    }

    /// Whether any machine credential would be accepted right now.
    ///
    /// A console with API keys and no user accounts is still a console someone configured,
    /// so it must not be treated as unconfigured and opened.
    pub fn has_live_api_key(&self) -> bool {
        let now = now_secs();
        self.conn
            .query_row(
                "SELECT count(*) FROM api_keys
                 WHERE revoked = 0 AND (expires_at IS NULL OR expires_at > ?1)",
                params![now],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0
    }

    pub fn api_keys(&self) -> Result<Vec<ApiKeyInfo>, String> {
        let mut q = self
            .conn
            .prepare(
                "SELECT label, role, created_at, expires_at, last_used_at, revoked
                 FROM api_keys ORDER BY created_at DESC",
            )
            .map_err(|e| e.to_string())?;
        let rows = q
            .query_map([], |r| {
                Ok(ApiKeyInfo {
                    label: r.get(0)?,
                    role: role_of(&r.get::<_, String>(1)?),
                    created_at: r.get(2)?,
                    expires_at: r.get(3)?,
                    last_used_at: r.get(4)?,
                    revoked: r.get::<_, i64>(5)? != 0,
                })
            })
            .map_err(|e| e.to_string())?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
    }

    /// Revoke rather than delete, so a key that turns up in a log later can still be
    /// explained. Returns how many rows changed.
    pub fn revoke_api_key(&self, label: &str) -> Result<usize, String> {
        self.conn
            .execute(
                "UPDATE api_keys SET revoked = 1 WHERE label = ?1 AND revoked = 0",
                params![label],
            )
            .map_err(|e| format!("revoke: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> AuthStore {
        AuthStore::in_memory().expect("opens")
    }

    #[test]
    fn a_session_is_found_by_its_id_and_never_stored_as_one() {
        let s = store();
        s.put_session("sid-abc", "alice", Role::Operator, now_secs() + 60).unwrap();

        let (label, role) = s.session("sid-abc").expect("live session");
        assert_eq!(label, "alice");
        assert_eq!(role, Role::Operator);

        // What is written down is the hash, so a copy of the database admits nobody.
        let stored: String = s
            .conn
            .query_row("SELECT key FROM sessions", [], |r| r.get(0))
            .unwrap();
        assert_ne!(stored, "sid-abc");
        assert_eq!(stored, digest("sid-abc"));
    }

    #[test]
    fn an_expired_session_is_refused_and_removed() {
        let s = store();
        s.put_session("stale", "alice", Role::Admin, now_secs() - 1).unwrap();

        assert!(s.session("stale").is_none(), "an expired session was admitted");
        let left: i64 = s.conn.query_row("SELECT count(*) FROM sessions", [], |r| r.get(0)).unwrap();
        assert_eq!(left, 0, "it should be dropped on the way past");
    }

    #[test]
    fn an_api_key_is_returned_once_and_only_its_hash_is_kept() {
        let s = store();
        let key = s.create_api_key("nightly-loader", Role::Operator, None).unwrap();

        assert!(key.starts_with("duckle_"), "a key should be recognisable: {key}");
        let (label, role) = s.api_key(&key).expect("live key");
        assert_eq!(label, "nightly-loader");
        assert_eq!(role, Role::Operator);

        let stored: String = s.conn.query_row("SELECT key_hash FROM api_keys", [], |r| r.get(0)).unwrap();
        assert_ne!(stored, key, "the key itself was written down");
        assert!(s.api_key("duckle_wrong").is_none(), "a wrong key was admitted");
    }

    #[test]
    fn a_revoked_key_stops_working_but_is_still_explainable() {
        let s = store();
        let key = s.create_api_key("ci", Role::Viewer, None).unwrap();
        assert!(s.api_key(&key).is_some());

        assert_eq!(s.revoke_api_key("ci").unwrap(), 1);
        assert!(s.api_key(&key).is_none(), "a revoked key still worked");
        // Revoked, not deleted: a key that turns up in a log later can still be named.
        assert_eq!(s.api_keys().unwrap().len(), 1);
        assert!(s.api_keys().unwrap()[0].revoked);
    }

    #[test]
    fn an_expired_key_is_refused() {
        let s = store();
        let key = s.create_api_key("temporary", Role::Admin, Some(now_secs() - 1)).unwrap();
        assert!(s.api_key(&key).is_none(), "an expired key was admitted");
    }

    #[test]
    fn using_a_key_records_that_it_was_used() {
        // The question an operator actually has before revoking something is whether
        // anything still uses it.
        let s = store();
        let key = s.create_api_key("maybe-dead", Role::Viewer, None).unwrap();
        assert!(s.api_keys().unwrap()[0].last_used_at.is_none());

        s.api_key(&key).expect("live");
        assert!(
            s.api_keys().unwrap()[0].last_used_at.is_some(),
            "a used key should say so"
        );
    }

    #[test]
    fn a_role_that_is_not_recognised_reads_as_the_least_privilege() {
        // A hand-edited or corrupted row must not become an admin.
        let s = store();
        s.conn
            .execute(
                "INSERT INTO api_keys (key_hash, label, role, created_at, revoked)
                 VALUES (?1, 'odd', 'superuser', 0, 0)",
                params![digest("k")],
            )
            .unwrap();
        assert_eq!(s.api_key("k").expect("found").1, Role::Viewer);
    }

    #[test]
    fn pruning_removes_only_what_has_expired() {
        let s = store();
        s.put_session("live", "a", Role::Viewer, now_secs() + 600).unwrap();
        s.put_session("dead", "b", Role::Viewer, now_secs() - 600).unwrap();

        assert_eq!(s.prune_sessions(), 1);
        assert!(s.session("live").is_some(), "a live session was pruned");
    }
}
