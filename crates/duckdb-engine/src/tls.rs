//! Shared TLS trust configuration for Duckle's HTTP clients.
//!
//! ureq (REST / cloud-API connectors) and reqwest (the desktop engine
//! downloads) both default to the bundled Mozilla root set (webpki-roots),
//! which ignores the operating-system trust store. Behind a TLS-inspecting
//! corporate proxy (Zscaler, Netskope, ...) that re-signs every certificate
//! with its own CA, that CA lives only in the OS store, so the handshake
//! fails with `UnknownIssuer`.
//!
//! We build ONE rustls client config whose root store is the union of:
//!   1. the bundled Mozilla roots (identical to the previous default), plus
//!   2. the OS native trust store (adds the corporate inspection CA), plus
//!   3. an optional explicit PEM bundle pointed at by `DUCKLE_CA_CERT`.
//!
//! It is a strict superset of the old trust set, so non-corporate users see
//! no behavioural change: everything that validated before still validates.
//! The OS store and env bundle are best-effort - a missing or unreadable
//! source just leaves the bundled roots in place.

use std::sync::{Arc, Mutex, RwLock};

/// Assemble the union root store: bundled Mozilla roots, the OS native store,
/// and an optional `DUCKLE_CA_CERT` PEM bundle.
fn build_root_store() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();

    // 1. Bundled Mozilla roots - the prior default on every platform, so no
    //    machine loses trust it had before.
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // 2. OS trust store - adds enterprise / proxy-inspection CAs. Best effort.
    match rustls_native_certs::load_native_certs() {
        Ok(certs) => {
            let _ = roots.add_parsable_certificates(certs);
        }
        Err(e) => {
            eprintln!("duckle: could not read OS certificate store: {e}");
        }
    }

    // 3. Optional explicit PEM bundle, for split-tunnel setups or where the
    //    proxy CA is handed out as a file rather than installed in the store.
    if let Ok(path) = std::env::var("DUCKLE_CA_CERT") {
        if !path.is_empty() {
            match std::fs::read(&path) {
                Ok(bytes) => {
                    let mut rd = std::io::BufReader::new(&bytes[..]);
                    let extra: Vec<_> = rustls_pemfile::certs(&mut rd)
                        .filter_map(Result::ok)
                        .collect();
                    let _ = roots.add_parsable_certificates(extra);
                }
                Err(e) => eprintln!("duckle: DUCKLE_CA_CERT unreadable ({path}): {e}"),
            }
        }
    }

    roots
}

/// Build a fresh rustls client config trusting bundled + OS-native (+ optional
/// `DUCKLE_CA_CERT`) roots. reqwest consumes this via `use_preconfigured_tls`.
pub fn build_client_config() -> rustls::ClientConfig {
    // Match ureq's provider (ring) so we add no second crypto backend and
    // avoid depending on a process-wide default provider being installed.
    rustls::ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .expect("ring provider supports TLS 1.2 + 1.3")
        .with_root_certificates(build_root_store())
        .with_no_client_auth()
}

/// Read an HTTP/HTTPS proxy URL from the environment. Prefers Duckle's own var
/// (so a user can point Duckle at a proxy without changing global env), then the
/// conventional HTTPS_PROXY / ALL_PROXY / HTTP_PROXY (any case). Unlike reqwest,
/// ureq does NOT pick these up on its own, so behind a corporate proxy every
/// REST / cloud-API call would connect directly and time out (os error 10060,
/// issue #80). The URL may include credentials, e.g. http://user:pass@host:8080.
pub fn proxy_url_from_env() -> Option<String> {
    for key in [
        "DUCKLE_HTTPS_PROXY",
        "DUCKLE_PROXY",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ] {
        if let Ok(v) = std::env::var(key) {
            let v = v.trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// A process-global proxy override set from the desktop Settings, so a user on a
/// locked-down corporate machine who cannot set a system environment variable
/// can still point Duckle at a proxy (#80). Preferred over the environment.
static PROXY_OVERRIDE: RwLock<Option<String>> = RwLock::new(None);
/// The cached ureq agent paired with the proxy it was built for. Keying the
/// cache on the resolved proxy means a proxy set AFTER startup rebuilds the
/// agent, instead of being frozen no-proxy at first use (the old OnceLock bug:
/// the startup update-check built the agent before any proxy was known, #80).
static AGENT_CACHE: Mutex<Option<(Option<String>, ureq::Agent)>> = Mutex::new(None);

/// Set (or clear) the HTTP/HTTPS proxy at run time, from the desktop Settings.
/// Mirrors the value into HTTPS_PROXY / HTTP_PROXY so the reqwest clients (engine
/// + model downloads, the in-app updater) pick it up too, and invalidates the
/// cached ureq agent so the next REST / cloud call rebuilds with the proxy.
pub fn set_proxy(url: Option<String>) {
    let url = url.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    *PROXY_OVERRIDE.write().unwrap() = url.clone();
    if let Some(u) = &url {
        std::env::set_var("HTTPS_PROXY", u);
        std::env::set_var("HTTP_PROXY", u);
    }
    *AGENT_CACHE.lock().unwrap() = None;
}

/// The proxy URL in effect: the Settings override first, then the environment.
pub fn current_proxy() -> Option<String> {
    if let Some(u) = PROXY_OVERRIDE.read().unwrap().clone() {
        return Some(u);
    }
    proxy_url_from_env()
}

/// A process-wide ureq agent using the merged trust config above, honoring any
/// configured proxy (#80). The agent is internally reference-counted, so cloning
/// it per request is cheap. It is cached keyed by the resolved proxy, so a proxy
/// set after startup rebuilds it rather than being frozen at first use.
pub fn http_agent() -> ureq::Agent {
    let want = current_proxy();
    {
        let cache = AGENT_CACHE.lock().unwrap();
        if let Some((have, agent)) = cache.as_ref() {
            if *have == want {
                return agent.clone();
            }
        }
    }
    let mut builder = ureq::AgentBuilder::new().tls_config(Arc::new(build_client_config()));
    if let Some(url) = &want {
        match ureq::Proxy::new(url) {
            Ok(p) => builder = builder.proxy(p),
            Err(e) => eprintln!("duckle: ignoring invalid proxy '{url}': {e}"),
        }
    }
    let agent = builder.build();
    *AGENT_CACHE.lock().unwrap() = Some((want, agent.clone()));
    agent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_store_is_a_superset_of_bundled_roots() {
        // The merged store must contain at least every bundled Mozilla root,
        // so non-corporate users never lose trust they had before.
        let bundled = webpki_roots::TLS_SERVER_ROOTS.len();
        let merged = build_root_store().roots.len();
        assert!(
            merged >= bundled,
            "merged roots ({merged}) dropped below bundled roots ({bundled})"
        );
    }

    #[test]
    fn agent_builds() {
        let _ = http_agent();
    }

    #[test]
    fn proxy_env_prefers_duckle_var() {
        // The Duckle-specific var wins over the conventional ones so a user can
        // point Duckle at a proxy without changing global env. (Best-effort
        // env-mutation test; the value is harmless - it is never connected to.)
        std::env::set_var("DUCKLE_HTTPS_PROXY", "http://proxy.example:8080");
        assert_eq!(
            proxy_url_from_env().as_deref(),
            Some("http://proxy.example:8080")
        );
        std::env::remove_var("DUCKLE_HTTPS_PROXY");
    }
}
