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
    // rustls-native-certs 0.8 reports partial success: a store can yield usable
    // certificates AND errors at once, where the old Result made it all or
    // nothing. Take whatever loaded and report the rest, so one unreadable
    // certificate no longer costs the machine its whole OS trust store.
    let native = rustls_native_certs::load_native_certs();
    if !native.certs.is_empty() {
        let _ = roots.add_parsable_certificates(native.certs);
    }
    for e in &native.errors {
        eprintln!("duckle: could not read part of the OS certificate store: {e}");
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
static AGENT_CACHE: Mutex<Option<Vec<(HttpTransport, ureq::Agent)>>> = Mutex::new(None);

/// #256: the transport settings an HTTP request can vary, in one place.
///
/// These used to be either global (the proxy, one value for the whole process)
/// or absent entirely (timeouts, User-Agent). One global proxy is demonstrably
/// the wrong granularity - the desktop's local llama-server calls deliberately
/// bypass the shared agent because the proxy would apply to them too - and no
/// timeout at all means a hung socket parks a stage forever, which matters more
/// now that AI stages keep several requests in flight at once.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct HttpTransport {
    /// Proxy URL for this transport. None falls back to the Settings override
    /// and then the environment, which is the behaviour every caller had.
    pub proxy: Option<String>,
    /// Seconds a single read may stall before the request fails. This is a
    /// per-read deadline, not a deadline on the whole transfer, so streaming a
    /// large file is unaffected as long as bytes keep arriving.
    pub read_timeout_secs: Option<u64>,
    /// Seconds to wait for the connection itself.
    pub connect_timeout_secs: Option<u64>,
    /// User-Agent sent on every request. Some public sites answer 403 to the
    /// default one.
    pub user_agent: Option<String>,
}

/// A read that has delivered nothing for this long is stalled, not slow. Set
/// generously: the point is that a dead socket cannot park a stage forever, not
/// to police how long a server may take. Override with DUCKLE_HTTP_READ_TIMEOUT.
const DEFAULT_READ_TIMEOUT_SECS: u64 = 300;
/// A connection that has not been established by now is not going to be.
/// Override with DUCKLE_HTTP_CONNECT_TIMEOUT.
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;

fn env_secs(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
}

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
    http_agent_with(&HttpTransport::default())
}

/// Enforce `network.allowedDomains` at the moment a request is made, not only
/// when the pipeline was compiled.
///
/// Two hooks, because neither alone is enough:
///
/// * the **resolver** fires once per connection AND once for every redirect hop
///   (ureq handles redirects inside `unit::connect`), so it is the only place
///   that sees where a 302 actually went;
/// * the **middleware** fires once, before anything is sent, and sees the
///   request URL - which the resolver does NOT when a proxy is configured,
///   because ureq then resolves the PROXY's netloc instead
///   (ureq-2.12.1 src/stream.rs:359).
///
/// That last asymmetry leaves one case the resolver cannot cover: a redirect
/// while a proxy is in use. Rather than document a hole, redirects are turned
/// off for that combination - a 3xx is then returned to the caller instead of
/// being followed somewhere nobody checked.
fn with_network_policy(builder: ureq::AgentBuilder, via_proxy: bool) -> ureq::AgentBuilder {
    use std::net::ToSocketAddrs;
    let builder = builder
        .resolver(|netloc: &str| {
            crate::policy::outbound_host_allowed(netloc)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::PermissionDenied, e))?;
            ToSocketAddrs::to_socket_addrs(netloc).map(|i| i.collect())
        })
        .middleware(
            |req: ureq::Request, next: ureq::MiddlewareNext| -> Result<ureq::Response, ureq::Error> {
                if let Err(e) = crate::policy::outbound_host_allowed(req.url()) {
                    // ureq's own error constructors are crate-private; the
                    // io::Error conversion is the public way in.
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        e,
                    )
                    .into());
                }
                next.handle(req)
            },
        );
    if via_proxy {
        builder.redirects(0)
    } else {
        builder
    }
}

/// #256: the agent for one set of transport settings.
///
/// Every HTTP-backed component in the engine goes through here, so a timeout or
/// a proxy set once applies to all of them rather than being re-implemented per
/// connector. Agents are cached by their effective settings: they are internally
/// reference-counted and hold the connection pool, so building one per request
/// would throw away keep-alive.
pub fn http_agent_with(transport: &HttpTransport) -> ureq::Agent {
    // Resolve everything BEFORE the cache lookup, so the key is what the agent
    // was actually built with. A proxy set after startup then rebuilds rather
    // than being frozen at first use (#80).
    let want = HttpTransport {
        proxy: transport.proxy.clone().or_else(current_proxy),
        read_timeout_secs: Some(
            transport
                .read_timeout_secs
                .or_else(|| env_secs("DUCKLE_HTTP_READ_TIMEOUT"))
                .unwrap_or(DEFAULT_READ_TIMEOUT_SECS),
        ),
        connect_timeout_secs: Some(
            transport
                .connect_timeout_secs
                .or_else(|| env_secs("DUCKLE_HTTP_CONNECT_TIMEOUT"))
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS),
        ),
        user_agent: transport.user_agent.clone(),
    };
    {
        let cache = AGENT_CACHE.lock().unwrap();
        if let Some(entries) = cache.as_ref() {
            if let Some((_, agent)) = entries.iter().find(|(have, _)| *have == want) {
                return agent.clone();
            }
        }
    }
    let mut builder = ureq::AgentBuilder::new().tls_config(Arc::new(build_client_config()));
    if crate::policy::network_is_restricted() {
        builder = with_network_policy(builder, want.proxy.is_some());
    }
    if let Some(url) = &want.proxy {
        match ureq::Proxy::new(url) {
            Ok(p) => builder = builder.proxy(p),
            Err(e) => eprintln!("duckle: ignoring invalid proxy '{url}': {e}"),
        }
    }
    if let Some(secs) = want.read_timeout_secs {
        builder = builder.timeout_read(std::time::Duration::from_secs(secs));
    }
    if let Some(secs) = want.connect_timeout_secs {
        builder = builder.timeout_connect(std::time::Duration::from_secs(secs));
    }
    if let Some(ua) = &want.user_agent {
        builder = builder.user_agent(ua);
    }
    let agent = builder.build();
    let mut cache = AGENT_CACHE.lock().unwrap();
    let entries = cache.get_or_insert_with(Vec::new);
    // A pipeline has a handful of distinct transports, not hundreds. Cap it so a
    // pathological one cannot grow this without bound; dropping the oldest only
    // costs a rebuild.
    if entries.len() >= 16 {
        entries.remove(0);
    }
    entries.push((want, agent.clone()));
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
