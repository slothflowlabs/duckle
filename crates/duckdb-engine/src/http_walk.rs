//! #256: what one HTTP-backed node keeps between its own requests in a run:
//! when it may send the next one, and the cookies its server set. Built for one
//! stage and dropped with it, never cached with an agent: agents are shared
//! across nodes and runs, and one pipeline's session must not reach another's.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// One node's requests in one run.
#[derive(Debug, Default)]
pub struct HttpWalk {
    /// The least time between two sends, from the node's requests-per-second.
    interval: Option<Duration>,
    /// When the latest request went.
    last_sent: Option<Instant>,
    /// Present when the node keeps cookies.
    cookies: Option<CookieJar>,
}

impl HttpWalk {
    pub fn new(pacing: &crate::tls::HttpPacing) -> Self {
        HttpWalk {
            interval: pacing
                .requests_per_second
                .filter(|r| r.is_finite() && *r > 0.0)
                .map(|r| Duration::from_secs_f64(1.0 / r)),
            last_sent: None,
            cookies: pacing.keep_cookies.then(CookieJar::default),
        }
    }

    /// How long a request about to go must still wait, or zero when it may go now,
    /// in which case `now` is recorded as when the latest request went.
    ///
    /// Asked at the moment of sending and asked again after each wait, so the pace
    /// counts from when the previous request actually went. Booking a slot ahead
    /// spaced the slots rather than the sends: a request that set off late, its
    /// sleep overrunning, left the next closer than the pace. On several threads,
    /// one goes per turn and the others are told to wait on.
    pub fn turn(&mut self, now: Instant) -> Duration {
        let Some(interval) = self.interval else {
            return Duration::ZERO;
        };
        match self.last_sent {
            Some(last) if now < last + interval => last + interval - now,
            _ => {
                self.last_sent = Some(now);
                Duration::ZERO
            }
        }
    }

    /// The `Cookie` header for a request to `url`, when there is one to send.
    pub fn cookie_header(&self, url: &str) -> Option<String> {
        self.cookies.as_ref()?.header_for(url)
    }

    /// Keep the cookies a response from `url` set, when the node keeps cookies.
    pub fn keep(&mut self, url: &str, set_cookies: &[&str]) {
        if let Some(jar) = &mut self.cookies {
            for line in set_cookies {
                jar.set(url, line);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Cookie {
    value: String,
    /// Sent only to the host that set it, unless it named a `Domain`.
    host_only: bool,
    /// Sent only over HTTPS.
    secure: bool,
}

/// Cookies by (domain, name). Paths are not tracked: one node talks to one
/// API, where sending a cookie to a sibling path of the same host is not a
/// difference that matters.
#[derive(Debug, Default)]
struct CookieJar {
    cookies: BTreeMap<(String, String), Cookie>,
}

impl CookieJar {
    /// Take one `Set-Cookie` header from a response to `url`.
    fn set(&mut self, url: &str, line: &str) {
        let host = crate::policy::netloc_host(url);
        let mut parts = line.split(';');
        let Some((name, value)) = parts.next().and_then(|p| p.split_once('=')) else {
            return;
        };
        let (name, value) = (name.trim().to_string(), value.trim().to_string());
        if name.is_empty() {
            return;
        }
        let (mut domain, mut host_only, mut secure, mut expired) = (host.clone(), true, false, false);
        for attr in parts {
            let (k, v) = attr.split_once('=').unwrap_or((attr, ""));
            match k.trim().to_ascii_lowercase().as_str() {
                "domain" => {
                    // A server may widen a cookie to its own parent domain,
                    // never to another site, and not to a bare top-level name.
                    let d = v.trim().trim_start_matches('.').to_ascii_lowercase();
                    if d.contains('.') && domain_matches(&host, &d) {
                        domain = d;
                        host_only = false;
                    } else {
                        return;
                    }
                }
                "secure" => secure = true,
                "max-age" => expired = v.trim().parse::<i64>().map(|s| s <= 0).unwrap_or(false),
                _ => {}
            }
        }
        let key = (domain, name);
        if expired {
            self.cookies.remove(&key);
        } else {
            self.cookies.insert(key, Cookie { value, host_only, secure });
        }
    }

    fn header_for(&self, url: &str) -> Option<String> {
        let host = crate::policy::netloc_host(url);
        let https = url.trim_start().to_ascii_lowercase().starts_with("https://");
        let pairs: Vec<String> = self
            .cookies
            .iter()
            .filter(|((domain, _), c)| if c.host_only { host == *domain } else { domain_matches(&host, domain) })
            .filter(|(_, c)| https || !c.secure)
            .map(|((_, name), c)| format!("{}={}", name, c.value))
            .collect();
        (!pairs.is_empty()).then(|| pairs.join("; "))
    }
}

/// `host` is `domain` or one of its subdomains.
fn domain_matches(host: &str, domain: &str) -> bool {
    host == domain || host.ends_with(&format!(".{domain}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::HttpPacing;

    fn keeping() -> HttpWalk {
        HttpWalk::new(&HttpPacing { requests_per_second: None, keep_cookies: true })
    }

    fn four_a_second() -> HttpWalk {
        HttpWalk::new(&HttpPacing { requests_per_second: Some(4.0), keep_cookies: false })
    }

    /// Spaced from when the previous request actually went, not from a slot booked
    /// for it. Booking slots ahead let a request that set off late, its sleep
    /// overrunning, leave the next one closer than the pace: on a loaded CI runner a
    /// walk at 4 a second sent two requests 186 ms apart.
    #[test]
    fn the_next_turn_counts_from_when_the_last_request_actually_went() {
        let mut w = four_a_second();
        let t = Instant::now();
        assert_eq!(w.turn(t), Duration::ZERO, "the first send goes at once");
        assert_eq!(w.turn(t), Duration::from_millis(250), "the next waits out the pace");
        // Its sleep overran by 40 ms. It goes then, and that is its turn.
        assert_eq!(w.turn(t + Duration::from_millis(290)), Duration::ZERO);
        // So the one after is 250 ms from 290, not from the 250 it was due at.
        assert_eq!(w.turn(t + Duration::from_millis(500)), Duration::from_millis(40));
        assert_eq!(w.turn(t + Duration::from_millis(540)), Duration::ZERO);
    }

    /// On several threads, one goes per turn and the rest wait on.
    #[test]
    fn requests_asking_together_go_one_per_turn() {
        let mut w = four_a_second();
        let t = Instant::now();
        assert_eq!(w.turn(t), Duration::ZERO);
        // Two threads asking at the same moment are both told to wait.
        assert_eq!(w.turn(t), Duration::from_millis(250));
        assert_eq!(w.turn(t), Duration::from_millis(250));
        // At the turn the first to ask again goes, and the other waits a whole pace more.
        assert_eq!(w.turn(t + Duration::from_millis(250)), Duration::ZERO);
        assert_eq!(w.turn(t + Duration::from_millis(250)), Duration::from_millis(250));
        let later = t + Duration::from_secs(5);
        assert_eq!(w.turn(later), Duration::ZERO, "after a long gap nothing is owed");
        assert_eq!(HttpWalk::new(&HttpPacing::default()).turn(t), Duration::ZERO, "no pace, no wait");
    }

    #[test]
    fn a_cookie_goes_back_to_the_host_that_set_it_and_no_other() {
        let mut w = keeping();
        w.keep("https://api.example.com/p1", &["sid=abc; Path=/; HttpOnly"]);
        assert_eq!(w.cookie_header("https://api.example.com/p2").as_deref(), Some("sid=abc"));
        assert_eq!(w.cookie_header("https://other.example.com/p2"), None, "host-only by default");
        assert_eq!(w.cookie_header("https://evil.org/"), None);
    }

    #[test]
    fn a_domain_widens_a_cookie_to_its_subdomains_but_never_to_another_site() {
        let mut w = keeping();
        w.keep("https://login.example.com/", &["sid=abc; Domain=.example.com"]);
        assert_eq!(w.cookie_header("https://api.example.com/").as_deref(), Some("sid=abc"));
        w.keep("https://login.example.com/", &["stolen=1; Domain=evil.org", "wide=1; Domain=com"]);
        assert_eq!(w.cookie_header("https://evil.org/"), None, "a server cannot set another site's cookie");
        assert_eq!(w.cookie_header("https://api.example.com/").as_deref(), Some("sid=abc"));
    }

    #[test]
    fn a_secure_cookie_stays_off_plain_http_and_max_age_zero_removes_it() {
        let mut w = keeping();
        w.keep("https://api.example.com/", &["sid=abc; Secure", "pref=1"]);
        assert_eq!(w.cookie_header("http://api.example.com/").as_deref(), Some("pref=1"));
        assert_eq!(w.cookie_header("https://api.example.com/").as_deref(), Some("pref=1; sid=abc"));
        w.keep("https://api.example.com/", &["sid=; Max-Age=0"]);
        assert_eq!(w.cookie_header("https://api.example.com/").as_deref(), Some("pref=1"));
    }

    #[test]
    fn a_node_that_does_not_keep_cookies_sends_none() {
        let mut w = HttpWalk::new(&HttpPacing::default());
        w.keep("https://api.example.com/", &["sid=abc"]);
        assert_eq!(w.cookie_header("https://api.example.com/"), None);
    }
}
