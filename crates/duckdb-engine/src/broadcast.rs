//! #292: the WebSocket server a follower broadcasts rows through.
//!
//! `duckle-runner follow` runs a pipeline pass after pass. A sink that bound a
//! port itself would bind and release it every pass, and every consumer would be
//! cut off between batches, so the listener lives here instead: the follow loop
//! owns a [`Hub`], the hub keeps one listener per address for as long as the loop
//! runs, and each pass's `snk.wsserver` publishes its rows into it.
//!
//! What it promises, stated rather than implied (decided on #292):
//! - Every consumer attached when a pass publishes is sent that pass's rows. One
//!   that attaches later, or is dropped, misses them: there is no replay, and a
//!   broadcast is not a delivery.
//! - Each consumer has a bounded queue, and one that cannot keep up loses its
//!   OLDEST queued rows, counted and reported per pass. Waiting on a slow
//!   consumer would hold the pass, and with it the follower's position, so one
//!   slow browser could halt ingestion; buffering without a bound only moves
//!   that to running out of memory.
//! - Zero consumers is a successful pass: the position advances either way.

use std::collections::{HashMap, VecDeque};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use tokio_tungstenite::tungstenite::{self, Message};

/// What one publish did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// Consumers attached when the rows were queued.
    pub consumers: usize,
    /// Rows dropped from slow consumers' queues to make room, over all consumers.
    pub dropped: u64,
}

/// One consumer's queue: bounded, losing its oldest messages when full.
pub(crate) struct Outbox {
    cap: usize,
    items: VecDeque<String>,
}

impl Outbox {
    pub(crate) fn new(cap: usize) -> Self {
        Self { cap: cap.max(1), items: VecDeque::new() }
    }

    /// Queue `messages`, dropping the oldest to stay within the bound. Returns
    /// how many had to be dropped.
    pub(crate) fn push_all(&mut self, messages: &[String]) -> u64 {
        let mut dropped = 0;
        for m in messages {
            if self.items.len() >= self.cap {
                self.items.pop_front();
                dropped += 1;
            }
            self.items.push_back(m.clone());
        }
        dropped
    }

    pub(crate) fn pop(&mut self) -> Option<String> {
        self.items.pop_front()
    }
}

struct Consumer {
    outbox: Mutex<Outbox>,
    ready: Condvar,
    closed: AtomicBool,
}

struct Server {
    path: String,
    queue: usize,
    consumers: Mutex<Vec<Arc<Consumer>>>,
    stop: AtomicBool,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// The listeners a follower broadcasts through, one per address, for as long
/// as the hub lives. Dropping it stops them and lets every consumer go.
#[derive(Default)]
pub struct Hub {
    servers: Mutex<HashMap<String, Arc<Server>>>,
}

impl Hub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Queue `messages` for every consumer attached at `addr`, starting the
    /// listener there on first use. Consumers connect on `path` only; `queue` is
    /// the bound on each one's queue, fixed when the listener starts.
    pub fn publish(&self, addr: &str, path: &str, queue: usize, messages: &[String]) -> Result<Delivery, String> {
        let server = self.server(addr, path, queue)?;
        let mut consumers = lock(&server.consumers);
        consumers.retain(|c| !c.closed.load(Ordering::Relaxed));
        let mut dropped = 0;
        for c in consumers.iter() {
            dropped += lock(&c.outbox).push_all(messages);
            c.ready.notify_one();
        }
        Ok(Delivery { consumers: consumers.len(), dropped })
    }

    /// Consumers attached at `addr` now; 0 when nothing listens there.
    pub fn consumers(&self, addr: &str) -> usize {
        lock(&self.servers).get(addr).map_or(0, |s| {
            lock(&s.consumers).iter().filter(|c| !c.closed.load(Ordering::Relaxed)).count()
        })
    }

    fn server(&self, addr: &str, path: &str, queue: usize) -> Result<Arc<Server>, String> {
        let mut servers = lock(&self.servers);
        if let Some(s) = servers.get(addr) {
            if s.path != path {
                return Err(format!(
                    "{addr} already broadcasts on {}; one listener serves one path",
                    s.path
                ));
            }
            return Ok(Arc::clone(s));
        }
        let listener = TcpListener::bind(addr).map_err(|e| format!("cannot listen on {addr}: {e}"))?;
        // Polled, so a stopped hub can end the loop instead of blocking in accept.
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("cannot listen on {addr}: {e}"))?;
        let server = Arc::new(Server {
            path: path.to_string(),
            queue,
            consumers: Mutex::new(Vec::new()),
            stop: AtomicBool::new(false),
        });
        let accepting = Arc::clone(&server);
        std::thread::Builder::new()
            .name(format!("wsserver {addr}"))
            .spawn(move || accept_loop(listener, accepting))
            .map_err(|e| format!("cannot start the listener on {addr}: {e}"))?;
        servers.insert(addr.to_string(), Arc::clone(&server));
        Ok(server)
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        for s in lock(&self.servers).values() {
            s.stop.store(true, Ordering::Relaxed);
            for c in lock(&s.consumers).iter() {
                c.closed.store(true, Ordering::Relaxed);
                c.ready.notify_all();
            }
        }
    }
}

fn accept_loop(listener: TcpListener, server: Arc<Server>) {
    while !server.stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                let s = Arc::clone(&server);
                std::thread::spawn(move || serve(stream, s));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => std::thread::sleep(Duration::from_millis(25)),
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

/// Complete one consumer's handshake, then send it what is queued for it until
/// it goes away or the hub stops.
fn serve(stream: TcpStream, server: Arc<Server>) {
    if stream.set_nonblocking(false).is_err() {
        return;
    }
    let path = server.path.clone();
    let on_path = |req: &tungstenite::handshake::server::Request, resp: tungstenite::handshake::server::Response| {
        if req.uri().path() == path {
            Ok(resp)
        } else {
            let mut refused = tungstenite::handshake::server::ErrorResponse::new(Some(format!(
                "nothing is broadcast on {}; try {path}",
                req.uri().path()
            )));
            *refused.status_mut() = tungstenite::http::StatusCode::NOT_FOUND;
            Err(refused)
        }
    };
    let Ok(mut ws) = tungstenite::accept_hdr(stream, on_path) else { return };
    // A short read timeout lets an idle consumer's close be noticed between
    // messages, so the count a pass reports is of consumers still there.
    let _ = ws.get_mut().set_read_timeout(Some(Duration::from_millis(20)));
    let consumer = Arc::new(Consumer {
        outbox: Mutex::new(Outbox::new(server.queue)),
        ready: Condvar::new(),
        closed: AtomicBool::new(false),
    });
    lock(&server.consumers).push(Arc::clone(&consumer));
    loop {
        let next = {
            let mut outbox = lock(&consumer.outbox);
            loop {
                if consumer.closed.load(Ordering::Relaxed) || server.stop.load(Ordering::Relaxed) {
                    drop(outbox);
                    let _ = ws.close(None);
                    return;
                }
                if let Some(m) = outbox.pop() {
                    break m;
                }
                let (guard, waited) = consumer
                    .ready
                    .wait_timeout(outbox, Duration::from_millis(250))
                    .unwrap_or_else(|p| p.into_inner());
                outbox = guard;
                if waited.timed_out() && gone(&mut ws) {
                    consumer.closed.store(true, Ordering::Relaxed);
                }
            }
        };
        if ws.send(Message::Text(next.into())).is_err() {
            consumer.closed.store(true, Ordering::Relaxed);
            return;
        }
    }
}

/// Whether the consumer has closed or dropped its connection. Anything it sent
/// is read and ignored: this is a broadcast, not a conversation.
fn gone(ws: &mut tungstenite::WebSocket<TcpStream>) -> bool {
    match ws.read() {
        Ok(Message::Close(_)) => true,
        Ok(_) => false,
        Err(tungstenite::Error::Io(e))
            if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) =>
        {
            false
        }
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use tokio_tungstenite::tungstenite::{self, Message};

    fn free_addr() -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    }

    fn wait_for(what: &str, mut ok: impl FnMut() -> bool) {
        let until = Instant::now() + Duration::from_secs(5);
        while !ok() {
            assert!(Instant::now() < until, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn text(m: Message) -> String {
        match m {
            Message::Text(t) => t.to_string(),
            other => panic!("expected a text frame, got {other:?}"),
        }
    }

    #[test]
    fn a_full_queue_loses_its_oldest_rows_and_counts_them() {
        // A consumer that cannot keep up must not stop the pass (that would stop
        // the follower's position), so its queue is bounded and drops the oldest.
        let mut q = Outbox::new(3);
        let rows: Vec<String> = (1..=5).map(|i| i.to_string()).collect();
        assert_eq!(q.push_all(&rows), 2, "two rows had to go to make room");
        let left: Vec<String> = std::iter::from_fn(|| q.pop()).collect();
        assert_eq!(left, ["3", "4", "5"], "the newest rows are the ones kept");
    }

    #[test]
    fn a_pass_with_nobody_attached_succeeds() {
        // Zero consumers is a successful pass: a fan-out has nobody to wait for.
        let hub = Hub::new();
        let addr = free_addr();
        let d = hub.publish(&addr, "/rows", 10, &["x".into()]).expect("publishes");
        assert_eq!(d, Delivery { consumers: 0, dropped: 0 });
    }

    #[test]
    fn a_consumer_gets_the_rows_of_every_pass_on_one_listener() {
        // The listener outlives a pass: a consumer attached once receives each
        // later pass's rows without reconnecting.
        let hub = Hub::new();
        let addr = free_addr();
        hub.publish(&addr, "/rows", 10, &[]).expect("starts listening");
        let (mut ws, _) = tungstenite::connect(format!("ws://{addr}/rows")).expect("a consumer connects");
        wait_for("the consumer to be attached", || hub.consumers(&addr) == 1);
        let first = hub.publish(&addr, "/rows", 10, &["a".into(), "b".into()]).expect("pass 1");
        assert_eq!(first, Delivery { consumers: 1, dropped: 0 });
        assert_eq!(text(ws.read().unwrap()), "a");
        assert_eq!(text(ws.read().unwrap()), "b");
        hub.publish(&addr, "/rows", 10, &["c".into()]).expect("pass 2");
        assert_eq!(text(ws.read().unwrap()), "c", "the second pass reached the same consumer");
    }

    #[test]
    fn a_consumer_that_leaves_stops_being_counted() {
        // The count a pass reports is of consumers still there, including one
        // that left while nothing was being sent.
        let hub = Hub::new();
        let addr = free_addr();
        hub.publish(&addr, "/rows", 10, &[]).expect("starts listening");
        let (ws, _) = tungstenite::connect(format!("ws://{addr}/rows")).expect("a consumer connects");
        wait_for("the consumer to be attached", || hub.consumers(&addr) == 1);
        drop(ws);
        wait_for("the consumer to be noticed gone", || hub.consumers(&addr) == 0);
        let d = hub.publish(&addr, "/rows", 10, &["x".into()]).expect("publishes");
        assert_eq!(d.consumers, 0, "a pass counted a consumer that had gone");
    }

    #[test]
    fn a_consumer_on_another_path_is_turned_away() {
        let hub = Hub::new();
        let addr = free_addr();
        hub.publish(&addr, "/rows", 10, &[]).expect("starts listening");
        assert!(tungstenite::connect(format!("ws://{addr}/other")).is_err(), "connected on a path nothing broadcasts on");
    }

    #[test]
    fn one_address_serves_one_path() {
        let hub = Hub::new();
        let addr = free_addr();
        hub.publish(&addr, "/rows", 10, &[]).expect("starts listening");
        let err = hub.publish(&addr, "/other", 10, &[]).expect_err("a second path on one listener");
        assert!(err.contains("/rows"), "{err}");
    }
}
