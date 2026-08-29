//! #258: a hard ceiling on what one inference stage may spend.
//!
//! Rate limiting controls how fast money leaves; it does not control how much.
//! A prompt template that accidentally embeds a whole document, over a source
//! that grew tenfold overnight, is a bill nobody approved.
//!
//! Two things make this a real ceiling rather than a report:
//!
//! - **Checked before the next request, not summed afterwards.** A total
//!   computed at the end of the stage tells you what you already owe.
//! - **Stopping is not a failure.** The rows bought before the limit are
//!   correct and paid for. The run says it is INCOMPLETE, keeps them, keeps the
//!   checkpoint, and does not let anything downstream publish them - which is
//!   the actual damage a partial dataset does.
//!
//! Tokens cannot be known before a request; only after one, from the provider's
//! own `usage`. So the guarantee is precisely: **no request is issued once the
//! recorded totals have reached the limit.** The last request may carry the
//! total past it, by at most one request's worth. Anything stronger would need
//! a local tokenizer per model and would still be an estimate.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::EngineError;

/// A ceiling for one stage, and what it has spent so far.
#[derive(Debug, Default)]
pub struct Budget {
    pub max_requests: Option<u64>,
    pub max_input_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub max_cost_usd: Option<f64>,
    /// USD per million tokens. Only meaningful with `max_cost_usd`, and a cost
    /// ceiling without them is refused rather than silently never firing.
    pub input_usd_per_mtok: f64,
    pub output_usd_per_mtok: f64,
    requests: AtomicU64,
    input_tokens: AtomicU64,
    output_tokens: AtomicU64,
}

/// What the run reports when a budget stopped it. Machine-readable on purpose:
/// an operator's alerting has to tell "we hit the ceiling" apart from "it
/// broke", and a sentence cannot be matched on.
pub const REASON_REQUESTS: &str = "budget:maxRequests";
pub const REASON_INPUT_TOKENS: &str = "budget:maxInputTokens";
pub const REASON_OUTPUT_TOKENS: &str = "budget:maxOutputTokens";
pub const REASON_COST: &str = "budget:maxEstimatedCostUsd";

impl Budget {
    /// A budget, or `None` when nothing was capped.
    ///
    /// A cost ceiling with no pricing is refused HERE, at plan time, rather
    /// than accepted and never triggered. A limit that cannot fire is worse
    /// than no limit: it is a limit somebody believes in.
    pub fn new(
        max_requests: Option<u64>,
        max_input_tokens: Option<u64>,
        max_output_tokens: Option<u64>,
        max_cost_usd: Option<f64>,
        input_usd_per_mtok: f64,
        output_usd_per_mtok: f64,
    ) -> Result<Option<Self>, EngineError> {
        if max_cost_usd.is_some() && input_usd_per_mtok <= 0.0 && output_usd_per_mtok <= 0.0 {
            return Err(EngineError::Config(
                "ai: a maximum estimated cost was set with no prices, so it could never be \
                 reached and would never stop anything. Fill in the input and output price per \
                 million tokens, or cap requests or tokens instead - those work against a \
                 self-hosted endpoint too, where there is no price to quote."
                    .into(),
            ));
        }
        if max_requests.is_none()
            && max_input_tokens.is_none()
            && max_output_tokens.is_none()
            && max_cost_usd.is_none()
        {
            return Ok(None);
        }
        Ok(Some(Budget {
            max_requests,
            max_input_tokens,
            max_output_tokens,
            max_cost_usd,
            input_usd_per_mtok,
            output_usd_per_mtok,
            ..Default::default()
        }))
    }

    /// USD spent so far, by the prices given.
    pub fn spent_usd(&self) -> f64 {
        let i = self.input_tokens.load(Ordering::Relaxed) as f64;
        let o = self.output_tokens.load(Ordering::Relaxed) as f64;
        i / 1_000_000.0 * self.input_usd_per_mtok + o / 1_000_000.0 * self.output_usd_per_mtok
    }

    /// May another request be issued? `Some(reason)` means stop.
    ///
    /// Called before every request, including the first: a budget of zero has
    /// to mean zero, not one.
    pub fn exhausted(&self) -> Option<&'static str> {
        if self
            .max_requests
            .is_some_and(|m| self.requests.load(Ordering::Relaxed) >= m)
        {
            return Some(REASON_REQUESTS);
        }
        if self
            .max_input_tokens
            .is_some_and(|m| self.input_tokens.load(Ordering::Relaxed) >= m)
        {
            return Some(REASON_INPUT_TOKENS);
        }
        if self
            .max_output_tokens
            .is_some_and(|m| self.output_tokens.load(Ordering::Relaxed) >= m)
        {
            return Some(REASON_OUTPUT_TOKENS);
        }
        if self.max_cost_usd.is_some_and(|m| self.spent_usd() >= m) {
            return Some(REASON_COST);
        }
        None
    }

    /// Claim one request slot before spending it.
    ///
    /// Separate from [`exhausted`] and done with a compare-and-swap because
    /// requests run concurrently: checking and then incrementing would let N
    /// workers all pass the same last slot.
    pub fn claim_request(&self) -> Option<&'static str> {
        // Token and cost first, and BEFORE the counter moves. They can only be
        // known after a reply, so the contract is that no request STARTS once
        // they are reached - and a request that never started must not be
        // counted as one, least of all in the message a person reads to find
        // out what they were charged for.
        if let Some(reason) = self.exhausted().filter(|r| *r != REASON_REQUESTS) {
            return Some(reason);
        }
        if let Some(max) = self.max_requests {
            loop {
                let now = self.requests.load(Ordering::Relaxed);
                if now >= max {
                    return Some(REASON_REQUESTS);
                }
                if self
                    .requests
                    .compare_exchange_weak(now, now + 1, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        } else {
            self.requests.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Record what a reply actually cost, from the provider's own `usage`.
    pub fn record(&self, input_tokens: u64, output_tokens: u64) {
        self.input_tokens.fetch_add(input_tokens, Ordering::Relaxed);
        self.output_tokens.fetch_add(output_tokens, Ordering::Relaxed);
    }

    /// Pull `usage` out of an OpenAI-shaped reply.
    ///
    /// Absent usage counts as zero rather than as an error: a compatible
    /// endpoint may not report it, and refusing to run against one would be a
    /// worse outcome than a request ceiling that still works there.
    pub fn record_usage(&self, response: &serde_json::Value) {
        let u = response.get("usage");
        let get = |k: &str| u.and_then(|u| u.get(k)).and_then(|v| v.as_u64()).unwrap_or(0);
        // Embeddings report prompt_tokens and total_tokens and no completion.
        self.record(get("prompt_tokens"), get("completion_tokens"));
    }

    /// What was spent, for the stage's own message.
    pub fn spent_note(&self) -> String {
        let r = self.requests.load(Ordering::Relaxed);
        let i = self.input_tokens.load(Ordering::Relaxed);
        let o = self.output_tokens.load(Ordering::Relaxed);
        let mut s = format!("{r} request(s), {i} input + {o} output token(s)");
        if self.max_cost_usd.is_some() {
            s.push_str(&format!(", about ${:.4}", self.spent_usd()));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(
        req: Option<u64>,
        inp: Option<u64>,
        out: Option<u64>,
        cost: Option<f64>,
        pi: f64,
        po: f64,
    ) -> Budget {
        Budget::new(req, inp, out, cost, pi, po).unwrap().unwrap()
    }

    #[test]
    fn no_ceiling_anywhere_is_no_budget_at_all() {
        assert!(Budget::new(None, None, None, None, 0.0, 0.0).unwrap().is_none());
    }

    /// A limit that cannot fire is worse than no limit: it is a limit somebody
    /// believes in.
    #[test]
    fn a_cost_ceiling_with_no_prices_is_refused() {
        let e = Budget::new(None, None, None, Some(50.0), 0.0, 0.0).unwrap_err().to_string();
        assert!(e.contains("never"), "must say why; got: {e}");
        // One price is enough - an endpoint may bill only for output.
        assert!(Budget::new(None, None, None, Some(50.0), 0.0, 1.5).unwrap().is_some());
    }

    #[test]
    fn a_request_ceiling_stops_at_exactly_that_many() {
        let bud = b(Some(3), None, None, None, 0.0, 0.0);
        assert!(bud.claim_request().is_none());
        assert!(bud.claim_request().is_none());
        assert!(bud.claim_request().is_none());
        assert_eq!(bud.claim_request(), Some(REASON_REQUESTS), "the fourth must not go");
    }

    /// A budget of zero has to mean zero, not one.
    #[test]
    fn a_zero_ceiling_issues_nothing() {
        let bud = b(Some(0), None, None, None, 0.0, 0.0);
        assert_eq!(bud.claim_request(), Some(REASON_REQUESTS));
    }

    /// Tokens are only knowable after a reply, so the contract is that no
    /// request STARTS once the total is reached.
    #[test]
    fn a_token_ceiling_stops_the_next_request_not_the_current_one() {
        let bud = b(None, Some(100), None, None, 0.0, 0.0);
        assert!(bud.claim_request().is_none());
        bud.record(90, 10);
        assert!(bud.claim_request().is_none(), "90 is under 100");
        bud.record(20, 5);
        assert_eq!(bud.claim_request(), Some(REASON_INPUT_TOKENS), "110 is over");
    }

    /// A stop is not a purchase. The counter was incremented BEFORE the token
    /// and cost ceilings were consulted, so a run stopped by tokens reported one
    /// more request than it had made - in the very message a person reads to
    /// find out what they were charged for.
    #[test]
    fn a_stop_does_not_consume_a_request_slot() {
        let bud = b(Some(10), Some(5), None, None, 0.0, 0.0);
        bud.record(6, 0);
        assert_eq!(bud.claim_request(), Some(REASON_INPUT_TOKENS));
        assert!(
            bud.spent_note().starts_with("0 request(s)"),
            "no request went out, so none may be counted: {}",
            bud.spent_note()
        );
    }

    #[test]
    fn output_tokens_have_their_own_ceiling() {
        let bud = b(None, None, Some(50), None, 0.0, 0.0);
        bud.record(10_000, 49);
        assert!(bud.claim_request().is_none());
        bud.record(0, 1);
        assert_eq!(bud.claim_request(), Some(REASON_OUTPUT_TOKENS));
    }

    /// $1/Mtok in, $3/Mtok out. 1M in + 1M out = $4, so a $3 ceiling stops.
    #[test]
    fn cost_is_the_two_prices_applied_to_the_two_counts() {
        let bud = b(None, None, None, Some(3.0), 1.0, 3.0);
        bud.record(1_000_000, 0);
        assert!((bud.spent_usd() - 1.0).abs() < 1e-9);
        assert!(bud.claim_request().is_none(), "$1 is under $3");
        bud.record(0, 1_000_000);
        assert!((bud.spent_usd() - 4.0).abs() < 1e-9);
        assert_eq!(bud.claim_request(), Some(REASON_COST));
    }

    /// A compatible endpoint that reports no usage must not be refused; the
    /// request ceiling still works there.
    #[test]
    fn a_reply_with_no_usage_counts_as_zero_rather_than_failing() {
        let bud = b(None, Some(10), None, None, 0.0, 0.0);
        bud.record_usage(&serde_json::json!({"choices": []}));
        assert!(bud.claim_request().is_none());
        bud.record_usage(&serde_json::json!({"usage": {"prompt_tokens": 11}}));
        assert_eq!(bud.claim_request(), Some(REASON_INPUT_TOKENS));
    }

    /// Requests run concurrently, so check-then-increment would let several
    /// workers through the same last slot.
    #[test]
    fn concurrent_workers_cannot_overspend_the_request_ceiling() {
        // Repeated, and with every thread released at the same instant onto a
        // ceiling of one. One slot and N contenders is the tightest form of the
        // race, and running it many times makes a check-then-increment lose
        // reliably rather than usually.
        for round in 0..40 {
            let bud = std::sync::Arc::new(b(Some(1), None, None, None, 0.0, 0.0));
            let go = std::sync::Arc::new(std::sync::Barrier::new(4));
            let granted = std::sync::Arc::new(AtomicU64::new(0));
            let mut handles = Vec::new();
            for _ in 0..4 {
                let bud = bud.clone();
                let granted = granted.clone();
                let go = go.clone();
                handles.push(std::thread::spawn(move || {
                    go.wait();
                    if bud.claim_request().is_none() {
                        granted.fetch_add(1, Ordering::Relaxed);
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(
                granted.load(Ordering::Relaxed),
                1,
                "round {round}: one slot must be granted once, never twice"
            );
        }
    }
}
