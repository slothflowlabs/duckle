//! Duckle streaming and incremental execution.
//!
//! Bounded, backpressure-aware operator pipelines for unbounded sources
//! (Kafka, Pulsar, change-data-capture). Operators share the same Arrow
//! data model as batch execution.
//!
//! See `ARCHITECTURE.md` for the design.
