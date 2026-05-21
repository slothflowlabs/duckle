//! Duckle native transform engine.
//!
//! Vectorized, Arrow-native operators: filter, project, join, aggregate,
//! union, sort, deduplicate, window, pivot. Operators consume and produce
//! `RecordBatch` and compose into streaming pipelines.
//!
//! See `ARCHITECTURE.md` for the design.
