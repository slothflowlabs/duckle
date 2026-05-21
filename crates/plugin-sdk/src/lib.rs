//! Duckle plugin SDK.
//!
//! Stable public traits and types third parties build against to ship
//! connectors, transforms, and execution engines as independent crates.
//!
//! Plugins compile as `cdylib` crates and are loaded dynamically at
//! startup. A versioned ABI guards against incompatible loads.
//!
//! See `ARCHITECTURE.md` for the design.
