//! Duckle execution core.
//!
//! Defines the engine-agnostic logical plan and the trait every engine
//! implements. The workflow engine produces logical plans here; the
//! DuckDB, SlothDB, and native engines translate them into concrete
//! execution.
//!
//! See `ARCHITECTURE.md` for the design.
