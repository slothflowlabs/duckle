//! Duckle DuckDB engine adapter.
//!
//! Translates Duckle logical plans into DuckDB SQL/relational API calls and
//! returns results as Arrow record batches via the DuckDB Arrow extension.
//!
//! The DuckDB engine binary is the default execution backend. If it is not
//! present on disk at startup the adapter fetches a pinned release.
//!
//! See `ARCHITECTURE.md` for the design.
