# Dependency advisories

Every known advisory affecting a crate in Duckle's dependency tree, what it can
and cannot reach in this codebase, and what would let the entry be removed.

Scanning runs on every push and pull request (`supply-chain` job in
`.github/workflows/ci.yml`) and on a weekly schedule, so an advisory published
against an existing dependency is found without anyone re-running anything. A
new advisory fails the build. The entries below are the only ones that do not,
and each is listed in `.cargo/audit.toml` with a pointer here.

**Review date: 2026-12-07.** The list is re-checked then whether or not anything
has changed upstream, because "no fix yet" ages badly without a date on it.

---

## Why anything is accepted at all

An advisory with a fix available is not accepted; it is fixed. Everything below
has no upgrade to take - in each case the crate that pulls it in is already at
its newest published version. The alternative to an accounted-for exception list
is a scanner that is permanently red, which is a scanner nobody reads.

Two questions decide each entry:

1. **Is there a fix?** If yes, take it. None of the below has one.
2. **Is the vulnerable code path reachable from Duckle?** This is not a way of
   talking the risk down - where the answer is "yes, narrowly", it says so.

---

## RUSTSEC-2023-0071 - `rsa` 0.9.10, Marvin timing sidechannel

**Severity as published:** medium (5.9). **Fix available:** none.

A timing sidechannel in RSA private-key operations that can, given enough
chosen-ciphertext decryptions and timing measurements, recover the key.

**Reachability: not reachable.** The `rsa` crate is used at exactly one site,
the Snowflake key-pair authentication path in `crates/duckdb-engine/src/lib.rs`.
What it does there:

1. Parse a private key the operator supplied, from local PEM.
2. Derive the public key and DER-encode it.
3. SHA-256 that, to build the `iss` claim Snowflake requires.

There is no RSA decryption in the tree, and the JWT is signed by `jsonwebtoken`,
not by this crate. Marvin requires an attacker who can submit ciphertexts for
decryption and measure how long each takes. Nothing here decrypts anything.

**Removed when:** the `rsa` crate publishes a release with constant-time
private-key operations.

---

## RUSTSEC-2026-0194, -0195 - `quick-xml` 0.40.1

**Severity as published:** denial of service. **Fix available:** 0.41.0+, and
Duckle's direct dependency is already past it.

Quadratic run time checking a start tag for duplicate attributes, and unbounded
namespace-declaration allocation in `NsReader`.

**Reachability: not from pipeline input.** Duckle's own XML reading - the
streaming XML source, the XML sink, SQL Server XML columns - is on `quick-xml`
0.42, which contains both fixes. The vulnerable copy comes only through the
optional `duckle-lance` sidecar: `quick-xml` 0.40.1, via `lance-namespace-impls`
under `lancedb` 0.40, where it parses the responses of Azure's own credential
endpoint (`credentials/azure.rs`, the Azure credential vendor) - not a document
a pipeline was pointed at. `duckle-lance` is a separate sidecar binary and is not
part of the engine, the runner or the desktop app.

`lancedb` 0.38 also carried 0.39.4, via `object_store` under `datafusion`; the
move to 0.40 removed that copy.

**Removed when:** `lancedb` takes a `lance-namespace-impls` release built on
`quick-xml` 0.41+.

---

## RUSTSEC-2026-0387 - `crypto-bigint` 0.5.5, timing variability

**Is there a fix?** On 0.7.6, which `crypto-bigint`'s other copy here is already on.
This copy is pinned by `jsonwebtoken` 11.1.0, the newest release, through `p256` and
`p384` 0.13.

**Is it reachable?** No. The flaw leaks through operations on secret integers, and
`p256` and `p384` serve the ES256 and ES384 algorithms. Duckle uses RS256 only:
Snowflake key-pair auth signs with it, and OIDC verification refuses any other
algorithm (`crates/duckle-runner/src/oidc.rs`). RSA goes through the `rsa` crate.

**Remove when:** `jsonwebtoken` moves to `p256` and `p384` releases on `crypto-bigint`
0.7.6 or later.

---

## RUSTSEC-2026-0391 - `zstd` 0.13.3, abandoned stream on a reused context

**Is there a fix?** On 0.14.3, which Duckle's own use of `zstd` is on, and `zip` since
9.0.5. This copy is pinned by `parquet`, `arrow-ipc` and `rskafka`, and by
`lance-encoding` and `vortex-zstd` in the optional `duckle-lance` sidecar.

**Is it reachable?** No. The flaw is in the `with_context` constructors of the stream
encoders and decoders, which reuse a context a previous stream may have left dirty. No
dependency calls one. `parquet` and `arrow-ipc` reuse a context through `zstd::bulk`,
which the advisory does not cover; `rskafka` builds a fresh `Decoder::new` for each
stream.

**Remove when:** the crates above reach `zstd` 0.14.3 or later.

---

## RUSTSEC-2026-0403 - `zstd-safe` 7.2.1, stale memory after a failed legacy frame

**Is there a fix?** On 8.2.0. This copy is the one `zstd` 0.13.3 requires, so it goes
when that does.

**Is it reachable?** No. The memory reaches a caller only through
`zstd::stream::write::Decoder`, and the advisory itself clears `stream::read::Decoder`,
`decode_all` and the bulk API. No dependency uses the write decoder: `rskafka` and
`parquet`'s command-line tools read through `Decoder::new`, and the rest use the bulk API.

**Remove when:** `zstd` 0.13.3 leaves the tree, as above.

---

## RUSTSEC-2026-0435 - `thrift` 0.17.0, unbounded allocation in the binary protocol

**Is there a fix?** On 0.23.0. This copy is pinned by `parquet` 58.4.0, whose 58 line
requires `thrift` 0.17, in the optional `duckle-lance` sidecar: `lancedb` 0.40, `lance` 13
and `vortex` 0.75 hold the sidecar on arrow 58. The engine's own `parquet` is 59, which no
longer uses the `thrift` crate.

**Is it reachable?** No. `read_bytes` allocates whatever length prefix it is given before
it reads the bytes, so a crafted Parquet footer can exhaust memory. The sidecar reads
Parquet in two places, `write` and `write-vortex` (`crates/duckle-lance/src/main.rs`), and
both are handed a file the engine has just written itself with DuckDB's
`COPY ... (FORMAT parquet)`, beside the run's database (`run_lance_sink` and
`run_vortex_sink` in `crates/duckdb-engine/src/connectors.rs`). Every length in a footer the
sidecar decodes is DuckDB's; a Parquet file from outside never reaches it.

**Remove when:** `lancedb`, `lance` and `vortex` move to arrow 59, so the sidecar can take
`parquet` 59, or `parquet` 58 takes `thrift` 0.23 or later.

---

## Informational warnings

`cargo audit` also reports unmaintained, unsound and yanked crates. These do not
fail the build, because "the author stopped publishing" is not by itself a
vulnerability and treating it as one trains people to skip the output. They are
reviewed on the same date as the list above. The current set is dominated by the
`unic-*` family (transitive, via the Unicode identifier tables) and `glib` 0.18
(transitive, via the Linux desktop toolchain).

---

## Related

* [Supply chain assurance](../explanation/supply-chain-assurance.md) - SBOM,
  provenance, and how a release is built
* [Vulnerability reporting and patching](../how-to/vulnerability-reporting-patching.md)
* [Trust boundary and threat model](../explanation/trust-boundary-and-threat-model.md)
