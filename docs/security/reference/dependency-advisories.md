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

## RUSTSEC-2026-0049, -0098, -0099, -0104 - `rustls-webpki` 0.102.8

**Fix available:** for the crate, yes (0.103.10+). For Duckle, no - see below.

Four certificate-path-validation defects: name constraints incorrectly accepted
for URI names, name constraints accepted for certificates asserting a CN, CRLs
not treated as authoritative for their distribution point, and a reachable panic
parsing a certificate revocation list.

**Reachability: narrow, and it is not Duckle's own TLS.** Duckle's own TLS - the
console, HTTPS sources and sinks, the updater - resolves to `rustls-webpki`
0.103.15, which is unaffected. The vulnerable copy arrives through one
connector:

| Path | Reached when |
| :--- | :--- |
| `imap` 3.0.0-alpha.15 → `rustls-connector` 0.19 → `rustls` 0.22 → `rustls-webpki` 0.102.8 | Connecting to an IMAP server over TLS |

SQL Server was the second path, through `tiberius` 0.12.3 and `rustls` 0.21
(`rustls-webpki` 0.101.7). `tiberius` 0.13 moved to `rustls` 0.23, which removed
that copy.

So the exposure is: a deployment that uses the IMAP connector, and whose
certificate chain runs through a CA that issues name constraints, could have a
chain accepted that a correct implementation would reject. That needs an
attacker positioned to present such a chain.

**No upgrade exists to take.** `imap` 3.0.0-alpha.15 is the newest published
version and pins the older `rustls` line. Forcing a newer `rustls` is not
possible across that semver boundary.

**Removed when:** `imap` publishes a release built on `rustls` 0.23 or later.
It is watched at the review date above.

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
