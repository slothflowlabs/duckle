//! A small S3 client, for the things DuckDB's httpfs cannot do for us.
//!
//! DuckDB already reads and writes `s3://` inside SQL, and where it can, it
//! should: it is faster and it is one less implementation. This exists for the
//! operations that are not SQL at all -
//!
//! - asking what an object's ETag and size are WITHOUT downloading it (#272),
//! - listing a prefix as metadata rather than as data,
//! - copying bytes from somewhere else to an object-storage key, hashing them
//!   on the way past, without holding the file in memory (#247).
//!
//! It speaks plain S3 REST with SigV4, so it works against AWS and against the
//! S3-compatible stores people actually use - MinIO, Backblaze B2, Cloudflare
//! R2, Google's interoperability endpoint - which is the point. Those need
//! `endpoint` and usually `urlStyle: "path"`, and the property names here are
//! deliberately the same ones the DuckDB secret already takes, so ONE saved
//! connection drives both paths and cannot describe two different buckets.

use std::io::Read;

use serde_json::Value as JsonValue;

use crate::EngineError;

/// Where a bucket is and how to authenticate to it.
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub session_token: Option<String>,
    /// Host (optionally `host:port`) of an S3-compatible endpoint. None is AWS.
    pub endpoint: Option<String>,
    /// "path" (`https://host/bucket/key`), "vhost" (`https://bucket.host/key`),
    /// or None for "nothing was said". The three are genuinely different: an
    /// unset style has to fall back on what the endpoint implies, and a store
    /// that needs one of them will not work with the other.
    pub url_style: Option<String>,
    pub use_ssl: bool,
}

/// One object, as the metadata operations see it. No bytes.
#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub size: Option<i64>,
    /// Quotes stripped. On AWS this is an MD5 only for a single-part upload;
    /// for a multipart object it is a digest-of-digests. Comparable to itself
    /// across runs, which is all a change detector needs, and NOT the object's
    /// content hash - see the note in `crate::connectors` on fingerprints.
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

/// Split `s3://bucket/key/parts` into its bucket and key.
pub fn parse_s3_uri(uri: &str) -> Result<(String, String), EngineError> {
    let rest = uri
        .strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .ok_or_else(|| EngineError::Config(format!("not an s3 uri: {uri}")))?;
    let mut it = rest.splitn(2, '/');
    let bucket = it.next().unwrap_or_default();
    if bucket.is_empty() {
        return Err(EngineError::Config(format!(
            "{uri} names no bucket - expected s3://bucket/key"
        )));
    }
    Ok((
        bucket.to_string(),
        it.next().unwrap_or_default().to_string(),
    ))
}

/// Percent-encode for a canonical request.
///
/// S3 signs the ENCODED path, and gets stricter about it than most APIs: every
/// byte outside the unreserved set is escaped, uppercase hex, and `/` is kept
/// only in a path. A key with a space or a `+` in it signs wrong otherwise and
/// comes back 403, which reads as a credentials problem and is not one.
fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex(&Sha256::digest(data))
}

/// The hash S3 expects when the body is not being signed.
///
/// Legal over TLS, and the only honest option for a streaming upload: signing
/// the payload means hashing it before sending it, which means holding it, and
/// not holding it is the entire point of the copy path.
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// SHA-256 of the empty string, the payload hash for GET / HEAD / LIST.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

impl S3Config {
    /// Build from connection properties.
    ///
    /// The names match the DuckDB S3 secret exactly - `accessKey`, `secretKey`,
    /// `region`, `sessionToken`, `endpoint`, `urlStyle`, `useSsl` - so a saved
    /// connection configured for a SQL read also drives a metadata probe. Two
    /// sets of names would let one describe a bucket the other cannot reach.
    pub fn from_props(props: &JsonValue) -> Option<Self> {
        let get = |k: &str| -> Option<String> {
            props
                .get(k)
                .and_then(JsonValue::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        };
        let access_key = get("accessKey")?;
        let secret_key = get("secretKey")?;
        let raw_endpoint = get("endpoint");
        let endpoint = raw_endpoint.as_ref().map(|e| {
            // Written with a scheme as often as not, and the scheme belongs to
            // useSsl rather than to the host.
            e.trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_end_matches('/')
                .to_string()
        });
        // An endpoint typed as `http://localhost:9000` is a plain-HTTP store,
        // and dialling it over TLS dies in the handshake. The scheme was being
        // stripped and thrown away, so a local MinIO could not be reached
        // without also finding the separate TLS switch. An explicit useSsl
        // still wins - the scheme only decides what "unset" means.
        let implied_ssl = raw_endpoint.as_deref().and_then(|e| {
            if e.starts_with("http://") {
                Some(false)
            } else if e.starts_with("https://") {
                Some(true)
            } else {
                None
            }
        });
        let use_ssl = props
            .get("useSsl")
            .and_then(|v| {
                v.as_bool()
                    .or_else(|| v.as_str().map(|s| !s.eq_ignore_ascii_case("false")))
            })
            .or(implied_ssl)
            .unwrap_or(true);
        Some(S3Config {
            access_key,
            secret_key,
            region: get("region").unwrap_or_else(|| "us-east-1".into()),
            session_token: get("sessionToken"),
            url_style: get("urlStyle").map(|u| u.to_ascii_lowercase()),
            endpoint,
            use_ssl,
        })
    }

    fn scheme(&self) -> &'static str {
        if self.use_ssl {
            "https"
        } else {
            "http"
        }
    }

    /// The host exactly as the HTTP client will send it.
    ///
    /// SigV4 signs the Host header, and the client derives Host from the parsed
    /// URL rather than from the string handed in: the `url` crate lowercases the
    /// host and drops a port that equals the scheme default. Signing the raw
    /// endpoint therefore covers `minio.internal:443` while the wire carries
    /// `minio.internal`, and every request comes back 403 SignatureDoesNotMatch
    /// - which reads as a bad access key and is not one. Normalising here means
    /// the signed value and the sent value are built from the same string.
    fn normalize_host(&self, host: &str) -> String {
        let lower = host.to_ascii_lowercase();
        let default_port = if self.use_ssl { ":443" } else { ":80" };
        match lower.strip_suffix(default_port) {
            Some(bare) if !bare.is_empty() => bare.to_string(),
            _ => lower,
        }
    }

    /// The host to send to and the path to sign, for one bucket and key.
    ///
    /// The order of these rules is the whole content of the function; each one
    /// exists because the rule below it gets that case wrong.
    fn address(&self, bucket: &str, key: &str) -> (String, String) {
        let base = self
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("s3.{}.amazonaws.com", self.region));
        let encoded = uri_encode(key, true);
        let vhost = |h: &str| (self.normalize_host(h), format!("/{encoded}"));
        let path = |h: &str| {
            (
                self.normalize_host(h),
                format!("/{}/{}", uri_encode(bucket, false), encoded),
            )
        };

        // 1. The endpoint IS the bucket's virtual host already - what you get by
        //    pasting a URL out of the AWS or R2 console. Prepending the bucket
        //    again gives `mybucket.mybucket.s3...`, which is NXDOMAIN.
        if self.endpoint.is_some() && base.starts_with(&format!("{bucket}.")) {
            return vhost(&base);
        }
        // 2. An explicit choice wins over everything below.
        if self.url_style.as_deref() == Some("path") {
            return path(&base);
        }
        // 3. A dotted bucket cannot go in a hostname: the wildcard certificate
        //    covers one label, so `my.data.lake.s3...` fails TLS before the
        //    request is made. AWS recommends path style for exactly these, and
        //    dotted names are common on buckets made before 2020. This has to
        //    sit ABOVE the explicit-vhost rule: vhost is not available here at
        //    all, so honouring it would only produce a handshake failure.
        if bucket.contains('.') {
            return path(&base);
        }
        if self.url_style.as_deref() == Some("vhost") {
            return vhost(&format!("{bucket}.{base}"));
        }
        // 4. Unset, with a custom endpoint: path style. MinIO and B2 need it,
        //    and a store that wants vhost can still ask for it above.
        if self.endpoint.is_some() {
            return path(&base);
        }
        // 5. Plain AWS with nothing said: virtual host, which is the default
        //    everywhere else too.
        vhost(&format!("{bucket}.{base}"))
    }

    /// Sign a request and return every header to set on it.
    fn sign(
        &self,
        method: &str,
        host: &str,
        canonical_uri: &str,
        canonical_query: &str,
        payload_hash: &str,
        extra: &[(String, String)],
    ) -> Vec<(String, String)> {
        self.sign_at(
            method,
            host,
            canonical_uri,
            canonical_query,
            payload_hash,
            extra,
            chrono::Utc::now(),
        )
    }

    /// The signing itself, with the timestamp passed in.
    ///
    /// Split out only so it can be checked against AWS's own published test
    /// vectors, which fix the date. A signer that can only be tested against
    /// itself is a signer whose 403s are unexplainable.
    #[allow(clippy::too_many_arguments)]
    fn sign_at(
        &self,
        method: &str,
        host: &str,
        canonical_uri: &str,
        canonical_query: &str,
        payload_hash: &str,
        extra: &[(String, String)],
        now: chrono::DateTime<chrono::Utc>,
    ) -> Vec<(String, String)> {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mac = |key: &[u8], data: &[u8]| -> Vec<u8> {
            let mut m = HmacSha256::new_from_slice(key).expect("hmac takes any key length");
            m.update(data);
            m.finalize().into_bytes().to_vec()
        };

        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let short_date = now.format("%Y%m%d").to_string();

        let mut headers: Vec<(String, String)> = vec![
            ("host".into(), host.to_string()),
            ("x-amz-content-sha256".into(), payload_hash.to_string()),
            ("x-amz-date".into(), amz_date.clone()),
        ];
        if let Some(t) = &self.session_token {
            headers.push(("x-amz-security-token".into(), t.clone()));
        }
        for (k, v) in extra {
            headers.push((k.to_ascii_lowercase(), v.clone()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_header_block: String = headers
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
            .collect();
        let signed_headers: String = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            canonical_uri,
            canonical_query,
            canonical_header_block,
            signed_headers,
            payload_hash
        );
        let scope = format!("{}/{}/s3/aws4_request", short_date, self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            scope,
            sha256_hex(canonical_request.as_bytes())
        );
        let k_date = mac(
            format!("AWS4{}", self.secret_key).as_bytes(),
            short_date.as_bytes(),
        );
        let k_region = mac(&k_date, self.region.as_bytes());
        let k_service = mac(&k_region, b"s3");
        let k_signing = mac(&k_service, b"aws4_request");
        let signature = hex(&mac(&k_signing, string_to_sign.as_bytes()));

        let mut out: Vec<(String, String)> = headers
            .into_iter()
            // ureq sets Host itself from the URL, and setting it twice is how a
            // signature stops matching the request that carries it.
            .filter(|(k, _)| k != "host")
            .collect();
        out.push((
            "authorization".into(),
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
                self.access_key, scope, signed_headers, signature
            ),
        ));
        out
    }

    fn url(&self, host: &str, canonical_uri: &str, query: &str) -> String {
        if query.is_empty() {
            format!("{}://{}{}", self.scheme(), host, canonical_uri)
        } else {
            format!("{}://{}{}?{}", self.scheme(), host, canonical_uri, query)
        }
    }

    /// One object's metadata, without fetching it.
    pub fn head(&self, bucket: &str, key: &str) -> Result<S3Object, EngineError> {
        let (host, canonical_uri) = self.address(bucket, key);
        let signed = self.sign("HEAD", &host, &canonical_uri, "", EMPTY_SHA256, &[]);
        let mut req = crate::tls::http_agent().head(&self.url(&host, &canonical_uri, ""));
        for (k, v) in &signed {
            req = req.set(k, v);
        }
        let resp = ok_2xx(
            req.call().map_err(|e| s3_error("HEAD", bucket, key, e))?,
            "HEAD",
            bucket,
            key,
        )?;
        Ok(S3Object {
            key: key.to_string(),
            size: resp.header("content-length").and_then(|s| s.parse().ok()),
            etag: resp.header("etag").map(|s| s.trim_matches('"').to_string()),
            last_modified: resp.header("last-modified").map(str::to_string),
        })
    }

    /// Every object under a prefix, following continuation tokens.
    ///
    /// `limit` bounds the work, not just the output: a prefix with a million
    /// objects must not be walked in full to hand back the first hundred.
    pub fn list(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<S3Object>, EngineError> {
        let mut out: Vec<S3Object> = Vec::new();
        let mut token: Option<String> = None;
        loop {
            // Canonical query has to be sorted by key, encoded, and identical
            // to what is sent.
            let mut params: Vec<(String, String)> = vec![
                ("list-type".into(), "2".into()),
                (
                    "max-keys".into(),
                    1000.min(limit.saturating_sub(out.len())).max(1).to_string(),
                ),
            ];
            if !prefix.is_empty() {
                params.push(("prefix".into(), prefix.to_string()));
            }
            if let Some(t) = &token {
                params.push(("continuation-token".into(), t.clone()));
            }
            params.sort_by(|a, b| a.0.cmp(&b.0));
            let query: String = params
                .iter()
                .map(|(k, v)| format!("{}={}", uri_encode(k, false), uri_encode(v, false)))
                .collect::<Vec<_>>()
                .join("&");

            let (host, canonical_uri) = self.address(bucket, "");
            // Listing addresses the bucket, not a key: path style ends at the
            // bucket, virtual-host style at the root.
            let canonical_uri = canonical_uri.trim_end_matches('/').to_string();
            let canonical_uri = if canonical_uri.is_empty() {
                "/".to_string()
            } else {
                canonical_uri
            };
            let signed = self.sign("GET", &host, &canonical_uri, &query, EMPTY_SHA256, &[]);
            let mut req = crate::tls::http_agent().get(&self.url(&host, &canonical_uri, &query));
            for (k, v) in &signed {
                req = req.set(k, v);
            }
            let body = ok_2xx(
                req.call()
                    .map_err(|e| s3_error("LIST", bucket, prefix, e))?,
                "LIST",
                bucket,
                prefix,
            )?
            .into_string()
            .map_err(|e| EngineError::Query(format!("s3 list {bucket}/{prefix}: {e}")))?;

            for chunk in body.split("<Contents>").skip(1) {
                let key = between(chunk, "<Key>", "</Key>").unwrap_or_default();
                if key.is_empty() || key.ends_with('/') {
                    // A zero-byte "folder marker" is not an object anyone wants
                    // to process.
                    continue;
                }
                out.push(S3Object {
                    size: between(chunk, "<Size>", "</Size>").and_then(|s| s.parse().ok()),
                    etag: between(chunk, "<ETag>", "</ETag>")
                        .map(|s| s.replace("&quot;", "").trim_matches('"').to_string()),
                    last_modified: between(chunk, "<LastModified>", "</LastModified>"),
                    key: unescape_xml(&key),
                });
                if out.len() >= limit {
                    return Ok(out);
                }
            }
            let truncated = between(&body, "<IsTruncated>", "</IsTruncated>")
                .map(|s| s == "true")
                .unwrap_or(false);
            token = between(&body, "<NextContinuationToken>", "</NextContinuationToken>");
            if !truncated || token.is_none() {
                return Ok(out);
            }
        }
    }

    /// Open an object for reading. The body is streamed, never buffered.
    pub fn get(&self, bucket: &str, key: &str) -> Result<Box<dyn Read + Send>, EngineError> {
        let (host, canonical_uri) = self.address(bucket, key);
        let signed = self.sign("GET", &host, &canonical_uri, "", EMPTY_SHA256, &[]);
        let mut req = crate::tls::http_agent().get(&self.url(&host, &canonical_uri, ""));
        for (k, v) in &signed {
            req = req.set(k, v);
        }
        let resp = ok_2xx(
            req.call().map_err(|e| s3_error("GET", bucket, key, e))?,
            "GET",
            bucket,
            key,
        )?;
        Ok(Box::new(resp.into_reader()))
    }

    /// Write an object from a reader, in one request.
    ///
    /// Needs the length up front, which is what S3 requires for a plain PUT.
    /// Callers that do not know it use `put_multipart`.
    pub fn put(
        &self,
        bucket: &str,
        key: &str,
        body: impl Read + Send + 'static,
        len: u64,
        content_type: Option<&str>,
    ) -> Result<Option<String>, EngineError> {
        let (host, canonical_uri) = self.address(bucket, key);
        let mut extra: Vec<(String, String)> = vec![("content-length".into(), len.to_string())];
        if let Some(ct) = content_type {
            extra.push(("content-type".into(), ct.to_string()));
        }
        let signed = self.sign("PUT", &host, &canonical_uri, "", UNSIGNED_PAYLOAD, &extra);
        let mut req = crate::tls::http_agent().put(&self.url(&host, &canonical_uri, ""));
        for (k, v) in &signed {
            req = req.set(k, v);
        }
        let resp = ok_2xx(
            req.send(body)
                .map_err(|e| s3_error("PUT", bucket, key, e))?,
            "PUT",
            bucket,
            key,
        )?;
        Ok(resp.header("etag").map(|s| s.trim_matches('"').to_string()))
    }

    /// Write an object in parts, for a body whose length is not known ahead of
    /// time or is too large for one request.
    ///
    /// Memory is bounded by `part_size` regardless of how big the object is,
    /// which is the requirement: a 40GB model file must not become 40GB of RSS.
    /// A failure aborts the upload rather than leaving parts to be billed for
    /// and never completed.
    pub fn put_multipart(
        &self,
        bucket: &str,
        key: &str,
        mut body: impl Read,
        part_size: usize,
        content_type: Option<&str>,
    ) -> Result<Option<String>, EngineError> {
        let upload_id = self.create_multipart(bucket, key, content_type)?;
        let mut etags: Vec<(u32, String)> = Vec::new();
        let mut part_number: u32 = 1;
        let mut buf = vec![0u8; part_size];

        let result = (|| -> Result<(), EngineError> {
            loop {
                let mut filled = 0usize;
                // Fill a whole part before sending: a short read from the
                // network is not the end of the body, and a short PART is
                // rejected by S3 for anything but the last one.
                while filled < part_size {
                    match body.read(&mut buf[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) => {
                            return Err(EngineError::Query(format!("s3 put {bucket}/{key}: {e}")))
                        }
                    }
                }
                if filled == 0 && part_number > 1 {
                    break;
                }
                let tag = self.upload_part(bucket, key, &upload_id, part_number, &buf[..filled])?;
                etags.push((part_number, tag));
                if filled < part_size {
                    break;
                }
                part_number += 1;
            }
            Ok(())
        })();

        // Completing counts as part of the upload, not as something after it.
        // A Complete that fails - an HTTP error, or the 200-with-an-Error-body
        // that S3 is documented to send - used to return straight to the caller
        // with the upload still open, and open parts are billed as storage while
        // showing up in no listing. Aborting an upload the server did complete is
        // harmless: it answers NoSuchUpload.
        let outcome =
            result.and_then(|()| self.complete_multipart(bucket, key, &upload_id, &etags));
        match outcome {
            Ok(etag) => Ok(etag),
            Err(e) => Err(match self.abort_multipart(bucket, key, &upload_id) {
                Ok(()) => e,
                // The parts could not be cleaned up either. Name the upload id,
                // because it is the only handle anyone has to remove them.
                Err(abort) => EngineError::Query(format!(
                    "{e}. The parts already uploaded could NOT be cleaned up ({abort}); they will be billed until upload id {upload_id} is aborted or the bucket's lifecycle rule removes it."
                )),
            }),
        }
    }

    fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> Result<String, EngineError> {
        let (host, canonical_uri) = self.address(bucket, key);
        let extra: Vec<(String, String)> = content_type
            .map(|ct| vec![("content-type".to_string(), ct.to_string())])
            .unwrap_or_default();
        let signed = self.sign(
            "POST",
            &host,
            &canonical_uri,
            "uploads=",
            EMPTY_SHA256,
            &extra,
        );
        let mut req = crate::tls::http_agent().post(&self.url(&host, &canonical_uri, "uploads="));
        for (k, v) in &signed {
            req = req.set(k, v);
        }
        let body = ok_2xx(
            req.call()
                .map_err(|e| s3_error("CreateMultipartUpload", bucket, key, e))?,
            "CreateMultipartUpload",
            bucket,
            key,
        )?
        .into_string()
        .map_err(|e| EngineError::Query(format!("s3 multipart {bucket}/{key}: {e}")))?;
        between(&body, "<UploadId>", "</UploadId>").ok_or_else(|| {
            EngineError::Query(format!(
                "s3 multipart {bucket}/{key}: the store returned no upload id"
            ))
        })
    }

    fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
    ) -> Result<String, EngineError> {
        let (host, canonical_uri) = self.address(bucket, key);
        let query = format!(
            "partNumber={}&uploadId={}",
            part_number,
            uri_encode(upload_id, false)
        );
        let hash = sha256_hex(data);
        let extra = vec![("content-length".to_string(), data.len().to_string())];
        let signed = self.sign("PUT", &host, &canonical_uri, &query, &hash, &extra);
        let mut req = crate::tls::http_agent().put(&self.url(&host, &canonical_uri, &query));
        for (k, v) in &signed {
            req = req.set(k, v);
        }
        let op = format!("UploadPart {part_number}");
        let resp = ok_2xx(
            req.send_bytes(data)
                .map_err(|e| s3_error(&op, bucket, key, e))?,
            &op,
            bucket,
            key,
        )?;
        resp.header("etag")
            .map(|s| s.trim_matches('"').to_string())
            .ok_or_else(|| {
                EngineError::Query(format!(
                    "s3 {bucket}/{key}: part {part_number} came back with no ETag, so the \
                     upload cannot be completed"
                ))
            })
    }

    fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        etags: &[(u32, String)],
    ) -> Result<Option<String>, EngineError> {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, tag) in etags {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>&quot;{}&quot;</ETag></Part>",
                n,
                tag.trim_matches('"')
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");

        let (host, canonical_uri) = self.address(bucket, key);
        let query = format!("uploadId={}", uri_encode(upload_id, false));
        let hash = sha256_hex(xml.as_bytes());
        let extra = vec![
            ("content-length".to_string(), xml.len().to_string()),
            ("content-type".to_string(), "application/xml".to_string()),
        ];
        let signed = self.sign("POST", &host, &canonical_uri, &query, &hash, &extra);
        let mut req = crate::tls::http_agent().post(&self.url(&host, &canonical_uri, &query));
        for (k, v) in &signed {
            req = req.set(k, v);
        }
        let body = ok_2xx(
            req.send_bytes(xml.as_bytes())
                .map_err(|e| s3_error("CompleteMultipartUpload", bucket, key, e))?,
            "CompleteMultipartUpload",
            bucket,
            key,
        )?
        .into_string()
        .unwrap_or_default();
        // S3 can answer 200 and then report a failure in the body. Treating
        // that as success is how a copy reports "done" with no object behind it.
        if body.contains("<Error>") {
            return Err(EngineError::Query(format!(
                "s3 {bucket}/{key}: completing the upload failed: {}",
                between(&body, "<Message>", "</Message>").unwrap_or(body.clone())
            )));
        }
        Ok(between(&body, "<ETag>", "</ETag>").map(|s| s.replace("&quot;", "")))
    }

    fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) -> Result<(), EngineError> {
        let (host, canonical_uri) = self.address(bucket, key);
        let query = format!("uploadId={}", uri_encode(upload_id, false));
        let signed = self.sign("DELETE", &host, &canonical_uri, &query, EMPTY_SHA256, &[]);
        let mut req = crate::tls::http_agent().delete(&self.url(&host, &canonical_uri, &query));
        for (k, v) in &signed {
            req = req.set(k, v);
        }
        ok_2xx(
            req.call()
                .map_err(|e| s3_error("AbortMultipartUpload", bucket, key, e))?,
            "AbortMultipartUpload",
            bucket,
            key,
        )?;
        Ok(())
    }
}

/// The error body S3 returns is far more useful than the status alone: it names
/// the bucket, the key and what was actually wrong. Throwing it away leaves
/// "403 Forbidden", which sends people to look at their keys when the problem
/// is a region or a url style.
fn s3_error(op: &str, bucket: &str, key: &str, e: ureq::Error) -> EngineError {
    match e {
        ureq::Error::Status(code, r) => {
            let body = r.into_string().unwrap_or_default();
            let detail = between(&body, "<Message>", "</Message>")
                .or_else(|| between(&body, "<Code>", "</Code>"))
                .unwrap_or_else(|| body.chars().take(200).collect());
            EngineError::Query(format!("s3 {op} {bucket}/{key}: HTTP {code}: {detail}"))
        }
        other => EngineError::Query(format!("s3 {op} {bucket}/{key}: {other}")),
    }
}

/// Insist on a 2xx before believing a response.
///
/// The HTTP client raises an error for 4xx and 5xx but hands back 1xx and 3xx
/// as ordinary successes, and S3 uses 3xx to say "wrong region" or "the bucket
/// is not here yet". Treating those as success is the worst possible outcome
/// for each verb: a GET streams the error XML as if it were the object, a PUT
/// reports a copy that never happened, and a HEAD returns the error page's own
/// Content-Length as the object's size. Each of those is silent.
fn ok_2xx(
    resp: ureq::Response,
    op: &str,
    bucket: &str,
    key: &str,
) -> Result<ureq::Response, EngineError> {
    let code = resp.status();
    if (200..300).contains(&code) {
        return Ok(resp);
    }
    // Read the region hint BEFORE consuming the body for the message.
    let header_region = resp.header("x-amz-bucket-region").map(str::to_string);
    let body = resp.into_string().unwrap_or_default();
    let hint = header_region
        .or_else(|| between(&body, "<Region>", "</Region>"))
        .map(|r| format!(" - the bucket is in region '{r}'; set that region on the connection"))
        .or_else(|| {
            between(&body, "<Endpoint>", "</Endpoint>")
                .map(|e| format!(" - the bucket answers on '{e}'; set that endpoint"))
        })
        .unwrap_or_default();
    let detail = between(&body, "<Message>", "</Message>")
        .map(|m| format!(": {m}"))
        .unwrap_or_default();
    Err(EngineError::Query(format!(
        "s3 {op} {bucket}/{key}: HTTP {code}{hint}{detail}. A redirect is not followed here on purpose - the signature is bound to the host it was made for, so following one would turn a clear message into a 403."
    )))
}

fn between(hay: &str, open: &str, close: &str) -> Option<String> {
    let start = hay.find(open)? + open.len();
    let end = hay[start..].find(close)? + start;
    Some(hay[start..end].to_string())
}

/// Decode the five XML entities, ampersand LAST.
///
/// Expanding `&amp;` first decodes an escaped entity twice: a key written
/// `report&lt;1&gt;.csv` arrives as `report&amp;lt;1&amp;gt;.csv`, and the
/// ampersand pass turns that into `&lt;` which the next pass turns into `<`.
/// The object is then fetched under a key that does not exist and is silently
/// missing from the listing.
fn unescape_xml(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// A reader that hashes what passes through it.
///
/// This is what makes "copy the bytes and record their sha256" a single pass.
/// Reading the file twice - once to hash, once to upload - doubles the transfer
/// off a remote source, and hashing first means buffering, which is the thing
/// being avoided.
pub struct HashingReader<R> {
    inner: R,
    hasher: sha2::Sha256,
    pub bytes: u64,
}

impl<R: Read> HashingReader<R> {
    pub fn new(inner: R) -> Self {
        use sha2::Digest;
        HashingReader {
            inner,
            hasher: sha2::Sha256::new(),
            bytes: 0,
        }
    }

    pub fn finish(self) -> (String, u64) {
        use sha2::Digest;
        (hex(&self.hasher.finalize()), self.bytes)
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use sha2::Digest;
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.hasher.update(&buf[..n]);
            self.bytes += n as u64;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_s3_uri_splits_into_bucket_and_key() {
        assert_eq!(
            parse_s3_uri("s3://raw/2026/08/doc.pdf").unwrap(),
            ("raw".to_string(), "2026/08/doc.pdf".to_string())
        );
        // A bucket with no key is a prefix listing of the whole bucket.
        assert_eq!(
            parse_s3_uri("s3://raw").unwrap(),
            ("raw".to_string(), String::new())
        );
        assert!(parse_s3_uri("https://example.com/x").is_err());
        assert!(
            parse_s3_uri("s3://").is_err(),
            "no bucket is an error, not an empty bucket"
        );
    }

    /// S3 signs the encoded path. A key with a space in it is the classic case:
    /// signing the raw form returns 403, which reads as bad credentials.
    #[test]
    fn keys_are_encoded_the_way_the_signature_expects() {
        assert_eq!(uri_encode("a b", false), "a%20b");
        assert_eq!(
            uri_encode("a/b", true),
            "a/b",
            "a path keeps its separators"
        );
        assert_eq!(uri_encode("a/b", false), "a%2Fb", "a query value does not");
        assert_eq!(uri_encode("a+b=c", false), "a%2Bb%3Dc");
        assert_eq!(
            uri_encode("~-_.", false),
            "~-_.",
            "the unreserved set is left alone"
        );
        assert_eq!(
            uri_encode("caf\u{e9}", false),
            "caf%C3%A9",
            "encoded per byte, not per char"
        );
    }

    /// An S3-compatible store is addressed differently from AWS, and getting it
    /// wrong is a 403 or a DNS failure rather than a clear message.
    #[test]
    fn path_style_and_virtual_host_style_address_differently() {
        let aws = S3Config {
            region: "eu-west-1".into(),
            ..Default::default()
        };
        let (host, uri) = aws.address("raw", "a/b.pdf");
        assert_eq!(host, "raw.s3.eu-west-1.amazonaws.com");
        assert_eq!(uri, "/a/b.pdf");

        let minio = S3Config {
            endpoint: Some("minio.internal:9000".into()),
            url_style: Some("path".into()),
            ..Default::default()
        };
        let (host, uri) = minio.address("raw", "a/b.pdf");
        assert_eq!(host, "minio.internal:9000");
        assert_eq!(
            uri, "/raw/a/b.pdf",
            "the bucket is in the path, not the host"
        );
    }

    /// A dotted bucket name cannot go in the hostname: the wildcard certificate
    /// does not cover it and TLS fails before the request is made.
    #[test]
    fn an_endpoint_addresses_by_path_even_without_url_style() {
        let b2 = S3Config {
            endpoint: Some("s3.eu-central-003.backblazeb2.com".into()),
            ..Default::default()
        };
        let (host, uri) = b2.address("my.raw.bucket", "doc.pdf");
        assert_eq!(host, "s3.eu-central-003.backblazeb2.com");
        assert_eq!(uri, "/my.raw.bucket/doc.pdf");
    }

    #[test]
    fn a_connection_reads_the_same_property_names_the_duckdb_secret_takes() {
        let c = S3Config::from_props(&json!({
            "accessKey": "AKIA", "secretKey": "shh", "region": "us-west-2",
            "endpoint": "https://minio.internal:9000/", "urlStyle": "path", "useSsl": false
        }))
        .expect("a key and a secret are enough");
        assert_eq!(c.region, "us-west-2");
        assert_eq!(
            c.endpoint.as_deref(),
            Some("minio.internal:9000"),
            "scheme is not the host"
        );
        assert_eq!(c.url_style.as_deref(), Some("path"));
        assert!(!c.use_ssl);
        assert_eq!(c.scheme(), "http");

        // No credentials is not a config with empty credentials: it means this
        // source is not authenticated and should say so rather than send blanks.
        assert!(S3Config::from_props(&json!({ "region": "us-east-1" })).is_none());
        // A default region, because an unset one signs as empty and 403s.
        let d = S3Config::from_props(&json!({ "accessKey": "A", "secretKey": "B" })).unwrap();
        assert_eq!(d.region, "us-east-1");
        assert!(d.use_ssl, "TLS unless told otherwise");
    }

    /// The signature has to cover x-amz-content-sha256, and Host must NOT be
    /// returned as a header to set - ureq derives it from the URL, and setting
    /// it twice makes the signature stop matching the request carrying it.
    #[test]
    fn signing_covers_the_content_hash_and_leaves_host_to_the_client() {
        let c = S3Config {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            region: "us-east-1".into(),
            ..Default::default()
        };
        let headers = c.sign(
            "GET",
            "raw.s3.amazonaws.com",
            "/a.pdf",
            "",
            EMPTY_SHA256,
            &[],
        );
        let names: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"x-amz-content-sha256"));
        assert!(names.contains(&"x-amz-date"));
        assert!(names.contains(&"authorization"));
        assert!(
            !names.contains(&"host"),
            "the client sets Host from the URL"
        );

        let auth = &headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .unwrap()
            .1;
        assert!(auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert!(auth.contains("Credential=AKIAIOSFODNN7EXAMPLE/"));
        assert!(auth.contains("/us-east-1/s3/aws4_request"));

        // A session token is signed, not merely sent - an unsigned one is a 403.
        let temp = S3Config {
            session_token: Some("tok".into()),
            ..c
        };
        let auth = temp
            .sign("GET", "h", "/a", "", EMPTY_SHA256, &[])
            .into_iter()
            .find(|(k, _)| k == "authorization")
            .unwrap()
            .1;
        assert!(auth.contains("x-amz-security-token"), "{auth}");
    }

    /// A copy has to produce the hash of what it actually transferred, in one
    /// pass. Reading twice doubles the transfer; hashing first means buffering.
    #[test]
    fn the_hashing_reader_hashes_exactly_what_passed_through() {
        let mut r = HashingReader::new(&b"hello world"[..]);
        let mut out = Vec::new();
        std::io::copy(&mut r, &mut out).unwrap();
        let (sha, bytes) = r.finish();
        assert_eq!(out, b"hello world");
        assert_eq!(bytes, 11);
        assert_eq!(
            sha, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
            "the known sha256 of 'hello world'"
        );
    }

    /// The list parser reads a real ListObjectsV2 body, including the folder
    /// markers that must not be handed downstream as objects.
    #[test]
    fn a_listing_body_yields_objects_and_skips_folder_markers() {
        let body = r#"<?xml version="1.0"?><ListBucketResult>
          <IsTruncated>false</IsTruncated>
          <Contents><Key>raw/</Key><Size>0</Size></Contents>
          <Contents><Key>raw/a b.pdf</Key><Size>120</Size>
            <ETag>&quot;abc123&quot;</ETag><LastModified>2026-08-01T10:00:00.000Z</LastModified>
          </Contents>
          <Contents><Key>raw/caf&amp;s.pdf</Key><Size>7</Size></Contents>
        </ListBucketResult>"#;
        let objects: Vec<S3Object> = body
            .split("<Contents>")
            .skip(1)
            .filter_map(|chunk| {
                let key = between(chunk, "<Key>", "</Key>").unwrap_or_default();
                if key.is_empty() || key.ends_with('/') {
                    return None;
                }
                Some(S3Object {
                    size: between(chunk, "<Size>", "</Size>").and_then(|s| s.parse().ok()),
                    etag: between(chunk, "<ETag>", "</ETag>")
                        .map(|s| s.replace("&quot;", "").trim_matches('"').to_string()),
                    last_modified: between(chunk, "<LastModified>", "</LastModified>"),
                    key: unescape_xml(&key),
                })
            })
            .collect();
        assert_eq!(objects.len(), 2, "the folder marker is not an object");
        assert_eq!(objects[0].key, "raw/a b.pdf");
        assert_eq!(
            objects[0].etag.as_deref(),
            Some("abc123"),
            "quotes are not part of the tag"
        );
        assert_eq!(objects[0].size, Some(120));
        assert_eq!(
            objects[1].key, "raw/caf&s.pdf",
            "XML entities are decoded in keys"
        );
    }

    /// AWS publishes worked examples for SigV4 against S3, with the date fixed
    /// and the expected signature spelled out. They are the only external check
    /// available here: everything else in this file tests the signer against
    /// itself, which cannot catch a systematic misreading of the spec.
    ///
    /// Source: "Signature Calculations for the Authorization Header: Transferring
    /// Payload in a Single Chunk", AWS S3 API reference.
    fn aws_example_config() -> S3Config {
        S3Config {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            region: "us-east-1".into(),
            session_token: None,
            endpoint: None,
            url_style: None,
            use_ssl: true,
        }
    }

    fn at_20130524() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2013-05-24T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn signature_of(headers: &[(String, String)]) -> String {
        let auth = &headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .unwrap()
            .1;
        auth.rsplit("Signature=").next().unwrap().to_string()
    }

    fn signed_headers_of(headers: &[(String, String)]) -> String {
        let auth = &headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .unwrap()
            .1;
        auth.split("SignedHeaders=")
            .nth(1)
            .unwrap()
            .split(',')
            .next()
            .unwrap()
            .to_string()
    }

    /// AWS worked example 1: GET Object with a Range header.
    #[test]
    fn matches_the_aws_published_get_object_signature() {
        let h = aws_example_config().sign_at(
            "GET",
            "examplebucket.s3.amazonaws.com",
            "/test.txt",
            "",
            EMPTY_SHA256,
            &[("range".into(), "bytes=0-9".into())],
            at_20130524(),
        );
        assert_eq!(
            signed_headers_of(&h),
            "host;range;x-amz-content-sha256;x-amz-date"
        );
        assert_eq!(
            signature_of(&h),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// AWS worked example 2: GET Bucket (List Objects) - a signed query string.
    /// This is the one the listing path depends on, and the canonical query
    /// rules are where a hand-rolled signer usually goes wrong.
    #[test]
    fn matches_the_aws_published_list_objects_signature() {
        let h = aws_example_config().sign_at(
            "GET",
            "examplebucket.s3.amazonaws.com",
            "/",
            "max-keys=2&prefix=J",
            EMPTY_SHA256,
            &[],
            at_20130524(),
        );
        assert_eq!(
            signed_headers_of(&h),
            "host;x-amz-content-sha256;x-amz-date"
        );
        assert_eq!(
            signature_of(&h),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    /// AWS worked example 3: PUT Object, with a `$` in the key and extra signed
    /// headers. Proves the encoded path is signed as sent, and that arbitrary
    /// extra headers land in the right sorted position.
    #[test]
    fn matches_the_aws_published_put_object_signature() {
        let h = aws_example_config().sign_at(
            "PUT",
            "examplebucket.s3.amazonaws.com",
            "/test%24file.text",
            "",
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
            &[
                ("date".into(), "Fri, 24 May 2013 00:00:00 GMT".into()),
                ("x-amz-storage-class".into(), "REDUCED_REDUNDANCY".into()),
            ],
            at_20130524(),
        );
        assert_eq!(
            signed_headers_of(&h),
            "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"
        );
        assert_eq!(
            signature_of(&h),
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    // -----------------------------------------------------------------------
    // Regressions. Every one of these passed a self-consistent test suite and
    // would have failed against a real store, which is the class of bug this
    // file is most exposed to: it is only ever right if it agrees with somebody
    // else's server.
    // -----------------------------------------------------------------------

    /// Pasting a bucket URL out of the AWS or R2 console gives an endpoint that
    /// ALREADY is the bucket's virtual host. Prepending the bucket again gives
    /// `mybucket.mybucket.s3...`, which does not resolve.
    #[test]
    fn an_endpoint_that_is_already_the_bucket_vhost_is_not_doubled() {
        let c = S3Config {
            endpoint: Some("mybucket.s3.us-east-1.amazonaws.com".into()),
            region: "us-east-1".into(),
            use_ssl: true,
            ..Default::default()
        };
        let (host, uri) = c.address("mybucket", "k.csv");
        assert_eq!(
            host, "mybucket.s3.us-east-1.amazonaws.com",
            "the bucket was added twice"
        );
        assert_eq!(uri, "/k.csv");
    }

    /// A dotted bucket name cannot go in a hostname: a wildcard certificate
    /// covers one label, so the TLS handshake fails before any request is made.
    /// This has to hold on plain AWS too, where there is no endpoint to hint at
    /// it. Dotted names are legal and common on older and static-site buckets.
    #[test]
    fn a_dotted_bucket_never_goes_in_the_hostname() {
        let aws = S3Config {
            region: "eu-west-1".into(),
            use_ssl: true,
            ..Default::default()
        };
        let (host, uri) = aws.address("my.data.lake", "k.csv");
        assert_eq!(host, "s3.eu-west-1.amazonaws.com");
        assert_eq!(uri, "/my.data.lake/k.csv");

        // Even asked for explicitly: vhost is not available for such a bucket,
        // so honouring it would only produce a handshake failure.
        let forced = S3Config {
            url_style: Some("vhost".into()),
            ..aws.clone()
        };
        assert_eq!(
            forced.address("my.data.lake", "k.csv").0,
            "s3.eu-west-1.amazonaws.com"
        );
        // A bucket with no dot still gets vhost when asked.
        assert_eq!(
            forced.address("plain", "k.csv").0,
            "plain.s3.eu-west-1.amazonaws.com"
        );
    }

    /// SigV4 signs Host, and the client derives Host from the parsed URL: it
    /// lowercases the host and drops a port equal to the scheme default. Signing
    /// the raw endpoint therefore covers a different value than the wire carries,
    /// and every request 403s with SignatureDoesNotMatch - which reads as a bad
    /// access key.
    #[test]
    fn the_signed_host_matches_what_the_client_will_send() {
        let https = S3Config {
            endpoint: Some("MinIO.Internal:443".into()),
            url_style: Some("path".into()),
            use_ssl: true,
            ..Default::default()
        };
        assert_eq!(
            https.address("raw", "k").0,
            "minio.internal",
            "lowercased, and :443 is dropped on https exactly as the URL parser does"
        );

        let http = S3Config {
            use_ssl: false,
            ..https.clone()
        };
        assert_eq!(
            http.address("raw", "k").0,
            "minio.internal:443",
            ":443 is NOT the default for http, so it stays"
        );
        let http80 = S3Config {
            endpoint: Some("minio.internal:80".into()),
            use_ssl: false,
            url_style: Some("path".into()),
            ..Default::default()
        };
        assert_eq!(http80.address("raw", "k").0, "minio.internal");

        // A non-default port is part of the host and must survive.
        let odd = S3Config {
            endpoint: Some("minio.internal:9000".into()),
            ..https
        };
        assert_eq!(odd.address("raw", "k").0, "minio.internal:9000");
    }

    /// An endpoint typed as `http://localhost:9000` is a plain-HTTP store. The
    /// scheme was being stripped and discarded, so it was dialled over TLS and
    /// died in the handshake unless the separate SSL switch was also found.
    #[test]
    fn an_http_endpoint_is_not_dialled_over_tls() {
        let c = S3Config::from_props(&json!({
            "accessKey": "A", "secretKey": "B", "endpoint": "http://localhost:9000"
        }))
        .unwrap();
        assert!(!c.use_ssl, "the scheme says plain HTTP");
        assert_eq!(c.scheme(), "http");

        // https:// implies TLS, and no scheme at all still defaults to TLS.
        assert!(
            S3Config::from_props(&json!({
                "accessKey": "A", "secretKey": "B", "endpoint": "https://s3.example.com"
            }))
            .unwrap()
            .use_ssl
        );
        assert!(
            S3Config::from_props(&json!({
                "accessKey": "A", "secretKey": "B", "endpoint": "s3.example.com"
            }))
            .unwrap()
            .use_ssl
        );

        // An explicit setting still wins - the scheme only decides what unset
        // means, so a store fronted by a TLS proxy can still be described.
        assert!(
            S3Config::from_props(&json!({
                "accessKey": "A", "secretKey": "B",
                "endpoint": "http://localhost:9000", "useSsl": true
            }))
            .unwrap()
            .use_ssl
        );
    }

    /// Expanding the ampersand entity FIRST decodes an escaped entity twice, so
    /// a key is read as something that does not exist and the object goes
    /// silently missing from a listing.
    #[test]
    fn an_escaped_entity_is_decoded_exactly_once() {
        // S3 sends `report&lt;1&gt;.csv` for a key literally named that, escaped
        // once more: the ampersands themselves are escaped.
        assert_eq!(
            unescape_xml("report&amp;lt;1&amp;gt;.csv"),
            "report&lt;1&gt;.csv"
        );
        assert_eq!(unescape_xml("a&amp;b.csv"), "a&b.csv");
        assert_eq!(unescape_xml("&lt;tag&gt;"), "<tag>");
        assert_eq!(unescape_xml("say &quot;hi&quot;"), "say \"hi\"");
    }

    /// A stub that answers a fixed script of responses and records the request
    /// lines, so a test can assert what actually went on the wire.
    fn stub(replies: Vec<String>) -> (u16, std::sync::mpsc::Receiver<String>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for (i, stream) in listener.incoming().take(replies.len()).enumerate() {
                let mut stream = match stream {
                    Ok(s) => s,
                    Err(_) => break,
                };
                stream
                    .set_read_timeout(Some(std::time::Duration::from_millis(400)))
                    .ok();
                // Drain the whole request, body included, before answering.
                // Replying to a PUT while its body is still being written makes
                // the client see a connection reset instead of the status - the
                // test then fails on a network error rather than on what it is
                // actually about.
                let mut req = String::new();
                let mut buf = [0u8; 8192];
                loop {
                    match stream.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            req.push_str(&String::from_utf8_lossy(&buf[..n]));
                            // Headers seen and nothing more waiting: a request
                            // with no body is complete here.
                            if n < buf.len() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let _ = tx.send(req.lines().next().unwrap_or_default().to_string());
                let _ = stream.write_all(replies[i].as_bytes());
                let _ = stream.flush();
                let _ = stream.shutdown(std::net::Shutdown::Write);
            }
        });
        (port, rx)
    }

    fn local(port: u16) -> S3Config {
        S3Config {
            access_key: "A".into(),
            secret_key: "B".into(),
            region: "us-east-1".into(),
            session_token: None,
            endpoint: Some(format!("127.0.0.1:{port}")),
            url_style: Some("path".into()),
            use_ssl: false,
        }
    }

    /// S3 answers a wrong-region request with a 3xx, which the HTTP client hands
    /// back as an ordinary success. Believing it is silent in the worst way for
    /// each verb: a HEAD adopts the error page's Content-Length as the object's
    /// size, and a GET streams the error XML as the object's bytes.
    #[test]
    fn a_redirect_is_an_error_that_names_the_region_not_a_result() {
        let redirect = "HTTP/1.1 301 Moved Permanently\r\n\
             x-amz-bucket-region: eu-west-1\r\n\
             Content-Length: 108\r\nConnection: close\r\n\r\n\
             <?xml version=\"1.0\"?><Error><Code>PermanentRedirect</Code>\
             <Message>The bucket is in another region</Message></Error>"
            .to_string();

        let (port, _rx) = stub(vec![redirect.clone(), redirect.clone(), redirect]);
        let c = local(port);

        let e = c.head("raw", "a.pdf").unwrap_err().to_string();
        assert!(
            e.contains("301") && e.contains("eu-west-1"),
            "must name the region: {e}"
        );
        let e = match c.get("raw", "a.pdf") {
            Ok(_) => panic!("a redirect body was handed back as the object's bytes"),
            Err(e) => e.to_string(),
        };
        assert!(
            e.contains("301"),
            "a redirect body is not the object's bytes: {e}"
        );
        let e = c
            .put("raw", "a.pdf", &b"hello"[..], 5, None)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("301"),
            "a write that did not happen is not a success: {e}"
        );
    }

    /// A multipart upload that fails at the last step must not leave its parts
    /// behind. They are billed as storage and appear in no listing, so nothing
    /// reports them. S3 signals this particular failure with a 200 carrying an
    /// Error body, which is exactly the case that used to skip the abort.
    #[test]
    fn a_multipart_upload_that_fails_to_complete_is_aborted() {
        let ok = |body: &str| {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{}",
                body.len(),
                body
            )
        };
        let (port, rx) = stub(vec![
            ok("<InitiateMultipartUploadResult><UploadId>up-1</UploadId></InitiateMultipartUploadResult>"),
            "HTTP/1.1 200 OK\r\nETag: \"p1\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
            // 200, and an error inside it. The documented S3 behaviour.
            ok("<Error><Code>InternalError</Code><Message>try again</Message></Error>"),
            // The abort this test exists to prove happens.
            "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string(),
        ]);

        let e = local(port)
            .put_multipart("raw", "big.bin", &b"0123456789"[..], 1024, None)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("InternalError") || e.contains("try again"),
            "{e}"
        );

        let seen: Vec<String> = (0..4)
            .filter_map(|_| rx.recv_timeout(std::time::Duration::from_secs(5)).ok())
            .collect();
        assert_eq!(seen.len(), 4, "the abort was never sent: {seen:?}");
        assert!(
            seen[0].starts_with("POST /raw/big.bin?uploads"),
            "{:?}",
            seen[0]
        );
        assert!(
            seen[1].starts_with("PUT /raw/big.bin?partNumber=1"),
            "{:?}",
            seen[1]
        );
        assert!(
            seen[2].starts_with("POST /raw/big.bin?uploadId=up-1"),
            "{:?}",
            seen[2]
        );
        assert!(
            seen[3].starts_with("DELETE /raw/big.bin?uploadId=up-1"),
            "the parts were left behind to be billed for: {:?}",
            seen[3]
        );
    }
}
