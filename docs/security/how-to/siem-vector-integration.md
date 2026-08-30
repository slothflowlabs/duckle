# How to Forward Audit Logs to SIEM using Vector / Fluent Bit

This guide demonstrates how to stream Duckle's append-only security log (`<workspace>/logs/audit.ndjson`) to enterprise Security Information and Event Management (SIEM) platforms including Datadog, Splunk, AWS CloudWatch, and Elasticsearch.

---

## 1. Architecture Overview

```text
┌────────────────────────────────────────────────────────┐
│                   Duckle Runner Host                   │
│                                                        │
│  ┌───────────────────────┐    ┌─────────────────────┐  │
│  │   duckle-runner serve │───►│ logs/audit.ndjson   │  │
│  └───────────────────────┘    └──────────┬──────────┘  │
│                                          │ (tail)      │
│                               ┌──────────▼──────────┐  │
│                               │   Vector / Agent    │  │
│                               └──────────┬──────────┘  │
└──────────────────────────────────────────┼─────────────┘
                                           │ TLS / HTTPS
                                           ▼
             ┌───────────────────────────────────────────────────────────┐
             │       Enterprise SIEM / Log Analytics Platform            │
             │  (Datadog · Splunk HEC · AWS CloudWatch · Elasticsearch)  │
             └───────────────────────────────────────────────────────────┘
```

---

## 2. Option A: Shipping with Vector (Recommended)

[Vector](https://vector.dev/) is an ultra-fast, memory-safe observability router written in Rust.

### Configuration (`/etc/vector/vector.yaml`)

```yaml
sources:
  duckle_audit_source:
    type: file
    include:
      - /var/duckle/workspace/logs/audit.ndjson
    read_from: beginning

transforms:
  parse_duckle_audit:
    type: remap
    inputs:
      - duckle_audit_source
    source: |
      . = parse_json!(.message)
      .service = "duckle-runner"
      .environment = "production"
      .source = "audit-log"
      # Map timestamp to Vector native @timestamp
      .@timestamp = parse_timestamp!(.at, format: "%+")

sinks:
  # Datadog Logs Sink
  datadog_sink:
    type: datadog_logs
    inputs:
      - parse_duckle_audit
    default_api_key: "${DATADOG_API_KEY}"
    site: datadoghq.com

  # Splunk HTTP Event Collector (HEC) Sink
  splunk_sink:
    type: splunk_hec_logs
    inputs:
      - parse_duckle_audit
    endpoint: "https://splunk.internal.net:8088"
    token: "${SPLUNK_HEC_TOKEN}"
    index: "security_audit"
    sourcetype: "duckle:audit:ndjson"

  # Elasticsearch / OpenSearch Sink
  elastic_sink:
    type: elasticsearch
    inputs:
      - parse_duckle_audit
    endpoint: "https://elasticsearch.internal.net:9200"
    mode: "bulk"
    auth:
      strategy: "bearer"
      token: "${ELASTIC_API_KEY}"
    index: "duckle-audit-%Y.%m.%d"
```

---

## 3. Option B: Shipping with Fluent Bit

[Fluent Bit](https://fluentbit.io/) is a lightweight log processor commonly deployed in Kubernetes and container environments.

### Configuration (`fluent-bit.conf`)

```ini
[SERVICE]
    Flush        1
    Daemon       Off
    Log_Level    info
    Parsers_File parsers.conf

[INPUT]
    Name         tail
    Path         /workspace/logs/audit.ndjson
    Parser       json_parser
    Tag          duckle.security.audit
    Refresh_Interval 2

[FILTER]
    Name         record_modifier
    Match        duckle.security.audit
    Record       hostname ${HOSTNAME}
    Record       service duckle-runner

[OUTPUT]
    Name         http
    Match        duckle.security.audit
    Host         siem-collector.internal.net
    Port         443
    URI          /api/v1/logs
    Format       json
    tls          On
    tls.verify   On
    Header       Authorization Bearer ${SIEM_INGEST_TOKEN}
```

---

## 4. Key SIEM Alert Signatures to Implement

Once ingested into your SIEM, configure real-time alert rules for the following patterns:

### 1. Repeated Authorization Denials (Brute Force / Probe)
* **Rule**: Alert when `outcome == "denied"` count exceeds 5 occurrences from the same actor within 5 minutes.
* **Severity**: High (Potential credential misuse or permission creep).

### 2. High-Frequency Unauthenticated Access
* **Rule**: Alert when `outcome == "unauthenticated"` count exceeds 10 within 60 seconds on any `/api/*` route.
* **Severity**: High (Port scanning, unauthenticated penetration attempt).

### 3. Critical Administrative Action
* **Rule**: Alert on any occurrence of `action == "console.claimed"` or `action == "POST"` on `/api/deploy`.
* **Severity**: Medium/Informational (Audit record of administrative pipeline change).
