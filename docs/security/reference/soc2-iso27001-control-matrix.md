# SOC 2 & ISO 27001 Control Matrix

This document provides a detailed mapping of Duckle's architectural capabilities, cryptographic mechanisms, and operational controls against the **AICPA SOC 2 Type II Trust Services Criteria** and **ISO/IEC 27001:2022 Annex A** control sets.

---

## 1. Compliance Stance for Self-Hosted Architecture

Duckle is **open-source software distributed for self-hosting** on customer-controlled infrastructure. 

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                    Customer Infrastructure Boundary                     │
│                                                                         │
│  ┌───────────────────────────┐    ┌──────────────────────────────────┐  │
│  │   Duckle Engine / Runner  │    │  Customer Network & Storage      │  │
│  │  - AES-256-GCM Encryption │    │  - Network Perimeter / Firewalls │  │
│  │  - Structured Audit Trail │    │  - OS User Access & Patching     │  │
│  │  - Granular RBAC          │    │  - Secrets Vault (Vault/AWS/Env) │  │
│  │  - Stripped Previews/DLQ  │    │  - SIEM Log Aggregation          │  │
│  └───────────────────────────┘    └──────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
        ▲
        │ NO TELEMETRY / NO HOSTED CLOUD / NO DATA TRANSIT
        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                Duckle Open Source Project Boundary                      │
│                                                                         │
│  - Public GitHub Source, Issue Tracking, and Advisories                 │
│  - Strict Multi-OS CI/CD Verification & Clippy/Rustfmt Quality Gates    │
│  - Reproducible Builds & Cryptographic Checksum Manifests               │
└─────────────────────────────────────────────────────────────────────────┘
```

* **Zero Sub-processors**: The vendor (SlothFlowLabs) does not store, transmit, or process your enterprise data.
* **Audit Applicability**: Organizations undergoing SOC 2 or ISO 27001 assessments can use this matrix to satisfy auditor requests regarding data processing controls and development integrity.

---

## 2. SOC 2 Type II Trust Services Criteria (TSC) Mapping

| Criteria ID | Description | Duckle Architectural Control | Customer Implementation Responsibility |
| :--- | :--- | :--- | :--- |
| **CC6.1** | Logical access security controls | Granular Role-Based Access Control (`admin`, `operator`, `viewer`) enforced at HTTP server boundary. Base64url API keys stored only as cryptographic hashes. | Provision unique tokens per team/service. Maintain token lifecycle and expiration (`--expires-days`). |
| **CC6.2** | User registration and credential management | CLI commands for key minting (`key-add`), listing (`key-list`), and instantaneous revocation (`key-revoke`). | Enforce key revocation upon employee offboarding or credential rotation schedule. |
| **CC6.3** | Access revocation and least privilege | Route-level permission gates. Deployment requires `admin`; schedule trigger requires `operator`; audit inspection requires `admin` or `viewer`. | Grant lowest required role to automated CI/CD service accounts. |
| **CC6.6** | Protection of data in transit | Headless runner operates behind customer reverse proxy / ingress terminating TLS (HTTPS). Internal inter-process memory channels never leave the host. | Terminate TLS 1.3 via Nginx, Traefik, AWS ALB, or Envoy in front of `duckle-runner serve`. |
| **CC6.8** | Protection against unauthorized code | Atomic pipeline deployment (staged in temp files and renamed into place); prevention of pipeline self-activation (schedules deployed disabled). | Require code review and merge gates on Git repositories containing pipeline JSON. |
| **CC7.1** | Vulnerability management and scanning | Automated CI dependency auditing (`cargo audit`), Dependabot alerts, and zero-day triage SLAs via GitHub Private Advisories. | Patch and update Duckle binaries / Docker container tags according to release notifications. |
| **CC7.2** | Security monitoring and event logging | Append-only NDJSON audit logging (`audit.ndjson`) recording all authentication attempts, route access, actor, role, and outcome (`allowed`, `denied`, `unauthenticated`). | Ingest `audit.ndjson` into SIEM (Vector / Datadog / Splunk) and alert on abnormal permission denial patterns. |
| **CC8.1** | Change management and SSDLC | Every change lands via pull request with mandatory multi-OS CI matrix tests, clippy linter enforcement, and end-to-end contract validation. | Maintain peer-review pull request requirements for data pipeline repositories. |

---

## 3. ISO/IEC 27001:2022 Annex A Control Mapping

| ISO 27001:2022 Control | Control Title | Duckle Technical Mechanism |
| :--- | :--- | :--- |
| **A.5.15** | Access Control | Role-Based Access Control (RBAC) separating administrative actions (`/api/deploy`, `/api/connections`) from operational actions (`/api/run`). |
| **A.5.16** | Identity Management | Per-actor API key tracking, storing only irreversible key hashes on disk. |
| **A.5.18** | Access Rights | Granular scoping of keys (`admin`, `operator`, `viewer`); immediate revocation via CLI. |
| **A.8.7** | Protection Against Malware | Static compilation of Rust runner binaries; release artifacts published with cryptographic SHA256 checksums. |
| **A.8.8** | Management of Technical Vulnerabilities | Coordinated vulnerability disclosure policy (`SECURITY.md`), CVSS triage SLAs, and automated dependency alerts. |
| **A.8.9** | Configuration Management | Pipelines stored as declarative JSON files under version control; environment variables decoupled from code. |
| **A.8.12** | Data Leakage Prevention | Deployment sanitizer automatically strips cached preview rows (`sampleRows`) and blocks hardcoded plaintext credentials. |
| **A.8.20** | Network Security | Localhost binding by default (`127.0.0.1`); 15-minute claim window for new setups to prevent unauthenticated network hijacking. |
| **A.8.24** | Use of Cryptography | AES-256-GCM encryption for stored connection credentials; support for PKI certificates in database sinks. |
| **A.8.28** | Secure Coding | Test-Driven Development (TDD) discipline, Rust memory-safety guarantees, Clippy lint enforcement, and unit/integration contract test suites. |
