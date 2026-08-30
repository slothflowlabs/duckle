# How to Execute Incident Response Runbooks

This runbook outlines operational procedures for identifying, containing, eradicating, and recovering from potential security incidents involving Duckle instances.

---

## 1. Incident Classification & Severity Matrix

| Severity | Definition | Examples | Response Target |
| :--- | :--- | :--- | :--- |
| **SEV-1 (Critical)** | Active compromise of server or production credentials with potential data exfiltration. | Exposed `admin` API key, unauthorized pipeline deployment executing remote shell code. | ≤ 15 minutes |
| **SEV-2 (High)** | Compromise of an unprivileged credential or repeated anomalous authorization failures. | Leaked `operator` key, brute force probe on `/api/*` endpoints. | ≤ 1 hour |
| **SEV-3 (Medium)** | Misconfiguration of workspace permissions without confirmed exploitation. | World-readable `.duckle/keys/` discovered during routine audit. | ≤ 4 hours |
| **SEV-4 (Low)** | Minor logging anomaly or non-exploitable security bug report. | Audit log formatting defect. | ≤ 24 hours |

---

## 2. Emergency Containment Runbook: Compromised Token

If an API key (`admin`, `operator`, or `viewer`) is compromised or leaked in logs:

### Step 1: Identify Key Label and Revoke Immediately
Connect to the host running Duckle Server:

```bash
# List all active credentials
duckle-runner console key-list --workspace /var/duckle/workspace

# Revoke the compromised key by label
duckle-runner console key-revoke compromised-key-label --workspace /var/duckle/workspace
```
Revocation takes effect immediately for all subsequent incoming requests.

### Step 2: Kill Active Pipeline Runs
If an unauthorized pipeline execution was triggered:

```bash
# Search and terminate active execution processes
pkill -f "duckle-runner run"
```

---

## 3. Emergency Containment Runbook: Compromised Workspace Keys

If the `.duckle/keys/` master encryption key was exposed:

### Step 1: Isolate the Server Network
Temporarily drop external inbound traffic to port 8095:
```bash
sudo iptables -A INPUT -p tcp --dport 8095 -j DROP
```

### Step 2: Rotate Downstream Service Credentials
Because saved connection profiles in `connections/` may have been decrypted:
1. Immediately rotate passwords, API tokens, and IAM secret keys in upstream and downstream systems (PostgreSQL, Snowflake, AWS S3, etc.).
2. Update local connection files or transition to runtime `${ENV:...}` injection.

### Step 3: Re-key the Duckle Workspace
1. Move the old keys out of the workspace:
   ```bash
   mv /var/duckle/workspace/.duckle/keys /var/duckle/workspace/.duckle/keys.bak
   ```
2. Restart Duckle Studio or run `duckle-runner serve` to initialize a new cryptographic master key.
3. Re-save the connection profiles in Duckle Studio with fresh passwords.

---

## 4. Forensic Investigation & Log Preservation

Before restarting services, snapshot the audit logs and run histories for forensic timeline reconstruction:

```bash
# Create an immutable evidence directory
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mkdir -p /var/log/forensics/duckle_$TIMESTAMP

# Copy audit log and run histories
cp /var/duckle/workspace/logs/audit.ndjson /var/log/forensics/duckle_$TIMESTAMP/
cp -r /var/duckle/workspace/run-history /var/log/forensics/duckle_$TIMESTAMP/

# Calculate SHA256 checksums to preserve chain of custody
sha256sum /var/log/forensics/duckle_$TIMESTAMP/* > /var/log/forensics/duckle_$TIMESTAMP/CHECKSUMS.txt
```

### Key Questions to Answer from `audit.ndjson`:
1. **Who**: Which `actor` and `role` performed the actions?
2. **What**: What endpoints (`/api/deploy`, `/api/run/<name>`, `/api/connections`) were accessed?
3. **When**: What was the exact start and end timestamp of the anomalous activity?
4. **Scope**: Were unauthorized pipelines installed, replaced, or run against production databases?

---

## 5. Post-Incident Review Template

Within 48 hours of resolving a SEV-1 or SEV-2 incident, complete a post-mortem containing:

1. **Incident Summary**: High-level timeline, affected components, and root cause.
2. **Detection**: How was the incident discovered (SIEM alert, manual discovery, external report)?
3. **Impact**: Were any production datasets accessed, modified, or exfiltrated?
4. **Corrective Actions**:
   - Technical remediations (e.g. enhanced network segmentation, rotation policies).
   - Process improvements (e.g. CI secret scanning enforcement, automated key expiration).
