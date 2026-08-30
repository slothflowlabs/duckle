# Tutorial: Ingesting Security Audit Logs into a SIEM

In this tutorial, you will set up Duckle Server, configure role-based access control, trigger various security events (successful logins, policy denials, unauthenticated probes), and observe the structured append-only audit trail in `audit.ndjson`.

---

## Learning Objectives

By the end of this tutorial, you will:
1. Initialize a standalone `duckle-runner serve` instance with audit logging enabled.
2. Generate authenticated API keys with differing roles (`admin`, `operator`, `viewer`).
3. Generate sample security telemetry by simulating authorized operations and unauthorized access attempts.
4. Parse and inspect the structured events written to `<workspace>/logs/audit.ndjson`.

---

## Prerequisites

* `duckle-runner` compiled binary or container image.
* A terminal environment with `curl` and `jq` installed.

---

## Step 1: Initialize the Server Instance

Create an isolated directory for the server's workspace and launch the daemon.

```bash
mkdir -p /tmp/duckle-audit-demo
cd /tmp/duckle-audit-demo

# Start the Duckle server on port 8095
duckle-runner serve --workspace /tmp/duckle-audit-demo --host 127.0.0.1 --port 8095 &
SERVER_PID=$!
sleep 2
```

The server automatically initializes `<workspace>/logs/audit.ndjson` for append-only record tracking.

---

## Step 2: Mint Scoped Access Keys

Generate distinct API credentials to observe how actor identities and roles are attributed in the audit stream:

```bash
# 1. Create an Administrator key
ADMIN_KEY=$(duckle-runner console key-add admin-bot --role admin --workspace /tmp/duckle-audit-demo | awk '{print $NF}')

# 2. Create an Operator key
OPERATOR_KEY=$(duckle-runner console key-add ops-runner --role operator --workspace /tmp/duckle-audit-demo | awk '{print $NF}')

# 3. Create a Viewer key
VIEWER_KEY=$(duckle-runner console key-add auditor-view --role viewer --workspace /tmp/duckle-audit-demo | awk '{print $NF}')

echo "Admin Key: $ADMIN_KEY"
echo "Operator Key: $OPERATOR_KEY"
echo "Viewer Key: $VIEWER_KEY"
```

---

## Step 3: Simulate Security Events

We will now perform four distinct actions to generate realistic audit records:

### 1. Authorized Admin Query (`Outcome::Allowed`)
```bash
curl -s -X GET http://127.0.0.1:8095/api/whoami \
  -H "Authorization: Bearer $ADMIN_KEY"
```

### 2. Operator Triggering a Pipeline Execution (`Outcome::Allowed`)
```bash
curl -s -X POST http://127.0.0.1:8095/api/run/sample_pipeline \
  -H "Authorization: Bearer $OPERATOR_KEY"
```

### 3. Viewer Attempting an Unauthorized Deployment (`Outcome::Denied`)
The `viewer` role does not have permission to deploy pipelines (`/api/deploy` requires `admin`).
```bash
curl -s -X POST http://127.0.0.1:8095/api/deploy \
  -H "Authorization: Bearer $VIEWER_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"malicious_job","pipeline":{}}'
```

### 4. Unauthenticated Probe (`Outcome::Unauthenticated`)
An external attacker or scanner probes the administrative audit endpoint without supplying a Bearer token:
```bash
curl -s -X GET http://127.0.0.1:8095/api/audit
```

---

## Step 4: Inspect and Analyze `audit.ndjson`

View the formatted audit entries using `jq`:

```bash
cat /tmp/duckle-audit-demo/logs/audit.ndjson | jq .
```

### Expected Output

```json
{
  "at": "2026-08-30T14:42:10.124Z",
  "actor": "admin-bot",
  "role": "admin",
  "action": "GET",
  "target": "/api/whoami",
  "outcome": "allowed"
}
{
  "at": "2026-08-30T14:42:15.892Z",
  "actor": "ops-runner",
  "role": "operator",
  "action": "POST",
  "target": "/api/run/sample_pipeline",
  "outcome": "allowed"
}
{
  "at": "2026-08-30T14:42:20.450Z",
  "actor": "auditor-view",
  "role": "viewer",
  "action": "POST",
  "target": "/api/deploy",
  "outcome": "denied"
}
{
  "at": "2026-08-30T14:42:25.011Z",
  "actor": "-",
  "role": "-",
  "action": "GET",
  "target": "/api/audit",
  "outcome": "unauthenticated"
}
```

---

## Step 5: Query Audit Trail via the Duckle CLI

You can also filter the audit trail directly using the Duckle CLI without external tools:

```bash
# Query all denied attempts
duckle-runner audit --workspace /tmp/duckle-audit-demo --outcome denied

# Query actions by a specific actor
duckle-runner audit --workspace /tmp/duckle-audit-demo --actor ops-runner
```

---

## Cleanup

Stop the background test server:
```bash
kill $SERVER_PID
rm -rf /tmp/duckle-audit-demo
```

---

## Summary

In this tutorial, you learned how to:
* Generate granular, role-scoped credentials.
* Attribute every server API interaction to a specific actor and role.
* Audit denied authorization attempts and unauthenticated probing.
* Parse the standardized `audit.ndjson` stream for compliance monitoring.
