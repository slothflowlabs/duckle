# Security Audit Event Schema & Action Dictionary

This document is the authoritative technical reference for the structured audit events produced by Duckle Server and the Duckle Engine.

---

## 1. Storage Format & Location

* **Path**: `<workspace>/logs/audit.ndjson`
* **Format**: Newline-Delimited JSON (NDJSON) — each line is an independently valid JSON object.
* **Encoding**: UTF-8 without BOM.
* **Write Semantics**: Append-only. A write failure logs an error to `stderr` but never halts or crashes the server.

---

## 2. Event JSON Schema

Each audit entry conforms to the following schema definition:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "DuckleAuditEvent",
  "type": "object",
  "required": ["at", "actor", "role", "action", "target", "outcome"],
  "properties": {
    "at": {
      "type": "string",
      "format": "date-time",
      "description": "RFC 3339 UTC timestamp with millisecond precision (e.g. '2026-08-30T14:42:10.124Z')."
    },
    "actor": {
      "type": "string",
      "description": "Identifier label of the identity that made the request. Unauthenticated requests are recorded as '-'."
    },
    "role": {
      "type": "string",
      "enum": ["admin", "operator", "viewer", "-"],
      "description": "Assigned role level at execution time. '-' indicates no authenticated role."
    },
    "action": {
      "type": "string",
      "description": "The HTTP method or system action verb (e.g., 'GET', 'POST', 'session.sign_in', 'console.claimed')."
    },
    "target": {
      "type": "string",
      "description": "Target endpoint URI or system resource (e.g., '/api/deploy', '/api/run/orders_etl')."
    },
    "outcome": {
      "type": "string",
      "enum": ["allowed", "denied", "unauthenticated"],
      "description": "Authorization result of the attempt."
    },
    "detail": {
      "type": ["string", "null"],
      "description": "Optional contextual information. Guaranteed never to contain raw passwords, keys, or payload bodies."
    }
  }
}
```

---

## 3. Outcome Definitions

| Outcome | Definition | SIEM Classification |
| :--- | :--- | :--- |
| `allowed` | The caller was authenticated and possessed sufficient role permissions to execute the target action. | Informational / Audit Trail |
| `denied` | The caller was successfully authenticated, but their role lacked permission for the requested route (e.g. `viewer` accessing `/api/deploy`). | Security Warning |
| `unauthenticated` | The caller supplied an invalid, expired, or missing Bearer token. | Security Warning / Anomaly |

---

## 4. Action & Route Dictionary

| Route / Action | HTTP Method | Required Role | Description |
| :--- | :--- | :--- | :--- |
| `session.sign_in` | `POST` | *None (Password auth)* | Web console user sign-in attempt. |
| `console.claimed` | `POST` | *Setup Token* | The initial one-time server claim during first setup. |
| `/api/whoami` | `GET` | `viewer` | Token verification and role query. |
| `/api/deploy` | `POST` | `admin` | Deploying or updating a pipeline file and its associated schedule. |
| `/api/run/:name` | `POST` | `operator` | Triggering immediate execution of a named pipeline. |
| `/api/cancel/:run_id` | `POST` | `operator` | Terminating an in-progress pipeline run. |
| `/api/schedules` | `GET` | `viewer` | Listing active pipeline schedules and next run times. |
| `/api/schedules/:name` | `POST` | `operator` | Enabling, disabling, or modifying a pipeline schedule cadence. |
| `/api/history` | `GET` | `viewer` | Reading pipeline execution history and status reports. |
| `/api/connections` | `GET` | `admin` | Reading configured connection metadata (secrets are never returned). |
| `/api/audit` | `GET` | `admin` | Reading and filtering the server audit log. |
| `/api/console/keys` | `GET`, `POST`, `DELETE`| `admin` | Managing server API keys. |
