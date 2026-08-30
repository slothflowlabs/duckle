# Security CLI Reference

This document provides command-line syntax, arguments, and usage examples for the security and credential commands in `duckle-runner`.

---

## 1. `console key-add`

Mint a new scoped API key for a person or automated CI/CD service account.

```bash
duckle-runner console key-add <LABEL> [OPTIONS]
```

### Arguments & Flags
* `<LABEL>`: Human-readable identifier for the key (e.g., `github-actions`, `alice-laptop`, `airflow-runner`). Shown in audit logs.
* `--role <ROLE>`: Permission level granted to the key. Choices: `admin`, `operator`, `viewer`. (Default: `viewer`).
* `--expires-days <DAYS>`: Key lifetime in days after which the key becomes invalid. (Optional; default: no expiration).
* `--workspace <PATH>`: Path to the target workspace folder. (Optional; defaults to current directory).

### Example
```bash
duckle-runner console key-add github-actions --role admin --expires-days 90 --workspace /var/duckle/workspace
```
*Note: The plaintext key is printed to stdout once. Only a cryptographic hash is stored on disk.*

---

## 2. `console key-list`

List all active and expired keys configured in the workspace.

```bash
duckle-runner console key-list [--workspace <PATH>]
```

### Output Example
```text
LABEL            ROLE      EXPIRES AT            LAST USED AT
github-actions   admin     2026-11-28 12:00:00   2026-08-30 08:30:11
ops-team         operator  -                     2026-08-29 17:15:04
auditor          viewer    2026-09-15 00:00:00   2026-08-25 10:00:00
```

---

## 3. `console key-revoke`

Immediately invalidate an API key so that any subsequent requests using it are rejected.

```bash
duckle-runner console key-revoke <LABEL> [--workspace <PATH>]
```

### Example
```bash
duckle-runner console key-revoke github-actions --workspace /var/duckle/workspace
```

---

## 4. `audit`

Filter and inspect the append-only security audit log (`audit.ndjson`).

```bash
duckle-runner audit [OPTIONS]
```

### Flags
* `--workspace <PATH>`: Target workspace directory.
* `--actor <NAME>`: Filter entries by specific actor identity.
* `--role <ROLE>`: Filter by role (`admin`, `operator`, `viewer`, `-`).
* `--action <VERB>`: Filter by HTTP verb or action name (e.g. `POST`, `session.sign_in`).
* `--outcome <OUTCOME>`: Filter by outcome (`allowed`, `denied`, `unauthenticated`).
* `--limit <N>`: Maximum number of recent entries to return (default: `100`).

### Example
```bash
# Query the last 20 denied requests
duckle-runner audit --outcome denied --limit 20 --workspace /var/duckle/workspace
```
