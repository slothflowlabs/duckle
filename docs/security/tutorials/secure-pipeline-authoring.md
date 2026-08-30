# Tutorial: Authoring Secure Data Pipelines

In this tutorial, you will learn how to design, configure, and verify an enterprise data pipeline with zero credential leakage and built-in data quality controls.

---

## Learning Objectives

By the end of this tutorial, you will be able to:
1. Parameterize sensitive secrets using `${ENV:VARIABLE}` interpolation.
2. Build an automated Data Quality (QA) validation stage with a Dead Letter Queue (DLQ) for PII handling.
3. Validate that cached preview rows and secrets are stripped prior to server deployment.
4. Execute a pipeline headless using isolated runtime environment variables.

---

## Prerequisites

* Duckle Desktop installed, or `duckle-runner` on your system `PATH`.
* Access to a terminal shell (Bash, PowerShell, or Zsh).
* A local sample CSV file (or use `samples/orders.csv`).

---

## Step 1: Create a Secure Workspace Folder

To maintain isolation, start by creating a dedicated workspace directory with restricted filesystem permissions.

### Linux / macOS:
```bash
mkdir -p ~/duckle-secure-workspace
chmod 700 ~/duckle-secure-workspace
```

### Windows (PowerShell):
```powershell
New-Item -ItemType Directory -Path "$HOME\duckle-secure-workspace"
icacls "$HOME\duckle-secure-workspace" /inheritance:r /grant:r "$($env:USERNAME):(OI)(CI)F"
```

Open Duckle, click **Switch Workspace**, and select this folder.

---

## Step 2: Configure Secrets Without Hardcoding

Never type production database passwords or API keys directly into connector property fields. Duckle provides secure runtime variable resolution.

1. Create a `.env` file in your workspace directory (ensure it is added to `.gitignore`):
   ```bash
   cat << 'EOF' > ~/duckle-secure-workspace/.env
   WAREHOUSE_HOST=postgres.internal.net
   WAREHOUSE_USER=etl_user
   WAREHOUSE_PASS=s3cur3_p@ssw0rd_992!
   EOF
   ```

2. Open the Duckle Canvas and drag a **PostgreSQL Sink** from the palette.
3. In the **Properties Panel** on the right, enter the configuration parameters:
   * **Host**: `${ENV:WAREHOUSE_HOST}`
   * **Database**: `production_warehouse`
   * **Username**: `${ENV:WAREHOUSE_USER}`
   * **Password**: `${ENV:WAREHOUSE_PASS}`

```text
┌─────────────────────────────────────────────────────────┐
│                 PostgreSQL Sink Config                  │
├─────────────────┬───────────────────────────────────────┤
│ Host            │ ${ENV:WAREHOUSE_HOST}                 │
│ User            │ ${ENV:WAREHOUSE_USER}                 │
│ Password        │ ${ENV:WAREHOUSE_PASS}                 │
│ Database        │ production_warehouse                  │
└─────────────────┴───────────────────────────────────────┘
```

> [!NOTE]
> Duckle pipeline files (`.json`) store only the literal string `${ENV:WAREHOUSE_PASS}`. The plaintext password is never written into the pipeline file and will never be committed to Git.

---

## Step 3: Add a QA Validator with a Dead-Letter Queue (DLQ)

To prevent corrupted records or invalid PII from reaching downstream analytics systems, isolate bad records using a QA Validator.

1. Drag a **CSV Source** node onto the canvas and select `samples/orders.csv`.
2. Drag a **QA Validator** node (e.g. `qa.notnull` or `qa.regex`) onto the canvas.
3. Connect the output of the **CSV Source** to the input of the **QA Validator**.
4. In the **QA Validator** properties, set the rule:
   * **Field**: `customer_email`
   * **Validation Rule**: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
5. Connect the top **Pass Port** (green circle) to your **PostgreSQL Sink**.
6. Drag a **Parquet Sink** onto the canvas and set the file path to `quarantine/invalid_records_${ENV:TODAY}.parquet`.
7. Connect the bottom **Reject Port** (red circle) of the validator to the **Parquet Sink**.

```text
                     ┌──────────────────┐
                     │ QA Email Validate│
  [CSV Orders] ─────►●                  ●──────► [PostgreSQL Sink] (Valid)
                     │                  │
                     │                  ●──────► [Quarantine Sink] (Invalid / DLQ)
                     └──────────────────┘
```

---

## Step 4: Verify Deployment Sanitization

Before publishing to a server, verify how Duckle strips sensitive cached data:

1. Click the **Run** button in the canvas toolbar to execute the pipeline locally.
2. Notice the live preview rows populated in the **Bottom Panel (Previews)**.
3. Click the **Deploy to Server** button in the toolbar.
4. In the pre-flight deployment dialog, review the **Deployment Manifest**:
   * **Cached preview rows**: Marked `REMOVED (sampleRows stripped)`.
   * **Placeholders**: Confirmed as unresolved `${ENV:...}` strings.
   * **Credential scan**: Verified `0 hardcoded secrets detected`.

---

## Step 5: Test Headless Execution with Environment Injection

Test running the pipeline headlessly without the visual UI, proving that secrets resolve safely at execution time:

```bash
# Export the runtime credentials to your process environment
export WAREHOUSE_HOST="postgres.internal.net"
export WAREHOUSE_USER="etl_user"
export WAREHOUSE_PASS="s3cur3_p@ssw0rd_992!"
export TODAY="$(date +%Y-%m-%d)"

# Execute the pipeline with duckle-runner
duckle-runner run \
  --workspace ~/duckle-secure-workspace \
  --pipeline orders_ingest
```

Inspect the output logs. The runner executes the pipeline, resolves the variables in memory, and writes execution metadata to `run-history/` without persisting the credentials to disk.

---

## Summary

You have successfully:
* Built a parameter-driven pipeline using `${ENV:...}` placeholders.
* Established an automated Dead-Letter Queue to trap and quarantine invalid records.
* Validated that sample preview rows and cleartext credentials do not travel across deployments.
* Executed the pipeline using secure runtime environment injection.
