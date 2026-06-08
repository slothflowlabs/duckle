# Scheduler & Automation

Duckle includes a visual scheduling panel to automatically run pipelines at specific times, set intervals, or in response to local file modifications.

---

## 1. Using the Schedule Editor

To manage automated triggers for your visual pipelines:

1. Click the **Calendar / Clock icon** in the top toolbar to open the **Schedule Editor Modal**.
2. Click the **"Add Schedule"** button to create a new trigger.
3. Configure the trigger parameters:
   * **Name**: Assign a title to describe this schedule.
   * **Pipeline**: Select which visual canvas file inside your workspace to run.
   * **Enabled Switch**: Toggle the switch to active (green) or inactive (gray).
   * **Trigger Type**: Select between Cron, Interval, or File Watch.

---

## 2. Trigger Type Configurations

You can configure three visual trigger types:

### Cron Trigger
* **Setup**: Enter standard cron string schedules (e.g. `0 2 * * *` to run daily at 2:00 AM).
* **Feedback**: The modal displays a text preview indicating when the next execution will occur.

### Interval Trigger
* **Setup**: Select your frequency value (e.g., `15`) and choose a time unit dropdown (Seconds, Minutes, Hours, Days).
* **Cadence**: Duckle schedules the next execution by adding your frequency value to the completion time of the previous run.

### File Watch Trigger
* **Setup**: Enter an absolute path to a folder or file on your disk (e.g., `/Users/username/data/inbox`).
* **Recursive Checkbox**: Check this box if you want Duckle to watch subdirectories.
* **Debounce Buffer**: When changes are detected, Duckle waits **2 seconds** before triggering. This ensures that large files are fully written by other programs before the pipeline begins processing.

---

## 3. Monitoring Scheduled Runs

The Schedule Editor displays a list of saved automation configurations and their status:

* **Last Run**: Timestamps of the last execution.
* **Duration**: Shows how long the pipeline took to execute in milliseconds.
* **Status Badge**: Displays a green **Success** or red **Failed** badge.
* **Error Logs**: If a run fails, hover over or click the failed status to view the error detail.
* **Next Run**: Displays the calculated timestamp of the next planned run (not applicable to File Watch triggers).

---

## 4. Running Pipelines on a Server (Headless)

The in-app scheduler above runs while Duckle is open. To run a pipeline on a server with no desktop app, use one of two headless paths.

### Build Pipeline (single self-contained file)

**Project tree, right-click a pipeline, then "Build Pipeline"** produces ONE self-contained executable file named after the pipeline (`my_pipeline.exe` on Windows, `my_pipeline` on macOS / Linux). It runs on any matching server with nothing installed: the headless engine, the DuckDB CLI, any DuckDB extensions the pipeline needs, and the resolved pipeline are all embedded inside that single file, which self-extracts to a temp cache on first run.

There is no folder to copy, no `run.sh`, and no separate runner download: the desktop app embeds the headless runner at build time, so the single file is everything you need.

* **Target**: the file is built for the operating system you run the build on (build it on Linux to deploy to a Linux server). Appending the payload makes the file unsigned, so do not codesign / Authenticode-sign it.
* **Context**: pick a context at build time; its non-secret variables are baked into the pipeline.
* **Secrets**: choose how credentials travel:
  * **Environment**: secret values are replaced with `${ENV:KEY}` placeholders. Set the environment variables on the server, or place a `secrets.env` (KEY=VALUE lines) next to the file before running. Nothing sensitive is written into the file.
  * **Passphrase**: secrets are encrypted inside the file. The runner decrypts them at run time from the `DUCKLE_BUNDLE_PASSPHRASE` environment variable.

Run it:

```bash
./my_pipeline                 # or my_pipeline.exe on Windows
```

### Headless runner (against an existing workspace)

The same headless runner can also execute a single already-built pipeline JSON directly from a workspace, resolving the context the same way the app does:

```bash
duckle-runner --pipeline "/path/to/pipeline.json" [--workspace "/path/to/workspace"] [--duckdb "/path/to/duckdb"]
```

There is no `run` subcommand: pass the pipeline with `--pipeline` (or as a bare positional path). It exits `0` on success and non-zero on failure, and writes the same NDJSON run logs under `logs/` for Splunk / Dynatrace ingestion.

### Scheduling on the server

Point your operating system's scheduler at the single file directly:

```cron
# Linux cron - run the file every day at 02:00
0 2 * * * /opt/duckle/my_pipeline >> /var/log/my_pipeline.log 2>&1
```

```ini
# Linux systemd timer (my_pipeline.service + my_pipeline.timer)
[Service]
Type=oneshot
ExecStart=/opt/duckle/my_pipeline
```

On Windows, create a **Task Scheduler** task that runs `my_pipeline.exe`; on macOS, a **launchd** plist that runs the file. For Environment secret mode, set the secret env vars (or drop a `secrets.env` beside the file); for Passphrase mode, set `DUCKLE_BUNDLE_PASSPHRASE`.
