# CI/CD and External Orchestrators

Two related questions, answered separately because they have different answers:

1. **"We use GitHub Actions. How do pipelines get from a merge onto a server?"** That works today, and there are templates to copy.
2. **"We already run Airflow / Dagster / Temporal. Can they run Duckle pipelines?"** Yes, through a documented CLI and HTTP surface. There are no provider packages yet, and this page is honest about what you would be writing yourself.

---

## Part 1: From a merge to a server

Duckle pipelines are plain JSON files, so they live in your repository and travel through the same review you already use. Two jobs cover it.

### The two jobs

| Job | When | Touches your data? | Needs a credential? |
| --- | --- | --- | --- |
| **Validate** | every commit, every fork | No | No |
| **Deploy** | merges to your default branch only | No | Yes, an `admin` key |

**Validate** compiles every pipeline to SQL. It opens no source, writes no sink, and needs no DuckDB binary, no credentials and no network, which is why it is safe to run on pull requests from forks.

**Deploy** installs those pipelines onto a running Duckle server.

### Templates to copy

| File | For |
| --- | --- |
| [`docs/ci/github-actions.yml`](../ci/github-actions.yml) | validate only |
| [`docs/ci/github-actions-deploy.yml`](../ci/github-actions-deploy.yml) | validate on PRs, deploy on merge |
| [`docs/ci/gitlab-ci.yml`](../ci/gitlab-ci.yml) | validate only |
| [`docs/ci/gitlab-ci-deploy.yml`](../ci/gitlab-ci-deploy.yml) | validate, then deploy |

### Step 1. Mint a key for the robot

On the server:

```bash
duckle-runner console key-add github-actions --role admin --expires-days 90
```

It prints the key **once** and stores only a hash. Deploying needs `admin`, because a deployed pipeline runs shell and SQL on that host.

> Keys use the base64url alphabet, so they can contain `-` and `_`. If you pipe one through a script, do not assume it is alphanumeric.

### Step 2. Store two secrets

| Secret | Value |
| --- | --- |
| `DUCKLE_URL` | `https://duckle.internal` (no trailing slash) |
| `DUCKLE_KEY` | the key from step 1 |

On GitLab, mark `DUCKLE_KEY` **masked and protected** so it is only exposed to protected branches.

### Step 3. Copy the template in

That is the whole setup. The deploy job posts each pipeline to `/api/deploy`:

```bash
curl --fail-with-body -sS -X POST "$DUCKLE_URL/api/deploy" \
  -H "Authorization: Bearer $DUCKLE_KEY" \
  -H "Content-Type: application/json" \
  -d "$(jq -c --arg n "$name" '{name: $n, pipeline: .}' "$f")"
```

A success looks like this:

```json
{"deployed":"orders_etl","replaced":false,"schedule":{"saved":true,"enabled":false}}
```

`replaced` tells you whether this overwrote an existing pipeline of that name.

### What the design buys you

**A deployed schedule always arrives switched off.** Send a schedule with a pipeline and the server forces `enabled: false`. A cadence that someone merged cannot start firing the moment it lands; turning it on is a separate act by a person.

**Shipping code and starting it need different roles.** Deploying is `admin`; enabling a schedule is `operator`. So a CI key can ship without being able to start anything:

| Role | `/api/run` | `/api/deploy` |
| --- | --- | --- |
| `viewer` | 403 | 403 |
| `operator` | 200 | 403 |
| `admin` | 200 | 200 |

A refusal explains itself: a mis-scoped key gets `{"error":"this needs the admin role; you have viewer"}`, which is why the templates use `curl --fail-with-body`.

**A deploy is atomic.** The server writes through a temporary file and renames it into place, so a scheduler tick on the far end can never read half a pipeline.

### What the templates deliberately do not do

* **Turn schedules on.** See above.
* **Run your pipelines.** A green deploy means the file is installed, not that it works against production data.
* **Roll back.** The previous version is whatever is in git; redeploy the earlier commit.

---

## Part 2: Driving Duckle from an orchestrator

**There are no Airflow, Dagster, Temporal or Prefect provider packages today.** If you want a `DuckleRunOperator`, you would be writing it. The good news is that it is thin, because the two surfaces underneath are stable and documented.

### The two ways in

**A. The command line** - for a `BashOperator`, a `KubernetesPodOperator`, or a Temporal activity.

```bash
duckle-runner --pipeline pipelines/orders_etl.json
```

The binary is a static musl executable with no shared-library dependencies, so it runs on any base image with no packages installed. Exit codes are stable and documented:

| Code | Means |
| --- | --- |
| `0` | success |
| `1` | the work ran and reported failure. A real finding. |
| `2` | the runner could not start the work: bad usage, unreadable file, missing engine. Not a finding about your data. |

That `1` versus `2` split is the useful part: a task can retry on `2` and alert on `1`.

**B. The HTTP API** - for an `HttpOperator` or a Dagster resource, against a running console.

```bash
curl -X POST "$DUCKLE_URL/api/run" \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"file":"orders_etl.json"}'
```

`file` is a path inside the server's workspace, and is required - the canonical form is `pipelines/<id>.json`. `params` is optional and supplies values for `${...}` placeholders. Needs the `operator` role.

> ### Read this before writing an operator
>
> **A failed pipeline still answers `HTTP 200`.** The failure is only in the body:
>
> ```json
> {"id":"orders_etl","status":"error","durationMs":41,
>  "error":"DuckDB engine isn't installed yet. Open Setup to install it.","nodes":{}}
> ```
>
> An operator that trusts the HTTP status alone - `resp.raise_for_status()` and nothing
> else - will mark failed loads as **successful**, and nobody will find out until the
> downstream numbers are wrong.
>
> **Check `status` yourself.** It is one of `"ok"`, `"error"` or `"cancelled"`; treat
> anything that is not `"ok"` as a failure.
>
> A non-2xx status means the run could not be *started* at all: `400` for a missing or
> unreadable file, `401`/`403` for a credential problem. Both cases need handling, and
> they are not the same case.

The response has exactly five keys - `id`, `status`, `durationMs`, `error`, `nodes` -
and `error` is present as `null` on success rather than omitted.

### Four things that will shape your operator

**It is fully synchronous.** The connection stays open for the whole run and returns when
it finishes. There is no run id and nothing to poll, so any client, proxy or load-balancer
timeout in front of it must be longer than your slowest pipeline, or you will lose the
result of a run that actually succeeded.

**One run at a time, by default.** The console serialises runs; raise it with
`DUCKLE_MAX_CONCURRENT_RUNS`. Two tasks firing together queue rather than fail, and the
wait is unbounded - so that client timeout matters here too.

**This route does not take the cross-process run lock.** The scheduler does, but a manual
`/api/run` does not, so a run triggered here can overlap a run of the same pipeline
started from a desktop app on the same workspace. If two things might trigger one
pipeline, let one of them own it.

**An empty string parameter is dropped, not sent.** `{"params":{"month":""}}` falls
through to the workspace default rather than overriding it with blank. If `params` is not
a JSON object it is ignored silently.

### Choosing between them

This is the part that decides your design, and it is not obvious:

| | CLI (`--pipeline`) | HTTP (`/api/run`) |
| --- | --- | --- |
| Needs a running server | No | Yes |
| Records run history | **No** | Yes |
| Updates the metrics file | **No** | Yes |
| Visible in the console | **No** | Yes |
| Alerts fire | **No** | Yes |

**A headless CLI run is invisible to Duckle's own observability.** It writes no run history, so it never appears in the console's Runs tab, never updates the Prometheus textfile, and never raises an alert. That is fine if your orchestrator is your source of truth for what ran; it is not fine if you expected the console to show it.

If you want your orchestrator to own the schedule *and* Duckle to keep its own history, use the HTTP route against a console.

### A note on overlap

If Airflow owns the schedule, Duckle's own scheduler and Plans are redundant, and you should leave them switched off rather than run both. Two schedulers pointed at one workspace will not corrupt anything - every run takes a lock on its pipeline, and the second is refused rather than doubled - but you will have two places that believe they decide when things run, and only one of them will be right.

---

## Part 3: Monitoring

### Prometheus / OpenMetrics

Every recorded run rewrites a textfile at:

```text
<workspace>/logs/duckle_metrics.prom
```

It is written atomically, so a scrape never reads a half-written file. Point node_exporter's textfile collector or Grafana Alloy at it; there is no HTTP server and no agent inside Duckle, which keeps headless and air-gapped deployments covered.

| Metric | Type | Meaning |
| --- | --- | --- |
| `duckle_run_last_status` | gauge | 1 when the most recent run succeeded, 0 when it failed or was cancelled |
| `duckle_run_last_unchanged` | gauge | 1 when the most recent run checked its sources, found nothing changed and wrote nothing. Such a run IS a success, so `duckle_run_last_status` is 1 for it too - this is what separates a poll that is working and finding nothing from one that is ingesting. |
| `duckle_run_last_duration_seconds` | gauge | how long the most recent run took |
| `duckle_run_last_rows` | gauge | rows the most recent run wrote |
| `duckle_run_last_timestamp_seconds` | gauge | when the most recent run finished |
| `duckle_runs_window` | gauge | how many runs are in the retained window |

> **These are windowed, not lifetime, counters.** All series are derived from the retained run history, which is a rolling window per pipeline, so `duckle_runs_window` is a count of what is retained rather than everything that has ever run. The metric names say so deliberately.

> It is written when a run is **recorded**, which means console runs, scheduled runs and desktop runs. A headless `duckle-runner --pipeline` run does not update it. See the table above.

### The API

For a dashboard of your own, the console exposes `GET /api/runs`, `GET /api/schedules`, `GET /api/summary` and `GET /api/catalog`, all with the `viewer` role. That is the right role for a monitoring integration: it can read everything and start nothing.

---

## What we would build next, and in what order

If you want native integrations, this is the honest ranking by effort against reach:

1. **A GitHub Actions deploy template.** Done - it is in `docs/ci/`.
2. **An Airflow provider** (`DuckleRunOperator`). A few hundred lines of Python over the CLI or the HTTP API, publishable to PyPI alongside the existing `pip install duckle`. Biggest installed base.
3. **A Dagster resource.** Same shape, smaller audience.
4. **A Temporal activity.** Thinnest of the three, because Temporal expects you to write the activity anyway.

None of these need changes inside Duckle. The reason they are cheap is that the CLI has stable exit codes and the HTTP API already exists, which was the hard part.

---

## Next steps

* [Running Duckle on a Server](server-deployment.md) - standing a server up and connecting the studio to it
* [Scheduler & Automation](scheduler.md) - Duckle's own scheduler, if you are not bringing one
* Full cloud recipes: <https://duckle.org/deploy.html>
