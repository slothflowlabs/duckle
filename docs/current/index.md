# Duckle User Guide

Welcome to the **Duckle User Guide**! Duckle is an open-source, local-first desktop ETL / ELT studio. It features an intuitive drag-and-drop canvas, a comprehensive properties panel, real-time data previews, and a built-in AI assistant (**Duckie**) that runs entirely on your local machine.

Using Duckle, you can construct visual pipelines to extract, clean, transform, validate, and load data without writing complex SQL scripts or code. Every visual node you place is translated into highly optimized queries behind the scenes, giving you full visibility and speed.

---

## Documentation Navigation

This guide is organized into the following sections to help you get the most out of the Duckle application:

### 1. [Installation & Setup](installation.md)
* How to download and run the Duckle application on **Windows, macOS, and Linux**.
* Running the **Guided Startup Setup** to download database and local AI engines.
* Understanding your **Workspace Folder** structure on disk.

### 2. [Getting Started Guide](getting-started.md)
* Learn how to navigate the **Canvas Interface** and use the **Component Palette**.
* Build your first data pipeline: connecting a CSV file to a Parquet output.
* Open the **Duckie AI Sidebar** to build and update pipelines in plain English.
* Run your pipeline and inspect results in the **Bottom Panel (Previews & SQL Plans)**.

### 3. [Connectors: Sources & Sinks](connectors.md)
* Dragging and dropping source and sink nodes from the Palette.
* Using the **Properties Panel** to map paths, databases, and variables.
* Deep dive into setting up the **CSV/TSV node**, automatic schema scanning, and custom quote/delimiter inputs.
* Summary of available visual connectors for files, databases, object stores, cloud warehouses, and vector databases.

### 4. [Transforms & Data Quality](transforms.md)
* Overview of visual transformation blocks (manipulating columns, filtering rows, aggregating data, and performing lookups).
* Joining tables visually using the interactive **Map Node Editor**.
* Taming messy data with **QA Validators** and routing invalid records to a dedicated **Reject Port**.
* Writing custom scripts directly within **JavaScript, WebAssembly, and SQL UDF nodes** in the properties panel.

### 5. [Execution Controls](engines.md)
* Running pipelines using the **Run** and **Stop** controls.
* Switching between execution backends (DuckDB and SlothDB) in the header.
* How the application pre-installs database extensions so you can work completely offline.

### 6. [Scheduler & Automation](scheduler.md)
* Opening the **Schedule Editor Modal** to trigger pipelines automatically.
* Creating schedules based on **Cron expressions**, **time intervals**, or **File-Watch folders**.
* Tracking execution history, duration, and error reports within the scheduler list.
* (Planned for 1.0) Executing saved pipelines headlessly from the terminal.

### 7. [How the Studio and the Server Fit Together](client-server-architecture.md)
* Diagrams of **what each half owns** and the four requests that pass between them.
* **What is stripped** before a pipeline is sent, and what is not.
* Where every credential is stored, in what form, **including the sharp edges**.
* Why deployed code **cannot start itself**.

### 8. [Running Duckle on a Server](server-deployment.md)
* Standing a server up and **claiming it from the studio**, with no shell session on the box.
* **Deploying a pipeline** from the desktop, and what travels with it.
* Why a deployed schedule always arrives **switched off**.
* Signing in to the web console, and the three roles.

### 9. [CI/CD and External Orchestrators](ci-and-orchestration.md)
* Getting pipelines from a **merge onto a server** with GitHub Actions or GitLab CI.
* Why a deployed schedule always arrives **switched off**, and which role does what.
* Running Duckle from **Airflow, Dagster or Temporal**, and what each route costs you.
* **Prometheus metrics** and the run-history API for dashboards of your own.

### 10. [Desktop Shell & Workspace Git Flow](architecture.md)
* Working with multiple workspace folders.
* Using the built-in **Git Panel** to stage, commit, branch, and push your visual pipeline files.
* Securely managing encrypted connection passwords.
* Interacting with the local AI assistant process panel.

### 11. [Security, Governance & Compliance](../security/index.md)
* Comprehensive compliance suite organized according to the **[Diátaxis Framework](https://diataxis.fr/)**.
* **[Tutorials](../security/index.md#1-tutorials-learning-oriented)**: Hands-on learning for [secure pipeline authoring](../security/tutorials/secure-pipeline-authoring.md) and [SIEM audit log ingestion](../security/tutorials/audit-logging-siem-tutorial.md).
* **[How-To Guides](../security/index.md#2-how-to-guides-task-oriented)**: Operational guides for [secrets management](../security/how-to/configure-secrets-management.md), [vulnerability reporting & patching](../security/how-to/vulnerability-reporting-patching.md), [Vector SIEM streaming](../security/how-to/siem-vector-integration.md), and [incident response runbooks](../security/how-to/incident-handling-runbook.md).
* **[Reference](../security/index.md#3-reference-information-oriented)**: Authoritative technical specifications including the [SOC 2 & ISO 27001 control matrix](../security/reference/soc2-iso27001-control-matrix.md), [audit event schema](../security/reference/audit-event-schema.md), [Okta SSO & MFA architecture spec](../security/reference/sso-okta-architecture-spec.md), and [security CLI commands](../security/reference/security-cli-reference.md).
* **[Explanation](../security/index.md#4-explanation-understanding-oriented)**: Architectural discussions on the [trust boundary and threat model](../security/explanation/trust-boundary-and-threat-model.md), [TDD-driven SSDLC](../security/explanation/ssdlc-and-tdd-philosophy.md), and [AES-256-GCM encryption hierarchy](../security/explanation/encryption-and-key-hierarchy.md).

---

## Core Visual Concepts

When using Duckle, you will primarily work with three visual structures:
* **The Canvas**: A large interactive board where you design pipelines by drawing connector lines between handles on nodes.
* **Nodes (Components)**: Visual blocks representing a source (e.g. CSV), a transformation (e.g. Filter), a QA validator, or a target destination (e.g. database table).
* **Ports & Edges**: Connective pins on nodes. Circles on the left are inputs; circles on the right are outputs. Connector lines (edges) carry the data flow.

