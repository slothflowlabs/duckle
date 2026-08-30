# How to Configure Secrets Management

This guide provides step-by-step instructions for managing encrypted credentials, securing workspace cryptographic keys, and safely passing production secrets into Duckle runtime environments.

---

## 1. Protecting the Workspace Key Directory

When you save database or cloud storage connection credentials in Duckle Studio, Duckle encrypts them with AES-256-GCM and stores the master key under `<workspace>/.duckle/keys/`.

> [!WARNING]
> If an attacker copies the `.duckle/keys/` directory along with your `connections/` directory, they can decrypt your stored credentials. Restrict filesystem permissions immediately upon workspace creation.

### Setting Permissions on Linux / macOS
```bash
# Restrict workspace access strictly to the owner user
chmod 700 /path/to/workspace
chmod 700 /path/to/workspace/.duckle
chmod 600 /path/to/workspace/.duckle/keys/*
```

### Setting Permissions on Windows (PowerShell)
```powershell
$workspace = "C:\path\to\workspace"

# Disable inheritance and grant full control only to the current user
icacls $workspace /inheritance:r /grant:r "$($env:USERNAME):(OI)(CI)F"
icacls "$workspace\.duckle\keys" /inheritance:r /grant:r "$($env:USERNAME):(OI)(CI)F"
```

### Ensure Git Ignore Rules are Active
Verify your workspace `.gitignore` file includes internal key stores:
```gitignore
# Duckle cryptographic keys and local cache
.duckle/keys/
.duckle/secrets/
.env
```

---

## 2. Using Runtime Environment Variables for Production

Do not store production database passwords or IAM secrets in static connection profiles. Use `${ENV:VARIABLE_NAME}` interpolation in your node properties.

### Step 1: Define Environment Placeholders in Duckle Studio
In any connector configuration field (e.g. Snowflake Sink, PostgreSQL Source, S3 Bucket), format sensitive fields as:
```text
${ENV:PROD_DB_PASSWORD}
${ENV:AWS_SECRET_ACCESS_KEY}
```

### Step 2: Inject Secrets at Runtime

#### In Docker / Container Deployments
Pass secrets directly from your secret manager (e.g., HashiCorp Vault, AWS Secrets Manager, Doppler):
```bash
docker run -d \
  --name duckle-runner \
  -p 8095:8095 \
  -v /var/duckle/workspace:/workspace \
  -e PROD_DB_PASSWORD="vault-injected-secret-value" \
  -e AWS_SECRET_ACCESS_KEY="vault-injected-aws-key" \
  slothflowlabs/duckle-runner:latest \
  serve --workspace /workspace --host 0.0.0.0 --port 8095
```

#### In Kubernetes via `Secret` Resources
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: duckle-runner
spec:
  template:
    spec:
      containers:
      - name: duckle-runner
        image: slothflowlabs/duckle-runner:latest
        command: ["duckle-runner", "serve", "--workspace", "/workspace", "--host", "0.0.0.0", "--port", "8095"]
        env:
        - name: PROD_DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: etl-production-secrets
              key: database-password
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: etl-production-secrets
              key: aws-secret-key
```

#### In Systemd Services
```ini
[Unit]
Description=Duckle Runner ETL Daemon
After=network.target

[Service]
Type=simple
User=duckle
Group=duckle
WorkingDirectory=/var/duckle/workspace
EnvironmentFile=/etc/duckle/secrets.env
ExecStart=/usr/local/bin/duckle-runner serve --workspace /var/duckle/workspace --host 127.0.0.1 --port 8095
Restart=on-failure
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/duckle/workspace

[Install]
WantedBy=multi-user.target
```

Ensure `/etc/duckle/secrets.env` has `chmod 600` permissions and is owned by `duckle:duckle`.

---

## 3. Fetching Secrets From an External Vault

Environment injection still writes the plaintext into the process environment.
Where a credential must never reach a workspace, an environment variable or an
image layer, Duckle can fetch it from the vault at run time instead and hold it
only for the duration of that run.

Write the placeholder in the node property:

```text
${VAULT:PROD_DB_PASSWORD}
```

and tell the host how to ask the vault for it:

```bash
export DUCKLE_VAULT_COMMAND="CLIPasswordSDK GetPassword -p Query=Object={name} -o Password"
```

Duckle replaces `{name}` with the object name from the placeholder, runs the
command, and uses whatever it prints on stdout (trailing newline trimmed) as
the secret.

### Worked examples

CyberArk AIM / Credential Provider:
```bash
export DUCKLE_VAULT_COMMAND="CLIPasswordSDK GetPassword -p AppDescs.AppID=Duckle -p Query=Safe=DataEng;Object={name} -o Password"
```

CyberArk Conjur:
```bash
export DUCKLE_VAULT_COMMAND="conjur variable get -i {name}"
```

HashiCorp Vault:
```bash
export DUCKLE_VAULT_COMMAND="vault kv get -field=value secret/duckle/{name}"
```

AWS Secrets Manager:
```bash
export DUCKLE_VAULT_COMMAND="aws secretsmanager get-secret-value --secret-id {name} --query SecretString --output text"
```

The template is split on whitespace, so a single argument cannot contain a
space. Where a vault call needs one, put the call in a small wrapper script and
point `DUCKLE_VAULT_COMMAND` at the script.

### Why the command is host configuration, never pipeline content

`DUCKLE_VAULT_COMMAND` is read from the host environment only. A pipeline
supplies the object name and nothing else. If a pipeline could name its own
command, authoring a pipeline would amount to shell access on the runner, and
authoring is not meant to carry that privilege.

The template is split into arguments and executed directly, without a shell, so
a name cannot append a second command. A name containing a control character is
refused rather than passed into the argument list.

### Behaviour to plan for

| Situation | What happens |
| --- | --- |
| `DUCKLE_VAULT_COMMAND` is unset | The placeholder is left verbatim, so a pipeline cannot silently change meaning on a host that never opted in. |
| The command fails, or prints nothing | The placeholder is left verbatim, and the run fails where the credential is used, naming it. |
| Several nodes use the same name | Fetched once per run and cached in memory for that run only. |
| The command writes to stderr | Not included in the error text, because a vault client often echoes the query and the query names the secret. |

This pass runs on every execution surface: Duckle Studio, the scheduler,
`duckle-runner run`, `serve`, `follow`, and the MCP server. Because it walks
every property of every node, it applies to all connectors rather than to a
maintained list of them.

---

## 4. Rotating Credentials

When rotating a password or API key:

1. **Vault Placeholders**: Rotate the secret in the vault. Nothing on the Duckle side changes, and no restart is required, because `${VAULT:...}` is resolved fresh on every run.
2. **Environment Variables**: Update the secret in your secret store / CI/CD pipeline and restart the `duckle-runner` process. No pipeline edits or re-deployments are required because the placeholder `${ENV:...}` remains unchanged.
3. **Saved Connections**:
   - Open Duckle Studio → **Connection Manager** (Key icon).
   - Select the connection profile, click **Edit**, input the updated secret, and click **Save**.
   - Duckle re-encrypts the secret using the workspace AES-256-GCM key.
