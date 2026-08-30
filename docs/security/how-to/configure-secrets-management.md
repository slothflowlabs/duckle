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

## 3. Rotating Credentials

When rotating a password or API key:

1. **Environment Variables**: Update the secret in your secret store / CI/CD pipeline and restart the `duckle-runner` process. No pipeline edits or re-deployments are required because the placeholder `${ENV:...}` remains unchanged.
2. **Saved Connections**:
   - Open Duckle Studio → **Connection Manager** (Key icon).
   - Select the connection profile, click **Edit**, input the updated secret, and click **Save**.
   - Duckle re-encrypts the secret using the workspace AES-256-GCM key.
