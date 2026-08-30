# Encryption & Key Hierarchy

This document explains the cryptographic standards, key management architecture, and data-at-rest protection mechanisms implemented in Duckle.

---

## 1. Cryptographic Algorithms

Duckle uses modern, industry-standard cryptography to protect sensitive assets:

| Data Category | Cryptographic Algorithm | Key Length | Notes |
| :--- | :--- | :--- | :--- |
| **Saved Connection Passwords** | AES-256-GCM (Galois/Counter Mode) | 256 bits | Authenticated encryption with associated data (AEAD) providing confidentiality and integrity. |
| **Server API Token Storage** | SHA-256 / PBKDF2 | 256 bits | Irreversible one-way cryptographic hashing. Cleartext tokens are never stored on disk. |
| **Git PAT Storage** | AES-256-GCM | 256 bits | Stored in encrypted form under `.duckle/secrets/`. |
| **In-Transit Communication** | TLS 1.3 / HTTPS | 256 bits | Terminated at edge reverse proxy (Nginx/Traefik/Cloud Ingress). |

---

## 2. Key Derivation & Hierarchy

```text
┌────────────────────────────────────────────────────────┐
│               Workspace Key Directory                  │
│                <workspace>/.duckle/keys/               │
│                                                        │
│  ┌──────────────────────────────────────────────────┐  │
│  │   Master Workspace Key (256-bit AES-GCM Key)     │  │
│  └─────────────────────────┬────────────────────────┘  │
└────────────────────────────┼───────────────────────────┘
                             │
       ┌─────────────────────┴─────────────────────┐
       ▼                                           ▼
┌───────────────────────────────┐   ┌───────────────────────────────┐
│ Connection Secrets            │   │ Git Access Tokens             │
│ (<workspace>/connections/*.json) │   │ (<workspace>/.duckle/secrets) │
└───────────────────────────────┘   └───────────────────────────────┘
```

### Encryption Process (AES-256-GCM)
1. **Random IV Generation**: For every encrypted payload, Duckle generates a cryptographically secure, unique 96-bit Initialization Vector (IV/nonce) using the OS secure random generator (`ring::rand` or `rand_core`).
2. **Authenticated Ciphertext**: The plaintext is encrypted using the 256-bit workspace key. The resulting payload includes the 96-bit nonce and the 128-bit GCM authentication tag.
3. **Storage**: The payload is stored in serialized Base64/JSON format within the connection profile.

---

## 3. Sharp Edges: What Is Encrypted vs. Plaintext

It is critical to understand the exact scope of encryption in Duckle:

```text
  ENCRYPTED ON DISK                       STORED IN PLAINTEXT
  ────────────────────────────────────    ────────────────────────────────────
  ✔ Saved connection passwords            ✖ Context variables (contexts/*.json)
  ✔ Server API key hashes                 ✖ Duckie AI provider API key
  ✔ Git personal access tokens (PAT)      ✖ Literal strings in canvas node fields
                                          ✖ Pipeline topology (nodes & edges)
```

### Why `${ENV:VAR}` Is Required for Pipelines
* While saved connections in the Connection Manager are encrypted on disk, inserting a database password directly as a string property inside a canvas node causes it to be stored as literal JSON text in `<workspace>/pipelines/<id>.json`.
* **Best Practice**: Always use `${ENV:NAME}` for sensitive pipeline values. The pipeline stores only the placeholder text, and Duckle dynamically resolves the secret from memory at run time.

---

## 4. Memory Hygiene

* Secrets decrypted in-process for database connections are kept in memory only for the duration of the execution task.
* Secrets are scrubbed from serialized logs, UI preview payloads, and execution traces.
