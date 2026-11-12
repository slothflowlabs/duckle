# Connectors: Sources & Sinks

Duckle provides 216 visual connectors, 202 of them available today, to read data from (Sources) and write data to (Sinks) files, databases, object storage, cloud warehouses, and API streams.

---

## 1. Visual Properties Panel

Every connector node you drop onto the canvas has a custom configuration form inside the **Properties Panel** on the right side of the screen.

* **Text Fields & Browsers**: Click the folder icon next to file paths to select local files.
* **Toggles & Dropdowns**: Switch write modes (overwrite, append, upsert) or file compression formats (ZSTD, GZIP).
* **Connections Dropdown**: Instead of entering passwords or API keys directly into a node, select a pre-saved credential profile.

---

## 2. Using the Connection Manager

To avoid entering sensitive passwords or credentials repeatedly, manage them centrally:

1. Click the **Key icon** in the top toolbar to open the **Connection Manager Modal**.
2. Click **"New Connection"** and choose a template (e.g., *PostgreSQL*, *AWS S3*, *Snowflake*).
3. Type in your host details, usernames, and passwords.
4. Click **"Save"**. Duckle automatically encrypts your passwords before writing them to your workspace folder.
5. In your canvas, select a node (e.g., a *PostgreSQL Sink*), click the **Connection** dropdown in the Properties Panel, and select your saved connection.

---

## 3. CSV/TSV Node Configuration

The CSV source node is the most common starting block for visual pipelines. Selecting the CSV node displays these configuration settings:

| GUI Input | Property Field | Purpose |
| :--- | :--- | :--- |
| **Path** | `path` | File location on disk. Supports context variables (e.g., `${MY_DATA_DIR}/orders.csv`). |
| **Has Header** | `hasHeader` | Check this box if the first line holds column names. Unchecking it assigns names like `col_1`, `col_2` automatically. |
| **Delimiter** | `delimiter` | The text separator. Common values are `,` (CSV), `;`, or `\t` (for tab-separated TSV files). |
| **Quote Char** | `quoteChar` | The text boundaries character (usually `"`). Leave empty to disable quote formatting. |
| **Encoding** | `encoding` | Choose the file's encoding standard (e.g., UTF-8, Latin-1, or Windows-1252). |
| **Skip Lines** | `skipLines` | Enter a number of lines to ignore at the top of the file before reading headers (useful for report logs). |
| **Null Value** | `nullValue` | Define text strings that represent missing values (such as `NA`, `N/A`, or `NULL`). |

* **"Autodetect schema" Button**: Click this button after setting your file path. The node reads a sample of the file, infers columns, and populates the **Schema** tab.
* **Visual Lineage**: Click the **Preview** tab in the bottom panel to inspect rows, ensuring column formatting matches your expectations.

---

## 4. Visual Connector Catalog

Connectors are grouped into folders inside the Left Sidebar Palette:

### File Types
* **Delimited & Structured**: CSV, TSV, Parquet, JSON, JSONL / NDJSON, Excel (.xlsx), YAML, TOML, XML, Fixed-Width, and Apache Avro.
* **Geospatial files**: GeoJSON, Shapefile, GeoPackage, KML, GPX, and GML.

### Databases & Warehouses
* **Databases**: PostgreSQL, MySQL, MariaDB, CockroachDB, SQL Server, Oracle, and ClickHouse.
* **Lakehouse tables**: Apache Iceberg, Delta Lake, and DuckLake.
* **Cloud warehouses**: MotherDuck, Snowflake, BigQuery, Redshift, and Databricks.

### Object Storage & Web
* **Cloud Storage**: Amazon S3, Google Cloud Storage, Azure Blob, HTTP(S), MinIO, Cloudflare R2, and Backblaze B2.
* **FTP & Webhooks**: FTP/FTPS, IMAP (mailboxes), and Webhook Listeners (binds a local port to accept incoming JSON payloads).

### Streaming & NoSQL
* **Streaming**: Apache Kafka, Redpanda, NATS JetStream, GCP Pub/Sub, RabbitMQ, and AWS Kinesis.
* **NoSQL**: MongoDB, Redis, Cassandra, Elasticsearch, and DynamoDB.

### Vector Databases
* pgvector, Pinecone, Qdrant, Weaviate, Milvus, Chroma, and LanceDB.
