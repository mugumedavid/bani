# Configuration

Bani loads configuration from multiple sources with a defined priority order. This page documents all configuration options, environment variables, and file formats.

---

## Configuration Priority

Settings are resolved in this order (highest priority first):

1. **Environment variables** (`BANI_*`)
2. **BDL project options** (`<options>` element)
3. **User config file** (`~/.config/bani/config.toml`)
4. **Built-in defaults**

A setting from a higher-priority source always overrides the same setting from a lower-priority source.

---

## Environment Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `BANI_BATCH_SIZE` | `int` | `100000` | Rows per Arrow RecordBatch. |
| `BANI_PARALLEL_WORKERS` | `int` | `4` | Number of tables transferred concurrently. |
| `BANI_MEMORY_LIMIT_MB` | `int` | `2048` | Soft memory cap in MB. |
| `BANI_LOG_LEVEL` | `str` | `INFO` | Logging level: `DEBUG`, `INFO`, `WARN`, `ERROR`. |
| `BANI_LOG_FORMAT` | `str` | `json` | Log format: `json` or `text`. |
| `BANI_DISABLE_PYODBC` | `str` | (unset) | If set, disables pyodbc and forces pymssql for MSSQL. |

```bash
export BANI_BATCH_SIZE=50000
export BANI_PARALLEL_WORKERS=8
export BANI_LOG_LEVEL=DEBUG
```

---

## Config File

The user config file is a TOML file at `~/.config/bani/config.toml`:

```toml
batch_size = 100000
parallel_workers = 4
memory_limit_mb = 2048
log_level = "INFO"
log_format = "json"
```

All keys correspond to the environment variables above (without the `BANI_` prefix, using snake_case).

If the file does not exist or cannot be parsed, Bani silently falls back to built-in defaults.

---

## Named Connections

Bani supports a connections registry at `~/.bani/connections.json` for defining named database connections that can be referenced by key in the MCP server and Web UI.

### File Format

```json
{
  "prod-mysql": {
    "name": "Production MySQL",
    "connector": "mysql",
    "host": "db.example.com",
    "port": 3306,
    "database": "erp",
    "username": "${env:PROD_MYSQL_USER}",
    "password": "${env:PROD_MYSQL_PASS}",
    "options": {
      "charset": "utf8mb4"
    }
  },
  "analytics-pg": {
    "name": "Analytics PostgreSQL",
    "connector": "postgresql",
    "host": "pg.example.com",
    "port": 5432,
    "database": "analytics",
    "username": "${env:PG_USER}",
    "password": "${env:PG_PASS}"
  },
  "local-mssql": {
    "name": "Local SQL Server",
    "connector": "mssql",
    "host": "localhost",
    "port": 1433,
    "database": "TestDB",
    "username": "sa",
    "password": "MyPassword123!"
  }
}
```

### Connection Fields

| Field | Required | Description |
|---|---|---|
| `name` | No | Human-readable display name. Defaults to the key. |
| `connector` | Yes | Connector name: `mysql`, `postgresql`, `mssql`, `oracle`, `sqlite`. |
| `host` | No | Database hostname. |
| `port` | No | Database port. |
| `database` | No | Database name (or file path for SQLite). |
| `username` | No | Username or `${env:VAR_NAME}` reference. |
| `password` | No | Password or `${env:VAR_NAME}` reference. |
| `options` | No | Connector-specific key-value pairs (e.g. `service_name` for Oracle). |

### Credential Handling

Credentials support two formats:

- **`${env:VAR_NAME}`** -- Resolved at runtime from the process environment. Recommended for production.
- **Literal values** -- Stored in the file. The file should have `0600` permissions.

When literal values are used, Bani injects them into `os.environ` under deterministic keys (e.g. `_BANI_CONN_prod-mysql_USER`) so the connector layer can use its standard env-var resolution.

### Using Named Connections

**MCP server:**

```json
{"tool": "bani_schema_inspect", "params": {"connection": "prod-mysql"}}
{"tool": "bani_generate_bdl", "params": {"source_connection": "prod-mysql", "target_connection": "analytics-pg"}}
```

**Web UI:** Connections appear in the dashboard dropdown menus.

---

## BDL Project Options

The `<options>` element in BDL files provides per-project settings that override the config file defaults:

```xml
<options>
  <batchSize>100000</batchSize>
  <parallelWorkers>4</parallelWorkers>
  <memoryLimitMB>2048</memoryLimitMB>
  <onError>log-and-continue</onError>
  <createTargetSchema>true</createTargetSchema>
  <dropTargetTablesFirst>false</dropTargetTablesFirst>
  <transferIndexes>true</transferIndexes>
  <transferForeignKeys>true</transferForeignKeys>
  <transferDefaults>true</transferDefaults>
  <transferCheckConstraints>true</transferCheckConstraints>
</options>
```

See the [BDL Reference](bdl-reference.md#options) for details on each option.

---

## CLI Overrides

Some settings can be overridden on the command line, which takes the highest priority:

```bash
bani run migration.bdl --batch-size 50000 --parallel 8
```

The priority chain becomes: CLI flags > env vars > BDL options > config file > defaults.

---

## Projects Directory

Bani stores project files, checkpoints, and run logs under `~/.bani/projects/` by default. This can be changed via:

- CLI: `bani ui --projects-dir /path/to/projects`
- BDL: Not configurable (always resolves to `~/.bani/projects/` for MCP and Web UI)

The directory structure:

```
~/.bani/
├── connections.json          # Named connections registry
└── projects/
    ├── my-migration.bdl      # Saved BDL projects
    ├── my-migration.checkpoint.json  # Checkpoint state
    └── my-migration.log      # Run log
```
