# CLI Reference

The `bani` command-line interface provides 11 commands for managing database migrations. All commands support global options for output format, verbosity, and logging.

---

## Global Options

These options are available on every command:

| Option | Short | Default | Description |
|---|---|---|---|
| `--output` | `-o` | `human` | Output format: `human` (Rich-formatted) or `json` (JSON lines). |
| `--quiet` | `-q` | `false` | Suppress progress output. |
| `--log-level` | -- | `info` | Logging level: `debug`, `info`, `warn`, `error`. |
| `--version` | -- | -- | Print version and exit. |

```bash
# JSON output for scripting
bani -o json run migration.bdl

# Quiet mode
bani -q run migration.bdl

# Debug logging
bani --log-level debug run migration.bdl
```

---

## `bani run`

Execute a database migration from a BDL project file.

```
bani run <project_file> [OPTIONS]
```

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `project_file` | `str` | Required | Path to the BDL project file (XML or JSON). |
| `--dry-run` | `bool` | `false` | Validate and plan but do not execute. |
| `--tables` | `str` | All | Comma-separated table names to migrate. |
| `--parallel` | `int` | `1` | Number of parallel workers. |
| `--batch-size` | `int` | `100000` | Rows per batch. |
| `--resume` | `bool` | `false` | Resume a previously failed migration from the last checkpoint. |

**Examples:**

```bash
# Full migration
bani run migration.bdl

# Dry run (validate only)
bani run migration.bdl --dry-run

# Migrate specific tables
bani run migration.bdl --tables "public.users,public.orders"

# Resume after failure
bani run migration.bdl --resume

# Parallel with custom batch size
bani run migration.bdl --parallel 8 --batch-size 50000

# JSON output for CI/CD pipelines
bani -o json run migration.bdl
```

The `--resume` flag uses checkpoint files to skip tables that completed successfully in a previous run. Tables that were in-progress or failed are dropped and re-transferred.

---

## `bani validate`

Validate a BDL project file without executing.

```
bani validate <project_file>
```

| Argument | Type | Description |
|---|---|---|
| `project_file` | `str` | Path to the BDL file. |

Performs both schema validation (XSD/JSON Schema) and semantic validation (parsing). Returns exit code 0 on success, 1 on failure.

```bash
bani validate migration.bdl
```

JSON output includes error codes:

```bash
bani -o json validate migration.bdl
# {"command": "validate", "status": "ok", "errors": [], "warnings": [], "schema_version": "1.0"}
```

---

## `bani preview`

Preview source data by sampling rows from each table.

```
bani preview <project_file> [OPTIONS]
```

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `project_file` | `str` | Required | Path to the BDL file. |
| `--sample-size` | `int` | `10` | Number of rows to sample per table. |

```bash
# Preview with 5 rows per table
bani preview migration.bdl --sample-size 5
```

Displays a Rich table for each source table showing column names, types, and sample data. Useful for verifying connectivity and inspecting data before a full migration.

---

## `bani init`

Create a new BDL project file via an interactive wizard.

```
bani init [OPTIONS]
```

| Option | Short | Default | Description |
|---|---|---|---|
| `--source` | -- | (prompt) | Source connector name (e.g. `postgresql`, `mysql`). |
| `--target` | -- | (prompt) | Target connector name. |
| `--out` | `-f` | `migration.bdl` | Output file path. |

```bash
# Interactive wizard
bani init

# Non-interactive with flags
bani init --source mysql --target postgresql --out my-project.bdl
```

The wizard prompts for host, port, database, and credential environment variable names for both source and target connections. Default ports are pre-filled based on the connector (PostgreSQL: 5432, MySQL: 3306, MSSQL: 1433, Oracle: 1521).

---

## `bani schema inspect`

Introspect a live database and display its schema.

```
bani schema inspect [OPTIONS]
```

| Option | Type | Default | Description |
|---|---|---|---|
| `--connector` | `str` | Required | Connector name (e.g. `postgresql`, `mysql`). |
| `--host` | `str` | `""` | Database host. |
| `--port` | `int` | `0` | Database port (auto-detected from connector if 0). |
| `--database` | `str` | Required | Database name. |
| `--username-env` | `str` | `""` | Environment variable for username. |
| `--password-env` | `str` | `""` | Environment variable for password. |
| `--schema` | `str` | All | Filter by schema name. |
| `--table` | `str` | All | Filter by table name. |

```bash
# Inspect a PostgreSQL database
bani schema inspect \
  --connector postgresql \
  --host localhost \
  --port 5432 \
  --database mydb \
  --username-env PG_USER \
  --password-env PG_PASS

# Filter to a specific table
bani schema inspect \
  --connector mysql \
  --host localhost \
  --database erp \
  --username-env MYSQL_USER \
  --password-env MYSQL_PASS \
  --table customers
```

Displays tables, columns (name, type, nullable, auto-increment), indexes, primary keys, and foreign keys.

---

## `bani schedule`

Register a migration with the OS scheduler (crontab on Linux/macOS).

```
bani schedule <project_file> [OPTIONS]
```

| Argument / Option | Type | Default | Description |
|---|---|---|---|
| `project_file` | `str` | Required | Path to the BDL file. |
| `--cron` | `str` | Required | Cron expression (e.g. `"0 2 * * *"`). |
| `--timezone` | `str` | `UTC` | IANA timezone. |

```bash
# Schedule a nightly migration at 2 AM UTC
bani schedule migration.bdl --cron "0 2 * * *"

# Schedule with timezone
bani schedule migration.bdl --cron "0 2 * * *" --timezone "America/New_York"
```

---

## `bani connectors list`

Show all discovered connectors.

```
bani connectors list
```

Displays a table with connector name, type (source, sink, or source+sink), version, and driver version. Connectors are discovered via Python entry points.

```bash
bani connectors list
# ┌────────────┬──────────────┬─────────┬──────────┐
# │ Name       │ Type         │ Version │ Driver   │
# ├────────────┼──────────────┼─────────┼──────────┤
# │ mssql      │ source+sink  │ 1.0.0   │ unknown  │
# │ mysql      │ source+sink  │ 1.0.0   │ unknown  │
# │ oracle     │ source+sink  │ 1.0.0   │ unknown  │
# │ postgresql │ source+sink  │ 1.0.0   │ unknown  │
# │ sqlite     │ source+sink  │ 1.0.0   │ unknown  │
# └────────────┴──────────────┴─────────┴──────────┘
```

---

## `bani connectors info`

Show detailed information about a specific connector.

```
bani connectors info <name>
```

| Argument | Type | Description |
|---|---|---|
| `name` | `str` | Connector name (e.g. `postgresql`). |

```bash
bani connectors info postgresql
```

---

## `bani mcp serve`

Start Bani as an MCP (Model Context Protocol) server.

```
bani mcp serve [OPTIONS]
```

| Option | Type | Default | Description |
|---|---|---|---|
| `--transport` | `str` | `stdio` | Transport protocol: `stdio` or `sse`. |
| `--port` | `int` | `8080` | Port for SSE transport (ignored for stdio). |

```bash
# Start MCP server over stdio (for Claude Desktop)
bani mcp serve

# Start with SSE transport (future)
bani mcp serve --transport sse --port 8080
```

See the [MCP Server](mcp-server.md) guide for Claude Desktop configuration.

---

## `bani ui`

Launch the Bani Web UI (FastAPI backend + React SPA).

```
bani ui [OPTIONS]
```

| Option | Type | Default | Description |
|---|---|---|---|
| `--host` | `str` | `127.0.0.1` | Bind address. Use `0.0.0.0` for all interfaces. |
| `--port` | `int` | `8910` | Listen port. |
| `--projects-dir` | `str` | `~/.bani/projects` | Directory for BDL project files. |

```bash
# Start Web UI on default port
bani ui

# Expose to network
bani ui --host 0.0.0.0 --port 9000
```

Open `http://localhost:8910` in your browser to access the dashboard.

---

## `bani version`

Show Bani version and installed connector versions.

```
bani version
```

Displays the Bani core version and a table of all installed connectors with their versions and driver versions.

```bash
bani version
# bani 0.1.0
# ┌────────────┬─────────┬──────────┐
# │ Connector  │ Version │ Driver   │
# ├────────────┼─────────┼──────────┤
# │ mysql      │ 1.0.0   │ unknown  │
# │ postgresql │ 1.0.0   │ unknown  │
# │ ...        │ ...     │ ...      │
# └────────────┴─────────┴──────────┘
```

---

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Error (validation failure, connection error, migration failure) |

All commands return exit code 1 on failure. With `-o json`, error details are included in the JSON output for programmatic consumption.
