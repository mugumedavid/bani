# Changelog

## v1.0.0

Initial release of Bani, an open-source database migration engine powered by Apache Arrow.

---

### Core Engine

- **Arrow interchange** -- All data flows as `pyarrow.RecordBatch` between source and sink connectors. Zero-copy columnar format for maximum throughput.
- **BDL parser** -- Declarative XML and JSON format for defining migrations with full support for table selections, column mappings, type overrides, filters, hooks, schedules, and incremental sync.
- **Dependency resolution** -- Topological sort of tables based on foreign key relationships. Circular FK chains are detected and deferred.
- **Checkpoint and resumability** -- Migration state is saved after each table. Failed migrations can be resumed with `--resume`, skipping completed tables and retrying failed ones.
- **I/O pipeline overlap** -- Producer/consumer queue overlaps source reads with target writes for maximum throughput.
- **Chunk-level parallelism** -- Large tables (>50k rows with a single integer PK) are split into range-based chunks and transferred concurrently.
- **Memory management** -- `gc.collect()` and Arrow memory pool release between tables to prevent cumulative memory pressure.

### Connectors

- **PostgreSQL** (9.6--17) -- psycopg 3.x driver. COPY binary protocol for writes, streaming cursor for reads, push-down CAST for jsonb/json/uuid, TCP keepalive.
- **MySQL** (5.5--8.4) -- PyMySQL driver. LOAD DATA LOCAL INFILE with executemany fallback, SSCursor streaming, version-specific handling (5.5 key length limits, 8.4 native password).
- **SQL Server** (2019--2022) -- Dual driver: pyodbc preferred (fast_executemany via ODBC array binding, 5-10x faster), pymssql fallback. ODBC driver auto-detection. Resilient FK creation with cascade cycle detection.
- **Oracle** (11g--23c) -- python-oracledb driver. Thin mode (12c+) and thick mode (11g with Oracle Instant Client). `batcherrors` for partial-batch writes, identifier shortening for 30-char limit.
- **SQLite** (3.x) -- stdlib sqlite3 driver. WAL journal mode, 64MB page cache, FK constraints in CREATE TABLE.
- **Bulk schema introspection** -- All 5 connectors use bulk queries (7 queries regardless of table count) instead of N+1 per-table queries.
- **Vectorized writes** -- All data writers use Arrow `to_pylist()` for C-level column extraction.
- **N-mapper type system** -- Each connector has a `from_arrow_type()` method. Source types map to Arrow, Arrow maps to target DDL. N mappers, not NxN.

### CLI

11 commands:

- `bani run` -- Execute a migration with progress tracking, dry run, resume, table filter, parallel workers, and batch size options.
- `bani validate` -- Validate BDL files (schema + semantic validation).
- `bani preview` -- Sample source data before migrating.
- `bani init` -- Interactive wizard to scaffold a BDL project file.
- `bani schema inspect` -- Introspect a live database schema with optional schema/table filters.
- `bani schedule` -- Register migrations with the OS scheduler (crontab).
- `bani connectors list` -- Show all discovered connectors.
- `bani connectors info` -- Detailed information about a specific connector.
- `bani mcp serve` -- Start the MCP server for AI agent integration.
- `bani ui` -- Launch the Web UI (FastAPI + React).
- `bani version` -- Show Bani and connector versions.

All commands support `--output json` for machine-readable output and `--quiet` for silent operation.

### Python SDK

- **`Bani.load(path)`** -- Load BDL files and return a `BaniProject`.
- **`BaniProject.validate()`** -- Validate project configuration.
- **`BaniProject.run()`** -- Execute with progress callbacks, resume, and cancellation support.
- **`BaniProject.preview()`** -- Preview source data.
- **`ProjectBuilder`** -- Fluent builder for constructing migrations programmatically.
- **`SchemaInspector`** -- Introspect live database schemas.

### MCP Server

10 tools for AI agent integration via the Model Context Protocol:

- `bani_connections` -- Discover named database connections.
- `bani_connectors_list` -- List available connector engines.
- `bani_connector_info` -- Get connector details and capabilities.
- `bani_schema_inspect` -- Introspect a live database schema.
- `bani_generate_bdl` -- Generate BDL templates from connections.
- `bani_validate_bdl` -- Validate BDL documents.
- `bani_save_project` -- Save BDL to the projects directory.
- `bani_preview` -- Preview source data.
- `bani_run` -- Execute migrations with progress notifications.
- `bani_status` -- Check checkpoint status.

Security model: env var references only, no plaintext credentials accepted.

### Web UI

- React dashboard with real-time progress tracking via Server-Sent Events (SSE).
- FastAPI backend with routes for projects, connections, connectors, schema inspection, and migration execution.
- Served by `bani ui` command on port 8910.

### Desktop App

- macOS menu bar application for running and monitoring migrations.
- Uses `rumps` for the menu bar and `pyobjc` for macOS integration.

### Docker

- Multi-arch image (`amd64` + `arm64`) based on `python:3.12-slim`.
- All 5 database drivers pre-installed (including ODBC Driver 18 and FreeTDS 1.4.26).
- Pre-built React UI included.
- Non-root `bani` user for security.
- `docker-compose.yml` with PostgreSQL 16, MySQL 8.4, MySQL 5.7, SQL Server 2022, and Oracle 23 Free.

### Packaging

- macOS `.dmg` installer
- Linux `.deb`, `.rpm`, and AppImage
- Windows `.exe` installer
- PyPI package (`pip install bani`)

### CI/CD

- Lint (Ruff), type check (mypy --strict), unit tests, integration tests, release pipeline.
- Conventional Commits enforced.
- Trunk-based development (main + short-lived feature branches).
