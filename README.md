# Bani

[![CI](https://github.com/mugumedavid/bani/actions/workflows/ci.yml/badge.svg)](https://github.com/mugumedavid/bani/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://github.com/mugumedavid/bani)
[![Docs](https://img.shields.io/badge/docs-docs.bani.tools-teal)](https://docs.bani.tools)

**An open-source database migration engine powered by Apache Arrow.**

Bani migrates schema, data, and indexes across relational databases using Apache Arrow as a universal columnar interchange format. Define migrations declaratively with BDL, programmatically with the Python SDK, or let an AI agent drive them via the MCP server.

## Features

- **5 Database Connectors** -- PostgreSQL, MySQL, MSSQL, Oracle, SQLite
- **Apache Arrow Engine** -- Columnar interchange for high-throughput batch transfers
- **Declarative BDL** -- Define migrations in XML or JSON, version control everything
- **Python SDK** -- Fluent `ProjectBuilder` API for programmatic migrations
- **CLI** -- 11 commands: run, validate, preview, init, schema inspect, and more
- **MCP Server** -- 10 tools for AI agents (Claude, Cursor, etc.)
- **Web Dashboard** -- Real-time migration monitoring with React UI
- **Cross-Platform** -- macOS app, Linux packages, Windows installer, Docker

## Quick Start

### Install

```bash
# With pip
pip install bani

# With uv
uv pip install bani

# With Docker
docker pull banilabs/bani:latest
```

### Your First Migration

1. Set up database credentials as environment variables:

```bash
export SOURCE_USER=myuser
export SOURCE_PASS=mypassword
export TARGET_USER=pguser
export TARGET_PASS=pgpassword
```

2. Create a migration project:

```bash
bani init --source mysql --target postgresql --out my-migration.bdl
```

3. Run the migration:

```bash
bani run my-migration.bdl
```

### Python SDK

```python
from bani.sdk import BaniProject, ProjectBuilder

project = (
    ProjectBuilder("my-migration")
    .source("mysql", host="localhost", port=3306, database="source_db",
            username_env="SOURCE_USER", password_env="SOURCE_PASS")
    .target("postgresql", host="localhost", port=5432, database="target_db",
            username_env="TARGET_USER", password_env="TARGET_PASS")
    .batch_size(100_000)
    .build()
)

result = BaniProject(project).run()
print(f"Migrated {result.total_rows_written} rows in {result.duration_seconds:.1f}s")
```

Or load from a BDL file:

```python
from bani.sdk import Bani

result = Bani.load("my-migration.bdl").run()
```

### MCP Server (AI Agent Integration)

Add to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "bani": {
      "command": "bani",
      "args": ["mcp", "start"]
    }
  }
}
```

Then ask Claude: *"Inspect the schema of my MySQL database and generate a migration to PostgreSQL."*

## Supported Databases

| Database | Versions | Source | Sink | Driver |
|----------|----------|--------|------|--------|
| PostgreSQL | 9.6 -- 17 | Yes | Yes | psycopg 3.x |
| MySQL | 5.5 -- 8.4 | Yes | Yes | PyMySQL |
| SQL Server | 2019 -- 2022 | Yes | Yes | pyodbc / pymssql |
| Oracle | 11g -- 23c | Yes | Yes | oracledb |
| SQLite | 3.x | Yes | Yes | sqlite3 (stdlib) |

## Architecture

Bani uses Apache Arrow `RecordBatch` as its universal data interchange format. Source connectors read database rows into Arrow batches; sink connectors write Arrow batches to the target database. This gives N type mappers (one per connector) instead of N x N, and enables high-throughput columnar transfers with minimal Python overhead.

```
Source DB --> Source Connector --> Arrow RecordBatch --> Sink Connector --> Target DB
```

Key components:

- **Connectors** -- Pluggable source/sink pairs discovered via Python entry points
- **Orchestrator** -- Manages table ordering (dependency-aware), batching, parallelism, and checkpointing
- **BDL Parser** -- Reads XML or JSON migration definitions into a `ProjectModel`
- **SDK** -- `ProjectBuilder` for programmatic construction, `SchemaInspector` for introspection
- **MCP Server** -- Exposes migration tools to AI agents via the Model Context Protocol

## Web UI

Launch the web dashboard:

```bash
bani ui
```

Monitor migrations in real-time with progress tracking, table-level status, and error reporting.

## Documentation

Full documentation is available at [docs.bani.tools](https://docs.bani.tools):

- [Getting Started](https://docs.bani.tools/en/latest/getting-started/) -- Install and run your first migration in under 10 minutes
- [BDL Reference](https://docs.bani.tools/en/latest/guides/bdl-reference/) -- Complete specification for the Bani Definition Language
- [CLI Reference](https://docs.bani.tools/en/latest/guides/cli-reference/) -- All commands, flags, and output formats
- [Python SDK](https://docs.bani.tools/en/latest/guides/python-sdk/) -- Programmatic migration API
- [MCP Server](https://docs.bani.tools/en/latest/guides/mcp-server/) -- AI agent integration guide
- [Connector Reference](https://docs.bani.tools/en/latest/connectors/) -- Per-database configuration and type mappings

## Development

```bash
# Clone and install
git clone https://github.com/mugumedavid/bani.git
cd bani
uv sync --all-extras --dev

# Run quality gates
ruff check && ruff format --check
mypy --strict
pytest

# Or use make
make all
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide.

## License

Apache-2.0. See [LICENSE](LICENSE) for details.

## Links

- [Website](https://bani.tools) -- Project homepage
- [Documentation](https://docs.bani.tools) -- Technical docs and guides
- [GitHub](https://github.com/mugumedavid/bani) -- Source code and issues
- [Docker Hub](https://hub.docker.com/r/banilabs/bani) -- Container images
- [Discord](https://discord.gg/ffVYuNQz) -- Community chat
