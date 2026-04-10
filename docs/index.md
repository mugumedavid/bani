# Bani

**An open-source database migration engine powered by Apache Arrow.**

[![PyPI](https://img.shields.io/pypi/v/bani)](https://pypi.org/project/bani/)
[![CI](https://img.shields.io/github/actions/workflow/status/mugumedavid/bani/ci.yml?branch=main)](https://github.com/mugumedavid/bani/actions)
[![License](https://img.shields.io/github/license/mugumedavid/bani)](https://github.com/mugumedavid/bani/blob/main/LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/bani)](https://pypi.org/project/bani/)

Bani moves data between databases using Apache Arrow as the in-memory interchange format. Define your migration in BDL (Bani Definition Language), then run it from the CLI, Python SDK, MCP server, or Web UI.

---

## Feature Highlights

- **5 Connectors** -- PostgreSQL, MySQL, SQL Server, Oracle, and SQLite, each with source and sink support.
- **Arrow Engine** -- All data flows as `pyarrow.RecordBatch` between connectors. N type mappers, not N*N.
- **BDL (Bani Definition Language)** -- Declarative XML or JSON format for defining migrations with table selections, column mappings, type overrides, hooks, schedules, and incremental sync.
- **CLI** -- 11 commands covering `run`, `validate`, `preview`, `init`, `schema inspect`, `schedule`, `connectors`, `mcp`, `ui`, and `version`.
- **Python SDK** -- `ProjectBuilder` for fluent project construction, `Bani.load()` for file-based loading, `SchemaInspector` for live introspection.
- **MCP Server** -- 10 tools that let AI agents discover connections, generate BDL, validate, preview, and execute migrations.
- **Web UI** -- React dashboard with real-time progress tracking via SSE, backed by a FastAPI server.
- **Desktop App** -- macOS menu bar application for running and monitoring migrations.
- **Docker** -- Multi-arch container image with all 5 database drivers pre-installed.

---

## Quick Start

```bash
pip install bani

# Scaffold a new migration project
bani init --source mysql --target postgresql

# Validate the generated BDL file
bani validate migration.bdl

# Run the migration
bani run migration.bdl
```

See the [Getting Started](getting-started.md) guide for a complete walkthrough.

---

## Documentation

| Section | Description |
|---|---|
| [Getting Started](getting-started.md) | Install, configure, and run your first migration |
| [BDL Reference](guides/bdl-reference.md) | Full specification of the Bani Definition Language |
| [CLI Reference](guides/cli-reference.md) | All commands, flags, and output formats |
| [Python SDK](guides/python-sdk.md) | Build and run migrations programmatically |
| [MCP Server](guides/mcp-server.md) | AI agent integration via Model Context Protocol |
| [Docker](guides/docker.md) | Container-based deployment |
| [Connectors](connectors/index.md) | Per-database setup, type mappings, and performance notes |
| [Architecture](developer/architecture.md) | Hexagonal design, Arrow interchange, connector discovery |
| [Contributing](developer/contributing.md) | Development setup, quality gates, PR process |
| [API Reference](api/index.md) | Auto-generated from source docstrings |
| [Changelog](changelog.md) | Release history |

---

## License

Bani is licensed under the [Apache License 2.0](https://github.com/mugumedavid/bani/blob/main/LICENSE).
