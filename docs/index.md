# Bani

**An open-source database migration engine powered by Apache Arrow.**

[![CI](https://img.shields.io/github/actions/workflow/status/mugumedavid/bani/ci.yml?branch=main&label=CI)](https://github.com/mugumedavid/bani/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/mugumedavid/bani/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://github.com/mugumedavid/bani)

Bani moves data between databases using Apache Arrow as the in-memory interchange format. Define your migration in BDL (Bani Definition Language), then run it from the CLI, Python SDK, MCP server, or Web UI.

---

## Feature Highlights

- **5 Connectors** -- PostgreSQL, MySQL, SQL Server, Oracle, and SQLite, each with source and sink support.
- **Arrow Engine** -- All data flows as `pyarrow.RecordBatch` between connectors. N type mappers, not N*N.
- **BDL (Bani Definition Language)** -- Declarative XML or JSON format for defining migrations with table selections, column mappings, type overrides, hooks, schedules, and incremental sync.
- **Web UI** -- React dashboard for building migrations visually, monitoring progress in real-time, and browsing schemas -- no command line required.
- **Cross-Platform Installers** -- Native installers for macOS (.dmg with menu bar app), Windows (.exe), and Linux (.deb, .rpm, AppImage). Download, install, and start migrating.
- **Docker** -- Multi-arch container image with all 5 database drivers pre-installed. `docker pull banilabs/bani:latest` and go.
- **AI Agent Integration** -- MCP server with 10 tools that let AI agents (Claude, Cursor, etc.) discover connections, generate BDL, validate, preview, and execute migrations.
- **CLI** -- 11 commands covering `run`, `validate`, `preview`, `init`, `schema inspect`, `schedule`, `connectors`, `mcp`, `ui`, and `version`.
- **Python SDK** -- `ProjectBuilder` for fluent project construction, `Bani.load()` for file-based loading, `SchemaInspector` for live introspection.

---

## Quick Start

### Download and install

Bani runs on macOS, Windows, Linux, and Docker. Download the installer for your platform from the [releases page](https://github.com/mugumedavid/bani/releases), or pull the Docker image:

```bash
docker pull banilabs/bani:latest
```

### Launch and use the Web UI

On macOS and Windows, Bani runs as a background app with a menu bar or system tray icon. After installing, the browser opens automatically with the Web UI. From there you can:

1. **Add connections** -- point Bani at your source and target databases.
2. **Browse schemas** -- explore tables, columns, and relationships visually.
3. **Build a migration** -- select tables, configure column mappings and type overrides.
4. **Run and monitor** -- execute the migration and watch progress in real-time.

Close the browser and Bani keeps running in the background. Click the tray/menu bar icon to reopen it. No command line needed.

On Linux and Docker, run `bani ui` to start the server and open the browser.

### Or use the CLI

```bash
# Scaffold a new migration project
bani init --source mysql --target postgresql

# Validate the generated BDL file
bani validate migration.bdl

# Run the migration
bani run migration.bdl
```

### Or let an AI agent do it

Connect Bani's MCP server to Claude, Cursor, or any MCP-compatible AI agent. The agent can inspect schemas, generate migration definitions, and run them -- all through natural language.

See the [Getting Started](getting-started.md) guide for a complete walkthrough.

---

## Documentation

| Section | Description |
|---|---|
| [Getting Started](getting-started.md) | Install, configure, and run your first migration |
| [Web UI Guide](guides/web-ui.md) | Dashboard, connections, schema browser, projects, migration monitor |
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
