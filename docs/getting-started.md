# Getting Started

This guide walks you through installing Bani and running your first database migration. By the end you will have migrated tables from one database to another.

---

## Installation

### Desktop / Server Installers (recommended)

Download the installer for your platform from the [releases page](https://github.com/mugumedavid/bani/releases):

| Platform | Format | Install |
|---|---|---|
| **macOS** | `.dmg` | Open the DMG, drag Bani to Applications. A menu bar app provides quick access to the Web UI and CLI. |
| **Windows** | `.exe` | Run the installer and follow the prompts. A tray icon and desktop shortcut are created automatically. |
| **Debian / Ubuntu** | `.deb` | `sudo dpkg -i bani-*.deb` |
| **RHEL / Fedora** | `.rpm` | `sudo rpm -i bani-*.rpm` |
| **Linux (any)** | `.AppImage` | `chmod +x Bani-*.AppImage && ./Bani-*.AppImage` |

These installers bundle Python, all 5 database drivers, and the Web UI. Nothing else to install.

### Docker

For containerised or CI environments:

```bash
docker pull banilabs/bani:latest

# Verify the installation
docker run --rm banilabs/bani:latest version

# Launch the Web UI
docker run -p 8910:8910 banilabs/bani:latest bani ui --host 0.0.0.0
```

### pip install (Python 3.11+)

If you have Python installed and prefer pip:

```bash
# Create a virtual environment (Python 3.11 minimum)
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install bani-tools
bani ui   # opens the Web UI in your browser
```

This gives you the `bani` CLI with all commands including the Web UI:

```bash
bani ui                  # Launch the Web UI in your browser
bani run migration.bdl   # Run a migration from a BDL file
bani mcp serve           # Start the MCP server for AI agents
bani --help              # See all 11 commands
```

!!! note "What pip install does NOT include"
    The native tray/menu bar app (macOS "B" icon, Windows system tray) is only available in the platform installers. With pip, you launch the Web UI manually with `bani ui` each time. The Web UI itself is identical -- it's only the always-running background launcher that differs.

!!! warning "System dependencies for some connectors"
    PostgreSQL, MySQL, Oracle, and SQLite work out of the box with pip. SQL Server requires additional system libraries (`unixodbc-dev` and FreeTDS on Linux, or `brew install unixodbc freetds` on macOS). The platform installers and Docker image bundle all system dependencies -- use those if you need MSSQL support without manual setup.

---

## Launching Bani

After installation, Bani runs as a background app with a tray or menu bar icon. The Web UI opens automatically in your browser.

| Platform | What happens after install |
|---|---|
| **macOS** | A **"B" icon** appears in the menu bar. Click it to open the Web UI. The server runs in the background -- close the browser and it keeps running. |
| **Windows** | A **tray icon** appears in the system tray (bottom-right). Right-click to open the Web UI. A "Bani" desktop shortcut is also created. The server runs in the background. |
| **Linux** | Run `bani ui` from a terminal. The browser opens automatically. |
| **Docker** | Run `docker run -p 8910:8910 banilabs/bani:latest bani ui --host 0.0.0.0`. The container logs print the auth token and a ready-to-click URL with the token embedded -- copy it from the logs and open it in your browser. |
| **pip install** | Run `bani ui` from a terminal. The browser opens automatically with the auth token in the URL. |

On macOS and Windows, the experience is the same: install, the tray/menu bar icon appears, the browser opens, and you are ready to build your first migration. No terminal needed.

See the [Web UI Guide](guides/web-ui.md) for a detailed walkthrough of every screen.

---

## Prerequisites

You need access to a source and target database. The Web UI lets you enter credentials directly when adding connections.

---

## Your First Migration (Web UI)

The Web UI is the easiest way to set up and run a migration. No command line or configuration files needed.

### Step 1: Add your database connections

From the dashboard, go to **Connections** and add your source and target databases. For each connection, provide:

- **Name** -- a label for this connection (e.g. "Production MySQL", "Analytics PG")
- **Connector** -- the database type (PostgreSQL, MySQL, SQL Server, Oracle, or SQLite)
- **Host, Port, Database** -- your database server details
- **Username and Password** -- credentials for the database

Click **Test Connection** to verify connectivity before saving.

### Step 2: Browse the source schema

Go to **Schema Browser** and select your source connection. Bani connects to the database and displays all schemas, tables, columns, indexes, and foreign keys. Use this to understand what you are migrating and verify that the connection is working.

### Step 3: Create a migration project

Go to **Projects** and click **New Project**. Then:

1. **Name** your project (e.g. "ERP to Analytics")
2. **Select source** -- choose the source connection you added
3. **Select target** -- choose the target connection
4. **Choose tables** -- select which tables to migrate, or leave blank to migrate all
5. **Configure options** -- Leave as is or set batch size, parallelism, error handling, and any column or type overrides
6. **Save** the project

Bani generates a BDL (Bani Definition Language) file behind the scenes. You can view and edit the raw BDL from the project editor if needed.

### Step 4: Run and monitor

Click **Run Migration** on your project. The **Migration Monitor** shows real-time progress:

- Overall progress bar with rows read and written
- Per-table status (pending, in progress, completed, failed)
- Elapsed time and estimated completion
- Warnings and errors as they occur

Once complete, the results summary shows total tables migrated, rows transferred, and any issues encountered.

### Step 5: Review history

The **Run History** page keeps a log of all past migrations with their results, so you can track what was migrated and when.

---

## Alternative: CLI

For scripting, automation, or headless environments, you can use the CLI:

```bash
# Scaffold a migration project
bani init --source mysql --target postgresql --out migration.bdl

# Edit migration.bdl with your connection details and table selections

# Validate the project
bani validate migration.bdl

# Preview source data (optional)
bani preview migration.bdl --sample-size 5

# Run the migration
bani run migration.bdl
```

See the [CLI Reference](guides/cli-reference.md) for all 11 commands.

---

## Alternative: Python SDK

For programmatic control, use the Python SDK:

```python
from bani.sdk.project_builder import ProjectBuilder
from bani.sdk.bani import BaniProject

project = (
    ProjectBuilder("my-migration")
    .source("mysql", host="localhost", port=3306, database="erp",
            username_env="SRC_DB_USER", password_env="SRC_DB_PASS")
    .target("postgresql", host="localhost", port=5432, database="analytics",
            username_env="TGT_DB_USER", password_env="TGT_DB_PASS")
    .batch_size(100_000)
    .build()
)

result = BaniProject(project).run()
print(f"{result.tables_completed} tables, {result.total_rows_written:,} rows")
```

See the [Python SDK](guides/python-sdk.md) guide for the full API.

---

## Alternative: AI Agent

Connect Bani to Claude, Cursor, or any MCP-compatible AI agent and let it drive the migration through natural language. See the [MCP Server](guides/mcp-server.md) guide.

---

## Next Steps

- [BDL Reference](guides/bdl-reference.md) -- Learn every element and attribute
- [CLI Reference](guides/cli-reference.md) -- Explore all 11 commands
- [Connectors](connectors/index.md) -- Database-specific setup and type mappings
- [Incremental Sync](guides/incremental-sync.md) -- Set up ongoing delta replication
- [MCP Server](guides/mcp-server.md) -- Let AI agents drive migrations
