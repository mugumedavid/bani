# Web UI Guide

Bani includes a full Web UI for building and running migrations without touching the command line. On macOS and Windows, the UI runs as a background app with a tray/menu bar icon. On Linux and Docker, you start it with `bani ui`.

---

## Launching Bani

The experience is the same across platforms -- install, launch, and a browser window opens with the Bani dashboard.

| Platform | How it works |
|---|---|
| **macOS** | After installing the `.dmg`, Bani appears as a **"B" icon in the menu bar**. Click it to open the Web UI, copy the auth token, or open a terminal. The server runs in the background -- close the browser and Bani keeps running. Click "Quit Bani" in the menu bar to stop it. |
| **Windows** | After running the `.exe` installer, Bani appears as a **tray icon in the system tray** (bottom-right of the taskbar). Right-click it to open the Web UI, copy the token, or open a terminal. The server runs in the background -- close the browser and Bani keeps running. Click "Quit Bani" in the tray menu to stop it. A desktop shortcut is also created. |
| **Linux** | Run `bani ui` from a terminal. The browser opens automatically. The server runs as long as the terminal is open. |
| **Docker** | Run `docker run -p 8910:8910 banilabs/bani:latest bani ui --host 0.0.0.0`. Open `http://localhost:8910` in your browser. |

On macOS and Windows, Bani starts automatically after installation and opens the browser. On subsequent launches, click the tray/menu bar icon or the desktop shortcut.

!!! tip "Background operation"
    On macOS and Windows, the Bani server runs in the background even when the browser is closed. Scheduled migrations, if configured, will continue to run. To fully stop Bani, use the "Quit Bani" option in the tray/menu bar menu.

---

## Dashboard

The dashboard is the home page of the Web UI. It provides an overview of your Bani instance:

- **Recent migrations** -- quick access to the latest migration runs and their status.
- **Connections** -- how many database connections are configured.
- **Projects** -- how many migration projects exist.

From here, navigate to any section using the sidebar.

---

## Connections

The **Connections** page lets you save database connections that can be reused across projects.

### Adding a connection

1. Click **Add Connection**.
2. Fill in the details:
    - **Name** -- a label you will recognise (e.g. "Production MySQL", "Staging PG").
    - **Connector** -- the database type: PostgreSQL, MySQL, SQL Server, Oracle, or SQLite.
    - **Host** -- database server address (e.g. `localhost`, `db.example.com`).
    - **Port** -- database port (auto-filled with the default for the selected connector).
    - **Database** -- the database or service name.
    - **Username** and **Password** -- database credentials.
3. Click **Test Connection** to verify connectivity.
4. Click **Save**.

Saved connections are stored locally in `~/.bani/connections.json`. They are available to the Web UI, CLI, and MCP server.

!!! note "MCP integration"
    Connections saved here are automatically available to AI agents via the MCP server. If you save a connection called "production-mysql", an AI agent can reference it by name when inspecting schemas or running migrations.

### Editing and deleting

Click on any saved connection to edit its details or delete it. Changes take effect immediately.

---

## Schema Browser

The **Schema Browser** lets you explore the structure of any connected database.

1. Select a saved connection from the dropdown.
2. Bani connects to the database and loads the schema.
3. Browse the tree of **schemas > tables > columns**.

For each table, you can see:

- Column names, data types, and nullability
- Primary key columns
- Indexes (unique and non-unique)
- Foreign key relationships
- Estimated row count

Use the Schema Browser to understand your source database before building a migration.

---

## Projects

A **project** is a migration definition -- it specifies the source, target, tables, and configuration. Projects are stored as BDL (Bani Definition Language) files.

### Creating a project

1. Go to **Projects** and click **New Project**.
2. In the visual editor:
    - **Name** your project.
    - **Select source** -- choose a saved connection.
    - **Select target** -- choose a saved connection.
    - **Choose tables** -- select which tables to migrate, or leave blank to migrate all.
    - **Configure options** -- batch size, parallel workers, error handling, type overrides.
3. Click **Save**.

### Visual editor vs source editor

The project editor has two tabs:

- **Visual** -- a form-based editor for building migrations without writing XML.
- **Source** -- the raw BDL XML. Any changes in the visual editor are reflected here, and vice versa.

You can switch between the two at any time. Power users may prefer editing the BDL directly for advanced features like column mappings, filters, hooks, and schedules.

### Managing projects

- Projects are saved to `~/.bani/projects/` as `.bdl` files.
- Click any project to edit it.
- Run a project directly from the project list.

---

## Running a Migration

### Starting a migration

From the project list or project editor, click **Run Migration**. Bani will:

1. Validate the project configuration.
2. Connect to the source and target databases.
3. Introspect the source schema.
4. Resolve table dependencies (foreign keys determine creation order).
5. Create tables in the target database.
6. Transfer data in batches as Apache Arrow RecordBatches.
7. Create indexes and foreign keys on the target.

### Migration Monitor

The **Migration Monitor** shows real-time progress while a migration is running:

- **Overall progress** -- total rows read and written, elapsed time.
- **Per-table status** -- each table shows its state: pending, in progress, completed, or failed.
- **Rows read vs written** -- highlighted in yellow if they differ (indicating skipped or failed rows).
- **Warnings** -- schema translation warnings, skipped indexes, renamed identifiers.
- **Errors** -- any failures with details.

The monitor updates in real-time via Server-Sent Events (SSE). You can close the browser and come back -- the migration continues running in the background, and the monitor will show the current state when you return.

### Dry run

Use the **Dry Run** option to validate the migration without actually transferring data. This checks connectivity, validates the BDL, and reports any schema translation issues.

---

## Run History

The **Run History** page shows all past migration runs with:

- Project name
- Start time and duration
- Tables completed and failed
- Rows read and written
- Status (completed, failed, cancelled)

Use this to track what was migrated and when, and to diagnose any failures.

---

## Settings

The **Settings** page shows server configuration:

- Server address and port
- Auth token (for API access)
- Projects directory path

---

## Authentication

The Web UI uses an auth token for security. By default, a random token is generated each time Bani starts and is included in the browser URL automatically.

If you need the token (e.g. for API access), use:

- **macOS**: Click "Copy Token" in the menu bar.
- **Windows**: Right-click the tray icon and click "Copy Token".
- **CLI**: The token is printed when `bani ui` starts.

### Persistent token

By default, the token changes every time Bani restarts. To use a fixed token that survives restarts, set the `BANI_AUTH_TOKEN` environment variable before starting Bani:

```bash
# Linux / macOS
export BANI_AUTH_TOKEN="your-stable-token-here"
bani ui

# Windows (cmd)
set BANI_AUTH_TOKEN=your-stable-token-here
bani ui

# Windows (PowerShell)
$env:BANI_AUTH_TOKEN = "your-stable-token-here"
bani ui
```

When this variable is set, Bani uses it instead of generating a random token. This is useful if you run Bani on a server and need the token to remain stable across restarts.

---

## Single-user design

Bani is designed for single-user use. Only one migration can run at a time -- if a migration is already running, starting another will be rejected until the current one completes.

Multiple browser tabs can connect to the same Bani instance to view status and browse schemas, but the migration engine is not designed for concurrent multi-user access. If you need shared access for a team, consider running separate Bani instances per user, or wait for Bani Cloud which will support multi-user workflows.
