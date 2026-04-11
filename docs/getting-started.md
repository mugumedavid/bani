# Getting Started

This guide walks you through installing Bani and running your first database migration. By the end you will have migrated tables from one database to another.

---

## Installation

"pip / uv"

```bash
# Using pip
pip install bani

# Using uv (recommended for faster installs)
uv pip install bani
```

    Optional extras:

```bash
# Enhanced SQLite support (apsw driver)
pip install "bani[sqlite-extras]"

# macOS desktop app dependencies
pip install "bani[macos-app]"
```

"Docker"

```bash
docker pull bani/bani:latest

# Verify the installation
docker run --rm bani/bani:latest version
```

"Desktop / Server Installers"

Download the installer for your platform from the [releases page](https://github.com/mugumedavid/bani/releases):

| Platform | Format | Install |
|---|---|---|
| **macOS** | `.dmg` | Open the DMG, drag Bani to Applications. A menu bar app provides quick access to the Web UI and CLI. |
| **Windows** | `.exe` | Run the installer and follow the prompts. Bani is added to your PATH automatically. |
| **Debian / Ubuntu** | `.deb` | `sudo dpkg -i bani-*.deb` |
| **RHEL / Fedora** | `.rpm` | `sudo rpm -i bani-*.rpm` |
| **Linux (any)** | `.AppImage` | `chmod +x Bani-*.AppImage && ./Bani-*.AppImage` |

Once installed, run `bani ui` to launch the Web UI, or use the CLI directly.

---

## Prerequisites

You need access to a source and target database. Bani reads credentials from environment variables -- never hardcode passwords in BDL files.

Set up your credentials:

```bash
# Source database (e.g. MySQL)
export SRC_DB_USER=myuser
export SRC_DB_PASS=mypassword

# Target database (e.g. PostgreSQL)
export TGT_DB_USER=pguser
export TGT_DB_PASS=pgpassword
```

---

## Step 1: Scaffold a Project

Use `bani init` to generate a BDL project file with an interactive wizard:

```bash
bani init --source mysql --target postgresql
```

This creates a `migration.bdl` file with connection placeholders. You can also specify an output path:

```bash
bani init --source mysql --target postgresql --out my-project.bdl
```

---

## Step 2: Edit the BDL File

Open the generated file and fill in your connection details. Here is a complete but simple example:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="my-first-migration"
           description="Migrate ERP tables from MySQL to PostgreSQL" />

  <source connector="mysql">
    <connection host="localhost"
                port="3306"
                database="erp"
                username="${env:SRC_DB_USER}"
                password="${env:SRC_DB_PASS}" />
  </source>

  <target connector="postgresql">
    <connection host="localhost"
                port="5432"
                database="analytics"
                username="${env:TGT_DB_USER}"
                password="${env:TGT_DB_PASS}" />
  </target>

</bani>
```

!!! tip "Credential security"
    Credentials use `${env:VAR_NAME}` syntax. Bani resolves these from the process environment at runtime. Though it is supported, never embed plaintext passwords in BDL files.

---

## Step 3: Validate

Check that the BDL file is well-formed and the configuration is valid:

```bash
bani validate migration.bdl
```

A successful validation prints a green checkmark. If there are errors, Bani prints each one with a diagnostic code.

---

## Step 4: Preview (Optional)

Before running a full migration, preview the source data:

```bash
bani preview migration.bdl --sample-size 5
```

This connects to the source database and displays sample rows from each table, helping you verify connectivity and inspect the data before committing to a full transfer.

---

## Step 5: Run the Migration

Execute the migration:

```bash
bani run migration.bdl
```

Bani will:

1. Parse and validate the BDL file
2. Connect to source and target databases
3. Introspect the source schema
4. Resolve table dependencies (foreign keys)
5. Create tables in the target database
6. Transfer data in batches as Arrow RecordBatches
7. Create indexes and foreign keys on the target
8. Report results

!!! note "Dry run"
    Use `--dry-run` to validate without executing: `bani run migration.bdl --dry-run`

---

## SDK Alternative

You can build and run migrations entirely in Python:

```python
from bani.sdk.project_builder import ProjectBuilder

project = (
    ProjectBuilder("my-migration")
    .source(
        dialect="mysql",
        host="localhost",
        port=3306,
        database="erp",
        username_env="SRC_DB_USER",
        password_env="SRC_DB_PASS",
    )
    .target(
        dialect="postgresql",
        host="localhost",
        port=5432,
        database="analytics",
        username_env="TGT_DB_USER",
        password_env="TGT_DB_PASS",
    )
    .batch_size(100_000)
    .parallel_workers(4)
    .build()
)

from bani.sdk.bani import Bani, BaniProject

bani_project = BaniProject(project)
result = bani_project.run()

print(f"Completed: {result.tables_completed} tables, "
      f"{result.total_rows_written:,} rows in {result.duration_seconds:.1f}s")
```

See the [Python SDK](guides/python-sdk.md) guide for the full API.

---

## Next Steps

- [BDL Reference](guides/bdl-reference.md) -- Learn every element and attribute
- [CLI Reference](guides/cli-reference.md) -- Explore all 11 commands
- [Connectors](connectors/index.md) -- Database-specific setup and type mappings
- [Incremental Sync](guides/incremental-sync.md) -- Set up ongoing delta replication
- [MCP Server](guides/mcp-server.md) -- Let AI agents drive migrations
