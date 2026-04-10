# Docker

Bani provides a multi-arch Docker image with all 5 database drivers pre-installed, including ODBC Driver 18 for SQL Server and FreeTDS 1.4.

---

## Quick Start

```bash
# Pull the image
docker pull bani/bani:latest

# Show help
docker run --rm bani/bani:latest --help

# Show version
docker run --rm bani/bani:latest version
```

---

## Running a Migration

Mount your BDL file and pass database credentials as environment variables:

```bash
docker run --rm \
  -v $(pwd)/migration.bdl:/home/bani/migration.bdl:ro \
  -e SRC_DB_USER=myuser \
  -e SRC_DB_PASS=mypassword \
  -e TGT_DB_USER=pguser \
  -e TGT_DB_PASS=pgpassword \
  bani/bani:latest \
  run /home/bani/migration.bdl
```

!!! tip
    Use `--network host` if your databases are running on the Docker host machine, or use the appropriate Docker network to reach containerized databases.

---

## Running the Web UI

```bash
docker run --rm -p 8910:8910 \
  -v $(pwd)/projects:/home/bani/.bani/projects \
  -e PG_USER=myuser \
  -e PG_PASS=mypassword \
  bani/bani:latest \
  ui --host 0.0.0.0
```

Open `http://localhost:8910` in your browser.

---

## Mounting BDL Files

The container runs as user `bani` with a home directory at `/home/bani`. Mount your BDL files to any path under this directory:

```bash
# Single file
-v $(pwd)/migration.bdl:/home/bani/migration.bdl:ro

# Directory of BDL files
-v $(pwd)/bdl:/home/bani/bdl:ro

# Projects directory (read-write for checkpoints)
-v $(pwd)/projects:/home/bani/.bani/projects
```

---

## Environment Variables

Pass database credentials and configuration via `-e`:

| Variable | Description |
|---|---|
| `PG_USER`, `PG_PASS` | PostgreSQL credentials |
| `MYSQL_USER`, `MYSQL_PASS` | MySQL credentials |
| `MSSQL_USER`, `MSSQL_PASS` | SQL Server credentials |
| `ORACLE_USER`, `ORACLE_PASS` | Oracle credentials |
| `BANI_BATCH_SIZE` | Override default batch size |
| `BANI_PARALLEL_WORKERS` | Override default parallel workers |
| `BANI_MEMORY_LIMIT_MB` | Override default memory limit |
| `BANI_LOG_LEVEL` | Logging level (debug, info, warn, error) |

---

## Docker Compose for Development

The project includes a `docker-compose.yml` that starts Bani alongside all 5 database services for testing:

```bash
docker compose up -d
```

### Services

| Service | Image | Port | Description |
|---|---|---|---|
| `bani` | `bani/bani:latest` | `8910` | Bani Web UI |
| `postgres` | `postgres:16` | `5433` | PostgreSQL 16 |
| `mysql` | `mysql:8.4` | `3306` | MySQL 8.4 |
| `mysql55` | `mysql:5.7` | `3307` | MySQL 5.7 |
| `mssql` | `mcr.microsoft.com/mssql/server:2022-latest` | `1433` | SQL Server 2022 Express |
| `oracle` | `gvenzl/oracle-free:23-slim-faststart` | `1521` | Oracle 23 Free |

### Default Credentials

| Database | Username | Password |
|---|---|---|
| PostgreSQL | `bani_test` | `bani_test` |
| MySQL 8.4 / 5.7 | `bani_test` | `bani_test` |
| SQL Server | `sa` | `BaniTest123!` |
| Oracle | `system` | `bani_test` |

### Usage Example

```bash
# Start all databases
docker compose up -d postgres mysql mssql

# Wait for health checks
docker compose ps

# Run a migration using the Bani service
docker compose run --rm bani run /home/bani/bdl/mysql-to-postgresql.bdl

# Or launch the Web UI
docker compose up bani
# Open http://localhost:8910
```

---

## Image Details

The Docker image is built using a multi-stage Dockerfile:

- **Stage 1 (builder):** Builds the Python wheel from source.
- **Stage 2 (runtime):** Installs system dependencies, database drivers, and the Bani wheel.

Included system dependencies:

- `libpq-dev` for PostgreSQL (psycopg)
- `unixodbc-dev` + ODBC Driver 18 for SQL Server (pyodbc)
- FreeTDS 1.4.26 built from source (pymssql)
- The pre-built React UI is copied into the package directory

The image runs as a non-root `bani` user with `PYTHONUNBUFFERED=1` for immediate log output. The default entrypoint is `bani`, so you pass subcommands directly:

```bash
docker run --rm bani/bani:latest validate /path/to/file.bdl
docker run --rm bani/bani:latest schema inspect --connector postgresql ...
```
