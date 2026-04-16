# Docker

Bani provides a multi-arch Docker image with all 5 database drivers pre-installed, including ODBC Driver 18 for SQL Server and FreeTDS 1.4.

---

## Quick Start: Web UI

The simplest way to use Bani in Docker is to launch the Web UI:

```bash
docker run -p 8910:8910 banilabs/bani:latest bani ui --host 0.0.0.0
```

The container logs will print a ready-to-click URL with the auth token embedded -- copy it from the logs and open it in your browser. From the Web UI you add connections, build migrations, and run them visually. No BDL files or environment variables required.

### Persist projects and connections across restarts

Mount a host directory so saved projects and connections survive container restarts:

```bash
docker run -p 8910:8910 \
  -v $(pwd)/.bani:/home/bani/.bani \
  banilabs/bani:latest bani ui --host 0.0.0.0
```

---

## Headless / CI usage

For automation, scheduled jobs, or CI pipelines, run a migration directly from a BDL file without the UI.

### With credentials in the BDL file

If your BDL file already includes credentials (e.g. `username="myuser" password="mypass"`), the `docker run` is straightforward:

```bash
docker run --rm \
  -v $(pwd)/migration.bdl:/home/bani/migration.bdl:ro \
  banilabs/bani:latest run /home/bani/migration.bdl
```

### With credentials in environment variables

If your BDL references env vars (e.g. `username="${env:SRC_DB_USER}"` or `username="SRC_DB_USER"`), pass them via `-e`:

```bash
docker run --rm \
  -v $(pwd)/migration.bdl:/home/bani/migration.bdl:ro \
  -e SRC_DB_USER=myuser \
  -e SRC_DB_PASS=mypassword \
  -e TGT_DB_USER=pguser \
  -e TGT_DB_PASS=pgpassword \
  banilabs/bani:latest run /home/bani/migration.bdl
```

!!! tip "Network access"
    Use `--network host` if your databases run on the Docker host. For containerised databases, attach to the same Docker network.

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

Bani supports the following environment variables in Docker:

| Variable | Description |
|---|---|
| `BANI_AUTH_TOKEN` | Use a fixed Web UI auth token (instead of a random one each restart). |
| `BANI_LOG_LEVEL` | Logging level (`debug`, `info`, `warn`, `error`). |

For database credentials, you can either:

- **Embed them in the BDL file** -- simplest, no `-e` flags needed
- **Reference env vars** -- use any names you like in the BDL (`${env:MY_VAR}` or bare `MY_VAR`) and pass them with `-e MY_VAR=value`

---

## Docker Compose for Development

The project includes a `docker-compose.yml` that starts Bani alongside all 5 database services for testing:

```bash
docker compose up -d
```

### Services

| Service | Image | Port | Description |
|---|---|---|---|
| `bani` | `banilabs/bani:latest` | `8910` | Bani Web UI |
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
docker run --rm banilabs/bani:latest validate /path/to/file.bdl
docker run --rm banilabs/bani:latest schema inspect --connector postgresql ...
```
