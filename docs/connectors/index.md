# Connectors Overview

Bani ships with 5 built-in connectors, each implementing both source (read) and sink (write) interfaces. Connectors are discovered via Python entry points and can be extended by third-party packages.

---

## Connector Comparison

| Connector | Supported Versions | Python Driver | Source | Sink | Key Performance Feature |
|---|---|---|---|---|---|
| [PostgreSQL](postgresql.md) | 9.6 -- 17 | psycopg 3.x | Yes | Yes | COPY binary protocol for writes |
| [MySQL](mysql.md) | 5.5 -- 8.4 | PyMySQL | Yes | Yes | LOAD DATA LOCAL INFILE with executemany fallback |
| [SQL Server](mssql.md) | 2019 -- 2022 | pyodbc (preferred) / pymssql (fallback) | Yes | Yes | fast_executemany via ODBC array binding |
| [Oracle](oracle.md) | 11g -- 23c | python-oracledb | Yes | Yes | batcherrors for partial-batch writes |
| [SQLite](sqlite.md) | 3.x | sqlite3 (stdlib) | Yes | Yes | WAL journal mode, 64MB page cache |

---

## Architecture

All connectors implement two abstract base classes from `bani.connectors.base`:

- **`SourceConnector`** -- `connect()`, `disconnect()`, `introspect_schema()`, `read_table()`, `estimate_row_count()`
- **`SinkConnector`** -- `connect()`, `disconnect()`, `create_table()`, `write_batch()`, `create_indexes()`, `create_foreign_keys()`, `execute_sql()`

Every connector implements both interfaces (source + sink).

### Arrow Interchange

Data flows as `pyarrow.RecordBatch` between source and sink connectors. This is a core architectural invariant:

```
Source DB  --[read_table()]--> RecordBatch --[write_batch()]--> Target DB
```

No intermediate Pandas DataFrames, dict-of-lists, or ORM objects are used.

### Type Mapping

Each connector has a type mapper with two directions:

- **Source side:** Maps native DB types to Arrow types during `introspect_schema()`.
- **Sink side:** `from_arrow_type()` maps Arrow type strings back to native DDL types during `create_table()`.

This gives N mappers (one per connector) instead of N*N cross-database translation tables. See [Type Mappings](../guides/type-mappings.md) for the complete mapping tables.

### Connector Discovery

Connectors are registered via Python entry points in `pyproject.toml`:

```toml
[project.entry-points."bani.connectors"]
mysql = "bani.connectors.mysql:MySQLConnector"
postgresql = "bani.connectors.postgresql:PostgreSQLConnector"
mssql = "bani.connectors.mssql:MSSQLConnector"
oracle = "bani.connectors.oracle:OracleConnector"
sqlite = "bani.connectors.sqlite:SQLiteConnector"
```

The `ConnectorRegistry` discovers all registered connectors at runtime. The orchestrator never references a concrete connector class directly.

### Connection Pooling

All connectors use a `ConnectionPool` that creates multiple connections for parallel table transfers. The pool size is controlled by the `parallel_workers` project option.

---

## Common Connection Config

All connectors accept credentials via environment variable references:

```xml
<source connector="postgresql">
  <connection host="localhost"
              port="5432"
              database="mydb"
              username="${env:PG_USER}"
              password="${env:PG_PASS}" />
</source>
```

The `${env:VAR_NAME}` syntax is resolved at runtime by each connector's `_resolve_env_var()` method.
