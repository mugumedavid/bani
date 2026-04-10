# SQL Server Connector

The SQL Server (MSSQL) connector supports reading from and writing to Microsoft SQL Server using pyodbc (preferred) with pymssql as a fallback.

---

## Supported Versions

SQL Server 2019 through 2022.

## Drivers

**pyodbc** (`pyodbc>=5.0`) -- Preferred. Uses ODBC Driver 18 for SQL Server with `fast_executemany` for high-throughput array parameter binding. 5-10x faster than pymssql for writes.

**pymssql** (`pymssql>=2.2`) -- Fallback when the ODBC driver is not available. Uses FreeTDS under the hood. The connector automatically tests pyodbc first and falls back to pymssql.

---

## Connection Configuration

```xml
<source connector="mssql">
  <connection host="localhost"
              port="1433"
              database="ERP_Production"
              username="${env:MSSQL_USER}"
              password="${env:MSSQL_PASS}"
              encrypt="true" />
</source>
```

| Parameter | Default | Description |
|---|---|---|
| `host` | Required | Database hostname or IP. |
| `port` | `1433` | Database port. |
| `database` | Required | Database name. |
| `username` | -- | Credential via `${env:VAR}`. |
| `password` | -- | Credential via `${env:VAR}`. |
| `encrypt` | `false` | Enable TLS encryption. |

### ODBC Driver Auto-Detection

The connector automatically sets the `ODBCSYSINI` environment variable to locate Homebrew's `odbcinst.ini`:

- **Apple Silicon (arm64):** `/opt/homebrew/etc`
- **Intel (x86_64):** `/usr/local/etc`
- **Linux:** `/etc`

Set `BANI_DISABLE_PYODBC=1` to force pymssql even when pyodbc is available.

---

## Performance Features

### fast_executemany (pyodbc)

When pyodbc is available, the data writer enables `fast_executemany=True`, which uses ODBC array parameter binding for bulk inserts. This is 5-10x faster than row-by-row inserts.

### pymssql Fallback

pymssql uses FreeTDS and inline SQL literal formatting for inserts. The connector caps the connection pool to 1 for pymssql (FreeTDS has known stability issues under concurrent load with "DBPROCESS is dead" errors).

### Recommended Defaults

The MSSQL connector declares lower recommended defaults than other connectors:

- `recommended_batch_size = 10,000` (vs 100,000 global default)
- `recommended_parallel_workers = 2` (vs 4 global default)

These prevent source connection timeouts during long target-side writes.

---

## Default Schema

The default schema is `dbo`.

---

## Type Mapping

See [Type Mappings > SQL Server](../guides/type-mappings.md#sql-server-type-mappings) for the complete mapping tables.

Key points:

- Arrow `string` maps to `NVARCHAR(MAX)`. When the source `data_type` carries a length <= 4000, the connector uses `NVARCHAR(length)` instead.
- `NVARCHAR(MAX)` columns that participate in indexes are automatically altered to `NVARCHAR(4000)` before index creation.
- Boolean defaults (`TRUE`/`FALSE`) are converted to `0`/`1` for BIT columns.
- MSSQL treats NULLs as duplicates in unique indexes. The connector adds `WHERE col IS NOT NULL` filters to unique indexes.

---

## FK Creation Resilience

The MSSQL connector has extensive FK creation error handling:

| Error | Recovery |
|---|---|
| `1785` (cascade cycles) | Retry with `ON DELETE NO ACTION ON UPDATE NO ACTION` |
| `1776` (missing PK/unique) | Create a unique index on referenced columns, then retry |
| `1778` (type mismatch) | Align source column types to match referenced PK types, then retry |
| Connection broken | Reconnect and retry (up to 3 consecutive failures before aborting) |

---

## Known Limitations

- pymssql caps the connection pool to 1 (no write parallelism).
- `SET TEXTSIZE 2147483647` is issued on connect to handle large text columns.
- TDS version 7.3 is used for pymssql (SQL Server 2008+ compatibility).

---

## Example BDL

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="mssql-to-pg" />
  <source connector="mssql">
    <connection host="sqlserver.local" port="1433"
                database="ERP"
                username="${env:MSSQL_USER}"
                password="${env:MSSQL_PASS}" />
  </source>
  <target connector="postgresql">
    <connection host="localhost" port="5432"
                database="analytics"
                username="${env:PG_USER}"
                password="${env:PG_PASS}" />
  </target>
  <options>
    <batchSize>10000</batchSize>
    <parallelWorkers>2</parallelWorkers>
  </options>
</bani>
```
