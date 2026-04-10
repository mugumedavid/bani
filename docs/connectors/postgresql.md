# PostgreSQL Connector

The PostgreSQL connector supports reading from and writing to PostgreSQL databases using the psycopg 3.x driver.

---

## Supported Versions

PostgreSQL 9.6 through 17.

## Driver

**psycopg 3.x** (`psycopg[binary]>=3.1`). Uses the binary build for faster installation.

---

## Connection Configuration

```xml
<source connector="postgresql">
  <connection host="localhost"
              port="5432"
              database="analytics"
              username="${env:PG_USER}"
              password="${env:PG_PASS}" />
</source>
```

| Parameter | Default | Description |
|---|---|---|
| `host` | Required | Database hostname or IP. |
| `port` | `5432` | Database port. |
| `database` | Required | Database name. |
| `username` | -- | Credential via `${env:VAR}`. |
| `password` | -- | Credential via `${env:VAR}`. |

### SSL / TLS

When `encrypt="true"` is set on the `<connection>` element, the connector uses `sslmode=prefer` (try SSL, fall back to plaintext). When not set, `sslmode=disable` is used.

### TCP Keepalive

The connector enables TCP keepalive by default to prevent idle connection timeouts during long target-side writes:

- `keepalives=1`
- `keepalives_idle=30` seconds
- `keepalives_interval=10` seconds
- `keepalives_count=5`

---

## Performance Features

### COPY Binary Protocol (Writes)

The `PostgreSQLDataWriter` uses the psycopg COPY protocol with `write_row()` for high-throughput bulk inserts. This is significantly faster than row-by-row INSERT statements.

### Streaming Cursor (Reads)

Reads use server-side cursors to stream data without loading the entire result set into memory. Data is fetched in batches of `batch_size` rows and converted to Arrow RecordBatches.

### Push-Down CAST

For jsonb, json, and uuid columns, the data reader issues `::text` casts in the SELECT statement. This pushes string conversion to the database server, eliminating Python-level `json.dumps()` and `str()` calls in the hot path.

### Vectorized Column Extraction

The data writer uses Arrow's `to_pylist()` for C-level column extraction instead of per-cell `scalar.as_py()` loops.

---

## Default Schema

The default schema is `public`.

---

## Type Mapping

See [Type Mappings > PostgreSQL](../guides/type-mappings.md#postgresql-type-mappings) for the complete source and sink mapping tables.

Key points:

- `serial` / `bigserial` are used for auto-increment columns (bigserial for int64 sources).
- Default values are normalized: MySQL's `CURRENT_TIMESTAMP` becomes `NOW()`, bare string literals are quoted.

---

## Known Limitations

- VARCHAR length is lost through the Arrow intermediate (`varchar(255)` becomes Arrow `string` becomes `text`).
- Numeric precision is not fully preserved (`DECIMAL(10,2)` becomes `decimal128(38,10)` then `numeric(38,10)`).
- Array types are stored as Arrow `string` (not native Arrow arrays).

---

## Example BDL

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="mysql-to-pg" />
  <source connector="mysql">
    <connection host="localhost" port="3306"
                database="erp"
                username="${env:MYSQL_USER}"
                password="${env:MYSQL_PASS}" />
  </source>
  <target connector="postgresql">
    <connection host="localhost" port="5432"
                database="analytics"
                username="${env:PG_USER}"
                password="${env:PG_PASS}" />
  </target>
  <options>
    <batchSize>100000</batchSize>
    <parallelWorkers>4</parallelWorkers>
  </options>
</bani>
```
