# MySQL Connector

The MySQL connector supports reading from and writing to MySQL databases using the PyMySQL driver.

---

## Supported Versions

MySQL 5.5 through 8.4 (and 9.0).

## Driver

**PyMySQL** (`pymysql>=1.0`). Pure Python implementation with broad compatibility across MySQL versions. Uses `utf8mb4` charset and `local_infile=True` by default.

---

## Connection Configuration

```xml
<source connector="mysql">
  <connection host="localhost"
              port="3306"
              database="erp"
              username="${env:MYSQL_USER}"
              password="${env:MYSQL_PASS}" />
  <connectorConfig>
    <option name="charset" value="utf8mb4" />
  </connectorConfig>
</source>
```

| Parameter | Default | Description |
|---|---|---|
| `host` | Required | Database hostname or IP. |
| `port` | `3306` | Database port. |
| `database` | Required | Database name. |
| `username` | -- | Credential via `${env:VAR}`. |
| `password` | -- | Credential via `${env:VAR}`. |

### SSL / TLS

When `encrypt="true"` is set, SSL is enabled. When not set, `ssl_disabled=True` is used.

---

## Performance Features

### LOAD DATA LOCAL INFILE (Writes)

The `MySQLDataWriter` uses MySQL's `LOAD DATA LOCAL INFILE` protocol for high-throughput bulk inserts. If the server does not support it (e.g. due to `local_infile=OFF`), the writer falls back to `executemany` with multi-value INSERT statements.

### SSCursor Streaming (Reads)

Reads use `SScursor` (server-side cursor) to stream data without loading the entire result set into memory.

### Vectorized Column Extraction

Uses Arrow's `to_pylist()` for C-level column extraction.

---

## Version-Specific Handling

### MySQL 5.5 / 5.7

- **Key length limits:** InnoDB has a 767-byte key limit. VARCHAR columns in primary keys that exceed this are automatically shortened to `VARCHAR(191)` (fits in 767 bytes with utf8mb4 at 4 bytes/char).
- **Row size limits:** Tables with many VARCHAR columns may exceed the 65,535-byte row limit. Non-PK VARCHAR columns are converted to TEXT.
- **Identifier length:** MySQL limits identifiers to 64 characters. FK constraint names are prefixed with the table name and truncated to 64 chars.

### MySQL 8.4

- Uses `--mysql-native-password=ON` instead of the deprecated `--default-authentication-plugin`.

---

## Default Schema

The default schema is the connected database name.

---

## Type Mapping

See [Type Mappings > MySQL](../guides/type-mappings.md#mysql-type-mappings) for the complete mapping tables.

Key points:

- Unsigned integers are promoted to the next wider signed type.
- `TINYINT(1)` maps to `bool` (BIT).
- Binary vs text disambiguation uses charset number 63.

---

## Table Creation Retry Logic

The MySQL connector handles three common DDL errors with automatic retries:

| Error Code | Problem | Retry Strategy |
|---|---|---|
| `1071` | Key too long (767-byte InnoDB limit) | Shorten PK VARCHAR columns to `VARCHAR(191)` |
| `1118` | Row too large (65,535-byte limit) | Convert non-PK VARCHAR to TEXT |
| `1067` | Invalid default value | Strip problematic defaults |

---

## Known Limitations

- VARCHAR length is preserved when <= 1000 characters (Arrow `string` maps to TEXT, but the connector recovers the original length from the source `data_type`).
- `ENUM` and `SET` types are stored as Arrow `string` (the enum values are not preserved in the target).

---

## Example BDL

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="pg-to-mysql" />
  <source connector="postgresql">
    <connection host="localhost" port="5432"
                database="source_db"
                username="${env:PG_USER}"
                password="${env:PG_PASS}" />
  </source>
  <target connector="mysql">
    <connection host="localhost" port="3306"
                database="target_db"
                username="${env:MYSQL_USER}"
                password="${env:MYSQL_PASS}" />
  </target>
  <options>
    <batchSize>100000</batchSize>
    <parallelWorkers>4</parallelWorkers>
  </options>
</bani>
```
