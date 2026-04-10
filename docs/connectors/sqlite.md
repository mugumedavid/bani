# SQLite Connector

The SQLite connector supports reading from and writing to SQLite databases using the Python standard library `sqlite3` module.

---

## Supported Versions

SQLite 3.x.

## Driver

**sqlite3** (Python stdlib). No additional pip dependencies required. Optional enhanced support via **apsw** (`pip install "bani[sqlite-extras]"`).

---

## Connection Configuration

```xml
<source connector="sqlite">
  <connection database="/path/to/data.db" />
</source>
```

| Parameter | Default | Description |
|---|---|---|
| `database` | Required | File path to the SQLite database, or `:memory:` for in-memory databases. |

SQLite does not require host, port, username, or password.

---

## Performance Features

### WAL Journal Mode

The connector enables WAL (Write-Ahead Logging) journal mode for better concurrent read performance:

```sql
PRAGMA journal_mode = WAL;
```

WAL mode is not enabled for `:memory:` databases.

### Synchronous Mode

Set to `NORMAL` for a balance between safety and performance:

```sql
PRAGMA synchronous = NORMAL;
```

### Page Cache

The connector configures a 64MB page cache:

```sql
PRAGMA cache_size = -64000;
```

### Foreign Keys

Foreign keys are enabled on every connection:

```sql
PRAGMA foreign_keys = ON;
```

---

## Default Schema

The default schema is `main`.

---

## Type Mapping

SQLite uses a type affinity system with only 5 storage classes: NULL, INTEGER, REAL, TEXT, BLOB. See [Type Mappings > SQLite](../guides/type-mappings.md#sqlite-type-mappings) for the complete mapping tables.

Key points:

- All integer types map to SQLite `INTEGER`.
- All float types map to SQLite `REAL`.
- All string types map to SQLite `TEXT`.
- Dates and timestamps are stored as `TEXT` (ISO 8601 format).
- Booleans are stored as `BOOLEAN` (SQLite treats them as INTEGER 0/1).
- The type mapper handles coercion of ISO 8601 strings to Python `datetime` objects on reads.

---

## Foreign Key Handling

SQLite does not support `ALTER TABLE ADD CONSTRAINT` for foreign keys. Foreign keys must be defined in the `CREATE TABLE` statement.

The connector includes FK definitions directly in the `CREATE TABLE` DDL. The `create_foreign_keys()` method is a no-op since constraints cannot be added after table creation.

---

## Known Limitations

- No concurrent writes. SQLite uses file-level locking.
- No `ALTER TABLE ADD FOREIGN KEY`. All FKs are included in `CREATE TABLE`.
- Dates are stored as TEXT strings in ISO 8601 format.
- No native DECIMAL type -- numeric values use SQLite's REAL storage class.
- Connection pool is used but SQLite's threading model means `check_same_thread=False` is set.

---

## Example BDL

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="sqlite-to-mysql" />
  <source connector="sqlite">
    <connection database="/data/app.db" />
  </source>
  <target connector="mysql">
    <connection host="localhost" port="3306"
                database="app_copy"
                username="${env:MYSQL_USER}"
                password="${env:MYSQL_PASS}" />
  </target>
  <options>
    <batchSize>50000</batchSize>
    <parallelWorkers>1</parallelWorkers>
  </options>
</bani>
```
