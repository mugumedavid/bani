# BDL Example Library

Annotated [BDL (Bani Definition Language)](https://bani.tools) files covering common database migration and synchronisation scenarios. Each file includes XML comments explaining every element, serving as documentation for both humans and AI models generating BDL.

## Files

| File | Description |
|------|-------------|
| [`mysql-to-postgresql.bdl`](mysql-to-postgresql.bdl) | Basic full migration from MySQL 8 to PostgreSQL 16 with table selections, column mappings, and default options |
| [`mssql-to-postgresql.bdl`](mssql-to-postgresql.bdl) | MSSQL to PostgreSQL with type mapping overrides (MONEY, DATETIME, BIT, NVARCHAR, UNIQUEIDENTIFIER) |
| [`oracle-to-postgresql.bdl`](oracle-to-postgresql.bdl) | Oracle-specific features: service name connections, NUMBER precision handling, uppercase identifiers, VARCHAR2/CLOB mappings |
| [`sqlite-to-mysql.bdl`](sqlite-to-mysql.bdl) | Embedded-to-server migration from SQLite (file path) to MySQL, covering type affinity and missing date types |
| [`mysql-to-mysql-upgrade.bdl`](mysql-to-mysql-upgrade.bdl) | Same-engine version upgrade (MySQL 5.5 on port 3307 to MySQL 8.4 on port 3306) with charset migration (utf8 to utf8mb4) |
| [`incremental-sync.bdl`](incremental-sync.bdl) | Timestamp-based delta sync with tracking columns, hourly schedule, and retry configuration |
| [`filtered-migration.bdl`](filtered-migration.bdl) | Per-table WHERE filters: active records, date ranges, regional subsets, subquery filters |
| [`multi-schema.bdl`](multi-schema.bdl) | Cross-schema migration from multiple MSSQL schemas (hr, finance, inventory) into organised PostgreSQL schemas |
| [`custom-hooks.bdl`](custom-hooks.bdl) | Pre/post SQL and shell hooks: pg_dump backup, trigger disabling, VACUUM ANALYZE, Slack notifications |
| [`scheduled-nightly.bdl`](scheduled-nightly.bdl) | Cron-scheduled nightly migration (0 2 * * *) combined with incremental sync and retry settings |

## BDL Format

BDL is an XML format for defining database migration projects. A BDL file specifies:

- **Source and target** database connections (with environment-variable credentials)
- **Table selections** with optional column mappings, type overrides, and row filters
- **Type mapping overrides** for cross-engine type translation
- **Options** for batch size, parallelism, error handling, and schema object transfer
- **Hooks** for running SQL or shell commands before/after migration
- **Scheduling** with cron expressions, timezones, and retry policies
- **Incremental sync** with timestamp, rowversion, or checksum strategies

The XSD schema is at [`src/bani/bdl/schemas/bdl-1.0.xsd`](../../src/bani/bdl/schemas/bdl-1.0.xsd).

## Credentials

All examples use `${env:VAR_NAME}` syntax for credentials. Set the required environment variables before running:

```bash
export MYSQL_USER=myuser
export MYSQL_PASS=mypassword
export PG_USER=pguser
export PG_PASS=pgpassword
# ... etc.

bani run examples/bdl/mysql-to-postgresql.bdl
```

Never hardcode passwords in BDL files.
