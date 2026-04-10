# BDL Reference

BDL (Bani Definition Language) is the declarative format for defining database migrations. BDL files use XML (`.bdl`) or JSON (`.bdl.json`) syntax. This reference documents every element and attribute.

---

## Document Structure

Every BDL document is wrapped in a `<bani>` root element:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project ... />
  <source ... />
  <target ... />
  <options> ... </options>
  <tables> ... </tables>
  <typeMappings> ... </typeMappings>
  <hooks> ... </hooks>
  <schedule> ... </schedule>
  <sync> ... </sync>
</bani>
```

| Attribute | Required | Description |
|---|---|---|
| `schemaVersion` | Yes | BDL schema version. Currently `"1.0"`. |

---

## `<project>`

Project metadata. The `name` attribute is required.

```xml
<project name="erp-to-analytics"
         description="Full migration from MySQL to PostgreSQL"
         author="data-engineering"
         created="2026-03-27T08:00:00Z">
  <tags>
    <tag>erp</tag>
    <tag>analytics</tag>
  </tags>
</project>
```

| Attribute | Required | Description |
|---|---|---|
| `name` | Yes | Short slug-style project identifier. |
| `description` | No | Human-readable description. |
| `author` | No | Author name or team. |
| `created` | No | ISO 8601 creation timestamp. |

The `<tags>` child element contains zero or more `<tag>` elements with text content.

---

## `<source>` and `<target>`

Connection definitions for the source and target databases. Both share the same structure.

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

### `<source>` / `<target>` attributes

| Attribute | Required | Description |
|---|---|---|
| `connector` | Yes | Connector name: `mysql`, `postgresql`, `mssql`, `oracle`, `sqlite`. |

### `<connection>` attributes

| Attribute | Required | Description |
|---|---|---|
| `host` | Yes | Database hostname or IP. |
| `port` | Yes | Database port. |
| `database` | Yes | Database name (or file path for SQLite). |
| `username` | No | Credential, typically `${env:VAR_NAME}`. |
| `password` | No | Credential, typically `${env:VAR_NAME}`. |
| `usernameEnv` | No | Alternative: environment variable name for username. |
| `passwordEnv` | No | Alternative: environment variable name for password. |

!!! warning "Credential security"
    Always use `${env:VAR_NAME}` references for credentials. Bani resolves these at runtime from the process environment. Plaintext passwords in BDL files are a security risk.

### `<connectorConfig>`

Optional connector-specific settings. Each `<option>` has `name` and `value` attributes.

```xml
<connectorConfig>
  <option name="charset" value="utf8mb4" />
  <option name="connectTimeout" value="30" />
  <option name="service_name" value="ORCLPDB1" />  <!-- Oracle -->
  <option name="oracle_client_lib" value="/opt/oracle/instantclient" />
</connectorConfig>
```

---

## `<tables>`

Defines which tables to migrate and how columns are mapped.

```xml
<tables mode="include">
  <table sourceName="customers"
         targetName="customers"
         sourceSchema="dbo"
         targetSchema="public"
         writeStrategy="insert">
    <columnMappings>
      <column source="id" target="customer_id" targetType="BIGINT" />
      <column source="full_name" target="name" />
    </columnMappings>
    <filter>WHERE is_active = 1</filter>
  </table>
</tables>
```

### `<tables>` attributes

| Attribute | Default | Description |
|---|---|---|
| `mode` | `include` | `"include"`: only listed tables. `"exclude"`: all tables except listed ones. |

### `<table>` attributes

| Attribute | Required | Description |
|---|---|---|
| `sourceName` | Yes | Table name in the source database. |
| `targetName` | No | Table name in the target. Defaults to `sourceName`. |
| `sourceSchema` | No | Schema in the source database. |
| `targetSchema` | No | Schema in the target database. |
| `filter` | No | SQL WHERE clause to restrict rows. |
| `writeStrategy` | No | `"insert"` (default), `"upsert"`, or `"truncate-insert"`. |

### `<columnMappings>`

Optional. Maps source columns to different target names or types.

| Attribute | Required | Description |
|---|---|---|
| `source` | Yes | Source column name. |
| `target` | Yes | Target column name. |
| `targetType` | No | Override the automatically mapped data type. |

If `<columnMappings>` is omitted, all columns are transferred with automatic name preservation and type mapping.

---

## `<options>`

Project-level configuration for the migration engine.

```xml
<options>
  <batchSize>100000</batchSize>
  <parallelWorkers>4</parallelWorkers>
  <memoryLimitMB>2048</memoryLimitMB>
  <onError>log-and-continue</onError>
  <createTargetSchema>true</createTargetSchema>
  <dropTargetTablesFirst>false</dropTargetTablesFirst>
  <transferIndexes>true</transferIndexes>
  <transferForeignKeys>true</transferForeignKeys>
</options>
```

| Element | Default | Description |
|---|---|---|
| `batchSize` | `100000` | Rows per Arrow RecordBatch. Larger batches improve throughput but use more memory. |
| `parallelWorkers` | `4` | Tables transferred concurrently. |
| `memoryLimitMB` | `2048` | Soft memory cap in MB. |
| `onError` | `log-and-continue` | `"log-and-continue"` skips failed rows/tables. `"fail-fast"` (or `"abort"`) halts on the first error. |
| `createTargetSchema` | `true` | Create the target schema if it does not exist. |
| `dropTargetTablesFirst` | `false` | Drop existing target tables before creating. Use `true` for repeatable full refreshes. |
| `transferIndexes` | `true` | Copy indexes from source to target. |
| `transferForeignKeys` | `true` | Copy foreign key constraints. |
| `transferDefaults` | `true` | Copy column default values. |
| `transferCheckConstraints` | `true` | Copy CHECK constraints. |

---

## `<typeMappings>`

Override the automatic type mapping for specific source types.

```xml
<typeMappings>
  <mapping sourceType="MEDIUMTEXT" targetType="TEXT" />
  <mapping sourceType="TINYINT(1)" targetType="BOOLEAN" />
</typeMappings>
```

Each `<mapping>` has:

| Attribute | Description |
|---|---|
| `sourceType` | Source database type to match. |
| `targetType` | Target database type to use instead of the automatic mapping. |

See [Type Mappings](type-mappings.md) for the default mapping tables.

---

## `<hooks>`

Pre- and post-migration hooks for running custom SQL or shell commands.

```xml
<hooks>
  <hook event="before-migration" type="sql" target="target">
    CREATE SCHEMA IF NOT EXISTS analytics;
  </hook>

  <hook event="after-table" tableName="orders" type="sql" target="target">
    CREATE INDEX idx_orders_date ON orders (order_date);
  </hook>

  <hook event="after-migration" type="shell" onFailure="continue">
    curl -s -X POST https://hooks.slack.com/... -d '{"text":"Migration complete"}'
  </hook>
</hooks>
```

### `<hook>` attributes

| Attribute | Required | Default | Description |
|---|---|---|---|
| `event` | Yes | -- | `"before-migration"`, `"after-migration"`, `"before-table"`, `"after-table"`. |
| `type` | Yes | -- | `"sql"` or `"shell"`. |
| `target` | For SQL | -- | `"source"` or `"target"` -- which database to run SQL against. |
| `tableName` | No | -- | For per-table events, the table this hook applies to. |
| `onFailure` | No | `"abort"` | `"abort"` halts the migration. `"continue"` logs and proceeds. |
| `timeout` | No | `300` | Maximum execution time in seconds. |

The text content of the `<hook>` element is the SQL statement or shell command.

!!! danger "Shell hook security"
    Shell hooks execute with the privileges of the Bani process. Only use them in trusted environments.

---

## `<schedule>`

Configure recurring scheduled migrations via the OS scheduler (crontab).

```xml
<schedule enabled="true">
  <cron>0 2 * * *</cron>
  <timezone>UTC</timezone>
  <retryOnFailure maxRetries="2" delaySeconds="120" />
</schedule>
```

| Element / Attribute | Default | Description |
|---|---|---|
| `enabled` | `false` | Whether the schedule is active. |
| `<cron>` | -- | Standard 5-field cron expression (e.g. `"0 2 * * *"` for daily at 2 AM). |
| `<timezone>` | `UTC` | IANA timezone for cron evaluation. |
| `maxRetries` | `0` | Number of retries on failure. |
| `delaySeconds` | `0` | Delay between retries in seconds. |

---

## `<sync>`

Enable incremental (delta) synchronisation.

```xml
<sync enabled="true">
  <strategy>timestamp</strategy>
  <trackingColumn table="Orders" column="ModifiedAt" />
  <trackingColumn table="Customers" column="UpdatedAt" />
</sync>
```

| Element / Attribute | Default | Description |
|---|---|---|
| `enabled` | `false` | Whether sync is active. |
| `<strategy>` | `full` | `"timestamp"`, `"rowversion"`, or `"checksum"`. |
| `<trackingColumn>` | -- | Maps a table to its change-tracking column. |

See [Incremental Sync](incremental-sync.md) for details on each strategy.

---

## JSON Format

BDL files can also be written as JSON (`.bdl.json`). The structure mirrors the XML:

```json
{
  "schemaVersion": "1.0",
  "project": {
    "name": "my-migration",
    "description": "MySQL to PostgreSQL"
  },
  "source": {
    "connector": "mysql",
    "connection": {
      "host": "localhost",
      "port": 3306,
      "database": "erp",
      "username": "${env:MYSQL_USER}",
      "password": "${env:MYSQL_PASS}"
    }
  },
  "target": {
    "connector": "postgresql",
    "connection": {
      "host": "localhost",
      "port": 5432,
      "database": "analytics",
      "username": "${env:PG_USER}",
      "password": "${env:PG_PASS}"
    }
  },
  "options": {
    "batchSize": 100000,
    "parallelWorkers": 4,
    "onError": "log-and-continue"
  },
  "tables": [
    {"sourceName": "customers", "targetName": "customers"},
    {"sourceName": "orders", "targetName": "orders"}
  ]
}
```

---

## Example Files

The `examples/bdl/` directory contains annotated example BDL files:

- `mysql-to-postgresql.bdl` -- Basic full migration with column mappings
- `incremental-sync.bdl` -- Delta sync with timestamp tracking
- `custom-hooks.bdl` -- Pre/post migration hooks (SQL and shell)
- `mssql-to-postgresql.bdl` -- Cross-dialect migration
- `oracle-to-postgresql.bdl` -- Oracle source with service_name
- `sqlite-to-mysql.bdl` -- SQLite file to MySQL
- `scheduled-nightly.bdl` -- Nightly scheduled migration
- `filtered-migration.bdl` -- Row filtering with WHERE clauses
- `multi-schema.bdl` -- Multi-schema migration
- `chained-migration.bdl` -- Multi-step migration pipeline
