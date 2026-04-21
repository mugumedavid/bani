# Incremental Sync

!!! info "Coming Soon"
    Incremental sync is on the roadmap but not yet available. This page will be updated when the feature is released. Want to help build it? [Contribute on GitHub](https://github.com/mugumedavid/bani).

<!-- HIDDEN_CONTENT_START

Incremental sync (delta synchronisation) transfers only rows that have changed since the last run, rather than copying the entire dataset. This is essential for ongoing replication, data warehouse refreshes, and near-real-time data pipelines.

---

## Sync Strategies

Bani supports three sync strategies, each suited to different use cases.

### TIMESTAMP

The timestamp strategy uses a datetime column on each table to detect changes. Bani stores a **high-water mark** (the maximum timestamp seen) in its checkpoint file after each run.

On the next run, Bani generates a WHERE clause:

```sql
WHERE updated_at > '2026-03-26T02:00:00Z'
```

to fetch only rows modified since the last checkpoint.

**Characteristics:**

- Detects inserts and updates
- Does **not** detect deletes (use soft deletes or the checksum strategy)
- Requires an `updated_at` or equivalent column on every synced table
- The tracking column must be indexed for performance
- The tracking column must be reliably updated on every INSERT and UPDATE

**BDL configuration:**

```xml
<sync enabled="true">
  <strategy>timestamp</strategy>
  <trackingColumn table="Orders" column="ModifiedAt" />
  <trackingColumn table="Customers" column="UpdatedAt" />
  <trackingColumn table="Products" column="UpdatedAt" />
</sync>
```

---

### ROWVERSION

The rowversion strategy uses SQL Server's `rowversion` (formerly `timestamp`) data type. This is a binary value that is automatically incremented by the database engine on every update.

**Characteristics:**

- SQL Server-specific
- No application-level column maintenance required
- Automatically tracks all changes
- Does not detect deletes

**BDL configuration:**

```xml
<sync enabled="true">
  <strategy>rowversion</strategy>
  <trackingColumn table="Orders" column="RowVer" />
  <trackingColumn table="Customers" column="RowVer" />
</sync>
```

---

### CHECKSUM

The checksum strategy computes a row-level hash to detect any change, including deletions. On each run, Bani computes checksums for all rows in both source and target, then transfers rows whose checksums differ.

**Characteristics:**

- Detects inserts, updates, **and** deletes
- Works without a tracking column
- Slower than timestamp/rowversion (requires full table scan)
- Higher memory usage for checksum comparison

**BDL configuration:**

```xml
<sync enabled="true">
  <strategy>checksum</strategy>
</sync>
```

---

## Configuration

### Enabling Sync

Add a `<sync>` element to your BDL file with `enabled="true"`:

```xml
<sync enabled="true">
  <strategy>timestamp</strategy>
  <trackingColumn table="Orders" column="ModifiedAt" />
  <trackingColumn table="Customers" column="UpdatedAt" />
</sync>
```

### Tracking Columns

Each `<trackingColumn>` maps a table name to the column that records when a row was last modified:

| Attribute | Description |
|---|---|
| `table` | Table name (must match a table in the `<tables>` section). |
| `column` | Column name used for change detection. |

!!! warning "Prerequisites"
    - Every synced table must have the specified tracking column.
    - The tracking column must be indexed on the source.
    - The tracking column must be updated on every INSERT and UPDATE.
    - The target table must already exist (from a previous full migration).

### Combining with Schedule

Incremental sync is most useful when combined with a schedule for automatic recurring runs:

```xml
<schedule enabled="true">
  <cron>0 * * * *</cron>
  <timezone>UTC</timezone>
  <retryOnFailure maxRetries="2" delaySeconds="120" />
</schedule>

<sync enabled="true">
  <strategy>timestamp</strategy>
  <trackingColumn table="Orders" column="ModifiedAt" />
</sync>
```

This runs an incremental sync every hour.

---

## Checkpoint Files and State Management

Bani stores sync state in checkpoint files under `~/.bani/projects/`. After each successful run, the checkpoint records:

- **High-water mark** for each table (maximum value of the tracking column)
- **Table completion status** (completed, failed, pending)
- **Row counts** transferred per table

On the next run with `resume=true` or in sync mode, Bani reads the checkpoint to determine which rows to fetch and which tables to skip.

### Checkpoint Lifecycle

1. **First run:** No checkpoint exists. Bani performs a full migration.
2. **Subsequent runs:** Bani reads the checkpoint, generates WHERE clauses based on high-water marks, and transfers only changed rows.
3. **After failure:** Use `--resume` to resume from the checkpoint. Completed tables are skipped; failed tables are retried.

### Inspecting Checkpoints

Use the CLI or MCP to check checkpoint status:

```bash
# CLI (via JSON output)
bani -o json run migration.bdl --dry-run
```

```python
# MCP
bani_status(project_name="my-migration")
```

---

## Options for Incremental Sync

When configuring `<options>` for incremental sync, consider these differences from full migrations:

```xml
<options>
  <!-- Smaller batches since fewer rows are transferred -->
  <batchSize>50000</batchSize>
  <!-- Fewer parallel workers to reduce source load -->
  <parallelWorkers>2</parallelWorkers>
  <!-- Do NOT drop target tables -->
  <dropTargetTablesFirst>false</dropTargetTablesFirst>
  <!-- Continue on errors so other tables still sync -->
  <onError>log-and-continue</onError>
</options>
```

!!! danger "Do not set `dropTargetTablesFirst` to `true`"
    This would destroy all previously synced data on every run.

---

## Complete Example

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="erp-incremental-sync"
           description="Hourly delta sync from MSSQL to PostgreSQL" />

  <source connector="mssql">
    <connection host="erp-server" port="1433"
                database="ERP_Production"
                username="${env:MSSQL_USER}"
                password="${env:MSSQL_PASS}" />
  </source>

  <target connector="postgresql">
    <connection host="analytics-server" port="5432"
                database="analytics"
                username="${env:PG_USER}"
                password="${env:PG_PASS}" />
  </target>

  <options>
    <batchSize>50000</batchSize>
    <parallelWorkers>2</parallelWorkers>
    <dropTargetTablesFirst>false</dropTargetTablesFirst>
    <onError>log-and-continue</onError>
  </options>

  <tables mode="include">
    <table sourceSchema="dbo" sourceName="Orders" targetName="orders" />
    <table sourceSchema="dbo" sourceName="Customers" targetName="customers" />
  </tables>

  <schedule enabled="true">
    <cron>0 * * * *</cron>
    <timezone>UTC</timezone>
  </schedule>

  <sync enabled="true">
    <strategy>timestamp</strategy>
    <trackingColumn table="Orders" column="ModifiedAt" />
    <trackingColumn table="Customers" column="UpdatedAt" />
  </sync>
</bani>
```

HIDDEN_CONTENT_END -->
