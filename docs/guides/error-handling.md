# Error Handling

!!! info "Coming Soon"
    Advanced error handling features (quarantine, structured context, and detailed resumability) are on the roadmap. This page will be updated when the feature is released. Want to help build it? [Contribute on GitHub](https://github.com/mugumedavid/bani).

<!-- HIDDEN_CONTENT_START

Bani uses a structured exception hierarchy so that callers can programmatically inspect failures without parsing message strings. Every exception carries a `context` dict with structured metadata.

---

## Exception Hierarchy

```
BaniError
├── ConfigurationError
│   ├── BDLValidationError
│   ├── ConnectionConfigError
│   └── TypeMappingError
├── BaniConnectionError
│   ├── SourceConnectionError
│   └── TargetConnectionError
├── SchemaError
│   ├── IntrospectionError
│   ├── SchemaTranslationError
│   └── DependencyResolutionError
├── DataTransferError
│   ├── ReadError
│   ├── WriteError
│   ├── BatchError
│   └── TransformError
├── HookExecutionError
└── SchedulerError
```

---

## Error Categories

### Configuration Errors

Raised during project parsing, validation, or connection setup.

**`ConfigurationError`** -- Base class for configuration-related failures.

**`BDLValidationError`** -- The BDL document failed XSD or semantic validation. Carries:

- `document_path` -- Path to the BDL file (if loaded from disk).
- `line_number` -- Line number of the error (if available).

**`ConnectionConfigError`** -- Invalid or missing connection configuration. Carries:

- `connection_name` -- The connection key that failed.

**`TypeMappingError`** -- A source type could not be mapped to a target type. Carries:

- `source_type` -- The source type string that failed.
- `target_dialect` -- The target dialect.

### Connection Errors

Raised when establishing or maintaining database connections.

**`BaniConnectionError`** -- Base class (named to avoid shadowing the built-in `ConnectionError`).

**`SourceConnectionError`** -- Failed to connect to the source database.

**`TargetConnectionError`** -- Failed to connect to the target database.

### Schema Errors

Raised during schema introspection, translation, or dependency resolution.

**`SchemaError`** -- Base class for schema-related failures.

**`IntrospectionError`** -- Failed to introspect the source database schema.

**`SchemaTranslationError`** -- Failed to translate schema between source and target dialects.

**`DependencyResolutionError`** -- Failed to resolve table dependencies (e.g. circular foreign key chains). Carries:

- `tables` -- Tuple of table names involved in the cycle.

### Data Transfer Errors

Raised during the actual data movement between databases.

**`DataTransferError`** -- Base class for transfer failures.

**`ReadError`** -- Failed to read data from the source.

**`WriteError`** -- Failed to write data to the target.

**`BatchError`** -- A specific batch failed during transfer. Carries:

- `batch_number` -- The 0-based batch index.
- `first_row_offset` -- The offset of the first row in the failed batch, so the resumability protocol can pick up from the right place.

**`TransformError`** -- A transform step failed during the pipeline.

### Hook and Scheduler Errors

**`HookExecutionError`** -- A pre- or post-migration hook failed.

**`SchedulerError`** -- Scheduler-related failure (cron integration, task scheduling).

---

## Error Handling Modes

Bani supports two error handling strategies, configured via the `<onError>` element in BDL:

### Abort (fail-fast)

```xml
<options>
  <onError>fail-fast</onError>
</options>
```

The migration halts immediately on the first error. This is the safest option when data consistency is critical. The checkpoint is saved so you can resume later.

### Log and Continue

```xml
<options>
  <onError>log-and-continue</onError>
</options>
```

Failed rows or tables are logged and skipped. The migration continues with remaining tables. This is useful for large migrations where a few problematic tables should not block the entire operation.

!!! note
    Even in `log-and-continue` mode, certain errors are always fatal: connection failures, BDL validation errors, and schema introspection errors. Only per-table and per-batch errors can be skipped.

---

## Checkpoint-Based Resumability

When a migration fails, Bani saves a checkpoint recording which tables completed, which failed, and where each in-progress table left off.

### Resuming After Failure

```bash
bani run migration.bdl --resume
```

Or via the SDK:

```python
result = project.run(resume=True)
```

Or via MCP:

```json
{"tool": "bani_run", "params": {"project_name": "my-migration", "resume": true}}
```

### Resume Behavior

1. **Completed tables** are skipped entirely.
2. **Failed or in-progress tables** are dropped and re-transferred from scratch.
3. **Pending tables** are transferred normally.

This ensures data correctness: partial writes to a table are never left in place.

### Checking Status Before Resume

Use `bani_status` (MCP) or inspect the checkpoint file to see which tables need attention:

```python
# MCP
bani_status(project_name="my-migration")
```

---

## Handling Errors in Python

```python
from bani.domain.errors import (
    BaniError,
    BDLValidationError,
    BaniConnectionError,
    DataTransferError,
)
from bani.sdk.bani import Bani

try:
    project = Bani.load("migration.bdl")
    result = project.run()
except BDLValidationError as e:
    print(f"BDL error at line {e.line_number}: {e}")
except BaniConnectionError as e:
    print(f"Connection failed: {e}")
except DataTransferError as e:
    print(f"Transfer error: {e}")
    print(f"Context: {e.context}")
except BaniError as e:
    print(f"Bani error: {e}")
```

### Accessing Structured Context

Every `BaniError` carries a `context` dict with key-value metadata:

```python
try:
    result = project.run()
except BatchError as e:
    print(f"Batch {e.batch_number} failed at row offset {e.first_row_offset}")
    print(f"Full context: {e.context}")
```

---

## Per-Table Error Handling

During migration, errors at the table level (create table, write batch, create indexes, create foreign keys) are handled with per-table try/except blocks. In `log-and-continue` mode:

- If `create_table` fails, the table is skipped and the error is recorded.
- If `write_batch` fails, the error is logged and the migration moves to the next table.
- If `create_indexes` or `create_foreign_keys` fails for a specific index or FK, that constraint is skipped and the migration continues.

This per-FK and per-index error handling is especially important for cross-dialect migrations where FK types, cascade rules, or index expressions may not translate perfectly.

HIDDEN_CONTENT_END -->
