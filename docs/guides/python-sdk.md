# Python SDK

The Bani Python SDK lets you build, validate, and execute migrations programmatically. It is the same API used internally by the CLI and Web UI.

---

## Installation

The SDK is included with all Bani installations (platform installers, Docker, and development installs). For development, install from source:

```bash
git clone https://github.com/mugumedavid/bani.git
cd bani
pip install -e .
```

---

## Core Classes

### `Bani`

Top-level entry point for loading BDL files.

```python
from bani.sdk.bani import Bani
```

#### `Bani.load(path) -> BaniProject`

Load a BDL project file (XML or JSON) and return a `BaniProject` ready for validation and execution.

```python
project = Bani.load("migration.bdl")
# or
project = Bani.load(Path("migration.bdl"))
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `path` | `str \| Path` | Path to the BDL file. |

**Returns:** `BaniProject`

**Raises:** `BDLValidationError` if parsing fails.

#### `Bani.validate_file(path) -> tuple[bool, list[str]]`

Validate a BDL file without loading it into a full project.

```python
is_valid, errors = Bani.validate_file("migration.bdl")
if not is_valid:
    for error in errors:
        print(f"Error: {error}")
```

---

### `BaniProject`

Wrapper around a loaded `ProjectModel` with validation and execution methods.

#### `BaniProject.validate() -> tuple[bool, list[str]]`

Validate the project configuration. Returns a tuple of `(is_valid, error_messages)`.

```python
project = Bani.load("migration.bdl")
is_valid, errors = project.validate()
```

#### `BaniProject.run(...) -> MigrationResult`

Execute the migration.

```python
result = project.run(
    on_progress=my_callback,   # Optional progress callback
    resume=False,              # Resume from checkpoint
    cancel_event=None,         # threading.Event for cancellation
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `on_progress` | `Callable[[Any], None] \| None` | `None` | Callback invoked for each progress event. |
| `resume` | `bool` | `False` | Resume from the last checkpoint. Completed tables are skipped. |
| `cancel_event` | `Any \| None` | `None` | A `threading.Event` that signals cancellation. |
| `checkpoint` | `Any \| None` | `None` | Optional `CheckpointManager` instance. |
| `projects_dir` | `str` | `~/.bani/projects` | Directory for checkpoint and log files. |

**Returns:** `MigrationResult`

**Raises:** `ValueError` if the project is invalid.

#### `BaniProject.preview(...) -> PreviewResult`

Preview data from the source database.

```python
result = project.preview(
    tables=["public.users"],  # Optional table filter
    sample_size=10,           # Rows per table
)
for table in result.tables:
    print(f"{table.table_name}: {table.row_count_estimate} rows")
    for row in table.sample_rows:
        print(row)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `tables` | `list[str] \| None` | `None` | Table names to preview. `None` for all. |
| `sample_size` | `int` | `10` | Number of rows to sample per table. |

**Returns:** `PreviewResult`

---

### `ProjectBuilder`

Fluent builder for constructing `ProjectModel` instances without writing BDL files.

```python
from bani.sdk.project_builder import ProjectBuilder
```

#### Builder Methods

All methods return `self` for method chaining.

```python
project_model = (
    ProjectBuilder("my-migration")
    .description("Migrate ERP data")
    .author("data-team")
    .tags(["erp", "production"])
    .source(
        dialect="mysql",
        host="localhost",
        port=3306,
        database="erp",
        username_env="MYSQL_USER",
        password_env="MYSQL_PASS",
    )
    .target(
        dialect="postgresql",
        host="localhost",
        port=5432,
        database="analytics",
        username_env="PG_USER",
        password_env="PG_PASS",
    )
    .include_tables([
        "erp.customers",
        "erp.orders",
        "erp.products",
    ])
    .batch_size(100_000)
    .parallel_workers(4)
    .memory_limit(2048)
    .type_mapping("MEDIUMTEXT", "TEXT")
    .build()
)
```

| Method | Parameters | Description |
|---|---|---|
| `source(...)` | `dialect, host, port, database, username_env, password_env, **extra` | Configure the source database. |
| `target(...)` | `dialect, host, port, database, username_env, password_env, **extra` | Configure the target database. |
| `include_tables(tables)` | `list[str]` in `"schema.table"` format | Include only these tables. |
| `exclude_tables(tables)` | `list[str]` in `"schema.table"` format | Exclude these tables. |
| `type_mapping(source_type, target_type)` | Two strings | Add a type mapping override. |
| `batch_size(size)` | `int` | Set rows per batch. |
| `parallel_workers(workers)` | `int` | Set parallel worker count. |
| `memory_limit(mb)` | `int` | Set memory limit in MB. |
| `description(desc)` | `str` | Set project description. |
| `author(name)` | `str` | Set project author. |
| `tags(tags)` | `list[str]` | Set project tags. |
| `build()` | -- | Build and return the `ProjectModel`. |

#### Running a Built Project

```python
from bani.sdk.bani import BaniProject

bani_project = BaniProject(project_model)
result = bani_project.run()
```

---

### `SchemaInspector`

Introspect a live database schema without a BDL file.

```python
from bani.sdk.schema_inspector import SchemaInspector
```

#### `SchemaInspector.inspect(...) -> DatabaseSchema`

```python
schema = SchemaInspector.inspect(
    dialect="postgresql",
    host="localhost",
    port=5432,
    database="mydb",
    username_env="PG_USER",
    password_env="PG_PASS",
)

for table in schema.tables:
    print(f"{table.schema_name}.{table.table_name}")
    for col in table.columns:
        print(f"  {col.name}: {col.data_type} (nullable={col.nullable})")
    print(f"  PK: {table.primary_key}")
    print(f"  Estimated rows: {table.row_count_estimate}")
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dialect` | `str` | Required | Database dialect (e.g. `"postgresql"`, `"mysql"`). |
| `host` | `str` | `""` | Database host. |
| `port` | `int` | `0` | Database port. |
| `database` | `str` | `""` | Database name. |
| `username_env` | `str` | `""` | Environment variable name for username. |
| `password_env` | `str` | `""` | Environment variable name for password. |
| `**kwargs` | `Any` | -- | Additional connector-specific arguments. |

**Returns:** `DatabaseSchema` with all tables, columns, indexes, and foreign keys.

---

## Result Types

### `MigrationResult`

Returned by `BaniProject.run()`.

| Field | Type | Description |
|---|---|---|
| `project_name` | `str` | Name of the migration project. |
| `tables_completed` | `int` | Number of tables successfully migrated. |
| `tables_failed` | `int` | Number of tables that failed. |
| `total_rows_read` | `int` | Total rows read from source. |
| `total_rows_written` | `int` | Total rows written to target. |
| `duration_seconds` | `float` | Total execution time. |
| `errors` | `tuple[str, ...]` | Error messages from failed tables. |
| `warnings` | `tuple[str, ...]` | Warning messages. |

### `PreviewResult`

Returned by `BaniProject.preview()`.

| Field | Type | Description |
|---|---|---|
| `source_dialect` | `str` | Source database dialect. |
| `tables` | `list[TablePreview]` | Preview data for each table. |

---

## Complete Example

```python
import os
from bani.sdk.project_builder import ProjectBuilder
from bani.sdk.bani import BaniProject

# Set credentials
os.environ["SRC_USER"] = "root"
os.environ["SRC_PASS"] = "password"
os.environ["TGT_USER"] = "pguser"
os.environ["TGT_PASS"] = "pgpass"

# Build project
model = (
    ProjectBuilder("mysql-to-pg")
    .source(
        dialect="mysql",
        host="localhost",
        port=3306,
        database="source_db",
        username_env="SRC_USER",
        password_env="SRC_PASS",
    )
    .target(
        dialect="postgresql",
        host="localhost",
        port=5432,
        database="target_db",
        username_env="TGT_USER",
        password_env="TGT_PASS",
    )
    .batch_size(50_000)
    .parallel_workers(2)
    .build()
)

# Execute
project = BaniProject(model)

def on_progress(event):
    print(f"Progress: {event}")

result = project.run(on_progress=on_progress)
print(f"Done: {result.tables_completed} tables, "
      f"{result.total_rows_written:,} rows in {result.duration_seconds:.1f}s")
if result.tables_failed > 0:
    print(f"Failed: {result.tables_failed} tables")
    for err in result.errors:
        print(f"  - {err}")
```
