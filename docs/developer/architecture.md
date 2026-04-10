# Architecture

Bani follows a hexagonal (ports and adapters) architecture with strict layer boundaries and Apache Arrow as the universal data interchange format.

---

## Layer Diagram

```
┌─────────────────────────────────────────────────────┐
│                    Interfaces                        │
│  CLI (Typer)  │  SDK  │  MCP Server  │  Web UI  │  │
│               │       │  (FastAPI)   │  (React) │  │
│  Desktop App  │       │              │          │  │
└────────────────────────┬────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────┐
│                   Application                        │
│  Orchestrator  │  Progress  │  Checkpoint  │  Hooks  │
│  Preview       │  Scheduler │  Dependency Resolver   │
└────────────────────────┬────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────┐
│                   Connectors                         │
│  PostgreSQL  │  MySQL  │  MSSQL  │  Oracle  │ SQLite │
│  (Source + Sink implementations)                     │
└────────────────────────┬────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────┐
│                  Infrastructure                      │
│  Config Loader  │  Connections Registry  │  Logging  │
│  Filesystem     │  OS Scheduler Bridge              │
└────────────────────────┬────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────┐
│                     Domain                           │
│  ProjectModel  │  DatabaseSchema  │  Errors          │
│  ConnectionConfig  │  TableMapping  │  Enums          │
│  (Pure business logic — zero external imports)       │
└─────────────────────────────────────────────────────┘
```

---

## Layer Rules

### Domain Layer

Location: `src/bani/domain/`

Contains pure business logic with **zero imports** from infrastructure, connectors, CLI, SDK, MCP, or UI. The domain defines:

- `ProjectModel` and related dataclasses (`ConnectionConfig`, `TableMapping`, `ProjectOptions`, etc.)
- `DatabaseSchema` with `TableDefinition`, `ColumnDefinition`, `IndexDefinition`, `ForeignKeyDefinition`
- Exception hierarchy (`BaniError` and all subclasses)
- Enums (`SyncStrategy`, `WriteStrategy`, `ErrorHandlingStrategy`)

All dataclasses are `frozen=True` and use tuples (not lists) for collection fields, ensuring immutability and thread safety.

### Application Layer

Location: `src/bani/application/`

Orchestrates the migration workflow. Contains:

- **`MigrationOrchestrator`** -- Coordinates the full migration lifecycle: introspect, plan, create tables, transfer data, create indexes/FKs.
- **`ProgressTracker`** -- Event-based progress reporting with typed events (`MigrationStarted`, `TableStarted`, `BatchComplete`, etc.).
- **`CheckpointManager`** -- Saves and loads migration state for resumability.
- **`DependencyResolver`** -- Topologically sorts tables based on FK dependencies.
- **`preview_source()`** -- Samples rows from the source for preview.

### Connectors Layer

Location: `src/bani/connectors/`

Five database connector implementations, each in its own subpackage:

```
connectors/
├── base.py           # SourceConnector and SinkConnector ABCs
├── registry.py       # Entry-point-based connector discovery
├── pool.py           # Generic connection pool
├── postgresql/
│   ├── connector.py
│   ├── schema_reader.py
│   ├── data_reader.py
│   ├── data_writer.py
│   └── type_mapper.py
├── mysql/
├── mssql/
├── oracle/
└── sqlite/
```

### Infrastructure Layer

Location: `src/bani/infra/`

External concerns: configuration loading, filesystem access, logging, OS scheduler integration, named connections registry.

### Interface Layer

Multiple entry points that consume the application layer:

- **CLI** (`src/bani/cli/`) -- Typer-based with Rich formatting
- **SDK** (`src/bani/sdk/`) -- `Bani`, `BaniProject`, `ProjectBuilder`, `SchemaInspector`
- **MCP Server** (`src/bani/mcp_server/`) -- 10 tools for AI agent integration
- **Web UI** (`src/bani/ui/`) -- FastAPI backend with SSE progress streaming
- **Desktop App** (`src/bani/desktop/`) -- macOS menu bar application

---

## Arrow Interchange Invariant

Data flows exclusively as `pyarrow.RecordBatch` between connectors. This is a core architectural invariant:

```
Source.read_table() --> Iterator[RecordBatch] --> Sink.write_batch()
```

No intermediate Pandas DataFrames, dict-of-lists, CSV strings, or ORM objects are used in the data path. This ensures:

- **Zero-copy potential** between source and sink
- **Columnar memory layout** for efficient batch processing
- **Type safety** through Arrow's type system

### Arrow as Canonical Type Intermediate

Source connectors populate `ColumnDefinition.arrow_type_str` during introspection via `str(pa_type)`. Sink connectors call `from_arrow_type(arrow_type_str)` to generate native DDL types.

This gives **N mappers** (one per connector) instead of **N*N** cross-database translation tables:

```
Source A type  -->  Arrow type string  -->  Target B DDL type
     (A.map_type_name)              (B.from_arrow_type)
```

---

## Connector Discovery

Connectors register via Python entry points in `pyproject.toml`:

```toml
[project.entry-points."bani.connectors"]
mysql = "bani.connectors.mysql:MySQLConnector"
postgresql = "bani.connectors.postgresql:PostgreSQLConnector"
mssql = "bani.connectors.mssql:MSSQLConnector"
oracle = "bani.connectors.oracle:OracleConnector"
sqlite = "bani.connectors.sqlite:SQLiteConnector"
```

The `ConnectorRegistry` class discovers connectors via `importlib.metadata.entry_points()`. The orchestrator and SDK never reference a concrete connector class -- they use `ConnectorRegistry.get(dialect)` to obtain the class dynamically.

Third-party connectors can be added by registering an entry point under the `bani.connectors` group:

```toml
[project.entry-points."bani.connectors"]
snowflake = "my_package.connectors.snowflake:SnowflakeConnector"
```

---

## I/O Pipeline

The orchestrator uses a producer/consumer pattern for data transfer:

1. **Producer thread:** `SourceConnector.read_table()` yields `RecordBatch` objects and puts them on a queue.
2. **Consumer thread:** `SinkConnector.write_batch()` takes batches from the queue and writes them.

This overlaps source reads with target writes for maximum throughput.

### Chunk-Level Parallelism

Tables with more than 50k rows and a single integer primary key are split into range-based chunks. Each chunk is transferred concurrently via `ThreadPoolExecutor`, enabling parallelism within a single large table.

### Memory Management

Between table transfers, the orchestrator runs `gc.collect()` and `pa.default_memory_pool().release_unused()` to prevent cumulative memory pressure from Arrow allocations.

---

## BDL Processing Pipeline

```
BDL file (XML/JSON)
    |
    v
bdl.parser.parse()  -->  ProjectModel
    |
    v
bdl.validator.validate_xml/json()  -->  list[str] errors
    |
    v
bdl.interpolator.interpolate()  -->  Resolved ${env:VAR} references
```

The parser supports both XML and JSON formats. XML uses `xml.etree.ElementTree` with namespace handling. JSON uses `jsonschema` for validation.
