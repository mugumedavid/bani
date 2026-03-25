# Bani — Build Prompt

> **Purpose of this document:** This is an AI-consumable engineering prompt. Feed this entire file to an AI coding assistant (Claude Code, etc.) and it will have the context needed to scaffold, implement, and iterate on the Bani project from zero to a shippable product.

---

## 1. What is Bani?

Bani is an open-source, cross-platform database conversion and migration engine written in Python. It is spiritually equivalent to Spectral Core's **FullConvert** — a commercial tool that copies tables, indexes, foreign keys, and data between 40+ database engines — but Bani departs from FullConvert in several deliberate ways:

| Concern | FullConvert | Bani |
|---|---|---|
| Language | C++ / Delphi (proprietary) | Python 3.12+ |
| Data transport layer | Custom internal engine | **Apache Arrow** (columnar, zero-copy, SIMD-optimised) |
| Automation interface | CLI flags + saved project files | **BDL (Bani Definition Language)** — an XML schema that any external party (AI agents, CI/CD pipelines, other systems, humans) can author |
| Platform | Windows-first, Linux via virtualisation | True cross-platform (Windows, macOS, Linux) with **self-contained Python runtimes** per installer |
| Connector shipping | Built-in, closed | **Modular connector architecture** — ships with MySQL, PostgreSQL, MSSQL, Oracle, and SQLite; community adds more |
| AI agent integration | None | **MCP server**, **Python SDK**, **JSON-mode CLI output**, and **BDL example library** — Bani is agent-native from v1.0 |
| Container support | None | Official **Docker image** with variants for general use, minimal base, and MCP-server-as-entrypoint |
| Management interface | Desktop GUI (Windows) | **Web UI** (React SPA served locally) + CLI + Python SDK + MCP server |
| Governance | Commercial / closed-source | Open-source (Apache-2.0) with community maintainership |
| Code philosophy | Unknown internals | **Clean Code** (Robert C. Martin's principles) enforced by linters, architectural rules, and review norms |

### 1.1 Core Capabilities (Feature Parity with FullConvert)

Bani must deliver every capability listed below. Each item maps to a FullConvert feature that users depend on.

1. **Schema introspection** — connect to a source database and discover all tables, columns (with data types), primary keys, indexes (unique, composite, filtered), foreign-key relationships, check constraints, default values, and sequences / auto-increment settings.
2. **Schema translation** — produce a semantically equivalent schema in the target database dialect, applying an extensible **data-type mapping ruleset** (e.g., `VARCHAR(MAX)` in MSSQL → `TEXT` in PostgreSQL, `NUMBER(10,2)` in Oracle → `NUMERIC(10,2)` in PostgreSQL, `REAL` in SQLite → `DOUBLE PRECISION` in PostgreSQL). Users and BDL files can override any mapping.
3. **Data transfer** — stream rows from source to target in configurable batches using Apache Arrow record batches. Must sustain **millions of rows per minute** on commodity hardware and scale linearly with CPU cores.
4. **Index and constraint creation** — after data is loaded, create all indexes and foreign keys on the target. Ordering must respect dependency graphs (e.g., a foreign key referencing another table that hasn't been created yet must be deferred).
5. **Incremental / differential sync** — given a previously completed migration, detect changed rows (via timestamps, row versions, or checksums) and apply only deltas.
6. **Custom data-type mappings** — a rule engine that lets users override how any source type maps to a target type. Rules are expressible in BDL and stored per-project.
7. **Pre- and post-migration hooks** — run arbitrary SQL or shell commands before and after each table, or before and after the entire migration.
8. **Scheduling** — a built-in cron-like scheduler (or OS-native integration) so migrations can run unattended at specified intervals.
9. **Parallel table transfer** — migrate multiple tables concurrently, with configurable concurrency limits.
10. **Progress reporting and logging** — real-time progress (rows transferred, throughput, ETA) via CLI, log files, and a machine-readable event stream (JSON lines) that UIs or monitoring tools can consume.
11. **Error handling and resumability** — on failure, log the exact row/batch that failed, allow the user to resume from that point, and optionally skip bad rows into a quarantine table.
12. **Data preview** — before committing a migration, allow sampling N rows from each table to verify correctness.
13. **Project persistence** — save all migration settings (source, target, selected tables, mapping overrides, hooks, schedule) as a BDL file that can be reloaded, version-controlled, and shared.

---

## 2. Execution Contract for the AI Assistant

This section defines how the AI coding assistant must work when implementing Bani. These rules are non-negotiable.

### 2.1 Priority Contract

When requirements conflict or trade-offs must be made, resolve them in this order (highest priority first):

1. **Security correctness** — credentials are never leaked, inputs are validated, transport is encrypted.
2. **Data correctness** — rows transferred match the source exactly; schema translation preserves semantics.
3. **Resumability** — failures are recoverable; no silent data loss.
4. **Performance** — throughput targets are met within the benchmark contract.
5. **Feature breadth** — more connectors, more sync strategies, more packaging formats.

If you must choose between, say, adding a fifth connector and fixing a data-correctness bug, fix the bug first.

### 2.2 General Rules

1. **Follow the phase order** — implement Bani in the sequence defined in Section 23 (Implementation Order). Do not skip ahead to a later phase. Each phase builds on the outputs of the previous one.
2. **One phase at a time** — complete a phase fully (see Definition of Done in Section 23) before starting the next. If a phase's exit criteria cannot be met, stop and report the blocker rather than moving on.
3. **Tests are not optional** — every piece of functionality must have corresponding tests before the phase is considered complete. Untested code is unfinished code.
4. **No silent architectural deviations** — if you believe the architecture described in this prompt should be changed (e.g., a different library, a different module boundary, a different interface), stop and explain why before making the change. Do not quietly substitute a different approach. Log the deviation in `docs/decisions.md` (see Section 2.6).
5. **Keep the domain layer pure** — the `domain/` package must have zero imports from `infrastructure/`, `connectors/`, `cli/`, `sdk/`, or `mcp_server/`. If you find yourself importing from an outer layer into the domain, refactor.
6. **Commit discipline** — use Conventional Commits (`feat:`, `fix:`, `test:`, `refactor:`, `docs:`, `ci:`, `chore:`). Each commit should be atomic and pass all tests.
7. **Ask, don't guess** — if a requirement is ambiguous or contradictory, ask for clarification rather than making assumptions. This prompt is large; contradictions may exist.

### 2.3 Explicit Non-Goals for v1.0

Do not implement these, even if the prompt describes them in architectural terms. They exist in the prompt only so the architecture doesn't preclude them. See Section 22 for the full roadmap and which items are open-source vs enterprise.

- Standalone headless REST / gRPC API (Section 22 item 1 — open-source, post-v1.0; the embedded FastAPI backend that serves the Web UI in Section 20 is allowed in v1.0)
- Arrow Flight remote sources
- View / stored procedure migration
- Data masking / anonymisation
- Schema diff (`bani schema diff` is listed in the CLI as "future")
- Additional connectors beyond MySQL, PostgreSQL, MSSQL, Oracle, SQLite
- Webhook notifications (enterprise)
- Advanced MCP capabilities (enterprise)
- Secrets manager integration (enterprise)
- RBAC and audit logging (enterprise)

If you find yourself building toward any of these, stop. You've gone off track.

### 2.4 Quality Gates (Apply to Every Phase)

- `ruff check` and `ruff format --check` pass with zero violations.
- `mypy --strict` passes with zero errors.
- `pytest` passes with zero failures.
- Test coverage meets the phase-specific threshold (see Section 23).
- No `# type: ignore` without an accompanying comment explaining why.
- No `TODO` or `FIXME` comments left unresolved within the phase's scope (future-phase TODOs are acceptable if clearly labelled).

### 2.5 When You Get Stuck

If you encounter a situation where:
- A library doesn't behave as this prompt assumes → report the discrepancy and propose an alternative. Log the decision in `docs/decisions.md`.
- A performance target seems unreachable → report your measured numbers and the conditions under which you measured them.
- Two sections of this prompt contradict each other → flag both sections and ask which takes precedence.
- A feature is more complex than expected → propose a simplification that preserves the user-facing contract.

### 2.6 Decision Log

Maintain a file at `docs/decisions.md` with the following format for each entry:

```markdown
## DECISION-NNN: <Short title>

- **Date:** YYYY-MM-DD
- **Status:** accepted | superseded | rejected
- **Context:** Why this decision was needed.
- **Decision:** What was decided.
- **Alternatives considered:** What else was evaluated and why it was rejected.
- **Consequences:** What changes as a result.
```

Log an entry whenever you: deviate from the architecture in this prompt, choose between competing library options, simplify a requirement, or make any decision that a future contributor would want to understand.

---

## 3. Architecture Overview

Bani follows a **hexagonal (ports-and-adapters) architecture** to cleanly separate the migration domain logic from infrastructure concerns (database drivers, file I/O, scheduling, UI).

```
┌──────────────────────────────────────────────────────────────────┐
│                        Presentation Layer                        │
│  ┌────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────┐ ┌────────────────┐  │
│  │  CLI   │ │   MCP    │ │  Python SDK  │ │  Web UI  │ │ Event Stream   │  │
│  │ (Rich) │ │  Server  │ │  (bani.sdk)  │ │ (React)  │ │ (JSON-L)       │  │
│  └───┬────┘ └────┬─────┘ └──────┬───────┘ └────┬─────┘ └───────┬────────┘  │
│      │           │              │                   │            │
│      └───────────┼──────────────┼───────────────────┘            │
│                       ▼                                          │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │                   Application Service Layer                  ││
│  │                                                              ││
│  │  ProjectManager · MigrationOrchestrator · SyncEngine         ││
│  │  SchedulerService · HookRunner · ProgressTracker             ││
│  └──────────────────────────┬───────────────────────────────────┘│
│                             │                                    │
│  ┌──────────────────────────▼───────────────────────────────────┐│
│  │                    Domain / Core Layer                        ││
│  │                                                              ││
│  │  SchemaModel · TypeMapper · DependencyResolver               ││
│  │  BatchTransferPipeline · DeltaDetector · DataValidator       ││
│  │  BDLParser · BDLSerializer                                   ││
│  └──────────────────────────┬───────────────────────────────────┘│
│                             │                                    │
│  ┌──────────────────────────▼───────────────────────────────────┐│
│  │                   Infrastructure / Adapters                   ││
│  │                                                              ││
│  │  ┌─────────┐ ┌────────────┐ ┌─────────┐ ┌────────┐ ┌────────┐││
│  │  │  MySQL  │ │ PostgreSQL │ │  MSSQL  │ │ Oracle │ │ SQLite │││
│  │  └────┬────┘ └─────┬──────┘ └────┬────┘ └───┬────┘ └───┬────┘││
│  │       │            │             │           │          │     ││
│  │       └────────────┼─────────────┼───────────┼──────────┘     ││
│  │                    ▼                                          ││
│  │              Apache Arrow RecordBatch Bus                    ││
│  │                                                              ││
│  │  FileSystem · Logging · OS Scheduler · Config                ││
│  └──────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────┘
```

### 3.1 Architectural Rules

1. **Dependency direction** — outer layers depend on inner layers, never the reverse. The Domain layer has zero imports from Infrastructure.
2. **Ports are abstract base classes** — every connector implements `SourceConnector` and/or `SinkConnector`. The orchestrator never references a concrete connector.
3. **Arrow as the universal interchange format** — data leaves a source connector as `pyarrow.RecordBatch` and arrives at a sink connector as `pyarrow.RecordBatch`. No intermediate Pandas or dict-of-lists representations.
4. **Plugin discovery via entry points** — connectors register themselves using Python `importlib.metadata` entry points so third-party packages can add new databases without modifying core code.

---

## 4. Data Engine — Apache Arrow

### 4.1 Why Arrow?

Bani's central performance requirement is moving millions of records between heterogeneous databases as fast as the network and disk allow. Apache Arrow is the correct choice because:

- **Columnar, zero-copy memory layout** — data read from a source can be written to a target without serialisation/deserialisation overhead.
- **SIMD-vectorised compute kernels** — type casting, null handling, and filtering happen at hardware speed.
- **Language-agnostic IPC** — if a future connector is written in Rust or C++, it can exchange Arrow buffers with the Python process with no copying.
- **PyArrow is mature** — first-class Python bindings, Apache-licensed, actively maintained.
- **Arrow Flight** — when Bani eventually supports remote sources (cloud databases, data lakes), Arrow Flight provides a high-throughput RPC framework that has been shown to deliver 20–50× better performance over ODBC.

### 4.2 Data Pipeline

```
Source DB
  │
  ▼
Source Connector  ──▶  pyarrow.RecordBatch (N rows)
                            │
                            ▼
                    Transform Pipeline (optional)
                    ┌──────────────────────────┐
                    │ • Type casting            │
                    │ • Column renaming         │
                    │ • Expression evaluation   │
                    │ • Null coercion           │
                    │ • Row filtering           │
                    └──────────┬───────────────┘
                               │
                               ▼
                    pyarrow.RecordBatch (transformed)
                               │
                               ▼
                    Sink Connector  ──▶  Target DB
```

### 4.3 Performance Targets

| Metric | Baseline Floor (must pass) | Stretch Target (aim for) |
|---|---|---|
| Throughput (wide table, 50 cols, local) | ≥ 200,000 rows/sec per core | ≥ 500,000 rows/sec per core |
| Throughput (narrow table, 5 cols, local) | ≥ 800,000 rows/sec per core | ≥ 2,000,000 rows/sec per core |
| Memory ceiling per worker | Configurable, default 512 MB | Same |
| Batch size | Configurable, default 100,000 rows | Same |
| Parallel workers | Default = CPU core count, configurable | Same |

The **baseline floor** is the hard gate for release — if throughput falls below it, the release is blocked until the regression is investigated. The **stretch target** is the aspiration — hitting it is celebrated but not required for release. Both are measured under the conditions defined in Section 4.4 (Benchmark Contract).

### 4.4 Benchmark Contract

The performance targets in Section 4.3 are measured against a **reference benchmark** defined here. All performance claims, CI regression checks, and documentation numbers must cite results from this benchmark.

**Reference hardware class:**

- CPU: 4-core x86_64 (e.g., GitHub Actions `ubuntu-latest` runner or equivalent)
- RAM: 16 GB
- Storage: SSD (local)
- Network: loopback (source and target databases run on the same host or in sibling Docker containers on the same Docker network)

**Reference schemas:**

1. **Wide table** — `bench_wide`: 50 columns (10× `INTEGER`, 10× `VARCHAR(255)`, 10× `DECIMAL(12,4)`, 10× `TIMESTAMP`, 5× `TEXT` avg 200 chars, 5× `BOOLEAN`). 1,000,000 rows.
2. **Narrow table** — `bench_narrow`: 5 columns (`id INTEGER PK`, `name VARCHAR(100)`, `value DOUBLE`, `created_at TIMESTAMP`, `active BOOLEAN`). 5,000,000 rows.

**Measurement rules:**

- Throughput = total rows transferred ÷ wall-clock seconds (from first batch read to last batch committed).
- Each benchmark run is executed 3 times; the **median** is reported.
- Database setup (schema creation, data seeding) is excluded from the measurement.
- Source and target are both PostgreSQL (the simplest connector pair).
- A single worker core is used for the per-core targets; a separate multi-core run with `workers = CPU count` measures parallel scaling.

**CI integration:**

- The benchmark suite lives in `benchmarks/` and runs at the release CI tier (see Section 15.3).
- A CI job compares the median throughput against both targets:
  - **Below baseline floor** → hard failure. Release is blocked.
  - **Between floor and stretch** → warning. Release proceeds with a note.
  - **At or above stretch** → pass.
- If throughput drops >10% from the previous release (even if still above floor), the job emits a regression warning for investigation.

### 4.5 Performance Levers

The following are the primary levers for reaching stretch targets. This list is not prescriptive — the right combination depends on profiling results — but it provides a checklist to work through when throughput meets the floor but misses the stretch:

1. **Batch size tuning** — larger batches amortise per-batch overhead (connection round-trips, commit latency) but increase memory pressure. Tune relative to `memoryLimitMB`.
2. **Connection pooling** — each parallel worker should hold a dedicated connection to both source and target. Avoid contention on shared connection objects.
3. **Write-side strategy** — the optimal insert method is connector-dependent: PostgreSQL's `COPY ... FROM STDIN (FORMAT binary)` is significantly faster than multi-row `INSERT`; MySQL benefits from `LOAD DATA LOCAL INFILE` or batched `INSERT ... VALUES` with large packet sizes; MSSQL performs best with `bcp`-style bulk insert via `pyodbc.fast_executemany`. Each connector should implement the fastest available path.
4. **I/O pipeline overlap** — read the next batch from the source while the current batch is being written to the target. A simple producer/consumer queue between the read and write stages can hide network latency on both sides.
5. **Memory pressure backoff** — if the process approaches `memoryLimitMB`, reduce batch size dynamically rather than OOM-killing. Log the backoff event so users can tune their configuration.
6. **Arrow zero-copy path** — avoid materialising intermediate Python objects. Data should flow from the source driver's wire format → Arrow RecordBatch → sink driver's wire format with no `dict`, `list`, or Pandas intermediary.

---

## 5. BDL — Bani Definition Language

BDL is an XML vocabulary that fully describes a migration project. Any party — a human editing XML, an AI agent generating XML, a CI pipeline templating XML — can produce a valid BDL document, hand it to Bani, and the engine executes it deterministically.

### 5.1 Design Goals

1. **Self-contained** — a single BDL file holds everything Bani needs: connection configuration (host, port, database, and credential references via `${env:...}`), table selections, mapping overrides, hooks, schedule, and options.
2. **Validatable** — an XSD (XML Schema Definition) and a JSON Schema ship with Bani. BDL files can be validated in either XML or JSON form before execution.
3. **Dual-format** — the canonical format is XML, but Bani also accepts a JSON representation of the same structure (see Section 18.5). This makes BDL more accessible to AI agents, which work better with JSON.
4. **Extensible** — connector-specific configuration lives in `<connectorConfig>` blocks whose internal schema is owned by the connector, not by BDL core.
5. **Human-readable** — element and attribute names use full English words, not abbreviations.
6. **Versionable** — includes a `schemaVersion` attribute; Bani validates compatibility and can migrate old BDL files forward.

### 5.2 Reference BDL Document

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0"
      xmlns="https://bani.dev/bdl/1.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="https://bani.dev/bdl/1.0 bdl-1.0.xsd">

  <!-- ─── Project Metadata ─────────────────────────────────── -->
  <project name="legacy-to-postgres"
           description="Nightly migration from MSSQL ERP to PostgreSQL analytics"
           author="ops-team"
           created="2026-03-19T10:00:00Z">
    <tags>
      <tag>erp</tag>
      <tag>nightly</tag>
    </tags>
  </project>

  <!-- ─── Source Connection ────────────────────────────────── -->
  <source connector="mssql">
    <connection
      host="erp-db.internal"
      port="1433"
      database="ERP_Production"
      username="${env:MSSQL_USER}"
      password="${env:MSSQL_PASS}"
      encrypt="true"
    />
    <connectorConfig>
      <!-- Connector-specific options (MSSQL) -->
      <option name="applicationIntent" value="ReadOnly" />
      <option name="commandTimeout" value="300" />
    </connectorConfig>
  </source>

  <!-- ─── Target Connection ────────────────────────────────── -->
  <target connector="postgresql">
    <connection
      host="analytics-pg.internal"
      port="5432"
      database="analytics"
      username="${env:PG_USER}"
      password="${env:PG_PASS}"
      sslMode="require"
    />
    <connectorConfig>
      <option name="targetSchema" value="erp_mirror" />
    </connectorConfig>
  </target>

  <!-- ─── Global Options ───────────────────────────────────── -->
  <options>
    <batchSize>100000</batchSize>
    <parallelWorkers>4</parallelWorkers>
    <memoryLimitMB>2048</memoryLimitMB>
    <onError>log-and-continue</onError>        <!-- or "abort" -->
    <createTargetSchema>true</createTargetSchema>
    <dropTargetTablesFirst>false</dropTargetTablesFirst>
    <transferIndexes>true</transferIndexes>
    <transferForeignKeys>true</transferForeignKeys>
    <transferDefaults>true</transferDefaults>
    <transferCheckConstraints>true</transferCheckConstraints>
  </options>

  <!-- ─── Type Mapping Overrides ───────────────────────────── -->
  <typeMappings>
    <mapping source="NVARCHAR(MAX)" target="TEXT" />
    <mapping source="DATETIME2"     target="TIMESTAMP WITH TIME ZONE" />
    <mapping source="MONEY"         target="NUMERIC(19,4)" />
    <mapping source="BIT"           target="BOOLEAN" />
    <!-- Oracle examples -->
    <mapping source="NUMBER(10,0)"  target="BIGINT" />
    <mapping source="VARCHAR2(4000)" target="TEXT" />
    <mapping source="CLOB"          target="TEXT" />
    <!-- SQLite examples (SQLite has flexible typing) -->
    <mapping source="INTEGER"       target="BIGINT" />
    <!-- Fallback: unmapped types use the connector's built-in defaults -->
  </typeMappings>

  <!-- ─── Table Selections ─────────────────────────────────── -->
  <tables mode="include">   <!-- "include" = only listed; "exclude" = all except listed -->

    <table sourceSchema="dbo" sourceName="Customers" targetName="customers">
      <columnMappings>
        <column source="CustID"    target="customer_id" />
        <column source="CustName"  target="customer_name" />
        <column source="IsActive"  target="is_active" targetType="BOOLEAN" />
      </columnMappings>
    </table>

    <table sourceSchema="dbo" sourceName="Orders" targetName="orders">
      <!-- No column mappings = transfer all columns with automatic naming -->
      <filter>WHERE OrderDate >= '2024-01-01'</filter>
    </table>

    <table sourceSchema="dbo" sourceName="OrderItems" targetName="order_items" />
    <table sourceSchema="dbo" sourceName="Products"   targetName="products" />

  </tables>

  <!-- ─── Hooks ────────────────────────────────────────────── -->
  <hooks>
    <hook event="before-migration" type="sql" target="target">
      CREATE SCHEMA IF NOT EXISTS erp_mirror;
    </hook>
    <hook event="after-table" tableName="customers" type="sql" target="target">
      CREATE INDEX IF NOT EXISTS idx_customers_name ON erp_mirror.customers (customer_name);
    </hook>
    <hook event="after-migration" type="shell">
      notify-ops --channel=#data-eng --message="ERP mirror refresh complete"
    </hook>
  </hooks>

  <!-- ─── Schedule (optional) ──────────────────────────────── -->
  <schedule enabled="true">
    <cron>0 2 * * *</cron>   <!-- Every day at 02:00 -->
    <timezone>Africa/Nairobi</timezone>
    <retryOnFailure maxRetries="3" delaySeconds="300" />
  </schedule>

  <!-- ─── Sync Mode (optional, for incremental) ────────────── -->
  <sync enabled="false">
    <strategy>timestamp</strategy>  <!-- "timestamp" | "rowversion" | "checksum" -->
    <trackingColumn table="Orders" column="ModifiedAt" />
    <trackingColumn table="Customers" column="UpdatedAt" />
  </sync>

</bani>
```

### 5.3 BDL Processing Pipeline

```
BDL File (XML or JSON)
  │
  ▼
Format Detection  ──▶  XML path or JSON path
  │
  ▼
Schema Validation  (XSD for XML, JSON Schema for JSON)  ──▶  reject if invalid
  │
  ▼
Environment Variable Interpolation  (${env:VAR})
  │
  ▼
BDL Parser  ──▶  ProjectModel (domain object)
  │
  ▼
MigrationOrchestrator.execute(project)
```

### 5.4 BDL XSD

Ship a formal XSD file (`bdl-1.0.xsd`) and a companion JSON Schema file (`bdl-1.0.schema.json`) that:

- Enumerate every element and attribute.
- Define enumerations for closed sets (`onError`: `abort | log-and-continue`; `strategy`: `timestamp | rowversion | checksum`).
- Mark required vs. optional elements.
- Document each element with `<xs:annotation>` blocks (XSD) and `"description"` fields (JSON Schema).
- Are themselves version-controlled alongside the Bani source.
- The JSON Schema is auto-generated from the XSD (or maintained in sync) to ensure both formats accept exactly the same structure. See Section 18.5 for the rationale behind dual-format support.

---

## 6. Connector System

### 6.1 Connector Interface (Port)

Every database connector must implement two abstract base classes:

```python
from abc import ABC, abstractmethod
from typing import Iterator, Optional
import pyarrow as pa

class SourceConnector(ABC):
    """Port: reads schema and data from a source database."""

    @abstractmethod
    def connect(self, config: ConnectionConfig) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def introspect_schema(self) -> DatabaseSchema: ...

    @abstractmethod
    def read_table(
        self,
        table: TableRef,
        columns: Optional[list[str]] = None,
        filter_sql: Optional[str] = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]: ...

    @abstractmethod
    def estimate_row_count(self, table: TableRef) -> int: ...


class SinkConnector(ABC):
    """Port: writes schema and data to a target database."""

    @abstractmethod
    def connect(self, config: ConnectionConfig) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def create_table(self, table_def: TableDefinition) -> None: ...

    @abstractmethod
    def write_batch(self, table: TableRef, batch: pa.RecordBatch) -> int: ...

    @abstractmethod
    def create_indexes(self, table: TableRef, indexes: list[IndexDef]) -> None: ...

    @abstractmethod
    def create_foreign_keys(self, fks: list[ForeignKeyDef]) -> None: ...

    @abstractmethod
    def execute_sql(self, sql: str) -> None: ...
```

### 6.2 Connector Registration

Connectors register via Python entry points in their `pyproject.toml`:

```toml
[project.entry-points."bani.connectors"]
mysql      = "bani.connectors.mysql:MySQLConnector"
postgresql = "bani.connectors.postgresql:PostgreSQLConnector"
mssql      = "bani.connectors.mssql:MSSQLConnector"
oracle     = "bani.connectors.oracle:OracleConnector"
sqlite     = "bani.connectors.sqlite:SQLiteConnector"
```

At runtime, `ConnectorRegistry` discovers all installed connectors:

```python
from importlib.metadata import entry_points

class ConnectorRegistry:
    @staticmethod
    def discover() -> dict[str, type]:
        eps = entry_points(group="bani.connectors")
        return {ep.name: ep.load() for ep in eps}
```

### 6.3 Initial Connectors (v1.0)

| Connector | Python Driver | Notes |
|---|---|---|
| MySQL | `mysql-connector-python` or `PyMySQL` + `PyArrow` | Use server-side cursors for large tables |
| PostgreSQL | `psycopg` (v3, async) + `COPY ... TO STDOUT (FORMAT binary)` | Postgres binary COPY → Arrow is extremely fast |
| MSSQL | `pyodbc` + FreeTDS or `pymssql` | Support Windows integrated auth and SQL auth |
| Oracle | `python-oracledb` (thin mode, no Oracle Client needed) | Supports Oracle 12c+ ; thin mode eliminates native lib dependency |
| SQLite | `sqlite3` (stdlib) + `apsw` for advanced features | File-based; no server; useful for embedded / edge migrations |

### 6.3.1 Connector Versioning and Database Version Support

> **Complexity note:** This subsection describes the most architecturally complex component in the connector system — namespace-isolated driver loading with simultaneous multi-version support. It underpins all connectors and must be implemented early (Phase 2). Budget accordingly.

Different versions of Python database drivers support different versions of the underlying database server. To ensure broad compatibility with the most popular database versions in active use, Bani adopts a **multi-version connector strategy**:

1. **Version matrix** — each connector's `connector.yaml` manifest declares a `supported_db_versions` map that pairs driver versions to the database server versions they support. Example:

   ```yaml
   # connectors/mysql/connector.yaml
   name: mysql
   driver: mysql-connector-python
   driver_versions:
     - version: "8.4.x"
       supported_db: ["MySQL 8.0", "MySQL 8.4", "MySQL 9.0"]
     - version: "8.0.x"
       supported_db: ["MySQL 5.5", "MySQL 5.6", "MySQL 5.7", "MySQL 8.0"]
   default_driver_version: "8.4.x"
   ```

   ```yaml
   # connectors/postgresql/connector.yaml
   name: postgresql
   driver: psycopg
   driver_versions:
     - version: "3.2.x"
       supported_db: ["PostgreSQL 12", "PostgreSQL 13", "PostgreSQL 14", "PostgreSQL 15", "PostgreSQL 16", "PostgreSQL 17"]
     - version: "3.1.x"
       supported_db: ["PostgreSQL 11", "PostgreSQL 12", "PostgreSQL 13", "PostgreSQL 14", "PostgreSQL 15"]
   default_driver_version: "3.2.x"
   ```

   ```yaml
   # connectors/mssql/connector.yaml
   name: mssql
   driver: pyodbc
   driver_versions:
     - version: "5.2.x"
       supported_db: ["SQL Server 2016", "SQL Server 2017", "SQL Server 2019", "SQL Server 2022"]
     - version: "4.0.x"
       supported_db: ["SQL Server 2012", "SQL Server 2014", "SQL Server 2016", "SQL Server 2017"]
   default_driver_version: "5.2.x"
   ```

   ```yaml
   # connectors/oracle/connector.yaml
   name: oracle
   driver: python-oracledb
   driver_versions:
     - version: "2.x"
       supported_db: ["Oracle 19c", "Oracle 21c", "Oracle 23ai"]
     - version: "1.x"
       supported_db: ["Oracle 12c", "Oracle 18c", "Oracle 19c", "Oracle 21c"]
   default_driver_version: "2.x"
   ```

   ```yaml
   # connectors/sqlite/connector.yaml
   name: sqlite
   driver: sqlite3
   driver_versions:
     - version: "stdlib"
       supported_db: ["SQLite 3.35+"]
     - version: "apsw-3.45.x"
       supported_db: ["SQLite 3.35", "SQLite 3.40", "SQLite 3.45"]
   default_driver_version: "stdlib"
   ```

2. **Bundled driver versions** — Bani ships multiple pinned versions of each Python driver inside the embedded runtime. At connection time, the connector selects the appropriate driver version based on the detected (or user-declared) database server version.

3. **Automatic detection** — when a connector establishes a connection, it queries the server version (e.g., `SELECT version()` for MySQL/PostgreSQL, `SELECT @@VERSION` for MSSQL, `SELECT * FROM V$VERSION` for Oracle, `SELECT sqlite_version()` for SQLite) and selects the most compatible driver version from its bundled set. Users can override this with an explicit `driverVersion` attribute in BDL:

   ```xml
   <connectorConfig>
     <option name="driverVersion" value="8.0.x" />
   </connectorConfig>
   ```

4. **Isolation** — driver versions are installed into separate namespace directories within the bundled environment (`lib/drivers/mysql/8.4.x/`, `lib/drivers/mysql/8.0.x/`, `lib/drivers/oracle/2.x/`, `lib/drivers/oracle/1.x/`, `lib/drivers/sqlite/stdlib/`, etc.) and loaded dynamically to avoid import conflicts.

5. **Simultaneous multi-version loading (same-engine migrations)** — when the source and target are the **same database engine but different server versions**, Bani must load two different driver versions concurrently within a single migration process — one for the source connection and one for the target connection. This is transparent to the user; they simply specify the source and target connections and Bani handles the rest.

   **Example — MySQL 5.5 → MySQL 8.4:**

   ```xml
   <source connector="mysql">
     <connection host="legacy-db" port="3306" database="erp_old" ... />
     <!-- Server reports MySQL 5.5 → Bani selects driver version 8.0.x (supports 5.7/5.5) -->
   </source>
   <target connector="mysql">
     <connection host="new-db" port="3306" database="erp_new" ... />
     <!-- Server reports MySQL 8.4 → Bani selects driver version 8.4.x -->
   </target>
   ```

   At runtime, the orchestrator creates two independent connector instances. Each instance loads its own driver from its isolated namespace directory (`lib/drivers/mysql/8.0.x/` for the source, `lib/drivers/mysql/8.4.x/` for the target). Because the drivers are namespace-isolated, there are no import conflicts even though both are variants of the same Python package. The data flows through Apache Arrow RecordBatches as usual — the driver version difference is invisible to the pipeline.

   **Implementation requirements:**
   - The `ConnectorRegistry` must instantiate connectors with a specific driver version, not a globally loaded module.
   - Each connector instance holds a reference to its own driver module, loaded via `importlib` from the version-specific path.
   - Connection pools, cursors, and all driver objects are scoped to the connector instance — no shared global state between driver versions.
   - The BDL `<connectorConfig>` block on each side (`<source>` and `<target>`) can independently override the `driverVersion` if the user wants manual control.

   **v1.0 testing scope:** For the initial release, the multi-version loading machinery (namespace isolation, per-instance driver loading, simultaneous same-engine migration) must be implemented and tested. However, only the **default driver version** for each connector needs full integration-test coverage across the entire test matrix. Additional driver versions should have at least one integration test proving they can connect and perform a basic migration against their target database server version. Exhaustive cross-version matrix testing can expand incrementally in subsequent releases.

6. **Community connectors** — third-party connectors follow the same convention, declaring their driver version matrix in `connector.yaml` so Bani's registry can display compatibility information via `bani connectors info NAME`.

### 6.4 Adding a New Connector (Community Guide)

A new connector is a Python package that:

1. Depends on `bani-core`.
2. Implements `SourceConnector` and/or `SinkConnector`.
3. Provides a `default_type_mappings.json` for its dialect.
4. Registers via entry points.
5. Ships its own tests against a Docker-based fixture of the database.
6. Provides a `connector.yaml` manifest (name, version, author, supported DB versions) including a **driver version matrix** mapping driver versions to supported database server versions (see Section 6.3.1).
7. If multiple driver versions are supported, bundles them in isolated namespace directories following the same pattern as built-in connectors (`lib/drivers/<name>/<version>/`).

---

## 7. Project Structure

```
bani/
├── pyproject.toml                  # PEP 621 project metadata, entry points
├── LICENSE
├── README.md
├── CONTRIBUTING.md
├── ARCHITECTURE.md                 # This architecture doc, expanded
├── Makefile                        # dev convenience targets
│
├── src/
│   └── bani/
│       ├── __init__.py
│       ├── __main__.py             # CLI entry: `python -m bani`
│       │
│       ├── domain/                 # Pure domain logic, zero infra imports
│       │   ├── __init__.py
│       │   ├── schema.py           # DatabaseSchema, TableDefinition, ColumnDef, IndexDef, FKDef
│       │   ├── type_mapping.py     # TypeMapper, MappingRule, MappingRuleSet
│       │   ├── dependency.py       # DependencyResolver (topological sort for FK ordering)
│       │   ├── delta.py            # DeltaDetector (timestamp, rowversion, checksum strategies)
│       │   ├── project.py          # ProjectModel, MigrationPlan
│       │   ├── pipeline.py         # BatchTransferPipeline, TransformStep
│       │   ├── validator.py        # DataValidator (row sampling, schema drift detection)
│       │   └── errors.py           # Domain-specific exception hierarchy
│       │
│       ├── bdl/                    # BDL parser and serialiser
│       │   ├── __init__.py
│       │   ├── parser.py           # XML → ProjectModel
│       │   ├── serializer.py       # ProjectModel → XML
│       │   ├── interpolator.py     # ${env:VAR} expansion
│       │   ├── validator.py        # XSD validation wrapper
│       │   └── schemas/
│       │       ├── bdl-1.0.xsd
│       │       └── bdl-1.0.schema.json
│       │
│       ├── application/            # Use-case orchestrators
│       │   ├── __init__.py
│       │   ├── orchestrator.py     # MigrationOrchestrator
│       │   ├── sync_engine.py      # IncrementalSyncEngine
│       │   ├── scheduler.py        # SchedulerService (cron integration)
│       │   ├── hook_runner.py      # Pre/post hook execution
│       │   └── progress.py         # ProgressTracker, event emitter
│       │
│       ├── connectors/             # Built-in connector implementations
│       │   ├── __init__.py
│       │   ├── base.py             # SourceConnector, SinkConnector ABCs
│       │   ├── registry.py         # ConnectorRegistry (entry-point discovery)
│       │   ├── mysql/
│       │   │   ├── __init__.py
│       │   │   ├── connector.py
│       │   │   ├── schema_reader.py
│       │   │   ├── data_reader.py
│       │   │   ├── data_writer.py
│       │   │   ├── type_defaults.json
│       │   │   └── tests/
│       │   ├── postgresql/
│       │   │   ├── __init__.py
│       │   │   ├── connector.py
│       │   │   ├── schema_reader.py
│       │   │   ├── data_reader.py
│       │   │   ├── data_writer.py
│       │   │   ├── type_defaults.json
│       │   │   └── tests/
│       │   ├── mssql/
│       │   │   ├── __init__.py
│       │   │   ├── connector.py
│       │   │   ├── schema_reader.py
│       │   │   ├── data_reader.py
│       │   │   ├── data_writer.py
│       │   │   ├── type_defaults.json
│       │   │   └── tests/
│       │   ├── oracle/
│       │   │   ├── __init__.py
│       │   │   ├── connector.py
│       │   │   ├── schema_reader.py
│       │   │   ├── data_reader.py
│       │   │   ├── data_writer.py
│       │   │   ├── type_defaults.json
│       │   │   └── tests/
│       │   └── sqlite/
│       │       ├── __init__.py
│       │       ├── connector.py
│       │       ├── schema_reader.py
│       │       ├── data_reader.py
│       │       ├── data_writer.py
│       │       ├── type_defaults.json
│       │       └── tests/
│       │
│       ├── infra/                  # Infrastructure adapters
│       │   ├── __init__.py
│       │   ├── config.py           # TOML/YAML config loader
│       │   ├── logging.py          # Structured logging (JSON-lines)
│       │   ├── filesystem.py       # File I/O abstractions
│       │   └── os_scheduler.py     # cron / Windows Task Scheduler bridge
│       │
│       ├── cli/                    # CLI presentation layer
│       │   ├── __init__.py
│       │   ├── app.py              # Typer/Click application
│       │   ├── commands/
│       │   │   ├── run.py          # `bani run project.bdl`
│       │   │   ├── validate.py     # `bani validate project.bdl`
│       │   │   ├── preview.py      # `bani preview project.bdl`
│       │   │   ├── schema.py       # `bani schema inspect --source ...`
│       │   │   ├── init.py         # `bani init` — interactive BDL scaffold
│       │   │   ├── connectors.py   # `bani connectors list`
│       │   │   └── mcp_cmd.py      # `bani mcp serve`
│       │   └── formatters.py       # Rich console output
│       │
│       ├── sdk/                    # Public Python SDK (Section 18.3)
│       │   ├── __init__.py
│       │   ├── project_builder.py  # Fluent API for building projects
│       │   └── schema_inspector.py # Programmatic schema introspection
│       │
│       ├── mcp_server/             # MCP server implementation (Section 18.6)
│       │   ├── __init__.py
│       │   ├── server.py           # MCP server entry point
│       │   └── tools.py            # MCP tool definitions → SDK method mapping
│       │
│       └── ui/                     # Web UI (Section 20)
│           ├── __init__.py
│           ├── server.py           # FastAPI app + Uvicorn launcher
│           ├── routes/             # REST API route modules
│           ├── websocket.py        # WebSocket endpoint for live progress
│           └── frontend/           # React SPA source (built at release time)
│               ├── package.json
│               ├── vite.config.ts
│               ├── src/
│               └── dist/           # Built assets (gitignored, populated by CI)
│
├── tests/
│   ├── conftest.py                 # Shared fixtures, Docker DB containers
│   ├── unit/
│   │   ├── domain/
│   │   ├── bdl/
│   │   ├── application/
│   │   ├── sdk/                    # SDK unit tests (Section 15.6)
│   │   ├── mcp_server/             # MCP server unit tests (Section 15.7)
│   │   └── ui/                     # Web UI unit tests
│   ├── integration/
│   │   ├── connectors/
│   │   ├── sdk/                    # SDK integration tests
│   │   ├── mcp/                    # MCP server integration tests
│   │   ├── ui/                     # Web UI integration tests
│   │   └── end_to_end/
│   └── fixtures/
│       ├── sample_bdl/             # XML format
│       ├── sample_bdl_json/        # JSON format (Section 18.5)
│       └── sample_data/
│
├── benchmarks/                     # Performance benchmarks (Section 15.3)
│   ├── conftest.py
│   └── test_transfer_throughput.py
│
├── packaging/                      # Installer build scripts
│   ├── windows/
│   │   ├── bani.iss                # Inno Setup script
│   │   └── build.ps1
│   ├── macos/
│   │   ├── bani.pkgproj
│   │   └── build.sh
│   ├── linux/
│   │   ├── deb/
│   │   ├── rpm/
│   │   ├── appimage/
│   │   └── build.sh
│   └── embedded_python/
│       └── fetch_python.py         # Downloads & bundles standalone Python
│
├── examples/
│   └── bdl/                        # BDL example library (Section 18.4)
│       ├── mysql-to-postgresql.bdl
│       ├── mssql-to-postgresql.bdl
│       ├── oracle-to-postgresql.bdl
│       ├── sqlite-to-mysql.bdl
│       ├── mysql-to-mysql-upgrade.bdl
│       ├── incremental-sync.bdl
│       ├── filtered-migration.bdl
│       ├── multi-schema.bdl
│       ├── custom-hooks.bdl
│       ├── scheduled-nightly.bdl
│       └── README.md
│
├── Dockerfile
├── docker-compose.yml              # Dev environment with all 5 DB engines
│
├── docs/                           # Technical docs source — docs.bani.dev via Read the Docs (Section 21.4)
│   ├── index.md                    # Docs landing page
│   ├── getting-started.md
│   ├── guides/
│   │   ├── bdl-reference.md
│   │   ├── bdl-xsd.md
│   │   ├── bdl-json-schema.md
│   │   ├── cli-reference.md
│   │   ├── python-sdk.md
│   │   ├── mcp-server.md
│   │   ├── docker.md
│   │   ├── incremental-sync.md
│   │   ├── error-handling.md
│   │   ├── type-mappings.md
│   │   └── configuration.md
│   ├── connectors/
│   │   ├── postgresql.md
│   │   ├── mysql.md
│   │   ├── mssql.md
│   │   ├── oracle.md
│   │   └── sqlite.md
│   ├── developer/
│   │   ├── architecture.md
│   │   ├── building-a-connector.md
│   │   └── contributing.md
│   ├── decisions.md                # Architecture Decision Log (Section 2.6)
│   └── api/                        # Auto-generated from docstrings (mkdocstrings)
│
├── site/                           # Marketing site source — bani.dev (Section 21.1)
│   ├── astro.config.mjs            # Astro configuration
│   ├── package.json
│   ├── tailwind.config.mjs
│   ├── src/
│   │   ├── pages/                  # Landing, /features, /compare, /cloud, /community, /about, /donate
│   │   ├── layouts/                # Base layouts (marketing, blog)
│   │   ├── components/             # Shared UI components (nav, footer, connector grid, etc.)
│   │   └── content/
│   │       └── blog/               # Blog posts in Markdown/MDX
│   ├── public/                     # Static assets (images, favicons, og-images)
│   └── README.md                   # Site development instructions
│
├── mkdocs.yml                      # MkDocs config for docs.bani.dev (Read the Docs)
├── .readthedocs.yaml               # Read the Docs build configuration
│
└── .github/
    └── workflows/
        ├── ci.yml                  # Lint, type-check, test on every PR
        ├── release.yml             # Build installers + Docker images on tag push
        ├── docker.yml              # Build and push Docker image variants to Docker Hub
        ├── connector-test.yml      # Matrix test all connectors against real DBs
        └── site.yml                # Build + deploy marketing site to GitHub Pages
```

---

## 8. Clean Code Principles

All contributors must follow Robert C. Martin's Clean Code principles. The codebase enforces these through tooling and review standards.

### 8.1 Naming

- **Classes**: nouns that describe what the object *is* (`SchemaReader`, `TypeMapper`, `DependencyResolver`).
- **Functions / methods**: verbs that describe what the function *does* (`read_table`, `create_indexes`, `resolve_dependencies`).
- **Variables**: intention-revealing names. Never `x`, `tmp`, `data` except in the narrowest local scope (e.g., a list comprehension counter).
- **No abbreviations** unless universally understood (`fk` for foreign key is acceptable; `tbl` for table is not).

### 8.2 Functions

- **Prefer small functions**: aim for ~20 lines or fewer. Functions exceeding 40 lines or cyclomatic complexity > 10 (as enforced by `radon`) should be refactored. Use judgement — a 30-line function that reads clearly is better than five 6-line functions that obscure the flow.
- **Single responsibility**: one function does one thing. If the name has "and" in it, consider splitting.
- **Few arguments**: prefer 0–2 arguments. If 3+, consider grouping into a dataclass / named tuple.
- **No side effects**: if a function is called `validate_schema`, it must not also modify the schema.
- **Command-query separation**: functions should preferably either *do* something (command) or *return* something (query), not both. Exceptions are acceptable when combining them significantly simplifies the API (document why).

### 8.3 Error Handling

- **Use exceptions, not return codes.** Define a domain exception hierarchy rooted at `BaniError`.
- **Don't catch generic `Exception`** unless re-raising with context.
- **Fail fast**: validate inputs at the boundary; don't let invalid state propagate deep.
- **Custom exceptions carry context**: `TypeMappingError(source_type="GEOGRAPHY", target_dialect="postgresql")`, `TypeMappingError(source_type="BLOB", target_dialect="oracle")`.

### 8.4 Testing

- **Test pyramid**: many unit tests (fast, isolated) → fewer integration tests (real DB containers) → a handful of end-to-end smoke tests.
- **Arrange-Act-Assert** pattern in every test.
- **Prefer one logical assertion per test**: multiple `assert` calls that verify a single concept are fine. If a test needs many unrelated assertions, consider splitting it — but use judgement over dogma.
- **Test names describe the scenario**: `test_type_mapper_converts_mssql_money_to_pg_numeric`, `test_type_mapper_converts_oracle_number_to_sqlite_real`.

### 8.5 Enforced Tooling

| Tool | Purpose | Config location |
|---|---|---|
| `ruff` | Linting + formatting (replaces flake8, isort, black) | `pyproject.toml` |
| `mypy` (strict mode) | Static type checking | `pyproject.toml` |
| `pytest` | Test runner | `pyproject.toml` |
| `pytest-cov` | Coverage (target: ≥ 90% on domain + BDL) | CI |
| `pre-commit` | Runs ruff + mypy before every commit | `.pre-commit-config.yaml` |
| `radon` | Cyclomatic complexity (max CC = 10 per function) | CI |
| `bandit` | Security linting | CI |

---

## 9. Cross-Platform Packaging

### 9.1 Embedded Python Runtime

Bani installers must **not** rely on a Python installation existing on the target machine. Each platform installer bundles a standalone Python runtime.

**Strategy**: Use [python-build-standalone](https://github.com/indygreg/python-build-standalone) — prebuilt, self-contained Python distributions for every major OS and architecture.

The build pipeline:

1. `fetch_python.py` downloads the correct standalone Python for the target `(os, arch)`.
2. Bani's dependencies are `pip install`-ed into that standalone environment.
3. The standalone Python + site-packages + Bani source are bundled into the platform-specific installer.

### 9.2 Platform Installers

| Platform | Format | Tool | Notes |
|---|---|---|---|
| Windows | `.exe` installer | Inno Setup | Adds `bani` to PATH, creates Start Menu shortcut |
| macOS | `.pkg` installer | `pkgbuild` + `productbuild` | Signed + notarised for Gatekeeper |
| Debian/Ubuntu | `.deb` | `dpkg-deb` | Installs to `/opt/bani`, symlinks `/usr/local/bin/bani` |
| RHEL/Fedora | `.rpm` | `rpmbuild` | Same layout as `.deb` |
| Universal Linux | AppImage | `appimagetool` | Single-file, no-install-needed |

### 9.3 DB Drivers Ship with Bani

All Python database drivers for the built-in connectors (MySQL, PostgreSQL, MSSQL, Oracle, SQLite) are pre-installed inside the bundled environment — including multiple pinned driver versions per connector to cover the most popular database server versions (see Section 6.3.1). Users never need to `pip install` anything. Community connectors distributed as separate packages will include their own driver dependencies.

---

## 10. CLI Interface

Bani's primary interface is a rich CLI built with **Typer** and **Rich**.

### 10.1 Command Reference

```
# ─── Global flags (available on ALL commands) ───────────────────
      --output FORMAT               human | json (default: human)
      --quiet                       Suppress all output except errors
      --log-level LEVEL             DEBUG | INFO | WARNING | ERROR

# ─── Migration ──────────────────────────────────────────────────
bani run <project.bdl>              Run a migration defined in a BDL file
      --dry-run                     Validate and plan but don't execute
      --tables TABLE [TABLE...]     Override: only migrate these tables
      --parallel N                  Override worker count
      --batch-size N                Override batch size
      --resume                      Resume a previously failed migration

bani validate <project.bdl>         Validate BDL against the XSD schema

bani preview <project.bdl>          Sample N rows per table, display in terminal
      --sample-size N               Rows to sample (default: 10)

# ─── Schema ─────────────────────────────────────────────────────
bani schema inspect                  Introspect a database and display its schema
      --connector NAME              e.g., "postgresql", "oracle", "sqlite"
      --host HOST --port PORT ...

bani schema diff                     Compare source and target schemas (future)

# ─── Project ────────────────────────────────────────────────────
bani init                            Interactive wizard to create a new BDL file
      --source CONNECTOR
      --target CONNECTOR

# ─── Connectors ─────────────────────────────────────────────────
bani connectors list                 Show all discovered connectors
bani connectors info NAME            Show connector details, supported types, and driver versions

# ─── Scheduling ─────────────────────────────────────────────────
bani schedule <project.bdl>          Register a migration with the OS scheduler
      --cron "EXPR"
      --timezone TZ

# ─── AI Agent Integration ───────────────────────────────────────
bani mcp serve                       Start Bani as an MCP server (stdio default)
      --transport TYPE              stdio | sse (default: stdio)
      --port PORT                   Port for SSE transport

# ─── Web UI ─────────────────────────────────────────────────────
bani ui                              Start the Web UI server (default: http://127.0.0.1:8910)
      --host HOST                   Bind to specific host (default: 127.0.0.1)
      --port PORT                   Bind to specific port (default: 8910)

# ─── Info ───────────────────────────────────────────────────────
bani version                         Show Bani version and connector versions
```

### 10.2 Output Modes

- **Interactive terminal** (`--output human`, default): Rich tables, progress bars, coloured output.
- **JSON mode** (`--output json`): machine-readable structured JSON. Every implemented v1.0 command supports this mode with a documented output schema (see Section 18.2 for schema examples). Streaming commands (`run`) emit JSON-lines (one JSON object per event). Non-streaming commands (`validate`, `schema inspect`, `connectors list`) emit a single JSON object. This mode is the **primary interface for AI agents**.
- **Quiet mode** (`--quiet`): suppress all output except errors (still respects `--output json` for error formatting).

---

## 11. Domain Models

### 11.1 Schema Model

```python
from dataclasses import dataclass, field
from enum import Enum, auto

class ConstraintType(Enum):
    PRIMARY_KEY = auto()
    UNIQUE = auto()
    CHECK = auto()
    FOREIGN_KEY = auto()

@dataclass(frozen=True)
class ColumnDefinition:
    name: str
    data_type: str               # Raw source type string, e.g. "VARCHAR(255)"
    nullable: bool = True
    default_value: str | None = None
    is_auto_increment: bool = False
    ordinal_position: int = 0

@dataclass(frozen=True)
class IndexDefinition:
    name: str
    columns: tuple[str, ...]
    is_unique: bool = False
    is_clustered: bool = False
    filter_expression: str | None = None

@dataclass(frozen=True)
class ForeignKeyDefinition:
    name: str
    source_table: str
    source_columns: tuple[str, ...]
    referenced_table: str
    referenced_columns: tuple[str, ...]
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"

@dataclass(frozen=True)
class TableDefinition:
    schema_name: str
    table_name: str
    columns: tuple[ColumnDefinition, ...]
    primary_key: tuple[str, ...] = ()
    indexes: tuple[IndexDefinition, ...] = ()
    foreign_keys: tuple[ForeignKeyDefinition, ...] = ()
    check_constraints: tuple[str, ...] = ()
    row_count_estimate: int | None = None  # populated by schema introspection if the DB supports it

@dataclass(frozen=True)
class DatabaseSchema:
    tables: tuple[TableDefinition, ...]
    source_dialect: str          # e.g. "mssql", "postgresql", "oracle", "sqlite"
```

### 11.2 Immutability

All domain models are **frozen dataclasses** or **NamedTuples**. This:

- Prevents accidental mutation during a pipeline.
- Makes them safe to share across threads.
- Aligns with Clean Code's principle that data structures should be transparent and inert.

---

## 12. Error Handling and Resumability

### 12.1 Exception Hierarchy

```
BaniError
├── ConfigurationError
│   ├── BDLValidationError
│   ├── ConnectionConfigError
│   └── TypeMappingError
├── ConnectionError
│   ├── SourceConnectionError
│   └── TargetConnectionError
├── SchemaError
│   ├── IntrospectionError
│   ├── SchemaTranslationError
│   └── DependencyResolutionError
├── DataTransferError
│   ├── ReadError
│   ├── WriteError
│   ├── BatchError (carries batch_number, first_row_offset)
│   └── TransformError
├── HookExecutionError
└── SchedulerError
```

### 12.2 Resumability Protocol

1. Before starting, the orchestrator writes a **checkpoint file** (JSON) that records: project hash, table list, and per-table status (`pending | in_progress | completed | failed`).
2. After each batch commit, the checkpoint updates with the last successfully committed row offset.
3. On `bani run --resume`, the orchestrator reads the checkpoint file and skips tables/batches already completed.
4. Failed rows are written to a **quarantine table** (`_bani_quarantine`) in the target database with the original row data and the error message.

---

## 13. Incremental Sync

### 13.1 Strategies

| Strategy | How it works | Requirement |
|---|---|---|
| `timestamp` | Reads rows where `tracking_column > last_sync_timestamp` | Table must have a reliable last-modified timestamp column |
| `rowversion` | Uses MSSQL `rowversion` / PostgreSQL `xmin` / Oracle `ORA_ROWSCN` to detect changes | Database-specific; connector provides the implementation. Not applicable to SQLite. |
| `checksum` | Computes a hash of each row in source and target, syncs differences | Slow but universal; no schema requirements |

### 13.2 State Storage

Sync state (last timestamp, last rowversion, table checksums) is stored in a `_bani_sync_state` table in the target database, keyed by `(project_name, table_name)`.

---

## 14. Security Model

This section is the single authoritative reference for all security-related behaviour in Bani. Every other section that mentions credentials, encryption, or access control defers to the rules here.

### 14.1 Credential Handling

1. **Environment variable interpolation** — BDL connection strings reference credentials via `${env:VAR_NAME}` syntax. The BDL parser resolves these at runtime from the process environment. BDL files **must never contain plaintext credentials** — the BDL XSD validator emits a warning if a `password` attribute contains a literal value rather than a `${env:...}` reference.
2. **No credential logging** — the logging subsystem (Section 16) redacts any value that was resolved from a `${env:...}` reference. Connection strings in log output replace credentials with `***REDACTED***`.
3. **No credential persistence** — checkpoint files, quarantine tables, progress logs, and JSON-lines event streams never include credential values.
4. **CLI credential input** — when using `bani schema inspect` or `bani run` without a BDL file, the CLI accepts credentials via `--password-env VAR_NAME` (reads from environment) or `--password-stdin` (reads from stdin). There is no `--password` flag that takes a plaintext argument.
5. **MCP server credentials** — the MCP server (Section 18.6) receives credential *values* exclusively through environment variables set in the MCP client configuration. MCP tool arguments must never include credential *values* (passwords, tokens, secrets); however, they may include environment variable *names* via the `credential_env_vars` parameter (e.g., `{username_env: "MYSQL_USER", password_env: "MYSQL_PASS"}`), which tell the server which env vars to read at runtime. The server rejects any tool call that passes a `password` or `credentials` field containing an actual secret.
6. **SDK credentials** — the Python SDK (Section 18.3) `ProjectBuilder.source()` / `.target()` accept credential arguments as keyword parameters (e.g., `username="reader", password_env="MYSQL_PASS"`). `password_env` names an environment variable resolved at runtime; there is no `password=` keyword that accepts a plaintext secret. All credential values are held in memory only and never serialised to disk.
7. **Docker credentials** — Docker containers receive credentials via `-e` environment variables or Docker secrets mounted at `/run/secrets/`. The Docker entrypoint script validates that required credential env vars are set before starting Bani.

**Credential handling summary by surface area:**

| Surface | How credentials are supplied | Plaintext in arguments? |
|---|---|---|
| BDL | `${env:VAR_NAME}` references in XML/JSON | Never — XSD warns on literals |
| CLI | `--password-env VAR_NAME` / `--password-stdin` | Never — no `--password` flag |
| Python SDK | `password_env="VAR_NAME"` keyword on `.source()` / `.target()` | Never — no `password=` keyword; values held in memory only |
| MCP server | `credential_env_vars: {password_env: "VAR_NAME"}` | Never — server rejects `password` / `credentials` fields |
| Docker | `-e` env vars or `/run/secrets/` mounts | Never — entrypoint validates env vars are set |

### 14.2 Transport Encryption

1. **TLS by default** — all connectors default to `encrypt="true"`. Unencrypted connections require an explicit `encrypt="false"` in BDL or CLI flags, and Bani logs a warning when encryption is disabled.
2. **Certificate verification** — connectors verify server TLS certificates by default. Self-signed certificates can be accepted via `trustServerCertificate="true"` in `<connectorConfig>`, but this also logs a warning.

### 14.3 Input Validation

1. **SQL injection hardening** — BDL `<filter>` expressions are parameterised via the connector's native parameter binding. Raw SQL strings from BDL are never interpolated into queries. If a connector does not support parameterised filters, the filter is validated against a safe SQL subset (column references, comparison operators, literals, AND/OR/NOT) before execution.
2. **BDL validation** — all BDL documents are validated against the XSD/JSON Schema before execution. Malformed or schema-invalid documents are rejected with a clear error message.
3. **Path traversal prevention** — BDL `<hook>` commands that reference file paths are resolved relative to the project directory. Absolute paths and `..` traversals are rejected.

### 14.4 Least Privilege

1. **Source credentials** — documentation and examples recommend read-only database users for source connections.
2. **Target credentials** — documentation recommends schema-scoped credentials (CREATE, INSERT, ALTER on the target schema only, not server-wide superuser).
3. **Docker** — the Bani Docker image runs as a non-root user (`bani`, UID 1000).

### 14.5 Future (Out of Scope for v1.0)

These are enterprise-tier features (see Section 21.1):

1. **Secrets manager integration** — support `${vault:path/to/secret}` for HashiCorp Vault, `${aws-sm:secret-name}` for AWS Secrets Manager, and similar pluggable resolvers.
2. **Audit logging** — structured audit trail of who ran what migration, when, with what parameters (credentials excluded).
3. **RBAC** — role-based access control for multi-user deployments.

---

## 15. Testing Strategy

### 15.1 Unit Tests

- Domain logic: type mapping, dependency resolution, delta detection, BDL parsing.
- Mocked connectors: verify orchestrator behaviour without real databases.
- Target: ≥ 95% line coverage on `domain/` and `bdl/`.

### 15.2 Integration Tests

- Use **testcontainers-python** to spin up MySQL, PostgreSQL, MSSQL, and Oracle in Docker. SQLite tests use temporary file-based databases (no container needed).
- Each connector has a test suite that: creates a source schema, populates it with fixture data, runs a migration, and asserts the target schema and data match.
- Matrix: test every `(source, target)` pair in the initial five connectors (25 combinations).
- **Driver version matrix tests** — each connector's test suite runs against every bundled driver version to verify compatibility with the corresponding database server versions (see Section 6.3.1).

### 15.3 CI Test Tiers

The full 25-pair × multi-version matrix is too expensive for every PR. Split into three tiers:

| Tier | Trigger | Scope | Max duration |
|---|---|---|---|
| **PR (smoke)** | Every push / PR | Unit tests + PostgreSQL ↔ PostgreSQL + PostgreSQL ↔ MySQL (default driver only) | ≤ 10 min |
| **Nightly** | Scheduled (daily) | Full 25-pair matrix with default driver versions + BDL conformance + SDK + MCP tests | ≤ 45 min |
| **Release** | Release candidate tag | Full 25-pair matrix × all bundled driver versions + performance benchmarks (Section 4.4) | ≤ 90 min |

The PR tier is a **hard gate** — merges are blocked if it fails. The nightly tier posts results to a dashboard or Slack channel; failures create issues but don't block development. The release tier is a **hard gate** for tagging a release.

### 15.4 Performance Benchmarks

- A separate `benchmarks/` directory with `pytest-benchmark` tests.
- Reference schemas and measurement rules are defined in Section 4.4 (Benchmark Contract).
- CI runs benchmarks at the release tier and publishes results to track regressions.

### 15.5 BDL Conformance Tests

- A suite of valid and invalid BDL files in **both XML and JSON formats**.
- Valid files must parse without error and produce expected `ProjectModel` values.
- Invalid files must raise `BDLValidationError` with a descriptive message.
- Verify that XML and JSON representations of the same project produce identical `ProjectModel` objects.

### 15.6 SDK Tests

- Every SDK method (`Bani.load()`, `ProjectBuilder`, `SchemaInspector`) has unit tests with mocked connectors.
- Integration tests verify the SDK produces the same results as the equivalent CLI commands.
- The `on_progress` callback receives all expected events in the correct order.

### 15.7 MCP Server Tests

- Each MCP tool has a unit test verifying it maps correctly to the underlying SDK method.
- Integration tests spin up the MCP server and invoke tools via the MCP client protocol, verifying end-to-end correctness.
- Credential handling: verify that the MCP server rejects plain-text credentials and requires environment variable references.

---

## 16. Logging and Observability

### 16.1 Structured Logging

All log entries are JSON-lines with a consistent schema:

```json
{
  "timestamp": "2026-03-19T02:15:03.412Z",
  "level": "INFO",
  "component": "orchestrator",
  "event": "table_transfer_complete",
  "table": "orders",
  "rows_transferred": 1450000,
  "duration_seconds": 12.7,
  "throughput_rows_per_sec": 114173
}
```

### 16.2 Event Stream

A parallel event stream (via a simple callback / observer pattern) emits typed events that any presentation layer can subscribe to. This is the **single source of truth** for progress reporting — the CLI's Rich output, the `--output json` JSON-lines stream, the SDK's `on_progress` callback (Section 18.3), and the MCP server's notifications (Section 18.6) all consume these same events:

```python
@dataclass
class TableTransferProgress:
    table: str
    rows_transferred: int
    total_rows: int
    bytes_transferred: int
    elapsed_seconds: float

@dataclass
class MigrationComplete:
    tables_succeeded: int
    tables_failed: int
    total_rows: int
    total_duration_seconds: float
```

---

## 17. Configuration Hierarchy

Bani resolves configuration in this precedence order (highest wins):

1. **SDK programmatic overrides** (e.g., `ProjectBuilder().batch_size(200_000)`) — highest, for in-process use
2. **CLI flags** (`--batch-size 50000`)
3. **Environment variables** (`BANI_BATCH_SIZE=50000`)
4. **BDL file** (`<batchSize>100000</batchSize>`)
5. **User config file** (`~/.config/bani/config.toml`)
6. **Built-in defaults**

When Bani is invoked via the MCP server (Section 18.6), MCP tool parameters map to CLI flags (level 2) in the hierarchy.

---

## 18. AI Agent Integration

Bani is designed from the ground up to be consumed by AI agents — not just human operators. BDL already provides the declarative interface agents need to define migrations, but additional affordances are required to make Bani truly **agent-native**: structured machine-readable I/O, a programmatic Python SDK, an MCP (Model Context Protocol) server, and a library of example BDL files that AI models can learn from.

### 18.1 Agentic Use Cases (v1.0)

The following workflows must be achievable by an AI agent using only BDL files and the CLI with `--output json`:

1. **Natural-language-to-migration** — an agent receives a user request ("migrate my legacy MySQL 5.5 ERP to PostgreSQL 16"), generates a BDL file, validates it, previews the data, and executes the migration — all without human intervention beyond the initial instruction.
2. **Schema discovery and analysis** — an agent connects to an unknown database via `bani schema inspect --output json`, receives the full schema as structured JSON, and reasons about table relationships, data types, and migration complexity before producing a plan.
3. **Iterative migration refinement** — an agent runs `bani preview`, examines sample data in JSON, detects issues (truncation, encoding mismatches, null violations), adjusts type mappings in the BDL file, and re-previews until the output is correct.
4. **Error recovery** — when a migration fails, the agent parses the structured error output and checkpoint file, identifies the failing table/batch, adjusts the BDL (e.g., adds a quarantine rule or fixes a type mapping), and resumes with `bani run --resume`.
5. **Scheduled monitoring** — an agent sets up a recurring migration via `bani schedule`, monitors the JSON-lines event stream for anomalies (throughput drops, error spikes), and triggers corrective actions or alerts.
6. **Multi-database orchestration** — an agent manages a fleet of migrations (e.g., consolidating five regional databases into one central warehouse) by generating and executing multiple BDL files, tracking progress across all of them.

### 18.2 Structured Machine-Readable Output (v1.0)

Every CLI command must support `--output json` mode. In this mode, all output is valid JSON (or JSON-lines for streaming commands), with documented schemas that agents can parse deterministically.

**The JSON examples below are normative** — the actual CLI output must match these field names, types, and nesting exactly. If you add fields, they must be additive (existing fields cannot be renamed or removed). Ship a JSON Schema file for each output type in `src/bani/schemas/` so consumers can validate output programmatically.

**Validation output:**

```json
{
  "command": "validate",
  "status": "error",
  "errors": [
    {
      "line": 42,
      "column": 15,
      "severity": "error",
      "code": "BDL-001",
      "message": "Unknown connector 'mysq'. Did you mean 'mysql'?",
      "element": "<source connector=\"mysq\">"
    }
  ],
  "warnings": [],
  "schema_version": "1.0"
}
```

**Schema inspect output:**

```json
{
  "command": "schema_inspect",
  "connector": "mysql",
  "server_version": "5.5.62",
  "driver_version": "8.0.x",
  "tables": [
    {
      "schema": "erp",
      "name": "customers",
      "row_count_estimate": 1450000,
      "columns": [
        {"name": "id", "type": "INT", "nullable": false, "auto_increment": true},
        {"name": "name", "type": "VARCHAR(255)", "nullable": false}
      ],
      "primary_key": ["id"],
      "indexes": [...],
      "foreign_keys": [...]
    }
  ]
}
```

**Migration progress (JSON-lines stream):**

```json
{"event": "migration_started", "timestamp": "...", "tables": 12, "estimated_rows": 45000000}
{"event": "table_started", "timestamp": "...", "table": "orders", "estimated_rows": 10000000}
{"event": "batch_complete", "timestamp": "...", "table": "orders", "batch": 5, "rows": 500000, "total_rows": 2500000, "throughput_rps": 385000}
{"event": "table_complete", "timestamp": "...", "table": "orders", "rows": 10000000, "duration_sec": 28.3}
{"event": "migration_complete", "timestamp": "...", "tables_succeeded": 12, "tables_failed": 0, "total_rows": 45000000, "duration_sec": 142.7}
```

**Error output:**

```json
{
  "command": "run",
  "status": "failed",
  "error": {
    "type": "DataTransferError.BatchError",
    "table": "orders",
    "batch_number": 47,
    "first_row_offset": 4700000,
    "message": "Target column 'amount' rejects NULL but source row 4700023 has NULL",
    "suggestion": "Add a NULL coercion rule or set onError to 'log-and-continue'"
  },
  "checkpoint_file": "/path/to/checkpoint.json",
  "resumable": true
}
```

**Connectors list output:**

```json
{
  "command": "connectors_list",
  "connectors": [
    {
      "name": "postgresql",
      "version": "1.0.0",
      "type": "source+sink",
      "default_driver_version": "3.2.x",
      "bundled_driver_versions": ["3.2.x", "3.1.x"],
      "supported_db_versions": ["PostgreSQL 12+"]
    },
    {
      "name": "mysql",
      "version": "1.0.0",
      "type": "source+sink",
      "default_driver_version": "8.4.x",
      "bundled_driver_versions": ["8.4.x", "8.0.x"],
      "supported_db_versions": ["MySQL 5.5+", "MySQL 8.0+"]
    }
  ]
}
```

### 18.3 Python SDK — `bani-core` (v1.0)

The Application Service Layer (Section 3) is already structured as a clean Python API. Expose it as a first-class public SDK so that agents (and other Python programs) can drive Bani in-process without shelling out to the CLI.

```python
from bani import Bani

# Load a BDL file and execute
project = Bani.load("migration.bdl")
project.validate()        # raises BDLValidationError with structured details
preview = project.preview(sample_size=10)  # returns dict[table_name, list[dict]]
result = project.run(on_progress=my_callback)  # returns MigrationResult

# Or build programmatically
from bani.sdk import ProjectBuilder

project = (
    ProjectBuilder()
    .source("mysql", host="legacy-db", port=3306, database="erp",
            username="reader", password_env="MYSQL_PASS")
    .target("postgresql", host="new-db", port=5432, database="analytics",
            username="writer", password_env="PG_PASS")
    .include_tables(["customers", "orders", "products"])
    .type_mapping("MONEY", "NUMERIC(19,4)")
    .batch_size(200_000)
    .parallel_workers(8)
    .build()
)
result = project.run()

# Schema introspection
from bani.sdk import SchemaInspector

inspector = SchemaInspector("oracle", host="db.internal", port=1521,
                             service_name="PROD")
schema = inspector.introspect()  # returns DatabaseSchema dataclass
for table in schema.tables:
    print(f"{table.table_name}: {len(table.columns)} columns, ~{table.row_count_estimate} rows")
```

**SDK design rules:**

- The SDK is the **same code** the CLI calls — no separate implementation.
- All SDK methods return typed dataclasses, not raw dicts or strings.
- All SDK methods accept an optional `on_progress` callback for real-time event streaming.
- Errors are raised as domain exceptions (Section 12.1), not wrapped in generic errors.
- The SDK is stateless between calls — no hidden singletons or module-level state.

### 18.4 BDL Example Library (v1.0)

Ship a `examples/bdl/` directory containing annotated BDL files covering common migration scenarios. These serve as few-shot examples for AI models generating BDL:

```
examples/bdl/
├── mysql-to-postgresql.bdl          # Basic full migration
├── mssql-to-postgresql.bdl          # With type mapping overrides
├── oracle-to-postgresql.bdl         # Oracle-specific features (sequences, synonyms)
├── sqlite-to-mysql.bdl              # Embedded-to-server migration
├── mysql-to-mysql-upgrade.bdl       # Same-engine version upgrade (5.5 → 8.4)
├── incremental-sync.bdl             # Timestamp-based delta sync
├── filtered-migration.bdl           # Per-table WHERE filters
├── multi-schema.bdl                 # Migrating multiple schemas
├── custom-hooks.bdl                 # Pre/post SQL and shell hooks
├── scheduled-nightly.bdl            # Cron-scheduled recurring migration
└── README.md                        # Index with one-line description per file
```

Each example file includes XML comments explaining every element — these comments are the "documentation" an AI model reads to understand BDL.

### 18.5 BDL JSON Schema (v1.0)

In addition to the XSD (Section 5.4), ship a **JSON Schema** equivalent (`bdl-1.0.schema.json`). Most AI models are better at generating and validating JSON than XML. The JSON Schema allows agents to:

1. Validate a BDL structure in JSON form before converting to XML.
2. Use JSON Schema-aware generation (many LLMs support structured output constrained by JSON Schema).
3. Programmatically explore what fields are available without parsing XSD.

Bani's BDL parser accepts both XML and a JSON representation of the same structure. The canonical format remains XML, but JSON is a supported alternative input:

```bash
bani run migration.bdl       # XML input
bani run migration.bdl.json  # JSON input (same schema, different syntax)
```

### 18.6 MCP Server (v1.0 — foundational)

Bani ships a built-in **Model Context Protocol (MCP)** server that exposes its capabilities as tools that any MCP-compatible AI agent can discover and invoke. MCP is becoming the standard protocol for AI agents to interact with external tools.

**MCP tools exposed by Bani:**

| Tool | Description | Parameters |
|---|---|---|
| `bani_schema_inspect` | Introspect a database and return its schema | connector, host, port, database, credential_env_vars (`{username_env, password_env}`) |
| `bani_validate_bdl` | Validate a BDL document (XML or JSON) | bdl_content (string) |
| `bani_preview` | Preview sample rows from a migration | bdl_content, sample_size |
| `bani_run` | Execute a migration | bdl_content, dry_run, resume |
| `bani_status` | Check the status of a running migration | checkpoint_id |
| `bani_connectors_list` | List all available connectors | (none) |
| `bani_connector_info` | Get details about a specific connector | connector_name |
| `bani_generate_bdl` | Generate a BDL template from source/target | source_connector, target_connector, tables (optional) |

**Credential handling in MCP tools:** Tools that need database access (e.g., `bani_schema_inspect`) accept a `credential_env_vars` parameter — a dict of `{username_env: "ENV_VAR_NAME", password_env: "ENV_VAR_NAME"}` — telling Bani which environment variables to read. The server resolves the actual credential values from its process environment at runtime. This means the AI agent never sees or transmits plaintext credentials; it only references env var names that were pre-configured in the MCP client setup (see configuration example above). If a tool call includes a `password` or `credentials` field directly, the server rejects it with a `SecurityError`.

**MCP server startup:**

```bash
bani mcp serve                       # Start MCP server on stdio (default)
bani mcp serve --transport sse       # Start MCP server with SSE transport
bani mcp serve --port 8765           # Start MCP server on a specific port
```

**MCP server configuration for AI clients (e.g., Claude Desktop):**

```json
{
  "mcpServers": {
    "bani": {
      "command": "bani",
      "args": ["mcp", "serve"],
      "env": {
        "BANI_CONFIG": "~/.config/bani/config.toml"
      }
    }
  }
}
```

**Implementation:**

- Use the `mcp` Python package (the official MCP SDK) to implement the server.
- Each MCP tool maps directly to an SDK method (Section 18.3) — the MCP layer is a thin adapter.
- The server streams progress events as MCP notifications during long-running operations.
- Credential security is enforced as described in the credential handling note above and in Section 14.1.

---

## 19. Docker Support

Bani ships an official Docker image for containerised operation. This is particularly valuable for AI agents (which can spin up containers programmatically), CI/CD pipelines, and environments where installing software is undesirable.

### 19.1 Docker Image

**Base image:** `python:3.12-slim` (Debian-based, small footprint, includes all needed system libraries).

**Reference Dockerfile:**

```dockerfile
FROM python:3.12-slim AS base

# Install system dependencies for DB drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    unixodbc-dev \
    freetds-dev \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

# Install Bani and all built-in connectors
COPY dist/bani-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/bani-*.whl && rm /tmp/bani-*.whl

# Non-root user for security
RUN useradd --create-home bani
USER bani
WORKDIR /home/bani

# Default entrypoint
ENTRYPOINT ["bani"]
CMD ["--help"]
```

### 19.2 Docker Usage

**Run a migration from a BDL file:**

```bash
# Mount the BDL file and run
docker run --rm \
  -v ./my-project.bdl:/home/bani/project.bdl:ro \
  -e MSSQL_USER=sa -e MSSQL_PASS=secret \
  -e PG_USER=writer -e PG_PASS=secret \
  --network=host \
  bani/bani:latest run project.bdl
```

**Inspect a remote database schema:**

```bash
docker run --rm --network=host \
  bani/bani:latest schema inspect \
  --connector postgresql --host db.internal --port 5432 \
  --database analytics --output json
```

**Run as an MCP server (for AI agents):**

```bash
docker run --rm -i \
  -e MYSQL_PASS=secret -e PG_PASS=secret \
  bani/bani:latest mcp serve
```

### 19.3 Docker Compose for Development and Testing

Ship a `docker-compose.yml` that spins up Bani alongside all five database engines for local development and integration testing:

```yaml
version: "3.9"

services:
  bani:
    build: .
    depends_on: [mysql, postgres, mssql, oracle]
    environment:
      MYSQL_PASS: testpass
      PG_PASS: testpass
      MSSQL_PASS: "TestPass123!"
      ORACLE_PASS: testpass
    volumes:
      - ./examples/bdl:/home/bani/bdl:ro
    networks: [bani-net]

  mysql:
    image: mysql:8.4
    environment:
      MYSQL_ROOT_PASSWORD: testpass
    ports: ["3306:3306"]
    networks: [bani-net]

  mysql55:
    image: mysql:5.5
    environment:
      MYSQL_ROOT_PASSWORD: testpass
    ports: ["3307:3306"]
    networks: [bani-net]

  postgres:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: testpass
    ports: ["5432:5432"]
    networks: [bani-net]

  mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "Y"
      SA_PASSWORD: "TestPass123!"
    ports: ["1433:1433"]
    networks: [bani-net]

  oracle:
    image: gvenzl/oracle-free:23-slim
    environment:
      ORACLE_PASSWORD: testpass
    ports: ["1521:1521"]
    networks: [bani-net]

networks:
  bani-net:
```

Note: SQLite requires no container — it operates on local files mounted into the Bani container.

### 19.4 Docker Image Variants

| Tag | Contents | Use case |
|---|---|---|
| `bani/bani:latest` | All five connectors + all driver versions | General use |
| `bani/bani:slim` | Core only, no connectors pre-installed | Minimal base for custom connector sets |
| `bani/bani:mcp` | All connectors + MCP server as default entrypoint | AI agent integration |

### 19.5 CI/CD Integration

The Docker image enables zero-install migration in CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Migrate test database
  run: |
    docker run --rm --network=host \
      -v ./migrations/nightly.bdl:/home/bani/project.bdl:ro \
      -e MSSQL_USER=${{ secrets.MSSQL_USER }} \
      -e MSSQL_PASS=${{ secrets.MSSQL_PASS }} \
      -e PG_USER=${{ secrets.PG_USER }} \
      -e PG_PASS=${{ secrets.PG_PASS }} \
      bani/bani:latest run project.bdl --output json
```

---

## 20. Web UI

Bani ships with a browser-based management interface that lets users create, edit, run, and monitor migration projects without touching BDL files or the CLI. The UI is a single-page application served by a lightweight embedded HTTP server bundled with the Bani installation.

### 20.1 Tech Stack

| Layer | Choice | Rationale |
|---|---|---|
| Frontend framework | React 18+ with TypeScript | Large ecosystem, strong typing, widely understood |
| Build tool | Vite | Fast HMR, optimised production builds |
| UI components | shadcn/ui + Tailwind CSS | Accessible, composable, no runtime dependency |
| State management | React Query (TanStack Query) + Zustand | Server-state caching + lightweight client state |
| Backend | FastAPI (embedded) | Already Python, async, auto-generates OpenAPI spec |
| API protocol | REST (JSON) | Simple, well-tooled; WebSocket for real-time progress |
| Packaging | Frontend assets built at release time and served by the FastAPI backend as static files | Single `bani ui` command starts the server — no separate Node process at runtime |

### 20.2 Architecture

The Web UI is a **presentation-layer adapter** — it sits alongside the CLI, MCP server, and Python SDK, and calls the same Application Service Layer. It adds no domain logic of its own.

```
Browser (React SPA)
     │
     ▼  REST / WebSocket
FastAPI server  (`bani ui` command)
     │
     ▼
Application Service Layer  (same as CLI / SDK / MCP)
```

The FastAPI server exposes:

1. **REST endpoints** — CRUD for migration projects (stored as BDL files on disk), trigger migrations, inspect schemas, list connectors, manage schedules.
2. **WebSocket endpoint** — streams real-time `ProgressTracker` events (row counts, throughput, ETA, errors) to the browser during a migration run.
3. **Static file serving** — the built React app is served from a `ui/dist/` directory bundled with the Bani package.

### 20.3 Core Screens

| Screen | Purpose |
|---|---|
| **Dashboard** | List all saved migration projects with status (idle, running, completed, failed). Quick-launch buttons. |
| **Project Editor** | Visual form to create/edit a migration project: select source/target connectors, configure credentials (via env var names — the UI never handles raw secrets), pick tables, define type mapping overrides, set hooks, and configure schedule. Saves as BDL. |
| **Schema Browser** | Connect to a database and browse its schema (tables, columns, types, keys, indexes) in a tree view. Used during project setup and for ad-hoc inspection. |
| **Migration Monitor** | Real-time view of a running migration: per-table progress bars, throughput chart, error log, ETA. Powered by WebSocket events from `ProgressTracker`. |
| **Run History** | Log of past migration runs with status, duration, row counts, and error summaries. Links to checkpoint files for resumability. |
| **Connector Catalog** | List of installed connectors with version, supported database versions, and link to docs. |
| **Settings** | Global Bani configuration (log level, default batch size, scheduler settings). Maps to the configuration hierarchy (Section 17). |

### 20.4 Security Considerations

- The UI backend binds to `127.0.0.1` by default — not accessible from the network unless explicitly configured.
- Credentials are never entered directly in the UI. The Project Editor prompts users to supply **environment variable names** (consistent with Section 14.1). The UI displays which env vars are expected and whether they are currently set, but never reveals values.
- CORS is locked to the same origin by default.
- CSRF protection via SameSite cookies and a sync token.
- All API endpoints require a local auth token generated at server start and printed to the console. The browser stores it in a session cookie.

### 20.5 CLI Integration

```
bani ui                       # Start the Web UI server (default: http://127.0.0.1:8910)
bani ui --host 0.0.0.0        # Bind to all interfaces (for Docker / remote access)
bani ui --port 9000            # Custom port
```

The `bani ui` command is a thin wrapper that starts the FastAPI server with Uvicorn. When running inside Docker, the `bani/bani:latest` image already includes the built UI assets; use `bani ui --host 0.0.0.0` to expose it.

---

## 21. Website and Documentation

Bani's web presence is split into three distinct properties, each with a clear purpose and audience:

| Property | Purpose | Hosting | Audience |
|---|---|---|---|
| **bani.dev** (marketing site) | Market Bani — explain what it is, why it exists, showcase features, link to repo | GitHub Pages | Prospective users, evaluators |
| **docs.bani.dev** (technical docs) | All technical and developer documentation, guides, BDL specs, API reference | Read the Docs | Active users, connector developers, contributors |
| **Community hub** | Discussion, real-time chat, contribution workflows, connector catalog | GitHub Discussions + Discord/Slack | Users, contributors, community |

### 21.1 Marketing Site — bani.dev

The marketing site is a static site hosted on **GitHub Pages**, generated from the `site/` directory in the repository. Its sole purpose is to **market Bani** — communicate what the product is, who it's for, and why someone should use it over alternatives.

**Tech stack:**

| Concern | Choice | Rationale |
|---|---|---|
| Framework | **Astro** (or Hugo as fallback) | Static-first, fast builds, Markdown/MDX native, minimal JS. |
| Styling | **Tailwind CSS** | Utility-first, consistent with modern open-source project sites. |
| Hosting | **GitHub Pages** | Zero cost, automatic deploys via GitHub Actions, custom domain support. |
| CI/CD | GitHub Actions (`site.yml`) | Build on push to `main`, deploy to GitHub Pages. Preview deploys on PRs via artifact. |

The site source lives in `site/` at the repository root.

**Site structure:**

```
bani.dev/
├── /                              # Landing page (hero, value prop, feature grid)
├── /features/                     # Detailed feature pages (Arrow engine, BDL, connectors, AI agent, etc.)
├── /compare/                      # Comparison pages (Bani vs FullConvert, Bani vs pgloader, etc.)
├── /roadmap/                      # Public roadmap with open-source vs enterprise labelling
├── /connectors/                   # Visual connector catalog (built-in + community, future marketplace)
├── /blog/                         # Release announcements, migration stories, technical deep-dives
├── /cloud/                        # Bani Cloud waitlist / early-access signup page
├── /about/                        # About the developer / team, project origin story, contact
├── /community/                    # Links to GitHub Discussions, Discord/Slack, contributing guide
└── /donate/                       # Donation page (Stripe + PayPal)
```

### 21.2 Landing Page

The landing page must communicate Bani's value proposition within 5 seconds of loading. Required sections:

1. **Hero** — one-liner tagline + "Get Started" (links to docs.bani.dev/getting-started) and "View on GitHub" CTAs. Example tagline: *"The open-source database migration engine. Schema, data, and indexes — across any database, powered by Apache Arrow."*
2. **Feature grid** — 6–8 cards highlighting: Arrow-powered performance, BDL declarative migrations, 5 built-in connectors, AI-agent-native (MCP + SDK), cross-platform, Docker support, incremental sync, open-source.
3. **Quick demo** — an animated terminal recording (asciinema or similar) showing a `bani run` migration from MySQL to PostgreSQL with progress output.
4. **Comparison table** — a concise version of the Section 1 comparison table (Bani vs FullConvert). Links to the full `/compare/fullconvert/` page.
5. **Connector showcase** — visual grid of supported databases with logos and "more coming" placeholder.
6. **AI agent integration callout** — a dedicated block showing the MCP setup snippet and a short example of an AI agent driving a migration. This is Bani's key differentiator — make it prominent.
7. **Community / open-source** — GitHub stars badge, contributor count, link to contributing guide.
8. **Bani Cloud teaser** — brief description with waitlist signup (links to `/cloud/`).
9. **Donate CTA** — a brief callout (e.g., "Support Bani") linking to `/donate/`. Visible on the landing page and optionally in the site-wide nav or footer.
10. **Footer** — links to docs (docs.bani.dev), GitHub repo, blog, community, donate, license (Apache-2.0), about the developer.

### 21.3 Additional Marketing Pages

**Features (`/features/`):** One page per major feature area with more depth than the landing page cards. Pages for: Apache Arrow engine, BDL (Bani Definition Language), connector system, AI agent integration (MCP + SDK), cross-platform packaging, Docker support, incremental sync, error handling and resumability. Each page links to the corresponding technical docs on docs.bani.dev.

**Comparison (`/compare/`):** Side-by-side comparisons with competing tools. At launch, at minimum: "Bani vs FullConvert" and "Bani vs pgloader". Each page: feature matrix table, honest assessment of where Bani is stronger and where it's not yet, migration path for users switching from the competing tool.

**Roadmap (`/roadmap/`):** A public-facing version of Section 22 (Future Roadmap) showing planned features, their open-source vs enterprise classification, and rough timelines. Kept in sync with the repo's roadmap. This builds trust and lets users see where the project is headed.

**Connectors (`/connectors/`):** A visual catalog of all connectors — built-in and community. Each connector has a card with: name, supported database versions, driver version matrix, status (stable/beta/planned), and a link to its docs page on docs.bani.dev. For v1.0 this is a static page; post-v1.0 it becomes a searchable, filterable catalog with a submission workflow for community connectors.

**About (`/about/`):** About the developer/team, the origin story of the project, project values (open-source, clean code, agent-native), and contact information.

**Blog (`/blog/`):** Release announcements, migration stories, and technical deep-dives. Required at launch:

1. **Launch announcement** — "Introducing Bani: Open-Source Database Migration Powered by Apache Arrow"
2. **Architecture deep-dive** — "How Bani Uses Apache Arrow for Zero-Copy Database Migrations"
3. **AI agent post** — "Agent-Native Database Migrations: How AI Agents Use Bani via MCP"

Post-launch, maintain a cadence of at least one post per release covering new features, migration case studies, and connector spotlights.

**Bani Cloud (`/cloud/`):** A single page with: brief description of the managed service vision (run migrations in the cloud, no infrastructure to manage), email waitlist signup form (collect email + optional "What databases do you migrate?" field), and clear messaging that Bani Cloud is coming soon — the open-source CLI/SDK is available now.

**Donate (`/donate/`):** A dedicated donation page that lets users and organisations support Bani's ongoing development. The page includes: a brief statement of how donations fund the project (infrastructure costs, maintainer time, documentation, community management), and two payment options:

1. **Stripe** — accepts debit and credit cards. Use Stripe Checkout (hosted payment page) or Stripe Payment Links to avoid handling card data directly. Supports one-time and recurring (monthly) donations with suggested tiers (e.g., $5, $15, $50, custom amount).
2. **PayPal** — a PayPal Donate button for users who prefer PayPal. Use PayPal's standard donate button or PayPal.me link. Supports one-time and recurring donations.

The page also includes: a "Sponsors" or "Supporters" section acknowledging recurring donors (with their consent), a transparent summary of how funds are used, and a link to the project's Open Collective or GitHub Sponsors page if one is set up in the future. The donate page is linked from the site footer on every page and from the landing page's community section.

### 21.4 Technical Documentation — docs.bani.dev (Read the Docs)

All technical and developer documentation lives on **Read the Docs** at `docs.bani.dev`. This is the authoritative reference for anyone using or extending Bani.

**Why Read the Docs:**

- Free hosting for open-source projects.
- Built-in versioning (v1.0, v1.1, `latest`) with a version switcher.
- Sphinx or MkDocs integration with auto-build on push.
- Full-text search built in.
- "Edit on GitHub" links on every page.
- PDF/epub export for offline reading.

**Documentation framework:** Use **MkDocs** with the **Material for MkDocs** theme (or Sphinx with the Furo theme as fallback). MkDocs Material is the de facto standard for Python project documentation — clean, responsive, dark mode, search, versioning via `mike`.

**Content source:** Documentation Markdown files live in `docs/` in the repo root (where they already exist per Section 7). Read the Docs builds directly from this directory. Documentation is always in sync with the code and reviewable in the same PR.

**Documentation structure on docs.bani.dev:**

```
docs.bani.dev/
├── Getting Started                # Install → first migration in <10 minutes
├── Guides/
│   ├── BDL Reference              # Full BDL specification — every element and attribute with examples
│   ├── BDL XSD                    # The XSD schema with inline commentary
│   ├── BDL JSON Schema            # The JSON Schema equivalent with inline commentary
│   ├── CLI Reference              # Every command, flag, and output format
│   ├── Python SDK                 # SDK classes, methods, and usage patterns
│   ├── MCP Server                 # MCP setup, tool reference, agent integration examples
│   ├── Docker                     # Image variants, docker-compose, CI/CD usage
│   ├── Incremental Sync           # Strategies, configuration, state management
│   ├── Error Handling             # Exception hierarchy, resumability, quarantine
│   ├── Type Mappings              # Per-connector type mapping tables and override rules
│   └── Configuration              # Config hierarchy, config file format, env vars
├── Connector Reference/
│   ├── PostgreSQL                 # Config, supported versions, driver matrix, type mappings
│   ├── MySQL                      # Same structure per connector
│   ├── MSSQL                      #
│   ├── Oracle                     #
│   └── SQLite                     #
├── Developer Guide/
│   ├── Architecture               # Hexagonal architecture, module boundaries, data flow
│   ├── Building a Connector       # Step-by-step community connector guide
│   ├── Contributing               # Code style, PR process, testing requirements
│   └── Decision Log               # Links to docs/decisions.md
├── API Reference/                 # Auto-generated from Python docstrings (mkdocstrings or sphinx-autodoc)
│   ├── Domain Models              # SchemaModel, TypeMapper, etc.
│   ├── SDK (bani.sdk)             # ProjectBuilder, SchemaInspector, MigrationResult
│   ├── Connector Interfaces       # SourceConnector, SinkConnector ABCs
│   └── BDL Parser                 # BDLParser, BDLSerializer
└── Changelog                      # Auto-generated from conventional commits
```

**Documentation requirements:**

1. **Code examples** — every guide page includes at least one runnable code example (CLI command, BDL snippet, or Python SDK snippet). Examples are tested in CI to prevent doc rot.
2. **Versioning** — docs are versioned by release using `mike` (MkDocs) or Read the Docs' built-in versioning. The `latest` version always reflects the `main` branch.
3. **Search** — Read the Docs provides full-text search out of the box.
4. **Edit links** — every page has an "Edit on GitHub" link pointing to the source Markdown file in the repo.
5. **API reference** — auto-generated from Python docstrings using `mkdocstrings` (for MkDocs) or `sphinx-autodoc` (for Sphinx). Covers: domain models, SDK classes and methods, connector interfaces, BDL parser API.
6. **Cross-linking** — the marketing site (bani.dev) links to docs.bani.dev for all "Get Started", "Learn More", and "Read the Docs" CTAs. The docs site links back to bani.dev for marketing pages (features, comparison, blog).

### 21.5 Community Hub

The community hub is not a separate site — it's a combination of existing platforms linked from both bani.dev and docs.bani.dev.

1. **GitHub Discussions** — primary forum for questions, feature requests, showcases, and RFCs. Linked prominently from the `/community/` page on bani.dev and the docs sidebar.
2. **Discord or Slack** — real-time chat for contributors and users. Channels: `#general`, `#help`, `#connectors`, `#contributing`, `#announcements`. Link with invite URL from `/community/` page.
3. **Contributing guide** — link to `CONTRIBUTING.md` in the repo. The docs site's "Developer Guide / Contributing" page expands on this with code style, PR process, and testing requirements.
4. **Connector marketplace placeholder** — the `/connectors/` page on bani.dev lists all available connectors (built-in and community) with metadata pulled from each connector's `connector.yaml`. For v1.0 this is a static page generated at build time; post-v1.0 it becomes a searchable, filterable catalog with a community submission workflow.

### 21.6 Technical Requirements

These apply to the marketing site (bani.dev). Read the Docs handles its own performance and accessibility.

1. **Performance** — Lighthouse score ≥ 95 on all four categories (Performance, Accessibility, Best Practices, SEO). The site is static; there's no excuse for poor scores.
2. **Responsive** — fully functional on mobile, tablet, and desktop.
3. **Dark mode** — support system-preference and manual toggle.
4. **Analytics** — privacy-respecting analytics (Plausible, Fathom, or Cloudflare Web Analytics). No Google Analytics.
5. **SEO** — structured data (JSON-LD), OpenGraph meta tags, sitemap.xml, robots.txt. Each page has a unique title and description.
6. **Accessibility** — WCAG 2.1 AA compliance.

---

## 22. Future Roadmap (Out of Scope for v1.0, but design for them now)

These features are NOT required for the initial release, but the architecture must not preclude them. All items marked **(open-source)** will be released under the same Apache-2.0 license as the core engine. Items marked **(enterprise)** are candidates for a paid tier (see Section 22.1).

1. **Standalone headless REST / gRPC API** **(open-source)** — expose all Bani functionality via a standalone, network-accessible REST or gRPC API for headless operation (i.e., without the Web UI frontend). The v1.0 Web UI (Section 20) already bundles an embedded FastAPI server with REST endpoints, but that server exists solely to power the UI. This roadmap item covers a dedicated API service for headless deployments, CI/CD integrations, and third-party clients.
2. **Additional connectors** **(open-source core, premium connectors possible)** — MariaDB, MongoDB (document → relational mapping), CSV/Excel, Firebird, and more. Core open-source connectors expand the community; specialty enterprise connectors (SAP HANA, Teradata, Snowflake) may be offered as paid add-ons or through a connector marketplace.
3. **Arrow Flight remote sources** **(open-source)** — connect to cloud databases and data lakes via Arrow Flight.
4. **View / stored procedure migration** **(open-source)** — translate SQL dialect-specific objects (views, sprocs, functions, triggers). This is extremely complex and should be a separate module.
5. **Schema diff** **(open-source)** — compare source and target schemas and generate ALTER statements.
6. **Data masking / anonymisation** **(open-source)** — transform sensitive columns during migration (e.g., hash emails, randomise names).
7. **RBAC and audit logging** **(enterprise)** — role-based access control for multi-user deployments and structured audit trails of migration operations.
8. **Secrets manager integration** **(enterprise)** — support `${vault:path/to/secret}` for HashiCorp Vault, `${aws-sm:secret-name}` for AWS Secrets Manager, and similar pluggable resolvers.
9. **Webhook notifications** **(enterprise)** — POST to a URL on migration events (Slack, PagerDuty, custom endpoints).
10. **Advanced MCP capabilities** **(enterprise)** — resource subscriptions (agents subscribe to migration progress as MCP resources), prompt templates (pre-built MCP prompts for common migration scenarios), and multi-agent coordination (multiple agents sharing a Bani instance).

### 22.1 Monetisation Strategy

Bani is and will remain a fully functional open-source product under Apache-2.0. The monetisation strategy preserves the open-source core while generating revenue through value-added services and enterprise features.

**Open-core model:**

The open-source product includes: the migration engine, all five initial connectors (+ community connectors), CLI, Python SDK, MCP server, BDL, Docker images, Web UI (Section 20), and all core migration features. The enterprise tier adds operational features that matter to organisations but not individual developers: RBAC, audit logging, secrets manager integration, webhook notifications, advanced MCP multi-agent coordination, and premium connectors for specialty enterprise databases (SAP HANA, Teradata, Snowflake).

**Managed service (Bani Cloud):**

A hosted version where users upload BDL files or use a visual builder, and Bani runs migrations in managed infrastructure. Pricing: per-migration, per-row, or monthly subscription for scheduled syncs. Particularly valuable because migrations are often one-off events where provisioning infrastructure is wasteful.

**Support and SLA contracts:**

Guaranteed response times, dedicated support channels, and migration assistance for enterprises running critical data transfers.

**Connector marketplace:**

A platform where third-party developers can publish connectors (free or paid). Bani takes a revenue share on paid connectors. The modular connector architecture (Section 6) and community connector guide (Section 6.4) already support this.

**Professional services:**

Migration planning, execution, and validation as a consulting service — using Bani as the tool. Each engagement feeds improvements back into the product.

**Training and certification:**

"Bani Certified Migration Engineer" program once adoption reaches sufficient scale.

---

## 23. Implementation Order

Build Bani in this sequence. Each phase produces a working, testable artifact. **Do not advance to the next phase until the current phase's Definition of Done is satisfied.** The quality gates in Section 2.4 apply to every phase.

**MVP milestone:** Phases 1–3a constitute the **Minimum Viable Product** — a working migration engine that handles PostgreSQL ↔ PostgreSQL plus one heterogeneous pair (PostgreSQL ↔ MySQL), with CLI, SDK, and basic JSON output. This is the earliest shippable artifact. Phases 3b–7 expand from MVP to the full v1.0.

### Phase 1 — Foundation

> **Phase focus:** Primary: Sections 2, 5, 7, 8, 11, 24. Reference: 3, 12. Sections 9, 10, 18–21 are not directly relevant to this phase.

1. Set up the repository: `pyproject.toml`, `ruff`, `mypy`, `pre-commit`, `pytest`, CI pipeline.
2. Implement domain models (`schema.py`, `type_mapping.py`, `dependency.py`, `errors.py`).
3. Implement BDL parser and XSD validator.
4. Implement BDL JSON Schema (`bdl-1.0.schema.json`) alongside the XSD.
5. Write unit tests for all domain logic and BDL parsing.

**Definition of Done — Phase 1:**
- CI pipeline runs on every push and PR (ruff, mypy, pytest).
- All domain models have ≥95% unit test coverage.
- BDL parser accepts the reference document (Section 5.2) and rejects at least 5 intentionally invalid BDL documents.
- Both XSD and JSON Schema validators accept/reject the same set of test documents.
- `mypy --strict` passes with zero errors.

### Phase 2 — First Connector Pair + SDK

> **Phase focus:** Primary: Sections 4, 6, 10, 14, 17, 18.3. Reference: 12, 15. Sections 19–21 are not directly relevant to this phase.

6. Implement the PostgreSQL connector (source + sink).
7. Implement the `MigrationOrchestrator` — just full-table copy, no sync, no hooks.
8. Expose the Application Service Layer as the public Python SDK (`bani.sdk`).
9. Build a minimal CLI with Typer + Rich for `bani run`, `bani validate`, and `bani schema inspect`, including `--output json`.
10. Integration test: PostgreSQL → PostgreSQL (self-migration) via both CLI and SDK.

**Definition of Done — Phase 2:**
- PostgreSQL → PostgreSQL full-table migration works end-to-end for a test schema with at least 5 tables, foreign keys, and indexes.
- Data integrity verified: row counts match, spot-check column values match.
- SDK `ProjectBuilder` → `MigrationOrchestrator.run()` path works programmatically.
- CLI `bani run`, `bani validate`, and `bani schema inspect` work in both Rich and `--output json` modes.
- Integration tests run against PostgreSQL in Docker (via `docker-compose`).
- User config file (`~/.config/bani/config.toml`) is loaded and merged into the configuration hierarchy (Section 17).
- The `ConnectorRegistry` loads drivers from namespace-isolated directories (Section 6.3.1); this isolation machinery must be in place before adding more connectors in Phase 3.
- Unit + integration test coverage ≥90%.

### Phase 3a — First Heterogeneous Pair (MVP Ship Candidate)

> **Phase focus:** Primary: Section 6. Reference: 4, 14, 15. Sections 19–21 are not directly relevant to this phase.

11. Implement the MySQL connector (source + sink).
12. Integration test: PostgreSQL → MySQL and MySQL → PostgreSQL.
13. Integration test: MySQL → MySQL (same-engine, validates multi-version driver loading).

**Definition of Done — Phase 3a (MVP):**
- PostgreSQL ↔ MySQL migrations work end-to-end (both directions).
- MySQL → MySQL same-engine migration works with multi-version driver loading (e.g., MySQL 5.x source, MySQL 8.x target).
- Type mapping coverage: both connectors handle all types in their `default_type_mappings.json`.
- CLI and SDK produce correct, documented JSON output for all supported commands.
- Unit + integration test coverage ≥90%.
- **This is a shippable artifact.** It can be tagged as a pre-release (e.g., v0.9.0) and used for early feedback.

### Phase 3b — Remaining Connectors

> **Phase focus:** Primary: Section 6, 15. Reference: 4. Sections 19–21 are not directly relevant to this phase.

14. Implement MSSQL connector.
15. Implement Oracle connector.
16. Implement SQLite connector.
17. Matrix integration tests: all 25 `(source, target)` combinations (see Section 15.3 for CI tiering).

**Definition of Done — Phase 3b:**
- All 25 source→target combinations pass integration tests with a common test schema (at the nightly CI tier minimum; full matrix at release CI).
- Each connector's driver version matrix (Section 6.3.1) is validated against at least the default driver version.
- Type mapping coverage: each connector handles all types in its `default_type_mappings.json` without error.
- Unit + integration test coverage ≥90%.

### Phase 4 — Advanced Features

> **Phase focus:** Primary: Sections 12, 13, 16. Reference: 5, 6. Sections 19–21 are not directly relevant to this phase.

18. Incremental sync engine.
19. Pre/post hooks.
20. Scheduler integration.
21. Resumability and quarantine.
22. Data preview.

**Definition of Done — Phase 4:**
- Incremental sync detects and transfers only changed rows (tested with insert, update, delete scenarios).
- Pre/post hooks execute shell commands and SQL statements at the correct lifecycle points.
- Resumability: a migration interrupted mid-transfer resumes from the last checkpoint without re-transferring completed tables.
- Quarantine: rows that fail type conversion are written to the quarantine table with error metadata.
- Data preview returns the correct N sampled rows for a given table.
- Unit + integration test coverage ≥90%.

### Phase 5 — Full CLI + AI Agent Integration

> **Phase focus:** Primary: Sections 10, 18. Reference: 14, 15. Sections 20–21 are not directly relevant to this phase.

23. Complete all v1.0 CLI commands from Section 10 (excluding commands marked `(future)`) with `--output json` on all commands.
24. Implement the MCP server (`bani mcp serve`).
25. Write BDL example library (`examples/bdl/`).

**Definition of Done — Phase 5:**
- Every v1.0 CLI command listed in Section 10 (i.e., those not marked `(future)`) works with both Rich and `--output json` modes.
- MCP server starts, registers all 8 tools (Section 18.6), and completes a full migration when invoked by an MCP client.
- BDL example library contains at least one example per connector pair and one example per advanced feature.
- Unit + integration test coverage ≥90%.

### Phase 5b — Web UI

> **Phase focus:** Primary: Section 20. Reference: 14, 18.3. Sections 9, 21 are not directly relevant to this phase.

26. Implement the FastAPI backend: REST endpoints for project CRUD, migration execution, schema inspection, connector listing, and settings.
27. Implement the WebSocket endpoint for real-time migration progress streaming.
28. Build the React SPA: Dashboard, Project Editor, Schema Browser, Migration Monitor, Run History, Connector Catalog, and Settings screens.
29. Integration tests: create a project via UI → run migration → verify data integrity.

**Definition of Done — Phase 5b:**
- `bani ui` starts the server and serves the React SPA at `http://127.0.0.1:8910`.
- Project Editor creates a valid BDL file that `bani validate` accepts.
- Schema Browser displays tables, columns, and relationships for all five connector types.
- Migration Monitor shows real-time progress via WebSocket during a PostgreSQL → MySQL migration.
- Run History displays completed and failed migration runs with correct metadata.
- Local auth token is required for all API endpoints; server binds to localhost by default.
- Unit + integration test coverage ≥90%.

### Phase 6 — Packaging and Docker

> **Phase focus:** Primary: Sections 9, 19. Reference: 4.4, 15. Sections 20, 21 are not directly relevant to this phase.

30. Build Docker image and docker-compose.yml.
31. Package with embedded Python for Windows, macOS, Linux.
32. End-to-end smoke tests on all three platforms + Docker.

**Definition of Done — Phase 6:**
- Docker image builds, starts, and runs a migration via `docker run`.
- Platform installers build for Windows, macOS, and Linux (at least one Linux format).
- End-to-end smoke tests pass on all three platforms and Docker.
- Benchmark suite (Section 4.4) runs and throughput meets the baseline floor (see Section 4.3).

### Phase 7 — Documentation, Website, and Release

> **Phase focus:** Primary: Section 21. Reference: all prior sections (for documentation accuracy). This phase requires familiarity with the entire prompt to produce accurate docs.

33. Write all technical documentation in `docs/` (Getting Started, BDL Reference, CLI Reference, SDK Guide, MCP Guide, Web UI Guide, Connector Reference pages, Developer Guide).
34. Set up MkDocs with Material theme + `mkdocs.yml` + `.readthedocs.yaml`. Configure Read the Docs to build docs.bani.dev from the `docs/` directory.
35. Build the bani.dev marketing site (Section 21.1–21.3): landing page, feature pages, comparison pages, roadmap, connector catalog, blog with 3 launch posts, Bani Cloud waitlist, about page, community links.
36. Set up CI/CD for the marketing site (`site.yml` workflow: build on push to main, deploy to GitHub Pages).
37. Set up GitHub Discussions and Discord/Slack community channels.
38. Publish v1.0.0 to PyPI, Docker Hub, and platform installers.

**Definition of Done — Phase 7:**
- docs.bani.dev is live on Read the Docs with all documentation sections from Section 21.4.
- Getting Started guide walks a new user from install to first successful migration in under 10 minutes.
- BDL Reference documents every element and attribute with examples. XSD and JSON Schema are included with inline commentary.
- Web UI Guide documents how to use the web UI dashboard, create projects, monitor migrations, and view run history.
- Connector Reference has a page per connector with config, supported versions, driver matrix, and type mappings.
- Developer Guide enables a developer to build and test a new connector without reading Bani source code.
- AI Agent Guide shows MCP setup, SDK usage, and BDL generation with working examples.
- API reference is auto-generated from docstrings and browsable on docs.bani.dev.
- All docs pass a spell-check and link-check CI job.
- bani.dev marketing site is live on GitHub Pages with: landing page, feature pages, at least one comparison page (vs FullConvert), roadmap, connector catalog, at least 3 launch blog posts, Bani Cloud waitlist, donate page (Stripe + PayPal), about page, and community links.
- Lighthouse scores ≥ 95 on all four categories for bani.dev.
- GitHub Discussions are enabled and Discord/Slack invite link is live.
- bani.dev links to docs.bani.dev for all technical CTAs; docs.bani.dev links back to bani.dev for marketing pages.
- v1.0.0 is tagged, published, and installable via `pip install bani`, `docker pull bani/bani:1.0.0`, and platform installers.

---

## 24. Conventions and Standards

| Concern | Standard |
|---|---|
| Python version | 3.12+ |
| Dependency management | `uv` (fast, lockfile-based) |
| Project metadata | PEP 621 (`pyproject.toml`) |
| Code style | Ruff (Black-compatible formatting, isort) |
| Type hints | Required on all public APIs; `mypy --strict` |
| Docstrings | Google style |
| Commit messages | Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `ci:`, `chore:`) |
| Branching | Trunk-based (main + short-lived feature branches) |
| Versioning | Semantic Versioning 2.0.0 |
| License | Apache-2.0 |
| Changelog | Auto-generated from conventional commits |

---

## 25. Glossary

| Term | Definition |
|---|---|
| **BDL** | Bani Definition Language — an XML vocabulary describing a migration project |
| **Connector** | A plugin that knows how to read from and/or write to a specific database engine |
| **MCP** | Model Context Protocol — the standard protocol for AI agents to discover and invoke external tools |
| **Port** | An abstract interface (`SourceConnector`, `SinkConnector`) that connectors implement |
| **RecordBatch** | An Apache Arrow columnar data chunk — Bani's universal data interchange unit |
| **Quarantine table** | A table in the target DB where failed rows are stored for manual inspection |
| **Checkpoint file** | A JSON file tracking migration progress for resumability |
| **SDK** | The `bani.sdk` Python package — the programmatic API for driving Bani in-process |
| **XSD** | XML Schema Definition — the formal grammar for BDL documents |
| **Web UI** | The browser-based management interface for Bani — a React SPA served by an embedded FastAPI server |

---

*End of prompt. Feed this entire document to your AI coding assistant and instruct it to begin at Phase 1.*
