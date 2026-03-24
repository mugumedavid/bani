# Bani — Claude Code Rules

The authoritative build spec is `prompt.md` at the repo root. Read it before starting any work. Identify your current phase from Section 23 and follow its Phase Focus hints.

**Current phase: 3b** (Phase 3a complete)

## What Was Completed in Phase 3a

- **MySQL connector** (`src/bani/connectors/mysql/`) — full source + sink with PyMySQL driver
- **Heterogeneous integration tests** (`tests/integration/end_to_end/`) — MySQL-to-PG, PG-to-MySQL, MySQL-to-MySQL
- **Arrow-based type mapping layer** — each connector has `from_arrow_type()` in its `type_mapper.py`, `ColumnDefinition` carries `arrow_type_str` populated during introspection, sinks use it for DDL generation (N mappers, not NxN)
- **Docker Compose** environment: PostgreSQL 16 (port 5433), MySQL 8.4 (port 3306), MySQL 5.7 (port 3307)
- **Migration script**: `scripts/migrate_mysql_to_pg.py` — tested end-to-end
- **Quality gates**: ruff clean, mypy --strict clean, 647 tests passing, 95% coverage

### Key architectural decisions made during Phase 3a

- `psycopg` uses `dbname` not `database` in connection strings; `sslmode=prefer` for flexible SSL
- MySQL 8.4 replaced `--default-authentication-plugin` with `--mysql-native-password=ON`
- Default values from MySQL introspection are bare strings; PG connector's `_normalize_default()` adds quoting
- `pyproject.toml` targets Python 3.10+ (not 3.12); uses conditional dep `tomli>=2.0; python_version < '3.11'`
- `type: ignore` comments in `config.py` and `registry.py` with `warn_unused_ignores = false` mypy override for cross-version compat

### Known gaps (not blockers, but tracked)

- Type mapping doesn't carry source precision through Arrow (e.g. `DECIMAL(10,2)` becomes `decimal128(38,10)` then `numeric(38,10)`)
- No DBMS-version-aware type mapping yet (e.g. `jsonb` vs `json` for PG < 9.4) — not needed for supported versions
- `varchar(N)` maps to Arrow `string` then to `text` — length constraint is lost

## Priority Contract (Section 2.1)

When requirements conflict, resolve in this order:

1. **Security correctness** — credentials are never leaked, inputs are validated, transport is encrypted.
2. **Data correctness** — rows transferred match the source exactly; schema translation preserves semantics.
3. **Resumability** — failures are recoverable; no silent data loss.
4. **Performance** — throughput targets are met within the benchmark contract.
5. **Feature breadth** — more connectors, more sync strategies, more packaging formats.

## Quality Gates (Section 2.4) — Run Before Declaring Done

- `ruff check` and `ruff format --check` pass with zero violations.
- `mypy --strict` passes with zero errors.
- `pytest` passes with zero failures.
- Test coverage meets the phase-specific threshold (see Section 23).
- No `# type: ignore` without an accompanying comment explaining why.
- No `TODO` or `FIXME` left unresolved within the current phase's scope.

## Working Rules

- **Ask, don't guess.** If a requirement is ambiguous or contradictory, stop and ask. Log deviations in `docs/decisions.md` (see Section 2.6 for format).
- **No silent architectural deviations.** If you believe the architecture should change, explain why before making the change.
- **One phase at a time.** Complete the current phase's Definition of Done before starting the next.

## Architectural Invariants

- **Domain layer purity:** `domain/` has zero imports from `infrastructure/`, `connectors/`, `cli/`, `sdk/`, `mcp_server/`, or `ui/`.
- **Arrow as universal interchange:** Data flows as `pyarrow.RecordBatch` between connectors. No intermediate Pandas, dict-of-lists, or ORM objects.
- **Arrow as canonical type intermediate:** Source introspection populates `ColumnDefinition.arrow_type_str` via `str(pa_type)`. Sink connectors call `from_arrow_type()` on their type mapper to get native DDL types. This gives N mappers, not NxN.
- **Connector discovery via entry points:** All connectors register through `importlib.metadata` entry points. The orchestrator never references a concrete connector class.

## Conventions (Section 24)

| Concern | Standard |
|---|---|
| Python | 3.10+ (tomli conditional dep for <3.11) |
| Dependency management | `uv` or `pip install -e .` |
| Code style | Ruff (Black-compatible) |
| Type hints | Required on all public APIs; `mypy --strict` |
| Docstrings | Google style |
| Commits | Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `ci:`, `chore:`) |
| Branching | Trunk-based (main + short-lived feature branches) |
| Versioning | SemVer 2.0.0 |
| License | Apache-2.0 |
