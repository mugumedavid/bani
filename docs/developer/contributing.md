# Contributing

Thank you for your interest in contributing to Bani. This guide covers development setup, quality standards, and the PR process.

---

## Development Setup

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Docker (for integration tests)

### Clone and Install

```bash
git clone https://github.com/mugumedavid/bani.git
cd bani

# Install with all extras and dev dependencies
uv sync --all-extras --dev

# Or with pip
pip install -e ".[sqlite-extras]"
pip install -e ".[dev]"
```

### Verify Installation

```bash
bani version
```

---

## Running Tests

### Unit Tests

```bash
# Run all unit tests (excludes integration and benchmark)
pytest

# Run with verbose output
pytest -v

# Run a specific test file
pytest tests/unit/test_parser.py

# Run a specific test
pytest tests/unit/test_parser.py::TestXMLParser::test_basic_project
```

### Integration Tests

Integration tests require Docker containers for database services:

```bash
# Start database services
docker compose up -d postgres mysql mssql oracle

# Wait for health checks
docker compose ps

# Run integration tests
pytest -m integration

# Run specific integration test
pytest -m integration tests/integration/end_to_end/
```

### Benchmarks

```bash
pytest -m benchmark
```

---

## Code Style

### Ruff (Linting and Formatting)

Bani uses [Ruff](https://docs.astral.sh/ruff/) for both linting and formatting (Black-compatible).

```bash
# Check for lint violations
ruff check src/ tests/

# Auto-fix violations
ruff check --fix src/ tests/

# Check formatting
ruff format --check src/ tests/

# Apply formatting
ruff format src/ tests/
```

Configuration in `pyproject.toml`:

- Target: Python 3.10
- Line length: 88
- Rules: E, W, F, I (isort), B (bugbear), UP (pyupgrade), RUF

### Type Checking

Bani requires full type safety with `mypy --strict`:

```bash
mypy --strict src/bani/
```

Rules:

- All public APIs must have type hints.
- No `# type: ignore` without an accompanying comment explaining why.
- Cross-version compatibility ignores (e.g. `tomllib` on Python <3.11) are configured in `pyproject.toml` overrides.

---

## Quality Gates

Before every commit, ensure all quality gates pass:

```bash
# Run everything at once
make all

# Or individually:
ruff check src/ tests/
ruff format --check src/ tests/
mypy --strict src/bani/
pytest
```

| Gate | Command | Requirement |
|---|---|---|
| Lint | `ruff check` | Zero violations |
| Format | `ruff format --check` | Consistent formatting |
| Types | `mypy --strict` | Zero errors |
| Tests | `pytest` | All passing |

---

## Commit Conventions

Bani uses [Conventional Commits](https://www.conventionalcommits.org/):

| Prefix | Use |
|---|---|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `docs:` | Documentation only |
| `refactor:` | Code restructuring without behavior change |
| `test:` | Adding or updating tests |
| `ci:` | CI/CD changes |
| `chore:` | Build process, dependencies, tooling |

Examples:

```
feat: add Oracle connector with thick mode support
fix: resolve MSSQL FK creation cascade cycle detection
docs: add type mapping tables for all connectors
test: add integration tests for MySQL 5.7 compatibility
```

---

## Pull Request Process

1. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/my-feature
   ```

2. **Make your changes** following the code style and conventions above.

3. **Ensure all quality gates pass** before pushing.

4. **Push and create a PR** against `main`:
   ```bash
   git push -u origin feature/my-feature
   gh pr create --title "feat: add my feature" --body "Description of changes"
   ```

5. **PR requirements:**
   - All CI checks must pass (lint, types, tests)
   - At least one approving review
   - Conventional commit message in the PR title
   - Tests for new functionality

### PR Template

```markdown
## Summary
- Brief description of what changed and why

## Test Plan
- [ ] Unit tests added/updated
- [ ] Integration tests pass
- [ ] Manual testing completed
```

---

## Project Structure

```
src/bani/
├── __init__.py          # Package version
├── domain/              # Pure business logic (no external deps)
│   ├── project.py       # ProjectModel and related dataclasses
│   ├── schema.py        # DatabaseSchema, TableDefinition, etc.
│   └── errors.py        # Exception hierarchy
├── application/         # Orchestration layer
│   ├── orchestrator.py  # Migration execution
│   ├── progress.py      # Event-based progress tracking
│   ├── checkpoint.py    # Resumability state management
│   └── preview.py       # Data preview
├── connectors/          # Database connector implementations
│   ├── base.py          # SourceConnector, SinkConnector ABCs
│   ├── registry.py      # Entry-point discovery
│   ├── postgresql/
│   ├── mysql/
│   ├── mssql/
│   ├── oracle/
│   └── sqlite/
├── bdl/                 # BDL parser and validator
│   ├── parser.py
│   └── validator.py
├── cli/                 # Typer-based CLI
│   ├── app.py
│   └── commands/
├── sdk/                 # Python SDK
│   ├── bani.py
│   ├── project_builder.py
│   └── schema_inspector.py
├── mcp_server/          # MCP server
│   ├── server.py
│   └── tools.py
├── ui/                  # FastAPI + React Web UI
│   ├── server.py
│   └── routes/
└── infra/               # Infrastructure concerns
    ├── config.py
    ├── connections.py
    └── logging.py
```

---

## Architectural Invariants

When contributing, respect these invariants:

1. **Domain layer purity:** `domain/` has zero imports from any other layer.
2. **Arrow interchange:** Data flows as `pyarrow.RecordBatch` -- no Pandas, no dicts.
3. **Arrow type intermediate:** Source populates `arrow_type_str`, sink reads it via `from_arrow_type()`.
4. **Entry-point discovery:** Connectors register via `pyproject.toml` entry points. The orchestrator never references a concrete connector class.

---

## Code of Conduct

Be kind, be respectful, and help each other build great software.
