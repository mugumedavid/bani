# Bani — Claude Code Rules

The authoritative build spec is `prompt.md` at the repo root. Read it before starting any work. Identify your current phase from Section 23 and follow its Phase Focus hints.

**Current phase: 3**

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
- **Connector discovery via entry points:** All connectors register through `importlib.metadata` entry points. The orchestrator never references a concrete connector class.

## Conventions (Section 24)

| Concern | Standard |
|---|---|
| Python | 3.12+ |
| Dependency management | `uv` |
| Code style | Ruff (Black-compatible) |
| Type hints | Required on all public APIs; `mypy --strict` |
| Docstrings | Google style |
| Commits | Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `ci:`, `chore:`) |
| Branching | Trunk-based (main + short-lived feature branches) |
| Versioning | SemVer 2.0.0 |
| License | Apache-2.0 |
