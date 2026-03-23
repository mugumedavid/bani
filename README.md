# Bani

An open-source database migration engine powered by Apache Arrow.

## Overview

Bani migrates data between relational databases (MySQL, PostgreSQL, MSSQL, Oracle, SQLite) using Apache Arrow as its universal columnar interchange format. Migrations are defined declaratively via BDL (Bani Definition Language) or programmatically through a Python SDK.

## Status

**Phase 1 — Foundation.** This project is under active development. Not yet ready for production use.

## Quick Start

```bash
# Install with uv
uv pip install bani

# Scaffold a new migration project
bani init

# Run a migration
bani run project.bdl
```

## Development

```bash
# Install dev dependencies
uv sync --all-extras --dev

# Run all quality checks
make all
```

## License

Apache-2.0. See [LICENSE](LICENSE) for details.
