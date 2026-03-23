# Contributing to Bani

Thank you for your interest in contributing to Bani!

## Getting Started

1. Fork the repository
2. Clone your fork
3. Install dependencies: `uv sync --all-extras --dev`
4. Install pre-commit hooks: `pre-commit install`
5. Create a feature branch: `git checkout -b feature/your-feature`
6. Make your changes and commit using [Conventional Commits](https://www.conventionalcommits.org/)
7. Open a pull request

## Quality Standards

All contributions must pass:

- `ruff check` — zero lint violations
- `ruff format --check` — consistent formatting
- `mypy --strict` — full type safety
- `pytest` — all tests passing

Run `make all` to check everything at once.

## Commit Messages

We use Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `ci:`, `chore:`.

## Code of Conduct

Be kind, be respectful, and help each other build great software.
