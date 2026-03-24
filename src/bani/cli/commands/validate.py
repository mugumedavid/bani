"""Validate command — validates a BDL project file (Section 10.2)."""

from __future__ import annotations

import json
from pathlib import Path

import click
import typer
from rich.console import Console

from bani.bdl.parser import parse
from bani.bdl.validator import validate_json, validate_xml
from bani.cli.formatters import format_validation_results
from bani.domain.errors import BaniError


def validate(
    project_file: str = typer.Argument(..., help="Path to BDL project file"),
) -> None:
    """Validate a BDL project file.

    Performs both XML/JSON schema validation and semantic validation by
    parsing the project file.

    Args:
        project_file: Path to the BDL XML or JSON project file.
    """
    try:
        ctx_obj = click.get_current_context().obj
    except RuntimeError:
        ctx_obj = None

    if ctx_obj is None:
        ctx_obj = {"output": "human", "console": Console()}

    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    project_path = Path(project_file)
    if not project_path.exists():
        console.print(f"[red]Error:[/red] File not found: {project_file}")
        raise typer.Exit(1)

    errors: list[str] = []
    warnings: list[str] = []

    # Schema validation
    try:
        content = project_path.read_text()
        if project_file.endswith(".json") or project_file.endswith(".bdl.json"):
            schema_errors = validate_json(content)
        else:
            schema_errors = validate_xml(content)
        errors.extend(schema_errors)
    except Exception as e:
        errors.append(f"Failed to validate schema: {e}")

    # Semantic validation (by parsing)
    try:
        parse(project_path)
        # Could add more semantic checks here
    except BaniError as e:
        errors.append(str(e))
    except Exception as e:
        errors.append(f"Semantic validation error: {e}")

    # Output results
    if output_format == "json":
        result = {
            "command": "validate",
            "status": "error" if errors else "ok",
            "errors": errors,
            "warnings": warnings,
            "schema_version": "1.0",
        }
        console.print(json.dumps(result))
    else:
        format_validation_results(console, errors, warnings)

    # Exit with appropriate code
    exit_code = 1 if errors else 0
    raise typer.Exit(exit_code)
