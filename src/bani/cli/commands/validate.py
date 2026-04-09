"""Validate command — validates a BDL project file (Section 10.1, 18.2)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click
import typer
from rich.console import Console

from bani.bdl.parser import parse
from bani.bdl.validator import validate_json, validate_xml
from bani.cli.formatters import format_validation_results
from bani.domain.errors import BaniError


def _get_ctx() -> dict[str, Any]:
    """Retrieve context from the Typer/Click context chain."""
    try:
        ctx_obj = click.get_current_context().obj
    except RuntimeError:
        ctx_obj = None

    if ctx_obj is None:
        ctx_obj = {"output": "human", "console": Console()}
    result: dict[str, Any] = ctx_obj
    return result


def validate(
    project_file: str = typer.Argument(..., help="Path to BDL project file"),
) -> None:
    """Validate a BDL project file.

    Performs both XML/JSON schema validation and semantic validation by
    parsing the project file.

    Args:
        project_file: Path to the BDL XML or JSON project file.
    """
    ctx_obj = _get_ctx()
    output_format = ctx_obj.get("output", "human")
    console: Console = ctx_obj.get("console", Console())

    project_path = Path(project_file)
    if not project_path.exists():
        if output_format == "json":
            result = {
                "command": "validate",
                "status": "error",
                "errors": [
                    {
                        "severity": "error",
                        "code": "BDL-000",
                        "message": f"File not found: {project_file}",
                    }
                ],
                "warnings": [],
                "schema_version": "1.0",
            }
            sys.stdout.write(json.dumps(result) + "\n")
            sys.stdout.flush()
        else:
            console.print(f"[red]Error:[/red] File not found: {project_file}")
        raise typer.Exit(1)

    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    # Schema validation
    try:
        content = project_path.read_text()
        if project_file.endswith(".json") or project_file.endswith(".bdl.json"):
            schema_errors = validate_json(content)
        else:
            schema_errors = validate_xml(content)
        for err_msg in schema_errors:
            errors.append(
                {
                    "severity": "error",
                    "code": "BDL-001",
                    "message": err_msg,
                }
            )
    except Exception as e:
        errors.append(
            {
                "severity": "error",
                "code": "BDL-002",
                "message": f"Failed to validate schema: {e}",
            }
        )

    # Semantic validation (by parsing)
    try:
        parse(project_path)
    except BaniError as e:
        errors.append(
            {
                "severity": "error",
                "code": "BDL-003",
                "message": str(e),
            }
        )
    except Exception as e:
        errors.append(
            {
                "severity": "error",
                "code": "BDL-004",
                "message": f"Semantic validation error: {e}",
            }
        )

    # Output results
    if output_format == "json":
        result_obj = {
            "command": "validate",
            "status": "error" if errors else "ok",
            "errors": errors,
            "warnings": warnings,
            "schema_version": "1.0",
        }
        sys.stdout.write(json.dumps(result_obj) + "\n")
        sys.stdout.flush()
    else:
        error_msgs = [e["message"] for e in errors]
        warning_msgs = [w["message"] for w in warnings]
        format_validation_results(console, error_msgs, warning_msgs)

    # Exit with appropriate code
    exit_code = 1 if errors else 0
    raise typer.Exit(exit_code)
