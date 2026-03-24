"""Tests for the main Bani CLI app."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from bani.cli.app import app


@pytest.fixture
def runner() -> CliRunner:
    """Create a Typer CLI runner."""
    return CliRunner()


def test_app_help(runner: CliRunner) -> None:
    """Test that the app displays help."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "bani" in result.stdout or "migration" in result.stdout


def test_app_version(runner: CliRunner) -> None:
    """Test that the app displays version."""
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "bani" in result.stdout


def test_no_args_shows_help(runner: CliRunner) -> None:
    """Test that running without args shows help (exit code 0 or 2)."""
    result = runner.invoke(app, [])
    # Typer with no_args_is_help=True exits with code 0 or 2 depending on version
    assert result.exit_code in (0, 2)
    assert "usage" in result.stdout.lower() or "help" in result.stdout.lower()


def test_run_command_exists(runner: CliRunner) -> None:
    """Test that the run command is registered."""
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    assert "migration" in result.stdout.lower() or "run" in result.stdout.lower()


def test_validate_command_exists(runner: CliRunner) -> None:
    """Test that the validate command is registered."""
    result = runner.invoke(app, ["validate", "--help"])
    assert result.exit_code == 0
    assert "validate" in result.stdout.lower()


def test_schema_command_exists(runner: CliRunner) -> None:
    """Test that the schema command group exists."""
    result = runner.invoke(app, ["schema", "--help"])
    assert result.exit_code == 0
    assert "schema" in result.stdout.lower()


def test_schema_inspect_command_exists(runner: CliRunner) -> None:
    """Test that the schema inspect subcommand exists."""
    result = runner.invoke(app, ["schema", "inspect", "--help"])
    assert result.exit_code == 0
    assert "inspect" in result.stdout.lower()
