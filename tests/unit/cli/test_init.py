"""Tests for the init command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from bani.cli.app import app


@pytest.fixture
def runner() -> CliRunner:
    """Create a Typer CLI runner."""
    return CliRunner()


def test_init_help(runner: CliRunner) -> None:
    """Test init command shows help."""
    from tests.unit.cli.conftest import strip_ansi

    result = runner.invoke(app, ["init", "--help"])
    assert result.exit_code == 0
    output = strip_ansi(result.stdout)
    assert "--source" in output
    assert "--target" in output


@patch("bani.cli.commands.init.ConnectorRegistry")
def test_init_json_mode(mock_registry: type, runner: CliRunner, tmp_path: Path) -> None:
    """Test init command in JSON mode with --source and --target flags."""
    mock_registry.discover.return_value = {
        "postgresql": type("FakeConnector", (), {}),
        "mysql": type("FakeConnector", (), {}),
    }

    out_file = tmp_path / "test_migration.bdl"

    result = runner.invoke(
        app,
        [
            "--output",
            "json",
            "init",
            "--source",
            "postgresql",
            "--target",
            "mysql",
            "--out",
            str(out_file),
        ],
    )
    assert result.exit_code == 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "init"
    assert output["status"] == "ok"
    assert output["source"] == "postgresql"
    assert output["target"] == "mysql"
    assert out_file.exists()

    # Verify generated BDL content
    content = out_file.read_text()
    assert 'connector="postgresql"' in content
    assert 'connector="mysql"' in content
    assert "schemaVersion" in content


@patch("bani.cli.commands.init.ConnectorRegistry")
def test_init_unknown_source(mock_registry: type, runner: CliRunner) -> None:
    """Test init command with unknown source connector."""
    mock_registry.discover.return_value = {
        "postgresql": type("FakeConnector", (), {}),
    }

    result = runner.invoke(
        app,
        [
            "--output",
            "json",
            "init",
            "--source",
            "nosuchdb",
            "--target",
            "postgresql",
        ],
    )
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["status"] == "error"


@patch("bani.cli.commands.init.ConnectorRegistry")
def test_init_unknown_target(mock_registry: type, runner: CliRunner) -> None:
    """Test init command with unknown target connector."""
    mock_registry.discover.return_value = {
        "postgresql": type("FakeConnector", (), {}),
    }

    result = runner.invoke(
        app,
        [
            "--output",
            "json",
            "init",
            "--source",
            "postgresql",
            "--target",
            "nosuchdb",
        ],
    )
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["status"] == "error"


@patch("bani.cli.commands.init.ConnectorRegistry")
def test_init_no_connectors(mock_registry: type, runner: CliRunner) -> None:
    """Test init command when no connectors are available."""
    mock_registry.discover.return_value = {}

    result = runner.invoke(
        app,
        [
            "--output",
            "json",
            "init",
            "--source",
            "postgresql",
            "--target",
            "mysql",
        ],
    )
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["status"] == "error"


@patch("bani.cli.commands.init.ConnectorRegistry")
def test_init_json_mode_missing_flags(mock_registry: type, runner: CliRunner) -> None:
    """Test init command in JSON mode without --source flag."""
    mock_registry.discover.return_value = {
        "postgresql": type("FakeConnector", (), {}),
    }

    result = runner.invoke(
        app,
        ["--output", "json", "init"],
    )
    assert result.exit_code != 0
