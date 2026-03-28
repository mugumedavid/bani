"""Tests for the validate command."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from bani.cli.app import app

os.environ.setdefault("DB_USER", "testuser")
os.environ.setdefault("DB_PASS", "testpass")


@pytest.fixture
def runner() -> CliRunner:
    """Create a Typer CLI runner."""
    return CliRunner()


@pytest.fixture
def valid_bdl_file(tmp_path: Path) -> Path:
    """Create a minimal valid BDL XML file."""
    bdl_content = """<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="test-project" description="Test" author="test" />
  <source connector="postgresql">
    <connection host="localhost" port="5432" database="source_db"
                username="${env:DB_USER}" password="${env:DB_PASS}" />
  </source>
  <target connector="postgresql">
    <connection host="localhost" port="5432" database="target_db"
                username="${env:DB_USER}" password="${env:DB_PASS}" />
  </target>
  <tables>
    <table sourceSchema="public" sourceName="users" targetName="users" />
  </tables>
</bani>
"""
    bdl_file = tmp_path / "valid.bdl"
    bdl_file.write_text(bdl_content)
    return bdl_file


@pytest.fixture
def invalid_bdl_file(tmp_path: Path) -> Path:
    """Create an invalid BDL file."""
    bdl_file = tmp_path / "invalid.bdl"
    bdl_file.write_text("<invalid>not a bdl</invalid>")
    return bdl_file


def test_validate_help(runner: CliRunner) -> None:
    """Test validate command shows help."""
    result = runner.invoke(app, ["validate", "--help"])
    assert result.exit_code == 0
    assert "validate" in result.stdout.lower()


def test_validate_missing_file(runner: CliRunner) -> None:
    """Test validate command with missing file."""
    result = runner.invoke(app, ["validate", "nonexistent.xml"])
    assert result.exit_code != 0
    assert "not found" in result.stdout.lower()


def test_validate_missing_file_json(runner: CliRunner) -> None:
    """Test validate command with missing file in JSON mode."""
    result = runner.invoke(app, ["--output", "json", "validate", "nonexistent.xml"])
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "validate"
    assert output["status"] == "error"
    assert len(output["errors"]) > 0
    assert output["schema_version"] == "1.0"


@patch("bani.cli.commands.validate.validate_xml")
@patch("bani.cli.commands.validate.parse")
def test_validate_valid_file(
    mock_parse: MagicMock,
    mock_validate: MagicMock,
    runner: CliRunner,
    valid_bdl_file: Path,
) -> None:
    """Test validate command with valid file."""
    mock_validate.return_value = []
    mock_parse.return_value = MagicMock()
    result = runner.invoke(app, ["validate", str(valid_bdl_file)])
    assert result.exit_code == 0


def test_validate_invalid_file(runner: CliRunner, invalid_bdl_file: Path) -> None:
    """Test validate command with invalid file."""
    result = runner.invoke(app, ["validate", str(invalid_bdl_file)])
    assert result.exit_code != 0


@patch("bani.cli.commands.validate.validate_xml")
@patch("bani.cli.commands.validate.parse")
def test_validate_json_output_ok(
    mock_parse: MagicMock,
    mock_validate: MagicMock,
    runner: CliRunner,
    valid_bdl_file: Path,
) -> None:
    """Test validate command with JSON output on valid file."""
    mock_validate.return_value = []
    mock_parse.return_value = MagicMock()
    result = runner.invoke(app, ["--output", "json", "validate", str(valid_bdl_file)])
    assert result.exit_code == 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "validate"
    assert output["status"] == "ok"
    assert output["errors"] == []
    assert output["warnings"] == []
    assert output["schema_version"] == "1.0"


def test_validate_json_output_invalid(
    runner: CliRunner, invalid_bdl_file: Path
) -> None:
    """Test validate command with JSON output on invalid file."""
    result = runner.invoke(app, ["--output", "json", "validate", str(invalid_bdl_file)])
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "validate"
    assert output["status"] == "error"
    assert len(output["errors"]) > 0


@patch("bani.cli.commands.validate.validate_xml")
@patch("bani.cli.commands.validate.parse")
def test_validate_json_error_structure(
    mock_parse: MagicMock,
    mock_validate: MagicMock,
    runner: CliRunner,
    valid_bdl_file: Path,
) -> None:
    """Test that validation errors follow Section 18.2 schema."""
    mock_validate.return_value = ["Unknown connector 'mysq'. Did you mean 'mysql'?"]
    mock_parse.return_value = MagicMock()
    result = runner.invoke(app, ["--output", "json", "validate", str(valid_bdl_file)])
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "validate"
    assert output["status"] == "error"
    err = output["errors"][0]
    assert "severity" in err
    assert "code" in err
    assert "message" in err
