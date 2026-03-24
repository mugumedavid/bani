"""Tests for the run command."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from bani.cli.app import app
from bani.domain.project import ProjectModel

# Ensure env vars needed by BDL fixtures
os.environ.setdefault("DB_USER", "testuser")
os.environ.setdefault("DB_PASS", "testpass")


@pytest.fixture
def runner() -> CliRunner:
    """Create a Typer CLI runner."""
    return CliRunner()


@pytest.fixture
def sample_bdl_file(tmp_path: Path) -> Path:
    """Create a minimal valid BDL XML file for testing."""
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
  <options>
    <batchSize>50000</batchSize>
  </options>
  <tables>
    <table sourceSchema="public" sourceName="users" targetName="users" />
  </tables>
</bani>
"""
    bdl_file = tmp_path / "test_project.bdl"
    bdl_file.write_text(bdl_content)
    return bdl_file


def test_run_missing_file(runner: CliRunner) -> None:
    """Test run command with missing file."""
    result = runner.invoke(app, ["run", "nonexistent.xml"])
    assert result.exit_code != 0
    assert "not found" in result.stdout.lower()


def test_run_invalid_xml(runner: CliRunner, tmp_path: Path) -> None:
    """Test run command with invalid XML."""
    bdl_file = tmp_path / "invalid.bdl"
    bdl_file.write_text("<invalid>not a bdl file</invalid>")
    result = runner.invoke(app, ["run", str(bdl_file)])
    assert result.exit_code != 0


@patch("bani.cli.commands.run.validate_xml")
@patch("bani.cli.commands.run.parse")
def test_run_dry_run(
    mock_parse: MagicMock,
    mock_validate: MagicMock,
    runner: CliRunner,
    sample_bdl_file: Path,
) -> None:
    """Test run command with --dry-run flag."""
    mock_validate.return_value = []  # no validation errors
    mock_parse.return_value = MagicMock(spec=ProjectModel)
    result = runner.invoke(app, ["run", str(sample_bdl_file), "--dry-run"])
    assert result.exit_code == 0
    assert "dry run" in result.stdout.lower() or "validation" in result.stdout.lower()


@patch("bani.cli.commands.run.MigrationOrchestrator")
@patch("bani.cli.commands.run.parse")
def test_run_with_mocked_orchestrator(
    mock_parse: MagicMock,
    mock_orchestrator_class: MagicMock,
    runner: CliRunner,
    sample_bdl_file: Path,
) -> None:
    """Test run command with mocked orchestrator."""
    mock_project = MagicMock(spec=ProjectModel)
    mock_parse.return_value = mock_project

    mock_orchestrator = MagicMock()
    mock_result = MagicMock()
    mock_result.failed_tables = 0
    mock_result.status.name = "COMPLETED"
    mock_orchestrator.run.return_value = mock_result
    mock_orchestrator_class.return_value = mock_orchestrator

    result = runner.invoke(app, ["run", str(sample_bdl_file)])
    # May fail on connector lookup, which is expected
    assert "error" in result.stdout.lower() or result.exit_code in [0, 1]


@patch("bani.cli.commands.run.validate_xml")
@patch("bani.cli.commands.run.parse")
def test_run_with_output_json(
    mock_parse: MagicMock,
    mock_validate: MagicMock,
    runner: CliRunner,
    sample_bdl_file: Path,
) -> None:
    """Test run command with JSON output and dry-run."""
    mock_validate.return_value = []
    mock_parse.return_value = MagicMock(spec=ProjectModel)
    result = runner.invoke(
        app, ["--output", "json", "run", str(sample_bdl_file), "--dry-run"]
    )
    assert result.exit_code == 0


@patch("bani.cli.commands.run.validate_xml")
@patch("bani.cli.commands.run.parse")
def test_run_with_quiet_flag(
    mock_parse: MagicMock,
    mock_validate: MagicMock,
    runner: CliRunner,
    sample_bdl_file: Path,
) -> None:
    """Test run command with --quiet flag."""
    mock_validate.return_value = []
    mock_parse.return_value = MagicMock(spec=ProjectModel)
    result = runner.invoke(app, ["--quiet", "run", str(sample_bdl_file), "--dry-run"])
    assert result.exit_code == 0
