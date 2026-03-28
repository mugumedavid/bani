"""Tests for the preview command."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from bani.application.preview import (
    ColumnPreview,
    PreviewResult,
    TablePreview,
)
from bani.cli.app import app

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
  <tables>
    <table sourceSchema="public" sourceName="users" targetName="users" />
  </tables>
</bani>
"""
    bdl_file = tmp_path / "preview_test.bdl"
    bdl_file.write_text(bdl_content)
    return bdl_file


def _mock_preview_result() -> PreviewResult:
    """Create a mock PreviewResult for testing."""
    columns = (
        ColumnPreview(name="id", data_type="INTEGER", nullable=False, arrow_type="int32"),
        ColumnPreview(name="name", data_type="VARCHAR(255)", nullable=True, arrow_type="string"),
    )
    sample_rows = (
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    )
    table_preview = TablePreview(
        table_name="users",
        schema_name="public",
        row_count_estimate=100,
        columns=columns,
        sample_rows=sample_rows,
    )
    return PreviewResult(tables=(table_preview,), source_dialect="postgresql")


def test_preview_help(runner: CliRunner) -> None:
    """Test preview command shows help."""
    result = runner.invoke(app, ["preview", "--help"])
    assert result.exit_code == 0
    assert "--sample-size" in result.stdout


def test_preview_missing_file(runner: CliRunner) -> None:
    """Test preview command with missing file."""
    result = runner.invoke(app, ["preview", "nonexistent.xml"])
    assert result.exit_code != 0
    assert "not found" in result.stdout.lower()


def test_preview_missing_file_json(runner: CliRunner) -> None:
    """Test preview command with missing file in JSON mode."""
    result = runner.invoke(app, ["--output", "json", "preview", "nonexistent.xml"])
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "preview"
    assert output["status"] == "error"


@patch("bani.cli.commands.preview.preview_source")
@patch("bani.cli.commands.preview.ConnectorRegistry")
@patch("bani.cli.commands.preview.validate_xml")
@patch("bani.cli.commands.preview.parse")
def test_preview_human_output(
    mock_parse: MagicMock,
    mock_validate: MagicMock,
    mock_registry: MagicMock,
    mock_preview: MagicMock,
    runner: CliRunner,
    sample_bdl_file: Path,
) -> None:
    """Test preview command with human output."""
    mock_validate.return_value = []
    mock_project = MagicMock()
    mock_project.source = MagicMock()
    mock_project.source.dialect = "postgresql"
    mock_parse.return_value = mock_project

    mock_connector = MagicMock()
    mock_registry.get.return_value = lambda: mock_connector

    mock_preview.return_value = _mock_preview_result()

    result = runner.invoke(app, ["preview", str(sample_bdl_file)])
    assert result.exit_code == 0
    assert "users" in result.stdout
    assert "Alice" in result.stdout or "postgresql" in result.stdout


@patch("bani.cli.commands.preview.preview_source")
@patch("bani.cli.commands.preview.ConnectorRegistry")
@patch("bani.cli.commands.preview.validate_xml")
@patch("bani.cli.commands.preview.parse")
def test_preview_json_output(
    mock_parse: MagicMock,
    mock_validate: MagicMock,
    mock_registry: MagicMock,
    mock_preview: MagicMock,
    runner: CliRunner,
    sample_bdl_file: Path,
) -> None:
    """Test preview command with JSON output."""
    mock_validate.return_value = []
    mock_project = MagicMock()
    mock_project.source = MagicMock()
    mock_project.source.dialect = "postgresql"
    mock_parse.return_value = mock_project

    mock_connector = MagicMock()
    mock_registry.get.return_value = lambda: mock_connector

    mock_preview.return_value = _mock_preview_result()

    result = runner.invoke(app, ["--output", "json", "preview", str(sample_bdl_file)])
    assert result.exit_code == 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "preview"
    assert output["source_dialect"] == "postgresql"
    assert len(output["tables"]) == 1
    table = output["tables"][0]
    assert table["table"] == "users"
    assert table["schema"] == "public"
    assert len(table["columns"]) == 2
    assert len(table["sample_rows"]) == 2
