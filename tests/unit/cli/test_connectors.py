"""Tests for the connectors list and info commands."""

from __future__ import annotations

import json
from typing import ClassVar
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from bani.cli.app import app
from bani.connectors.base import SinkConnector, SourceConnector


@pytest.fixture
def runner() -> CliRunner:
    """Create a Typer CLI runner."""
    return CliRunner()


class FakeSourceSink(SourceConnector, SinkConnector):
    """Fake connector that implements both source and sink for testing."""

    VERSION = "1.0.0"
    DRIVER_VERSION = "3.2.1"
    SUPPORTED_DB_VERSIONS: ClassVar[list[str]] = ["PostgreSQL 12+"]

    def connect(self, config):  # type: ignore[override]
        pass

    def disconnect(self) -> None:
        pass

    def introspect_schema(self):  # type: ignore[override]
        pass

    def read_table(  # type: ignore[override]
        self,
        table_name,
        schema_name="",
        columns=None,
        filter_sql=None,
        batch_size=100000,
    ):
        yield from ()

    def estimate_row_count(self, table_name, schema_name=""):  # type: ignore[override]
        return 0

    def create_table(self, table_def):  # type: ignore[override]
        pass

    def write_batch(self, table_name, schema_name, batch):  # type: ignore[override]
        return 0

    def create_indexes(self, table_name, schema_name, indexes):  # type: ignore[override]
        pass

    def create_foreign_keys(self, fks):  # type: ignore[override]
        pass

    def execute_sql(self, sql):  # type: ignore[override]
        pass


def test_connectors_list_help(runner: CliRunner) -> None:
    """Test connectors list shows help."""
    result = runner.invoke(app, ["connectors", "list", "--help"])
    assert result.exit_code == 0


def test_connectors_info_help(runner: CliRunner) -> None:
    """Test connectors info shows help."""
    result = runner.invoke(app, ["connectors", "info", "--help"])
    assert result.exit_code == 0


@patch("bani.cli.commands.connectors.ConnectorRegistry")
def test_connectors_list_human(mock_registry: type, runner: CliRunner) -> None:
    """Test connectors list in human mode."""
    mock_registry.discover.return_value = {
        "postgresql": FakeSourceSink,
    }

    result = runner.invoke(app, ["connectors", "list"])
    assert result.exit_code == 0
    assert "postgresql" in result.stdout


@patch("bani.cli.commands.connectors.ConnectorRegistry")
def test_connectors_list_json(mock_registry: type, runner: CliRunner) -> None:
    """Test connectors list JSON output matches Section 18.2."""
    mock_registry.discover.return_value = {
        "postgresql": FakeSourceSink,
        "mysql": FakeSourceSink,
    }

    result = runner.invoke(app, ["--output", "json", "connectors", "list"])
    assert result.exit_code == 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "connectors_list"
    assert len(output["connectors"]) == 2
    conn = output["connectors"][0]
    assert "name" in conn
    assert "version" in conn
    assert "type" in conn
    assert "default_driver_version" in conn
    assert "bundled_driver_versions" in conn
    assert "supported_db_versions" in conn
    assert conn["type"] == "source+sink"


@patch("bani.cli.commands.connectors.ConnectorRegistry")
def test_connectors_list_empty(mock_registry: type, runner: CliRunner) -> None:
    """Test connectors list when no connectors are available."""
    mock_registry.discover.return_value = {}

    result = runner.invoke(app, ["connectors", "list"])
    assert result.exit_code == 0
    assert "no connectors" in result.stdout.lower()


@patch("bani.cli.commands.connectors.ConnectorRegistry")
def test_connectors_info_json(mock_registry: type, runner: CliRunner) -> None:
    """Test connectors info in JSON mode."""
    mock_registry.get.return_value = FakeSourceSink

    result = runner.invoke(
        app, ["--output", "json", "connectors", "info", "postgresql"]
    )
    assert result.exit_code == 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "connectors_info"
    assert output["connector"]["name"] == "postgresql"
    assert output["connector"]["type"] == "source+sink"


@patch("bani.cli.commands.connectors.ConnectorRegistry")
def test_connectors_info_human(mock_registry: type, runner: CliRunner) -> None:
    """Test connectors info in human mode."""
    mock_registry.get.return_value = FakeSourceSink

    result = runner.invoke(app, ["connectors", "info", "postgresql"])
    assert result.exit_code == 0
    assert "postgresql" in result.stdout


@patch("bani.cli.commands.connectors.ConnectorRegistry")
def test_connectors_info_unknown(mock_registry: type, runner: CliRunner) -> None:
    """Test connectors info with unknown connector."""
    mock_registry.get.side_effect = ValueError("Connector 'nosuchdb' not found")

    result = runner.invoke(app, ["connectors", "info", "nosuchdb"])
    assert result.exit_code != 0
    assert "not found" in result.stdout.lower()


@patch("bani.cli.commands.connectors.ConnectorRegistry")
def test_connectors_info_unknown_json(mock_registry: type, runner: CliRunner) -> None:
    """Test connectors info with unknown connector in JSON mode."""
    mock_registry.get.side_effect = ValueError("Connector 'nosuchdb' not found")

    result = runner.invoke(app, ["--output", "json", "connectors", "info", "nosuchdb"])
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["status"] == "error"
