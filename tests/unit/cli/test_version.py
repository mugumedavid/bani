"""Tests for the version command."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from bani import __version__
from bani.cli.app import app
from bani.connectors.base import SinkConnector, SourceConnector


@pytest.fixture
def runner() -> CliRunner:
    """Create a Typer CLI runner."""
    return CliRunner()


class FakeConnector(SourceConnector, SinkConnector):
    """Fake connector for version testing."""

    VERSION = "1.0.0"
    DRIVER_VERSION = "3.2.1"

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


def test_version_help(runner: CliRunner) -> None:
    """Test version command shows help."""
    result = runner.invoke(app, ["version", "--help"])
    assert result.exit_code == 0


@patch("bani.cli.commands.version.ConnectorRegistry")
def test_version_human(mock_registry: type, runner: CliRunner) -> None:
    """Test version command in human mode."""
    mock_registry.discover.return_value = {
        "postgresql": FakeConnector,
    }

    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout
    assert "postgresql" in result.stdout


@patch("bani.cli.commands.version.ConnectorRegistry")
def test_version_json(mock_registry: type, runner: CliRunner) -> None:
    """Test version command in JSON mode."""
    mock_registry.discover.return_value = {
        "postgresql": FakeConnector,
        "mysql": FakeConnector,
    }

    result = runner.invoke(app, ["--output", "json", "version"])
    assert result.exit_code == 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "version"
    assert output["bani_version"] == __version__
    assert len(output["connectors"]) == 2
    conn = output["connectors"][0]
    assert "name" in conn
    assert "version" in conn
    assert "driver_version" in conn


@patch("bani.cli.commands.version.ConnectorRegistry")
def test_version_no_connectors(mock_registry: type, runner: CliRunner) -> None:
    """Test version command when no connectors are installed."""
    mock_registry.discover.return_value = {}

    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout
    assert "no connectors" in result.stdout.lower()


@patch("bani.cli.commands.version.ConnectorRegistry")
def test_version_json_no_connectors(mock_registry: type, runner: CliRunner) -> None:
    """Test version JSON output with no connectors."""
    mock_registry.discover.return_value = {}

    result = runner.invoke(app, ["--output", "json", "version"])
    assert result.exit_code == 0
    output = json.loads(result.stdout.strip())
    assert output["bani_version"] == __version__
    assert output["connectors"] == []
