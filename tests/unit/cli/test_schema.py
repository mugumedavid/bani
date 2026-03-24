"""Tests for the schema inspect command."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from bani.cli.app import app


@pytest.fixture
def runner() -> CliRunner:
    """Create a Typer CLI runner."""
    return CliRunner()


def test_schema_inspect_help(runner: CliRunner) -> None:
    """Test schema inspect help."""
    result = runner.invoke(app, ["schema", "inspect", "--help"])
    assert result.exit_code == 0
    assert "inspect" in result.stdout.lower()


def test_schema_inspect_missing_env_vars(runner: CliRunner) -> None:
    """Test schema inspect with missing environment variables."""
    result = runner.invoke(
        app,
        [
            "schema",
            "inspect",
            "--connector",
            "postgresql",
            "--host",
            "localhost",
            "--port",
            "5432",
            "--database",
            "testdb",
            "--username-env",
            "NONEXISTENT_USERNAME",
            "--password-env",
            "NONEXISTENT_PASSWORD",
        ],
    )
    assert result.exit_code != 0
    assert "not set" in result.stdout.lower()


@patch("bani.cli.commands.schema.ConnectorRegistry")
def test_schema_inspect_with_mock_connector(
    mock_registry_class: MagicMock, runner: CliRunner
) -> None:
    """Test schema inspect with mocked connector."""
    import os

    # Set environment variables
    os.environ["TEST_USERNAME"] = "testuser"
    os.environ["TEST_PASSWORD"] = "testpass"

    # Mock the connector registry and connector
    mock_registry = MagicMock()
    mock_connector = MagicMock()
    mock_schema = MagicMock()
    mock_schema.tables = ()
    mock_schema.source_dialect = "postgresql"
    mock_connector.introspect_schema.return_value = mock_schema
    mock_registry.create_source_connector.return_value = mock_connector
    mock_registry_class.return_value = mock_registry

    result = runner.invoke(
        app,
        [
            "schema",
            "inspect",
            "--connector",
            "postgresql",
            "--host",
            "localhost",
            "--port",
            "5432",
            "--database",
            "testdb",
            "--username-env",
            "TEST_USERNAME",
            "--password-env",
            "TEST_PASSWORD",
        ],
    )
    assert result.exit_code == 0


def test_schema_inspect_json_output(runner: CliRunner) -> None:
    """Test schema inspect with JSON output."""
    import os

    os.environ["TEST_USERNAME"] = "testuser"
    os.environ["TEST_PASSWORD"] = "testpass"

    with patch("bani.cli.commands.schema.ConnectorRegistry") as mock_registry_class:
        mock_registry = MagicMock()
        mock_connector = MagicMock()
        mock_schema = MagicMock()
        mock_schema.tables = ()
        mock_schema.source_dialect = "postgresql"
        mock_connector.introspect_schema.return_value = mock_schema
        mock_registry.create_source_connector.return_value = mock_connector
        mock_registry_class.return_value = mock_registry

        result = runner.invoke(
            app,
            [
                "--output",
                "json",
                "schema",
                "inspect",
                "--connector",
                "postgresql",
                "--host",
                "localhost",
                "--port",
                "5432",
                "--database",
                "testdb",
                "--username-env",
                "TEST_USERNAME",
                "--password-env",
                "TEST_PASSWORD",
            ],
        )
        assert result.exit_code == 0
