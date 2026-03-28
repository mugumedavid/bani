"""Tests for the schema inspect command."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from bani.cli.app import app
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


@pytest.fixture
def runner() -> CliRunner:
    """Create a Typer CLI runner."""
    return CliRunner()


def _mock_schema() -> DatabaseSchema:
    """Create a mock DatabaseSchema for testing."""
    col1 = ColumnDefinition(
        name="id",
        data_type="INTEGER",
        nullable=False,
        is_auto_increment=True,
        ordinal_position=0,
    )
    col2 = ColumnDefinition(
        name="name",
        data_type="VARCHAR(255)",
        nullable=True,
        ordinal_position=1,
    )
    idx = IndexDefinition(
        name="idx_name",
        columns=("name",),
        is_unique=False,
    )
    fk = ForeignKeyDefinition(
        name="fk_orders_user",
        source_table="orders",
        source_columns=("user_id",),
        referenced_table="users",
        referenced_columns=("id",),
    )
    table = TableDefinition(
        schema_name="public",
        table_name="users",
        columns=(col1, col2),
        primary_key=("id",),
        indexes=(idx,),
        foreign_keys=(fk,),
        row_count_estimate=1000,
    )
    return DatabaseSchema(tables=(table,), source_dialect="postgresql")


def test_schema_inspect_help(runner: CliRunner) -> None:
    """Test schema inspect help."""
    result = runner.invoke(app, ["schema", "inspect", "--help"])
    assert result.exit_code == 0
    assert "inspect" in result.stdout.lower()
    assert "--connector" in result.stdout


def test_schema_inspect_missing_env_vars(runner: CliRunner) -> None:
    """Test schema inspect with missing environment variables."""
    # Ensure vars are not set
    os.environ.pop("NONEXISTENT_USERNAME", None)
    os.environ.pop("NONEXISTENT_PASSWORD", None)
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
def test_schema_inspect_human(
    mock_registry_class: MagicMock, runner: CliRunner
) -> None:
    """Test schema inspect with human output."""
    os.environ["TEST_USERNAME"] = "testuser"
    os.environ["TEST_PASSWORD"] = "testpass"

    mock_connector = MagicMock()
    mock_connector.return_value = mock_connector
    mock_connector.introspect_schema.return_value = _mock_schema()
    mock_registry_class.get.return_value = lambda: mock_connector

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


@patch("bani.cli.commands.schema.ConnectorRegistry")
def test_schema_inspect_json(
    mock_registry_class: MagicMock, runner: CliRunner
) -> None:
    """Test schema inspect with JSON output matching Section 18.2."""
    os.environ["TEST_USERNAME"] = "testuser"
    os.environ["TEST_PASSWORD"] = "testpass"

    mock_connector = MagicMock()
    mock_connector.return_value = mock_connector
    mock_connector.introspect_schema.return_value = _mock_schema()
    mock_registry_class.get.return_value = lambda: mock_connector

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
    output = json.loads(result.stdout.strip())
    assert output["command"] == "schema_inspect"
    assert output["connector"] == "postgresql"
    assert len(output["tables"]) == 1
    table = output["tables"][0]
    assert table["schema"] == "public"
    assert table["name"] == "users"
    assert table["row_count_estimate"] == 1000
    assert table["primary_key"] == ["id"]
    assert len(table["columns"]) == 2
    col = table["columns"][0]
    assert col["name"] == "id"
    assert col["type"] == "INTEGER"
    assert col["nullable"] is False
    assert col["auto_increment"] is True
    assert len(table["indexes"]) == 1
    assert len(table["foreign_keys"]) == 1
