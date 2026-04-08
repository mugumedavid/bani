"""Tests for MySQL create_table catch-and-retry logic.

Covers:
- Error 1071 (key too long) → retry with VARCHAR(191) PK columns
- Error 1118 (row too large) → retry with TEXT for large VARCHAR columns
- Error 1067 (invalid default) → retry without defaults
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pymysql
import pytest

from bani.connectors.mysql.connector import MySQLConnector
from bani.domain.schema import ColumnDefinition, TableDefinition


def _make_table_def(
    name: str = "test_table",
    columns: list[ColumnDefinition] | None = None,
    pk: list[str] | None = None,
) -> TableDefinition:
    if columns is None:
        columns = [
            ColumnDefinition(
                name="id",
                data_type="INT",
                nullable=False,
                arrow_type_str="int32",
            ),
            ColumnDefinition(
                name="name",
                data_type="VARCHAR(255)",
                nullable=True,
                arrow_type_str="string",
            ),
        ]
    return TableDefinition(
        schema_name="BANI_TEST",
        table_name=name,
        columns=tuple(columns),
        primary_key=tuple(pk or ["id"]),
        indexes=(),
        foreign_keys=(),
    )


class TestMySQLCreateTableRetry:
    """Tests for MySQL create_table error handling."""

    def _setup_connector(self) -> tuple[MySQLConnector, MagicMock]:
        connector = MySQLConnector()
        pool = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        pool.acquire.return_value.__enter__ = MagicMock(return_value=conn)
        pool.acquire.return_value.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        connector._pool = pool
        return connector, cursor

    def test_1071_retries_with_varchar_191(self) -> None:
        """Error 1071 (key too long) should retry with VARCHAR(191) PKs."""
        connector, cursor = self._setup_connector()

        columns = [
            ColumnDefinition(
                name="owneruid",
                data_type="character varying(255)",
                nullable=False,
                arrow_type_str="string",
            ),
            ColumnDefinition(
                name="key",
                data_type="character varying(255)",
                nullable=False,
                arrow_type_str="string",
            ),
        ]
        table_def = _make_table_def(
            columns=columns, pk=["owneruid", "key"]
        )

        executed_sqls: list[str] = []

        def execute_side_effect(sql: str, *args: object) -> None:
            if "CREATE TABLE" in sql:
                executed_sqls.append(sql)
                if "VARCHAR(255)" in sql:
                    raise pymysql.err.OperationalError(
                        1071,
                        "Specified key was too long; max key length is 767 bytes",
                    )
            # fetchall for FK cleanup
            if "KEY_COLUMN_USAGE" in sql:
                return None

        cursor.execute.side_effect = execute_side_effect
        cursor.fetchall.return_value = []

        connector.create_table(table_def)

        assert len(executed_sqls) == 2
        assert "VARCHAR(255)" in executed_sqls[0]
        assert "VARCHAR(191)" in executed_sqls[1]

    def test_1118_retries_with_text_columns(self) -> None:
        """Error 1118 (row too large) should convert large VARCHAR to TEXT."""
        connector, cursor = self._setup_connector()

        columns = [
            ColumnDefinition(
                name="id",
                data_type="INT",
                nullable=False,
                arrow_type_str="int32",
            ),
            ColumnDefinition(
                name="data",
                data_type="VARCHAR(1000)",
                nullable=True,
                arrow_type_str="string",
            ),
        ]
        table_def = _make_table_def(columns=columns)

        executed_sqls: list[str] = []

        def execute_side_effect(sql: str, *args: object) -> None:
            if "CREATE TABLE" in sql:
                executed_sqls.append(sql)
                if "VARCHAR(1000)" in sql:
                    raise pymysql.err.OperationalError(
                        1118,
                        "Row size too large",
                    )
            if "KEY_COLUMN_USAGE" in sql:
                return None

        cursor.execute.side_effect = execute_side_effect
        cursor.fetchall.return_value = []

        connector.create_table(table_def)

        assert len(executed_sqls) == 2
        assert "VARCHAR(1000)" in executed_sqls[0]
        # Non-PK column should be converted to TEXT (or LONGTEXT)
        assert "VARCHAR(1000)" not in executed_sqls[1]

    def test_1067_retries_without_defaults(self) -> None:
        """Error 1067 (invalid default) should retry stripping defaults."""
        connector, cursor = self._setup_connector()

        columns = [
            ColumnDefinition(
                name="id",
                data_type="INT",
                nullable=False,
                arrow_type_str="int32",
            ),
            ColumnDefinition(
                name="installed_on",
                data_type="DATETIME",
                nullable=True,
                arrow_type_str="timestamp[us]",
                default_value="CURRENT_TIMESTAMP",
            ),
        ]
        table_def = _make_table_def(columns=columns)

        executed_sqls: list[str] = []

        call_count = 0

        def execute_side_effect(sql: str, *args: object) -> None:
            nonlocal call_count
            if "CREATE TABLE" in sql:
                executed_sqls.append(sql)
                call_count += 1
                if call_count == 1:
                    raise pymysql.err.OperationalError(
                        1067,
                        "Invalid default value for 'installed_on'",
                    )
            if "KEY_COLUMN_USAGE" in sql:
                return None

        cursor.execute.side_effect = execute_side_effect
        cursor.fetchall.return_value = []

        connector.create_table(table_def)

        assert len(executed_sqls) == 2
        # First attempt has column DEFAULT, second does not
        assert "DEFAULT CURRENT_TIMESTAMP" in executed_sqls[0] or "DEFAULT" in executed_sqls[0].split("CHARSET")[0]
        # Second attempt should not have column defaults
        create_part = executed_sqls[1].split("ENGINE=")[0]
        assert "DEFAULT" not in create_part

    def test_unknown_error_raises(self) -> None:
        """Unknown MySQL errors should propagate."""
        connector, cursor = self._setup_connector()

        table_def = _make_table_def()

        def execute_side_effect(sql: str, *args: object) -> None:
            if "CREATE TABLE" in sql:
                raise pymysql.err.OperationalError(
                    9999, "Unknown error"
                )
            if "KEY_COLUMN_USAGE" in sql:
                return None

        cursor.execute.side_effect = execute_side_effect
        cursor.fetchall.return_value = []

        with pytest.raises(pymysql.err.OperationalError, match="9999"):
            connector.create_table(table_def)
