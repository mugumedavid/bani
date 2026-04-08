"""Tests for Oracle identifier shortening and create_table retry logic.

Covers:
- _shorten_identifier truncation with hash suffix
- ORA-00972 (identifier too long) catch-and-retry
- ORA-02000 (GENERATED ALWAYS AS IDENTITY) catch-and-retry
- Name mapping propagation to write_batch, create_indexes, create_foreign_keys
- PK columns downgraded to VARCHAR2(255) instead of VARCHAR2(4000)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pyarrow as pa
import pytest

from bani.connectors.oracle.connector import (
    OracleConnector,
    _shorten_identifier,
)
from bani.domain.schema import (
    ColumnDefinition,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class TestShortenIdentifier:
    """Tests for the _shorten_identifier helper."""

    def test_short_name_unchanged(self) -> None:
        assert _shorten_identifier("users", 30) == "users"

    def test_exact_length_unchanged(self) -> None:
        name = "a" * 30
        assert _shorten_identifier(name, 30) == name

    def test_long_name_truncated_with_hash(self) -> None:
        name = "eventvisualization_categoryoptiongroupsetdimensions"
        result = _shorten_identifier(name, 30)
        assert len(result) == 30
        # Should end with _XXXX (underscore + 4 hex chars)
        assert result[-5] == "_"
        assert all(c in "0123456789abcdef" for c in result[-4:])

    def test_deterministic(self) -> None:
        """Same input always produces the same output."""
        name = "very_long_identifier_that_exceeds_thirty_chars"
        assert _shorten_identifier(name, 30) == _shorten_identifier(name, 30)

    def test_different_names_different_hashes(self) -> None:
        """Two long names should produce different shortened names."""
        a = _shorten_identifier("a" * 50, 30)
        b = _shorten_identifier("b" * 50, 30)
        assert a != b

    def test_custom_max_length(self) -> None:
        result = _shorten_identifier("abcdefghij", 8)
        assert len(result) == 8


class TestOracleCreateTableRetry:
    """Tests for create_table catch-and-retry on ORA errors."""

    def _make_table_def(
        self,
        name: str = "test_table",
        columns: list[ColumnDefinition] | None = None,
        pk: list[str] | None = None,
        auto_increment: bool = False,
    ) -> TableDefinition:
        if columns is None:
            columns = [
                ColumnDefinition(
                    name="id",
                    data_type="NUMBER(10,0)",
                    nullable=False,
                    arrow_type_str="int32",
                    is_auto_increment=auto_increment,
                ),
                ColumnDefinition(
                    name="name",
                    data_type="VARCHAR2(100)",
                    nullable=True,
                    arrow_type_str="string",
                ),
            ]
        return TableDefinition(
            schema_name="SA",
            table_name=name,
            columns=tuple(columns),
            primary_key=tuple(pk or ["id"]),
            indexes=(),
            foreign_keys=(),
        )

    @patch("bani.connectors.oracle.connector.oracledb")
    def test_ora_00972_retries_with_shortened_names(
        self, mock_oracledb: MagicMock
    ) -> None:
        """ORA-00972 should trigger retry with shortened identifiers."""
        connector = OracleConnector()
        pool = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        pool.acquire.return_value.__enter__ = MagicMock(return_value=conn)
        pool.acquire.return_value.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor
        connector._pool = pool
        connector._owner = "SA"

        long_name = "a" * 50
        table_def = self._make_table_def(name=long_name)

        # First call raises ORA-00972, second succeeds
        call_count = 0

        def execute_side_effect(sql: str) -> None:
            nonlocal call_count
            if "CREATE TABLE" in sql:
                call_count += 1
                if call_count == 1:
                    raise Exception("ORA-00972: identifier is too long")

        cursor.execute.side_effect = execute_side_effect

        connector.create_table(table_def)

        # Should have been called twice (original + retry)
        assert call_count == 2
        # Name map should have the shortened name
        assert long_name in connector._name_map
        assert len(connector._name_map[long_name]) == 30

    @patch("bani.connectors.oracle.connector.oracledb")
    def test_ora_02000_retries_without_identity(
        self, mock_oracledb: MagicMock
    ) -> None:
        """ORA-02000 should trigger retry without IDENTITY syntax."""
        connector = OracleConnector()
        pool = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        pool.acquire.return_value.__enter__ = MagicMock(return_value=conn)
        pool.acquire.return_value.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor
        connector._pool = pool
        connector._owner = "SA"

        table_def = self._make_table_def(auto_increment=True)

        executed_sqls: list[str] = []

        def execute_side_effect(sql: str) -> None:
            if "CREATE TABLE" in sql:
                executed_sqls.append(sql)
                if "IDENTITY" in sql:
                    raise Exception("ORA-02000: missing ALWAYS keyword")

        cursor.execute.side_effect = execute_side_effect

        connector.create_table(table_def)

        # First attempt has IDENTITY, second does not
        assert len(executed_sqls) == 2
        assert "IDENTITY" in executed_sqls[0]
        assert "IDENTITY" not in executed_sqls[1]

    def test_pk_columns_use_varchar2_255_not_4000(self) -> None:
        """PK columns mapped from CLOB should use VARCHAR2(255)."""
        connector = OracleConnector()
        pool = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        pool.acquire.return_value.__enter__ = MagicMock(return_value=conn)
        pool.acquire.return_value.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor
        connector._pool = pool
        connector._owner = "SA"

        columns = [
            ColumnDefinition(
                name="key_col",
                data_type="text",
                nullable=False,
                arrow_type_str="string",
            ),
        ]
        table_def = self._make_table_def(columns=columns, pk=["key_col"])

        executed_sqls: list[str] = []
        cursor.execute.side_effect = lambda sql: executed_sqls.append(sql)

        connector.create_table(table_def)

        create_sql = [s for s in executed_sqls if "CREATE TABLE" in s][0]
        assert "VARCHAR2(255)" in create_sql
        assert "VARCHAR2(4000)" not in create_sql


class TestNameMapPropagation:
    """Tests that _name_map is used in write_batch, create_indexes, etc."""

    def test_resolve_name_returns_shortened(self) -> None:
        connector = OracleConnector()
        connector._name_map["original_name"] = "short_nm"
        assert connector._resolve_name("original_name") == "short_nm"

    def test_resolve_name_returns_original_if_no_mapping(self) -> None:
        connector = OracleConnector()
        assert connector._resolve_name("some_name") == "some_name"

    @patch("bani.connectors.oracle.connector.oracledb")
    def test_write_batch_renames_columns(
        self, mock_oracledb: MagicMock
    ) -> None:
        """write_batch should rename Arrow columns using _name_map."""
        connector = OracleConnector()
        pool = MagicMock()
        conn = MagicMock()
        pool.acquire.return_value.__enter__ = MagicMock(return_value=conn)
        pool.acquire.return_value.__exit__ = MagicMock(return_value=False)
        connector._pool = pool
        connector._owner = "SA"
        connector._name_map["long_column_name"] = "short_col"
        connector._name_map["long_table"] = "short_tbl"

        batch = pa.record_batch(
            [pa.array([1]), pa.array(["x"])],
            names=["id", "long_column_name"],
        )

        with patch(
            "bani.connectors.oracle.connector.OracleDataWriter"
        ) as MockWriter:
            mock_writer = MockWriter.return_value
            mock_writer.write_batch.return_value = 1
            mock_writer.batch_errors = []

            connector.write_batch("long_table", "SA", batch)

            # Verify the writer received the shortened table name
            args = mock_writer.write_batch.call_args
            assert args[0][0] == "short_tbl"
            # Verify column was renamed in the batch
            written_batch = args[0][2]
            assert "short_col" in written_batch.schema.names
            assert "long_column_name" not in written_batch.schema.names
