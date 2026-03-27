"""Unit tests for Oracle schema reader (mocked DB interactions)."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

from bani.connectors.oracle.schema_reader import OracleSchemaReader
from bani.domain.schema import (
    ColumnDefinition,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class TestOracleSchemaReader:
    """Tests for schema reader with mocked connection."""

    def _make_mock_connection(self) -> MagicMock:
        """Create a mock oracledb connection."""
        return MagicMock()

    def _make_reader(
        self, mock_conn: MagicMock | None = None, owner: str = "TESTUSER"
    ) -> OracleSchemaReader:
        """Create an OracleSchemaReader with a mock connection."""
        if mock_conn is None:
            mock_conn = self._make_mock_connection()
        return OracleSchemaReader(mock_conn, owner)

    # ------------------------------------------------------------------
    # __init__
    # ------------------------------------------------------------------

    def test_init_stores_connection(self) -> None:
        """Reader should store the connection."""
        mock_conn = self._make_mock_connection()
        reader = OracleSchemaReader(mock_conn, "testuser")
        assert reader.connection is mock_conn

    def test_init_uppercases_owner(self) -> None:
        """Owner should be uppercased."""
        reader = self._make_reader(owner="my_schema")
        assert reader.owner == "MY_SCHEMA"

    # ------------------------------------------------------------------
    # _normalize_delete_rule
    # ------------------------------------------------------------------

    def test_normalize_delete_rule_cascade(self) -> None:
        """Should normalize CASCADE."""
        assert OracleSchemaReader._normalize_delete_rule("CASCADE") == "CASCADE"

    def test_normalize_delete_rule_set_null(self) -> None:
        """Should normalize SET NULL."""
        assert OracleSchemaReader._normalize_delete_rule("SET NULL") == "SET NULL"

    def test_normalize_delete_rule_restrict(self) -> None:
        """Should normalize RESTRICT."""
        assert OracleSchemaReader._normalize_delete_rule("RESTRICT") == "RESTRICT"

    def test_normalize_delete_rule_none(self) -> None:
        """Should default to NO ACTION for None."""
        assert OracleSchemaReader._normalize_delete_rule(None) == "NO ACTION"

    def test_normalize_delete_rule_unknown(self) -> None:
        """Should default to NO ACTION for unknown values."""
        assert OracleSchemaReader._normalize_delete_rule("SOMETHING") == "NO ACTION"

    def test_normalize_delete_rule_case_insensitive(self) -> None:
        """Should handle lowercase input."""
        assert OracleSchemaReader._normalize_delete_rule("cascade") == "CASCADE"

    # ------------------------------------------------------------------
    # read_schema
    # ------------------------------------------------------------------

    def test_read_schema_returns_database_schema(self) -> None:
        """Should return DatabaseSchema with oracle dialect."""
        reader = self._make_reader()

        with patch.object(
            reader,
            "_read_tables",
            return_value=[
                TableDefinition(
                    schema_name="TESTUSER",
                    table_name="USERS",
                    columns=(
                        ColumnDefinition(
                            name="ID",
                            data_type="NUMBER(10,0)",
                            ordinal_position=0,
                        ),
                    ),
                )
            ],
        ):
            result = reader.read_schema()

            assert result.source_dialect == "oracle"
            assert len(result.tables) == 1
            assert result.tables[0].table_name == "USERS"

    # ------------------------------------------------------------------
    # _read_tables (assembly from bulk queries)
    # ------------------------------------------------------------------

    def test_read_tables_assembles_from_bulk_queries(self) -> None:
        """Should issue 6 bulk queries and assemble TableDefinitions."""
        reader = self._make_reader()

        table_name = "USERS"
        col = ColumnDefinition(
            name="ID", data_type="NUMBER(10,0)", ordinal_position=0
        )
        idx = IndexDefinition(
            name="IDX_USERS_NAME",
            columns=("NAME",),
            is_unique=False,
            is_clustered=False,
        )
        fk = ForeignKeyDefinition(
            name="FK_USERS_DEPT",
            source_table="TESTUSER.USERS",
            source_columns=("DEPT_ID",),
            referenced_table="TESTUSER.DEPARTMENTS",
            referenced_columns=("ID",),
            on_delete="CASCADE",
            on_update="NO ACTION",
        )

        with (
            patch.object(
                reader, "_fetch_table_list", return_value=[table_name]
            ),
            patch.object(
                reader, "_fetch_all_columns", return_value={table_name: [col]}
            ),
            patch.object(
                reader,
                "_fetch_all_primary_keys",
                return_value={table_name: ["ID"]},
            ),
            patch.object(
                reader, "_fetch_all_indexes", return_value={table_name: [idx]}
            ),
            patch.object(
                reader,
                "_fetch_all_foreign_keys",
                return_value={table_name: [fk]},
            ),
            patch.object(
                reader,
                "_fetch_all_row_counts",
                return_value={table_name: 42},
            ),
        ):
            tables = reader._read_tables()

            assert len(tables) == 1
            t = tables[0]
            assert t.schema_name == "TESTUSER"
            assert t.table_name == "USERS"
            assert t.columns == (col,)
            assert t.primary_key == ("ID",)
            assert t.indexes == (idx,)
            assert t.foreign_keys == (fk,)
            assert t.check_constraints == ()
            assert t.row_count_estimate == 42

    def test_read_tables_empty_database(self) -> None:
        """Should return empty list when no tables exist."""
        reader = self._make_reader()

        with patch.object(reader, "_fetch_table_list", return_value=[]):
            tables = reader._read_tables()
            assert tables == []

    def test_read_tables_missing_keys_default_to_empty(self) -> None:
        """Tables not found in bulk maps should get empty defaults."""
        reader = self._make_reader()

        with (
            patch.object(
                reader, "_fetch_table_list", return_value=["ORPHAN_TABLE"]
            ),
            patch.object(reader, "_fetch_all_columns", return_value={}),
            patch.object(reader, "_fetch_all_primary_keys", return_value={}),
            patch.object(reader, "_fetch_all_indexes", return_value={}),
            patch.object(reader, "_fetch_all_foreign_keys", return_value={}),
            patch.object(reader, "_fetch_all_row_counts", return_value={}),
        ):
            tables = reader._read_tables()

            assert len(tables) == 1
            t = tables[0]
            assert t.table_name == "ORPHAN_TABLE"
            assert t.columns == ()
            assert t.primary_key == ()
            assert t.indexes == ()
            assert t.foreign_keys == ()
            assert t.row_count_estimate is None

    # ------------------------------------------------------------------
    # _fetch_table_list
    # ------------------------------------------------------------------

    def test_fetch_table_list_queries_user_tables(self) -> None:
        """Should query user_tables and return table names."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("USERS",), ("ORDERS",)]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_table_list()

        assert result == ["USERS", "ORDERS"]
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_fetch_table_list_empty(self) -> None:
        """Should return empty list when no tables exist."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        reader = self._make_reader(mock_conn)
        result = reader._fetch_table_list()

        assert result == []

    # ------------------------------------------------------------------
    # _fetch_all_columns
    # ------------------------------------------------------------------

    def test_fetch_all_columns_builds_types(self) -> None:
        """Should build full type strings from data_type + precision/scale."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            # (table, col, data_type, data_len, precision, scale, nullable, col_id, default)
            ("USERS", "ID", "NUMBER", None, 10, 0, "N", 1, None),
            ("USERS", "NAME", "VARCHAR2", 100, None, None, "N", 2, None),
            ("USERS", "BALANCE", "NUMBER", None, 12, 2, "Y", 3, None),
            ("USERS", "STATUS", "CHAR", 1, None, None, "Y", 4, "'A'"),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_columns()

        assert "USERS" in result
        cols = result["USERS"]
        assert len(cols) == 4
        assert cols[0].data_type == "NUMBER(10,0)"
        assert cols[0].nullable is False
        assert cols[1].data_type == "VARCHAR2(100)"
        assert cols[2].data_type == "NUMBER(12,2)"
        assert cols[3].data_type == "CHAR(1)"
        assert cols[3].default_value == "'A'"

    def test_fetch_all_columns_detects_auto_increment(self) -> None:
        """Should detect ISEQ$$ and .nextval as auto-increment."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("T", "ID", "NUMBER", None, 10, 0, "N", 1, '"TESTUSER"."ISEQ$$_123".nextval'),
            ("T", "SEQ", "NUMBER", None, 10, 0, "N", 2, "my_seq.nextval"),
            ("T", "NAME", "VARCHAR2", 50, None, None, "Y", 3, None),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_columns()

        cols = result["T"]
        assert cols[0].is_auto_increment is True
        assert cols[0].default_value is None
        assert cols[1].is_auto_increment is True
        assert cols[1].default_value is None
        assert cols[2].is_auto_increment is False

    def test_fetch_all_columns_groups_by_table(self) -> None:
        """Should group columns by table name."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("ORDERS", "ID", "NUMBER", None, 10, 0, "N", 1, None),
            ("USERS", "ID", "NUMBER", None, 10, 0, "N", 1, None),
            ("USERS", "NAME", "VARCHAR2", 100, None, None, "N", 2, None),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_columns()

        assert len(result) == 2
        assert len(result["ORDERS"]) == 1
        assert len(result["USERS"]) == 2

    def test_fetch_all_columns_number_without_precision(self) -> None:
        """NUMBER without precision should remain as 'NUMBER'."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("T", "VAL", "NUMBER", None, None, None, "Y", 1, None),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_columns()

        assert result["T"][0].data_type == "NUMBER"

    def test_fetch_all_columns_number_precision_no_scale(self) -> None:
        """NUMBER(p) without scale should omit scale."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("T", "VAL", "NUMBER", None, 5, None, "Y", 1, None),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_columns()

        assert result["T"][0].data_type == "NUMBER(5)"

    # ------------------------------------------------------------------
    # _fetch_all_primary_keys
    # ------------------------------------------------------------------

    def test_fetch_all_primary_keys(self) -> None:
        """Should fetch PK columns grouped by table."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("ORDERS", "ORDER_ID"),
            ("USERS", "ID"),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_primary_keys()

        assert result["USERS"] == ["ID"]
        assert result["ORDERS"] == ["ORDER_ID"]

    def test_fetch_all_primary_keys_composite(self) -> None:
        """Should handle composite primary keys."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("ORDER_ITEMS", "ORDER_ID"),
            ("ORDER_ITEMS", "ITEM_ID"),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_primary_keys()

        assert result["ORDER_ITEMS"] == ["ORDER_ID", "ITEM_ID"]

    # ------------------------------------------------------------------
    # _fetch_all_indexes
    # ------------------------------------------------------------------

    def test_fetch_all_indexes_groups_columns(self) -> None:
        """Should group index columns and detect uniqueness."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            # (table, index_name, uniqueness, col_name, col_pos)
            ("USERS", "IDX_USERS_EMAIL", "UNIQUE", "EMAIL", 1),
            ("USERS", "IDX_USERS_NAME_AGE", "NONUNIQUE", "NAME", 1),
            ("USERS", "IDX_USERS_NAME_AGE", "NONUNIQUE", "AGE", 2),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_indexes()

        assert "USERS" in result
        indexes = result["USERS"]
        assert len(indexes) == 2

        email_idx = next(i for i in indexes if i.name == "IDX_USERS_EMAIL")
        assert email_idx.columns == ("EMAIL",)
        assert email_idx.is_unique is True

        name_age_idx = next(i for i in indexes if i.name == "IDX_USERS_NAME_AGE")
        assert name_age_idx.columns == ("NAME", "AGE")
        assert name_age_idx.is_unique is False

    def test_fetch_all_indexes_multiple_tables(self) -> None:
        """Should handle indexes across multiple tables."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("ORDERS", "IDX_ORDERS_DATE", "NONUNIQUE", "ORDER_DATE", 1),
            ("USERS", "IDX_USERS_EMAIL", "UNIQUE", "EMAIL", 1),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_indexes()

        assert len(result) == 2
        assert len(result["ORDERS"]) == 1
        assert len(result["USERS"]) == 1

    # ------------------------------------------------------------------
    # _fetch_all_foreign_keys
    # ------------------------------------------------------------------

    def test_fetch_all_foreign_keys_single_column(self) -> None:
        """Should build FK definitions from the self-join query."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            # (src_table, fk_name, src_owner, delete_rule,
            #  src_col, src_pos, ref_owner, ref_table, ref_col)
            ("ORDERS", "FK_ORDERS_USER", "TESTUSER", "CASCADE",
             "USER_ID", 1, "TESTUSER", "USERS", "ID"),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_foreign_keys()

        assert "ORDERS" in result
        fks = result["ORDERS"]
        assert len(fks) == 1
        fk = fks[0]
        assert fk.name == "FK_ORDERS_USER"
        assert fk.source_table == "TESTUSER.ORDERS"
        assert fk.source_columns == ("USER_ID",)
        assert fk.referenced_table == "TESTUSER.USERS"
        assert fk.referenced_columns == ("ID",)
        assert fk.on_delete == "CASCADE"
        assert fk.on_update == "NO ACTION"

    def test_fetch_all_foreign_keys_composite(self) -> None:
        """Should handle composite foreign keys."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("SHIPMENTS", "FK_SHIP_ITEM", "TESTUSER", None,
             "ORDER_ID", 1, "TESTUSER", "ORDER_ITEMS", "ORDER_ID"),
            ("SHIPMENTS", "FK_SHIP_ITEM", "TESTUSER", None,
             "ITEM_ID", 2, "TESTUSER", "ORDER_ITEMS", "ITEM_ID"),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_foreign_keys()

        fks = result["SHIPMENTS"]
        assert len(fks) == 1
        fk = fks[0]
        assert fk.source_columns == ("ORDER_ID", "ITEM_ID")
        assert fk.referenced_columns == ("ORDER_ID", "ITEM_ID")
        assert fk.on_delete == "NO ACTION"

    def test_fetch_all_foreign_keys_multiple_tables(self) -> None:
        """Should group FKs by source table."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("ORDERS", "FK_ORDERS_USER", "TESTUSER", "CASCADE",
             "USER_ID", 1, "TESTUSER", "USERS", "ID"),
            ("SHIPMENTS", "FK_SHIP_ORDER", "TESTUSER", "SET NULL",
             "ORDER_ID", 1, "TESTUSER", "ORDERS", "ID"),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_foreign_keys()

        assert len(result) == 2
        assert result["ORDERS"][0].on_delete == "CASCADE"
        assert result["SHIPMENTS"][0].on_delete == "SET NULL"

    # ------------------------------------------------------------------
    # _fetch_all_row_counts
    # ------------------------------------------------------------------

    def test_fetch_all_row_counts(self) -> None:
        """Should return row counts keyed by table name."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("ORDERS", 500),
            ("USERS", 100),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_row_counts()

        assert result["USERS"] == 100
        assert result["ORDERS"] == 500

    def test_fetch_all_row_counts_none(self) -> None:
        """Should return None for tables without stats."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("USERS", None),
        ]

        reader = self._make_reader(mock_conn)
        result = reader._fetch_all_row_counts()

        assert result["USERS"] is None

    # ------------------------------------------------------------------
    # Column/index definition creation (basic sanity)
    # ------------------------------------------------------------------

    def test_column_definition_creation(self) -> None:
        """Should create column definitions with proper attributes."""
        col = ColumnDefinition(
            name="ID",
            data_type="NUMBER(10,0)",
            nullable=False,
            is_auto_increment=True,
            ordinal_position=0,
        )

        assert col.name == "ID"
        assert col.data_type == "NUMBER(10,0)"
        assert col.nullable is False
        assert col.is_auto_increment is True
        assert col.ordinal_position == 0

    def test_index_definition_creation(self) -> None:
        """Should create index definitions with proper attributes."""
        index = IndexDefinition(
            name="IDX_NAME",
            columns=("COL1", "COL2"),
            is_unique=True,
            is_clustered=False,
            filter_expression=None,
        )

        assert index.name == "IDX_NAME"
        assert index.columns == ("COL1", "COL2")
        assert index.is_unique is True
        assert index.is_clustered is False

    def test_table_definition_creation(self) -> None:
        """Should create table definitions with all metadata."""
        table = TableDefinition(
            schema_name="TESTUSER",
            table_name="USERS",
            columns=(
                ColumnDefinition(
                    name="ID", data_type="NUMBER(10,0)", ordinal_position=0
                ),
                ColumnDefinition(
                    name="NAME", data_type="VARCHAR2(100)", ordinal_position=1
                ),
            ),
            primary_key=("ID",),
            indexes=(),
            foreign_keys=(),
            check_constraints=(),
            row_count_estimate=100,
        )

        assert table.schema_name == "TESTUSER"
        assert table.table_name == "USERS"
        assert len(table.columns) == 2
        assert table.primary_key == ("ID",)
        assert table.fully_qualified_name == "TESTUSER.USERS"

    # ------------------------------------------------------------------
    # Cursor management
    # ------------------------------------------------------------------

    def test_cursor_closed_on_success(self) -> None:
        """Cursor should be closed after successful query."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("USERS",)]

        reader = self._make_reader(mock_conn)
        reader._fetch_table_list()

        mock_cursor.close.assert_called_once()

    def test_cursor_closed_on_error(self) -> None:
        """Cursor should be closed even when query fails."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = RuntimeError("DB error")

        reader = self._make_reader(mock_conn)
        try:
            reader._fetch_table_list()
        except RuntimeError:
            pass

        mock_cursor.close.assert_called_once()

    # ------------------------------------------------------------------
    # Total query count verification
    # ------------------------------------------------------------------

    def test_total_query_count_is_six(self) -> None:
        """read_schema should issue exactly 6 queries regardless of table count."""
        mock_conn = self._make_mock_connection()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Simulate 3 tables to verify no per-table queries
        mock_cursor.fetchall.side_effect = [
            # 1. _fetch_table_list
            [("T1",), ("T2",), ("T3",)],
            # 2. _fetch_all_columns
            [
                ("T1", "ID", "NUMBER", None, 10, 0, "N", 1, None),
                ("T2", "ID", "NUMBER", None, 10, 0, "N", 1, None),
                ("T3", "ID", "NUMBER", None, 10, 0, "N", 1, None),
            ],
            # 3. _fetch_all_primary_keys
            [("T1", "ID"), ("T2", "ID"), ("T3", "ID")],
            # 4. _fetch_all_indexes
            [],
            # 5. _fetch_all_foreign_keys
            [],
            # 6. _fetch_all_row_counts
            [("T1", 10), ("T2", 20), ("T3", 30)],
        ]

        reader = OracleSchemaReader(mock_conn, "TESTUSER")
        schema = reader.read_schema()

        # Exactly 6 execute calls, not 6 + N*k
        assert mock_cursor.execute.call_count == 6
        assert len(schema.tables) == 3
        assert schema.source_dialect == "oracle"
