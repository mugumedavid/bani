"""End-to-end tests for type mapping layer integration.

Tests verify that the type mapping layer works correctly for:
1. ColumnDefinition with arrow_type_str field
2. MySQL -> Arrow -> PostgreSQL round-trip conversions
3. PostgreSQL -> Arrow -> MySQL round-trip conversions
4. Fallback behavior when arrow_type_str is None

These tests use str(pa_type) directly — exactly what the real
schema readers do — so they catch mismatches between PyArrow's
actual string output and the from_arrow_type() lookup tables.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from bani.connectors.mysql.type_mapper import MySQLTypeMapper
from bani.connectors.postgresql.type_mapper import PostgreSQLTypeMapper
from bani.domain.schema import ColumnDefinition, TableDefinition


class TestColumnDefinitionArrowTypeStr:
    """Test the arrow_type_str field on ColumnDefinition."""

    def test_arrow_type_str_defaults_to_none(self) -> None:
        """arrow_type_str should default to None when not provided."""
        col = ColumnDefinition(name="test_col", data_type="INT")
        assert col.arrow_type_str is None

    def test_arrow_type_str_can_be_set(self) -> None:
        """arrow_type_str can be set during construction."""
        col = ColumnDefinition(name="test_col", data_type="INT", arrow_type_str="int32")
        assert col.arrow_type_str == "int32"

    def test_arrow_type_str_with_complex_arrow_type(self) -> None:
        """arrow_type_str can hold complex Arrow type strings."""
        col = ColumnDefinition(
            name="timestamp_col",
            data_type="TIMESTAMP",
            arrow_type_str="timestamp[us, tz=UTC]",
        )
        assert col.arrow_type_str == "timestamp[us, tz=UTC]"

    def test_column_definition_is_frozen(self) -> None:
        """ColumnDefinition should be frozen (immutable)."""
        col = ColumnDefinition(name="test_col", data_type="INT", arrow_type_str="int32")
        with pytest.raises(AttributeError):
            col.arrow_type_str = "int64"  # type: ignore[misc]


# ------------------------------------------------------------------
# Helper: simulate what introspection does (str(arrow_type))
# ------------------------------------------------------------------


def _mysql_introspect(mysql_type: str) -> ColumnDefinition:
    """Simulate MySQL schema introspection for one column."""
    arrow = MySQLTypeMapper().map_mysql_type_name(mysql_type)
    return ColumnDefinition(
        name="col",
        data_type=mysql_type,
        arrow_type_str=str(arrow),  # exactly what the reader does
    )


def _pg_introspect(pg_type: str) -> ColumnDefinition:
    """Simulate PG schema introspection for one column."""
    arrow = PostgreSQLTypeMapper().map_pg_type_name(pg_type)
    return ColumnDefinition(
        name="col",
        data_type=pg_type,
        arrow_type_str=str(arrow),  # exactly what the reader does
    )


class TestMySQLToArrowToPGRoundTrip:
    """Test MySQL -> Arrow -> PostgreSQL conversion paths.

    Each test verifies the full chain: MySQL type name is mapped to
    an Arrow type via map_mysql_type_name(), Arrow's str() is stored,
    and from_arrow_type() on the PG mapper produces valid PG DDL.
    """

    def test_mysql_int11_to_pg_integer(self) -> None:
        col = _mysql_introspect("INT(11)")
        assert col.arrow_type_str == "int32"
        assert PostgreSQLTypeMapper.from_arrow_type("int32") == "integer"

    def test_mysql_bigint_to_pg_bigint(self) -> None:
        col = _mysql_introspect("BIGINT")
        assert col.arrow_type_str == "int64"
        assert PostgreSQLTypeMapper.from_arrow_type("int64") == "bigint"

    def test_mysql_tinyint1_to_pg_smallint(self) -> None:
        """tinyint(1) is often boolean, but Arrow sees int8."""
        col = _mysql_introspect("TINYINT(1)")
        assert col.arrow_type_str == "int8"
        assert PostgreSQLTypeMapper.from_arrow_type("int8") == "smallint"

    def test_mysql_varchar255_to_pg_text(self) -> None:
        col = _mysql_introspect("VARCHAR(255)")
        assert col.arrow_type_str == "string"
        assert PostgreSQLTypeMapper.from_arrow_type("string") == "text"

    def test_mysql_datetime_to_pg_timestamp(self) -> None:
        col = _mysql_introspect("DATETIME")
        assert col.arrow_type_str == "timestamp[us]"
        assert PostgreSQLTypeMapper.from_arrow_type("timestamp[us]") == "timestamp"

    def test_mysql_timestamp_to_pg_timestamptz(self) -> None:
        col = _mysql_introspect("TIMESTAMP")
        assert col.arrow_type_str == "timestamp[us, tz=UTC]"
        assert (
            PostgreSQLTypeMapper.from_arrow_type("timestamp[us, tz=UTC]")
            == "timestamp with time zone"
        )

    def test_mysql_double_to_pg_double_precision(self) -> None:
        col = _mysql_introspect("DOUBLE")
        # PyArrow str(pa.float64()) == "double"
        assert col.arrow_type_str == "double"
        assert PostgreSQLTypeMapper.from_arrow_type("double") == "double precision"

    def test_mysql_float_to_pg_real(self) -> None:
        col = _mysql_introspect("FLOAT")
        # PyArrow str(pa.float32()) == "float"
        assert col.arrow_type_str == "float"
        assert PostgreSQLTypeMapper.from_arrow_type("float") == "real"

    def test_mysql_blob_to_pg_bytea(self) -> None:
        col = _mysql_introspect("BLOB")
        assert col.arrow_type_str == "binary"
        assert PostgreSQLTypeMapper.from_arrow_type("binary") == "bytea"

    def test_mysql_json_to_pg_text(self) -> None:
        col = _mysql_introspect("JSON")
        assert col.arrow_type_str == "string"
        assert PostgreSQLTypeMapper.from_arrow_type("string") == "text"

    def test_mysql_date_to_pg_date(self) -> None:
        col = _mysql_introspect("DATE")
        # PyArrow str(pa.date32()) == "date32[day]"
        assert col.arrow_type_str == "date32[day]"
        assert PostgreSQLTypeMapper.from_arrow_type("date32[day]") == "date"

    def test_mysql_decimal_to_pg_numeric(self) -> None:
        col = _mysql_introspect("DECIMAL(10,2)")
        assert col.arrow_type_str == "decimal128(38, 10)"
        assert (
            PostgreSQLTypeMapper.from_arrow_type("decimal128(38, 10)")
            == "numeric(38, 10)"
        )

    def test_mysql_text_to_pg_text(self) -> None:
        col = _mysql_introspect("TEXT")
        assert col.arrow_type_str == "string"
        assert PostgreSQLTypeMapper.from_arrow_type("string") == "text"

    def test_mysql_enum_to_pg_text(self) -> None:
        col = _mysql_introspect("ENUM('a','b')")
        assert col.arrow_type_str == "string"
        assert PostgreSQLTypeMapper.from_arrow_type("string") == "text"

    def test_mysql_mediumtext_to_pg_text(self) -> None:
        col = _mysql_introspect("MEDIUMTEXT")
        assert col.arrow_type_str == "string"
        assert PostgreSQLTypeMapper.from_arrow_type("string") == "text"

    def test_mysql_time_to_pg_time(self) -> None:
        col = _mysql_introspect("TIME")
        # PyArrow str(pa.time64("us")) == "time64[us]"
        assert col.arrow_type_str == "time64[us]"
        assert PostgreSQLTypeMapper.from_arrow_type("time64[us]") == "time"


class TestPostgresToArrowToMySQLRoundTrip:
    """Test PostgreSQL -> Arrow -> MySQL conversion paths."""

    def test_pg_integer_to_mysql_int(self) -> None:
        col = _pg_introspect("integer")
        assert col.arrow_type_str == "int32"
        assert MySQLTypeMapper.from_arrow_type("int32") == "INT"

    def test_pg_text_to_mysql_text(self) -> None:
        col = _pg_introspect("text")
        assert col.arrow_type_str == "string"
        assert MySQLTypeMapper.from_arrow_type("string") == "TEXT"

    def test_pg_timestamp_to_mysql_datetime(self) -> None:
        col = _pg_introspect("timestamp")
        assert col.arrow_type_str == "timestamp[us]"
        assert MySQLTypeMapper.from_arrow_type("timestamp[us]") == "DATETIME"

    def test_pg_timestamptz_to_mysql_timestamp(self) -> None:
        col = _pg_introspect("timestamp with time zone")
        assert col.arrow_type_str == "timestamp[us, tz=UTC]"
        assert MySQLTypeMapper.from_arrow_type("timestamp[us, tz=UTC]") == "TIMESTAMP"

    def test_pg_boolean_to_mysql_tinyint1(self) -> None:
        col = _pg_introspect("boolean")
        assert col.arrow_type_str == "bool"
        assert MySQLTypeMapper.from_arrow_type("bool") == "TINYINT(1)"

    def test_pg_bytea_to_mysql_blob(self) -> None:
        col = _pg_introspect("bytea")
        assert col.arrow_type_str == "binary"
        assert MySQLTypeMapper.from_arrow_type("binary") == "BLOB"

    def test_pg_double_precision_to_mysql_double(self) -> None:
        col = _pg_introspect("double precision")
        # PyArrow str(pa.float64()) == "double"
        assert col.arrow_type_str == "double"
        assert MySQLTypeMapper.from_arrow_type("double") == "DOUBLE"

    def test_pg_real_to_mysql_float(self) -> None:
        col = _pg_introspect("real")
        # PyArrow str(pa.float32()) == "float"
        assert col.arrow_type_str == "float"
        assert MySQLTypeMapper.from_arrow_type("float") == "FLOAT"

    def test_pg_bigint_to_mysql_bigint(self) -> None:
        col = _pg_introspect("bigint")
        assert col.arrow_type_str == "int64"
        assert MySQLTypeMapper.from_arrow_type("int64") == "BIGINT"

    def test_pg_smallint_to_mysql_smallint(self) -> None:
        col = _pg_introspect("smallint")
        assert col.arrow_type_str == "int16"
        assert MySQLTypeMapper.from_arrow_type("int16") == "SMALLINT"

    def test_pg_date_to_mysql_date(self) -> None:
        col = _pg_introspect("date")
        # PyArrow str(pa.date32()) == "date32[day]"
        assert col.arrow_type_str == "date32[day]"
        assert MySQLTypeMapper.from_arrow_type("date32[day]") == "DATE"

    def test_pg_time_to_mysql_time(self) -> None:
        col = _pg_introspect("time")
        # PyArrow str(pa.time64("us")) == "time64[us]"
        assert col.arrow_type_str == "time64[us]"
        assert MySQLTypeMapper.from_arrow_type("time64[us]") == "TIME"

    def test_pg_interval_to_mysql_time(self) -> None:
        col = _pg_introspect("interval")
        # PyArrow str(pa.duration("us")) == "duration[us]"
        assert col.arrow_type_str == "duration[us]"
        assert MySQLTypeMapper.from_arrow_type("duration[us]") == "TIME"


class TestFallbackWhenArrowTypeStrIsNone:
    """Test fallback behavior when arrow_type_str is None."""

    def test_column_with_none_arrow_type_str(self) -> None:
        col = ColumnDefinition(name="test_col", data_type="INT", arrow_type_str=None)
        assert col.arrow_type_str is None
        assert col.data_type == "INT"

    def test_hand_built_table_def_without_arrow_types(self) -> None:
        """Hand-built TableDefinition can omit arrow_type_str."""
        columns = (
            ColumnDefinition(name="id", data_type="INT", arrow_type_str=None),
            ColumnDefinition(
                name="name",
                data_type="VARCHAR(255)",
                arrow_type_str=None,
            ),
        )
        table = TableDefinition(
            schema_name="test_schema",
            table_name="test_table",
            columns=columns,
        )
        for col in table.columns:
            assert col.arrow_type_str is None

    def test_mixed_arrow_types_and_none(self) -> None:
        """Some columns can have arrow_type_str while others don't."""
        columns = (
            ColumnDefinition(name="id", data_type="INT", arrow_type_str="int32"),
            ColumnDefinition(
                name="unknown",
                data_type="CUSTOM_TYPE",
                arrow_type_str=None,
            ),
        )
        table = TableDefinition(schema_name="s", table_name="t", columns=columns)
        assert table.columns[0].arrow_type_str == "int32"
        assert table.columns[1].arrow_type_str is None

    def test_mappers_handle_none_gracefully(self) -> None:
        """With arrow_type_str, we convert via Arrow; without, raw data_type is used."""
        col_with = ColumnDefinition(name="c1", data_type="INT", arrow_type_str="int32")
        col_without = ColumnDefinition(name="c2", data_type="INT", arrow_type_str=None)
        assert col_with.arrow_type_str is not None
        assert (
            PostgreSQLTypeMapper.from_arrow_type(col_with.arrow_type_str) == "integer"
        )
        assert col_without.arrow_type_str is None
        assert col_without.data_type == "INT"


class TestFromArrowTypeWithRealPyArrowStrings:
    """Verify from_arrow_type handles the *actual* str(pa_type) output.

    This is the critical test class: PyArrow's str() doesn't always
    match the short alias.  E.g. float64 -> "double", date32 ->
    "date32[day]", float32 -> "float".
    """

    @pytest.mark.parametrize(
        ("pa_type", "expected_pg"),
        [
            (pa.int8(), "smallint"),
            (pa.int16(), "smallint"),
            (pa.int32(), "integer"),
            (pa.int64(), "bigint"),
            (pa.float32(), "real"),
            (pa.float64(), "double precision"),
            (pa.bool_(), "boolean"),
            (pa.string(), "text"),
            (pa.binary(), "bytea"),
            (pa.date32(), "date"),
            (pa.time64("us"), "time"),
            (pa.timestamp("us"), "timestamp"),
            (pa.timestamp("us", tz="UTC"), "timestamp with time zone"),
            (pa.duration("us"), "interval"),
            (pa.decimal128(38, 10), "numeric(38, 10)"),
            (pa.null(), "text"),
        ],
    )
    def test_pg_from_real_pyarrow_str(
        self, pa_type: pa.DataType, expected_pg: str
    ) -> None:
        """PG from_arrow_type handles str(pa_type) correctly."""
        assert PostgreSQLTypeMapper.from_arrow_type(str(pa_type)) == expected_pg

    @pytest.mark.parametrize(
        ("pa_type", "expected_mysql"),
        [
            (pa.int8(), "TINYINT"),
            (pa.int16(), "SMALLINT"),
            (pa.int32(), "INT"),
            (pa.int64(), "BIGINT"),
            (pa.float32(), "FLOAT"),
            (pa.float64(), "DOUBLE"),
            (pa.bool_(), "TINYINT(1)"),
            (pa.string(), "TEXT"),
            (pa.binary(), "BLOB"),
            (pa.date32(), "DATE"),
            (pa.time64("us"), "TIME"),
            (pa.timestamp("us"), "DATETIME"),
            (pa.timestamp("us", tz="UTC"), "TIMESTAMP"),
            (pa.duration("us"), "TIME"),
            (pa.decimal128(38, 10), "DECIMAL(38, 10)"),
            (pa.null(), "TEXT"),
        ],
    )
    def test_mysql_from_real_pyarrow_str(
        self, pa_type: pa.DataType, expected_mysql: str
    ) -> None:
        """MySQL from_arrow_type handles str(pa_type) correctly."""
        assert MySQLTypeMapper.from_arrow_type(str(pa_type)) == expected_mysql


class TestTableDefinitionWithArrowTypes:
    """Test TableDefinition usage with arrow_type_str."""

    def test_table_with_all_arrow_types_populated(self) -> None:
        """A fully introspected table has arrow_type_str on all cols."""
        columns = (
            ColumnDefinition(
                name="id",
                data_type="INT",
                arrow_type_str="int32",
                is_auto_increment=True,
            ),
            ColumnDefinition(
                name="username",
                data_type="VARCHAR(255)",
                arrow_type_str="string",
            ),
            ColumnDefinition(
                name="created_at",
                data_type="DATETIME",
                arrow_type_str="timestamp[us]",
            ),
        )
        table = TableDefinition(
            schema_name="public",
            table_name="users",
            columns=columns,
            primary_key=("id",),
        )
        assert table.fully_qualified_name == "public.users"
        for col in table.columns:
            assert col.arrow_type_str is not None

    def test_end_to_end_introspection_to_ddl(self) -> None:
        """Simulate full introspection -> DDL path."""
        mysql_mapper = MySQLTypeMapper()
        mysql_types = [
            ("id", "INT"),
            ("email", "VARCHAR(255)"),
            ("is_active", "TINYINT(1)"),
            ("created", "DATETIME"),
            ("price", "DECIMAL(10,2)"),
            ("dob", "DATE"),
        ]
        columns: list[ColumnDefinition] = []
        for col_name, mysql_type in mysql_types:
            arrow = mysql_mapper.map_mysql_type_name(mysql_type)
            columns.append(
                ColumnDefinition(
                    name=col_name,
                    data_type=mysql_type,
                    arrow_type_str=str(arrow),
                    ordinal_position=len(columns),
                )
            )

        table = TableDefinition(
            schema_name="mydb",
            table_name="users",
            columns=tuple(columns),
            primary_key=("id",),
        )

        # Every column should convert to valid PG DDL
        for col in table.columns:
            assert col.arrow_type_str is not None
            pg = PostgreSQLTypeMapper.from_arrow_type(col.arrow_type_str)
            assert isinstance(pg, str)
            assert len(pg) > 0
