"""Unit tests for SQLite type mapper."""

from __future__ import annotations

import pyarrow as pa

from bani.connectors.sqlite.type_mapper import SQLiteTypeMapper


class TestMapSQLiteTypeName:
    """Tests for SQLiteTypeMapper.map_sqlite_type_name()."""

    def setup_method(self) -> None:
        self.mapper = SQLiteTypeMapper()

    def test_integer_types(self) -> None:
        assert self.mapper.map_sqlite_type_name("INTEGER") == pa.int64()
        assert self.mapper.map_sqlite_type_name("INT") == pa.int64()
        assert self.mapper.map_sqlite_type_name("TINYINT") == pa.int8()
        assert self.mapper.map_sqlite_type_name("SMALLINT") == pa.int16()
        assert self.mapper.map_sqlite_type_name("MEDIUMINT") == pa.int32()
        assert self.mapper.map_sqlite_type_name("BIGINT") == pa.int64()
        assert self.mapper.map_sqlite_type_name("INT2") == pa.int16()
        assert self.mapper.map_sqlite_type_name("INT8") == pa.int64()

    def test_real_types(self) -> None:
        assert self.mapper.map_sqlite_type_name("REAL") == pa.float64()
        assert self.mapper.map_sqlite_type_name("DOUBLE") == pa.float64()
        assert self.mapper.map_sqlite_type_name("DOUBLE PRECISION") == pa.float64()
        assert self.mapper.map_sqlite_type_name("FLOAT") == pa.float64()

    def test_text_types(self) -> None:
        assert self.mapper.map_sqlite_type_name("TEXT") == pa.string()
        assert self.mapper.map_sqlite_type_name("CLOB") == pa.string()
        assert self.mapper.map_sqlite_type_name("CHARACTER") == pa.string()
        assert self.mapper.map_sqlite_type_name("VARCHAR") == pa.string()
        assert self.mapper.map_sqlite_type_name("NCHAR") == pa.string()
        assert self.mapper.map_sqlite_type_name("NVARCHAR") == pa.string()

    def test_blob_type(self) -> None:
        assert self.mapper.map_sqlite_type_name("BLOB") == pa.binary()

    def test_boolean_type(self) -> None:
        assert self.mapper.map_sqlite_type_name("BOOLEAN") == pa.bool_()
        assert self.mapper.map_sqlite_type_name("BOOL") == pa.bool_()

    def test_date_time_types(self) -> None:
        assert self.mapper.map_sqlite_type_name("DATE") == pa.date32()
        assert self.mapper.map_sqlite_type_name("DATETIME") == pa.timestamp("us")
        assert self.mapper.map_sqlite_type_name("TIMESTAMP") == pa.timestamp("us")

    def test_numeric_types(self) -> None:
        assert self.mapper.map_sqlite_type_name("NUMERIC") == pa.decimal128(38, 10)
        assert self.mapper.map_sqlite_type_name("DECIMAL") == pa.decimal128(38, 10)

    def test_type_with_parameters(self) -> None:
        assert self.mapper.map_sqlite_type_name("VARCHAR(255)") == pa.string()
        result = self.mapper.map_sqlite_type_name("DECIMAL(10,2)")
        assert result == pa.decimal128(38, 10)
        assert self.mapper.map_sqlite_type_name("NCHAR(100)") == pa.string()

    def test_empty_type(self) -> None:
        assert self.mapper.map_sqlite_type_name("") == pa.binary()

    def test_affinity_rules_int(self) -> None:
        """Types containing 'INT' should have INTEGER affinity."""
        assert self.mapper.map_sqlite_type_name("UNSIGNED BIG INT") == pa.int64()

    def test_affinity_rules_text(self) -> None:
        """Types containing 'CHAR', 'CLOB', or 'TEXT' have TEXT affinity."""
        assert self.mapper.map_sqlite_type_name("VARYING CHARACTER(255)") == pa.string()

    def test_affinity_rules_real(self) -> None:
        """Types containing 'REAL', 'FLOA', or 'DOUB' have REAL affinity."""
        assert self.mapper.map_sqlite_type_name("DOUBLE PRECISION") == pa.float64()

    def test_case_insensitive(self) -> None:
        assert self.mapper.map_sqlite_type_name("integer") == pa.int64()
        assert self.mapper.map_sqlite_type_name("Text") == pa.string()
        assert self.mapper.map_sqlite_type_name("real") == pa.float64()


class TestFromArrowType:
    """Tests for SQLiteTypeMapper.from_arrow_type()."""

    def test_boolean(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("bool") == "BOOLEAN"

    def test_integer_types(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("int8") == "INTEGER"
        assert SQLiteTypeMapper.from_arrow_type("int16") == "INTEGER"
        assert SQLiteTypeMapper.from_arrow_type("int32") == "INTEGER"
        assert SQLiteTypeMapper.from_arrow_type("int64") == "INTEGER"
        assert SQLiteTypeMapper.from_arrow_type("uint8") == "INTEGER"
        assert SQLiteTypeMapper.from_arrow_type("uint64") == "INTEGER"

    def test_float_types(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("float") == "REAL"
        assert SQLiteTypeMapper.from_arrow_type("double") == "REAL"
        assert SQLiteTypeMapper.from_arrow_type("float32") == "REAL"
        assert SQLiteTypeMapper.from_arrow_type("float64") == "REAL"

    def test_string_types(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("string") == "TEXT"
        assert SQLiteTypeMapper.from_arrow_type("utf8") == "TEXT"
        assert SQLiteTypeMapper.from_arrow_type("large_string") == "TEXT"

    def test_binary_types(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("binary") == "BLOB"
        assert SQLiteTypeMapper.from_arrow_type("large_binary") == "BLOB"

    def test_date_types(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("date32[day]") == "TEXT"
        assert SQLiteTypeMapper.from_arrow_type("date64[ms]") == "TEXT"

    def test_timestamp_types(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("timestamp[us]") == "TEXT"
        assert SQLiteTypeMapper.from_arrow_type("timestamp[us, tz=UTC]") == "TEXT"

    def test_time_types(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("time64[us]") == "TEXT"
        assert SQLiteTypeMapper.from_arrow_type("time32[ms]") == "TEXT"

    def test_decimal_type(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("decimal128(38, 10)") == "NUMERIC"

    def test_null_type(self) -> None:
        assert SQLiteTypeMapper.from_arrow_type("null") == "TEXT"


class TestCoerceValue:
    """Tests for SQLiteTypeMapper.coerce_value()."""

    def setup_method(self) -> None:
        self.mapper = SQLiteTypeMapper()

    def test_none_passthrough(self) -> None:
        assert self.mapper.coerce_value(None, pa.int64()) is None

    def test_boolean_from_int(self) -> None:
        assert self.mapper.coerce_value(1, pa.bool_()) is True
        assert self.mapper.coerce_value(0, pa.bool_()) is False

    def test_date_from_string(self) -> None:
        import datetime

        result = self.mapper.coerce_value("2024-01-15", pa.date32())
        assert result == datetime.date(2024, 1, 15)

    def test_invalid_date_returns_none(self) -> None:
        result = self.mapper.coerce_value("not-a-date", pa.date32())
        assert result is None

    def test_timestamp_from_string(self) -> None:
        import datetime

        result = self.mapper.coerce_value("2024-01-15T10:30:00", pa.timestamp("us"))
        assert result == datetime.datetime(2024, 1, 15, 10, 30, 0)

    def test_decimal_coercion(self) -> None:
        import decimal

        result = self.mapper.coerce_value(3.14, pa.decimal128(10, 2))
        assert isinstance(result, decimal.Decimal)

    def test_regular_value_passthrough(self) -> None:
        assert self.mapper.coerce_value(42, pa.int64()) == 42
        assert self.mapper.coerce_value("hello", pa.string()) == "hello"
