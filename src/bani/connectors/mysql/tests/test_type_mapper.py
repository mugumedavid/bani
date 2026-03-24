"""Unit tests for MySQL type mapper."""

from __future__ import annotations

import datetime
from decimal import Decimal

import pyarrow as pa

from bani.connectors.mysql.type_mapper import (
    UNSIGNED_FLAG,
    MySQLFieldType,
    MySQLTypeMapper,
)


class TestMySQLTypeMapperByCode:
    """Tests for type code mapping."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mapper = MySQLTypeMapper()

    def test_map_tiny_to_int8(self) -> None:
        """TINYINT should map to int8."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.TINY)
        assert result == pa.int8()

    def test_map_short_to_int16(self) -> None:
        """SMALLINT should map to int16."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.SHORT)
        assert result == pa.int16()

    def test_map_long_to_int32(self) -> None:
        """INT should map to int32."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.LONG)
        assert result == pa.int32()

    def test_map_longlong_to_int64(self) -> None:
        """BIGINT should map to int64."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.LONGLONG)
        assert result == pa.int64()

    def test_map_float_to_float32(self) -> None:
        """FLOAT should map to float32."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.FLOAT)
        assert result == pa.float32()

    def test_map_double_to_float64(self) -> None:
        """DOUBLE should map to float64."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.DOUBLE)
        assert result == pa.float64()

    def test_map_varchar_to_string(self) -> None:
        """VARCHAR should map to string."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.VARCHAR)
        assert result == pa.string()

    def test_map_blob_to_binary(self) -> None:
        """BLOB should map to binary."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.BLOB)
        assert result == pa.binary()

    def test_map_date_to_date32(self) -> None:
        """DATE should map to date32."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.DATE)
        assert result == pa.date32()

    def test_map_datetime_to_timestamp(self) -> None:
        """DATETIME should map to timestamp."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.DATETIME)
        assert result == pa.timestamp("us")

    def test_map_timestamp_to_timestamp_utc(self) -> None:
        """TIMESTAMP should map to timestamp with UTC."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.TIMESTAMP)
        assert result == pa.timestamp("us", tz="UTC")

    def test_map_json_to_string(self) -> None:
        """JSON should map to string."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.JSON)
        assert result == pa.string()

    def test_map_bit_to_bool(self) -> None:
        """BIT should map to bool."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.BIT)
        assert result == pa.bool_()

    def test_map_year_to_int16(self) -> None:
        """YEAR should map to int16."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.YEAR)
        assert result == pa.int16()

    def test_map_enum_to_string(self) -> None:
        """ENUM should map to string."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.ENUM)
        assert result == pa.string()

    def test_map_set_to_string(self) -> None:
        """SET should map to string."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.SET)
        assert result == pa.string()

    def test_map_unknown_to_string(self) -> None:
        """Unknown type codes should default to string."""
        result = self.mapper.map_mysql_type_code(9999)
        assert result == pa.string()


class TestUnsignedTypeMapping:
    """Tests for unsigned integer type mapping."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mapper = MySQLTypeMapper()

    def test_unsigned_tiny_promotes_to_int16(self) -> None:
        """UNSIGNED TINYINT should promote to int16."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.TINY, UNSIGNED_FLAG)
        assert result == pa.int16()

    def test_unsigned_short_promotes_to_int32(self) -> None:
        """UNSIGNED SMALLINT should promote to int32."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.SHORT, UNSIGNED_FLAG)
        assert result == pa.int32()

    def test_unsigned_long_promotes_to_int64(self) -> None:
        """UNSIGNED INT should promote to int64."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.LONG, UNSIGNED_FLAG)
        assert result == pa.int64()

    def test_unsigned_longlong_promotes_to_decimal(self) -> None:
        """UNSIGNED BIGINT should promote to decimal."""
        result = self.mapper.map_mysql_type_code(MySQLFieldType.LONGLONG, UNSIGNED_FLAG)
        assert pa.types.is_decimal(result)


class TestTypeNameMapping:
    """Tests for type name string mapping."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mapper = MySQLTypeMapper()

    def test_map_int_name(self) -> None:
        """INT type name should map to int32."""
        result = self.mapper.map_mysql_type_name("INT")
        assert result == pa.int32()

    def test_map_varchar_name_with_params(self) -> None:
        """VARCHAR(255) should map to string."""
        result = self.mapper.map_mysql_type_name("VARCHAR(255)")
        assert result == pa.string()

    def test_map_unsigned_int_name(self) -> None:
        """INT UNSIGNED should promote to int64."""
        result = self.mapper.map_mysql_type_name("INT UNSIGNED")
        assert result == pa.int64()

    def test_map_unsigned_bigint_name(self) -> None:
        """BIGINT UNSIGNED should promote to decimal."""
        result = self.mapper.map_mysql_type_name("BIGINT UNSIGNED")
        assert pa.types.is_decimal(result)

    def test_map_text_name(self) -> None:
        """TEXT type should map to string."""
        result = self.mapper.map_mysql_type_name("TEXT")
        assert result == pa.string()

    def test_map_datetime_name(self) -> None:
        """DATETIME type should map to timestamp."""
        result = self.mapper.map_mysql_type_name("DATETIME")
        assert result == pa.timestamp("us")

    def test_map_json_name(self) -> None:
        """JSON type should map to string."""
        result = self.mapper.map_mysql_type_name("JSON")
        assert result == pa.string()

    def test_map_enum_name(self) -> None:
        """ENUM type should map to string."""
        result = self.mapper.map_mysql_type_name("ENUM")
        assert result == pa.string()

    def test_map_unknown_name_to_string(self) -> None:
        """Unknown type names should default to string."""
        result = self.mapper.map_mysql_type_name("CUSTOMTYPE")
        assert result == pa.string()


class TestValueCoercion:
    """Tests for value coercion to Arrow-compatible types."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mapper = MySQLTypeMapper()

    def test_coerce_none_returns_none(self) -> None:
        """None values should pass through unchanged."""
        result = self.mapper.coerce_value(None, pa.int32())
        assert result is None

    def test_coerce_timedelta_to_microseconds(self) -> None:
        """timedelta from TIME columns should convert to microseconds."""
        td = datetime.timedelta(hours=1, minutes=30, seconds=15)
        result = self.mapper.coerce_value(td, pa.time64("us"))
        expected = int(td.total_seconds() * 1_000_000)
        assert result == expected

    def test_coerce_bytes_bit_to_bool_true(self) -> None:
        """Bytes from BIT(1) columns should convert to True."""
        result = self.mapper.coerce_value(b"\x01", pa.bool_())
        assert result is True

    def test_coerce_bytes_bit_to_bool_false(self) -> None:
        """Bytes from BIT(1) columns should convert to False."""
        result = self.mapper.coerce_value(b"\x00", pa.bool_())
        assert result is False

    def test_coerce_int_to_decimal(self) -> None:
        """Integer values should convert to Decimal for decimal128 types."""
        result = self.mapper.coerce_value(42, pa.decimal128(20, 0))
        assert isinstance(result, Decimal)
        assert result == Decimal("42")

    def test_coerce_normal_value_passes_through(self) -> None:
        """Normal values should pass through unchanged."""
        result = self.mapper.coerce_value(42, pa.int32())
        assert result == 42

    def test_coerce_valid_date_passes_through(self) -> None:
        """Valid dates should pass through unchanged."""
        d = datetime.date(2024, 1, 15)
        result = self.mapper.coerce_value(d, pa.date32())
        assert result == d
