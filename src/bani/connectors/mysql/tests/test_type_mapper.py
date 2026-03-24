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


class TestFromArrowType:
    """Tests for Arrow-to-MySQL DDL type mapping."""

    def test_bool_to_tinyint1(self) -> None:
        """bool → TINYINT(1)."""
        result = MySQLTypeMapper.from_arrow_type("bool")
        assert result == "TINYINT(1)"

    def test_int8_to_tinyint(self) -> None:
        """int8 → TINYINT."""
        result = MySQLTypeMapper.from_arrow_type("int8")
        assert result == "TINYINT"

    def test_int16_to_smallint(self) -> None:
        """int16 → SMALLINT."""
        result = MySQLTypeMapper.from_arrow_type("int16")
        assert result == "SMALLINT"

    def test_int32_to_int(self) -> None:
        """int32 → INT."""
        result = MySQLTypeMapper.from_arrow_type("int32")
        assert result == "INT"

    def test_int64_to_bigint(self) -> None:
        """int64 → BIGINT."""
        result = MySQLTypeMapper.from_arrow_type("int64")
        assert result == "BIGINT"

    def test_uint8_to_smallint_unsigned(self) -> None:
        """uint8 → SMALLINT UNSIGNED."""
        result = MySQLTypeMapper.from_arrow_type("uint8")
        assert result == "SMALLINT UNSIGNED"

    def test_uint16_to_int_unsigned(self) -> None:
        """uint16 → INT UNSIGNED."""
        result = MySQLTypeMapper.from_arrow_type("uint16")
        assert result == "INT UNSIGNED"

    def test_uint32_to_bigint_unsigned(self) -> None:
        """uint32 → BIGINT UNSIGNED."""
        result = MySQLTypeMapper.from_arrow_type("uint32")
        assert result == "BIGINT UNSIGNED"

    def test_uint64_to_decimal20_unsigned(self) -> None:
        """uint64 → DECIMAL(20,0) UNSIGNED."""
        result = MySQLTypeMapper.from_arrow_type("uint64")
        assert result == "DECIMAL(20,0) UNSIGNED"

    def test_float16_to_float(self) -> None:
        """float16 → FLOAT."""
        result = MySQLTypeMapper.from_arrow_type("float16")
        assert result == "FLOAT"

    def test_float32_to_float(self) -> None:
        """float32 → FLOAT."""
        result = MySQLTypeMapper.from_arrow_type("float32")
        assert result == "FLOAT"

    def test_float64_to_double(self) -> None:
        """float64 → DOUBLE."""
        result = MySQLTypeMapper.from_arrow_type("float64")
        assert result == "DOUBLE"

    def test_halffloat_to_float(self) -> None:
        """halffloat → FLOAT."""
        result = MySQLTypeMapper.from_arrow_type("halffloat")
        assert result == "FLOAT"

    def test_string_to_text(self) -> None:
        """string → TEXT."""
        result = MySQLTypeMapper.from_arrow_type("string")
        assert result == "TEXT"

    def test_utf8_to_text(self) -> None:
        """utf8 → TEXT."""
        result = MySQLTypeMapper.from_arrow_type("utf8")
        assert result == "TEXT"

    def test_large_string_to_longtext(self) -> None:
        """large_string → LONGTEXT."""
        result = MySQLTypeMapper.from_arrow_type("large_string")
        assert result == "LONGTEXT"

    def test_large_utf8_to_longtext(self) -> None:
        """large_utf8 → LONGTEXT."""
        result = MySQLTypeMapper.from_arrow_type("large_utf8")
        assert result == "LONGTEXT"

    def test_binary_to_blob(self) -> None:
        """binary → BLOB."""
        result = MySQLTypeMapper.from_arrow_type("binary")
        assert result == "BLOB"

    def test_large_binary_to_longblob(self) -> None:
        """large_binary → LONGBLOB."""
        result = MySQLTypeMapper.from_arrow_type("large_binary")
        assert result == "LONGBLOB"

    def test_date32_to_date(self) -> None:
        """date32 → DATE."""
        result = MySQLTypeMapper.from_arrow_type("date32")
        assert result == "DATE"

    def test_date64_to_date(self) -> None:
        """date64 → DATE."""
        result = MySQLTypeMapper.from_arrow_type("date64")
        assert result == "DATE"

    def test_null_to_text(self) -> None:
        """null → TEXT (fallback)."""
        result = MySQLTypeMapper.from_arrow_type("null")
        assert result == "TEXT"

    def test_timestamp_with_tz_to_timestamp(self) -> None:
        """timestamp[us, tz=UTC] → TIMESTAMP."""
        result = MySQLTypeMapper.from_arrow_type("timestamp[us, tz=UTC]")
        assert result == "TIMESTAMP"

    def test_timestamp_with_different_tz_to_timestamp(self) -> None:
        """timestamp[us, tz=US/Eastern] → TIMESTAMP."""
        result = MySQLTypeMapper.from_arrow_type("timestamp[us, tz=US/Eastern]")
        assert result == "TIMESTAMP"

    def test_timestamp_without_tz_to_datetime(self) -> None:
        """timestamp[us] (no tz) → DATETIME."""
        result = MySQLTypeMapper.from_arrow_type("timestamp[us]")
        assert result == "DATETIME"

    def test_timestamp_with_ns_no_tz_to_datetime(self) -> None:
        """timestamp[ns] (no tz) → DATETIME."""
        result = MySQLTypeMapper.from_arrow_type("timestamp[ns]")
        assert result == "DATETIME"

    def test_timestamp_with_ms_and_tz_to_timestamp(self) -> None:
        """timestamp[ms, tz=UTC] → TIMESTAMP."""
        result = MySQLTypeMapper.from_arrow_type("timestamp[ms, tz=UTC]")
        assert result == "TIMESTAMP"

    def test_time32_ms_to_time(self) -> None:
        """time32[ms] → TIME."""
        result = MySQLTypeMapper.from_arrow_type("time32[ms]")
        assert result == "TIME"

    def test_time32_s_to_time(self) -> None:
        """time32[s] → TIME."""
        result = MySQLTypeMapper.from_arrow_type("time32[s]")
        assert result == "TIME"

    def test_time64_us_to_time(self) -> None:
        """time64[us] → TIME."""
        result = MySQLTypeMapper.from_arrow_type("time64[us]")
        assert result == "TIME"

    def test_time64_ns_to_time(self) -> None:
        """time64[ns] → TIME."""
        result = MySQLTypeMapper.from_arrow_type("time64[ns]")
        assert result == "TIME"

    def test_duration_s_to_time(self) -> None:
        """duration[s] → TIME."""
        result = MySQLTypeMapper.from_arrow_type("duration[s]")
        assert result == "TIME"

    def test_duration_ms_to_time(self) -> None:
        """duration[ms] → TIME."""
        result = MySQLTypeMapper.from_arrow_type("duration[ms]")
        assert result == "TIME"

    def test_duration_us_to_time(self) -> None:
        """duration[us] → TIME."""
        result = MySQLTypeMapper.from_arrow_type("duration[us]")
        assert result == "TIME"

    def test_duration_ns_to_time(self) -> None:
        """duration[ns] → TIME."""
        result = MySQLTypeMapper.from_arrow_type("duration[ns]")
        assert result == "TIME"

    def test_decimal128_with_params(self) -> None:
        """decimal128(38, 10) → DECIMAL(38, 10)."""
        result = MySQLTypeMapper.from_arrow_type("decimal128(38, 10)")
        assert result == "DECIMAL(38, 10)"

    def test_decimal128_with_different_params(self) -> None:
        """decimal128(20, 5) → DECIMAL(20, 5)."""
        result = MySQLTypeMapper.from_arrow_type("decimal128(20, 5)")
        assert result == "DECIMAL(20, 5)"

    def test_decimal128_with_no_scale(self) -> None:
        """decimal128(10, 0) → DECIMAL(10, 0)."""
        result = MySQLTypeMapper.from_arrow_type("decimal128(10, 0)")
        assert result == "DECIMAL(10, 0)"

    def test_whitespace_stripping_leading(self) -> None:
        """Leading whitespace should be stripped."""
        result = MySQLTypeMapper.from_arrow_type("  int32")
        assert result == "INT"

    def test_whitespace_stripping_trailing(self) -> None:
        """Trailing whitespace should be stripped."""
        result = MySQLTypeMapper.from_arrow_type("int32  ")
        assert result == "INT"

    def test_whitespace_stripping_both(self) -> None:
        """Leading and trailing whitespace should be stripped."""
        result = MySQLTypeMapper.from_arrow_type("  string  ")
        assert result == "TEXT"

    def test_unknown_type_fallback_passthrough(self) -> None:
        """Unknown types should pass through unchanged."""
        result = MySQLTypeMapper.from_arrow_type("custom_type")
        assert result == "custom_type"

    def test_unknown_type_with_whitespace_fallback(self) -> None:
        """Unknown types with whitespace should pass through as-is."""
        result = MySQLTypeMapper.from_arrow_type("  my_custom_type  ")
        assert result == "  my_custom_type  "


class TestFromArrowTypeRoundTrip:
    """Round-trip consistency tests: MySQL → Arrow → MySQL DDL."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mapper = MySQLTypeMapper()

    def test_int_roundtrip(self) -> None:
        """INT → int32 → INT."""
        arrow_type = self.mapper.map_mysql_type_name("INT")
        arrow_str = str(arrow_type)
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        assert mysql_ddl == "INT"

    def test_varchar_roundtrip(self) -> None:
        """VARCHAR(255) → string → TEXT."""
        arrow_type = self.mapper.map_mysql_type_name("VARCHAR(255)")
        arrow_str = str(arrow_type)
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        assert mysql_ddl == "TEXT"

    def test_datetime_roundtrip(self) -> None:
        """DATETIME → timestamp[us] → DATETIME."""
        arrow_type = self.mapper.map_mysql_type_name("DATETIME")
        arrow_str = str(arrow_type)
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        assert mysql_ddl == "DATETIME"

    def test_timestamp_roundtrip(self) -> None:
        """TIMESTAMP → timestamp[us, tz=UTC] → TIMESTAMP."""
        arrow_type = self.mapper.map_mysql_type_name("TIMESTAMP")
        arrow_str = str(arrow_type)
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        assert mysql_ddl == "TIMESTAMP"

    def test_bigint_unsigned_roundtrip(self) -> None:
        """BIGINT UNSIGNED → decimal128(20,0) → DECIMAL(20,0) UNSIGNED."""
        arrow_type = self.mapper.map_mysql_type_name("BIGINT UNSIGNED")
        arrow_str = str(arrow_type)
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        # The DDL type should be DECIMAL with precision/scale
        assert "DECIMAL" in mysql_ddl

    def test_date_roundtrip(self) -> None:
        """DATE -> date32[day] -> DATE."""
        arrow_type = self.mapper.map_mysql_type_name("DATE")
        arrow_str = str(arrow_type)
        assert arrow_str == "date32[day]"
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        assert mysql_ddl == "DATE"

    def test_blob_roundtrip(self) -> None:
        """BLOB → binary → BLOB."""
        arrow_type = self.mapper.map_mysql_type_name("BLOB")
        arrow_str = str(arrow_type)
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        assert mysql_ddl == "BLOB"

    def test_text_roundtrip(self) -> None:
        """TEXT → string → TEXT."""
        arrow_type = self.mapper.map_mysql_type_name("TEXT")
        arrow_str = str(arrow_type)
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        assert mysql_ddl == "TEXT"

    def test_float_roundtrip(self) -> None:
        """FLOAT -> float (PyArrow str) -> FLOAT."""
        arrow_type = self.mapper.map_mysql_type_name("FLOAT")
        arrow_str = str(arrow_type)
        assert arrow_str == "float"
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        assert mysql_ddl == "FLOAT"

    def test_double_roundtrip(self) -> None:
        """DOUBLE -> double (PyArrow str) -> DOUBLE."""
        arrow_type = self.mapper.map_mysql_type_name("DOUBLE")
        arrow_str = str(arrow_type)
        assert arrow_str == "double"
        mysql_ddl = MySQLTypeMapper.from_arrow_type(arrow_str)
        assert mysql_ddl == "DOUBLE"
