"""Unit tests for Oracle type mapper."""

from __future__ import annotations

import datetime
from decimal import Decimal

import pyarrow as pa

from bani.connectors.oracle.type_mapper import OracleTypeMapper


class TestOracleTypeMapperByName:
    """Tests for Oracle type name mapping to Arrow types."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mapper = OracleTypeMapper()

    def test_map_number_to_decimal128(self) -> None:
        """NUMBER should map to decimal128."""
        result = self.mapper.map_oracle_type_name("NUMBER")
        assert result == pa.decimal128(38, 10)

    def test_map_number_with_precision_and_scale(self) -> None:
        """NUMBER(10,2) should map to decimal128(10,2)."""
        result = self.mapper.map_oracle_type_name("NUMBER(10,2)")
        assert result == pa.decimal128(10, 2)

    def test_map_number_with_precision_only(self) -> None:
        """NUMBER(10) should map to decimal128(10,0)."""
        result = self.mapper.map_oracle_type_name("NUMBER(10)")
        assert result == pa.decimal128(10, 0)

    def test_map_float_to_float64(self) -> None:
        """FLOAT should map to float64."""
        result = self.mapper.map_oracle_type_name("FLOAT")
        assert result == pa.float64()

    def test_map_integer_to_int64(self) -> None:
        """INTEGER should map to int64."""
        result = self.mapper.map_oracle_type_name("INTEGER")
        assert result == pa.int64()

    def test_map_varchar2_to_string(self) -> None:
        """VARCHAR2 should map to string."""
        result = self.mapper.map_oracle_type_name("VARCHAR2(100)")
        assert result == pa.string()

    def test_map_nvarchar2_to_string(self) -> None:
        """NVARCHAR2 should map to string."""
        result = self.mapper.map_oracle_type_name("NVARCHAR2(100)")
        assert result == pa.string()

    def test_map_char_to_string(self) -> None:
        """CHAR should map to string."""
        result = self.mapper.map_oracle_type_name("CHAR(10)")
        assert result == pa.string()

    def test_map_clob_to_string(self) -> None:
        """CLOB should map to string."""
        result = self.mapper.map_oracle_type_name("CLOB")
        assert result == pa.string()

    def test_map_blob_to_binary(self) -> None:
        """BLOB should map to binary."""
        result = self.mapper.map_oracle_type_name("BLOB")
        assert result == pa.binary()

    def test_map_raw_to_binary(self) -> None:
        """RAW should map to binary."""
        result = self.mapper.map_oracle_type_name("RAW(2000)")
        assert result == pa.binary()

    def test_map_date_to_timestamp(self) -> None:
        """DATE should map to timestamp (Oracle DATE includes time)."""
        result = self.mapper.map_oracle_type_name("DATE")
        assert result == pa.timestamp("us")

    def test_map_timestamp_to_timestamp_no_tz(self) -> None:
        """TIMESTAMP should map to timestamp."""
        result = self.mapper.map_oracle_type_name("TIMESTAMP")
        assert result == pa.timestamp("us")

    def test_map_timestamp_with_tz_to_timestamp_utc(self) -> None:
        """TIMESTAMP WITH TIME ZONE should map to timestamp with UTC."""
        result = self.mapper.map_oracle_type_name("TIMESTAMP WITH TIME ZONE")
        assert result == pa.timestamp("us", tz="UTC")

    def test_map_timestamp_with_local_tz(self) -> None:
        """TIMESTAMP WITH LOCAL TIME ZONE should map to timestamp with UTC."""
        result = self.mapper.map_oracle_type_name("TIMESTAMP WITH LOCAL TIME ZONE")
        assert result == pa.timestamp("us", tz="UTC")

    def test_map_long_to_string(self) -> None:
        """LONG (deprecated) should map to string."""
        result = self.mapper.map_oracle_type_name("LONG")
        assert result == pa.string()

    def test_map_unknown_type_to_string(self) -> None:
        """Unknown types should map to string."""
        result = self.mapper.map_oracle_type_name("UNKNOWN_TYPE")
        assert result == pa.string()


class TestOracleTypeMapperFromArrow:
    """Tests for Arrow type to Oracle DDL mapping."""

    def test_from_arrow_bool_to_number(self) -> None:
        """bool should map to NUMBER(1,0)."""
        result = OracleTypeMapper.from_arrow_type("bool")
        assert result == "NUMBER(1,0)"

    def test_from_arrow_int32_to_number(self) -> None:
        """int32 should map to NUMBER(10,0)."""
        result = OracleTypeMapper.from_arrow_type("int32")
        assert result == "NUMBER(10,0)"

    def test_from_arrow_int64_to_number(self) -> None:
        """int64 should map to NUMBER(19,0)."""
        result = OracleTypeMapper.from_arrow_type("int64")
        assert result == "NUMBER(19,0)"

    def test_from_arrow_float_to_binary_float(self) -> None:
        """float should map to BINARY_FLOAT."""
        result = OracleTypeMapper.from_arrow_type("float")
        assert result == "BINARY_FLOAT"

    def test_from_arrow_double_to_binary_double(self) -> None:
        """double should map to BINARY_DOUBLE."""
        result = OracleTypeMapper.from_arrow_type("double")
        assert result == "BINARY_DOUBLE"

    def test_from_arrow_string_to_varchar2(self) -> None:
        """string should map to VARCHAR2(4000)."""
        result = OracleTypeMapper.from_arrow_type("string")
        assert result == "VARCHAR2(4000)"

    def test_from_arrow_large_string_to_clob(self) -> None:
        """large_string should map to CLOB."""
        result = OracleTypeMapper.from_arrow_type("large_string")
        assert result == "CLOB"

    def test_from_arrow_binary_to_raw(self) -> None:
        """binary should map to RAW(2000)."""
        result = OracleTypeMapper.from_arrow_type("binary")
        assert result == "RAW(2000)"

    def test_from_arrow_large_binary_to_blob(self) -> None:
        """large_binary should map to BLOB."""
        result = OracleTypeMapper.from_arrow_type("large_binary")
        assert result == "BLOB"

    def test_from_arrow_date32_to_date(self) -> None:
        """date32[day] should map to DATE."""
        result = OracleTypeMapper.from_arrow_type("date32[day]")
        assert result == "DATE"

    def test_from_arrow_timestamp_to_timestamp(self) -> None:
        """timestamp[us] should map to TIMESTAMP."""
        result = OracleTypeMapper.from_arrow_type("timestamp[us]")
        assert result == "TIMESTAMP"

    def test_from_arrow_timestamp_with_tz_to_timestamp_tz(self) -> None:
        """timestamp[us, tz=UTC] should map to TIMESTAMP WITH TIME ZONE."""
        result = OracleTypeMapper.from_arrow_type("timestamp[us, tz=UTC]")
        assert result == "TIMESTAMP WITH TIME ZONE"

    def test_from_arrow_decimal128_to_number(self) -> None:
        """decimal128(10, 2) should map to NUMBER(10, 2)."""
        result = OracleTypeMapper.from_arrow_type("decimal128(10, 2)")
        assert result == "NUMBER(10, 2)"

    def test_from_arrow_unknown_to_varchar2(self) -> None:
        """Unknown Arrow types should default to VARCHAR2(4000)."""
        result = OracleTypeMapper.from_arrow_type("unknown_type")
        assert result == "VARCHAR2(4000)"


class TestOracleTypeMapperCoercion:
    """Tests for value coercion."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mapper = OracleTypeMapper()

    def test_coerce_none_returns_none(self) -> None:
        """None should remain None."""
        result = self.mapper.coerce_value(None, pa.string())
        assert result is None

    def test_coerce_datetime_to_timestamp(self) -> None:
        """Datetime should be coercible to timestamp."""
        dt = datetime.datetime(2024, 1, 15, 12, 30, 45)
        result = self.mapper.coerce_value(dt, pa.timestamp("us"))
        assert result == dt

    def test_coerce_decimal_to_decimal128(self) -> None:
        """String should be coercible to Decimal."""
        result = self.mapper.coerce_value("123.45", pa.decimal128(10, 2))
        assert isinstance(result, Decimal)
        assert result == Decimal("123.45")

    def test_coerce_int_to_decimal128(self) -> None:
        """Int should be coercible to Decimal."""
        result = self.mapper.coerce_value(100, pa.decimal128(10, 2))
        assert isinstance(result, Decimal)
        assert result == Decimal("100")
