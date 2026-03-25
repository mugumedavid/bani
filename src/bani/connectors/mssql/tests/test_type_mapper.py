"""Unit tests for MSSQL type mapper."""

from __future__ import annotations

import pyarrow as pa  # type: ignore[import-untyped]

from bani.connectors.mssql.type_mapper import MSSQLTypeMapper


class TestMSSQLTypeMapper:
    """Tests for MSSQLTypeMapper."""

    def test_map_mssql_type_name_int(self) -> None:
        """Test mapping INT type."""
        mapper = MSSQLTypeMapper()
        assert mapper.map_mssql_type_name("int") == pa.int32()

    def test_map_mssql_type_name_bigint(self) -> None:
        """Test mapping BIGINT type."""
        mapper = MSSQLTypeMapper()
        assert mapper.map_mssql_type_name("bigint") == pa.int64()

    def test_map_mssql_type_name_varchar(self) -> None:
        """Test mapping VARCHAR type."""
        mapper = MSSQLTypeMapper()
        assert mapper.map_mssql_type_name("varchar(255)") == pa.string()

    def test_map_mssql_type_name_nvarchar(self) -> None:
        """Test mapping NVARCHAR type."""
        mapper = MSSQLTypeMapper()
        assert mapper.map_mssql_type_name("nvarchar(max)") == pa.string()

    def test_map_mssql_type_name_datetime2(self) -> None:
        """Test mapping DATETIME2 type."""
        mapper = MSSQLTypeMapper()
        assert mapper.map_mssql_type_name("datetime2") == pa.timestamp("us")

    def test_map_mssql_type_name_date(self) -> None:
        """Test mapping DATE type."""
        mapper = MSSQLTypeMapper()
        assert mapper.map_mssql_type_name("date") == pa.date32()

    def test_map_mssql_type_name_decimal(self) -> None:
        """Test mapping DECIMAL type with parameters."""
        mapper = MSSQLTypeMapper()
        result = mapper.map_mssql_type_name("decimal(18,2)")
        assert pa.types.is_decimal(result)

    def test_map_mssql_type_name_bit(self) -> None:
        """Test mapping BIT type."""
        mapper = MSSQLTypeMapper()
        assert mapper.map_mssql_type_name("bit") == pa.bool_()

    def test_map_mssql_type_name_unknown(self) -> None:
        """Test mapping unknown type defaults to string."""
        mapper = MSSQLTypeMapper()
        assert mapper.map_mssql_type_name("unknown_type") == pa.string()

    def test_from_arrow_type_int32(self) -> None:
        """Test Arrow int32 maps to MSSQL INT."""
        assert MSSQLTypeMapper.from_arrow_type("int32") == "INT"

    def test_from_arrow_type_int64(self) -> None:
        """Test Arrow int64 maps to MSSQL BIGINT."""
        assert MSSQLTypeMapper.from_arrow_type("int64") == "BIGINT"

    def test_from_arrow_type_string(self) -> None:
        """Test Arrow string maps to MSSQL NVARCHAR(MAX)."""
        assert MSSQLTypeMapper.from_arrow_type("string") == "NVARCHAR(MAX)"

    def test_from_arrow_type_float(self) -> None:
        """Test Arrow float maps to MSSQL REAL."""
        assert MSSQLTypeMapper.from_arrow_type("float") == "REAL"

    def test_from_arrow_type_double(self) -> None:
        """Test Arrow double maps to MSSQL FLOAT."""
        assert MSSQLTypeMapper.from_arrow_type("double") == "FLOAT"

    def test_from_arrow_type_bool(self) -> None:
        """Test Arrow bool maps to MSSQL BIT."""
        assert MSSQLTypeMapper.from_arrow_type("bool") == "BIT"

    def test_from_arrow_type_date32(self) -> None:
        """Test Arrow date32 maps to MSSQL DATE."""
        assert MSSQLTypeMapper.from_arrow_type("date32[day]") == "DATE"

    def test_from_arrow_type_timestamp(self) -> None:
        """Test Arrow timestamp maps to MSSQL DATETIME2."""
        assert MSSQLTypeMapper.from_arrow_type("timestamp[us]") == "DATETIME2"

    def test_from_arrow_type_timestamp_tz(self) -> None:
        """Test Arrow timestamp with TZ maps to MSSQL DATETIMEOFFSET."""
        assert (
            MSSQLTypeMapper.from_arrow_type("timestamp[us, tz=UTC]") == "DATETIMEOFFSET"
        )

    def test_from_arrow_type_time64(self) -> None:
        """Test Arrow time64 maps to MSSQL TIME."""
        assert MSSQLTypeMapper.from_arrow_type("time64[us]") == "TIME"

    def test_from_arrow_type_decimal128(self) -> None:
        """Test Arrow decimal128 maps to MSSQL DECIMAL."""
        assert (
            MSSQLTypeMapper.from_arrow_type("decimal128(38, 10)") == "DECIMAL(38, 10)"
        )

    def test_from_arrow_type_binary(self) -> None:
        """Test Arrow binary maps to MSSQL VARBINARY(MAX)."""
        assert MSSQLTypeMapper.from_arrow_type("binary") == "VARBINARY(MAX)"

    def test_from_arrow_type_unknown(self) -> None:
        """Test unknown Arrow type is passed through."""
        result = MSSQLTypeMapper.from_arrow_type("unknown_type")
        assert result == "unknown_type"
