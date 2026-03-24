from __future__ import annotations

from typing import cast

import pyarrow as pa

from bani.connectors.postgresql.type_mapper import PostgreSQLTypeMapper


class TestFromArrowType:
    """Tests for PostgreSQLTypeMapper.from_arrow_type() static method."""

    # -----------------------------------------------------------------------
    # Test exact-match Arrow types → PG types
    # -----------------------------------------------------------------------

    def test_bool_to_boolean(self) -> None:
        """Test bool Arrow type maps to boolean."""
        assert PostgreSQLTypeMapper.from_arrow_type("bool") == "boolean"

    def test_int8_to_smallint(self) -> None:
        """Test int8 Arrow type maps to smallint."""
        assert PostgreSQLTypeMapper.from_arrow_type("int8") == "smallint"

    def test_int16_to_smallint(self) -> None:
        """Test int16 Arrow type maps to smallint."""
        assert PostgreSQLTypeMapper.from_arrow_type("int16") == "smallint"

    def test_int32_to_integer(self) -> None:
        """Test int32 Arrow type maps to integer."""
        assert PostgreSQLTypeMapper.from_arrow_type("int32") == "integer"

    def test_int64_to_bigint(self) -> None:
        """Test int64 Arrow type maps to bigint."""
        assert PostgreSQLTypeMapper.from_arrow_type("int64") == "bigint"

    def test_uint8_to_smallint(self) -> None:
        """Test uint8 Arrow type maps to smallint."""
        assert PostgreSQLTypeMapper.from_arrow_type("uint8") == "smallint"

    def test_uint16_to_integer(self) -> None:
        """Test uint16 Arrow type maps to integer."""
        assert PostgreSQLTypeMapper.from_arrow_type("uint16") == "integer"

    def test_uint32_to_bigint(self) -> None:
        """Test uint32 Arrow type maps to bigint."""
        assert PostgreSQLTypeMapper.from_arrow_type("uint32") == "bigint"

    def test_uint64_to_numeric(self) -> None:
        """Test uint64 Arrow type maps to numeric(20)."""
        assert PostgreSQLTypeMapper.from_arrow_type("uint64") == "numeric(20)"

    def test_float16_to_real(self) -> None:
        """Test float16 (halffloat) Arrow type maps to real."""
        assert PostgreSQLTypeMapper.from_arrow_type("float16") == "real"

    def test_float32_to_real(self) -> None:
        """Test float32 Arrow type maps to real."""
        assert PostgreSQLTypeMapper.from_arrow_type("float32") == "real"

    def test_float64_to_double_precision(self) -> None:
        """Test float64 Arrow type maps to double precision."""
        assert PostgreSQLTypeMapper.from_arrow_type("float64") == "double precision"

    def test_halffloat_to_real(self) -> None:
        """Test halffloat Arrow type maps to real."""
        assert PostgreSQLTypeMapper.from_arrow_type("halffloat") == "real"

    def test_string_to_text(self) -> None:
        """Test string Arrow type maps to text."""
        assert PostgreSQLTypeMapper.from_arrow_type("string") == "text"

    def test_utf8_to_text(self) -> None:
        """Test utf8 Arrow type maps to text."""
        assert PostgreSQLTypeMapper.from_arrow_type("utf8") == "text"

    def test_large_string_to_text(self) -> None:
        """Test large_string Arrow type maps to text."""
        assert PostgreSQLTypeMapper.from_arrow_type("large_string") == "text"

    def test_large_utf8_to_text(self) -> None:
        """Test large_utf8 Arrow type maps to text."""
        assert PostgreSQLTypeMapper.from_arrow_type("large_utf8") == "text"

    def test_binary_to_bytea(self) -> None:
        """Test binary Arrow type maps to bytea."""
        assert PostgreSQLTypeMapper.from_arrow_type("binary") == "bytea"

    def test_large_binary_to_bytea(self) -> None:
        """Test large_binary Arrow type maps to bytea."""
        assert PostgreSQLTypeMapper.from_arrow_type("large_binary") == "bytea"

    def test_date32_to_date(self) -> None:
        """Test date32 Arrow type maps to date."""
        assert PostgreSQLTypeMapper.from_arrow_type("date32") == "date"

    def test_date64_to_date(self) -> None:
        """Test date64 Arrow type maps to date."""
        assert PostgreSQLTypeMapper.from_arrow_type("date64") == "date"

    def test_null_to_text(self) -> None:
        """Test null Arrow type maps to text."""
        assert PostgreSQLTypeMapper.from_arrow_type("null") == "text"

    # -----------------------------------------------------------------------
    # Test timestamp variants (with/without timezone)
    # -----------------------------------------------------------------------

    def test_timestamp_without_tz(self) -> None:
        """Test timestamp without timezone maps to timestamp."""
        assert PostgreSQLTypeMapper.from_arrow_type("timestamp[us]") == "timestamp"

    def test_timestamp_with_bracket_no_tz(self) -> None:
        """Test timestamp with unit in brackets (no tz)."""
        assert PostgreSQLTypeMapper.from_arrow_type("timestamp[s]") == "timestamp"

    def test_timestamp_with_tz_utc(self) -> None:
        """Test timestamp with UTC timezone."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("timestamp[us, tz=UTC]")
            == "timestamp with time zone"
        )

    def test_timestamp_with_tz_other(self) -> None:
        """Test timestamp with any timezone (not just UTC)."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("timestamp[us, tz=America/New_York]")
            == "timestamp with time zone"
        )

    def test_timestamp_ms_no_tz(self) -> None:
        """Test timestamp with millisecond precision, no timezone."""
        assert PostgreSQLTypeMapper.from_arrow_type("timestamp[ms]") == "timestamp"

    def test_timestamp_ns_with_tz(self) -> None:
        """Test timestamp with nanosecond precision and timezone."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("timestamp[ns, tz=UTC]")
            == "timestamp with time zone"
        )

    # -----------------------------------------------------------------------
    # Test time32 and time64 variants
    # -----------------------------------------------------------------------

    def test_time32_seconds(self) -> None:
        """Test time32 with seconds unit."""
        assert PostgreSQLTypeMapper.from_arrow_type("time32[s]") == "time"

    def test_time32_milliseconds(self) -> None:
        """Test time32 with milliseconds unit."""
        assert PostgreSQLTypeMapper.from_arrow_type("time32[ms]") == "time"

    def test_time64_microseconds(self) -> None:
        """Test time64 with microseconds unit."""
        assert PostgreSQLTypeMapper.from_arrow_type("time64[us]") == "time"

    def test_time64_nanoseconds(self) -> None:
        """Test time64 with nanoseconds unit."""
        assert PostgreSQLTypeMapper.from_arrow_type("time64[ns]") == "time"

    # -----------------------------------------------------------------------
    # Test duration variants
    # -----------------------------------------------------------------------

    def test_duration_seconds(self) -> None:
        """Test duration with seconds unit."""
        assert PostgreSQLTypeMapper.from_arrow_type("duration[s]") == "interval"

    def test_duration_milliseconds(self) -> None:
        """Test duration with milliseconds unit."""
        assert PostgreSQLTypeMapper.from_arrow_type("duration[ms]") == "interval"

    def test_duration_microseconds(self) -> None:
        """Test duration with microseconds unit."""
        assert PostgreSQLTypeMapper.from_arrow_type("duration[us]") == "interval"

    def test_duration_nanoseconds(self) -> None:
        """Test duration with nanoseconds unit."""
        assert PostgreSQLTypeMapper.from_arrow_type("duration[ns]") == "interval"

    # -----------------------------------------------------------------------
    # Test decimal128 with parameters
    # -----------------------------------------------------------------------

    def test_decimal128_default_precision(self) -> None:
        """Test decimal128 with default precision and scale."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("decimal128(38, 10)")
            == "numeric(38, 10)"
        )

    def test_decimal128_custom_precision(self) -> None:
        """Test decimal128 with custom precision and scale."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("decimal128(20, 5)")
            == "numeric(20, 5)"
        )

    def test_decimal128_high_precision(self) -> None:
        """Test decimal128 with high precision."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("decimal128(65, 30)")
            == "numeric(65, 30)"
        )

    def test_decimal128_low_scale(self) -> None:
        """Test decimal128 with low scale."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("decimal128(10, 0)")
            == "numeric(10, 0)"
        )

    def test_decimal128_spaces_in_params(self) -> None:
        """Test decimal128 preserves spacing in parameters."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("decimal128(38, 10)")
            == "numeric(38, 10)"
        )

    # -----------------------------------------------------------------------
    # Test whitespace stripping
    # -----------------------------------------------------------------------

    def test_whitespace_leading(self) -> None:
        """Test that leading whitespace is stripped."""
        assert PostgreSQLTypeMapper.from_arrow_type("  int32") == "integer"

    def test_whitespace_trailing(self) -> None:
        """Test that trailing whitespace is stripped."""
        assert PostgreSQLTypeMapper.from_arrow_type("int32  ") == "integer"

    def test_whitespace_both_sides(self) -> None:
        """Test that whitespace on both sides is stripped."""
        assert PostgreSQLTypeMapper.from_arrow_type("  bool  ") == "boolean"

    def test_whitespace_tabs(self) -> None:
        """Test that tabs are stripped."""
        assert PostgreSQLTypeMapper.from_arrow_type("\tint64\t") == "bigint"

    def test_whitespace_newlines(self) -> None:
        """Test that newlines are stripped."""
        assert PostgreSQLTypeMapper.from_arrow_type("\nstring\n") == "text"

    def test_whitespace_mixed(self) -> None:
        """Test mixed whitespace is stripped."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("  \t timestamp[us]  \n")
            == "timestamp"
        )

    # -----------------------------------------------------------------------
    # Test fallback for unknown types
    # -----------------------------------------------------------------------

    def test_unknown_type_passthrough(self) -> None:
        """Test that unknown types are passed through as-is."""
        assert PostgreSQLTypeMapper.from_arrow_type("custom_type") == "custom_type"

    def test_unknown_type_with_params(self) -> None:
        """Test that unknown parameterized types are passed through."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("unknown[param1, param2]")
            == "unknown[param1, param2]"
        )

    def test_invalid_but_passthrough(self) -> None:
        """Test that invalid Arrow type strings are passed through."""
        assert (
            PostgreSQLTypeMapper.from_arrow_type("not_a_real_type")
            == "not_a_real_type"
        )


class TestMapPgTypeName:
    """Tests for PostgreSQLTypeMapper.map_pg_type_name() method."""

    def test_basic_integer_types(self) -> None:
        """Test basic integer type mapping."""
        mapper = PostgreSQLTypeMapper()
        assert mapper.map_pg_type_name("integer") == pa.int32()
        assert mapper.map_pg_type_name("bigint") == pa.int64()
        assert mapper.map_pg_type_name("smallint") == pa.int16()

    def test_float_types(self) -> None:
        """Test floating-point type mapping."""
        mapper = PostgreSQLTypeMapper()
        assert mapper.map_pg_type_name("real") == pa.float32()
        assert mapper.map_pg_type_name("double precision") == pa.float64()

    def test_string_types(self) -> None:
        """Test string type mapping."""
        mapper = PostgreSQLTypeMapper()
        assert mapper.map_pg_type_name("text") == pa.string()
        assert mapper.map_pg_type_name("varchar") == pa.string()
        assert mapper.map_pg_type_name("char") == pa.string()

    def test_timestamp_types(self) -> None:
        """Test timestamp type mapping."""
        mapper = PostgreSQLTypeMapper()
        ts_no_tz = mapper.map_pg_type_name("timestamp")
        assert ts_no_tz == pa.timestamp("us")
        assert cast(pa.TimestampType, ts_no_tz).tz is None

        ts_with_tz = mapper.map_pg_type_name("timestamp with time zone")
        assert ts_with_tz == pa.timestamp("us", tz="UTC")
        assert cast(pa.TimestampType, ts_with_tz).tz == "UTC"

    def test_whitespace_stripping(self) -> None:
        """Test that whitespace is stripped from type names."""
        mapper = PostgreSQLTypeMapper()
        assert mapper.map_pg_type_name("  integer  ") == pa.int32()
        assert mapper.map_pg_type_name("\ttext\n") == pa.string()

    def test_case_insensitivity(self) -> None:
        """Test that type names are case-insensitive."""
        mapper = PostgreSQLTypeMapper()
        assert mapper.map_pg_type_name("INTEGER") == pa.int32()
        assert mapper.map_pg_type_name("IntEgEr") == pa.int32()
        assert mapper.map_pg_type_name("TEXT") == pa.string()

    def test_parameterized_types_stripped(self) -> None:
        """Test that type parameters are stripped before matching."""
        mapper = PostgreSQLTypeMapper()
        assert mapper.map_pg_type_name("varchar(255)") == pa.string()
        assert mapper.map_pg_type_name("numeric(10, 2)") == pa.decimal128(38, 10)
        assert mapper.map_pg_type_name("char(50)") == pa.string()

    def test_unknown_type_defaults_to_string(self) -> None:
        """Test that unknown types default to string."""
        mapper = PostgreSQLTypeMapper()
        assert mapper.map_pg_type_name("unknown_type") == pa.string()
        assert mapper.map_pg_type_name("custom") == pa.string()


class TestRoundTripConsistency:
    """Tests for round-trip consistency: PG type → Arrow → PG type."""

    def test_roundtrip_integer(self) -> None:
        """Test integer round-trip."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("integer")
        arrow_str = str(arrow_type)
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "integer"

    def test_roundtrip_bigint(self) -> None:
        """Test bigint round-trip."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("bigint")
        arrow_str = str(arrow_type)
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "bigint"

    def test_roundtrip_text(self) -> None:
        """Test text round-trip."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("text")
        arrow_str = str(arrow_type)
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "text"

    def test_roundtrip_timestamp_no_tz(self) -> None:
        """Test timestamp without timezone round-trip."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("timestamp")
        arrow_str = str(arrow_type)
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "timestamp"

    def test_roundtrip_timestamp_with_tz(self) -> None:
        """Test timestamp with timezone round-trip."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("timestamp with time zone")
        arrow_str = str(arrow_type)
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "timestamp with time zone"

    def test_roundtrip_date(self) -> None:
        """Test date round-trip: date -> date32[day] -> date."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("date")
        arrow_str = str(arrow_type)
        assert arrow_str == "date32[day]"
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "date"

    def test_roundtrip_real(self) -> None:
        """Test real round-trip: real -> float -> real."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("real")
        arrow_str = str(arrow_type)
        assert arrow_str == "float"
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "real"

    def test_roundtrip_double(self) -> None:
        """double precision -> double -> double precision."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("double precision")
        arrow_str = str(arrow_type)
        assert arrow_str == "double"
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "double precision"

    def test_roundtrip_boolean(self) -> None:
        """Test boolean round-trip."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("boolean")
        arrow_str = str(arrow_type)
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "boolean"

    def test_roundtrip_bytea(self) -> None:
        """Test bytea round-trip."""
        mapper = PostgreSQLTypeMapper()
        arrow_type = mapper.map_pg_type_name("bytea")
        arrow_str = str(arrow_type)
        pg_type = PostgreSQLTypeMapper.from_arrow_type(arrow_str)
        assert pg_type == "bytea"
