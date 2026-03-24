"""Unit tests for PostgreSQL data reader (Arrow type mapping)."""

from __future__ import annotations

import pyarrow as pa

from bani.connectors.postgresql.type_mapper import PostgreSQLTypeMapper


class TestPostgreSQLTypeMapper:
    """Tests for PostgreSQL to Arrow type mapping."""

    def test_map_integer_types(self) -> None:
        """Should map PostgreSQL integer types to Arrow int types."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(16) == pa.bool_()  # boolean
        assert mapper.map_pg_type_oid(21) == pa.int16()  # smallint
        assert mapper.map_pg_type_oid(23) == pa.int32()  # integer
        assert mapper.map_pg_type_oid(20) == pa.int64()  # bigint

    def test_map_float_types(self) -> None:
        """Should map PostgreSQL float types to Arrow float types."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(700) == pa.float32()  # real
        assert mapper.map_pg_type_oid(701) == pa.float64()  # double precision

    def test_map_string_types(self) -> None:
        """Should map PostgreSQL string types to Arrow string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(25) == pa.string()  # text
        assert mapper.map_pg_type_oid(1043) == pa.string()  # varchar
        assert mapper.map_pg_type_oid(1042) == pa.string()  # char

    def test_map_date_time_types(self) -> None:
        """Should map PostgreSQL date/time types to Arrow."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(1082) == pa.date32()  # date
        assert mapper.map_pg_type_oid(1083) == pa.time64("us")  # time
        assert mapper.map_pg_type_oid(1114) == pa.timestamp("us")  # timestamp
        assert mapper.map_pg_type_oid(1184) == pa.timestamp("us", tz="UTC")

    def test_map_json_types(self) -> None:
        """Should map PostgreSQL JSON types to Arrow string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(114) == pa.string()  # json
        assert mapper.map_pg_type_oid(3802) == pa.string()  # jsonb

    def test_map_uuid_type(self) -> None:
        """Should map PostgreSQL UUID to Arrow string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(2950) == pa.string()  # uuid

    def test_map_unknown_type(self) -> None:
        """Should default to string for unknown types."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(99999) == pa.string()

    def test_map_type_by_name_numeric(self) -> None:
        """Should map numeric type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("smallint") == pa.int16()
        assert mapper.map_pg_type_name("integer") == pa.int32()
        assert mapper.map_pg_type_name("int") == pa.int32()
        assert mapper.map_pg_type_name("bigint") == pa.int64()
        assert mapper.map_pg_type_name("real") == pa.float32()
        assert mapper.map_pg_type_name("double precision") == pa.float64()

    def test_map_type_by_name_string(self) -> None:
        """Should map string type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("text") == pa.string()
        assert mapper.map_pg_type_name("varchar") == pa.string()
        assert mapper.map_pg_type_name("char") == pa.string()

    def test_map_type_by_name_boolean(self) -> None:
        """Should map boolean type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("boolean") == pa.bool_()
        assert mapper.map_pg_type_name("bool") == pa.bool_()

    def test_map_type_by_name_case_insensitive(self) -> None:
        """Should handle case-insensitive type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("INTEGER") == pa.int32()
        assert mapper.map_pg_type_name("Integer") == pa.int32()
        assert mapper.map_pg_type_name("TEXT") == pa.string()

    def test_map_type_by_name_with_parameters(self) -> None:
        """Should strip parameters when mapping by name."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("varchar(255)") == pa.string()
        assert mapper.map_pg_type_name("numeric(10,2)") == pa.decimal128(38, 10)

    def test_map_type_by_name_with_spaces(self) -> None:
        """Should handle whitespace in type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("  double precision  ") == pa.float64()
        assert mapper.map_pg_type_name("double precision") == pa.float64()

    def test_map_serial_types(self) -> None:
        """Should map serial types to their base integer types."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("smallserial") == pa.int16()
        assert mapper.map_pg_type_name("serial") == pa.int32()
        assert mapper.map_pg_type_name("bigserial") == pa.int64()

    def test_map_date_time_type_names(self) -> None:
        """Should map date/time type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("date") == pa.date32()
        assert mapper.map_pg_type_name("time") == pa.time64("us")
        assert mapper.map_pg_type_name("timestamp") == pa.timestamp("us")
        assert mapper.map_pg_type_name("timestamptz") == pa.timestamp("us", tz="UTC")

    def test_map_json_type_names(self) -> None:
        """Should map JSON type names to string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("json") == pa.string()
        assert mapper.map_pg_type_name("jsonb") == pa.string()

    def test_map_uuid_type_name(self) -> None:
        """Should map UUID type name to string."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("uuid") == pa.string()

    def test_map_bytea_type(self) -> None:
        """Should map bytea to Arrow binary."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_oid(17) == pa.binary()  # bytea
        assert mapper.map_pg_type_name("bytea") == pa.binary()

    def test_map_unknown_type_by_name(self) -> None:
        """Should default to string for unknown type names."""
        mapper = PostgreSQLTypeMapper()

        assert mapper.map_pg_type_name("unknown_type_xyz") == pa.string()
