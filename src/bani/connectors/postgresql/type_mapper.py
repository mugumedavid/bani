"""Type mapping between PostgreSQL and Arrow types."""

from __future__ import annotations

from typing import ClassVar

import pyarrow as pa


class PostgreSQLTypeMapper:
    """Maps PostgreSQL type OIDs to Arrow types.

    PostgreSQL uses type OIDs internally to identify types. This mapper
    provides conversion to Arrow types for efficient in-memory representation.
    """

    # PostgreSQL built-in type OIDs (common ones)
    _PG_TYPE_MAP: ClassVar[dict[int, pa.DataType]] = {
        # Numeric types
        16: pa.bool_(),  # boolean
        20: pa.int64(),  # bigint
        21: pa.int16(),  # smallint
        23: pa.int32(),  # integer
        700: pa.float32(),  # real
        701: pa.float64(),  # double precision
        1700: pa.decimal128(38, 10),  # numeric
        # Character types
        25: pa.string(),  # text
        1043: pa.string(),  # varchar
        1042: pa.string(),  # char
        # Byte types
        17: pa.binary(),  # bytea
        # Date/Time types
        1082: pa.date32(),  # date
        1083: pa.time64("us"),  # time without timezone
        1114: pa.timestamp("us"),  # timestamp without timezone
        1184: pa.timestamp("us", tz="UTC"),  # timestamp with timezone
        1186: pa.duration("us"),  # interval
        # UUID
        2950: pa.string(),  # uuid (stored as string)
        # JSON types
        114: pa.string(),  # json (stored as string)
        3802: pa.string(),  # jsonb (stored as string)
        # Network types
        869: pa.string(),  # inet
        650: pa.string(),  # cidr
        804: pa.string(),  # macaddr
        # Array types (generic - arrays are stored as string)
        1007: pa.string(),  # integer array
        1009: pa.string(),  # text array
    }

    def map_pg_type_oid(self, type_oid: int) -> pa.DataType:
        """Map a PostgreSQL type OID to an Arrow type.

        Args:
            type_oid: PostgreSQL type OID.

        Returns:
            Corresponding Arrow data type. Defaults to string if OID not recognized.
        """
        if type_oid in self._PG_TYPE_MAP:
            return self._PG_TYPE_MAP[type_oid]
        # Default to string for unknown types
        return pa.string()

    def map_pg_type_name(self, type_name: str) -> pa.DataType:
        """Map a PostgreSQL type name to an Arrow type.

        Args:
            type_name: PostgreSQL type name (e.g., "integer", "text").

        Returns:
            Corresponding Arrow data type.
        """
        type_lower = type_name.lower().strip()

        # Handle parameterized types
        if "(" in type_lower:
            type_lower = type_lower[: type_lower.index("(")]

        mapping: dict[str, pa.DataType] = {
            # Numeric types
            "smallint": pa.int16(),
            "integer": pa.int32(),
            "int": pa.int32(),
            "int4": pa.int32(),
            "int8": pa.int64(),
            "bigint": pa.int64(),
            "real": pa.float32(),
            "float4": pa.float32(),
            "double": pa.float64(),
            "double precision": pa.float64(),
            "float8": pa.float64(),
            "numeric": pa.decimal128(38, 10),
            "decimal": pa.decimal128(38, 10),
            # Boolean
            "boolean": pa.bool_(),
            "bool": pa.bool_(),
            # Character types
            "text": pa.string(),
            "varchar": pa.string(),
            "char": pa.string(),
            "character": pa.string(),
            "name": pa.string(),
            # Binary
            "bytea": pa.binary(),
            # Date/Time
            "date": pa.date32(),
            "time": pa.time64("us"),
            "time without time zone": pa.time64("us"),
            "timestamp": pa.timestamp("us"),
            "timestamp without time zone": pa.timestamp("us"),
            "timestamp with time zone": pa.timestamp("us", tz="UTC"),
            "timestamptz": pa.timestamp("us", tz="UTC"),
            "interval": pa.duration("us"),
            # UUID
            "uuid": pa.string(),
            # JSON
            "json": pa.string(),
            "jsonb": pa.string(),
            # Network
            "inet": pa.string(),
            "cidr": pa.string(),
            "macaddr": pa.string(),
            # Serial types (map to int)
            "smallserial": pa.int16(),
            "serial": pa.int32(),
            "bigserial": pa.int64(),
        }

        return mapping.get(type_lower, pa.string())
