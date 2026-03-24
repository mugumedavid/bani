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

    # ------------------------------------------------------------------
    # Arrow → PostgreSQL DDL mapping  (used by create_table in the sink)
    # ------------------------------------------------------------------

    @staticmethod
    def from_arrow_type(arrow_type_str: str) -> str:
        """Convert a canonical Arrow type string to a PostgreSQL DDL type.

        This is the *reverse* of ``map_pg_type_name`` / ``map_pg_type_oid``.
        Every sink connector implements one of these so that N connectors
        need only N mappers (not NxN cross-database translation tables).

        Args:
            arrow_type_str: Arrow type string as produced by
                ``str(pa_type)`` — e.g. ``"int32"``, ``"timestamp[us]"``,
                ``"decimal128(38, 10)"``, ``"string"``.

        Returns:
            A PostgreSQL DDL type string such as ``"integer"`` or
            ``"timestamp"``.
        """
        # Exact-match table.  Keys must use the *actual* strings
        # produced by ``str(pa_type)`` — e.g. PyArrow emits
        # ``"float"`` (not ``"float32"``), ``"double"`` (not
        # ``"float64"``), ``"date32[day]"`` (not ``"date32"``).
        # We include both the PyArrow form and the short alias so
        # callers using either convention are handled.
        _ARROW_TO_PG: dict[str, str] = {
            # Boolean
            "bool": "boolean",
            # Integer types
            "int8": "smallint",
            "int16": "smallint",
            "int32": "integer",
            "int64": "bigint",
            "uint8": "smallint",
            "uint16": "integer",
            "uint32": "bigint",
            "uint64": "numeric(20)",
            # Floating-point — PyArrow str() forms
            "float": "real",
            "double": "double precision",
            # Floating-point — explicit aliases
            "float16": "real",
            "float32": "real",
            "float64": "double precision",
            "halffloat": "real",
            # String / binary
            "string": "text",
            "utf8": "text",
            "large_string": "text",
            "large_utf8": "text",
            "binary": "bytea",
            "large_binary": "bytea",
            # Null
            "null": "text",
        }

        ts = arrow_type_str.strip()

        # Fast exact match
        if ts in _ARROW_TO_PG:
            return _ARROW_TO_PG[ts]

        # date32[day], date64[ms]
        if ts.startswith("date32") or ts.startswith("date64"):
            return "date"

        # timestamp[us], timestamp[us, tz=UTC], etc.
        if ts.startswith("timestamp"):
            if "tz=" in ts:
                return "timestamp with time zone"
            return "timestamp"

        # time32[ms], time64[us]
        if ts.startswith("time32") or ts.startswith("time64"):
            return "time"

        # duration[us]
        if ts.startswith("duration"):
            return "interval"

        # decimal128(p, s) -> numeric(p, s)
        if ts.startswith("decimal128"):
            params = ts[len("decimal128"):]  # e.g. "(38, 10)"
            return f"numeric{params}"

        # Fallback: pass through (might already be a PG type)
        return arrow_type_str
