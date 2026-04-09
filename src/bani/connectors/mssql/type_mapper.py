"""Type mapping between MSSQL and Arrow types.

Handles MSSQL system types to Arrow types for introspection,
and provides from_arrow_type() for reverse mapping (Arrow to MSSQL DDL).
"""

from __future__ import annotations

from typing import ClassVar

import pyarrow as pa


class MSSQLTypeMapper:
    """Maps MSSQL types to Arrow types and vice versa.

    MSSQL uses system type names accessible via sys.types and INFORMATION_SCHEMA.
    This mapper converts between MSSQL type names and Arrow types for efficient
    in-memory representation.
    """

    # MSSQL type name -> Arrow type mapping
    # Based on SQL Server documentation for all common types
    _MSSQL_TYPE_MAP: ClassVar[dict[str, pa.DataType]] = {
        # Numeric types
        "tinyint": pa.uint8(),
        "smallint": pa.int16(),
        "int": pa.int32(),
        "bigint": pa.int64(),
        "decimal": pa.decimal128(38, 10),
        "numeric": pa.decimal128(38, 10),
        "smallmoney": pa.decimal128(10, 4),
        "money": pa.decimal128(19, 4),
        "float": pa.float64(),
        "real": pa.float32(),
        # Boolean
        "bit": pa.bool_(),
        # String types (non-Unicode)
        "char": pa.string(),
        "varchar": pa.string(),
        "text": pa.string(),
        # Unicode string types
        "nchar": pa.string(),
        "nvarchar": pa.string(),
        "ntext": pa.string(),
        # Binary types
        "binary": pa.binary(),
        "varbinary": pa.binary(),
        "image": pa.binary(),
        # Date/Time types
        "date": pa.date32(),
        "time": pa.time64("us"),
        "datetime": pa.timestamp("us"),
        "datetime2": pa.timestamp("us"),
        "smalldatetime": pa.timestamp("us"),
        "datetimeoffset": pa.timestamp("us", tz="UTC"),
        # Special types
        "uniqueidentifier": pa.string(),
        "xml": pa.string(),
        "json": pa.string(),
        "sql_variant": pa.string(),
        # Spatial types (stored as strings)
        "geometry": pa.string(),
        "geography": pa.string(),
        # Row version
        "rowversion": pa.binary(),
        "timestamp": pa.binary(),  # deprecated alias for rowversion
    }

    def map_mssql_type_name(self, type_name: str) -> pa.DataType:
        """Map a MSSQL type name to an Arrow type.

        Handles parameterized types like decimal(18,2), varchar(255), etc.

        Args:
            type_name: MSSQL type name, possibly with parameters.

        Returns:
            Corresponding Arrow data type. Defaults to string if unknown.
        """
        type_upper = type_name.upper().strip()

        # Strip parameters like (255), (18,2), etc.
        base_type = type_upper
        if "(" in base_type:
            base_type = base_type[: base_type.index("(")].strip()

        # Look up in mapping
        base_type_lower = base_type.lower()
        if base_type_lower in self._MSSQL_TYPE_MAP:
            return self._MSSQL_TYPE_MAP[base_type_lower]

        # Special handling for parameterized types
        if base_type_lower == "decimal" or base_type_lower == "numeric":
            # decimal(p, s) -> decimal128(p, s)
            # Extract precision and scale
            if "(" in type_name and ")" in type_name:
                params = type_name[type_name.index("(") + 1 : type_name.index(")")]
                parts = params.split(",")
                if len(parts) >= 1:
                    try:
                        precision = int(parts[0].strip())
                        scale = int(parts[1].strip()) if len(parts) > 1 else 0
                        return pa.decimal128(precision, scale)
                    except (ValueError, IndexError):
                        pass
            return pa.decimal128(38, 10)

        # Default to string for unknown types
        return pa.string()

    # ------------------------------------------------------------------
    # Arrow → MSSQL DDL mapping (used by create_table in the sink)
    # ------------------------------------------------------------------

    @staticmethod
    def from_arrow_type(arrow_type_str: str) -> str:
        """Convert a canonical Arrow type string to an MSSQL DDL type.

        Counterpart of ``map_mssql_type_name``.
        Every sink connector implements one of these so that N connectors
        need only N mappers (not NxN cross-database translation tables).

        Args:
            arrow_type_str: Arrow type string as produced by
                ``str(pa_type)`` — e.g. ``"int32"``, ``"timestamp[us]"``,
                ``"decimal128(38, 10)"``, ``"string"``.

        Returns:
            An MSSQL DDL type string such as ``"INT"`` or ``"DATETIME2"``.
        """
        # Exact-match table. Keys must use the *actual* strings
        # produced by ``str(pa_type)`` — e.g. PyArrow emits
        # ``"float"`` (not ``"float32"``), ``"double"`` (not ``"float64"``).
        _ARROW_TO_MSSQL: dict[str, str] = {
            # Boolean
            "bool": "BIT",
            # Integer types
            "int8": "SMALLINT",  # MSSQL TINYINT is unsigned (0-255)
            "int16": "SMALLINT",
            "int32": "INT",
            "int64": "BIGINT",
            "uint8": "SMALLINT",
            "uint16": "INT",
            "uint32": "BIGINT",
            "uint64": "DECIMAL(20,0)",
            # Floating-point — PyArrow str() forms
            "float": "REAL",
            "double": "FLOAT",
            # Floating-point — explicit aliases
            "float16": "REAL",
            "float32": "REAL",
            "float64": "FLOAT",
            "halffloat": "REAL",
            # String / binary
            "string": "NVARCHAR(MAX)",
            "utf8": "NVARCHAR(MAX)",
            "large_string": "NVARCHAR(MAX)",
            "large_utf8": "NVARCHAR(MAX)",
            "binary": "VARBINARY(MAX)",
            "large_binary": "VARBINARY(MAX)",
            # Null
            "null": "NVARCHAR(MAX)",
        }

        ts = arrow_type_str.strip()

        # Fast exact match
        if ts in _ARROW_TO_MSSQL:
            return _ARROW_TO_MSSQL[ts]

        # date32[day], date64[ms]
        if ts.startswith("date32") or ts.startswith("date64"):
            return "DATE"

        # timestamp[us], timestamp[us, tz=UTC], etc.
        if ts.startswith("timestamp"):
            if "tz=" in ts:
                return "DATETIMEOFFSET"
            return "DATETIME2"

        # time32[ms], time64[us]
        if ts.startswith("time32") or ts.startswith("time64"):
            return "TIME"

        # duration[us]
        if ts.startswith("duration"):
            return "TIME"

        # decimal128(p, s) -> DECIMAL(p, s)
        if ts.startswith("decimal128"):
            params = ts[len("decimal128") :]
            return f"DECIMAL{params}"

        # Fallback: pass through
        return ts
