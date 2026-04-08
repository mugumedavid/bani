"""Type mapping between Oracle and Arrow types."""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa


class OracleTypeMapper:
    """Maps Oracle type names to Arrow types.

    Handles Oracle-specific quirks like NUMBER without precision,
    VARCHAR2 vs NVARCHAR2, TIMESTAMP WITH TIME ZONE variants, and
    deprecated LONG/LONG RAW types.
    """

    # Oracle type name -> Arrow type mapping
    _ORACLE_TYPE_MAP: ClassVar[dict[str, pa.DataType]] = {
        # Numeric types
        "NUMBER": pa.decimal128(38, 10),  # Default precision for NUMBER without params
        "FLOAT": pa.float64(),
        "INTEGER": pa.int64(),
        "INT": pa.int64(),
        "SMALLINT": pa.int32(),
        "BINARY_FLOAT": pa.float32(),
        "BINARY_DOUBLE": pa.float64(),
        # String types
        "VARCHAR2": pa.string(),
        "NVARCHAR2": pa.string(),
        "CHAR": pa.string(),
        "NCHAR": pa.string(),
        "CLOB": pa.string(),
        "NCLOB": pa.string(),
        "VARCHAR": pa.string(),
        # Binary types
        "RAW": pa.binary(),
        "BLOB": pa.binary(),
        "LONG RAW": pa.binary(),  # Deprecated but still used
        # Date/Time types
        "DATE": pa.timestamp("us"),  # Oracle DATE includes time
        "TIMESTAMP": pa.timestamp("us"),
        "TIMESTAMP WITH TIME ZONE": pa.timestamp("us", tz="UTC"),
        "TIMESTAMP WITH LOCAL TIME ZONE": pa.timestamp("us", tz="UTC"),
        # Other types
        "XMLTYPE": pa.string(),
        "LONG": pa.string(),  # Deprecated, but still encountered
    }

    def map_oracle_type_name(self, type_name: str) -> pa.DataType:
        """Map an Oracle type name to an Arrow type.

        Handles NUMBER with and without precision/scale, and other
        Oracle-specific type variants.

        Args:
            type_name: Oracle type name (e.g., "VARCHAR2(100)", "NUMBER(10,2)").

        Returns:
            Corresponding Arrow data type.
        """
        type_upper = type_name.upper().strip()

        # Fast exact match (handles types with parameters)
        if type_upper in self._ORACLE_TYPE_MAP:
            return self._ORACLE_TYPE_MAP[type_upper]

        # Extract base type (everything before the opening paren)
        base_type = type_upper
        if "(" in type_upper:
            base_type = type_upper[: type_upper.index("(")].strip()

        # Special handling for NUMBER with precision/scale
        # (must check before general match)
        if base_type == "NUMBER":
            # NUMBER(p,s) maps to decimal128(p, s)
            # NUMBER(p) with no scale maps to decimal128(p, 0)
            # NUMBER with no params maps to decimal128(38, 10)
            try:
                if "(" in type_upper:
                    start_idx = type_upper.index("(") + 1
                    end_idx = type_upper.index(")")
                    params = type_upper[start_idx:end_idx]
                    parts = [p.strip() for p in params.split(",")]
                    if len(parts) == 2:
                        precision = int(parts[0])
                        scale = int(parts[1])
                    elif len(parts) == 1:
                        precision = int(parts[0])
                        scale = 0
                    else:
                        return pa.decimal128(38, 10)

                    # Scale 0 means integer — use a native int type
                    # to stay compatible with serial/identity PKs.
                    if scale == 0:
                        if precision <= 4:
                            return pa.int16()
                        if precision <= 9:
                            return pa.int32()
                        if precision <= 18:
                            return pa.int64()
                    return pa.decimal128(precision, scale)
            except (ValueError, IndexError):
                pass
            # Default for NUMBER without params
            return pa.decimal128(38, 10)

        # Check for exact base type match (for non-NUMBER types)
        if base_type in self._ORACLE_TYPE_MAP:
            return self._ORACLE_TYPE_MAP[base_type]

        # Fallback: default to string
        return pa.string()

    def coerce_value(self, value: Any, arrow_type: pa.DataType) -> Any:
        """Coerce a Python value to be compatible with the target Arrow type.

        Handles Oracle-specific quirks like cx_Oracle.Variable types.

        Args:
            value: The raw Python value from oracledb.
            arrow_type: The target Arrow data type.

        Returns:
            A value compatible with the target Arrow type.
        """
        if value is None:
            return None

        # Handle datetime conversion
        import datetime

        if isinstance(value, (datetime.date, datetime.datetime)):
            try:
                # Validate the date is representable
                value.isoformat()
            except (ValueError, OverflowError):
                return None

        # Handle Decimal for decimal128
        if pa.types.is_decimal(arrow_type):
            import decimal as decimal_mod

            if not isinstance(value, decimal_mod.Decimal):
                return decimal_mod.Decimal(str(value))

        return value

    # ------------------------------------------------------------------
    # Arrow → Oracle DDL mapping (used by create_table in the sink)
    # ------------------------------------------------------------------

    @staticmethod
    def from_arrow_type(arrow_type_str: str) -> str:
        """Convert a canonical Arrow type string to an Oracle DDL type.

        Counterpart of ``map_oracle_type_name``.
        Every sink connector implements one of these so that N connectors
        need only N mappers (not NxN cross-database translation tables).

        Args:
            arrow_type_str: Arrow type string as produced by
                ``str(pa_type)`` — e.g. ``"int32"``, ``"timestamp[us]"``,
                ``"decimal128(38, 10)"``, ``"string"``.

        Returns:
            An Oracle DDL type string such as ``"NUMBER(10,0)"`` or
            ``"VARCHAR2(2000)"``.
        """
        # Exact-match table. Keys must use the *actual* strings produced by
        # ``str(pa_type)`` — e.g. PyArrow emits ``"float"`` (not ``"float32"``),
        # ``"double"`` (not ``"float64"``).
        _ARROW_TO_ORACLE: dict[str, str] = {
            # Boolean
            "bool": "NUMBER(1,0)",
            # Integer types
            "int8": "NUMBER(3,0)",
            "int16": "NUMBER(5,0)",
            "int32": "NUMBER(10,0)",
            "int64": "NUMBER(19,0)",
            "uint8": "NUMBER(3,0)",
            "uint16": "NUMBER(5,0)",
            "uint32": "NUMBER(10,0)",
            "uint64": "NUMBER(20,0)",
            # Floating-point — PyArrow str() forms
            "float": "BINARY_FLOAT",
            "double": "BINARY_DOUBLE",
            # Floating-point — explicit aliases
            "float16": "BINARY_FLOAT",
            "float32": "BINARY_FLOAT",
            "float64": "BINARY_DOUBLE",
            "halffloat": "BINARY_FLOAT",
            # String / binary
            "string": "CLOB",
            "utf8": "CLOB",
            "large_string": "CLOB",
            "large_utf8": "CLOB",
            "binary": "BLOB",
            "large_binary": "BLOB",
            # Null
            "null": "VARCHAR2(4000)",
        }

        ts = arrow_type_str.strip()

        # Fast exact match
        if ts in _ARROW_TO_ORACLE:
            return _ARROW_TO_ORACLE[ts]

        # date32[day], date64[ms]
        if ts.startswith("date32") or ts.startswith("date64"):
            return "DATE"

        # timestamp[us], timestamp[us, tz=UTC]
        if ts.startswith("timestamp"):
            if "tz=" in ts:
                return "TIMESTAMP WITH TIME ZONE"
            return "TIMESTAMP"

        # time32[ms], time64[us]
        if ts.startswith("time32") or ts.startswith("time64"):
            return "VARCHAR2(20)"

        # duration[us]
        if ts.startswith("duration"):
            return "VARCHAR2(20)"

        # decimal128(p, s) -> NUMBER(p, s)
        if ts.startswith("decimal128"):
            params = ts[len("decimal128") :]
            return f"NUMBER{params}"

        # Fallback: VARCHAR2(4000)
        return "VARCHAR2(4000)"
