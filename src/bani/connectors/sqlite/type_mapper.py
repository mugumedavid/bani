"""Type mapping between SQLite and Arrow types.

SQLite uses a type affinity system with only 5 storage classes:
NULL, INTEGER, REAL, TEXT, BLOB. However, it accepts any type name
in column declarations. This mapper handles both directions:

- SQLite declared type → Arrow type (for introspection)
- Arrow type string → SQLite DDL type (for sink/create_table)
"""

from __future__ import annotations

import re
from typing import Any, ClassVar

import pyarrow as pa


class SQLiteTypeMapper:
    """Maps SQLite type affinities to Arrow types and vice versa.

    Follows SQLite's type affinity rules from the official documentation:
    https://www.sqlite.org/datatype3.html#type_affinity

    Affinity is determined by the declared type name:
    1. INTEGER: type name contains "INT"
    2. TEXT: type name contains "CHAR", "CLOB", or "TEXT"
    3. BLOB: type name is "BLOB" or no type specified
    4. REAL: type name contains "REAL", "FLOA", or "DOUB"
    5. NUMERIC: otherwise (includes DECIMAL, BOOLEAN, DATE, DATETIME)
    """

    # Exact match mapping for common declared types → Arrow
    _EXACT_TYPE_MAP: ClassVar[dict[str, pa.DataType]] = {
        # Integer types
        "INTEGER": pa.int64(),
        "INT": pa.int64(),
        "TINYINT": pa.int8(),
        "SMALLINT": pa.int16(),
        "MEDIUMINT": pa.int32(),
        "BIGINT": pa.int64(),
        "INT2": pa.int16(),
        "INT8": pa.int64(),
        # Real types
        "REAL": pa.float64(),
        "DOUBLE": pa.float64(),
        "DOUBLE PRECISION": pa.float64(),
        "FLOAT": pa.float64(),
        # Text types
        "TEXT": pa.string(),
        "CLOB": pa.string(),
        "CHARACTER": pa.string(),
        "VARCHAR": pa.string(),
        "NCHAR": pa.string(),
        "NVARCHAR": pa.string(),
        "VARYING CHARACTER": pa.string(),
        "NATIVE CHARACTER": pa.string(),
        # Blob
        "BLOB": pa.binary(),
        # Boolean (stored as INTEGER 0/1 in SQLite)
        "BOOLEAN": pa.bool_(),
        "BOOL": pa.bool_(),
        # Date/Time (stored as TEXT in SQLite, ISO 8601)
        "DATE": pa.date32(),
        "DATETIME": pa.timestamp("us"),
        "TIMESTAMP": pa.timestamp("us"),
        # Numeric
        "NUMERIC": pa.decimal128(38, 10),
        "DECIMAL": pa.decimal128(38, 10),
    }

    def map_sqlite_type_name(self, type_name: str) -> pa.DataType:
        """Map a SQLite declared type name to an Arrow type.

        Uses SQLite's type affinity rules to determine the best Arrow type.

        Args:
            type_name: The declared column type (e.g., "INTEGER",
                "VARCHAR(255)", "BOOLEAN").

        Returns:
            Corresponding Arrow data type.
        """
        if not type_name:
            # No type declared → BLOB affinity per SQLite rules
            return pa.binary()

        type_upper = type_name.upper().strip()

        # Strip parameters like (255) or (10,2)
        base_type = re.sub(r"\(.*\)", "", type_upper).strip()

        # Try exact match first
        if base_type in self._EXACT_TYPE_MAP:
            return self._EXACT_TYPE_MAP[base_type]

        # Apply SQLite type affinity rules (order matters!)
        # Rule 1: INTEGER affinity
        if "INT" in type_upper:
            return pa.int64()

        # Rule 2: TEXT affinity
        if any(kw in type_upper for kw in ("CHAR", "CLOB", "TEXT")):
            return pa.string()

        # Rule 3: BLOB affinity (only if exactly "BLOB" or empty)
        if type_upper == "BLOB":
            return pa.binary()

        # Rule 4: REAL affinity
        if any(kw in type_upper for kw in ("REAL", "FLOA", "DOUB")):
            return pa.float64()

        # Rule 5: NUMERIC affinity (catch-all)
        return pa.string()

    def coerce_value(self, value: Any, arrow_type: pa.DataType) -> Any:
        """Coerce a Python value to be compatible with the target Arrow type.

        Handles SQLite-specific quirks like booleans stored as integers
        and dates stored as text.

        Args:
            value: The raw Python value from sqlite3.
            arrow_type: The target Arrow data type.

        Returns:
            A value compatible with the target Arrow type.
        """
        if value is None:
            return None

        # Boolean coercion (SQLite stores as INTEGER 0/1)
        if pa.types.is_boolean(arrow_type):
            return bool(value)

        # Date coercion (SQLite stores as TEXT ISO 8601)
        if pa.types.is_date(arrow_type) and isinstance(value, str):
            import datetime

            try:
                return datetime.date.fromisoformat(value)
            except (ValueError, TypeError):
                return None

        # Timestamp coercion
        if pa.types.is_timestamp(arrow_type) and isinstance(value, str):
            import datetime

            try:
                return datetime.datetime.fromisoformat(value)
            except (ValueError, TypeError):
                return None

        # Decimal coercion
        if pa.types.is_decimal(arrow_type):
            import decimal as decimal_mod

            if not isinstance(value, decimal_mod.Decimal):
                return decimal_mod.Decimal(str(value))

        return value

    # ------------------------------------------------------------------
    # Arrow → SQLite DDL mapping  (used by create_table in the sink)
    # ------------------------------------------------------------------

    @staticmethod
    def from_arrow_type(arrow_type_str: str) -> str:
        """Convert a canonical Arrow type string to a SQLite DDL type.

        Counterpart of ``map_sqlite_type_name``.  Every sink connector
        implements one of these so that N connectors need only N mappers
        (not NxN cross-database translation tables).

        Args:
            arrow_type_str: Arrow type string as produced by
                ``str(pa_type)`` — e.g. ``"int32"``, ``"timestamp[us]"``,
                ``"decimal128(38, 10)"``, ``"string"``.

        Returns:
            A SQLite DDL type string such as ``"INTEGER"`` or ``"TEXT"``.
        """
        _ARROW_TO_SQLITE: dict[str, str] = {
            # Boolean
            "bool": "BOOLEAN",
            # Integer types — all map to INTEGER in SQLite
            "int8": "INTEGER",
            "int16": "INTEGER",
            "int32": "INTEGER",
            "int64": "INTEGER",
            "uint8": "INTEGER",
            "uint16": "INTEGER",
            "uint32": "INTEGER",
            "uint64": "INTEGER",
            # Floating-point — PyArrow str() forms
            "float": "REAL",
            "double": "REAL",
            # Floating-point — explicit aliases
            "float16": "REAL",
            "float32": "REAL",
            "float64": "REAL",
            "halffloat": "REAL",
            # String / binary
            "string": "TEXT",
            "utf8": "TEXT",
            "large_string": "TEXT",
            "large_utf8": "TEXT",
            "binary": "BLOB",
            "large_binary": "BLOB",
            # Null
            "null": "TEXT",
        }

        ts = arrow_type_str.strip()

        # Fast exact match
        if ts in _ARROW_TO_SQLITE:
            return _ARROW_TO_SQLITE[ts]

        # date32[day], date64[ms]
        if ts.startswith("date32") or ts.startswith("date64"):
            return "TEXT"

        # timestamp[us], timestamp[us, tz=UTC]
        if ts.startswith("timestamp"):
            return "TEXT"

        # time32[ms], time64[us]
        if ts.startswith("time32") or ts.startswith("time64"):
            return "TEXT"

        # duration[us]
        if ts.startswith("duration"):
            return "TEXT"

        # decimal128(p, s) -> NUMERIC
        if ts.startswith("decimal128"):
            return "NUMERIC"

        # Fallback
        return "TEXT"
