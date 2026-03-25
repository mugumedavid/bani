"""Type mapping between MySQL and Arrow types."""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa


# MySQL field type constants from PyMySQL/MySQLdb
# These correspond to FIELD_TYPE values in the MySQL C API
class MySQLFieldType:
    """MySQL field type constants matching PyMySQL FIELD_TYPE."""

    DECIMAL: int = 0
    TINY: int = 1
    SHORT: int = 2
    LONG: int = 3
    FLOAT: int = 4
    DOUBLE: int = 5
    NULL: int = 6
    TIMESTAMP: int = 7
    LONGLONG: int = 8
    INT24: int = 9
    DATE: int = 10
    TIME: int = 11
    DATETIME: int = 12
    YEAR: int = 13
    NEWDATE: int = 14
    VARCHAR: int = 15
    BIT: int = 16
    JSON: int = 245
    NEWDECIMAL: int = 246
    ENUM: int = 247
    SET: int = 248
    TINY_BLOB: int = 249
    MEDIUM_BLOB: int = 250
    LONG_BLOB: int = 251
    BLOB: int = 252
    VAR_STRING: int = 253
    STRING: int = 254
    GEOMETRY: int = 255


# MySQL flag constants
UNSIGNED_FLAG: int = 32


class MySQLTypeMapper:
    """Maps MySQL type codes to Arrow types.

    MySQL uses field type codes internally to identify types. This mapper
    provides conversion to Arrow types for efficient in-memory representation.
    Handles MySQL-specific quirks like unsigned integers.
    """

    # MySQL field type code -> Arrow type mapping
    _MYSQL_TYPE_MAP: ClassVar[dict[int, pa.DataType]] = {
        # Numeric types
        MySQLFieldType.TINY: pa.int8(),
        MySQLFieldType.SHORT: pa.int16(),
        MySQLFieldType.LONG: pa.int32(),
        MySQLFieldType.LONGLONG: pa.int64(),
        MySQLFieldType.INT24: pa.int32(),
        MySQLFieldType.FLOAT: pa.float32(),
        MySQLFieldType.DOUBLE: pa.float64(),
        MySQLFieldType.DECIMAL: pa.decimal128(38, 10),
        MySQLFieldType.NEWDECIMAL: pa.decimal128(38, 10),
        # String types
        MySQLFieldType.VARCHAR: pa.string(),
        MySQLFieldType.VAR_STRING: pa.string(),
        MySQLFieldType.STRING: pa.string(),
        MySQLFieldType.ENUM: pa.string(),
        MySQLFieldType.SET: pa.string(),
        # Binary types
        MySQLFieldType.TINY_BLOB: pa.binary(),
        MySQLFieldType.MEDIUM_BLOB: pa.binary(),
        MySQLFieldType.LONG_BLOB: pa.binary(),
        MySQLFieldType.BLOB: pa.binary(),
        # Date/Time types
        MySQLFieldType.DATE: pa.date32(),
        MySQLFieldType.NEWDATE: pa.date32(),
        MySQLFieldType.TIME: pa.time64("us"),
        MySQLFieldType.DATETIME: pa.timestamp("us"),
        MySQLFieldType.TIMESTAMP: pa.timestamp("us", tz="UTC"),
        MySQLFieldType.YEAR: pa.int16(),
        # Other
        MySQLFieldType.BIT: pa.bool_(),
        MySQLFieldType.JSON: pa.string(),
        MySQLFieldType.NULL: pa.null(),
        MySQLFieldType.GEOMETRY: pa.string(),
    }

    # Unsigned variants for integer types (promote to wider types)
    _UNSIGNED_TYPE_MAP: ClassVar[dict[int, pa.DataType]] = {
        MySQLFieldType.TINY: pa.int16(),  # unsigned tinyint -> int16
        MySQLFieldType.SHORT: pa.int32(),  # unsigned smallint -> int32
        MySQLFieldType.LONG: pa.int64(),  # unsigned int -> int64
        MySQLFieldType.INT24: pa.int64(),  # unsigned mediumint -> int64
        MySQLFieldType.LONGLONG: pa.decimal128(20, 0),  # unsigned bigint -> decimal
    }

    def map_mysql_type_code(self, type_code: int, flags: int = 0) -> pa.DataType:
        """Map a MySQL field type code to an Arrow type.

        Args:
            type_code: MySQL field type code from cursor description.
            flags: MySQL column flags (used to detect UNSIGNED).

        Returns:
            Corresponding Arrow data type. Defaults to string if unknown.
        """
        is_unsigned = bool(flags & UNSIGNED_FLAG)

        if is_unsigned and type_code in self._UNSIGNED_TYPE_MAP:
            return self._UNSIGNED_TYPE_MAP[type_code]

        return self._MYSQL_TYPE_MAP.get(type_code, pa.string())

    def map_mysql_type_name(self, type_name: str) -> pa.DataType:
        """Map a MySQL type name to an Arrow type.

        Args:
            type_name: MySQL type name (e.g., "INT", "VARCHAR(255)").

        Returns:
            Corresponding Arrow data type.
        """
        type_upper = type_name.upper().strip()
        is_unsigned = "UNSIGNED" in type_upper

        # Strip UNSIGNED and parameters
        base_type = type_upper.replace("UNSIGNED", "").strip()
        if "(" in base_type:
            base_type = base_type[: base_type.index("(")].strip()

        mapping: dict[str, pa.DataType] = {
            # Numeric types
            "TINYINT": pa.int16() if is_unsigned else pa.int8(),
            "SMALLINT": pa.int32() if is_unsigned else pa.int16(),
            "MEDIUMINT": pa.int64() if is_unsigned else pa.int32(),
            "INT": pa.int64() if is_unsigned else pa.int32(),
            "INTEGER": pa.int64() if is_unsigned else pa.int32(),
            "BIGINT": pa.decimal128(20, 0) if is_unsigned else pa.int64(),
            "FLOAT": pa.float32(),
            "DOUBLE": pa.float64(),
            "DECIMAL": pa.decimal128(38, 10),
            "NUMERIC": pa.decimal128(38, 10),
            # Boolean
            "BIT": pa.bool_(),
            "BOOLEAN": pa.bool_(),
            "BOOL": pa.bool_(),
            # Character types
            "CHAR": pa.string(),
            "VARCHAR": pa.string(),
            "TINYTEXT": pa.string(),
            "TEXT": pa.string(),
            "MEDIUMTEXT": pa.string(),
            "LONGTEXT": pa.string(),
            # Binary types
            "BINARY": pa.binary(),
            "VARBINARY": pa.binary(),
            "TINYBLOB": pa.binary(),
            "BLOB": pa.binary(),
            "MEDIUMBLOB": pa.binary(),
            "LONGBLOB": pa.binary(),
            # Date/Time
            "DATE": pa.date32(),
            "TIME": pa.time64("us"),
            "DATETIME": pa.timestamp("us"),
            "TIMESTAMP": pa.timestamp("us", tz="UTC"),
            "YEAR": pa.int16(),
            # JSON
            "JSON": pa.string(),
            # ENUM/SET
            "ENUM": pa.string(),
            "SET": pa.string(),
            # Spatial
            "GEOMETRY": pa.string(),
            "POINT": pa.string(),
            "LINESTRING": pa.string(),
            "POLYGON": pa.string(),
        }

        return mapping.get(base_type, pa.string())

    def coerce_value(self, value: Any, arrow_type: pa.DataType) -> Any:
        """Coerce a Python value to be compatible with the target Arrow type.

        Handles MySQL-specific quirks like zero dates, timedelta for TIME,
        and bytes for BIT fields.

        Args:
            value: The raw Python value from PyMySQL.
            arrow_type: The target Arrow data type.

        Returns:
            A value compatible with the target Arrow type.
        """
        if value is None:
            return None

        # Handle timedelta from TIME columns
        import datetime

        if isinstance(value, datetime.timedelta):
            # Convert timedelta to microseconds for time64
            total_us = int(value.total_seconds() * 1_000_000)
            return total_us

        # Handle zero dates (MySQL allows '0000-00-00')
        if isinstance(value, (datetime.date, datetime.datetime)):
            try:
                # Validate the date is representable
                value.isoformat()
            except (ValueError, OverflowError):
                return None

        # Handle bytes from BIT columns
        if isinstance(value, (bytes, bytearray)) and pa.types.is_boolean(arrow_type):
            return bool(int.from_bytes(value, byteorder="big"))

        # Handle Decimal for decimal128
        if pa.types.is_decimal(arrow_type):
            import decimal as decimal_mod

            if not isinstance(value, decimal_mod.Decimal):
                return decimal_mod.Decimal(str(value))

        return value

    # ------------------------------------------------------------------
    # Arrow → MySQL DDL mapping  (used by create_table in the sink)
    # ------------------------------------------------------------------

    @staticmethod
    def from_arrow_type(arrow_type_str: str) -> str:
        """Convert a canonical Arrow type string to a MySQL DDL type.

        Counterpart of ``map_mysql_type_name`` / ``map_mysql_type_code``.
        Every sink connector implements one of these so that N connectors
        need only N mappers (not NxN cross-database translation tables).

        Args:
            arrow_type_str: Arrow type string as produced by
                ``str(pa_type)`` — e.g. ``"int32"``, ``"timestamp[us]"``,
                ``"decimal128(38, 10)"``, ``"string"``.

        Returns:
            A MySQL DDL type string such as ``"INT"`` or ``"DATETIME"``.
        """
        # Exact-match table.  Keys must use the *actual* strings
        # produced by ``str(pa_type)`` — e.g. PyArrow emits
        # ``"float"`` (not ``"float32"``), ``"double"`` (not
        # ``"float64"``).  We include both forms.
        _ARROW_TO_MYSQL: dict[str, str] = {
            # Boolean
            "bool": "TINYINT(1)",
            # Integer types
            "int8": "TINYINT",
            "int16": "SMALLINT",
            "int32": "INT",
            "int64": "BIGINT",
            "uint8": "SMALLINT UNSIGNED",
            "uint16": "INT UNSIGNED",
            "uint32": "BIGINT UNSIGNED",
            "uint64": "DECIMAL(20,0) UNSIGNED",
            # Floating-point — PyArrow str() forms
            "float": "FLOAT",
            "double": "DOUBLE",
            # Floating-point — explicit aliases
            "float16": "FLOAT",
            "float32": "FLOAT",
            "float64": "DOUBLE",
            "halffloat": "FLOAT",
            # String / binary
            "string": "TEXT",
            "utf8": "TEXT",
            "large_string": "LONGTEXT",
            "large_utf8": "LONGTEXT",
            "binary": "BLOB",
            "large_binary": "LONGBLOB",
            # Null
            "null": "TEXT",
        }

        ts = arrow_type_str.strip()

        # Fast exact match
        if ts in _ARROW_TO_MYSQL:
            return _ARROW_TO_MYSQL[ts]

        # date32[day], date64[ms]
        if ts.startswith("date32") or ts.startswith("date64"):
            return "DATE"

        # timestamp[us], timestamp[us, tz=UTC]
        if ts.startswith("timestamp"):
            if "tz=" in ts:
                return "TIMESTAMP"
            return "DATETIME"

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
        return arrow_type_str
