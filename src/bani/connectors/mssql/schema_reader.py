"""MSSQL schema introspection reader.

Queries sys.* catalog views to build a complete picture of the database
schema, including tables, columns, types, PKs, indexes, FKs, check
constraints, identity columns, and row count estimates.

All metadata is fetched in bulk (7 queries total, regardless of table
count) and then assembled in Python — eliminating the N+1 round-trips
that made the previous implementation slow on large databases.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass  # pymssql typing not needed since we use Any in __init__

from bani.connectors.mssql.type_mapper import MSSQLTypeMapper
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)

# Type alias for the (schema, table) key used to group rows in Python.
_TableKey = tuple[str, str]


_log = logging.getLogger(__name__)


class MSSQLSchemaReader:
    """Introspects MSSQL schema using sys views.

    Handles MSSQL-specific features like identity columns, nvarchar vs varchar,
    datetime2 vs datetime, SCOPE_IDENTITY, and schema-qualified names.

    All metadata is fetched in bulk queries and assembled in Python.
    """

    def __init__(
        self,
        connection: Any,
        database: str,
        reconnect_fn: Any | None = None,
    ) -> None:
        """Initialize the schema reader.

        Args:
            connection: An active pymssql connection.
            database: The database name to introspect.
            reconnect_fn: Optional callable that returns a fresh connection.
        """
        self.connection = connection
        self.database = database
        self._type_mapper = MSSQLTypeMapper()
        self._reconnect_fn = reconnect_fn

    def read_schema(self) -> DatabaseSchema:
        """Introspect the complete schema and return a DatabaseSchema.

        Returns:
            A DatabaseSchema with all tables and their metadata.

        Raises:
            Exception: If any query fails.
        """
        tables = self._read_tables()
        return DatabaseSchema(tables=tuple(tables), source_dialect="mssql")

    # ------------------------------------------------------------------
    # Bulk readers — one query each, across *all* user tables
    # ------------------------------------------------------------------

    def _reconnect(self) -> None:
        """Close and replace the connection if reconnect_fn is available."""
        if self._reconnect_fn is None:
            return
        _log.info("[MSSQL-SCHEMA] reconnecting to reset FreeTDS state")
        try:
            self.connection.close()
        except Exception:
            pass
        self.connection = self._reconnect_fn()

    def _fetch_with_retry(self, method_name: str) -> Any:
        """Call a _fetch_* method with retry on connection failure."""
        method = getattr(self, method_name)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return method()
            except Exception as exc:
                exc_str = str(exc).lower()
                is_conn_error = (
                    "dead" in exc_str
                    or "not connected" in exc_str
                    or "connection" in exc_str
                )
                if is_conn_error and attempt < max_retries - 1 and self._reconnect_fn:
                    _log.warning(
                        "[MSSQL-SCHEMA] %s failed (attempt %d/%d): %s — reconnecting",
                        method_name, attempt + 1, max_retries, exc,
                    )
                    self._reconnect()
                else:
                    raise
        return None  # unreachable

    def _read_tables(self) -> list[TableDefinition]:
        """Read all user tables and their metadata using bulk queries.

        Returns:
            List of TableDefinition objects.
        """
        table_keys = self._fetch_with_retry("_fetch_table_list")
        if not table_keys:
            return []

        columns_map = self._fetch_with_retry("_fetch_all_columns")
        pk_map = self._fetch_with_retry("_fetch_all_primary_keys")
        idx_map = self._fetch_with_retry("_fetch_all_indexes")
        fk_map = self._fetch_with_retry("_fetch_all_foreign_keys")
        chk_map = self._fetch_with_retry("_fetch_all_check_constraints")
        rowcount_map = self._fetch_with_retry("_fetch_all_row_counts")

        tables: list[TableDefinition] = []
        for key in table_keys:
            schema_name, table_name = key
            tables.append(
                TableDefinition(
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=tuple(columns_map.get(key, [])),
                    primary_key=tuple(pk_map.get(key, [])),
                    indexes=tuple(idx_map.get(key, [])),
                    foreign_keys=tuple(fk_map.get(key, [])),
                    check_constraints=tuple(chk_map.get(key, [])),
                    row_count_estimate=rowcount_map.get(key),
                )
            )
        return tables

    # 1. Table list --------------------------------------------------------

    def _fetch_table_list(self) -> list[_TableKey]:
        """Fetch all user table names in one query.

        Returns:
            Ordered list of (schema_name, table_name) tuples.
        """
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    s.name AS table_schema,
                    o.name AS table_name
                FROM sys.objects o
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE o.type = 'U'
                AND o.is_ms_shipped = 0
                ORDER BY s.name, o.name
            """)
            return [(r[0], r[1]) for r in cur.fetchall()]

    # 2. Columns (with full type and identity detection) -------------------

    def _fetch_all_columns(self) -> dict[_TableKey, list[ColumnDefinition]]:
        """Fetch all columns across all user tables in one query.

        Uses sys.columns + sys.types + sys.schemas for full type info,
        including identity detection inline via sys.columns.is_identity.

        Returns:
            Dict mapping (schema, table) to ordered list of ColumnDefinitions.
        """
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    s.name            AS schema_name,
                    o.name            AS table_name,
                    c.name            AS column_name,
                    tp.name           AS data_type,
                    c.is_nullable,
                    dc.definition     AS column_default,
                    c.column_id       AS ordinal_position,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_identity
                FROM sys.columns c
                JOIN sys.objects o   ON c.object_id = o.object_id
                JOIN sys.schemas s   ON o.schema_id = s.schema_id
                JOIN sys.types tp    ON c.user_type_id = tp.user_type_id
                LEFT JOIN sys.default_constraints dc
                    ON dc.parent_object_id = c.object_id
                    AND dc.parent_column_id = c.column_id
                WHERE o.type = 'U'
                  AND o.is_ms_shipped = 0
                ORDER BY s.name, o.name, c.column_id
            """)
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        result: dict[_TableKey, list[ColumnDefinition]] = defaultdict(list)
        for (
            schema_name,
            table_name,
            col_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_pos,
            max_length,
            precision,
            scale,
            is_identity,
        ) in rows:
            # sys.columns.max_length is in bytes; nchar/nvarchar store 2
            # bytes per character, so convert to character length.
            char_max_len: int | None = None
            bt = data_type.lower()
            if bt in (
                "char", "varchar", "binary", "varbinary",
                "nchar", "nvarchar",
            ):
                if max_length == -1:
                    char_max_len = -1  # MAX
                elif bt in ("nchar", "nvarchar"):
                    char_max_len = max_length // 2 if max_length else None
                else:
                    char_max_len = max_length if max_length else None

            numeric_prec: int | None = None
            numeric_scale: int | None = None
            if bt in ("decimal", "numeric"):
                numeric_prec = precision
                numeric_scale = scale

            full_type = self._build_full_type(
                data_type, char_max_len, numeric_prec, numeric_scale
            )
            arrow_type = self._type_mapper.map_mssql_type_name(data_type)
            arrow_type_str = str(arrow_type)

            # MSSQL wraps defaults in parentheses: (getdate()), ('pending').
            # Strip the outer parens so the shared translate_default engine
            # can recognise the expression.
            clean_default = column_default
            if clean_default is not None:
                cd = clean_default.strip()
                while cd.startswith("(") and cd.endswith(")"):
                    cd = cd[1:-1].strip()
                clean_default = cd if cd else None

            result[(schema_name, table_name)].append(
                ColumnDefinition(
                    name=col_name,
                    data_type=full_type,
                    nullable=bool(is_nullable),
                    default_value=clean_default,
                    is_auto_increment=bool(is_identity),
                    ordinal_position=int(ordinal_pos) - 1,
                    arrow_type_str=arrow_type_str,
                )
            )
        return dict(result)

    # 3. Primary keys ------------------------------------------------------

    def _fetch_all_primary_keys(self) -> dict[_TableKey, list[str]]:
        """Fetch all primary key columns across all user tables in one query.

        Returns:
            Dict mapping (schema, table) to ordered list of PK column names.
        """
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    s.name  AS schema_name,
                    o.name  AS table_name,
                    c.name  AS column_name
                FROM sys.indexes i
                JOIN sys.index_columns ic
                    ON i.object_id = ic.object_id
                    AND i.index_id = ic.index_id
                JOIN sys.columns c
                    ON ic.object_id = c.object_id
                    AND ic.column_id = c.column_id
                JOIN sys.objects o ON i.object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE i.is_primary_key = 1
                  AND o.type = 'U'
                  AND o.is_ms_shipped = 0
                ORDER BY s.name, o.name, ic.key_ordinal
            """)
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        result: dict[_TableKey, list[str]] = defaultdict(list)
        for schema_name, table_name, col_name in rows:
            result[(schema_name, table_name)].append(col_name)
        return dict(result)

    # 4. Indexes (with inline columns, excluding PKs) ----------------------

    def _fetch_all_indexes(self) -> dict[_TableKey, list[IndexDefinition]]:
        """Fetch all indexes and their columns across all user tables.

        Excludes primary key indexes. Includes filter expressions.

        Returns:
            Dict mapping (schema, table) to list of IndexDefinitions.
        """
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    s.name            AS schema_name,
                    o.name            AS table_name,
                    i.name            AS index_name,
                    i.is_unique,
                    i.filter_definition,
                    c.name            AS column_name,
                    ic.key_ordinal
                FROM sys.indexes i
                JOIN sys.index_columns ic
                    ON i.object_id = ic.object_id
                    AND i.index_id = ic.index_id
                JOIN sys.columns c
                    ON ic.object_id = c.object_id
                    AND ic.column_id = c.column_id
                JOIN sys.objects o ON i.object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE i.is_primary_key = 0
                  AND i.type > 0
                  AND i.name IS NOT NULL
                  AND o.type = 'U'
                  AND o.is_ms_shipped = 0
                  AND ic.is_included_column = 0
                ORDER BY s.name, o.name, i.name, ic.key_ordinal
            """)
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        # Group rows by (schema, table, index_name) to collect columns.
        _IndexKey = tuple[str, str, str]
        idx_cols: dict[_IndexKey, list[str]] = defaultdict(list)
        idx_meta: dict[_IndexKey, tuple[bool, str | None]] = {}

        for (
            schema_name,
            table_name,
            index_name,
            is_unique,
            filter_def,
            col_name,
            _key_ordinal,
        ) in rows:
            idx_key: _IndexKey = (schema_name, table_name, index_name)
            idx_cols[idx_key].append(col_name)
            if idx_key not in idx_meta:
                idx_meta[idx_key] = (bool(is_unique), filter_def)

        result: dict[_TableKey, list[IndexDefinition]] = defaultdict(list)
        for (schema_name, table_name, index_name), columns in idx_cols.items():
            is_unique, filter_expr = idx_meta[(schema_name, table_name, index_name)]
            result[(schema_name, table_name)].append(
                IndexDefinition(
                    name=index_name,
                    columns=tuple(columns),
                    is_unique=is_unique,
                    is_clustered=False,
                    filter_expression=filter_expr,
                )
            )
        return dict(result)

    # 5. Foreign keys ------------------------------------------------------

    def _fetch_all_foreign_keys(
        self,
    ) -> dict[_TableKey, list[ForeignKeyDefinition]]:
        """Fetch all foreign keys across all user tables in one query.

        Uses sys.foreign_keys + sys.foreign_key_columns + sys.columns.

        Returns:
            Dict mapping (schema, table) to list of ForeignKeyDefinitions.
        """
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    ss.name  AS src_schema,
                    so.name  AS src_table,
                    fk.name  AS fk_name,
                    sc.name  AS src_column,
                    rs.name  AS ref_schema,
                    ro.name  AS ref_table,
                    rc.name  AS ref_column,
                    fk.update_referential_action_desc,
                    fk.delete_referential_action_desc,
                    fkc.constraint_column_id
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc
                    ON fk.object_id = fkc.constraint_object_id
                JOIN sys.objects so  ON fk.parent_object_id = so.object_id
                JOIN sys.schemas ss  ON so.schema_id = ss.schema_id
                JOIN sys.columns sc
                    ON fkc.parent_object_id = sc.object_id
                    AND fkc.parent_column_id = sc.column_id
                JOIN sys.objects ro  ON fk.referenced_object_id = ro.object_id
                JOIN sys.schemas rs  ON ro.schema_id = rs.schema_id
                JOIN sys.columns rc
                    ON fkc.referenced_object_id = rc.object_id
                    AND fkc.referenced_column_id = rc.column_id
                WHERE so.type = 'U'
                  AND so.is_ms_shipped = 0
                ORDER BY ss.name, so.name, fk.name, fkc.constraint_column_id
            """)
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        # Group rows by FK name to collect multi-column FKs.
        _FKKey = tuple[str, str, str]  # (src_schema, src_table, fk_name)
        fk_data: dict[_FKKey, dict[str, Any]] = {}

        for (
            src_schema,
            src_table,
            fk_name,
            src_col,
            ref_schema,
            ref_table,
            ref_col,
            update_action,
            delete_action,
            _col_id,
        ) in rows:
            fk_key: _FKKey = (src_schema, src_table, fk_name)
            if fk_key not in fk_data:
                # MSSQL returns action descriptions like "NO_ACTION",
                # "CASCADE", "SET_NULL", "SET_DEFAULT".  Normalise to
                # the INFORMATION_SCHEMA style with spaces.
                fk_data[fk_key] = {
                    "src_schema": src_schema,
                    "src_table": src_table,
                    "src_cols": [],
                    "ref_schema": ref_schema,
                    "ref_table": ref_table,
                    "ref_cols": [],
                    "update_rule": update_action.replace("_", " "),
                    "delete_rule": delete_action.replace("_", " "),
                }
            fk_data[fk_key]["src_cols"].append(src_col)
            fk_data[fk_key]["ref_cols"].append(ref_col)

        result: dict[_TableKey, list[ForeignKeyDefinition]] = defaultdict(list)
        for (src_schema, src_table, fk_name), info in fk_data.items():
            result[(src_schema, src_table)].append(
                ForeignKeyDefinition(
                    name=fk_name,
                    source_table=f"{info['src_schema']}.{info['src_table']}",
                    source_columns=tuple(info["src_cols"]),
                    referenced_table=f"{info['ref_schema']}.{info['ref_table']}",
                    referenced_columns=tuple(info["ref_cols"]),
                    on_delete=info["delete_rule"],
                    on_update=info["update_rule"],
                )
            )
        return dict(result)

    # 6. Check constraints -------------------------------------------------

    def _fetch_all_check_constraints(self) -> dict[_TableKey, list[str]]:
        """Fetch all CHECK constraints across all user tables in one query.

        Returns:
            Dict mapping (schema, table) to list of constraint expressions.
        """
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    s.name  AS schema_name,
                    o.name  AS table_name,
                    cc.definition
                FROM sys.check_constraints cc
                JOIN sys.objects o ON cc.parent_object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE o.type = 'U'
                  AND o.is_ms_shipped = 0
                ORDER BY s.name, o.name, cc.name
            """)
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        result: dict[_TableKey, list[str]] = defaultdict(list)
        for schema_name, table_name, definition in rows:
            result[(schema_name, table_name)].append(definition)
        return dict(result)

    # 7. Row count estimates -----------------------------------------------

    def _fetch_all_row_counts(self) -> dict[_TableKey, int | None]:
        """Fetch estimated row counts for all user tables in one query.

        Uses sys.dm_db_partition_stats for fast estimates.

        Returns:
            Dict mapping (schema, table) to estimated row count.
        """
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    s.name  AS schema_name,
                    o.name  AS table_name,
                    SUM(p.row_count) AS row_count
                FROM sys.dm_db_partition_stats p
                JOIN sys.objects o ON p.object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE o.type = 'U'
                  AND o.is_ms_shipped = 0
                  AND p.index_id <= 1
                GROUP BY s.name, o.name
                ORDER BY s.name, o.name
            """)
            rows: list[tuple[Any, ...]] = list(cur.fetchall())

        return {
            (r[0], r[1]): int(r[2]) if r[2] is not None else None
            for r in rows
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_full_type(
        base_type: str,
        char_max_len: int | None,
        numeric_prec: int | None,
        numeric_scale: int | None,
    ) -> str:
        """Build a full type string like ``nvarchar(255)`` or ``decimal(10,2)``.

        MSSQL's ``sys.types.name`` returns only the base name.  This
        combines it with length / precision so that downstream sinks can
        recover the constraint.
        """
        bt = base_type.lower()

        # Character / binary types with a length
        if bt in (
            "char", "nchar", "varchar", "nvarchar", "binary", "varbinary",
        ):
            if char_max_len is not None and char_max_len > 0:
                return f"{base_type}({char_max_len})"
            if char_max_len == -1:  # -1 means MAX
                return f"{base_type}(MAX)"
            return base_type

        # Numeric types with precision / scale
        if bt in ("decimal", "numeric") and numeric_prec is not None:
            scale = numeric_scale if numeric_scale is not None else 0
            return f"{base_type}({numeric_prec},{scale})"

        return base_type
