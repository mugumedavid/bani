"""PostgreSQL schema introspection reader.

Queries pg_catalog to build a complete picture of the database schema,
including tables, columns, indexes, constraints, and foreign keys.

All metadata is fetched in bulk (7 queries total, regardless of table
count) and then assembled in Python — eliminating the N+1 round-trips
that made the previous implementation slow on large databases.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg

from bani.connectors.postgresql.type_mapper import PostgreSQLTypeMapper
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)

# Type alias for the (schema, table) key used to group rows in Python.
_TableKey = tuple[str, str]


class PostgreSQLSchemaReader:
    """Introspects PostgreSQL schema using pg_catalog.

    All metadata is fetched in bulk queries and assembled in Python.
    """

    def __init__(self, connection: psycopg.Connection[tuple[str, ...]]) -> None:
        """Initialize the schema reader.

        Args:
            connection: An active psycopg connection.
        """
        self.connection = connection
        self._type_mapper = PostgreSQLTypeMapper()

    def read_schema(self) -> DatabaseSchema:
        """Introspect the complete schema and return a DatabaseSchema.

        Returns:
            A DatabaseSchema with all tables and their metadata.

        Raises:
            Exception: If any query fails.
        """
        tables = self._read_tables()
        return DatabaseSchema(tables=tuple(tables), source_dialect="postgresql")

    # ------------------------------------------------------------------
    # Bulk readers — one query each, across *all* user tables
    # ------------------------------------------------------------------

    def _read_tables(self) -> list[TableDefinition]:
        """Read all user tables and their metadata using bulk queries.

        Returns:
            List of TableDefinition objects.
        """
        table_keys = self._fetch_table_list()
        if not table_keys:
            return []

        columns_map = self._fetch_all_columns()
        pk_map = self._fetch_all_primary_keys()
        idx_map = self._fetch_all_indexes()
        fk_map = self._fetch_all_foreign_keys()
        chk_map = self._fetch_all_check_constraints()
        rowcount_map = self._fetch_all_row_counts()

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
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT n.nspname, c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY n.nspname, c.relname
            """)
            return [(r[0], r[1]) for r in cur.fetchall()]

    # 2. Columns (with full type via format_type) --------------------------

    def _fetch_all_columns(self) -> dict[_TableKey, list[ColumnDefinition]]:
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    n.nspname,
                    c.relname,
                    a.attname,
                    format_type(a.atttypid, a.atttypmod) AS full_type,
                    NOT a.attnotnull                      AS is_nullable,
                    pg_get_expr(d.adbin, d.adrelid)       AS col_default,
                    a.attnum
                FROM pg_attribute a
                JOIN pg_class c     ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid
                                      AND d.adnum   = a.attnum
                WHERE c.relkind = 'r'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY n.nspname, c.relname, a.attnum
            """)
            rows = cur.fetchall()

        result: dict[_TableKey, list[ColumnDefinition]] = defaultdict(list)
        for (
            schema_name,
            table_name,
            col_name,
            full_type,
            is_nullable,
            column_default,
            ordinal_pos,
        ) in rows:
            is_auto = self._is_auto_increment(full_type) or (
                column_default is not None and "nextval(" in str(column_default)
            )

            arrow_type = self._type_mapper.map_pg_type_name(full_type)
            arrow_type_str = str(arrow_type)

            clean_default: str | None = column_default
            if is_auto and clean_default and "nextval(" in str(clean_default):
                clean_default = None

            result[(schema_name, table_name)].append(
                ColumnDefinition(
                    name=col_name,
                    data_type=full_type,
                    nullable=bool(is_nullable),
                    default_value=clean_default,
                    is_auto_increment=is_auto,
                    ordinal_position=int(ordinal_pos) - 1,
                    arrow_type_str=arrow_type_str,
                )
            )
        return dict(result)

    # 3. Primary keys ------------------------------------------------------

    def _fetch_all_primary_keys(self) -> dict[_TableKey, list[str]]:
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT n.nspname, t.relname, a.attname
                FROM pg_index i
                JOIN pg_class t     ON t.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                                   AND a.attnum = ANY(i.indkey)
                WHERE i.indisprimary
                  AND t.relkind = 'r'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY n.nspname, t.relname, a.attnum
            """)
            rows = cur.fetchall()

        result: dict[_TableKey, list[str]] = defaultdict(list)
        for schema_name, table_name, col_name in rows:
            result[(schema_name, table_name)].append(col_name)
        return dict(result)

    # 4. Indexes (with inline filter expressions) --------------------------

    def _fetch_all_indexes(self) -> dict[_TableKey, list[IndexDefinition]]:
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    n.nspname,
                    t.relname,
                    i.relname                            AS idx_name,
                    ix.indisunique,
                    ix.indisclustered,
                    pg_get_expr(ix.indpred, ix.indrelid) AS filter_expr,
                    array_agg(a.attname ORDER BY a.attnum)
                FROM pg_index ix
                JOIN pg_class i     ON i.oid = ix.indexrelid
                JOIN pg_class t     ON t.oid = ix.indrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_attribute a ON a.attrelid = t.oid
                                   AND a.attnum = ANY(ix.indkey)
                WHERE NOT ix.indisprimary
                  AND t.relkind = 'r'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                GROUP BY n.nspname, t.relname, i.relname,
                         ix.indisunique, ix.indisclustered, ix.indpred,
                         ix.indrelid
                ORDER BY n.nspname, t.relname, i.relname
            """)
            rows = cur.fetchall()

        result: dict[_TableKey, list[IndexDefinition]] = defaultdict(list)
        for (
            schema_name,
            table_name,
            idx_name,
            is_unique,
            is_clustered,
            filter_expr,
            col_names,
        ) in rows:
            result[(schema_name, table_name)].append(
                IndexDefinition(
                    name=idx_name,
                    columns=self._parse_pg_array(col_names),
                    is_unique=bool(is_unique),
                    is_clustered=bool(is_clustered),
                    filter_expression=filter_expr,
                )
            )
        return dict(result)

    # 5. Foreign keys ------------------------------------------------------

    def _fetch_all_foreign_keys(
        self,
    ) -> dict[_TableKey, list[ForeignKeyDefinition]]:
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    sn.nspname  AS src_schema,
                    sc.relname  AS src_table,
                    con.conname AS fk_name,
                    array_agg(sa.attname ORDER BY u.ord) AS src_cols,
                    rn.nspname  AS ref_schema,
                    rc.relname  AS ref_table,
                    array_agg(ra.attname ORDER BY u.ord) AS ref_cols,
                    CASE con.confupdtype
                        WHEN 'a' THEN 'NO ACTION'
                        WHEN 'r' THEN 'RESTRICT'
                        WHEN 'c' THEN 'CASCADE'
                        WHEN 'n' THEN 'SET NULL'
                        WHEN 'd' THEN 'SET DEFAULT'
                        ELSE 'NO ACTION'
                    END AS update_rule,
                    CASE con.confdeltype
                        WHEN 'a' THEN 'NO ACTION'
                        WHEN 'r' THEN 'RESTRICT'
                        WHEN 'c' THEN 'CASCADE'
                        WHEN 'n' THEN 'SET NULL'
                        WHEN 'd' THEN 'SET DEFAULT'
                        ELSE 'NO ACTION'
                    END AS delete_rule
                FROM pg_constraint con
                JOIN pg_class sc       ON sc.oid = con.conrelid
                JOIN pg_namespace sn   ON sn.oid = sc.relnamespace
                JOIN pg_class rc       ON rc.oid = con.confrelid
                JOIN pg_namespace rn   ON rn.oid = rc.relnamespace
                CROSS JOIN LATERAL unnest(con.conkey, con.confkey)
                    WITH ORDINALITY AS u(src_attnum, ref_attnum, ord)
                JOIN pg_attribute sa   ON sa.attrelid = con.conrelid
                                      AND sa.attnum   = u.src_attnum
                JOIN pg_attribute ra   ON ra.attrelid = con.confrelid
                                      AND ra.attnum   = u.ref_attnum
                WHERE con.contype = 'f'
                  AND sn.nspname NOT IN ('pg_catalog', 'information_schema')
                GROUP BY sn.nspname, sc.relname, con.conname,
                         rn.nspname, rc.relname,
                         con.confupdtype, con.confdeltype
                ORDER BY sn.nspname, sc.relname, con.conname
            """)
            rows = cur.fetchall()

        result: dict[_TableKey, list[ForeignKeyDefinition]] = defaultdict(list)
        for (
            src_schema,
            src_table,
            fk_name,
            src_cols,
            ref_schema,
            ref_table,
            ref_cols,
            update_rule,
            delete_rule,
        ) in rows:
            result[(src_schema, src_table)].append(
                ForeignKeyDefinition(
                    name=fk_name,
                    source_table=f"{src_schema}.{src_table}",
                    source_columns=self._parse_pg_array(src_cols),
                    referenced_table=f"{ref_schema}.{ref_table}",
                    referenced_columns=self._parse_pg_array(ref_cols),
                    on_delete=delete_rule,
                    on_update=update_rule,
                )
            )
        return dict(result)

    # 6. Check constraints -------------------------------------------------

    def _fetch_all_check_constraints(self) -> dict[_TableKey, list[str]]:
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    n.nspname,
                    c.relname,
                    pg_get_constraintdef(con.oid) AS constraint_def
                FROM pg_constraint con
                JOIN pg_class c     ON c.oid = con.conrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE con.contype = 'c'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY n.nspname, c.relname, con.conname
            """)
            rows = cur.fetchall()

        result: dict[_TableKey, list[str]] = defaultdict(list)
        for schema_name, table_name, constraint_def in rows:
            condition = constraint_def
            if condition.startswith("CHECK "):
                condition = condition[6:]
            result[(schema_name, table_name)].append(condition)
        return dict(result)

    # 7. Row count estimates -----------------------------------------------

    def _fetch_all_row_counts(self) -> dict[_TableKey, int | None]:
        with self.connection.cursor() as cur:
            # Use pg_stat_user_tables (n_live_tup) with a fallback to
            # pg_class (reltuples) for tables where stats haven't been
            # collected yet (n_live_tup = 0 after bulk load without ANALYZE).
            cur.execute("""
                SELECT
                    s.schemaname,
                    s.relname,
                    CASE
                        WHEN s.n_live_tup > 0 THEN s.n_live_tup
                        ELSE GREATEST(c.reltuples::bigint, 0)
                    END AS estimated_rows
                FROM pg_stat_user_tables s
                JOIN pg_class c
                    ON c.relname = s.relname
                    AND c.relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = s.schemaname
                    )
                ORDER BY s.schemaname, s.relname
            """)
            rows = cur.fetchall()

        return {(r[0], r[1]): int(r[2]) if r[2] is not None else None for r in rows}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_auto_increment(column_type: str) -> bool:
        """Check if a column type is auto-increment (serial/bigserial)."""
        col_lower = column_type.lower()
        return col_lower in (
            "smallserial",
            "serial",
            "bigserial",
        ) or col_lower.startswith(("smallserial", "serial", "bigserial"))

    @staticmethod
    def _parse_pg_array(value: object) -> tuple[str, ...]:
        """Convert a PostgreSQL array result to a tuple of strings.

        Handles both cases:
        - psycopg returns a Python list (e.g. ['col1', 'col2'])
        - psycopg returns a PG array literal string (e.g. '{col1,col2}')
        """
        if isinstance(value, (list, tuple)):
            return tuple(str(v) for v in value)
        s = str(value).strip()
        if s.startswith("{") and s.endswith("}"):
            s = s[1:-1]
        if not s:
            return ()
        return tuple(c.strip().strip('"') for c in s.split(","))
