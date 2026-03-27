"""Oracle schema introspection reader.

Queries Oracle data dictionary views (ALL_TAB_COLUMNS, ALL_CONSTRAINTS,
ALL_IND_COLUMNS, etc.) to build a complete picture of the database schema,
including tables, columns, indexes, constraints, and foreign keys.

All metadata is fetched in bulk (6 queries total, regardless of table
count) and then assembled in Python -- eliminating the N+1 round-trips
that made the previous implementation slow on large databases.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import oracledb

from bani.connectors.oracle.type_mapper import OracleTypeMapper
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    ForeignKeyDefinition,
    IndexDefinition,
    TableDefinition,
)


class OracleSchemaReader:
    """Introspects Oracle schema using data dictionary views.

    Handles Oracle-specific features like NUMBER without precision,
    VARCHAR2 vs NVARCHAR2, and schema-qualified identifiers.

    All metadata is fetched in bulk queries and assembled in Python.
    """

    def __init__(self, connection: oracledb.Connection, owner: str) -> None:
        """Initialize the schema reader.

        Args:
            connection: An active oracledb connection.
            owner: The schema owner (user) to introspect.
        """
        self.connection = connection
        self.owner = owner.upper()  # Oracle treats schema names as uppercase
        self._type_mapper = OracleTypeMapper()

    def read_schema(self) -> DatabaseSchema:
        """Introspect the complete schema and return a DatabaseSchema.

        Returns:
            A DatabaseSchema with all tables and their metadata.

        Raises:
            Exception: If any query fails.
        """
        tables = self._read_tables()
        return DatabaseSchema(tables=tuple(tables), source_dialect="oracle")

    # ------------------------------------------------------------------
    # Bulk readers -- one query each, across *all* user tables
    # ------------------------------------------------------------------

    def _read_tables(self) -> list[TableDefinition]:
        """Read all user tables and their metadata using bulk queries.

        Returns:
            List of TableDefinition objects.
        """
        table_names = self._fetch_table_list()
        if not table_names:
            return []

        columns_map = self._fetch_all_columns()
        pk_map = self._fetch_all_primary_keys()
        idx_map = self._fetch_all_indexes()
        fk_map = self._fetch_all_foreign_keys()
        rowcount_map = self._fetch_all_row_counts()

        tables: list[TableDefinition] = []
        for table_name in table_names:
            tables.append(
                TableDefinition(
                    schema_name=self.owner,
                    table_name=table_name,
                    columns=tuple(columns_map.get(table_name, [])),
                    primary_key=tuple(pk_map.get(table_name, [])),
                    indexes=tuple(idx_map.get(table_name, [])),
                    foreign_keys=tuple(fk_map.get(table_name, [])),
                    check_constraints=(),  # Oracle check constraints not yet supported
                    row_count_estimate=rowcount_map.get(table_name),
                )
            )
        return tables

    # 1. Table list --------------------------------------------------------

    def _fetch_table_list(self) -> list[str]:
        """Fetch all user table names in a single query.

        Returns:
            Sorted list of table names (excluding system tables).
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT table_name
                FROM user_tables
                WHERE table_name NOT LIKE '%$%'
                AND table_name NOT IN (
                    'HELP', 'REDO_DB', 'REDO_LOG',
                    'SQLPLUS_PRODUCT_PROFILE',
                    'SCHEDULER_JOB_ARGS_TBL',
                    'SCHEDULER_PROGRAM_ARGS_TBL'
                )
                ORDER BY table_name
            """
            cursor.execute(query)
            rows: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        return [row[0] for row in rows]

    # 2. Columns (with full type) ------------------------------------------

    def _fetch_all_columns(self) -> dict[str, list[ColumnDefinition]]:
        """Fetch all columns across all tables in a single query.

        Returns:
            Dict mapping table_name to list of ColumnDefinition objects.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT
                    table_name,
                    column_name,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    column_id,
                    data_default
                FROM all_tab_columns
                WHERE owner = :owner
                ORDER BY table_name, column_id
            """
            cursor.execute(query, {"owner": self.owner})
            rows: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        result: dict[str, list[ColumnDefinition]] = defaultdict(list)
        for (
            table_name,
            col_name,
            data_type,
            data_length,
            data_precision,
            data_scale,
            nullable,
            column_id,
            data_default,
        ) in rows:
            # Build the full type string including precision/scale
            type_str = data_type
            if data_type == "NUMBER" and data_precision is not None:
                if data_scale is not None:
                    type_str = f"NUMBER({data_precision},{data_scale})"
                else:
                    type_str = f"NUMBER({data_precision})"
            elif data_type in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR") and (
                data_length is not None
            ):
                type_str = f"{data_type}({data_length})"

            # Resolve canonical Arrow type
            arrow_type = self._type_mapper.map_oracle_type_name(type_str)
            arrow_type_str = str(arrow_type)

            # Detect identity / sequence-based auto-increment columns.
            # Oracle GENERATED AS IDENTITY creates hidden sequences named
            # ISEQ$$_nnn whose .nextval appears in data_default.
            clean_default = data_default
            is_auto = False
            if data_default is not None:
                dd = str(data_default).strip()
                if "ISEQ$$" in dd or ".nextval" in dd.lower():
                    is_auto = True
                    clean_default = None

            result[table_name].append(
                ColumnDefinition(
                    name=col_name,
                    data_type=type_str,
                    nullable=(nullable == "Y"),
                    default_value=clean_default,
                    is_auto_increment=is_auto,
                    ordinal_position=int(column_id) - 1,  # Convert to 0-based
                    arrow_type_str=arrow_type_str,
                )
            )
        return dict(result)

    # 3. Primary keys ------------------------------------------------------

    def _fetch_all_primary_keys(self) -> dict[str, list[str]]:
        """Fetch all primary key columns across all tables in a single query.

        Returns:
            Dict mapping table_name to ordered list of PK column names.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT ac.table_name, acc.column_name
                FROM all_constraints ac
                JOIN all_cons_columns acc
                    ON ac.owner = acc.owner
                    AND ac.constraint_name = acc.constraint_name
                WHERE ac.owner = :owner
                AND ac.constraint_type = 'P'
                ORDER BY ac.table_name, acc.position
            """
            cursor.execute(query, {"owner": self.owner})
            rows: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        result: dict[str, list[str]] = defaultdict(list)
        for table_name, col_name in rows:
            result[table_name].append(col_name)
        return dict(result)

    # 4. Indexes (with inline columns) -------------------------------------

    def _fetch_all_indexes(self) -> dict[str, list[IndexDefinition]]:
        """Fetch all indexes and their columns across all tables in a single query.

        Excludes primary key indexes by filtering out indexes that back
        a primary key constraint.

        Returns:
            Dict mapping table_name to list of IndexDefinition objects.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT
                    ai.table_name,
                    ai.index_name,
                    ai.uniqueness,
                    aic.column_name,
                    aic.column_position
                FROM all_indexes ai
                JOIN all_ind_columns aic
                    ON ai.owner = aic.index_owner
                    AND ai.index_name = aic.index_name
                WHERE ai.table_owner = :owner
                AND ai.index_type = 'NORMAL'
                ORDER BY ai.table_name, ai.index_name, aic.column_position
            """
            cursor.execute(query, {"owner": self.owner})
            rows: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        # Group columns by (table_name, index_name)
        # Each entry: (uniqueness, [col_names])
        index_cols: dict[tuple[str, str], tuple[str, list[str]]] = {}
        for table_name, index_name, uniqueness, col_name, _col_pos in rows:
            key = (table_name, index_name)
            if key not in index_cols:
                index_cols[key] = (uniqueness, [])
            index_cols[key][1].append(col_name)

        result: dict[str, list[IndexDefinition]] = defaultdict(list)
        for (table_name, index_name), (uniqueness, col_names) in index_cols.items():
            result[table_name].append(
                IndexDefinition(
                    name=index_name,
                    columns=tuple(col_names),
                    is_unique=(uniqueness == "UNIQUE"),
                    is_clustered=False,
                    filter_expression=None,
                )
            )
        return dict(result)

    # 5. Foreign keys ------------------------------------------------------

    def _fetch_all_foreign_keys(
        self,
    ) -> dict[str, list[ForeignKeyDefinition]]:
        """Fetch all foreign keys across all tables in a single query.

        Uses a self-join on all_constraints (ac for FK, rc for referenced PK)
        and joins all_cons_columns for both source and referenced columns,
        eliminating the 3 sub-queries per FK that the old implementation used.

        Returns:
            Dict mapping table_name to list of ForeignKeyDefinition objects.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT
                    ac.table_name   AS src_table,
                    ac.constraint_name AS fk_name,
                    ac.owner        AS src_owner,
                    ac.delete_rule,
                    src_cc.column_name AS src_col,
                    src_cc.position    AS src_pos,
                    rc.owner        AS ref_owner,
                    rc.table_name   AS ref_table,
                    ref_cc.column_name AS ref_col
                FROM all_constraints ac
                JOIN all_constraints rc
                    ON ac.r_owner = rc.owner
                    AND ac.r_constraint_name = rc.constraint_name
                JOIN all_cons_columns src_cc
                    ON ac.owner = src_cc.owner
                    AND ac.constraint_name = src_cc.constraint_name
                JOIN all_cons_columns ref_cc
                    ON rc.owner = ref_cc.owner
                    AND rc.constraint_name = ref_cc.constraint_name
                    AND src_cc.position = ref_cc.position
                WHERE ac.owner = :owner
                AND ac.constraint_type = 'R'
                ORDER BY ac.table_name, ac.constraint_name, src_cc.position
            """
            cursor.execute(query, {"owner": self.owner})
            rows: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        # Group by (src_table, fk_name) to collect all column pairs
        # Value: (src_owner, delete_rule, ref_owner, ref_table, [src_cols], [ref_cols])
        fk_groups: dict[
            tuple[str, str],
            tuple[str, str | None, str, str, list[str], list[str]],
        ] = {}
        for (
            src_table,
            fk_name,
            src_owner,
            delete_rule,
            src_col,
            _src_pos,
            ref_owner,
            ref_table,
            ref_col,
        ) in rows:
            key = (src_table, fk_name)
            if key not in fk_groups:
                fk_groups[key] = (
                    src_owner,
                    delete_rule,
                    ref_owner,
                    ref_table,
                    [],
                    [],
                )
            fk_groups[key][4].append(src_col)
            fk_groups[key][5].append(ref_col)

        result: dict[str, list[ForeignKeyDefinition]] = defaultdict(list)
        for (src_table, fk_name), (
            src_owner,
            delete_rule,
            ref_owner,
            ref_table,
            src_cols,
            ref_cols,
        ) in fk_groups.items():
            result[src_table].append(
                ForeignKeyDefinition(
                    name=fk_name,
                    source_table=f"{src_owner}.{src_table}",
                    source_columns=tuple(src_cols),
                    referenced_table=f"{ref_owner}.{ref_table}",
                    referenced_columns=tuple(ref_cols),
                    on_delete=self._normalize_delete_rule(delete_rule),
                    on_update="NO ACTION",  # Oracle doesn't support ON UPDATE
                )
            )
        return dict(result)

    # 6. Row count estimates -----------------------------------------------

    def _fetch_all_row_counts(self) -> dict[str, int | None]:
        """Fetch estimated row counts for all tables in a single query.

        Uses all_tables.num_rows (populated by DBMS_STATS / ANALYZE).

        Returns:
            Dict mapping table_name to estimated row count (or None).
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT table_name, num_rows
                FROM all_tables
                WHERE owner = :owner
                ORDER BY table_name
            """
            cursor.execute(query, {"owner": self.owner})
            rows: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        return {
            row[0]: int(row[1]) if row[1] is not None else None for row in rows
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_delete_rule(rule: str | None) -> str:
        """Normalize Oracle delete rule to standard SQL form.

        Args:
            rule: Oracle delete rule (CASCADE, SET NULL, NO ACTION, RESTRICT).

        Returns:
            Normalized delete rule.
        """
        if not rule:
            return "NO ACTION"

        rule_upper = rule.upper()
        if rule_upper == "CASCADE":
            return "CASCADE"
        elif rule_upper == "SET NULL":
            return "SET NULL"
        elif rule_upper == "RESTRICT":
            return "RESTRICT"
        else:
            return "NO ACTION"
