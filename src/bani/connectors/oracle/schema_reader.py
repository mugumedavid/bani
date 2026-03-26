"""Oracle schema introspection reader.

Queries Oracle data dictionary views (ALL_TAB_COLUMNS, ALL_CONSTRAINTS,
ALL_IND_COLUMNS, etc.) to build a complete picture of the database schema,
including tables, columns, indexes, constraints, and foreign keys.
"""

from __future__ import annotations

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

    def _read_tables(self) -> list[TableDefinition]:
        """Read all user tables and their metadata.

        Returns:
            List of TableDefinition objects.
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
            tables_list: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        tables = []
        for (table_name,) in tables_list:
            columns = self._read_columns(table_name)
            primary_key = self._read_primary_key(table_name)
            indexes = self._read_indexes(table_name)
            foreign_keys = self._read_foreign_keys(table_name)
            row_estimate = self._estimate_row_count(table_name)

            table_def = TableDefinition(
                schema_name=self.owner,
                table_name=table_name,
                columns=tuple(columns),
                primary_key=tuple(primary_key),
                indexes=tuple(indexes),
                foreign_keys=tuple(foreign_keys),
                check_constraints=(),  # Oracle check constraints could be added
                row_count_estimate=row_estimate,
            )
            tables.append(table_def)

        return tables

    def _read_columns(self, table_name: str) -> list[ColumnDefinition]:
        """Read all columns for a table.

        Returns:
            List of ColumnDefinition objects.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT
                    column_name,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    column_id,
                    data_default
                FROM all_tab_columns
                WHERE owner = :owner AND table_name = :table_name
                ORDER BY column_id
            """
            cursor.execute(query, {"owner": self.owner, "table_name": table_name})
            rows: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        columns = []
        for (
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

            col_def = ColumnDefinition(
                name=col_name,
                data_type=type_str,
                nullable=(nullable == "Y"),
                default_value=clean_default,
                is_auto_increment=is_auto,
                ordinal_position=int(column_id) - 1,  # Convert to 0-based
                arrow_type_str=arrow_type_str,
            )
            columns.append(col_def)

        return columns

    def _read_primary_key(self, table_name: str) -> list[str]:
        """Read the primary key columns for a table.

        Returns:
            List of column names in primary key order.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT acc.column_name
                FROM all_constraints ac
                JOIN all_cons_columns acc
                    ON ac.owner = acc.owner
                    AND ac.constraint_name = acc.constraint_name
                WHERE ac.owner = :owner
                AND ac.table_name = :table_name
                AND ac.constraint_type = 'P'
                ORDER BY acc.position
            """
            cursor.execute(query, {"owner": self.owner, "table_name": table_name})
            rows: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        return [row[0] for row in rows]

    def _read_indexes(self, table_name: str) -> list[IndexDefinition]:
        """Read all indexes on a table (excluding primary key).

        Returns:
            List of IndexDefinition objects.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT ai.index_name, ai.uniqueness
                FROM all_indexes ai
                WHERE ai.table_owner = :owner
                AND ai.table_name = :table_name
                AND ai.index_type = 'NORMAL'
                ORDER BY ai.index_name
            """
            cursor.execute(query, {"owner": self.owner, "table_name": table_name})
            indexes_list: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        indexes = []
        for index_name, uniqueness in indexes_list:
            # Get columns in the index
            cursor = self.connection.cursor()
            try:
                col_query = """
                    SELECT column_name
                    FROM all_ind_columns
                    WHERE index_owner = :owner
                    AND index_name = :index_name
                    ORDER BY column_position
                """
                cursor.execute(
                    col_query, {"owner": self.owner, "index_name": index_name}
                )
                col_rows: list[tuple[Any, ...]] = list(cursor.fetchall())
            finally:
                cursor.close()

            col_names = [row[0] for row in col_rows]

            idx_def = IndexDefinition(
                name=index_name,
                columns=tuple(col_names),
                is_unique=(uniqueness == "UNIQUE"),
                is_clustered=False,
                filter_expression=None,
            )
            indexes.append(idx_def)

        return indexes

    def _read_foreign_keys(self, table_name: str) -> list[ForeignKeyDefinition]:
        """Read all foreign keys for a table.

        Returns:
            List of ForeignKeyDefinition objects.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT
                    ac.constraint_name,
                    ac.owner,
                    ac.table_name,
                    ac.r_owner,
                    ac.r_constraint_name,
                    ac.delete_rule
                FROM all_constraints ac
                WHERE ac.owner = :owner
                AND ac.table_name = :table_name
                AND ac.constraint_type = 'R'
                ORDER BY ac.constraint_name
            """
            cursor.execute(query, {"owner": self.owner, "table_name": table_name})
            fks_list: list[tuple[Any, ...]] = list(cursor.fetchall())
        finally:
            cursor.close()

        fks = []
        for (
            fk_name,
            src_owner,
            src_table,
            ref_owner,
            ref_constraint_name,
            delete_rule,
        ) in fks_list:
            # Get source columns
            cursor = self.connection.cursor()
            try:
                src_col_query = """
                    SELECT column_name
                    FROM all_cons_columns
                    WHERE owner = :owner
                    AND constraint_name = :fk_name
                    ORDER BY position
                """
                cursor.execute(src_col_query, {"owner": src_owner, "fk_name": fk_name})
                src_col_rows: list[tuple[Any, ...]] = list(cursor.fetchall())
            finally:
                cursor.close()

            src_cols = [row[0] for row in src_col_rows]

            # Get referenced table and columns
            cursor = self.connection.cursor()
            try:
                ref_query = """
                    SELECT table_name
                    FROM all_constraints
                    WHERE owner = :owner
                    AND constraint_name = :ref_constraint_name
                """
                cursor.execute(
                    ref_query,
                    {"owner": ref_owner, "ref_constraint_name": ref_constraint_name},
                )
                ref_row: tuple[Any, ...] | None = cursor.fetchone()
                ref_table = ref_row[0] if ref_row else "UNKNOWN"
            finally:
                cursor.close()

            # Get referenced columns
            cursor = self.connection.cursor()
            try:
                ref_col_query = """
                    SELECT column_name
                    FROM all_cons_columns
                    WHERE owner = :owner
                    AND constraint_name = :ref_constraint_name
                    ORDER BY position
                """
                cursor.execute(
                    ref_col_query,
                    {"owner": ref_owner, "ref_constraint_name": ref_constraint_name},
                )
                ref_col_rows: list[tuple[Any, ...]] = list(cursor.fetchall())
            finally:
                cursor.close()

            ref_cols = [row[0] for row in ref_col_rows]

            fk_def = ForeignKeyDefinition(
                name=fk_name,
                source_table=f"{src_owner}.{src_table}",
                source_columns=tuple(src_cols),
                referenced_table=f"{ref_owner}.{ref_table}",
                referenced_columns=tuple(ref_cols),
                on_delete=self._normalize_delete_rule(delete_rule),
                on_update="NO ACTION",  # Oracle doesn't support ON UPDATE
            )
            fks.append(fk_def)

        return fks

    def _estimate_row_count(self, table_name: str) -> int | None:
        """Get an estimated row count for a table.

        Uses user_tables.num_rows if available, otherwise NULL.

        Args:
            table_name: Name of the table.

        Returns:
            Estimated row count or None.
        """
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT num_rows
                FROM all_tables
                WHERE owner = :owner
                AND table_name = :table_name
            """
            cursor.execute(query, {"owner": self.owner, "table_name": table_name})
            result: tuple[Any, ...] | None = cursor.fetchone()

            if result and result[0] is not None:
                return int(result[0])
        finally:
            cursor.close()

        return None

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
