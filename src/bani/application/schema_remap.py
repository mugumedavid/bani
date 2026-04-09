"""Cross-dialect schema remapping for heterogeneous migrations.

When migrating between different database dialects (e.g. PostgreSQL to MSSQL),
schema-level differences must be reconciled before target table creation:

- Schema namespaces differ (``public`` in PG, ``dbo`` in MSSQL, ``main`` in SQLite).
- CHECK constraints use dialect-specific syntax (PG casts ``::text``, regex ``~``).
- Foreign key references embed the schema name and must be updated.

``SchemaRemapper`` encapsulates these transformations so that
``MigrationOrchestrator.execute()`` can apply them automatically.
"""

from __future__ import annotations

from dataclasses import replace as dc_replace
from typing import ClassVar

from bani.domain.schema import DatabaseSchema, TableDefinition


class SchemaRemapper:
    """Remap a ``DatabaseSchema`` for cross-dialect compatibility.

    This is a stateless utility class — all methods are static.
    """

    _DEFAULT_SCHEMAS: ClassVar[dict[str, str]] = {
        "postgresql": "public",
        "mysql": "",
        "mssql": "dbo",
        "oracle": "",
        "sqlite": "main",
    }

    @staticmethod
    def remap_schema(
        schema: DatabaseSchema,
        source_dialect: str,
        target_dialect: str,
        target_schema: str | None = None,
    ) -> DatabaseSchema:
        """Remap schema for cross-dialect compatibility.

        Args:
            schema: The source ``DatabaseSchema`` as introspected.
            source_dialect: Dialect of the source database
                (e.g. ``"postgresql"``).
            target_dialect: Dialect of the target database
                (e.g. ``"mssql"``).
            target_schema: Explicit target schema name override.
                When ``None``, the default schema for *target_dialect*
                is used.

        Returns:
            A new ``DatabaseSchema`` with remapped table definitions.
        """
        resolved_target = (
            target_schema
            if target_schema is not None
            else SchemaRemapper._DEFAULT_SCHEMAS.get(target_dialect, "")
        )

        source_default = SchemaRemapper._DEFAULT_SCHEMAS.get(source_dialect, "")

        # Determine if remapping is needed: skip when schemas already match
        needs_remap = resolved_target != source_default

        is_cross_dialect = source_dialect != target_dialect

        tables: list[TableDefinition] = []
        for table in schema.tables:
            # Filter empty tables
            if not table.columns:
                continue

            new_schema_name = resolved_target if needs_remap else table.schema_name

            # Remap FK references
            remapped_fks = tuple(
                dc_replace(
                    fk,
                    source_table=SchemaRemapper._remap_fqn(
                        fk.source_table, resolved_target
                    ),
                    referenced_table=SchemaRemapper._remap_fqn(
                        fk.referenced_table, resolved_target
                    ),
                )
                for fk in table.foreign_keys
            ) if needs_remap else table.foreign_keys

            # Strip check constraints for cross-dialect migrations
            check_constraints = (
                () if is_cross_dialect else table.check_constraints
            )

            tables.append(
                dc_replace(
                    table,
                    schema_name=new_schema_name,
                    foreign_keys=remapped_fks,
                    check_constraints=check_constraints,
                )
            )

        return DatabaseSchema(
            tables=tuple(tables),
            source_dialect=schema.source_dialect,
        )

    @staticmethod
    def _remap_fqn(fqn: str, target_schema: str) -> str:
        """Replace the schema portion of a fully qualified table name.

        Args:
            fqn: Original fully qualified name (e.g. ``"public.users"``).
            target_schema: New schema name (e.g. ``"dbo"``).

        Returns:
            Remapped name (e.g. ``"dbo.users"``).
        """
        table_part = fqn.split(".")[-1]
        return f"{target_schema}.{table_part}"
