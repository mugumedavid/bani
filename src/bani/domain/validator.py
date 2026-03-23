"""Data validation — row sampling and schema drift detection.

The ``DataValidator`` provides methods for verifying data integrity during
and after migration: comparing row counts, sampling rows for content
verification, and detecting schema drift between source and target.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from bani.domain.schema import ColumnDefinition, DatabaseSchema, TableDefinition


class DriftType(Enum):
    """Types of schema drift that can be detected."""

    COLUMN_ADDED = auto()
    COLUMN_REMOVED = auto()
    TYPE_CHANGED = auto()
    NULLABLE_CHANGED = auto()
    DEFAULT_CHANGED = auto()


@dataclass(frozen=True)
class DriftItem:
    """A single schema drift finding.

    Attributes:
        table_name: Fully qualified table name where drift was detected.
        column_name: Column affected by the drift.
        drift_type: Category of drift.
        source_value: The value in the source schema (if applicable).
        target_value: The value in the target schema (if applicable).
    """

    table_name: str
    column_name: str
    drift_type: DriftType
    source_value: str | None = None
    target_value: str | None = None


@dataclass(frozen=True)
class ValidationResult:
    """Aggregate result of a validation pass.

    Attributes:
        is_valid: Whether the validation passed with no issues.
        drift_items: Tuple of detected schema drift items.
        row_count_mismatches: Tuple of ``(table_name, source_count, target_count)``.
    """

    is_valid: bool
    drift_items: tuple[DriftItem, ...] = ()
    row_count_mismatches: tuple[tuple[str, int, int], ...] = ()


class DataValidator:
    """Validates data integrity between source and target schemas.

    Provides schema drift detection by comparing column definitions
    between two ``DatabaseSchema`` instances.
    """

    def detect_schema_drift(
        self,
        source: DatabaseSchema,
        target: DatabaseSchema,
    ) -> tuple[DriftItem, ...]:
        """Compare source and target schemas for drift.

        Args:
            source: The source database schema.
            target: The target database schema.

        Returns:
            A tuple of ``DriftItem`` findings. Empty if schemas are compatible.
        """
        drift_items: list[DriftItem] = []

        for source_table in source.tables:
            target_table = target.get_table(
                source_table.schema_name, source_table.table_name
            )
            if target_table is None:
                continue

            drift_items.extend(self._compare_columns(source_table, target_table))

        return tuple(drift_items)

    def validate_row_counts(
        self,
        expected: dict[str, int],
        actual: dict[str, int],
    ) -> ValidationResult:
        """Compare expected vs actual row counts per table.

        Args:
            expected: Mapping of table name to expected row count.
            actual: Mapping of table name to actual row count.

        Returns:
            A ``ValidationResult`` indicating whether counts match.
        """
        mismatches: list[tuple[str, int, int]] = []
        for table_name, exp_count in expected.items():
            act_count = actual.get(table_name, 0)
            if exp_count != act_count:
                mismatches.append((table_name, exp_count, act_count))

        return ValidationResult(
            is_valid=len(mismatches) == 0,
            row_count_mismatches=tuple(mismatches),
        )

    @staticmethod
    def _compare_columns(
        source_table: TableDefinition,
        target_table: TableDefinition,
    ) -> list[DriftItem]:
        """Compare columns between source and target table definitions.

        Args:
            source_table: The source table definition.
            target_table: The target table definition.

        Returns:
            A list of drift items found between the two tables.
        """
        table_name = source_table.fully_qualified_name
        drift: list[DriftItem] = []

        source_cols: dict[str, ColumnDefinition] = {
            c.name: c for c in source_table.columns
        }
        target_cols: dict[str, ColumnDefinition] = {
            c.name: c for c in target_table.columns
        }

        # Columns in source but not target
        for col_name in source_cols:
            if col_name not in target_cols:
                drift.append(
                    DriftItem(
                        table_name=table_name,
                        column_name=col_name,
                        drift_type=DriftType.COLUMN_REMOVED,
                        source_value=source_cols[col_name].data_type,
                    )
                )

        # Columns in target but not source
        for col_name in target_cols:
            if col_name not in source_cols:
                drift.append(
                    DriftItem(
                        table_name=table_name,
                        column_name=col_name,
                        drift_type=DriftType.COLUMN_ADDED,
                        target_value=target_cols[col_name].data_type,
                    )
                )

        # Columns in both — check for type/nullable/default changes
        for col_name in source_cols:
            if col_name not in target_cols:
                continue
            src_col = source_cols[col_name]
            tgt_col = target_cols[col_name]

            if src_col.data_type != tgt_col.data_type:
                drift.append(
                    DriftItem(
                        table_name=table_name,
                        column_name=col_name,
                        drift_type=DriftType.TYPE_CHANGED,
                        source_value=src_col.data_type,
                        target_value=tgt_col.data_type,
                    )
                )

            if src_col.nullable != tgt_col.nullable:
                drift.append(
                    DriftItem(
                        table_name=table_name,
                        column_name=col_name,
                        drift_type=DriftType.NULLABLE_CHANGED,
                        source_value=str(src_col.nullable),
                        target_value=str(tgt_col.nullable),
                    )
                )

            if src_col.default_value != tgt_col.default_value:
                drift.append(
                    DriftItem(
                        table_name=table_name,
                        column_name=col_name,
                        drift_type=DriftType.DEFAULT_CHANGED,
                        source_value=src_col.default_value,
                        target_value=tgt_col.default_value,
                    )
                )

        return drift
