"""Tests for data validator — schema drift and row count validation."""

from __future__ import annotations

from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    TableDefinition,
)
from bani.domain.validator import DataValidator, DriftType


def _make_schema(
    columns: tuple[ColumnDefinition, ...],
    dialect: str = "postgresql",
) -> DatabaseSchema:
    """Helper: create a DatabaseSchema with a single table."""
    return DatabaseSchema(
        tables=(
            TableDefinition(
                schema_name="public",
                table_name="users",
                columns=columns,
            ),
        ),
        source_dialect=dialect,
    )


class TestSchemaDataDrift:
    """Tests for DataValidator.detect_schema_drift."""

    def test_no_drift(self) -> None:
        cols = (
            ColumnDefinition(name="id", data_type="INT"),
            ColumnDefinition(name="name", data_type="VARCHAR(255)"),
        )
        source = _make_schema(cols)
        target = _make_schema(cols)

        validator = DataValidator()
        drift = validator.detect_schema_drift(source, target)
        assert drift == ()

    def test_column_added(self) -> None:
        source_cols = (ColumnDefinition(name="id", data_type="INT"),)
        target_cols = (
            ColumnDefinition(name="id", data_type="INT"),
            ColumnDefinition(name="new_col", data_type="TEXT"),
        )

        validator = DataValidator()
        drift = validator.detect_schema_drift(
            _make_schema(source_cols), _make_schema(target_cols)
        )

        added = [d for d in drift if d.drift_type == DriftType.COLUMN_ADDED]
        assert len(added) == 1
        assert added[0].column_name == "new_col"

    def test_column_removed(self) -> None:
        source_cols = (
            ColumnDefinition(name="id", data_type="INT"),
            ColumnDefinition(name="old_col", data_type="TEXT"),
        )
        target_cols = (ColumnDefinition(name="id", data_type="INT"),)

        validator = DataValidator()
        drift = validator.detect_schema_drift(
            _make_schema(source_cols), _make_schema(target_cols)
        )

        removed = [d for d in drift if d.drift_type == DriftType.COLUMN_REMOVED]
        assert len(removed) == 1
        assert removed[0].column_name == "old_col"

    def test_type_changed(self) -> None:
        source = _make_schema((ColumnDefinition(name="age", data_type="INT"),))
        target = _make_schema((ColumnDefinition(name="age", data_type="BIGINT"),))

        validator = DataValidator()
        drift = validator.detect_schema_drift(source, target)

        changed = [d for d in drift if d.drift_type == DriftType.TYPE_CHANGED]
        assert len(changed) == 1
        assert changed[0].source_value == "INT"
        assert changed[0].target_value == "BIGINT"

    def test_nullable_changed(self) -> None:
        source = _make_schema(
            (ColumnDefinition(name="email", data_type="TEXT", nullable=True),)
        )
        target = _make_schema(
            (ColumnDefinition(name="email", data_type="TEXT", nullable=False),)
        )

        validator = DataValidator()
        drift = validator.detect_schema_drift(source, target)

        changed = [d for d in drift if d.drift_type == DriftType.NULLABLE_CHANGED]
        assert len(changed) == 1

    def test_default_changed(self) -> None:
        source = _make_schema(
            (ColumnDefinition(name="status", data_type="TEXT", default_value="active"),)
        )
        target = _make_schema(
            (
                ColumnDefinition(
                    name="status", data_type="TEXT", default_value="pending"
                ),
            )
        )

        validator = DataValidator()
        drift = validator.detect_schema_drift(source, target)

        changed = [d for d in drift if d.drift_type == DriftType.DEFAULT_CHANGED]
        assert len(changed) == 1


class TestRowCountValidation:
    """Tests for DataValidator.validate_row_counts."""

    def test_counts_match(self) -> None:
        validator = DataValidator()
        result = validator.validate_row_counts(
            expected={"users": 100, "orders": 500},
            actual={"users": 100, "orders": 500},
        )
        assert result.is_valid is True
        assert result.row_count_mismatches == ()

    def test_counts_mismatch(self) -> None:
        validator = DataValidator()
        result = validator.validate_row_counts(
            expected={"users": 100},
            actual={"users": 99},
        )
        assert result.is_valid is False
        assert len(result.row_count_mismatches) == 1
        assert result.row_count_mismatches[0] == ("users", 100, 99)

    def test_missing_table_in_actual(self) -> None:
        validator = DataValidator()
        result = validator.validate_row_counts(
            expected={"users": 100},
            actual={},
        )
        assert result.is_valid is False
        assert result.row_count_mismatches[0] == ("users", 100, 0)
