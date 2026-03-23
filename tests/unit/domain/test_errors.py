"""Tests for the domain exception hierarchy."""

from __future__ import annotations

import pytest

from bani.domain.errors import (
    BaniConnectionError,
    BaniError,
    BatchError,
    BDLValidationError,
    ConfigurationError,
    ConnectionConfigError,
    DataTransferError,
    DependencyResolutionError,
    HookExecutionError,
    IntrospectionError,
    ReadError,
    SchedulerError,
    SchemaError,
    SchemaTranslationError,
    SourceConnectionError,
    TargetConnectionError,
    TransformError,
    TypeMappingError,
    WriteError,
)


class TestBaniError:
    """Tests for the base BaniError."""

    def test_message(self) -> None:
        err = BaniError("something went wrong")
        assert str(err) == "something went wrong"

    def test_context_kwargs(self) -> None:
        err = BaniError("oops", table="users", row=42)
        assert err.context == {"table": "users", "row": 42}

    def test_empty_context(self) -> None:
        err = BaniError("oops")
        assert err.context == {}


class TestConfigurationErrors:
    """Tests for configuration error subtree."""

    def test_hierarchy(self) -> None:
        assert issubclass(ConfigurationError, BaniError)
        assert issubclass(BDLValidationError, ConfigurationError)
        assert issubclass(ConnectionConfigError, ConfigurationError)
        assert issubclass(TypeMappingError, ConfigurationError)

    def test_bdl_validation_error_fields(self) -> None:
        err = BDLValidationError(
            "invalid element",
            document_path="/tmp/project.bdl",
            line_number=42,
        )
        assert err.document_path == "/tmp/project.bdl"
        assert err.line_number == 42

    def test_connection_config_error_fields(self) -> None:
        err = ConnectionConfigError(
            "missing password_env",
            connection_name="source_mysql",
        )
        assert err.connection_name == "source_mysql"

    def test_type_mapping_error_fields(self) -> None:
        err = TypeMappingError(
            "unmapped type",
            source_type="GEOGRAPHY",
            target_dialect="postgresql",
        )
        assert err.source_type == "GEOGRAPHY"
        assert err.target_dialect == "postgresql"


class TestConnectionErrors:
    """Tests for connection error subtree."""

    def test_hierarchy(self) -> None:
        assert issubclass(BaniConnectionError, BaniError)
        assert issubclass(SourceConnectionError, BaniConnectionError)
        assert issubclass(TargetConnectionError, BaniConnectionError)

    def test_no_shadow_builtin(self) -> None:
        # BaniConnectionError should NOT shadow built-in ConnectionError
        assert BaniConnectionError is not ConnectionError


class TestSchemaErrors:
    """Tests for schema error subtree."""

    def test_hierarchy(self) -> None:
        assert issubclass(SchemaError, BaniError)
        assert issubclass(IntrospectionError, SchemaError)
        assert issubclass(SchemaTranslationError, SchemaError)
        assert issubclass(DependencyResolutionError, SchemaError)

    def test_dependency_resolution_error_tables(self) -> None:
        err = DependencyResolutionError(
            "cycle detected",
            tables=("a", "b", "c"),
        )
        assert err.tables == ("a", "b", "c")


class TestDataTransferErrors:
    """Tests for data-transfer error subtree."""

    def test_hierarchy(self) -> None:
        assert issubclass(DataTransferError, BaniError)
        assert issubclass(ReadError, DataTransferError)
        assert issubclass(WriteError, DataTransferError)
        assert issubclass(BatchError, DataTransferError)
        assert issubclass(TransformError, DataTransferError)

    def test_batch_error_fields(self) -> None:
        err = BatchError(
            "batch failed",
            batch_number=5,
            first_row_offset=50000,
        )
        assert err.batch_number == 5
        assert err.first_row_offset == 50000


class TestOtherErrors:
    """Tests for hook and scheduler errors."""

    def test_hierarchy(self) -> None:
        assert issubclass(HookExecutionError, BaniError)
        assert issubclass(SchedulerError, BaniError)

    def test_all_catchable_as_bani_error(self) -> None:
        """Every domain exception should be catchable as BaniError."""
        exceptions: list[BaniError] = [
            BDLValidationError("x"),
            ConnectionConfigError("x"),
            TypeMappingError("x"),
            SourceConnectionError("x"),
            TargetConnectionError("x"),
            IntrospectionError("x"),
            SchemaTranslationError("x"),
            DependencyResolutionError("x"),
            ReadError("x"),
            WriteError("x"),
            BatchError("x", batch_number=0, first_row_offset=0),
            TransformError("x"),
            HookExecutionError("x"),
            SchedulerError("x"),
        ]
        for exc in exceptions:
            with pytest.raises(BaniError):
                raise exc
