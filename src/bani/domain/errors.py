"""Domain-specific exception hierarchy for Bani.

All exceptions inherit from ``BaniError``. Each exception carries structured
context so that callers can programmatically inspect the failure without
parsing message strings.

Hierarchy (Section 12.1)::

    BaniError
    ├── ConfigurationError
    │   ├── BDLValidationError
    │   ├── ConnectionConfigError
    │   └── TypeMappingError
    ├── ConnectionError
    │   ├── SourceConnectionError
    │   └── TargetConnectionError
    ├── SchemaError
    │   ├── IntrospectionError
    │   ├── SchemaTranslationError
    │   └── DependencyResolutionError
    ├── DataTransferError
    │   ├── ReadError
    │   ├── WriteError
    │   ├── BatchError
    │   └── TransformError
    ├── HookExecutionError
    └── SchedulerError
"""

from __future__ import annotations


class BaniError(Exception):
    """Base exception for all Bani errors."""

    def __init__(self, message: str, **context: object) -> None:
        super().__init__(message)
        self.context = context


# ---------------------------------------------------------------------------
# Configuration errors
# ---------------------------------------------------------------------------


class ConfigurationError(BaniError):
    """Error in project or connection configuration."""


class BDLValidationError(ConfigurationError):
    """BDL document failed XSD or semantic validation."""

    def __init__(
        self,
        message: str,
        *,
        document_path: str | None = None,
        line_number: int | None = None,
        **context: object,
    ) -> None:
        super().__init__(
            message,
            document_path=document_path,
            line_number=line_number,
            **context,
        )
        self.document_path = document_path
        self.line_number = line_number


class ConnectionConfigError(ConfigurationError):
    """Invalid or missing connection configuration."""

    def __init__(
        self,
        message: str,
        *,
        connection_name: str | None = None,
        **context: object,
    ) -> None:
        super().__init__(message, connection_name=connection_name, **context)
        self.connection_name = connection_name


class TypeMappingError(ConfigurationError):
    """A source type could not be mapped to a target type."""

    def __init__(
        self,
        message: str,
        *,
        source_type: str | None = None,
        target_dialect: str | None = None,
        **context: object,
    ) -> None:
        super().__init__(
            message,
            source_type=source_type,
            target_dialect=target_dialect,
            **context,
        )
        self.source_type = source_type
        self.target_dialect = target_dialect


# ---------------------------------------------------------------------------
# Connection errors
# ---------------------------------------------------------------------------


class BaniConnectionError(BaniError):
    """Base for connection-related failures.

    Named ``BaniConnectionError`` to avoid shadowing the built-in
    ``ConnectionError``.
    """


class SourceConnectionError(BaniConnectionError):
    """Failed to connect to the source database."""


class TargetConnectionError(BaniConnectionError):
    """Failed to connect to the target database."""


# ---------------------------------------------------------------------------
# Schema errors
# ---------------------------------------------------------------------------


class SchemaError(BaniError):
    """Error during schema introspection or translation."""


class IntrospectionError(SchemaError):
    """Failed to introspect the source database schema."""


class SchemaTranslationError(SchemaError):
    """Failed to translate schema between source and target dialects."""


class DependencyResolutionError(SchemaError):
    """Failed to resolve table dependencies (e.g. circular FK chains)."""

    def __init__(
        self,
        message: str,
        *,
        tables: tuple[str, ...] = (),
        **context: object,
    ) -> None:
        super().__init__(message, tables=tables, **context)
        self.tables = tables


# ---------------------------------------------------------------------------
# Data-transfer errors
# ---------------------------------------------------------------------------


class DataTransferError(BaniError):
    """Error during data transfer between source and target."""


class ReadError(DataTransferError):
    """Failed to read data from the source."""


class WriteError(DataTransferError):
    """Failed to write data to the target."""


class BatchError(DataTransferError):
    """A specific batch failed during transfer.

    Carries the batch number and the offset of the first row in the batch so
    that the resumability protocol can pick up from the right place.
    """

    def __init__(
        self,
        message: str,
        *,
        batch_number: int,
        first_row_offset: int,
        **context: object,
    ) -> None:
        super().__init__(
            message,
            batch_number=batch_number,
            first_row_offset=first_row_offset,
            **context,
        )
        self.batch_number = batch_number
        self.first_row_offset = first_row_offset


class TransformError(DataTransferError):
    """A transform step failed during the pipeline."""


# ---------------------------------------------------------------------------
# Hook / scheduler errors
# ---------------------------------------------------------------------------


class HookExecutionError(BaniError):
    """A pre- or post-migration hook failed."""


class SchedulerError(BaniError):
    """Scheduler-related failure (cron integration, task scheduling)."""
