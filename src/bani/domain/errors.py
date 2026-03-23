"""Domain exception stubs for BDL module."""  # STUB

from __future__ import annotations


class BaniError(Exception):
    """Base exception for Bani."""

    def __init__(self, message: str, **context: object) -> None:
        """Initialize BaniError.

        Args:
            message: Error message.
            **context: Additional context information.
        """
        super().__init__(message)
        self.context = context


class ConfigurationError(BaniError):
    """Configuration-related error."""

    pass


class BDLValidationError(ConfigurationError):
    """BDL validation error."""

    def __init__(
        self,
        message: str,
        *,
        document_path: str | None = None,
        line_number: int | None = None,
        **context: object,
    ) -> None:
        """Initialize BDLValidationError.

        Args:
            message: Error message.
            document_path: Path to the document.
            line_number: Line number where error occurred.
            **context: Additional context information.
        """
        super().__init__(
            message,
            document_path=document_path,
            line_number=line_number,
            **context,
        )
        self.document_path = document_path
        self.line_number = line_number
