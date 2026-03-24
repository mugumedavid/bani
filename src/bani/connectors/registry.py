"""Connector registry with entry point discovery (Section 6.2).

Provides a singleton registry that auto-discovers connector implementations
via entry points (pyproject.toml [project.entry-points."bani.connectors"]).
"""

from __future__ import annotations

from typing import Any, ClassVar, cast

from bani.connectors.base import SinkConnector, SourceConnector
from bani.domain.errors import ConfigurationError

# Define a type for entry points that works across versions
EntryPointIterator = Any


class ConnectorRegistry:
    """Registry for source and sink connector implementations.

    Uses entry point discovery to auto-load connector classes from installed
    packages. Connectors are registered once at module import time.
    """

    _source_connectors: ClassVar[dict[str, type[SourceConnector]]] = {}
    _sink_connectors: ClassVar[dict[str, type[SinkConnector]]] = {}
    _initialized: ClassVar[bool] = False

    @classmethod
    def initialize(cls) -> None:
        """Discover and register all available connectors via entry points.

        Safe to call multiple times (subsequent calls are no-ops).
        """
        if cls._initialized:
            return

        # Entry point discovery has complex typing across Python versions
        # Suppress mypy checks for this entire block - the typing conflicts
        # between importlib.metadata and importlib_metadata across Python versions
        try:
            from importlib.metadata import entry_points

            eps = entry_points(group="bani.connectors")
        except TypeError:
            try:
                from importlib.metadata import entry_points as legacy_entry_points

                all_eps = legacy_entry_points()
                if hasattr(all_eps, "select"):
                    eps = all_eps.select(group="bani.connectors")
                else:
                    eps = all_eps.get("bani.connectors", [])  # type: ignore[attr-defined]
            except ImportError:
                from importlib_metadata import entry_points as alt_entry_points

                all_eps = alt_entry_points()  # type: ignore[assignment]
                if hasattr(all_eps, "select"):
                    eps = all_eps.select(group="bani.connectors")
                else:
                    eps = all_eps.get("bani.connectors", [])  # type: ignore[attr-defined]

        for ep in eps:
            try:
                connector_class = ep.load()
                name = ep.name.lower()

                # Determine if source or sink (or both) by checking inheritance
                if issubclass(connector_class, SourceConnector):
                    cls._source_connectors[name] = cast(
                        type[SourceConnector], connector_class
                    )
                if issubclass(connector_class, SinkConnector):
                    cls._sink_connectors[name] = cast(
                        type[SinkConnector], connector_class
                    )
            except Exception as e:
                # Log but don't fail — allow partial discovery
                import warnings

                warnings.warn(
                    f"Failed to load connector {ep.name}: {e}",
                    stacklevel=2,
                )

        cls._initialized = True

    @classmethod
    def get_source_connector(cls, dialect: str) -> type[SourceConnector]:
        """Get a source connector class by dialect name.

        Args:
            dialect: The database dialect (e.g. "postgresql", "mysql").

        Returns:
            The source connector class.

        Raises:
            ConfigurationError: If the dialect is not registered or is not
                a source connector.
        """
        cls.initialize()
        dialect_lower = dialect.lower()

        if dialect_lower not in cls._source_connectors:
            available = ", ".join(sorted(cls._source_connectors.keys()))
            raise ConfigurationError(
                f"No source connector found for dialect '{dialect}'. "
                f"Available: {available or 'none'}",
                connection_name=dialect,
            )

        return cls._source_connectors[dialect_lower]

    @classmethod
    def get_sink_connector(cls, dialect: str) -> type[SinkConnector]:
        """Get a sink connector class by dialect name.

        Args:
            dialect: The database dialect (e.g. "postgresql", "mysql").

        Returns:
            The sink connector class.

        Raises:
            ConfigurationError: If the dialect is not registered or is not
                a sink connector.
        """
        cls.initialize()
        dialect_lower = dialect.lower()

        if dialect_lower not in cls._sink_connectors:
            available = ", ".join(sorted(cls._sink_connectors.keys()))
            raise ConfigurationError(
                f"No sink connector found for dialect '{dialect}'. "
                f"Available: {available or 'none'}",
                connection_name=dialect,
            )

        return cls._sink_connectors[dialect_lower]

    @classmethod
    def list_source_connectors(cls) -> dict[str, type[SourceConnector]]:
        """List all registered source connectors.

        Returns:
            Dictionary of {dialect: ConnectorClass}.
        """
        cls.initialize()
        return dict(cls._source_connectors)

    @classmethod
    def list_sink_connectors(cls) -> dict[str, type[SinkConnector]]:
        """List all registered sink connectors.

        Returns:
            Dictionary of {dialect: ConnectorClass}.
        """
        cls.initialize()
        return dict(cls._sink_connectors)

    @classmethod
    def create_source_connector(cls, dialect: str, **kwargs: Any) -> SourceConnector:
        """Create and return a source connector instance.

        Args:
            dialect: The database dialect.
            **kwargs: Arguments passed to the connector's __init__.

        Returns:
            An instance of the source connector.

        Raises:
            ConfigurationError: If the dialect is not registered.
        """
        connector_class = cls.get_source_connector(dialect)
        return connector_class(**kwargs)

    @classmethod
    def create_sink_connector(cls, dialect: str, **kwargs: Any) -> SinkConnector:
        """Create and return a sink connector instance.

        Args:
            dialect: The database dialect.
            **kwargs: Arguments passed to the connector's __init__.

        Returns:
            An instance of the sink connector.

        Raises:
            ConfigurationError: If the dialect is not registered.
        """
        connector_class = cls.get_sink_connector(dialect)
        return connector_class(**kwargs)
