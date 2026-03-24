"""Connector registry for discovery via entry points (Section 6.2)."""

from __future__ import annotations

from typing import Any, ClassVar

from bani.connectors.base import SinkConnector, SourceConnector


class ConnectorRegistry:
    """Discovers and instantiates connectors by dialect name."""

    _source_registry: ClassVar[dict[str, type[SourceConnector]]] = {}
    _sink_registry: ClassVar[dict[str, type[SinkConnector]]] = {}

    @classmethod
    def register_source(
        cls, dialect: str, connector_cls: type[SourceConnector]
    ) -> None:
        """Register a source connector class for a dialect."""
        cls._source_registry[dialect] = connector_cls

    @classmethod
    def register_sink(cls, dialect: str, connector_cls: type[SinkConnector]) -> None:
        """Register a sink connector class for a dialect."""
        cls._sink_registry[dialect] = connector_cls

    @classmethod
    def get_source(cls, dialect: str, **kwargs: Any) -> SourceConnector:
        """Get a source connector instance for the given dialect.

        Args:
            dialect: The database dialect name.
            **kwargs: Arguments to pass to the connector constructor.

        Returns:
            An instantiated SourceConnector.

        Raises:
            KeyError: If no source connector is registered for the dialect.
        """
        if dialect not in cls._source_registry:
            cls._load_entry_points()
        if dialect not in cls._source_registry:
            msg = f"No source connector registered for dialect '{dialect}'"
            raise KeyError(msg)
        return cls._source_registry[dialect](**kwargs)

    @classmethod
    def get_sink(cls, dialect: str, **kwargs: Any) -> SinkConnector:
        """Get a sink connector instance for the given dialect.

        Args:
            dialect: The database dialect name.
            **kwargs: Arguments to pass to the connector constructor.

        Returns:
            An instantiated SinkConnector.

        Raises:
            KeyError: If no sink connector is registered for the dialect.
        """
        if dialect not in cls._sink_registry:
            cls._load_entry_points()
        if dialect not in cls._sink_registry:
            msg = f"No sink connector registered for dialect '{dialect}'"
            raise KeyError(msg)
        return cls._sink_registry[dialect](**kwargs)

    @classmethod
    def _load_entry_points(cls) -> None:
        """Load connectors from entry points."""
        try:
            from importlib.metadata import entry_points

            eps = entry_points()
            for ep in eps.select(group="bani.connectors.source"):
                if ep.name not in cls._source_registry:
                    cls._source_registry[ep.name] = ep.load()
            for ep in eps.select(group="bani.connectors.sink"):
                if ep.name not in cls._sink_registry:
                    cls._sink_registry[ep.name] = ep.load()
        except Exception:
            pass
