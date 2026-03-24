"""Connector registry for discovering and retrieving connector implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from collections.abc import Mapping

from bani.connectors.base import SinkConnector, SourceConnector


class ConnectorRegistry:
    """Discovers and provides access to installed connectors via entry points.

    Connectors are registered in pyproject.toml under
    [project.entry-points."bani.connectors"]. If entry points are not
    available (e.g., in development), falls back to directly importing
    known connectors.
    """

    _cache: ClassVar[dict[str, type[SourceConnector] | type[SinkConnector]]] = {}

    @staticmethod
    def discover() -> Mapping[str, type[SourceConnector] | type[SinkConnector]]:
        """Discover all installed connectors via entry points.

        Returns a dictionary mapping connector names to their classes.
        Caches the result after first call.

        Returns:
            Dictionary of connector_name -> ConnectorClass.
        """
        if ConnectorRegistry._cache:
            return ConnectorRegistry._cache

        # Try to use entry points (Python 3.10+)
        try:
            from importlib.metadata import entry_points

            eps = entry_points()
            # Handle different versions of importlib.metadata
            if hasattr(eps, "select"):
                # Python 3.10+
                connector_eps = eps.select(group="bani.connectors")
            else:
                # Fallback for older versions
                connector_eps = eps.get("bani.connectors", [])  # type: ignore[attr-defined]

            for ep in connector_eps:
                try:
                    connector_class = ep.load()
                    ConnectorRegistry._cache[ep.name] = connector_class
                except Exception:
                    # Skip connectors that fail to load
                    pass
        except (ImportError, KeyError, AttributeError):
            # Entry points not available or failed
            pass

        # Fallback: directly import known connectors for development
        if not ConnectorRegistry._cache:
            try:
                from bani.connectors.postgresql import PostgreSQLConnector

                ConnectorRegistry._cache["postgresql"] = PostgreSQLConnector
            except ImportError:
                pass

            # Other connectors not yet implemented
            try:
                import bani.connectors.mysql

                ConnectorRegistry._cache["mysql"] = bani.connectors.mysql.MySQLConnector
            except (ImportError, AttributeError):
                pass

            try:
                import bani.connectors.mssql

                ConnectorRegistry._cache["mssql"] = bani.connectors.mssql.MSSQLConnector  # type: ignore[attr-defined]
            except (ImportError, AttributeError):
                pass

            try:
                import bani.connectors.oracle

                ConnectorRegistry._cache["oracle"] = (
                    bani.connectors.oracle.OracleConnector  # type: ignore[attr-defined]
                )
            except (ImportError, AttributeError):
                pass

            try:
                import bani.connectors.sqlite

                ConnectorRegistry._cache["sqlite"] = (
                    bani.connectors.sqlite.SQLiteConnector  # type: ignore[attr-defined]
                )
            except (ImportError, AttributeError):
                pass

        return ConnectorRegistry._cache

    @staticmethod
    def get(name: str) -> type[SourceConnector] | type[SinkConnector]:
        """Get a specific connector class by name.

        Args:
            name: Connector name (e.g., "postgresql", "mysql").

        Returns:
            The connector class.

        Raises:
            ValueError: If connector is not found.
        """
        connectors = ConnectorRegistry.discover()
        if name not in connectors:
            available = ", ".join(connectors.keys())
            raise ValueError(f"Connector '{name}' not found. Available: {available}")
        return connectors[name]
