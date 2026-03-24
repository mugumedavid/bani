"""Schema introspection via connectors."""

from __future__ import annotations

from typing import Any

from bani.connectors.registry import ConnectorRegistry
from bani.domain.schema import DatabaseSchema


class SchemaInspector:
    """Inspects database schema via connector registry."""

    @staticmethod
    def inspect(
        dialect: str,
        host: str = "",
        port: int = 0,
        database: str = "",
        username_env: str = "",
        password_env: str = "",
        **kwargs: Any,
    ) -> DatabaseSchema:
        """Introspect a database schema.

        Args:
            dialect: Database dialect (e.g. "postgresql", "mysql").
            host: Database host.
            port: Database port.
            database: Database name.
            username_env: Environment variable name for username.
            password_env: Environment variable name for password.
            **kwargs: Additional connector-specific arguments.

        Returns:
            The introspected DatabaseSchema.

        Raises:
            KeyError: If no connector is registered for the dialect.
            Exception: If connection or introspection fails.
        """
        connector_kwargs = {
            "host": host,
            "port": port,
            "database": database,
            "username_env": username_env,
            "password_env": password_env,
        }
        connector_kwargs.update(kwargs)

        source = ConnectorRegistry.get_source(dialect, **connector_kwargs)
        try:
            source.connect(**connector_kwargs)
            return source.introspect_schema()
        finally:
            source.close()
