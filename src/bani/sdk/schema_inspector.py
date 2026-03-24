"""Schema introspection via connectors."""

from __future__ import annotations

from typing import Any, cast

from bani.connectors.base import SourceConnector
from bani.connectors.registry import ConnectorRegistry
from bani.domain.project import ConnectionConfig
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
        extra_config: tuple[tuple[str, str], ...] = tuple(
            (k, str(v)) for k, v in kwargs.items()
        )
        config = ConnectionConfig(
            dialect=dialect,
            host=host,
            port=port,
            database=database,
            username_env=username_env,
            password_env=password_env,
            extra=extra_config,
        )

        connector_class = ConnectorRegistry.get(dialect)
        source = cast(type[SourceConnector], connector_class)()
        try:
            source.connect(config)
            return source.introspect_schema()
        finally:
            source.disconnect()
