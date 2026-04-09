"""Named database connections registry.

Reads ``~/.bani/connections.json`` and provides lookup-by-key for named
database connections.  Both the MCP server and the Web UI share this
registry so that connections are configured once and discoverable
everywhere.

The file should have ``0600`` permissions since it may contain plaintext
credentials.  Credential fields also support ``${env:VAR}`` references
that are resolved at runtime by the connector layer.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from bani.domain.project import ConnectionConfig

logger = logging.getLogger(__name__)

_ENV_REF_PREFIX = "${env:"
_ENV_REF_SUFFIX = "}"


@dataclass(frozen=True)
class RegisteredConnection:
    """A single entry from ``connections.json``.

    The ``options`` field holds connector-specific key-value pairs
    (e.g. ``service_name`` for Oracle, ``charset`` for MySQL).
    These map to ``ConnectionConfig.extra``.
    """

    key: str
    name: str
    connector: str
    host: str
    port: int
    database: str
    username: str
    password: str
    options: tuple[tuple[str, str], ...] = ()


class ConnectionRegistry:
    """File-backed registry of named database connections.

    The registry file is a JSON object keyed by connection identifier.
    Each value contains ``name``, ``connector``, ``host``, ``port``,
    ``database``, ``username``, and ``password`` fields.
    """

    _DEFAULT_PATH: ClassVar[Path] = Path("~/.bani/connections.json")

    @classmethod
    def load(cls, path: Path | None = None) -> dict[str, RegisteredConnection]:
        """Load all connections from the registry file.

        Args:
            path: Override for the registry file location.

        Returns:
            Dict mapping connection key to ``RegisteredConnection``.
            Returns an empty dict if the file is missing or malformed.
        """
        resolved = (path or cls._DEFAULT_PATH).expanduser()
        if not resolved.exists():
            return {}
        try:
            data = json.loads(resolved.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to read connections registry: %s", exc)
            return {}
        if not isinstance(data, dict):
            return {}
        result: dict[str, RegisteredConnection] = {}
        for key, entry in data.items():
            if not isinstance(entry, dict):
                continue
            try:
                raw_opts = entry.get("options", {})
                opts: tuple[tuple[str, str], ...] = ()
                if isinstance(raw_opts, dict):
                    opts = tuple((str(k), str(v)) for k, v in raw_opts.items())
                result[key] = RegisteredConnection(
                    key=key,
                    name=str(entry.get("name", key)),
                    connector=entry["connector"],
                    host=entry.get("host", ""),
                    port=int(entry.get("port", 0)),
                    database=entry.get("database", ""),
                    username=entry.get("username", ""),
                    password=entry.get("password", ""),
                    options=opts,
                )
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning("Skipping malformed connection '%s': %s", key, exc)
        return result

    @classmethod
    def get(cls, key: str, path: Path | None = None) -> RegisteredConnection:
        """Look up a single connection by key.

        Args:
            key: Connection identifier.
            path: Override for the registry file location.

        Returns:
            The matching ``RegisteredConnection``.

        Raises:
            ValueError: If the key is not found.
        """
        connections = cls.load(path)
        if key not in connections:
            available = ", ".join(sorted(connections)) or "(none)"
            msg = f"Connection '{key}' not found in registry. Available: {available}"
            raise ValueError(msg)
        return connections[key]

    @classmethod
    def to_connection_config(cls, conn: RegisteredConnection) -> ConnectionConfig:
        """Convert a ``RegisteredConnection`` to a domain ``ConnectionConfig``.

        If ``username`` or ``password`` contain ``${env:VAR}`` patterns
        they are passed through as ``username_env`` / ``password_env``
        (the connector's ``_resolve_env_var()`` handles resolution).

        Literal values are injected into ``os.environ`` under a
        deterministic key so the connector can read them via the same
        env-var mechanism.

        Args:
            conn: The registered connection to convert.

        Returns:
            A ``ConnectionConfig`` ready for connector use.
        """
        username_env = _ensure_env(conn.key, "USER", conn.username)
        password_env = _ensure_env(conn.key, "PASS", conn.password)

        return ConnectionConfig(
            dialect=conn.connector,
            host=conn.host,
            port=conn.port,
            database=conn.database,
            username_env=username_env,
            password_env=password_env,
            extra=conn.options,
        )

    @classmethod
    def safe_summary(cls, conn: RegisteredConnection) -> dict[str, Any]:
        """Return a dict safe for AI consumption (no credentials).

        Args:
            conn: The connection to summarise.

        Returns:
            Dict with key, name, connector, host, port, and database.
        """
        return {
            "key": conn.key,
            "name": conn.name,
            "connector": conn.connector,
            "host": conn.host,
            "port": conn.port,
            "database": conn.database,
        }


def _is_env_ref(value: str) -> bool:
    """Check whether a value is an ``${env:VAR}`` reference."""
    return value.startswith(_ENV_REF_PREFIX) and value.endswith(_ENV_REF_SUFFIX)


def _ensure_env(conn_key: str, suffix: str, value: str) -> str:
    """Return an env-var name that resolves to *value*.

    If *value* is already an ``${env:VAR}`` reference, return it as-is.
    Otherwise, inject *value* into ``os.environ`` under a deterministic
    key and return that key.
    """
    if _is_env_ref(value):
        return value
    env_name = f"_BANI_CONN_{conn_key}_{suffix}"
    os.environ[env_name] = value
    return env_name
