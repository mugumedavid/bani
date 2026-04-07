"""Named connections routes.

Exposes the shared ``~/.bani/connections.json`` registry to the Web UI
so that connections configured once are discoverable from both the
dashboard and the MCP server.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from bani.ui.auth import verify_token

router = APIRouter(tags=["connections"], dependencies=[Depends(verify_token)])


@router.get("/connections")
async def list_connections() -> dict[str, Any]:
    """List all named connections (credentials excluded).

    Returns:
        Dict with ``connections`` (keyed by identifier) and ``count``.
    """
    from bani.infra.connections import ConnectionRegistry

    connections = ConnectionRegistry.load()
    summaries = {
        key: ConnectionRegistry.safe_summary(conn)
        for key, conn in connections.items()
    }
    return {"connections": summaries, "count": len(summaries)}


@router.get("/connections/{key}")
async def get_connection(key: str) -> dict[str, Any]:
    """Get a single connection by key (credentials excluded).

    Args:
        key: Connection identifier.

    Returns:
        Connection summary dict.

    Raises:
        HTTPException: 404 if the key is not found.
    """
    from bani.infra.connections import ConnectionRegistry

    try:
        conn = ConnectionRegistry.get(key)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ConnectionRegistry.safe_summary(conn)


@router.get("/connections/{key}/config")
async def get_connection_config(key: str) -> dict[str, Any]:
    """Get a connection as a full ConnectionConfig for form population.

    Includes credentials so the Project Editor can populate all fields.
    Protected by the same auth token as all other UI endpoints.

    Args:
        key: Connection identifier.

    Returns:
        Dict shaped like the frontend ``ConnectionConfig`` type.

    Raises:
        HTTPException: 404 if the key is not found.
    """
    from bani.infra.connections import ConnectionRegistry, _is_env_ref

    try:
        conn = ConnectionRegistry.get(key)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    # Build a dict matching the frontend ConnectionConfig shape.
    username_is_env = _is_env_ref(conn.username)
    password_is_env = _is_env_ref(conn.password)

    # Strip ${env:...} wrapper for the form value if it's an env ref.
    username_val = (
        conn.username[6:-1] if username_is_env else conn.username
    )
    password_val = (
        conn.password[6:-1] if password_is_env else conn.password
    )

    extra: dict[str, str] = dict(conn.options)

    return {
        "name": conn.name,
        "connector": conn.connector,
        "host": conn.host,
        "port": conn.port,
        "database": conn.database,
        "username_env": username_val,
        "password_env": password_val,
        "username_is_env": username_is_env,
        "password_is_env": password_is_env,
        "extra": extra,
    }
