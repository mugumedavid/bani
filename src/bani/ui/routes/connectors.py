"""Connector info routes (Section 20.3).

Provides endpoints for listing available connectors and retrieving
details for a specific connector.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from bani.ui.auth import verify_token
from bani.ui.models import ConnectorInfo

router = APIRouter(tags=["connectors"], dependencies=[Depends(verify_token)])


@router.get("/connectors", response_model=list[ConnectorInfo])
async def list_connectors() -> list[ConnectorInfo]:
    """List all discovered connectors.

    Returns:
        List of connector info objects.
    """
    from bani.connectors.registry import ConnectorRegistry

    connectors = ConnectorRegistry.discover()
    results: list[ConnectorInfo] = []
    for name, cls in sorted(connectors.items()):
        results.append(
            ConnectorInfo(
                name=name,
                class_name=cls.__name__,
                module=cls.__module__,
            )
        )
    return results


@router.get("/connectors/{name}", response_model=ConnectorInfo)
async def get_connector(name: str) -> ConnectorInfo:
    """Get details for a specific connector.

    Args:
        name: Connector name (e.g. ``postgresql``, ``mysql``).

    Returns:
        Connector info.

    Raises:
        HTTPException: 404 if the connector is not found.
    """
    from bani.connectors.registry import ConnectorRegistry

    connectors = ConnectorRegistry.discover()
    if name not in connectors:
        raise HTTPException(
            status_code=404,
            detail=f"Connector '{name}' not found",
        )
    cls = connectors[name]
    return ConnectorInfo(
        name=name,
        class_name=cls.__name__,
        module=cls.__module__,
    )
