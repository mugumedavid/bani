"""WebSocket progress streaming for the Bani Web UI (Section 20.2).

Bridges sync migration events (from ProgressTracker) to async WebSocket
clients via an ``asyncio.Queue``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from bani.application.progress import ProgressEvent

logger = logging.getLogger(__name__)


def event_to_json(event: ProgressEvent) -> str:
    """Serialise a ProgressEvent dataclass to a JSON string.

    Handles ``datetime`` objects by converting them to ISO-8601 strings.

    Args:
        event: A progress event dataclass.

    Returns:
        JSON string representation of the event.
    """
    data: dict[str, Any] = asdict(event)  # type: ignore[arg-type]
    data["event_type"] = type(event).__name__

    # Convert datetime fields to ISO strings
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()

    return json.dumps(data)


async def progress_websocket_handler(
    websocket: WebSocket,
    ws_queue: asyncio.Queue[dict[str, Any]],
) -> None:
    """Handle a WebSocket connection for progress streaming.

    Reads from the shared ``ws_queue`` and forwards events to the
    connected WebSocket client. Exits on disconnect.

    Args:
        websocket: The WebSocket connection.
        ws_queue: Shared async queue that receives progress events.
    """
    await websocket.accept()
    try:
        while True:
            try:
                event_data = await asyncio.wait_for(ws_queue.get(), timeout=30.0)
                await websocket.send_json(event_data)
            except asyncio.TimeoutError:
                # Send a keepalive ping
                await websocket.send_json({"event": "ping"})
    except WebSocketDisconnect:
        logger.debug("WebSocket client disconnected")
    except Exception:
        logger.debug("WebSocket connection closed", exc_info=True)
