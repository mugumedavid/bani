"""Server-Sent Events (SSE) broadcaster for migration progress.

Provides a thread-safe broadcast mechanism that fans out progress
events to all connected SSE clients.  Each client gets its own
``asyncio.Queue``; slow clients have events dropped rather than
blocking the migration thread.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import Request
from fastapi.responses import StreamingResponse

from bani.ui.auth import verify_token_from_query

logger = logging.getLogger(__name__)


class SSEBroadcaster:
    """Manages SSE client connections and broadcasts events."""

    def __init__(self) -> None:
        self._clients: list[asyncio.Queue[dict[str, Any]]] = []

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        """Register a new client and return its personal queue."""
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=256)
        self._clients.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        """Remove a client queue."""
        try:
            self._clients.remove(q)
        except ValueError:
            pass

    def broadcast(self, event: dict[str, Any]) -> None:
        """Push an event to every connected client.

        Non-blocking: if a client's queue is full the event is dropped
        for that client rather than blocking the caller (which runs in
        the migration thread).
        """
        for q in list(self._clients):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass


async def sse_progress_endpoint(request: Request) -> StreamingResponse:
    """SSE endpoint that streams migration progress events.

    Clients connect with ``EventSource("/api/migrate/progress?token=...")``.
    Events are pushed as ``data: {json}\\n\\n`` lines.  A keepalive
    comment is sent every 15 seconds to prevent proxy timeouts.
    """
    # Verify auth token from query param (EventSource can't set headers)
    verify_token_from_query(request)

    broadcaster: SSEBroadcaster = request.app.state.sse_broadcaster
    q = broadcaster.subscribe()

    async def event_generator() -> Any:
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            broadcaster.unsubscribe(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
