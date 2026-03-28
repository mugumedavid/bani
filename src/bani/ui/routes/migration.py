"""Migration execution routes (Section 20.3).

Provides endpoints for starting, monitoring, and cancelling migrations.
Uses the SDK (``Bani.load()`` / ``BaniProject.run()``) internally.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from bani.ui.auth import verify_token
from bani.ui.models import MigrateRequest, MigrateResult, MigrateStatus

logger = logging.getLogger(__name__)

router = APIRouter(tags=["migration"], dependencies=[Depends(verify_token)])


def _get_migration_state(request: Request) -> dict[str, Any]:
    """Return the shared migration state dict from app.state.

    Args:
        request: The incoming FastAPI request.

    Returns:
        Mutable dict tracking migration progress.
    """
    state: dict[str, Any] = request.app.state.migration_state
    return state


@router.post("/migrate", response_model=MigrateResult)
async def start_migration(body: MigrateRequest, request: Request) -> MigrateResult:
    """Start a migration for the named project.

    Runs the migration in a background thread via ``asyncio.to_thread``
    so the async event loop is not blocked.

    Args:
        body: Migration request with project name and options.
        request: The incoming request.

    Raises:
        HTTPException: 409 if a migration is already running.
        HTTPException: 404 if the project file does not exist.
        HTTPException: 500 if the migration fails.
    """
    state = _get_migration_state(request)

    if state.get("running"):
        raise HTTPException(status_code=409, detail="A migration is already running")

    # Resolve project path
    projects_dir: str = getattr(
        request.app.state, "projects_dir", "~/.bani/projects"
    )
    from pathlib import Path

    project_path = Path(projects_dir).expanduser() / f"{body.project_name}.bdl"
    if not project_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Project '{body.project_name}' not found",
        )

    # Mark as running
    state["running"] = True
    state["project_name"] = body.project_name
    state["tables_completed"] = 0
    state["tables_failed"] = 0
    state["total_rows_read"] = 0
    state["total_rows_written"] = 0
    state["error"] = None
    state["cancel_event"] = threading.Event()

    ws_queue: asyncio.Queue[dict[str, Any]] | None = getattr(
        request.app.state, "ws_queue", None
    )

    def _run_migration() -> MigrateResult:
        """Execute the migration synchronously in a worker thread."""
        try:
            from bani.sdk.bani import Bani

            bp = Bani.load(str(project_path))
            result = bp.run()

            state["tables_completed"] = result.tables_completed
            state["tables_failed"] = result.tables_failed
            state["total_rows_read"] = result.total_rows_read
            state["total_rows_written"] = result.total_rows_written

            return MigrateResult(
                project_name=result.project_name,
                tables_completed=result.tables_completed,
                tables_failed=result.tables_failed,
                total_rows_read=result.total_rows_read,
                total_rows_written=result.total_rows_written,
                duration_seconds=result.duration_seconds,
                errors=list(result.errors),
            )
        except Exception as exc:
            state["error"] = str(exc)
            logger.exception("Migration failed for project '%s'", body.project_name)
            raise
        finally:
            state["running"] = False
            # Notify WebSocket listeners that migration has ended
            if ws_queue is not None:
                try:
                    ws_queue.put_nowait(
                        {"event": "migration_ended", "project": body.project_name}
                    )
                except asyncio.QueueFull:
                    pass

    try:
        result = await asyncio.to_thread(_run_migration)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return result


@router.get("/migrate/status", response_model=MigrateStatus)
async def get_status(request: Request) -> MigrateStatus:
    """Get the current migration status.

    Returns:
        Current migration state including progress counters.
    """
    state = _get_migration_state(request)
    return MigrateStatus(
        running=state.get("running", False),
        project_name=state.get("project_name"),
        tables_completed=state.get("tables_completed", 0),
        tables_failed=state.get("tables_failed", 0),
        total_rows_read=state.get("total_rows_read", 0),
        total_rows_written=state.get("total_rows_written", 0),
        error=state.get("error"),
    )


@router.post("/migrate/cancel", status_code=202)
async def cancel_migration(request: Request) -> dict[str, str]:
    """Cancel a running migration.

    Returns:
        Acknowledgement message.

    Raises:
        HTTPException: 409 if no migration is currently running.
    """
    state = _get_migration_state(request)

    if not state.get("running"):
        raise HTTPException(status_code=409, detail="No migration is running")

    cancel_event: threading.Event | None = state.get("cancel_event")
    if cancel_event is not None:
        cancel_event.set()

    return {"detail": "Cancellation requested"}
