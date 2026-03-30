"""Migration execution routes (Section 20.3).

Provides endpoints for starting, monitoring, and cancelling migrations.
Uses the SDK (``Bani.load()`` / ``BaniProject.run()``) internally.
Progress is streamed to clients via SSE (``/api/migrate/progress``).
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from bani.ui.auth import verify_token
from bani.ui.models import MigrateRequest, MigrateStarted, MigrateStatus
from bani.ui.sse import SSEBroadcaster

logger = logging.getLogger(__name__)


def _setup_direct_credentials(
    project: Any,
) -> list[str]:
    """Set temp env vars for direct credentials in ConnectionConfig.

    The connector system resolves credentials from env vars. If the BDL
    has direct values (not ``${env:...}``), we detect them and set temp
    env vars so the connector can find them.

    Returns:
        List of env var names that were set (for cleanup).
    """
    import os
    import re

    env_pattern = re.compile(r"^\$\{env:(.+)\}$")
    temp_vars: list[str] = []

    for role, cfg in [("SRC", project.source), ("TGT", project.target)]:
        if cfg is None:
            continue
        # Check username
        if cfg.username_env:
            m = env_pattern.match(cfg.username_env)
            if m:
                # It's already an env var reference — the resolver handles it
                pass
            else:
                # Direct value — set a temp env var
                env_name = f"_BANI_{role}_USER"
                os.environ[env_name] = cfg.username_env
                temp_vars.append(env_name)
                # Mutate the config to point to the temp var
                object.__setattr__(cfg, "username_env", env_name)

        if cfg.password_env:
            m = env_pattern.match(cfg.password_env)
            if m:
                pass
            else:
                env_name = f"_BANI_{role}_PASS"
                os.environ[env_name] = cfg.password_env
                temp_vars.append(env_name)
                object.__setattr__(cfg, "password_env", env_name)

    return temp_vars


def _cleanup_credentials(temp_vars: list[str]) -> None:
    """Remove temp env vars set by _setup_direct_credentials."""
    import os

    for name in temp_vars:
        os.environ.pop(name, None)


def _event_to_sse(event: object) -> dict[str, Any] | None:
    """Convert a ProgressEvent to an SSE-friendly dict.

    Returns ``None`` for event types that should not be streamed.
    """
    from bani.application.progress import (
        BatchComplete,
        IntrospectionComplete,
        MigrationComplete,
        MigrationStarted,
        PhaseChange,
        TableComplete,
        TableCreateFailed,
        TableStarted,
    )

    if isinstance(event, PhaseChange):
        return {
            "type": "phase_change",
            "phase": event.phase,
        }
    if isinstance(event, IntrospectionComplete):
        return {
            "type": "introspection_complete",
            "source_dialect": event.source_dialect,
            "tables": [
                {"name": name, "estimated_rows": rows}
                for name, rows in event.tables
            ],
        }
    if isinstance(event, MigrationStarted):
        return {
            "type": "migration_started",
            "table_count": event.table_count,
            "source_dialect": event.source_dialect,
            "target_dialect": event.target_dialect,
        }
    if isinstance(event, TableStarted):
        return {
            "type": "table_start",
            "table_name": event.table_name,
            "estimated_rows": event.estimated_rows,
        }
    if isinstance(event, BatchComplete):
        return {
            "type": "batch_complete",
            "table_name": event.table_name,
            "batch_number": event.batch_number,
            "rows_read": event.rows_read,
            "rows_written": event.rows_written,
        }
    if isinstance(event, TableComplete):
        return {
            "type": "table_complete",
            "table_name": event.table_name,
            "total_rows_read": event.total_rows_read,
            "total_rows_written": event.total_rows_written,
            "batch_count": event.batch_count,
        }
    if isinstance(event, TableCreateFailed):
        return {
            "type": "table_create_failed",
            "table_name": event.table_name,
            "reason": event.reason,
        }
    if isinstance(event, MigrationComplete):
        return {
            "type": "migration_complete",
            "tables_completed": event.tables_completed,
            "tables_failed": event.tables_failed,
            "total_rows_read": event.total_rows_read,
            "total_rows_written": event.total_rows_written,
            "duration_seconds": event.duration_seconds,
        }
    return None


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


@router.post("/migrate/validate")
async def validate_migration(body: MigrateRequest, request: Request) -> dict[str, str]:
    """Test source and target connections before running a migration.

    Loads the project BDL, creates connectors for source and target,
    and attempts to connect to each. Returns structured results indicating
    success or failure for each connection.

    Args:
        body: Migration request with project name.
        request: The incoming request.

    Returns:
        Dict with ``source`` and ``target`` keys, each ``"ok"`` or an error message.

    Raises:
        HTTPException: 404 if the project file does not exist.
    """
    # Resolve project path
    projects_dir: str = getattr(request.app.state, "projects_dir", "~/.bani/projects")
    from pathlib import Path

    project_path = Path(projects_dir).expanduser() / f"{body.project_name}.bdl"
    if not project_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Project '{body.project_name}' not found",
        )

    def _validate() -> dict[str, str]:
        from bani.connectors.registry import ConnectorRegistry
        from bani.sdk.bani import Bani

        bp = Bani.load(str(project_path))
        project = bp._project  # noqa: SLF001

        # Set temp env vars for direct credentials
        temp_vars = _setup_direct_credentials(project)

        try:
            result: dict[str, str] = {"source": "ok", "target": "ok"}

            # Test source connection
            source_cfg = project.source
            if source_cfg is None:
                result["source"] = "No source connection configured"
            else:
                try:
                    connector_class = ConnectorRegistry.get(source_cfg.dialect)
                    connector = connector_class()
                    connector.connect(source_cfg)
                    connector.disconnect()
                except Exception as exc:
                    result["source"] = str(exc)

            # Test target connection
            target_cfg = project.target
            if target_cfg is None:
                result["target"] = "No target connection configured"
            else:
                try:
                    connector_class = ConnectorRegistry.get(target_cfg.dialect)
                    connector = connector_class()
                    connector.connect(target_cfg)
                    connector.disconnect()
                except Exception as exc:
                    result["target"] = str(exc)

            return result
        finally:
            _cleanup_credentials(temp_vars)

    try:
        return await asyncio.to_thread(_validate)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


async def _dry_run(project_path: Any, project_name: str) -> Any:
    """Validate connections, introspect schema, return plan without executing."""
    from typing import cast

    from bani.connectors.base import SourceConnector
    from bani.connectors.registry import ConnectorRegistry
    from bani.sdk.bani import Bani

    def _execute() -> dict[str, Any]:
        bp = Bani.load(str(project_path))
        project = bp._project  # noqa: SLF001
        temp_vars = _setup_direct_credentials(project)

        try:
            is_valid, errors = bp.validate()
            if not is_valid:
                return {"status": "invalid", "errors": errors}

            source_cfg = project.source
            assert source_cfg is not None

            connector_class = ConnectorRegistry.get(source_cfg.dialect)
            source = cast(type[SourceConnector], connector_class)()
            source.connect(source_cfg)

            try:
                schema = source.introspect_schema()
                tables = [
                    {
                        "name": t.fully_qualified_name,
                        "columns": len(t.columns),
                        "estimated_rows": t.row_count_estimate,
                    }
                    for t in schema.tables
                ]
                total_rows = sum(
                    t.row_count_estimate or 0 for t in schema.tables
                )
                return {
                    "status": "ok",
                    "dry_run": True,
                    "project_name": project_name,
                    "source_dialect": schema.source_dialect,
                    "target_dialect": project.target.dialect
                    if project.target
                    else "unknown",
                    "table_count": len(tables),
                    "total_estimated_rows": total_rows,
                    "tables": tables,
                }
            finally:
                source.disconnect()
        finally:
            _cleanup_credentials(temp_vars)

    try:
        result = await asyncio.to_thread(_execute)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/migrate", status_code=202)
async def start_migration(body: MigrateRequest, request: Request) -> Any:
    """Start a migration for the named project.

    Starts the migration in a background thread and returns immediately
    with a 202 Accepted response. Progress is streamed via SSE at
    ``GET /api/migrate/progress`` and can also be polled via
    ``GET /api/migrate/status``.

    Args:
        body: Migration request with project name and options.
        request: The incoming request.

    Returns:
        A MigrateStarted response with status and project name.

    Raises:
        HTTPException: 409 if a migration is already running.
        HTTPException: 404 if the project file does not exist.
    """
    state = _get_migration_state(request)

    if state.get("running"):
        raise HTTPException(status_code=409, detail="A migration is already running")

    # Resolve project path
    projects_dir: str = getattr(request.app.state, "projects_dir", "~/.bani/projects")
    from pathlib import Path

    project_path = Path(projects_dir).expanduser() / f"{body.project_name}.bdl"
    if not project_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Project '{body.project_name}' not found",
        )

    # Dry run: validate, introspect, return plan — no data transfer
    if body.dry_run:
        return await _dry_run(project_path, body.project_name)

    # Mark as running
    state["running"] = True
    state["phase"] = "introspecting"
    state["project_name"] = body.project_name
    state["tables_completed"] = 0
    state["tables_failed"] = 0
    state["total_tables"] = 0
    state["total_rows_read"] = 0
    state["total_rows_written"] = 0
    state["error"] = None
    state["current_table"] = None
    state["table_failures"] = []
    state["cancel_event"] = threading.Event()
    state["started_at"] = time.time()

    broadcaster: SSEBroadcaster | None = getattr(
        request.app.state, "sse_broadcaster", None
    )

    def _run_migration() -> None:
        """Execute the migration synchronously in a worker thread."""
        from bani.application.progress import (
            BatchComplete,
            IntrospectionComplete,
            MigrationComplete,
            PhaseChange,
            TableComplete,
            TableCreateFailed,
            TableStarted,
        )

        # Track all concurrently running tables so that completing one
        # doesn't wipe the current_table while others are still active.
        running_tables: set[str] = set()

        def _on_progress(event: object) -> None:
            """Update shared state dict and broadcast via SSE."""
            if isinstance(event, PhaseChange):
                state["phase"] = event.phase
                state["current_table"] = None
            elif isinstance(event, IntrospectionComplete):
                state["phase"] = "transferring"
                state["total_tables"] = len(event.tables)
            elif isinstance(event, TableStarted):
                running_tables.add(event.table_name)
                state["current_table"] = event.table_name
            elif isinstance(event, BatchComplete):
                state["total_rows_read"] = (
                    state.get("total_rows_read", 0) + event.rows_read
                )
                state["total_rows_written"] = (
                    state.get("total_rows_written", 0) + event.rows_written
                )
            elif isinstance(event, TableComplete):
                state["tables_completed"] = (
                    state.get("tables_completed", 0) + 1
                )
                running_tables.discard(event.table_name)
                state["current_table"] = next(iter(running_tables), None)
            elif isinstance(event, TableCreateFailed):
                state["tables_failed"] = (
                    state.get("tables_failed", 0) + 1
                )
                fails = state.get("table_failures", [])
                fails.append(f"{event.table_name}: {event.reason}")
                state["table_failures"] = fails
            elif isinstance(event, MigrationComplete):
                pass  # Final totals come from the result object

            # Broadcast to SSE clients
            if broadcaster is not None:
                sse_data = _event_to_sse(event)
                if sse_data is not None:
                    broadcaster.broadcast(sse_data)

        temp_vars: list[str] = []
        try:
            from bani.sdk.bani import Bani
            from bani.ui.routes.settings import _load_settings

            bp = Bani.load(str(project_path))

            # Apply global settings as defaults when project doesn't specify
            settings = _load_settings()
            project = bp._project  # noqa: SLF001
            if project.options:
                from dataclasses import replace as dc_replace
                opts = project.options
                # Only override if the project uses the hardcoded defaults
                # (i.e., the BDL didn't explicitly set them)
                from bani.domain.project import ProjectOptions
                _defaults = ProjectOptions()
                new_opts = dc_replace(
                    opts,
                    batch_size=(
                        settings.batch_size
                        if opts.batch_size == _defaults.batch_size
                        else opts.batch_size
                    ),
                    parallel_workers=(
                        settings.max_workers
                        if opts.parallel_workers == _defaults.parallel_workers
                        else opts.parallel_workers
                    ),
                    memory_limit_mb=(
                        settings.memory_limit_mb
                        if opts.memory_limit_mb == _defaults.memory_limit_mb
                        else opts.memory_limit_mb
                    ),
                )
                object.__setattr__(project, "options", new_opts)

            # Set up checkpoint manager based on settings
            ckpt_mgr = None
            if settings.checkpoint_enabled:
                from pathlib import Path as _Path
                from bani.application.checkpoint import CheckpointManager
                ckpt_mgr = CheckpointManager(
                    base_dir=_Path(settings.checkpoint_dir).expanduser()
                )

            # Set temp env vars for direct credentials
            temp_vars = _setup_direct_credentials(project)
            result = bp.run(
                on_progress=_on_progress,
                resume=body.resume,
                cancel_event=state.get("cancel_event"),
                checkpoint=ckpt_mgr,
            )

            # Overwrite with authoritative final totals from result
            state["tables_completed"] = result.tables_completed
            state["tables_failed"] = result.tables_failed
            state["total_rows_read"] = result.total_rows_read
            state["total_rows_written"] = result.total_rows_written
            if result.errors:
                state["error"] = "; ".join(result.errors[:10])

        except Exception as exc:
            state["error"] = str(exc)
            logger.exception("Migration failed for project '%s'", body.project_name)
        finally:
            _cleanup_credentials(temp_vars)
            state["running"] = False

    # Start migration in a background daemon thread and return immediately
    thread = threading.Thread(target=_run_migration, daemon=True)
    thread.start()

    return MigrateStarted(status="started", project_name=body.project_name)


@router.get("/migrate/status", response_model=MigrateStatus)
async def get_status(request: Request) -> MigrateStatus:
    """Get the current migration status.

    Returns:
        Current migration state including progress counters.
    """
    state = _get_migration_state(request)
    started_at = state.get("started_at")
    elapsed = int(time.time() - started_at) if started_at and state.get("running") else 0
    return MigrateStatus(
        running=state.get("running", False),
        phase=state.get("phase"),
        project_name=state.get("project_name"),
        tables_completed=state.get("tables_completed", 0),
        tables_failed=state.get("tables_failed", 0),
        total_tables=state.get("total_tables", 0),
        total_rows_read=state.get("total_rows_read", 0),
        total_rows_written=state.get("total_rows_written", 0),
        error=state.get("error"),
        current_table=state.get("current_table"),
        table_failures=state.get("table_failures", []),
        elapsed_seconds=elapsed,
    )


@router.get("/runs")
async def list_runs() -> list[dict[str, Any]]:
    """List past migration runs from the run history log.

    Returns:
        List of the most recent 50 run entries, newest first.
    """
    from bani.application.run_log import RunLog

    return RunLog().recent(50)  # type: ignore[return-value]


@router.get("/runs/summary")
async def run_summary() -> dict[str, Any]:
    """Return summary stats for the dashboard.

    Returns:
        Dict with total_runs, last_run, and lifetime_rows.
    """
    from bani.application.run_log import RunLog

    return RunLog().summary()  # type: ignore[return-value]


@router.delete("/runs")
async def clear_run_history() -> dict[str, str]:
    """Delete all run history entries.

    Returns:
        Acknowledgement message.
    """
    from bani.application.run_log import RunLog

    RunLog().clear()
    return {"detail": "Run history cleared"}


@router.get("/migrate/checkpoint/{project_name}")
async def get_checkpoint(project_name: str) -> dict[str, Any]:
    """Return checkpoint info for a project, if one exists.

    Returns:
        Dict with ``exists``, and if exists: ``tables_completed``,
        ``tables_total``, ``created_at``.
    """
    from bani.application.checkpoint import CheckpointManager

    mgr = CheckpointManager()
    data = mgr.load(project_name)
    if data is None:
        return {"exists": False}

    tables = data.get("tables", {})
    completed = sum(
        1 for t in tables.values() if t.get("status") == "completed"
    )
    return {
        "exists": True,
        "tables_completed": completed,
        "tables_total": len(tables),
        "created_at": data.get("created_at"),
    }


@router.delete("/migrate/checkpoint/{project_name}")
async def delete_checkpoint(project_name: str) -> dict[str, str]:
    """Delete the checkpoint file for a project.

    Returns:
        Acknowledgement message.
    """
    from bani.application.checkpoint import CheckpointManager

    mgr = CheckpointManager()
    mgr.clear(project_name)
    return {"detail": f"Checkpoint for '{project_name}' deleted"}


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
