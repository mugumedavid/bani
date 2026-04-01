"""Scheduler registry — manages SchedulerService instances for all projects.

Scans the projects directory for BDL files with enabled cron schedules
and starts each in its own daemon thread. Connectors are created lazily
when the cron fires — not at server startup — so unreachable databases
don't block the server.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, cast

logger = logging.getLogger(__name__)


class SchedulerRegistry:
    """Manages the lifecycle of scheduled migration services."""

    def __init__(self, projects_dir: str) -> None:
        self._projects_dir = Path(projects_dir).expanduser()
        self._threads: dict[str, threading.Thread] = {}
        self._stop_events: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    def list_schedules(self) -> list[dict[str, Any]]:
        """Return schedule info for all projects with enabled cron schedules."""
        import xml.etree.ElementTree as ET

        from bani.application.scheduler import _next_cron_time
        from datetime import datetime, timezone

        results: list[dict[str, Any]] = []
        if not self._projects_dir.exists():
            return results

        for bdl_file in sorted(
            self._projects_dir.glob("*.bdl"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        ):
            name = bdl_file.stem

            # Lightweight XML check — no full parse needed
            try:
                tree = ET.parse(bdl_file)  # noqa: S314
                root = tree.getroot()
            except ET.ParseError:
                continue

            sched_elem = root.find("schedule")
            if sched_elem is None:
                continue
            if sched_elem.get("enabled", "false").lower() != "true":
                continue
            cron_elem = sched_elem.find("cron")
            if cron_elem is None or not (cron_elem.text or "").strip():
                continue
            cron_expr = (cron_elem.text or "").strip()

            # Calculate next run time
            now = datetime.now(timezone.utc)
            try:
                next_time = _next_cron_time(cron_expr, now)
                next_run = next_time.isoformat() if next_time else None
            except Exception:
                next_run = None

            # Check if thread is actually running
            with self._lock:
                thread = self._threads.get(name)
                is_alive = thread is not None and thread.is_alive()

            results.append({
                "project": name,
                "cron": cron_expr,
                "next_run": next_run,
                "status": "active" if is_alive else "failed",
            })

        return results

    def scan_and_start_all(self) -> None:
        """Scan projects directory and start schedulers for enabled schedules."""
        if not self._projects_dir.exists():
            return
        for bdl_file in self._projects_dir.glob("*.bdl"):
            try:
                self._start_if_scheduled(bdl_file.stem)
            except Exception:
                logger.exception(
                    "Failed to start scheduler for project '%s'",
                    bdl_file.stem,
                )

    def reload(self, project_name: str) -> None:
        """Stop existing scheduler (if any) and restart if schedule is enabled."""
        self.stop(project_name)
        try:
            self._start_if_scheduled(project_name)
        except Exception:
            logger.exception(
                "Failed to reload scheduler for project '%s'",
                project_name,
            )

    def stop(self, project_name: str) -> None:
        """Stop the scheduler for a project if one is running."""
        with self._lock:
            stop_event = self._stop_events.pop(project_name, None)
            thread = self._threads.pop(project_name, None)
        if stop_event is not None:
            logger.info("Stopping scheduler for project '%s'", project_name)
            stop_event.set()
        if thread is not None:
            thread.join(timeout=10)

    def stop_all(self) -> None:
        """Stop all running schedulers (called on server shutdown)."""
        with self._lock:
            names = list(self._stop_events.keys())
        for name in names:
            self.stop(name)

    def _start_if_scheduled(self, project_name: str) -> None:
        """Check if project has an enabled schedule and start a cron thread."""
        import xml.etree.ElementTree as ET

        bdl_path = self._projects_dir / f"{project_name}.bdl"
        if not bdl_path.exists():
            return

        # Quick check using raw XML — avoids the full BDL parser
        # (which requires env vars for credential interpolation).
        try:
            tree = ET.parse(bdl_path)  # noqa: S314
            root = tree.getroot()
        except ET.ParseError:
            return

        sched_elem = root.find("schedule")
        if sched_elem is None:
            return
        if sched_elem.get("enabled", "false").lower() != "true":
            return
        cron_elem = sched_elem.find("cron")
        if cron_elem is None or not (cron_elem.text or "").strip():
            return
        cron_expr = (cron_elem.text or "").strip()

        # Check source/target connectors exist in the XML
        source_elem = root.find("source")
        target_elem = root.find("target")
        if source_elem is None or target_elem is None:
            logger.warning(
                "Project '%s' has a schedule but missing source/target config",
                project_name,
            )
            return

        # Start a lightweight cron thread — connectors created lazily per run
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._cron_loop,
            args=(project_name, bdl_path, cron_expr, stop_event),
            daemon=True,
            name=f"sched-{project_name}",
        )
        thread.start()

        with self._lock:
            self._stop_events[project_name] = stop_event
            self._threads[project_name] = thread

        logger.info(
            "Scheduler started for project '%s' (cron: %s)",
            project_name,
            cron_expr,
        )

    def _cron_loop(
        self,
        project_name: str,
        bdl_path: Path,
        cron_expr: str,
        stop_event: threading.Event,
    ) -> None:
        """Sleep until the next cron match, then run the migration."""
        from bani.application.scheduler import _next_cron_time

        while not stop_event.is_set():
            try:
                from datetime import datetime, timezone

                now = datetime.now(timezone.utc)
                next_time = _next_cron_time(cron_expr, now)
                if next_time is None:
                    logger.warning(
                        "No next cron time for project '%s'", project_name
                    )
                    break

                wait_seconds = (next_time - now).total_seconds()
                logger.info(
                    "Project '%s' next run in %.0f seconds (%s)",
                    project_name,
                    wait_seconds,
                    next_time.isoformat(),
                )

                # Sleep in 1-second intervals to respect stop_event
                while wait_seconds > 0 and not stop_event.is_set():
                    time.sleep(min(1.0, wait_seconds))
                    now = datetime.now(timezone.utc)
                    wait_seconds = (next_time - now).total_seconds()

                if stop_event.is_set():
                    break

                # Time to run — create connectors, execute, disconnect
                self._execute_scheduled_run(project_name, bdl_path, stop_event)

            except Exception:
                logger.exception(
                    "Scheduler error for project '%s'", project_name
                )
                # Wait before retrying to avoid tight error loops
                stop_event.wait(timeout=60)

    def _execute_scheduled_run(
        self,
        project_name: str,
        bdl_path: Path,
        stop_event: threading.Event,
    ) -> None:
        """Parse project, create connectors, run migration, disconnect."""
        from bani.application.checkpoint import CheckpointManager
        from bani.application.orchestrator import MigrationOrchestrator
        from bani.bdl.parser import parse
        from bani.connectors.base import SinkConnector, SourceConnector
        from bani.connectors.registry import ConnectorRegistry

        self._pre_setup_credentials_from_xml(bdl_path, project_name)
        project = parse(bdl_path)

        pool_size = (
            project.options.parallel_workers if project.options else 4
        )

        source_class = ConnectorRegistry.get(project.source.dialect)
        source = cast(type[SourceConnector], source_class)()
        source.connect(project.source, pool_size=pool_size)

        sink_class = ConnectorRegistry.get(project.target.dialect)
        sink = cast(type[SinkConnector], sink_class)()
        sink.connect(project.target, pool_size=pool_size)

        try:
            checkpoint = CheckpointManager()
            resume = checkpoint.load(project.name) is not None

            orchestrator = MigrationOrchestrator(
                project, source, sink, checkpoint=checkpoint
            )
            orchestrator.set_cancel_event(stop_event)

            result = orchestrator.execute(resume=resume)

            logger.info(
                "Scheduled run for '%s': %d tables, %d rows in %.1fs",
                project_name,
                result.tables_completed,
                result.total_rows_written,
                result.duration_seconds,
            )
        finally:
            try:
                source.disconnect()
            except Exception:
                pass
            try:
                sink.disconnect()
            except Exception:
                pass

    @staticmethod
    def _pre_setup_credentials_from_xml(
        bdl_path: Path, project_name: str
    ) -> None:
        """Set env vars for direct credentials before parsing.

        For direct credentials (e.g. ``username="postgres"``), we set
        env vars matching the raw value so the parser's interpolator
        resolves them.
        """
        import xml.etree.ElementTree as ET

        env_pattern = re.compile(r"^\$\{env:(.+)\}$")

        try:
            tree = ET.parse(bdl_path)  # noqa: S314
            root = tree.getroot()
        except ET.ParseError:
            return

        for tag in ["source", "target"]:
            wrapper = root.find(tag)
            if wrapper is None:
                continue
            conn = wrapper.find("connection")
            if conn is None:
                continue

            for attr in ["username", "password"]:
                val = conn.get(attr, "")
                if val and not env_pattern.match(val):
                    os.environ[val] = val
