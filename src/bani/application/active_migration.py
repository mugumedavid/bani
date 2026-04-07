"""Track active (in-progress) migrations via marker files.

Writes a JSON marker to ``~/.bani/active/{project_name}.json`` when a
migration starts and removes it when the migration finishes (success or
failure).  This allows the Web UI to detect migrations started from any
caller (MCP, CLI, SDK) and display them on the dashboard.

Stale markers (from crashed processes) are detected by checking whether
the recorded PID is still alive.
"""

from __future__ import annotations

import json
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

_DEFAULT_ACTIVE_DIR = Path("~/.bani/active")


class ActiveMigrationTracker:
    """File-based tracker for in-progress migrations.

    Args:
        active_dir: Override for the marker directory (defaults to
            ``~/.bani/active``).
    """

    def __init__(self, active_dir: Path | None = None) -> None:
        self._dir = (active_dir or _DEFAULT_ACTIVE_DIR).expanduser()

    def _marker_path(self, project_name: str) -> Path:
        return self._dir / f"{project_name}.json"

    def start(
        self,
        project_name: str,
        source_dialect: str,
        target_dialect: str,
    ) -> None:
        """Write a marker indicating this project is being migrated.

        Args:
            project_name: Name of the migration project.
            source_dialect: Source connector dialect (e.g. ``postgresql``).
            target_dialect: Target connector dialect (e.g. ``mysql``).
        """
        self._dir.mkdir(parents=True, exist_ok=True)
        marker = {
            "project_name": project_name,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "pid": os.getpid(),
            "source_dialect": source_dialect,
            "target_dialect": target_dialect,
        }
        self._marker_path(project_name).write_text(
            json.dumps(marker, indent=2), encoding="utf-8"
        )

    def finish(self, project_name: str) -> None:
        """Remove the active marker for a project.

        Safe to call even if no marker exists.

        Args:
            project_name: Name of the migration project.
        """
        path = self._marker_path(project_name)
        if path.exists():
            path.unlink(missing_ok=True)

    def list_active(self) -> list[dict[str, object]]:
        """Return all active migrations, excluding stale markers.

        A marker is considered stale if its PID is no longer alive.
        Stale markers are automatically cleaned up.

        Returns:
            List of active migration dicts, one per project.
        """
        if not self._dir.exists():
            return []
        active: list[dict[str, object]] = []
        for path in sorted(self._dir.glob("*.json")):
            try:
                data: dict[str, object] = json.loads(
                    path.read_text(encoding="utf-8")
                )
            except (json.JSONDecodeError, OSError):
                continue
            pid = data.get("pid")
            if isinstance(pid, int) and not _pid_alive(pid):
                # Stale marker — process crashed.
                path.unlink(missing_ok=True)
                continue
            active.append(data)
        return active

    def is_active(self, project_name: str) -> bool:
        """Check whether a project is currently being migrated.

        Args:
            project_name: Name of the migration project.

        Returns:
            ``True`` if a live marker exists for this project.
        """
        path = self._marker_path(project_name)
        if not path.exists():
            return False
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return False
        pid = data.get("pid")
        if isinstance(pid, int) and not _pid_alive(pid):
            path.unlink(missing_ok=True)
            return False
        return True


def _pid_alive(pid: int) -> bool:
    """Check whether a process with the given PID is still running."""
    try:
        os.kill(pid, signal.SIG_DFL)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we can't signal it — still alive.
        return True
