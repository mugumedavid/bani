"""Checkpoint manager for migration resumability (Section 12.2).

Manages JSON checkpoint files at ``.bani/checkpoints/{project_name}.json``
that track per-table migration progress. On ``bani run --resume``, the
orchestrator reads the checkpoint to skip completed tables and resume
in-progress tables from their last committed row offset.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bani.domain.project import ProjectModel

logger = logging.getLogger(__name__)

_CHECKPOINT_DIR = Path(".bani") / "checkpoints"

# Valid per-table statuses
STATUS_PENDING = "pending"
STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"


class CheckpointManager:
    """Manages checkpoint files for migration resumability.

    Checkpoint files are stored as JSON under ``.bani/checkpoints/`` relative
    to the current working directory. Each project gets one checkpoint file.

    Thread safety: writes use atomic rename (write to temp file then
    ``os.replace``) to avoid corruption from concurrent access.
    """

    def __init__(self, base_dir: Path | None = None) -> None:
        """Initialize the checkpoint manager.

        Args:
            base_dir: Base directory for checkpoint storage. Defaults to
                the current working directory.
        """
        self._base_dir = base_dir or Path.cwd()

    def _checkpoint_path(self, project_name: str) -> Path:
        """Return the path to a project's checkpoint file.

        Args:
            project_name: Name of the migration project.

        Returns:
            Absolute path to the checkpoint JSON file.
        """
        return self._base_dir / _CHECKPOINT_DIR / f"{project_name}.json"

    @staticmethod
    def compute_hash(project: ProjectModel) -> str:
        """Compute a deterministic SHA-256 hash of a project's key fields.

        The hash covers fields that affect migration behaviour: name,
        source/target dialect, table mappings, type overrides, and options.
        If any of these change, a previous checkpoint is invalidated.

        Args:
            project: The project model to hash.

        Returns:
            Hex-encoded SHA-256 digest.
        """
        key_fields: dict[str, Any] = {
            "name": project.name,
            "source_dialect": project.source.dialect if project.source else None,
            "target_dialect": project.target.dialect if project.target else None,
            "source_host": project.source.host if project.source else None,
            "source_database": project.source.database if project.source else None,
            "target_host": project.target.host if project.target else None,
            "target_database": project.target.database if project.target else None,
            "table_mappings": [
                {
                    "source_schema": tm.source_schema,
                    "source_table": tm.source_table,
                    "target_schema": tm.target_schema,
                    "target_table": tm.target_table,
                }
                for tm in project.table_mappings
            ],
            "type_overrides": [
                {"source_type": to.source_type, "target_type": to.target_type}
                for to in project.type_overrides
            ],
            "batch_size": project.options.batch_size if project.options else None,
        }
        serialised = json.dumps(key_fields, sort_keys=True, default=str)
        return hashlib.sha256(serialised.encode("utf-8")).hexdigest()

    def create(
        self,
        project_name: str,
        project_hash: str,
        table_names: tuple[str, ...] | list[str],
    ) -> dict[str, Any]:
        """Create a new checkpoint file with all tables as 'pending'.

        Args:
            project_name: Name of the migration project.
            project_hash: SHA-256 hash of the project configuration.
            table_names: Ordered sequence of fully qualified table names.

        Returns:
            The checkpoint data dict.
        """
        tables: dict[str, dict[str, Any]] = {}
        for name in table_names:
            tables[name] = {
                "status": STATUS_PENDING,
                "last_row_offset": 0,
                "rows_completed": 0,
                "error_message": None,
            }

        checkpoint: dict[str, Any] = {
            "project_hash": project_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tables": tables,
        }

        self._write(project_name, checkpoint)
        logger.info("Checkpoint created for project '%s'", project_name)
        return checkpoint

    def load(self, project_name: str) -> dict[str, Any] | None:
        """Load an existing checkpoint file.

        Args:
            project_name: Name of the migration project.

        Returns:
            The checkpoint data dict, or ``None`` if no checkpoint exists.
        """
        path = self._checkpoint_path(project_name)
        if not path.exists():
            return None
        try:
            data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load checkpoint for '%s': %s", project_name, exc)
            return None

    def update_table_status(
        self,
        project_name: str,
        table_name: str,
        status: str,
        rows: int = 0,
        error: str | None = None,
    ) -> None:
        """Update a single table's status in the checkpoint.

        Args:
            project_name: Name of the migration project.
            table_name: Fully qualified table name.
            status: New status (pending, in_progress, completed, failed).
            rows: Number of rows completed so far.
            error: Error message (for failed status).
        """
        checkpoint = self.load(project_name)
        if checkpoint is None:
            return

        tables = checkpoint.get("tables", {})
        if table_name not in tables:
            tables[table_name] = {
                "status": STATUS_PENDING,
                "last_row_offset": 0,
                "rows_completed": 0,
                "error_message": None,
            }

        tables[table_name]["status"] = status
        if rows > 0:
            tables[table_name]["rows_completed"] = rows
        if error is not None:
            tables[table_name]["error_message"] = error

        checkpoint["tables"] = tables
        self._write(project_name, checkpoint)

    def update_row_offset(
        self,
        project_name: str,
        table_name: str,
        offset: int,
    ) -> None:
        """Update the last successfully committed row offset for a table.

        Called after each successful batch write to track resume position.

        Args:
            project_name: Name of the migration project.
            table_name: Fully qualified table name.
            offset: Row offset of the last successfully committed batch.
        """
        checkpoint = self.load(project_name)
        if checkpoint is None:
            return

        tables = checkpoint.get("tables", {})
        if table_name in tables:
            tables[table_name]["last_row_offset"] = offset
            tables[table_name]["rows_completed"] = offset
            checkpoint["tables"] = tables
            self._write(project_name, checkpoint)

    def is_table_completed(self, project_name: str, table_name: str) -> bool:
        """Check whether a table has been marked as completed.

        Args:
            project_name: Name of the migration project.
            table_name: Fully qualified table name.

        Returns:
            ``True`` if the table's status is 'completed'.
        """
        checkpoint = self.load(project_name)
        if checkpoint is None:
            return False
        tables = checkpoint.get("tables", {})
        entry = tables.get(table_name)
        if entry is None:
            return False
        return entry.get("status") == STATUS_COMPLETED  # type: ignore[no-any-return]

    def get_resume_offset(self, project_name: str, table_name: str) -> int:
        """Get the row offset from which to resume a table transfer.

        Args:
            project_name: Name of the migration project.
            table_name: Fully qualified table name.

        Returns:
            The last committed row offset, or ``0`` if no checkpoint exists.
        """
        checkpoint = self.load(project_name)
        if checkpoint is None:
            return 0
        tables = checkpoint.get("tables", {})
        entry = tables.get(table_name)
        if entry is None:
            return 0
        return int(entry.get("last_row_offset", 0))

    def is_valid(self, project_name: str, project_hash: str) -> bool:
        """Check whether a checkpoint matches the current project config.

        A checkpoint is valid when its stored hash matches the hash
        computed from the current ``ProjectModel``. If the project
        configuration has changed the checkpoint must be discarded.

        Args:
            project_name: Name of the migration project.
            project_hash: SHA-256 hash of the current project configuration.

        Returns:
            ``True`` if the checkpoint exists and its hash matches.
        """
        checkpoint = self.load(project_name)
        if checkpoint is None:
            return False
        return bool(checkpoint.get("project_hash") == project_hash)

    def clear(self, project_name: str) -> None:
        """Delete the checkpoint file for a project.

        Args:
            project_name: Name of the migration project.
        """
        path = self._checkpoint_path(project_name)
        try:
            path.unlink(missing_ok=True)
            logger.info("Checkpoint cleared for project '%s'", project_name)
        except OSError as exc:
            logger.warning("Failed to clear checkpoint for '%s': %s", project_name, exc)

    def _write(self, project_name: str, data: dict[str, Any]) -> None:
        """Atomically write checkpoint data to disk.

        Uses write-to-temp-then-rename to prevent corruption if the
        process is interrupted mid-write.

        Args:
            project_name: Name of the migration project.
            data: The checkpoint data dict.
        """
        path = self._checkpoint_path(project_name)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Write to a temp file in the same directory, then atomically replace.
        fd, tmp_path = tempfile.mkstemp(
            dir=str(path.parent), suffix=".tmp", prefix=".ckpt_"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp_path, str(path))
        except BaseException:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
