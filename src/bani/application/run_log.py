"""Append-only JSONL run history log.

Stores migration run entries in ``~/.bani/run_history.jsonl``, capped at
:data:`MAX_ENTRIES` to prevent unbounded growth.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

MAX_ENTRIES = 1000


@dataclass(frozen=True)
class RunLogEntry:
    """A single migration run record."""

    project_name: str
    started_at: str  # ISO timestamp
    finished_at: str  # ISO timestamp
    status: str  # "completed" | "failed"
    tables_completed: int
    tables_failed: int
    total_rows: int
    duration_seconds: float
    error: str | None = None


class RunLog:
    """File-backed run history log using JSONL format.

    Args:
        path: Override for the log file location (defaults to
            ``~/.bani/run_history.jsonl``).
    """

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or Path("~/.bani/run_history.jsonl").expanduser()

    def append(self, entry: RunLogEntry) -> None:
        """Append an entry. If file exceeds MAX_ENTRIES, trim oldest.

        Args:
            entry: The run log entry to persist.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)

        # Read existing entries
        entries = self._read_all()
        entries.append(self._to_dict(entry))

        # Trim to last MAX_ENTRIES
        if len(entries) > MAX_ENTRIES:
            entries = entries[-MAX_ENTRIES:]

        # Write back
        self._path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")

    def recent(self, n: int = 10) -> list[dict[str, object]]:
        """Return the last *n* entries, newest first.

        Args:
            n: Maximum number of entries to return.

        Returns:
            List of run log dicts, newest first.
        """
        entries = self._read_all()
        return list(reversed(entries[-n:]))

    def summary(self) -> dict[str, object]:
        """Return summary stats for the dashboard.

        Returns:
            Dict with ``total_runs``, ``last_run``, and ``lifetime_rows``.
        """
        entries = self._read_all()
        if not entries:
            return {"total_runs": 0, "last_run": None, "lifetime_rows": 0}
        last = entries[-1]
        lifetime_rows = sum(e.get("total_rows", 0) for e in entries)  # type: ignore[arg-type]
        return {
            "total_runs": len(entries),
            "last_run": last,
            "lifetime_rows": lifetime_rows,
        }

    def clear(self) -> None:
        """Delete all run history entries."""
        if self._path.exists():
            self._path.unlink()

    def _read_all(self) -> list[dict[str, object]]:
        """Read all entries from the JSONL file.

        Corrupt lines are silently skipped.

        Returns:
            List of run log dicts in chronological order.
        """
        if not self._path.exists():
            return []
        text = self._path.read_text().strip()
        if not text:
            return []
        lines = text.split("\n")
        result: list[dict[str, object]] = []
        for line in lines:
            if line.strip():
                try:
                    result.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return result

    @staticmethod
    def _to_dict(entry: RunLogEntry) -> dict[str, object]:
        """Convert a :class:`RunLogEntry` to a plain dict.

        Args:
            entry: The entry to convert.

        Returns:
            Dict representation suitable for JSON serialisation.
        """
        return {
            "project_name": entry.project_name,
            "started_at": entry.started_at,
            "finished_at": entry.finished_at,
            "status": entry.status,
            "tables_completed": entry.tables_completed,
            "tables_failed": entry.tables_failed,
            "total_rows": entry.total_rows,
            "duration_seconds": entry.duration_seconds,
            "error": entry.error,
        }
