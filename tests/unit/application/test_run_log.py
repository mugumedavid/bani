"""Tests for the RunLog append-only JSONL run history."""

from __future__ import annotations

from pathlib import Path

from bani.application.run_log import MAX_ENTRIES, RunLog, RunLogEntry


def _make_entry(
    *,
    project_name: str = "test_project",
    status: str = "completed",
    total_rows: int = 100,
    idx: int = 0,
) -> RunLogEntry:
    """Create a RunLogEntry for testing."""
    return RunLogEntry(
        project_name=project_name,
        started_at=f"2025-01-01T00:00:{idx:02d}Z",
        finished_at=f"2025-01-01T00:01:{idx:02d}Z",
        status=status,
        tables_completed=5 if status == "completed" else 3,
        tables_failed=0 if status == "completed" else 2,
        total_rows=total_rows,
        duration_seconds=60.0 + idx,
        error=None if status == "completed" else "something went wrong",
    )


class TestRunLogAppendAndRead:
    """Tests for appending entries and reading them back."""

    def test_append_and_read_back(self, tmp_path: Path) -> None:
        """Appended entries can be read back correctly."""
        log = RunLog(path=tmp_path / "history.jsonl")
        entry = _make_entry()
        log.append(entry)

        entries = log.recent(10)
        assert len(entries) == 1
        assert entries[0]["project_name"] == "test_project"
        assert entries[0]["status"] == "completed"
        assert entries[0]["total_rows"] == 100
        assert entries[0]["duration_seconds"] == 60.0

    def test_append_multiple(self, tmp_path: Path) -> None:
        """Multiple entries are appended in order."""
        log = RunLog(path=tmp_path / "history.jsonl")
        for i in range(5):
            log.append(_make_entry(idx=i))

        entries = log.recent(10)
        assert len(entries) == 5

    def test_failed_entry_fields(self, tmp_path: Path) -> None:
        """Failed entries store error and correct status."""
        log = RunLog(path=tmp_path / "history.jsonl")
        entry = _make_entry(status="failed")
        log.append(entry)

        entries = log.recent(1)
        assert entries[0]["status"] == "failed"
        assert entries[0]["tables_failed"] == 2
        assert entries[0]["error"] == "something went wrong"


class TestRunLogRecent:
    """Tests for the recent() method."""

    def test_recent_returns_newest_first(self, tmp_path: Path) -> None:
        """recent() returns entries in reverse chronological order."""
        log = RunLog(path=tmp_path / "history.jsonl")
        for i in range(5):
            log.append(_make_entry(project_name=f"project_{i}", idx=i))

        entries = log.recent(5)
        assert entries[0]["project_name"] == "project_4"
        assert entries[4]["project_name"] == "project_0"

    def test_recent_respects_limit(self, tmp_path: Path) -> None:
        """recent(n) returns at most n entries."""
        log = RunLog(path=tmp_path / "history.jsonl")
        for i in range(10):
            log.append(_make_entry(idx=i))

        entries = log.recent(3)
        assert len(entries) == 3

    def test_recent_with_fewer_entries_than_requested(self, tmp_path: Path) -> None:
        """recent(n) returns all entries when fewer than n exist."""
        log = RunLog(path=tmp_path / "history.jsonl")
        log.append(_make_entry())

        entries = log.recent(10)
        assert len(entries) == 1


class TestRunLogSummary:
    """Tests for the summary() method."""

    def test_summary_returns_correct_stats(self, tmp_path: Path) -> None:
        """summary() returns correct total_runs, last_run, lifetime_rows."""
        log = RunLog(path=tmp_path / "history.jsonl")
        log.append(_make_entry(total_rows=100, idx=0))
        log.append(_make_entry(total_rows=200, idx=1))
        log.append(_make_entry(total_rows=300, idx=2))

        s = log.summary()
        assert s["total_runs"] == 3
        assert s["lifetime_rows"] == 600
        assert s["last_run"] is not None
        last = s["last_run"]
        assert isinstance(last, dict)
        assert last["total_rows"] == 300

    def test_summary_empty(self, tmp_path: Path) -> None:
        """summary() returns zeroed stats when log is empty."""
        log = RunLog(path=tmp_path / "history.jsonl")
        s = log.summary()
        assert s["total_runs"] == 0
        assert s["last_run"] is None
        assert s["lifetime_rows"] == 0


class TestRunLogCap:
    """Tests for the MAX_ENTRIES cap."""

    def test_cap_at_max_entries(self, tmp_path: Path) -> None:
        """Entries beyond MAX_ENTRIES are trimmed (oldest removed)."""
        # Use a smaller cap for testing by monkeypatching
        import bani.application.run_log as run_log_mod

        original = run_log_mod.MAX_ENTRIES
        try:
            run_log_mod.MAX_ENTRIES = 5
            log = RunLog(path=tmp_path / "history.jsonl")
            for i in range(10):
                log.append(_make_entry(project_name=f"p_{i}", idx=i))

            entries = log.recent(100)
            assert len(entries) == 5
            # Should have p_5 through p_9 (newest)
            names = [e["project_name"] for e in entries]
            assert names == ["p_9", "p_8", "p_7", "p_6", "p_5"]
        finally:
            run_log_mod.MAX_ENTRIES = original

    def test_default_max_entries_value(self) -> None:
        """MAX_ENTRIES is set to 1000."""
        assert MAX_ENTRIES == 1000


class TestRunLogEdgeCases:
    """Tests for edge cases: missing file, empty file, corrupt lines."""

    def test_missing_file(self, tmp_path: Path) -> None:
        """Reading from a non-existent file returns empty results."""
        log = RunLog(path=tmp_path / "does_not_exist.jsonl")
        assert log.recent(10) == []
        assert log.summary() == {
            "total_runs": 0,
            "last_run": None,
            "lifetime_rows": 0,
        }

    def test_empty_file(self, tmp_path: Path) -> None:
        """Reading from an empty file returns empty results."""
        path = tmp_path / "empty.jsonl"
        path.write_text("")
        log = RunLog(path=path)
        assert log.recent(10) == []
        assert log.summary()["total_runs"] == 0

    def test_corrupt_lines_are_skipped(self, tmp_path: Path) -> None:
        """Corrupt JSON lines are silently skipped."""
        path = tmp_path / "corrupt.jsonl"
        path.write_text(
            '{"project_name":"good","started_at":"t","finished_at":"t",'
            '"status":"completed","tables_completed":1,"tables_failed":0,'
            '"total_rows":10,"duration_seconds":1.0,"error":null}\n'
            "this is not json\n"
            '{"project_name":"also_good","started_at":"t","finished_at":"t",'
            '"status":"completed","tables_completed":2,"tables_failed":0,'
            '"total_rows":20,"duration_seconds":2.0,"error":null}\n'
        )
        log = RunLog(path=path)
        entries = log.recent(10)
        assert len(entries) == 2
        assert entries[0]["project_name"] == "also_good"
        assert entries[1]["project_name"] == "good"

    def test_whitespace_only_file(self, tmp_path: Path) -> None:
        """A file containing only whitespace is treated as empty."""
        path = tmp_path / "whitespace.jsonl"
        path.write_text("   \n\n  \n")
        log = RunLog(path=path)
        assert log.recent(10) == []

    def test_append_creates_parent_directories(self, tmp_path: Path) -> None:
        """Appending to a path with non-existent parents creates them."""
        path = tmp_path / "a" / "b" / "c" / "history.jsonl"
        log = RunLog(path=path)
        log.append(_make_entry())
        assert path.exists()
        assert len(log.recent(10)) == 1
