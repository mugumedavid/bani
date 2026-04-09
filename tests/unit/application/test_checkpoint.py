"""Tests for CheckpointManager (resumability protocol)."""

from __future__ import annotations

from pathlib import Path

from bani.application.checkpoint import (
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_IN_PROGRESS,
    STATUS_PENDING,
    CheckpointManager,
)
from bani.domain.project import ConnectionConfig, ProjectModel, ProjectOptions


def _make_project(name: str = "test_project") -> ProjectModel:
    """Create a minimal project model for testing."""
    return ProjectModel(
        name=name,
        source=ConnectionConfig(dialect="postgresql", host="localhost", database="src"),
        target=ConnectionConfig(dialect="mssql", host="localhost", database="tgt"),
        options=ProjectOptions(batch_size=50_000),
    )


class TestCreate:
    """Tests for CheckpointManager.create()."""

    def test_creates_checkpoint_file(self, tmp_path: Path) -> None:
        """create() should write a JSON checkpoint to disk."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "abc123", ("public.users", "public.orders"))

        data = mgr.load("proj")
        assert data is not None
        assert data["project_hash"] == "abc123"
        assert "created_at" in data
        assert "public.users" in data["tables"]
        assert "public.orders" in data["tables"]

    def test_all_tables_start_pending(self, tmp_path: Path) -> None:
        """All tables should start with status 'pending'."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "hash1", ("t1", "t2", "t3"))

        data = mgr.load("proj")
        assert data is not None
        for tbl in ("t1", "t2", "t3"):
            entry = data["tables"][tbl]
            assert entry["status"] == STATUS_PENDING
            assert entry["last_row_offset"] == 0
            assert entry["rows_completed"] == 0
            assert entry["error_message"] is None

    def test_create_overwrites_existing(self, tmp_path: Path) -> None:
        """create() should overwrite a previous checkpoint for the same project."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "hash_old", ("t1",))
        mgr.create("proj", "hash_new", ("t1", "t2"))

        data = mgr.load("proj")
        assert data is not None
        assert data["project_hash"] == "hash_new"
        assert len(data["tables"]) == 2


class TestLoad:
    """Tests for CheckpointManager.load()."""

    def test_returns_none_when_no_checkpoint(self, tmp_path: Path) -> None:
        """load() should return None if no checkpoint exists."""
        mgr = CheckpointManager(base_dir=tmp_path)
        assert mgr.load("nonexistent") is None

    def test_loads_existing_checkpoint(self, tmp_path: Path) -> None:
        """load() should return the stored checkpoint data."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))

        data = mgr.load("proj")
        assert data is not None
        assert data["project_hash"] == "h1"

    def test_returns_none_on_corrupt_json(self, tmp_path: Path) -> None:
        """load() should return None for a corrupt JSON file."""
        mgr = CheckpointManager(base_dir=tmp_path)
        path = mgr._checkpoint_path("proj")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("NOT VALID JSON{{{", encoding="utf-8")

        assert mgr.load("proj") is None


class TestUpdateTableStatus:
    """Tests for CheckpointManager.update_table_status()."""

    def test_updates_status(self, tmp_path: Path) -> None:
        """update_table_status() should change the table's status."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))

        mgr.update_table_status("proj", "t1", STATUS_IN_PROGRESS)
        data = mgr.load("proj")
        assert data is not None
        assert data["tables"]["t1"]["status"] == STATUS_IN_PROGRESS

    def test_updates_rows_and_error(self, tmp_path: Path) -> None:
        """update_table_status() should set rows and error fields."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))

        mgr.update_table_status(
            "proj", "t1", STATUS_FAILED, rows=5000, error="disk full"
        )
        data = mgr.load("proj")
        assert data is not None
        assert data["tables"]["t1"]["rows_completed"] == 5000
        assert data["tables"]["t1"]["error_message"] == "disk full"

    def test_noop_when_no_checkpoint(self, tmp_path: Path) -> None:
        """update_table_status() should not crash if no checkpoint exists."""
        mgr = CheckpointManager(base_dir=tmp_path)
        # Should not raise
        mgr.update_table_status("missing", "t1", STATUS_COMPLETED)

    def test_creates_table_entry_if_missing(self, tmp_path: Path) -> None:
        """update_table_status() should add a new table entry if needed."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))

        mgr.update_table_status("proj", "t2", STATUS_COMPLETED, rows=100)
        data = mgr.load("proj")
        assert data is not None
        assert "t2" in data["tables"]
        assert data["tables"]["t2"]["status"] == STATUS_COMPLETED


class TestUpdateRowOffset:
    """Tests for CheckpointManager.update_row_offset()."""

    def test_updates_offset(self, tmp_path: Path) -> None:
        """update_row_offset() should set last_row_offset."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))

        mgr.update_row_offset("proj", "t1", 50_000)
        data = mgr.load("proj")
        assert data is not None
        assert data["tables"]["t1"]["last_row_offset"] == 50_000
        assert data["tables"]["t1"]["rows_completed"] == 50_000

    def test_noop_when_no_checkpoint(self, tmp_path: Path) -> None:
        """update_row_offset() should not crash if no checkpoint exists."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.update_row_offset("missing", "t1", 1000)


class TestIsTableCompleted:
    """Tests for CheckpointManager.is_table_completed()."""

    def test_returns_true_for_completed(self, tmp_path: Path) -> None:
        """is_table_completed() should return True for completed tables."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))
        mgr.update_table_status("proj", "t1", STATUS_COMPLETED)

        assert mgr.is_table_completed("proj", "t1") is True

    def test_returns_false_for_pending(self, tmp_path: Path) -> None:
        """is_table_completed() should return False for pending tables."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))

        assert mgr.is_table_completed("proj", "t1") is False

    def test_returns_false_for_in_progress(self, tmp_path: Path) -> None:
        """is_table_completed() should return False for in-progress tables."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))
        mgr.update_table_status("proj", "t1", STATUS_IN_PROGRESS)

        assert mgr.is_table_completed("proj", "t1") is False

    def test_returns_false_for_missing_table(self, tmp_path: Path) -> None:
        """is_table_completed() should return False for unknown tables."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))

        assert mgr.is_table_completed("proj", "unknown") is False

    def test_returns_false_for_no_checkpoint(self, tmp_path: Path) -> None:
        """is_table_completed() should return False with no checkpoint."""
        mgr = CheckpointManager(base_dir=tmp_path)
        assert mgr.is_table_completed("proj", "t1") is False


class TestGetResumeOffset:
    """Tests for CheckpointManager.get_resume_offset()."""

    def test_returns_offset(self, tmp_path: Path) -> None:
        """get_resume_offset() should return the stored row offset."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))
        mgr.update_row_offset("proj", "t1", 75_000)

        assert mgr.get_resume_offset("proj", "t1") == 75_000

    def test_returns_zero_for_new_table(self, tmp_path: Path) -> None:
        """get_resume_offset() should return 0 for a fresh table."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))

        assert mgr.get_resume_offset("proj", "t1") == 0

    def test_returns_zero_for_no_checkpoint(self, tmp_path: Path) -> None:
        """get_resume_offset() should return 0 with no checkpoint."""
        mgr = CheckpointManager(base_dir=tmp_path)
        assert mgr.get_resume_offset("proj", "t1") == 0


class TestIsValid:
    """Tests for CheckpointManager.is_valid() — config invalidation."""

    def test_valid_when_hash_matches(self, tmp_path: Path) -> None:
        """is_valid() should return True when hashes match."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "abc123", ("t1",))

        assert mgr.is_valid("proj", "abc123") is True

    def test_invalid_when_hash_differs(self, tmp_path: Path) -> None:
        """is_valid() should return False when the project config has changed."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "old_hash", ("t1",))

        assert mgr.is_valid("proj", "new_hash") is False

    def test_invalid_when_no_checkpoint(self, tmp_path: Path) -> None:
        """is_valid() should return False when no checkpoint exists."""
        mgr = CheckpointManager(base_dir=tmp_path)
        assert mgr.is_valid("proj", "any_hash") is False


class TestClear:
    """Tests for CheckpointManager.clear()."""

    def test_deletes_checkpoint_file(self, tmp_path: Path) -> None:
        """clear() should remove the checkpoint file."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.create("proj", "h1", ("t1",))

        assert mgr.load("proj") is not None
        mgr.clear("proj")
        assert mgr.load("proj") is None

    def test_clear_nonexistent_is_noop(self, tmp_path: Path) -> None:
        """clear() should not raise for a missing checkpoint."""
        mgr = CheckpointManager(base_dir=tmp_path)
        mgr.clear("nonexistent")  # Should not raise


class TestComputeHash:
    """Tests for CheckpointManager.compute_hash()."""

    def test_deterministic(self) -> None:
        """Same project should produce the same hash."""
        project = _make_project()
        h1 = CheckpointManager.compute_hash(project)
        h2 = CheckpointManager.compute_hash(project)
        assert h1 == h2

    def test_different_projects_produce_different_hashes(self) -> None:
        """Different project configs should produce different hashes."""
        p1 = _make_project("proj_a")
        p2 = _make_project("proj_b")
        assert CheckpointManager.compute_hash(p1) != CheckpointManager.compute_hash(p2)

    def test_hash_changes_with_dialect(self) -> None:
        """Changing the target dialect should change the hash."""
        p1 = ProjectModel(
            name="proj",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mssql"),
        )
        p2 = ProjectModel(
            name="proj",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mysql"),
        )
        assert CheckpointManager.compute_hash(p1) != CheckpointManager.compute_hash(p2)

    def test_hash_is_sha256_hex(self) -> None:
        """Hash should be a 64-character hex string (SHA-256)."""
        project = _make_project()
        h = CheckpointManager.compute_hash(project)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


class TestResumeFlow:
    """End-to-end resume flow tests."""

    def test_full_resume_flow(self, tmp_path: Path) -> None:
        """Simulate a full create -> update -> resume check flow."""
        mgr = CheckpointManager(base_dir=tmp_path)
        project = _make_project()
        project_hash = mgr.compute_hash(project)

        # 1. Create checkpoint
        mgr.create(project.name, project_hash, ("t1", "t2", "t3"))

        # 2. Complete t1
        mgr.update_table_status(project.name, "t1", STATUS_IN_PROGRESS)
        mgr.update_row_offset(project.name, "t1", 50_000)
        mgr.update_row_offset(project.name, "t1", 100_000)
        mgr.update_table_status(project.name, "t1", STATUS_COMPLETED, rows=100_000)

        # 3. t2 in progress
        mgr.update_table_status(project.name, "t2", STATUS_IN_PROGRESS)
        mgr.update_row_offset(project.name, "t2", 25_000)

        # 4. t3 still pending

        # 5. Verify state
        assert mgr.is_table_completed(project.name, "t1") is True
        assert mgr.is_table_completed(project.name, "t2") is False
        assert mgr.is_table_completed(project.name, "t3") is False
        assert mgr.get_resume_offset(project.name, "t2") == 25_000
        assert mgr.get_resume_offset(project.name, "t3") == 0

        # 6. Validate checkpoint
        assert mgr.is_valid(project.name, project_hash) is True
        assert mgr.is_valid(project.name, "wrong_hash") is False
