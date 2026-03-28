"""Tests for OSSchedulerBridge (crontab integration)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from bani.domain.errors import SchedulerError
from bani.infra.os_scheduler import (
    OSSchedulerBridge,
    _filter_out_project,
    _project_name_from_path,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_subprocess_run(
    stdout: str = "",
    stderr: str = "",
    returncode: int = 0,
) -> MagicMock:
    """Create a mock for subprocess.run with specified outputs."""
    mock_result = MagicMock()
    mock_result.stdout = stdout
    mock_result.stderr = stderr
    mock_result.returncode = returncode
    return mock_result


# ---------------------------------------------------------------------------
# _project_name_from_path
# ---------------------------------------------------------------------------


class TestProjectNameFromPath:
    """Tests for _project_name_from_path()."""

    def test_simple_bdl_file(self) -> None:
        """Should extract stem from simple path."""
        assert _project_name_from_path("my_project.bdl") == "my_project"

    def test_full_path(self) -> None:
        """Should extract stem from full path."""
        assert _project_name_from_path("/home/user/projects/migrate.bdl") == "migrate"

    def test_no_extension(self) -> None:
        """Should return full name when no extension."""
        assert _project_name_from_path("project_name") == "project_name"

    def test_windows_path(self) -> None:
        """Should handle Windows-style paths."""
        assert _project_name_from_path("C:\\Users\\dev\\project.bdl") == "project"


# ---------------------------------------------------------------------------
# _filter_out_project
# ---------------------------------------------------------------------------


class TestFilterOutProject:
    """Tests for _filter_out_project()."""

    def test_removes_tagged_entry(self) -> None:
        """Should remove the tag line and the following cron line."""
        crontab = (
            "0 * * * * /some/other/job\n"
            "# bani:my_project\n"
            "0 2 * * * bani run my_project.bdl\n"
            "30 * * * * /another/job\n"
        )
        result = _filter_out_project(crontab, "my_project")
        assert len(result) == 2
        assert "# bani:my_project" not in "\n".join(result)
        assert "my_project.bdl" not in "\n".join(result)

    def test_preserves_other_entries(self) -> None:
        """Should keep non-matching crontab lines."""
        crontab = (
            "0 * * * * /some/job\n# bani:other_project\n0 3 * * * bani run other.bdl\n"
        )
        result = _filter_out_project(crontab, "my_project")
        assert len(result) == 3

    def test_empty_crontab(self) -> None:
        """Should handle empty crontab gracefully."""
        result = _filter_out_project("", "my_project")
        assert result == []


# ---------------------------------------------------------------------------
# OSSchedulerBridge.register
# ---------------------------------------------------------------------------


class TestRegister:
    """Tests for OSSchedulerBridge.register()."""

    @patch("bani.infra.os_scheduler.platform")
    def test_raises_on_windows(self, mock_platform: MagicMock) -> None:
        """Should raise SchedulerError on Windows."""
        mock_platform.system.return_value = "Windows"
        with pytest.raises(SchedulerError, match="Windows"):
            OSSchedulerBridge.register("/path/to/project.bdl", "0 2 * * *")

    @patch("bani.infra.os_scheduler._write_crontab")
    @patch("bani.infra.os_scheduler._read_crontab")
    @patch("bani.infra.os_scheduler.shutil")
    @patch("bani.infra.os_scheduler.platform")
    def test_adds_crontab_entry(
        self,
        mock_platform: MagicMock,
        mock_shutil: MagicMock,
        mock_read: MagicMock,
        mock_write: MagicMock,
    ) -> None:
        """Should add a tagged crontab entry."""
        mock_platform.system.return_value = "Linux"
        mock_shutil.which.return_value = "/usr/bin/crontab"
        mock_read.return_value = ""

        OSSchedulerBridge.register(
            "/path/to/project.bdl", "0 2 * * *", "Africa/Nairobi"
        )

        mock_write.assert_called_once()
        written = mock_write.call_args[0][0]
        assert "# bani:project" in written
        assert "0 2 * * *" in written
        assert "TZ=Africa/Nairobi" in written
        assert "project.bdl" in written

    @patch("bani.infra.os_scheduler._write_crontab")
    @patch("bani.infra.os_scheduler._read_crontab")
    @patch("bani.infra.os_scheduler.shutil")
    @patch("bani.infra.os_scheduler.platform")
    def test_replaces_existing_entry(
        self,
        mock_platform: MagicMock,
        mock_shutil: MagicMock,
        mock_read: MagicMock,
        mock_write: MagicMock,
    ) -> None:
        """Should replace an existing entry for the same project."""
        mock_platform.system.return_value = "Linux"
        mock_shutil.which.return_value = "/usr/bin/crontab"
        mock_read.return_value = (
            "# bani:project\nTZ=UTC 0 1 * * * bani run /path/to/project.bdl\n"
        )

        OSSchedulerBridge.register("/path/to/project.bdl", "30 3 * * *", "UTC")

        mock_write.assert_called_once()
        written = mock_write.call_args[0][0]
        # Should have the new cron, not the old one
        assert "30 3 * * *" in written
        # Old entry should be gone
        assert "0 1 * * *" not in written

    @patch("bani.infra.os_scheduler.shutil")
    @patch("bani.infra.os_scheduler.platform")
    def test_raises_when_crontab_not_found(
        self,
        mock_platform: MagicMock,
        mock_shutil: MagicMock,
    ) -> None:
        """Should raise if crontab command is not on PATH."""
        mock_platform.system.return_value = "Linux"
        mock_shutil.which.return_value = None

        with pytest.raises(SchedulerError, match="crontab command not found"):
            OSSchedulerBridge.register("/path/to/project.bdl", "0 2 * * *")


# ---------------------------------------------------------------------------
# OSSchedulerBridge.unregister
# ---------------------------------------------------------------------------


class TestUnregister:
    """Tests for OSSchedulerBridge.unregister()."""

    @patch("bani.infra.os_scheduler.platform")
    def test_raises_on_windows(self, mock_platform: MagicMock) -> None:
        """Should raise SchedulerError on Windows."""
        mock_platform.system.return_value = "Windows"
        with pytest.raises(SchedulerError, match="Windows"):
            OSSchedulerBridge.unregister("my_project")

    @patch("bani.infra.os_scheduler._write_crontab")
    @patch("bani.infra.os_scheduler._read_crontab")
    @patch("bani.infra.os_scheduler.platform")
    def test_removes_correct_entry(
        self,
        mock_platform: MagicMock,
        mock_read: MagicMock,
        mock_write: MagicMock,
    ) -> None:
        """Should remove only the matching project's entry."""
        mock_platform.system.return_value = "Linux"
        mock_read.return_value = (
            "0 * * * * /some/job\n"
            "# bani:my_project\n"
            "0 2 * * * bani run my_project.bdl\n"
            "# bani:other_project\n"
            "0 3 * * * bani run other.bdl\n"
        )

        OSSchedulerBridge.unregister("my_project")

        mock_write.assert_called_once()
        written = mock_write.call_args[0][0]
        assert "# bani:my_project" not in written
        assert "my_project.bdl" not in written
        assert "# bani:other_project" in written
        assert "other.bdl" in written

    @patch("bani.infra.os_scheduler._write_crontab")
    @patch("bani.infra.os_scheduler._read_crontab")
    @patch("bani.infra.os_scheduler.platform")
    def test_noop_when_not_registered(
        self,
        mock_platform: MagicMock,
        mock_read: MagicMock,
        mock_write: MagicMock,
    ) -> None:
        """Should not crash when project is not registered."""
        mock_platform.system.return_value = "Linux"
        mock_read.return_value = "0 * * * * /some/job\n"

        OSSchedulerBridge.unregister("nonexistent")

        mock_write.assert_called_once()


# ---------------------------------------------------------------------------
# OSSchedulerBridge.list_registered
# ---------------------------------------------------------------------------


class TestListRegistered:
    """Tests for OSSchedulerBridge.list_registered()."""

    @patch("bani.infra.os_scheduler.platform")
    def test_raises_on_windows(self, mock_platform: MagicMock) -> None:
        """Should raise SchedulerError on Windows."""
        mock_platform.system.return_value = "Windows"
        with pytest.raises(SchedulerError, match="Windows"):
            OSSchedulerBridge.list_registered()

    @patch("bani.infra.os_scheduler._read_crontab")
    @patch("bani.infra.os_scheduler.platform")
    def test_lists_registered_projects(
        self,
        mock_platform: MagicMock,
        mock_read: MagicMock,
    ) -> None:
        """Should return all Bani-tagged entries."""
        mock_platform.system.return_value = "Linux"
        mock_read.return_value = (
            "0 * * * * /some/job\n"
            "# bani:project_a\n"
            "0 2 * * * bani run project_a.bdl\n"
            "# bani:project_b\n"
            "0 3 * * * bani run project_b.bdl\n"
        )

        result = OSSchedulerBridge.list_registered()

        assert len(result) == 2
        assert result[0]["project_name"] == "project_a"
        assert "0 2 * * *" in result[0]["cron_entry"]
        assert result[1]["project_name"] == "project_b"
        assert "0 3 * * *" in result[1]["cron_entry"]

    @patch("bani.infra.os_scheduler._read_crontab")
    @patch("bani.infra.os_scheduler.platform")
    def test_empty_crontab(
        self,
        mock_platform: MagicMock,
        mock_read: MagicMock,
    ) -> None:
        """Should return empty list for empty crontab."""
        mock_platform.system.return_value = "Linux"
        mock_read.return_value = ""

        result = OSSchedulerBridge.list_registered()
        assert result == []

    @patch("bani.infra.os_scheduler._read_crontab")
    @patch("bani.infra.os_scheduler.platform")
    def test_no_bani_entries(
        self,
        mock_platform: MagicMock,
        mock_read: MagicMock,
    ) -> None:
        """Should return empty list when no Bani entries exist."""
        mock_platform.system.return_value = "Linux"
        mock_read.return_value = "0 * * * * /some/other/job\n"

        result = OSSchedulerBridge.list_registered()
        assert result == []
