"""Tests for SchedulerService and cron parser."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from bani.application.orchestrator import MigrationResult
from bani.application.progress import ProgressTracker
from bani.application.scheduler import (
    SchedulerService,
    _matches_cron,
    _next_cron_time,
    _parse_cron,
    _parse_cron_field,
)
from bani.domain.errors import SchedulerError
from bani.domain.project import (
    ConnectionConfig,
    ProjectModel,
    ScheduleConfig,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_project(
    name: str = "test_project",
    cron: str = "0 2 * * *",
    timezone: str = "UTC",
    max_retries: int = 0,
    retry_delay_seconds: int = 0,
    enabled: bool = True,
) -> ProjectModel:
    """Create a minimal project with schedule config."""
    return ProjectModel(
        name=name,
        source=ConnectionConfig(dialect="postgresql", host="localhost", database="src"),
        target=ConnectionConfig(dialect="mssql", host="localhost", database="tgt"),
        schedule=ScheduleConfig(
            enabled=enabled,
            cron=cron,
            timezone=timezone,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        ),
    )


def _make_migration_result(
    project_name: str = "test_project",
    tables_completed: int = 5,
    tables_failed: int = 0,
    total_rows_read: int = 1000,
    total_rows_written: int = 1000,
    duration_seconds: float = 10.0,
) -> MigrationResult:
    """Create a successful MigrationResult."""
    return MigrationResult(
        project_name=project_name,
        tables_completed=tables_completed,
        tables_failed=tables_failed,
        total_rows_read=total_rows_read,
        total_rows_written=total_rows_written,
        duration_seconds=duration_seconds,
    )


def _mock_source() -> MagicMock:
    """Create a mock SourceConnector."""
    return MagicMock()


def _mock_sink() -> MagicMock:
    """Create a mock SinkConnector."""
    return MagicMock()


# ---------------------------------------------------------------------------
# Cron field parsing
# ---------------------------------------------------------------------------


class TestParseCronField:
    """Tests for _parse_cron_field()."""

    def test_wildcard(self) -> None:
        """Wildcard should match all values in range."""
        result = _parse_cron_field("*", 0, 59)
        assert result == set(range(0, 60))

    def test_specific_value(self) -> None:
        """A specific value should produce a single-element set."""
        result = _parse_cron_field("30", 0, 59)
        assert result == {30}

    def test_range(self) -> None:
        """A range should produce all values between start and end."""
        result = _parse_cron_field("1-5", 0, 6)
        assert result == {1, 2, 3, 4, 5}

    def test_step(self) -> None:
        """Wildcard with step should produce evenly spaced values."""
        result = _parse_cron_field("*/15", 0, 59)
        assert result == {0, 15, 30, 45}

    def test_range_with_step(self) -> None:
        """A range with step should produce stepped values within range."""
        result = _parse_cron_field("1-30/5", 0, 59)
        assert result == {1, 6, 11, 16, 21, 26}

    def test_comma_list(self) -> None:
        """Comma-separated values should produce a union set."""
        result = _parse_cron_field("1,15,30", 0, 59)
        assert result == {1, 15, 30}

    def test_values_clipped_to_range(self) -> None:
        """Values outside the valid range should be excluded."""
        result = _parse_cron_field("58-62", 0, 59)
        assert result == {58, 59}

    def test_invalid_step_raises(self) -> None:
        """Non-numeric step should raise SchedulerError."""
        with pytest.raises(SchedulerError, match="Invalid step"):
            _parse_cron_field("*/abc", 0, 59)

    def test_empty_element_raises(self) -> None:
        """Empty list element should raise SchedulerError."""
        with pytest.raises(SchedulerError, match="Empty element"):
            _parse_cron_field("1,,3", 0, 59)


# ---------------------------------------------------------------------------
# Full cron expression parsing
# ---------------------------------------------------------------------------


class TestParseCron:
    """Tests for _parse_cron()."""

    def test_every_minute(self) -> None:
        """'* * * * *' should match all values for all fields."""
        minutes, hours, doms, months, dows = _parse_cron("* * * * *")
        assert minutes == set(range(0, 60))
        assert hours == set(range(0, 24))
        assert doms == set(range(1, 32))
        assert months == set(range(1, 13))
        assert dows == set(range(0, 7))

    def test_specific_time(self) -> None:
        """'30 2 * * *' should match minute=30, hour=2."""
        minutes, hours, _doms, _months, _dows = _parse_cron("30 2 * * *")
        assert minutes == {30}
        assert hours == {2}

    def test_weekday_range(self) -> None:
        """'0 */6 * * 1-5' should match hours 0,6,12,18 and weekdays 1-5."""
        minutes, hours, _doms, _months, dows = _parse_cron("0 */6 * * 1-5")
        assert minutes == {0}
        assert hours == {0, 6, 12, 18}
        assert dows == {1, 2, 3, 4, 5}

    def test_first_of_month(self) -> None:
        """'0 2 1 * *' should match day-of-month=1."""
        minutes, hours, doms, _months, _dows = _parse_cron("0 2 1 * *")
        assert doms == {1}
        assert minutes == {0}
        assert hours == {2}

    def test_wrong_field_count_raises(self) -> None:
        """Cron expression with wrong number of fields should raise."""
        with pytest.raises(SchedulerError, match="5 fields"):
            _parse_cron("* * *")

    def test_six_fields_raises(self) -> None:
        """Six-field cron expression should raise."""
        with pytest.raises(SchedulerError, match="5 fields"):
            _parse_cron("0 0 * * * *")


# ---------------------------------------------------------------------------
# _matches_cron
# ---------------------------------------------------------------------------


class TestMatchesCron:
    """Tests for _matches_cron()."""

    def test_every_minute_always_matches(self) -> None:
        """'* * * * *' should match any datetime."""
        dt = datetime(2025, 6, 15, 14, 30)
        assert _matches_cron("* * * * *", dt) is True

    def test_specific_time_matches(self) -> None:
        """'30 2 * * *' should match 02:30 on any day."""
        dt = datetime(2025, 6, 15, 2, 30)
        assert _matches_cron("30 2 * * *", dt) is True

    def test_specific_time_no_match_minute(self) -> None:
        """'30 2 * * *' should not match 02:15."""
        dt = datetime(2025, 6, 15, 2, 15)
        assert _matches_cron("30 2 * * *", dt) is False

    def test_specific_time_no_match_hour(self) -> None:
        """'30 2 * * *' should not match 03:30."""
        dt = datetime(2025, 6, 15, 3, 30)
        assert _matches_cron("30 2 * * *", dt) is False

    def test_day_of_week_sunday(self) -> None:
        """'0 0 * * 0' should match Sunday (cron dow=0)."""
        # 2025-06-15 is a Sunday
        dt = datetime(2025, 6, 15, 0, 0)
        assert _matches_cron("0 0 * * 0", dt) is True

    def test_day_of_week_monday(self) -> None:
        """'0 0 * * 1' should match Monday (cron dow=1)."""
        # 2025-06-16 is a Monday
        dt = datetime(2025, 6, 16, 0, 0)
        assert _matches_cron("0 0 * * 1", dt) is True

    def test_day_of_week_no_match(self) -> None:
        """'0 0 * * 1' should not match Sunday."""
        # 2025-06-15 is a Sunday
        dt = datetime(2025, 6, 15, 0, 0)
        assert _matches_cron("0 0 * * 1", dt) is False

    def test_specific_month(self) -> None:
        """'0 0 1 6 *' should match June 1st at midnight."""
        dt = datetime(2025, 6, 1, 0, 0)
        assert _matches_cron("0 0 1 6 *", dt) is True

    def test_specific_month_no_match(self) -> None:
        """'0 0 1 6 *' should not match July 1st."""
        dt = datetime(2025, 7, 1, 0, 0)
        assert _matches_cron("0 0 1 6 *", dt) is False

    def test_step_hours(self) -> None:
        """'0 */6 * * *' should match hours 0, 6, 12, 18."""
        for hour in (0, 6, 12, 18):
            dt = datetime(2025, 6, 15, hour, 0)
            assert _matches_cron("0 */6 * * *", dt) is True

        for hour in (1, 3, 7, 13, 23):
            dt = datetime(2025, 6, 15, hour, 0)
            assert _matches_cron("0 */6 * * *", dt) is False


# ---------------------------------------------------------------------------
# _next_cron_time
# ---------------------------------------------------------------------------


class TestNextCronTime:
    """Tests for _next_cron_time()."""

    def test_next_minute(self) -> None:
        """'* * * * *' from 14:30:00 should give 14:31."""
        after = datetime(2025, 6, 15, 14, 30, 0)
        result = _next_cron_time("* * * * *", after)
        assert result == datetime(2025, 6, 15, 14, 31)

    def test_next_specific_time(self) -> None:
        """'30 2 * * *' from 14:30 should give tomorrow 02:30."""
        after = datetime(2025, 6, 15, 14, 30, 0)
        result = _next_cron_time("30 2 * * *", after)
        assert result == datetime(2025, 6, 16, 2, 30)

    def test_next_same_day(self) -> None:
        """'30 14 * * *' from 13:00 should give same day 14:30."""
        after = datetime(2025, 6, 15, 13, 0, 0)
        result = _next_cron_time("30 14 * * *", after)
        assert result == datetime(2025, 6, 15, 14, 30)

    def test_next_crosses_midnight(self) -> None:
        """'0 0 * * *' from 23:30 should give next day midnight."""
        after = datetime(2025, 6, 15, 23, 30, 0)
        result = _next_cron_time("0 0 * * *", after)
        assert result == datetime(2025, 6, 16, 0, 0)

    def test_next_first_of_month(self) -> None:
        """'0 2 1 * *' from June 15 should give July 1st 02:00."""
        after = datetime(2025, 6, 15, 14, 0, 0)
        result = _next_cron_time("0 2 1 * *", after)
        assert result == datetime(2025, 7, 1, 2, 0)

    def test_next_weekday(self) -> None:
        """'0 9 * * 1' from Sunday should give next Monday 09:00."""
        # 2025-06-15 is Sunday
        after = datetime(2025, 6, 15, 10, 0, 0)
        result = _next_cron_time("0 9 * * 1", after)
        assert result == datetime(2025, 6, 16, 9, 0)

    def test_next_skips_current_minute(self) -> None:
        """Result should always be strictly after `after` (next whole minute)."""
        after = datetime(2025, 6, 15, 14, 30, 45)
        result = _next_cron_time("* * * * *", after)
        assert result == datetime(2025, 6, 15, 14, 31)

    def test_seconds_zeroed(self) -> None:
        """Result should have seconds and microseconds zeroed."""
        after = datetime(2025, 6, 15, 14, 30, 59, 999999)
        result = _next_cron_time("* * * * *", after)
        assert result.second == 0
        assert result.microsecond == 0


# ---------------------------------------------------------------------------
# SchedulerService.__init__
# ---------------------------------------------------------------------------


class TestSchedulerServiceInit:
    """Tests for SchedulerService construction."""

    def test_raises_when_schedule_disabled(self) -> None:
        """Should raise SchedulerError if schedule is not enabled."""
        project = _make_project(enabled=False)
        with pytest.raises(SchedulerError, match="not enabled"):
            SchedulerService(project, _mock_source(), _mock_sink())

    def test_raises_when_no_cron(self) -> None:
        """Should raise SchedulerError if cron expression is None."""
        project = ProjectModel(
            name="no_cron",
            schedule=ScheduleConfig(enabled=True, cron=None),
        )
        with pytest.raises(SchedulerError, match="No cron expression"):
            SchedulerService(project, _mock_source(), _mock_sink())

    def test_raises_on_invalid_cron(self) -> None:
        """Should raise SchedulerError if cron expression is invalid."""
        project = _make_project(cron="invalid cron expr here now")
        with pytest.raises(SchedulerError):
            SchedulerService(project, _mock_source(), _mock_sink())

    def test_valid_construction(self) -> None:
        """Should construct successfully with valid schedule config."""
        project = _make_project()
        svc = SchedulerService(project, _mock_source(), _mock_sink())
        assert svc.project.name == "test_project"

    def test_uses_timezone(self) -> None:
        """Should configure ZoneInfo from schedule timezone."""
        project = _make_project(timezone="Africa/Nairobi")
        svc = SchedulerService(project, _mock_source(), _mock_sink())
        assert svc._tz == ZoneInfo("Africa/Nairobi")

    def test_accepts_custom_tracker(self) -> None:
        """Should accept and store a custom ProgressTracker."""
        tracker = ProgressTracker()
        project = _make_project()
        svc = SchedulerService(project, _mock_source(), _mock_sink(), tracker=tracker)
        assert svc._tracker is tracker


# ---------------------------------------------------------------------------
# SchedulerService.stop
# ---------------------------------------------------------------------------


class TestSchedulerServiceStop:
    """Tests for SchedulerService.stop()."""

    def test_stop_sets_event(self) -> None:
        """stop() should set the internal stop event."""
        project = _make_project()
        svc = SchedulerService(project, _mock_source(), _mock_sink())
        assert not svc._stop_event.is_set()
        svc.stop()
        assert svc._stop_event.is_set()


# ---------------------------------------------------------------------------
# SchedulerService.next_run_time
# ---------------------------------------------------------------------------


class TestSchedulerServiceNextRunTime:
    """Tests for SchedulerService.next_run_time()."""

    def test_returns_datetime_with_timezone(self) -> None:
        """next_run_time() should return a timezone-aware datetime."""
        project = _make_project(cron="0 2 * * *", timezone="Africa/Nairobi")
        svc = SchedulerService(project, _mock_source(), _mock_sink())
        result = svc.next_run_time()
        assert result is not None
        assert result.tzinfo is not None

    def test_returns_future_time(self) -> None:
        """next_run_time() should return a time in the future."""
        project = _make_project(cron="* * * * *")
        svc = SchedulerService(project, _mock_source(), _mock_sink())
        result = svc.next_run_time()
        assert result is not None
        now = datetime.now(ZoneInfo("UTC"))
        assert result > now


# ---------------------------------------------------------------------------
# SchedulerService.run_once
# ---------------------------------------------------------------------------


class TestSchedulerServiceRunOnce:
    """Tests for SchedulerService.run_once()."""

    @patch("bani.application.scheduler.MigrationOrchestrator")
    def test_calls_orchestrator_execute(self, mock_orch_cls: MagicMock) -> None:
        """run_once() should create an orchestrator and call execute()."""
        mock_result = _make_migration_result()
        mock_orch = MagicMock()
        mock_orch.execute.return_value = mock_result
        mock_orch_cls.return_value = mock_orch

        project = _make_project()
        svc = SchedulerService(project, _mock_source(), _mock_sink())
        result = svc.run_once()

        assert result is mock_result
        mock_orch.execute.assert_called_once()

    @patch("bani.application.scheduler.MigrationOrchestrator")
    def test_retry_on_failure_then_success(self, mock_orch_cls: MagicMock) -> None:
        """run_once() should retry on failure up to max_retries."""
        mock_result = _make_migration_result()
        mock_orch = MagicMock()
        # Fail once, then succeed
        mock_orch.execute.side_effect = [
            RuntimeError("connection lost"),
            mock_result,
        ]
        mock_orch_cls.return_value = mock_orch

        project = _make_project(max_retries=2, retry_delay_seconds=0)
        svc = SchedulerService(project, _mock_source(), _mock_sink())
        result = svc.run_once()

        assert result is mock_result
        assert mock_orch.execute.call_count == 2

    @patch("bani.application.scheduler.MigrationOrchestrator")
    def test_max_retries_exceeded_raises(self, mock_orch_cls: MagicMock) -> None:
        """run_once() should raise SchedulerError when all retries fail."""
        mock_orch = MagicMock()
        mock_orch.execute.side_effect = RuntimeError("persistent failure")
        mock_orch_cls.return_value = mock_orch

        project = _make_project(max_retries=2, retry_delay_seconds=0)
        svc = SchedulerService(project, _mock_source(), _mock_sink())

        with pytest.raises(SchedulerError, match="3 attempt"):
            svc.run_once()

        # 1 initial + 2 retries = 3 calls
        assert mock_orch.execute.call_count == 3

    @patch("bani.application.scheduler.MigrationOrchestrator")
    def test_no_retries_raises_immediately(self, mock_orch_cls: MagicMock) -> None:
        """run_once() with max_retries=0 should raise after first failure."""
        mock_orch = MagicMock()
        mock_orch.execute.side_effect = RuntimeError("boom")
        mock_orch_cls.return_value = mock_orch

        project = _make_project(max_retries=0)
        svc = SchedulerService(project, _mock_source(), _mock_sink())

        with pytest.raises(SchedulerError, match="1 attempt"):
            svc.run_once()

        assert mock_orch.execute.call_count == 1

    @patch("bani.application.scheduler.MigrationOrchestrator")
    def test_run_once_passes_resume_when_checkpoint_exists(
        self, mock_orch_cls: MagicMock
    ) -> None:
        """run_once() should pass resume=True if a checkpoint exists."""
        mock_result = _make_migration_result()
        mock_orch = MagicMock()
        mock_orch.execute.return_value = mock_result
        mock_orch_cls.return_value = mock_orch

        project = _make_project()
        svc = SchedulerService(project, _mock_source(), _mock_sink())
        # Simulate existing checkpoint
        svc._checkpoint = MagicMock()
        svc._checkpoint.load.return_value = {"some": "data"}

        svc.run_once()

        mock_orch.execute.assert_called_once_with(resume=True)

    @patch("bani.application.scheduler.MigrationOrchestrator")
    def test_run_once_passes_no_resume_when_no_checkpoint(
        self, mock_orch_cls: MagicMock
    ) -> None:
        """run_once() should pass resume=False if no checkpoint exists."""
        mock_result = _make_migration_result()
        mock_orch = MagicMock()
        mock_orch.execute.return_value = mock_result
        mock_orch_cls.return_value = mock_orch

        project = _make_project()
        svc = SchedulerService(project, _mock_source(), _mock_sink())
        # Simulate no checkpoint
        svc._checkpoint = MagicMock()
        svc._checkpoint.load.return_value = None

        svc.run_once()

        mock_orch.execute.assert_called_once_with(resume=False)
