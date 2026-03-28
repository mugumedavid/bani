"""In-process scheduler for running migrations on a cron schedule (Section 5.2).

Provides ``SchedulerService`` which parses cron expressions, calculates next
run times, and executes migrations via ``MigrationOrchestrator`` with retry
support. Also provides a self-contained cron parser (no external dependencies).
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from bani.application.checkpoint import CheckpointManager
from bani.application.orchestrator import MigrationOrchestrator, MigrationResult
from bani.application.progress import ProgressTracker
from bani.domain.errors import SchedulerError
from bani.domain.project import ProjectModel, ScheduleConfig

if TYPE_CHECKING:
    from bani.connectors.base import SinkConnector, SourceConnector

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cron parser — self-contained, no external dependencies
# ---------------------------------------------------------------------------

# Field indices for standard 5-field cron expressions
_MINUTE = 0
_HOUR = 1
_DAY_OF_MONTH = 2
_MONTH = 3
_DAY_OF_WEEK = 4

# Valid ranges for each field
_FIELD_RANGES: tuple[tuple[int, int], ...] = (
    (0, 59),  # minute
    (0, 23),  # hour
    (1, 31),  # day of month
    (1, 12),  # month
    (0, 6),  # day of week (0=Sunday)
)


def _parse_cron_field(field: str, min_val: int, max_val: int) -> set[int]:
    """Parse a single cron field into a set of matching integer values.

    Supports: ``*``, specific values (``30``), ranges (``1-5``), steps
    (``*/15``), range+step (``1-30/5``), and comma-separated lists
    (``1,15,30``).

    Args:
        field: The cron field string.
        min_val: Minimum valid value for this field.
        max_val: Maximum valid value for this field.

    Returns:
        Set of integer values that match the field expression.

    Raises:
        SchedulerError: If the field is invalid.
    """
    result: set[int] = set()

    for part in field.split(","):
        part = part.strip()
        if not part:
            raise SchedulerError(
                f"Empty element in cron field: {field!r}",
                field=field,
            )

        step: int | None = None
        if "/" in part:
            base, step_str = part.split("/", 1)
            try:
                step = int(step_str)
            except ValueError as exc:
                raise SchedulerError(
                    f"Invalid step value in cron field: {part!r}",
                    field=field,
                ) from exc
            if step < 1:
                raise SchedulerError(
                    f"Step must be >= 1 in cron field: {part!r}",
                    field=field,
                )
        else:
            base = part

        if base == "*":
            start, end = min_val, max_val
        elif "-" in base:
            range_parts = base.split("-", 1)
            try:
                start = int(range_parts[0])
                end = int(range_parts[1])
            except ValueError as exc:
                raise SchedulerError(
                    f"Invalid range in cron field: {part!r}",
                    field=field,
                ) from exc
        else:
            try:
                val = int(base)
            except ValueError as exc:
                raise SchedulerError(
                    f"Invalid value in cron field: {part!r}",
                    field=field,
                ) from exc
            if step is not None:
                start, end = val, max_val
            else:
                result.add(val)
                continue

        # Generate values with optional step
        effective_step = step or 1
        for v in range(start, end + 1, effective_step):
            if min_val <= v <= max_val:
                result.add(v)

    return result


def _parse_cron(cron_expr: str) -> tuple[set[int], ...]:
    """Parse a 5-field cron expression into sets of matching values.

    Args:
        cron_expr: Standard 5-field cron expression
            (minute hour day-of-month month day-of-week).

    Returns:
        Tuple of 5 sets: (minutes, hours, days_of_month, months, days_of_week).

    Raises:
        SchedulerError: If the expression is invalid.
    """
    fields = cron_expr.strip().split()
    if len(fields) != 5:
        raise SchedulerError(
            f"Cron expression must have exactly 5 fields, got {len(fields)}: "
            f"{cron_expr!r}",
            cron_expr=cron_expr,
        )

    parsed: list[set[int]] = []
    for i, field in enumerate(fields):
        min_val, max_val = _FIELD_RANGES[i]
        parsed.append(_parse_cron_field(field, min_val, max_val))

    return tuple(parsed)


def _matches_cron(cron_expr: str, dt: datetime) -> bool:
    """Check whether a datetime matches a cron expression.

    Args:
        cron_expr: Standard 5-field cron expression.
        dt: The datetime to check.

    Returns:
        True if the datetime matches the cron expression.
    """
    minutes, hours, days_of_month, months, days_of_week = _parse_cron(cron_expr)

    # Python weekday: Monday=0 .. Sunday=6
    # Cron weekday: Sunday=0 .. Saturday=6
    cron_dow = (dt.weekday() + 1) % 7

    return (
        dt.minute in minutes
        and dt.hour in hours
        and dt.day in days_of_month
        and dt.month in months
        and cron_dow in days_of_week
    )


def _next_cron_time(cron_expr: str, after: datetime) -> datetime:
    """Find the next datetime matching a cron expression after the given time.

    Iterates forward from ``after`` one minute at a time until a match is
    found. To avoid infinite loops, gives up after scanning 4 years
    (approximately 2,102,400 minutes).

    Args:
        cron_expr: Standard 5-field cron expression.
        after: The datetime to start searching from (exclusive).

    Returns:
        The next matching datetime (with seconds and microseconds zeroed).

    Raises:
        SchedulerError: If no match is found within the scan window.
    """
    # Pre-parse the cron expression to avoid re-parsing on every iteration
    minutes, hours, days_of_month, months, days_of_week = _parse_cron(cron_expr)

    # Start from the next whole minute after `after`
    candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)

    # Scan up to ~4 years of minutes
    max_iterations = 366 * 24 * 60 * 4
    for _ in range(max_iterations):
        cron_dow = (candidate.weekday() + 1) % 7
        if (
            candidate.minute in minutes
            and candidate.hour in hours
            and candidate.day in days_of_month
            and candidate.month in months
            and cron_dow in days_of_week
        ):
            return candidate
        candidate += timedelta(minutes=1)

    raise SchedulerError(
        f"No matching time found for cron expression {cron_expr!r} "
        f"within 4-year scan window",
        cron_expr=cron_expr,
    )


# ---------------------------------------------------------------------------
# SchedulerService
# ---------------------------------------------------------------------------


class SchedulerService:
    """Runs migrations on a cron schedule with retry support.

    Uses ``MigrationOrchestrator`` internally for each run. The scheduler
    is an in-process blocking loop that sleeps between runs, not a full
    job queue.

    Attributes:
        project: The migration project model.
    """

    def __init__(
        self,
        project: ProjectModel,
        source: SourceConnector,
        sink: SinkConnector,
        tracker: ProgressTracker | None = None,
    ) -> None:
        """Initialize the scheduler.

        Args:
            project: The migration project model (must have a schedule config).
            source: Source database connector.
            sink: Target database connector.
            tracker: Optional progress tracker for event emission.

        Raises:
            SchedulerError: If the project has no schedule configuration or
                the schedule is disabled.
        """
        schedule = project.schedule or ScheduleConfig()
        if not schedule.enabled:
            raise SchedulerError(
                f"Schedule is not enabled for project {project.name!r}",
                project_name=project.name,
            )
        if not schedule.cron:
            raise SchedulerError(
                f"No cron expression configured for project {project.name!r}",
                project_name=project.name,
            )

        # Validate the cron expression eagerly
        _parse_cron(schedule.cron)

        self.project = project
        self._source = source
        self._sink = sink
        self._tracker = tracker or ProgressTracker()
        self._schedule = schedule
        self._tz = ZoneInfo(schedule.timezone)
        self._stop_event = threading.Event()
        self._checkpoint = CheckpointManager()

    def start(self) -> None:
        """Start the scheduler. Blocks until stopped.

        Calculates the next run time from the cron expression, sleeps
        until then, executes the migration, and repeats. Checks the
        stop event periodically during sleep.
        """
        cron_expr = self._schedule.cron
        assert cron_expr is not None  # Validated in __init__

        logger.info(
            "Scheduler started for project %r (cron=%r, tz=%s)",
            self.project.name,
            cron_expr,
            self._schedule.timezone,
        )

        while not self._stop_event.is_set():
            now = datetime.now(self._tz)
            next_time = _next_cron_time(cron_expr, now)
            next_time = next_time.replace(tzinfo=self._tz)

            logger.info(
                "Next run for %r scheduled at %s",
                self.project.name,
                next_time.isoformat(),
            )

            # Sleep until next run time, checking stop event every second
            while not self._stop_event.is_set():
                now = datetime.now(self._tz)
                remaining = (next_time - now).total_seconds()
                if remaining <= 0:
                    break
                # Sleep in short intervals to allow clean shutdown
                self._stop_event.wait(timeout=min(remaining, 1.0))

            if self._stop_event.is_set():
                break

            # Execute the migration with retry logic
            logger.info("Starting scheduled run for project %r", self.project.name)
            try:
                self._run_with_retries()
            except Exception:
                logger.exception(
                    "Scheduled run failed for project %r after all retries",
                    self.project.name,
                )

        logger.info("Scheduler stopped for project %r", self.project.name)

    def stop(self) -> None:
        """Signal the scheduler to stop after the current run completes."""
        logger.info("Stop requested for scheduler of project %r", self.project.name)
        self._stop_event.set()

    def run_once(self) -> MigrationResult:
        """Execute a single migration run (useful for testing).

        Returns:
            MigrationResult with summary statistics.

        Raises:
            SchedulerError: If all retry attempts are exhausted.
        """
        return self._run_with_retries()

    def next_run_time(self) -> datetime | None:
        """Calculate the next run time from the cron expression.

        Returns:
            The next datetime matching the cron schedule in the configured
            timezone, or ``None`` if the cron expression is not set.
        """
        cron_expr = self._schedule.cron
        if not cron_expr:
            return None

        now = datetime.now(self._tz)
        next_time = _next_cron_time(cron_expr, now)
        return next_time.replace(tzinfo=self._tz)

    def _run_with_retries(self) -> MigrationResult:
        """Execute a migration with retry logic.

        On failure, retries up to ``max_retries`` times with
        ``retry_delay_seconds`` between attempts. Uses ``resume=True``
        if a checkpoint exists.

        Returns:
            MigrationResult from a successful run.

        Raises:
            SchedulerError: If all retry attempts are exhausted.
        """
        max_retries = self._schedule.max_retries
        retry_delay = self._schedule.retry_delay_seconds
        last_error: Exception | None = None

        for attempt in range(max_retries + 1):
            if attempt > 0:
                logger.info(
                    "Retry attempt %d/%d for project %r (delay=%ds)",
                    attempt,
                    max_retries,
                    self.project.name,
                    retry_delay,
                )
                # Wait before retrying, but respect stop event
                if self._stop_event.wait(timeout=retry_delay):
                    raise SchedulerError(
                        "Scheduler stopped during retry delay",
                        project_name=self.project.name,
                    )

            try:
                orchestrator = MigrationOrchestrator(
                    project=self.project,
                    source=self._source,
                    sink=self._sink,
                    tracker=self._tracker,
                    checkpoint=self._checkpoint,
                )

                # Use resume if a checkpoint exists
                resume = self._checkpoint.load(self.project.name) is not None

                result = orchestrator.execute(resume=resume)

                logger.info(
                    "Scheduled run completed for project %r: "
                    "%d tables, %d rows in %.1fs",
                    self.project.name,
                    result.tables_completed,
                    result.total_rows_written,
                    result.duration_seconds,
                )

                return result

            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Migration attempt %d failed for project %r: %s",
                    attempt + 1,
                    self.project.name,
                    exc,
                )

        raise SchedulerError(
            f"Migration failed after {max_retries + 1} attempt(s) for "
            f"project {self.project.name!r}: {last_error}",
            project_name=self.project.name,
            attempts=max_retries + 1,
        )
