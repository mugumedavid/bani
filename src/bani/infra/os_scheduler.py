"""OS scheduler bridge for registering Bani migrations with cron (Section 5.2).

On Linux and macOS, manages crontab entries tagged with ``# bani:<project>``
comments so they can be identified, listed, and removed. Windows Task
Scheduler support is a future extension.
"""

from __future__ import annotations

import logging
import platform
import shutil
import subprocess
import sys

from bani.domain.errors import SchedulerError

logger = logging.getLogger(__name__)

_BANI_TAG_PREFIX = "# bani:"
"""Comment prefix used to tag Bani-managed crontab entries."""


class OSSchedulerBridge:
    """Register/unregister Bani migrations with the OS scheduler.

    On Linux and macOS the bridge manages the current user's crontab.
    Each entry is tagged with a ``# bani:<project_name>`` comment so
    that ``unregister`` and ``list_registered`` can find and manage
    them without touching unrelated cron jobs.
    """

    @staticmethod
    def register(
        project_path: str,
        cron_expr: str,
        timezone: str = "UTC",
    ) -> None:
        """Register a migration with the OS scheduler.

        On Linux/macOS this adds a crontab entry. The entry runs
        ``bani run <project_path>`` on the specified cron schedule.

        Args:
            project_path: Path to the ``.bdl`` project file.
            cron_expr: Standard 5-field cron expression.
            timezone: IANA timezone (stored as a ``TZ=`` environment
                variable in the crontab entry).

        Raises:
            SchedulerError: If the platform is not supported or the
                crontab command fails.
        """
        if platform.system() == "Windows":
            raise SchedulerError(
                "Windows Task Scheduler integration is not yet supported",
                platform="Windows",
            )

        if not shutil.which("crontab"):
            raise SchedulerError(
                "crontab command not found on this system",
                platform=platform.system(),
            )

        # Derive project name from the file path (stem without extension)
        project_name = _project_name_from_path(project_path)
        tag = f"{_BANI_TAG_PREFIX}{project_name}"

        # Read current crontab
        current = _read_crontab()

        # Remove any existing entry for this project
        lines = _filter_out_project(current, project_name)

        # Build the new entry
        bani_cmd = _bani_command()
        entry_line = f"TZ={timezone} {cron_expr} {bani_cmd} run {project_path}"
        lines.append(tag)
        lines.append(entry_line)

        # Write back
        new_crontab = "\n".join(lines)
        if not new_crontab.endswith("\n"):
            new_crontab += "\n"

        _write_crontab(new_crontab)

        logger.info(
            "Registered project %r with OS scheduler: %s",
            project_name,
            cron_expr,
        )

    @staticmethod
    def unregister(project_name: str) -> None:
        """Remove a migration from the OS scheduler.

        Args:
            project_name: The project name (used in the ``# bani:``
                tag to identify the entry).

        Raises:
            SchedulerError: If the platform is not supported or the
                crontab command fails.
        """
        if platform.system() == "Windows":
            raise SchedulerError(
                "Windows Task Scheduler integration is not yet supported",
                platform="Windows",
            )

        current = _read_crontab()
        lines = _filter_out_project(current, project_name)

        new_crontab = "\n".join(lines)
        if new_crontab and not new_crontab.endswith("\n"):
            new_crontab += "\n"

        _write_crontab(new_crontab)

        logger.info("Unregistered project %r from OS scheduler", project_name)

    @staticmethod
    def list_registered() -> list[dict[str, str]]:
        """List all Bani migrations registered with the OS scheduler.

        Returns:
            List of dicts with ``project_name`` and ``cron_entry`` keys.

        Raises:
            SchedulerError: If the platform is not supported.
        """
        if platform.system() == "Windows":
            raise SchedulerError(
                "Windows Task Scheduler integration is not yet supported",
                platform="Windows",
            )

        current = _read_crontab()
        lines = current.splitlines()
        result: list[dict[str, str]] = []

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith(_BANI_TAG_PREFIX):
                project_name = line[len(_BANI_TAG_PREFIX) :].strip()
                cron_entry = ""
                if i + 1 < len(lines):
                    cron_entry = lines[i + 1].strip()
                result.append(
                    {
                        "project_name": project_name,
                        "cron_entry": cron_entry,
                    }
                )
                i += 2
            else:
                i += 1

        return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _project_name_from_path(project_path: str) -> str:
    """Extract a project name from a file path.

    Uses the file stem (name without extension) as the project name.

    Args:
        project_path: Path to a ``.bdl`` file.

    Returns:
        The project name string.
    """
    # Handle both forward and backslashes
    name = project_path.replace("\\", "/").rsplit("/", 1)[-1]
    if "." in name:
        name = name.rsplit(".", 1)[0]
    return name


def _bani_command() -> str:
    """Return the command to invoke Bani.

    Prefers ``bani`` if available on PATH; falls back to
    ``python -m bani``.

    Returns:
        Shell command string for invoking Bani.
    """
    if shutil.which("bani"):
        return "bani"
    return f"{sys.executable} -m bani"


def _read_crontab() -> str:
    """Read the current user's crontab.

    Returns:
        The crontab contents as a string. Returns an empty string if
        no crontab is installed.

    Raises:
        SchedulerError: If the crontab command fails unexpectedly.
    """
    try:
        result = subprocess.run(
            ["crontab", "-l"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            # "no crontab for user" is a common non-error
            stderr = result.stderr.lower()
            if "no crontab" in stderr:
                return ""
            raise SchedulerError(
                f"Failed to read crontab: {result.stderr.strip()}",
                returncode=result.returncode,
            )
        return result.stdout
    except FileNotFoundError as exc:
        raise SchedulerError("crontab command not found on this system") from exc


def _write_crontab(content: str) -> None:
    """Write new content to the current user's crontab.

    Args:
        content: The full crontab content to install.

    Raises:
        SchedulerError: If the crontab command fails.
    """
    try:
        result = subprocess.run(
            ["crontab", "-"],
            input=content,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise SchedulerError(
                f"Failed to write crontab: {result.stderr.strip()}",
                returncode=result.returncode,
            )
    except FileNotFoundError as exc:
        raise SchedulerError("crontab command not found on this system") from exc


def _filter_out_project(crontab_content: str, project_name: str) -> list[str]:
    """Remove all lines belonging to a given project from crontab content.

    Recognizes the two-line pattern: ``# bani:<project_name>`` followed
    by the cron entry line.

    Args:
        crontab_content: Current crontab content.
        project_name: Project name to remove.

    Returns:
        List of remaining lines.
    """
    tag = f"{_BANI_TAG_PREFIX}{project_name}"
    lines = crontab_content.splitlines()
    result: list[str] = []
    skip_next = False

    for line in lines:
        if skip_next:
            skip_next = False
            continue
        if line.strip() == tag:
            skip_next = True
            continue
        result.append(line)

    return result
