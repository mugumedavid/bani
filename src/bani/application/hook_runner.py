"""Pre/post hook execution system for Bani migrations.

Executes shell commands and SQL statements as pre- or post-migration hooks,
with timeout enforcement, variable substitution, and configurable failure
handling.
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass
from typing import Protocol

from bani.domain.errors import HookExecutionError
from bani.domain.project import HookConfig

logger = logging.getLogger(__name__)


class SqlExecutor(Protocol):
    """Protocol for objects that can execute SQL statements."""

    def execute_sql(self, sql: str) -> None: ...


@dataclass(frozen=True)
class HookResult:
    """Result of a single hook execution.

    Attributes:
        name: Hook name.
        phase: Hook phase (``"pre"`` or ``"post"``).
        success: Whether the hook completed without error.
        duration_seconds: Wall-clock execution time in seconds.
        output: Captured stdout (shell) or success message (SQL).
        error: Captured stderr (shell) or error message on failure.
    """

    name: str
    phase: str
    success: bool
    duration_seconds: float
    output: str = ""
    error: str = ""


class HookRunner:
    """Executes pre/post migration hooks.

    Supports two command types:

    - **Shell commands** (default): executed via ``subprocess.run(shell=True)``
      with timeout enforcement and stdout/stderr capture.
    - **SQL commands** (prefixed with ``sql:``): executed against a database
      connection via its ``execute_sql()`` method.

    Variable substitution is performed on commands before execution, replacing
    ``{project_name}``, ``{table_count}``, ``{source_dialect}``, and
    ``{target_dialect}`` from the provided context dict.

    Args:
        sql_executor: Optional callable/object with ``execute_sql(sql)`` method
            for running SQL hooks. If not provided, SQL hooks will fail.
    """

    def __init__(
        self,
        sql_executor: SqlExecutor | None = None,
    ) -> None:
        self._sql_executor = sql_executor

    def execute_hooks(
        self,
        hooks: tuple[HookConfig, ...],
        phase: str,
        context: dict[str, str] | None = None,
    ) -> list[HookResult]:
        """Run all hooks matching the given phase.

        Args:
            hooks: Tuple of hook configurations to filter and execute.
            phase: Phase to filter on (e.g. ``"pre"`` or ``"post"``).
            context: Variable substitution context. Keys like
                ``project_name`` are substituted for ``{project_name}``
                in the command string.

        Returns:
            List of ``HookResult`` for each executed hook.

        Raises:
            HookExecutionError: If a hook fails and its ``on_failure``
                is ``"abort"``.
        """
        ctx = context or {}
        results: list[HookResult] = []

        matching = [h for h in hooks if h.phase == phase]

        for hook in matching:
            command = self._substitute_variables(hook.command, ctx)
            logger.info(
                "Executing %s hook '%s': %s",
                phase,
                hook.name,
                command,
            )

            if command.startswith("sql:"):
                result = self._execute_sql_hook(hook, command, phase)
            else:
                result = self._execute_shell_hook(hook, command, phase)

            results.append(result)

            if not result.success:
                if hook.on_failure == "abort":
                    logger.error(
                        "Hook '%s' failed with on_failure='abort': %s",
                        hook.name,
                        result.error,
                    )
                    raise HookExecutionError(
                        f"Hook '{hook.name}' failed: {result.error}",
                        hook_name=hook.name,
                        phase=phase,
                        command=command,
                    )
                else:
                    logger.warning(
                        "Hook '%s' failed (on_failure='%s'): %s",
                        hook.name,
                        hook.on_failure,
                        result.error,
                    )

        return results

    def _substitute_variables(
        self,
        command: str,
        context: dict[str, str],
    ) -> str:
        """Replace {variable} placeholders in a command string.

        Only replaces variables present in the context dict. Unknown
        placeholders are left as-is.

        Args:
            command: Command string with ``{variable}`` placeholders.
            context: Mapping of variable names to values.

        Returns:
            Command with substitutions applied.
        """
        result = command
        for key, value in context.items():
            result = result.replace(f"{{{key}}}", value)
        return result

    def _execute_shell_hook(
        self,
        hook: HookConfig,
        command: str,
        phase: str,
    ) -> HookResult:
        """Execute a shell command hook.

        Args:
            hook: The hook configuration.
            command: The resolved command string.
            phase: The hook phase.

        Returns:
            HookResult with captured stdout/stderr.
        """
        start = time.monotonic()
        try:
            proc = subprocess.run(
                command,
                shell=True,  # noqa: S602
                capture_output=True,
                text=True,
                timeout=hook.timeout_seconds,
            )
            duration = time.monotonic() - start

            if proc.returncode != 0:
                return HookResult(
                    name=hook.name,
                    phase=phase,
                    success=False,
                    duration_seconds=duration,
                    output=proc.stdout,
                    error=proc.stderr or f"Exit code {proc.returncode}",
                )

            return HookResult(
                name=hook.name,
                phase=phase,
                success=True,
                duration_seconds=duration,
                output=proc.stdout,
            )

        except subprocess.TimeoutExpired:
            duration = time.monotonic() - start
            return HookResult(
                name=hook.name,
                phase=phase,
                success=False,
                duration_seconds=duration,
                error=f"Timed out after {hook.timeout_seconds}s",
            )
        except OSError as exc:
            duration = time.monotonic() - start
            return HookResult(
                name=hook.name,
                phase=phase,
                success=False,
                duration_seconds=duration,
                error=str(exc),
            )

    def _execute_sql_hook(
        self,
        hook: HookConfig,
        command: str,
        phase: str,
    ) -> HookResult:
        """Execute a SQL command hook.

        The ``sql:`` prefix is stripped before execution.

        Args:
            hook: The hook configuration.
            command: The resolved command string (including ``sql:`` prefix).
            phase: The hook phase.

        Returns:
            HookResult with success/error information.
        """
        sql = command[len("sql:") :]
        start = time.monotonic()

        if self._sql_executor is None:
            duration = time.monotonic() - start
            return HookResult(
                name=hook.name,
                phase=phase,
                success=False,
                duration_seconds=duration,
                error="No SQL executor configured for SQL hooks",
            )

        try:
            self._sql_executor.execute_sql(sql)
            duration = time.monotonic() - start
            return HookResult(
                name=hook.name,
                phase=phase,
                success=True,
                duration_seconds=duration,
                output=f"SQL executed: {sql}",
            )
        except Exception as exc:  # noqa: BLE001
            duration = time.monotonic() - start
            return HookResult(
                name=hook.name,
                phase=phase,
                success=False,
                duration_seconds=duration,
                error=str(exc),
            )
