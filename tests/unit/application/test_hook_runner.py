"""Tests for the pre/post hook execution system."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from bani.application.hook_runner import HookResult, HookRunner
from bani.domain.errors import HookExecutionError
from bani.domain.project import HookConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def runner() -> HookRunner:
    """A HookRunner with no SQL executor."""
    return HookRunner()


@pytest.fixture()
def sql_executor() -> MagicMock:
    """A mock SQL executor with an execute_sql method."""
    mock = MagicMock()
    mock.execute_sql = MagicMock()
    return mock


@pytest.fixture()
def runner_with_sql(sql_executor: MagicMock) -> HookRunner:
    """A HookRunner wired with a mock SQL executor."""
    return HookRunner(sql_executor=sql_executor)


# ---------------------------------------------------------------------------
# HookResult dataclass
# ---------------------------------------------------------------------------


class TestHookResult:
    """Tests for HookResult dataclass."""

    def test_fields(self) -> None:
        result = HookResult(
            name="backup",
            phase="pre",
            success=True,
            duration_seconds=1.5,
            output="done",
            error="",
        )
        assert result.name == "backup"
        assert result.phase == "pre"
        assert result.success is True
        assert result.duration_seconds == 1.5
        assert result.output == "done"
        assert result.error == ""

    def test_defaults(self) -> None:
        result = HookResult(
            name="cleanup",
            phase="post",
            success=False,
            duration_seconds=0.1,
        )
        assert result.output == ""
        assert result.error == ""

    def test_frozen(self) -> None:
        result = HookResult(
            name="x", phase="pre", success=True, duration_seconds=0.0
        )
        with pytest.raises(AttributeError):
            result.name = "y"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Shell command execution
# ---------------------------------------------------------------------------


class TestShellExecution:
    """Tests for shell command hooks."""

    def test_success(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="echo-test",
            event="pre",
            command="echo hello",
            timeout_seconds=10,
            on_failure="abort",
        )
        results = runner.execute_hooks((hook,), phase="pre")

        assert len(results) == 1
        r = results[0]
        assert r.success is True
        assert r.name == "echo-test"
        assert r.phase == "pre"
        assert "hello" in r.output
        assert r.duration_seconds >= 0.0

    def test_failure_nonzero_exit(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="fail-cmd",
            event="pre",
            command="exit 1",
            timeout_seconds=10,
            on_failure="warn",
        )
        results = runner.execute_hooks((hook,), phase="pre")

        assert len(results) == 1
        assert results[0].success is False
        assert results[0].error != ""

    def test_timeout(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="slow-cmd",
            event="pre",
            command="sleep 30",
            timeout_seconds=1,
            on_failure="warn",
        )
        results = runner.execute_hooks((hook,), phase="pre")

        assert len(results) == 1
        assert results[0].success is False
        assert "Timed out" in results[0].error

    def test_captures_stderr(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="stderr-cmd",
            event="pre",
            command="echo oops >&2 && exit 1",
            timeout_seconds=10,
            on_failure="warn",
        )
        results = runner.execute_hooks((hook,), phase="pre")

        assert len(results) == 1
        assert results[0].success is False
        assert "oops" in results[0].error


# ---------------------------------------------------------------------------
# SQL command execution
# ---------------------------------------------------------------------------


class TestSqlExecution:
    """Tests for SQL command hooks (mocked)."""

    def test_sql_success(
        self,
        runner_with_sql: HookRunner,
        sql_executor: MagicMock,
    ) -> None:
        hook = HookConfig(
            name="vacuum",
            event="post",
            command="sql:VACUUM ANALYZE",
            timeout_seconds=60,
            on_failure="abort",
        )
        results = runner_with_sql.execute_hooks((hook,), phase="post")

        assert len(results) == 1
        r = results[0]
        assert r.success is True
        assert "VACUUM ANALYZE" in r.output
        sql_executor.execute_sql.assert_called_once_with("VACUUM ANALYZE")

    def test_sql_failure(
        self,
        runner_with_sql: HookRunner,
        sql_executor: MagicMock,
    ) -> None:
        sql_executor.execute_sql.side_effect = RuntimeError("connection lost")

        hook = HookConfig(
            name="bad-sql",
            event="post",
            command="sql:DROP TABLE important",
            timeout_seconds=60,
            on_failure="warn",
        )
        results = runner_with_sql.execute_hooks((hook,), phase="post")

        assert len(results) == 1
        assert results[0].success is False
        assert "connection lost" in results[0].error

    def test_sql_no_executor(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="orphan-sql",
            event="pre",
            command="sql:SELECT 1",
            timeout_seconds=10,
            on_failure="warn",
        )
        results = runner.execute_hooks((hook,), phase="pre")

        assert len(results) == 1
        assert results[0].success is False
        assert "No SQL executor" in results[0].error


# ---------------------------------------------------------------------------
# on_failure behavior
# ---------------------------------------------------------------------------


class TestOnFailure:
    """Tests for abort vs warn failure handling."""

    def test_abort_raises(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="critical",
            event="pre",
            command="exit 1",
            timeout_seconds=10,
            on_failure="abort",
        )
        with pytest.raises(HookExecutionError, match="critical"):
            runner.execute_hooks((hook,), phase="pre")

    def test_warn_continues(
        self,
        runner: HookRunner,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        hooks = (
            HookConfig(
                name="optional",
                event="pre",
                command="exit 1",
                timeout_seconds=10,
                on_failure="warn",
            ),
            HookConfig(
                name="second",
                event="pre",
                command="echo ok",
                timeout_seconds=10,
                on_failure="abort",
            ),
        )
        with caplog.at_level(logging.WARNING):
            results = runner.execute_hooks(hooks, phase="pre")

        assert len(results) == 2
        assert results[0].success is False
        assert results[1].success is True
        assert any("optional" in r.message for r in caplog.records)

    def test_abort_on_timeout(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="slow-critical",
            event="pre",
            command="sleep 30",
            timeout_seconds=1,
            on_failure="abort",
        )
        with pytest.raises(HookExecutionError, match="slow-critical"):
            runner.execute_hooks((hook,), phase="pre")

    def test_abort_includes_result_before_raising(
        self,
        runner: HookRunner,
    ) -> None:
        """The failing hook's result is appended before the exception."""
        hooks = (
            HookConfig(
                name="ok-hook",
                event="pre",
                command="echo fine",
                timeout_seconds=10,
                on_failure="abort",
            ),
            HookConfig(
                name="bad-hook",
                event="pre",
                command="exit 1",
                timeout_seconds=10,
                on_failure="abort",
            ),
        )
        with pytest.raises(HookExecutionError):
            runner.execute_hooks(hooks, phase="pre")


# ---------------------------------------------------------------------------
# Variable substitution
# ---------------------------------------------------------------------------


class TestVariableSubstitution:
    """Tests for {variable} replacement in commands."""

    def test_single_variable(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="greet",
            event="pre",
            command="echo {project_name}",
            timeout_seconds=10,
            on_failure="abort",
        )
        results = runner.execute_hooks(
            (hook,),
            phase="pre",
            context={"project_name": "myproject"},
        )

        assert results[0].success is True
        assert "myproject" in results[0].output

    def test_multiple_variables(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="info",
            event="pre",
            command="echo {source_dialect} to {target_dialect} tables={table_count}",
            timeout_seconds=10,
            on_failure="abort",
        )
        results = runner.execute_hooks(
            (hook,),
            phase="pre",
            context={
                "source_dialect": "mysql",
                "target_dialect": "postgresql",
                "table_count": "42",
            },
        )

        assert results[0].success is True
        assert "mysql" in results[0].output
        assert "postgresql" in results[0].output
        assert "42" in results[0].output

    def test_unknown_variables_left_asis(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="partial",
            event="pre",
            command="echo {project_name} {unknown_var}",
            timeout_seconds=10,
            on_failure="abort",
        )
        results = runner.execute_hooks(
            (hook,),
            phase="pre",
            context={"project_name": "test"},
        )
        assert results[0].success is True
        assert "test" in results[0].output
        assert "{unknown_var}" in results[0].output

    def test_no_context(self, runner: HookRunner) -> None:
        hook = HookConfig(
            name="nocontext",
            event="pre",
            command="echo hello",
            timeout_seconds=10,
            on_failure="abort",
        )
        results = runner.execute_hooks((hook,), phase="pre", context=None)
        assert results[0].success is True

    def test_sql_variable_substitution(
        self,
        runner_with_sql: HookRunner,
        sql_executor: MagicMock,
    ) -> None:
        hook = HookConfig(
            name="alter-hook",
            event="post",
            command="sql:ALTER TABLE {project_name}.foo DISABLE TRIGGER ALL",
            timeout_seconds=60,
            on_failure="abort",
        )
        runner_with_sql.execute_hooks(
            (hook,),
            phase="post",
            context={"project_name": "myschema"},
        )
        sql_executor.execute_sql.assert_called_once_with(
            "ALTER TABLE myschema.foo DISABLE TRIGGER ALL"
        )


# ---------------------------------------------------------------------------
# Phase filtering
# ---------------------------------------------------------------------------


class TestPhaseFiltering:
    """Tests for running only hooks that match the requested phase."""

    def test_only_pre_hooks_run(self, runner: HookRunner) -> None:
        hooks = (
            HookConfig(
                name="pre-hook",
                event="pre",
                command="echo pre",
                timeout_seconds=10,
                on_failure="abort",
            ),
            HookConfig(
                name="post-hook",
                event="post",
                command="echo post",
                timeout_seconds=10,
                on_failure="abort",
            ),
        )
        results = runner.execute_hooks(hooks, phase="pre")

        assert len(results) == 1
        assert results[0].name == "pre-hook"
        assert results[0].phase == "pre"

    def test_only_post_hooks_run(self, runner: HookRunner) -> None:
        hooks = (
            HookConfig(
                name="pre-hook",
                event="pre",
                command="echo pre",
                timeout_seconds=10,
                on_failure="abort",
            ),
            HookConfig(
                name="post-hook",
                event="post",
                command="echo post",
                timeout_seconds=10,
                on_failure="abort",
            ),
        )
        results = runner.execute_hooks(hooks, phase="post")

        assert len(results) == 1
        assert results[0].name == "post-hook"
        assert results[0].phase == "post"

    def test_no_matching_hooks(self, runner: HookRunner) -> None:
        hooks = (
            HookConfig(
                name="pre-hook",
                event="pre",
                command="echo pre",
                timeout_seconds=10,
                on_failure="abort",
            ),
        )
        results = runner.execute_hooks(hooks, phase="post")
        assert results == []

    def test_empty_hooks(self, runner: HookRunner) -> None:
        results = runner.execute_hooks((), phase="pre")
        assert results == []

    def test_multiple_hooks_same_phase(self, runner: HookRunner) -> None:
        hooks = (
            HookConfig(
                name="first",
                event="pre",
                command="echo 1",
                timeout_seconds=10,
                on_failure="abort",
            ),
            HookConfig(
                name="second",
                event="pre",
                command="echo 2",
                timeout_seconds=10,
                on_failure="abort",
            ),
        )
        results = runner.execute_hooks(hooks, phase="pre")

        assert len(results) == 2
        assert results[0].name == "first"
        assert results[1].name == "second"
