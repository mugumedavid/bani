"""Tests for the schedule command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from bani.cli.app import app
from bani.domain.errors import SchedulerError


@pytest.fixture
def runner() -> CliRunner:
    """Create a Typer CLI runner."""
    return CliRunner()


@pytest.fixture
def sample_bdl_file(tmp_path: Path) -> Path:
    """Create a minimal BDL file for schedule testing."""
    bdl_file = tmp_path / "schedule_test.bdl"
    bdl_file.write_text("<bani></bani>")
    return bdl_file


def test_schedule_help(runner: CliRunner) -> None:
    """Test schedule command shows help."""
    from tests.unit.cli.conftest import strip_ansi

    result = runner.invoke(app, ["schedule", "--help"])
    assert result.exit_code == 0
    assert "--cron" in strip_ansi(result.stdout)


def test_schedule_missing_file(runner: CliRunner) -> None:
    """Test schedule command with missing file."""
    result = runner.invoke(app, ["schedule", "nonexistent.bdl", "--cron", "0 2 * * *"])
    assert result.exit_code != 0
    assert "not found" in result.stdout.lower()


def test_schedule_missing_file_json(runner: CliRunner) -> None:
    """Test schedule command with missing file in JSON mode."""
    result = runner.invoke(
        app,
        ["--output", "json", "schedule", "nonexistent.bdl", "--cron", "0 2 * * *"],
    )
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["status"] == "error"


@patch("bani.cli.commands.schedule.OSSchedulerBridge")
def test_schedule_register_human(
    mock_bridge: type, runner: CliRunner, sample_bdl_file: Path
) -> None:
    """Test schedule command in human mode."""
    mock_bridge.register.return_value = None

    result = runner.invoke(
        app,
        [
            "schedule",
            str(sample_bdl_file),
            "--cron",
            "0 2 * * *",
            "--timezone",
            "America/New_York",
        ],
    )
    assert result.exit_code == 0
    assert "scheduled" in result.stdout.lower() or "cron" in result.stdout.lower()


@patch("bani.cli.commands.schedule.OSSchedulerBridge")
def test_schedule_register_json(
    mock_bridge: type, runner: CliRunner, sample_bdl_file: Path
) -> None:
    """Test schedule command in JSON mode."""
    mock_bridge.register.return_value = None

    result = runner.invoke(
        app,
        [
            "--output",
            "json",
            "schedule",
            str(sample_bdl_file),
            "--cron",
            "0 2 * * *",
            "--timezone",
            "UTC",
        ],
    )
    assert result.exit_code == 0
    output = json.loads(result.stdout.strip())
    assert output["command"] == "schedule"
    assert output["status"] == "ok"
    assert output["cron"] == "0 2 * * *"
    assert output["timezone"] == "UTC"


@patch("bani.cli.commands.schedule.OSSchedulerBridge")
def test_schedule_register_error(
    mock_bridge: type, runner: CliRunner, sample_bdl_file: Path
) -> None:
    """Test schedule command when scheduler fails."""
    mock_bridge.register.side_effect = SchedulerError("crontab command not found")

    result = runner.invoke(
        app,
        ["schedule", str(sample_bdl_file), "--cron", "0 2 * * *"],
    )
    assert result.exit_code != 0


@patch("bani.cli.commands.schedule.OSSchedulerBridge")
def test_schedule_register_error_json(
    mock_bridge: type, runner: CliRunner, sample_bdl_file: Path
) -> None:
    """Test schedule command error in JSON mode."""
    mock_bridge.register.side_effect = SchedulerError("crontab not found")

    result = runner.invoke(
        app,
        [
            "--output",
            "json",
            "schedule",
            str(sample_bdl_file),
            "--cron",
            "0 2 * * *",
        ],
    )
    assert result.exit_code != 0
    output = json.loads(result.stdout.strip())
    assert output["status"] == "error"
