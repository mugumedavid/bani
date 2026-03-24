"""Tests for configuration loading."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from bani.infra.config import BaniConfig, ConfigLoader


def test_config_loader_uses_defaults() -> None:
    """Test that ConfigLoader uses default values when no config exists."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "nonexistent.toml"
        config = ConfigLoader.load(config_path)

        assert config.batch_size == 100_000
        assert config.parallel_workers == 4
        assert config.memory_limit_mb == 2048
        assert config.log_level == "INFO"
        assert config.log_format == "json"


def test_config_loader_reads_toml_file() -> None:
    """Test that ConfigLoader reads and parses TOML config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.toml"
        config_path.write_text(
            """
batch_size = 50000
parallel_workers = 8
memory_limit_mb = 4096
log_level = "DEBUG"
log_format = "text"
"""
        )

        config = ConfigLoader.load(config_path)

        assert config.batch_size == 50_000
        assert config.parallel_workers == 8
        assert config.memory_limit_mb == 4096
        assert config.log_level == "DEBUG"
        assert config.log_format == "text"


def test_config_loader_env_vars_override_toml() -> None:
    """Test that environment variables override TOML settings."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.toml"
        config_path.write_text("batch_size = 50000")

        # Set env var
        os.environ["BANI_BATCH_SIZE"] = "75000"
        try:
            config = ConfigLoader.load(config_path)
            assert config.batch_size == 75_000
        finally:
            del os.environ["BANI_BATCH_SIZE"]


def test_config_loader_env_vars_override_defaults() -> None:
    """Test that environment variables override defaults."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "nonexistent.toml"

        os.environ["BANI_PARALLEL_WORKERS"] = "16"
        try:
            config = ConfigLoader.load(config_path)
            assert config.parallel_workers == 16
        finally:
            del os.environ["BANI_PARALLEL_WORKERS"]


def test_config_loader_project_options_override_toml() -> None:
    """Test that project options override TOML settings."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.toml"
        config_path.write_text("batch_size = 50000")

        project_opts = {"batch_size": 25_000}
        config = ConfigLoader.load(config_path, project_options=project_opts)

        assert config.batch_size == 25_000


def test_config_loader_priority_order() -> None:
    """Test the priority order: env vars > project opts > toml > defaults."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.toml"
        config_path.write_text("batch_size = 50000\nparallel_workers = 8")

        # TOML provides batch_size=50000, parallel_workers=8
        project_opts = {"parallel_workers": 6}
        # Project opts provide parallel_workers=6
        # (env var not set, so project_opts wins over TOML)

        config = ConfigLoader.load(config_path, project_options=project_opts)

        assert config.batch_size == 50_000  # From TOML
        assert config.parallel_workers == 6  # From project_opts (overrides TOML)

        # Now add env var
        os.environ["BANI_PARALLEL_WORKERS"] = "12"
        try:
            config = ConfigLoader.load(config_path, project_options=project_opts)
            assert config.parallel_workers == 12  # From env var (overrides everything)
        finally:
            del os.environ["BANI_PARALLEL_WORKERS"]


def test_config_loader_invalid_toml_uses_defaults() -> None:
    """Test that invalid TOML is gracefully ignored."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.toml"
        config_path.write_text("this is not valid toml ][{")

        # Should not raise, should use defaults
        config = ConfigLoader.load(config_path)

        assert config.batch_size == 100_000
        assert config.parallel_workers == 4


def test_config_loader_env_var_integer_conversion() -> None:
    """Test that integer env vars are correctly converted."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "nonexistent.toml"

        os.environ["BANI_BATCH_SIZE"] = "123456"
        os.environ["BANI_MEMORY_LIMIT_MB"] = "8192"
        try:
            config = ConfigLoader.load(config_path)
            assert config.batch_size == 123_456
            assert config.memory_limit_mb == 8192
            assert isinstance(config.batch_size, int)
            assert isinstance(config.memory_limit_mb, int)
        finally:
            del os.environ["BANI_BATCH_SIZE"]
            del os.environ["BANI_MEMORY_LIMIT_MB"]


def test_config_loader_invalid_env_var_ignored() -> None:
    """Test that invalid integer env vars are ignored."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "nonexistent.toml"

        os.environ["BANI_BATCH_SIZE"] = "not_a_number"
        try:
            config = ConfigLoader.load(config_path)
            # Should fall back to default
            assert config.batch_size == 100_000
        finally:
            del os.environ["BANI_BATCH_SIZE"]


def test_bani_config_is_frozen() -> None:
    """Test that BaniConfig is a frozen dataclass."""
    config = BaniConfig(
        batch_size=100_000,
        parallel_workers=4,
        memory_limit_mb=2048,
        log_level="INFO",
        log_format="json",
    )

    # Attempting to modify should raise
    with pytest.raises(AttributeError):
        config.batch_size = 50_000  # type: ignore[misc]


def test_config_loader_partial_toml() -> None:
    """Test that partially-filled TOML merges with defaults."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "config.toml"
        config_path.write_text('log_level = "WARNING"')

        config = ConfigLoader.load(config_path)

        assert config.log_level == "WARNING"  # From TOML
        assert config.batch_size == 100_000  # From defaults
        assert config.parallel_workers == 4  # From defaults
