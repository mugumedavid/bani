"""Configuration loader supporting TOML and environment variable overrides.

Loads Bani configuration from ~/.config/bani/config.toml and merges with
environment variable overrides (BANI_BATCH_SIZE, BANI_PARALLEL_WORKERS, etc.).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]


@dataclass(frozen=True)
class BaniConfig:
    """Resolved Bani configuration from all sources."""

    batch_size: int
    parallel_workers: int
    memory_limit_mb: int
    log_level: str
    log_format: str


class ConfigLoader:
    """Loads and merges Bani configuration from multiple sources."""

    # Environment variable name prefix
    _ENV_PREFIX: ClassVar[str] = "BANI_"

    # Mapping from config key to env var name (without prefix)
    _ENV_KEYS: ClassVar[dict[str, str]] = {
        "batch_size": "BATCH_SIZE",
        "parallel_workers": "PARALLEL_WORKERS",
        "memory_limit_mb": "MEMORY_LIMIT_MB",
        "log_level": "LOG_LEVEL",
        "log_format": "LOG_FORMAT",
    }

    @classmethod
    def load(
        cls,
        config_path: str | Path | None = None,
        project_options: dict[str, Any] | None = None,
    ) -> BaniConfig:
        """Load configuration from TOML and environment variables.

        Priority (highest to lowest):
        1. Environment variables (BANI_*)
        2. Project options (from BDL)
        3. User config TOML (~/.config/bani/config.toml)
        4. Defaults

        Args:
            config_path: Optional path to config.toml file. If not provided,
                uses ~/.config/bani/config.toml.
            project_options: Optional dict of project-level options from BDL.

        Returns:
            Resolved BaniConfig dataclass.
        """
        # Start with defaults
        config: dict[str, Any] = {
            "batch_size": 100_000,
            "parallel_workers": 4,
            "memory_limit_mb": 2048,
            "log_level": "INFO",
            "log_format": "json",
        }

        # Merge user config from TOML
        if config_path is None:
            config_path = Path.home() / ".config" / "bani" / "config.toml"
        else:
            config_path = Path(config_path)

        if config_path.exists():
            try:
                with open(config_path, "rb") as f:
                    toml_data = tomllib.load(f)
                    config.update(toml_data)
            except Exception:
                # Silently ignore TOML parse errors; use defaults
                pass

        # Merge project options
        if project_options:
            config.update(project_options)

        # Merge environment variable overrides
        for key, env_suffix in cls._ENV_KEYS.items():
            env_var = cls._ENV_PREFIX + env_suffix
            if env_var in os.environ:
                value_str = os.environ[env_var]
                # Try to convert to appropriate type
                if key.startswith("parallel") or key == "batch_size":
                    try:
                        config[key] = int(value_str)
                    except ValueError:
                        pass
                elif key.endswith("_mb"):
                    try:
                        config[key] = int(value_str)
                    except ValueError:
                        pass
                else:
                    config[key] = value_str

        return BaniConfig(
            batch_size=int(config.get("batch_size", 100_000)),
            parallel_workers=int(config.get("parallel_workers", 4)),
            memory_limit_mb=int(config.get("memory_limit_mb", 2048)),
            log_level=str(config.get("log_level", "INFO")),
            log_format=str(config.get("log_format", "json")),
        )
