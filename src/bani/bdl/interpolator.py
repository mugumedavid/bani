"""Environment variable interpolation for BDL."""

from __future__ import annotations

import os
import re
from typing import Any

from bani.domain.errors import ConfigurationError


def interpolate(text: str) -> str:
    """Interpolate environment variables in text.

    Expands ${env:VAR_NAME} references from os.environ.

    Args:
        text: Text containing ${env:VAR_NAME} patterns.

    Returns:
        Text with environment variables interpolated.

    Raises:
        ConfigurationError: If a referenced environment variable is not set.
    """
    pattern = r"\$\{env:(\w+)\}"

    def replace_env_var(match: re.Match[str]) -> str:
        var_name = match.group(1)
        if var_name not in os.environ:
            raise ConfigurationError(
                f"Environment variable '{var_name}' is not set",
                variable=var_name,
            )
        return os.environ[var_name]

    return re.sub(pattern, replace_env_var, text)


def interpolate_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively interpolate environment variables in a dictionary.

    Args:
        data: Dictionary to interpolate.

    Returns:
        Dictionary with environment variables interpolated.

    Raises:
        ConfigurationError: If a referenced environment variable is not set.
    """
    result: dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, str):
            result[key] = interpolate(value)
        elif isinstance(value, dict):
            result[key] = interpolate_dict(value)
        elif isinstance(value, list):
            result[key] = [
                interpolate(item) if isinstance(item, str) else item for item in value
            ]
        else:
            result[key] = value
    return result
