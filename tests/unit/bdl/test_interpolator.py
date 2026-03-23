"""Tests for BDL environment variable interpolator."""

from __future__ import annotations

import os

import pytest

from bani.bdl.interpolator import interpolate, interpolate_dict
from bani.domain.errors import ConfigurationError


class TestInterpolate:
    """Tests for interpolate function."""

    def test_interpolate_single_env_var(self) -> None:
        """Test interpolating a single environment variable."""
        os.environ["TEST_VAR"] = "test_value"
        result = interpolate("Value is ${env:TEST_VAR}")
        assert result == "Value is test_value"

    def test_interpolate_multiple_env_vars(self) -> None:
        """Test interpolating multiple environment variables."""
        os.environ["VAR1"] = "value1"
        os.environ["VAR2"] = "value2"
        result = interpolate("${env:VAR1} and ${env:VAR2}")
        assert result == "value1 and value2"

    def test_interpolate_no_vars(self) -> None:
        """Test text without environment variables."""
        result = interpolate("plain text")
        assert result == "plain text"

    def test_interpolate_missing_env_var(self) -> None:
        """Test that missing env var raises error."""
        with pytest.raises(ConfigurationError) as exc_info:
            interpolate("Value is ${env:NONEXISTENT_VAR}")
        assert "NONEXISTENT_VAR" in str(exc_info.value)

    def test_interpolate_partial_match(self) -> None:
        """Test that partial patterns are not interpolated."""
        result = interpolate("Text with $env:VAR or ${env VAR} not matching")
        assert result == "Text with $env:VAR or ${env VAR} not matching"


class TestInterpolateDict:
    """Tests for interpolate_dict function."""

    def test_interpolate_dict_string_values(self) -> None:
        """Test interpolating string values in a dictionary."""
        os.environ["DB_USER"] = "admin"
        data = {"username": "${env:DB_USER}", "host": "localhost"}
        result = interpolate_dict(data)
        assert result["username"] == "admin"
        assert result["host"] == "localhost"

    def test_interpolate_dict_nested(self) -> None:
        """Test interpolating nested dictionaries."""
        os.environ["DB_PASS"] = "secret"
        data = {
            "connection": {
                "password": "${env:DB_PASS}",
                "host": "localhost",
            }
        }
        result = interpolate_dict(data)
        assert result["connection"]["password"] == "secret"

    def test_interpolate_dict_list_values(self) -> None:
        """Test interpolating string values in lists."""
        os.environ["TAG1"] = "tag-one"
        os.environ["TAG2"] = "tag-two"
        data = {
            "tags": [
                "${env:TAG1}",
                "static",
                "${env:TAG2}",
            ]
        }
        result = interpolate_dict(data)
        assert result["tags"][0] == "tag-one"
        assert result["tags"][1] == "static"
        assert result["tags"][2] == "tag-two"

    def test_interpolate_dict_preserves_non_strings(self) -> None:
        """Test that non-string values are preserved."""
        data = {
            "number": 42,
            "boolean": True,
            "null_value": None,
        }
        result = interpolate_dict(data)
        assert result["number"] == 42
        assert result["boolean"] is True
        assert result["null_value"] is None
