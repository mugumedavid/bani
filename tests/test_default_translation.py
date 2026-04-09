"""Unit tests for the shared default translation utility.

These tests ensure that column defaults from any source database are
correctly translated (or dropped) for any target dialect, and that
temporal guards prevent timestamp functions from landing on non-temporal
column types.

Run with:  pytest tests/test_default_translation.py -v
"""

from __future__ import annotations

from typing import ClassVar

import pytest

import bani.connectors.mssql.connector
import bani.connectors.mysql.connector
import bani.connectors.oracle.connector
import bani.connectors.postgresql.connector

# Import connectors to trigger dialect registration
import bani.connectors.sqlite.connector  # noqa: F401
from bani.connectors.default_translation import translate_default

DIALECTS = ["postgresql", "mysql", "mssql", "oracle", "sqlite"]


# ── Timestamp defaults: accepted on temporal columns ─────────────────

class TestTimestampOnTemporalColumns:
    """Timestamp defaults should be translated when the column IS temporal."""

    _TEMPORAL_COL_TYPES: ClassVar[dict[str, str]] = {
        "postgresql": "TIMESTAMP WITHOUT TIME ZONE",
        "mysql": "DATETIME",
        "mssql": "DATETIME2",
        "oracle": "DATE",
        "sqlite": "DATETIME",
    }

    _EXPECTED_DEFAULTS: ClassVar[dict[str, str]] = {
        "postgresql": "NOW()",
        "mysql": "CURRENT_TIMESTAMP",
        "mssql": "GETDATE()",
        "oracle": "SYSDATE",
        "sqlite": "CURRENT_TIMESTAMP",
    }

    @pytest.mark.parametrize("source_default", [
        "now()", "CURRENT_TIMESTAMP", "current_timestamp()",
        "GETDATE()", "SYSDATE", "localtimestamp",
    ])
    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_timestamp_on_temporal_column(
        self, source_default: str, dialect: str
    ) -> None:
        col_type = self._TEMPORAL_COL_TYPES[dialect]
        result = translate_default(source_default, dialect, col_type)
        assert result == self._EXPECTED_DEFAULTS[dialect]


# ── Timestamp defaults: rejected on non-temporal columns ─────────────

class TestTimestampOnNonTemporalColumns:
    """Timestamp defaults should be SKIPPED when the column is NOT temporal."""

    @pytest.mark.parametrize("source_default", [
        "now()", "CURRENT_TIMESTAMP", "GETDATE()", "SYSDATE",
    ])
    @pytest.mark.parametrize("dialect,col_type", [
        ("mysql", "TEXT"),
        ("mysql", "VARCHAR(255)"),
        ("mssql", "NVARCHAR(MAX)"),
        ("oracle", "VARCHAR2(200)"),
        ("oracle", "CLOB"),
        ("sqlite", "TEXT"),
    ])
    def test_timestamp_on_non_temporal_column(
        self, source_default: str, dialect: str, col_type: str
    ) -> None:
        result = translate_default(source_default, dialect, col_type)
        assert result is None


# ── Non-portable defaults: always skipped ────────────────────────────

class TestNonPortableDefaults:
    """Source-specific defaults should always be dropped."""

    @pytest.mark.parametrize("default_val", [
        "nextval('seq'::regclass)",
        "gen_random_uuid()",
        "sys_guid()",
        "'hello'::text",
        "GENERATED ALWAYS",
        "NEWID()",
    ])
    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_non_portable_always_none(
        self, default_val: str, dialect: str
    ) -> None:
        result = translate_default(default_val, dialect, "TEXT")
        assert result is None


# ── Pass-through defaults ────────────────────────────────────────────

class TestPassthroughDefaults:
    """Normal defaults (literals, quoted strings) should pass through."""

    @pytest.mark.parametrize("default_val", [
        "'pending'",
        "0",
        "1",
        "42.5",
        "'hello world'",
        "NULL",
        "TRUE",
        "FALSE",
    ])
    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_literal_passthrough(
        self, default_val: str, dialect: str
    ) -> None:
        result = translate_default(default_val, dialect, "TEXT")
        assert result == default_val


# ── SQLite: function calls in defaults are rejected ──────────────────

class TestSQLiteFunctionReject:
    """SQLite cannot handle arbitrary function calls in DEFAULT clauses."""

    @pytest.mark.parametrize("default_val", [
        "random()",
        "upper('hello')",
        "substr('abc', 1, 2)",
    ])
    def test_sqlite_rejects_functions(self, default_val: str) -> None:
        result = translate_default(default_val, "sqlite", "TEXT")
        assert result is None

    @pytest.mark.parametrize("default_val", [
        "random()",
        "upper('hello')",
    ])
    @pytest.mark.parametrize("dialect", ["mysql", "mssql", "oracle", "postgresql"])
    def test_other_dialects_allow_functions(
        self, default_val: str, dialect: str
    ) -> None:
        result = translate_default(default_val, dialect, "TEXT")
        assert result == default_val
