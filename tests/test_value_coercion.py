"""Unit tests for the shared value coercion utility.

These tests ensure that every edge-case Python type returned by
``arrow_scalar.as_py()`` is correctly coerced for each database driver
before being passed to ``cursor.executemany``.

Run with:  pytest tests/test_value_coercion.py -v
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

import bani.connectors.mssql.data_writer
import bani.connectors.mysql.data_writer
import bani.connectors.oracle.data_writer
import bani.connectors.postgresql.data_writer

# Import data writers to trigger driver profile registration
import bani.connectors.sqlite.data_writer  # noqa: F401
from bani.connectors.value_coercion import coerce_for_binding

# ── Helpers ───────────────────────────────────────────────────────────

DRIVERS = ["sqlite3", "pymysql", "pymssql", "oracledb", "psycopg"]

_TEST_DECIMAL = Decimal("123.456789")
_TEST_UUID = UUID("12345678-1234-5678-1234-567812345678")
_TEST_DATE = date(2024, 6, 15)
_TEST_TIME = time(14, 30, 0)
_TEST_DATETIME = datetime(2024, 6, 15, 14, 30, 0)
_TEST_TIMEDELTA = timedelta(hours=2, minutes=30, seconds=45)
_TEST_TIMEDELTA_NEG = timedelta(hours=-1, minutes=-15)
_TEST_LIST = [1, 2, 3]
_TEST_DICT = {"key": "value"}
_TEST_BYTES = b"\x00\x01\x02\xff"


# ── Decimal coercion ─────────────────────────────────────────────────

class TestDecimalCoercion:
    """Decimal must be float for sqlite3, pymysql, pymssql; native elsewhere."""

    @pytest.mark.parametrize("driver", ["sqlite3", "pymysql", "pymssql"])
    def test_decimal_becomes_float(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_DECIMAL, driver)
        assert isinstance(result, float)
        assert abs(result - 123.456789) < 1e-6

    @pytest.mark.parametrize("driver", ["oracledb", "psycopg"])
    def test_decimal_stays_decimal(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_DECIMAL, driver)
        assert isinstance(result, Decimal)
        assert result == _TEST_DECIMAL


# ── UUID coercion ────────────────────────────────────────────────────

class TestUUIDCoercion:
    """UUID must be string for all drivers except psycopg."""

    @pytest.mark.parametrize("driver", ["sqlite3", "pymysql", "pymssql", "oracledb"])
    def test_uuid_becomes_string(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_UUID, driver)
        assert isinstance(result, str)
        assert result == str(_TEST_UUID)

    def test_uuid_stays_uuid_psycopg(self) -> None:
        result = coerce_for_binding(_TEST_UUID, "psycopg")
        assert isinstance(result, UUID)


# ── time coercion ────────────────────────────────────────────────────

class TestTimeCoercion:
    """time needs coercion for sqlite3, pymssql, oracledb."""

    def test_sqlite3_time_becomes_isoformat(self) -> None:
        result = coerce_for_binding(_TEST_TIME, "sqlite3")
        assert result == "14:30:00"

    def test_pymssql_time_becomes_isoformat(self) -> None:
        result = coerce_for_binding(_TEST_TIME, "pymssql")
        assert result == "14:30:00"

    def test_oracledb_time_becomes_datetime(self) -> None:
        result = coerce_for_binding(_TEST_TIME, "oracledb")
        assert isinstance(result, datetime)
        assert result.hour == 14
        assert result.minute == 30

    @pytest.mark.parametrize("driver", ["pymysql", "psycopg"])
    def test_time_stays_time(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_TIME, driver)
        assert isinstance(result, time)


# ── timedelta coercion ───────────────────────────────────────────────

class TestTimedeltaCoercion:
    """timedelta must become HH:MM:SS string for most drivers."""

    @pytest.mark.parametrize("driver", ["sqlite3", "pymysql", "pymssql", "oracledb"])
    def test_timedelta_becomes_hms_string(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_TIMEDELTA, driver)
        assert isinstance(result, str)
        assert result == "2:30:45"

    @pytest.mark.parametrize("driver", ["sqlite3", "pymysql", "pymssql", "oracledb"])
    def test_negative_timedelta(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_TIMEDELTA_NEG, driver)
        assert isinstance(result, str)
        assert result.startswith("-")

    def test_timedelta_stays_psycopg(self) -> None:
        result = coerce_for_binding(_TEST_TIMEDELTA, "psycopg")
        assert isinstance(result, timedelta)


# ── date coercion ────────────────────────────────────────────────────

class TestDateCoercion:
    """date needs isoformat for sqlite3; native elsewhere."""

    def test_sqlite3_date_becomes_isoformat(self) -> None:
        result = coerce_for_binding(_TEST_DATE, "sqlite3")
        assert result == "2024-06-15"

    @pytest.mark.parametrize("driver", ["pymysql", "pymssql", "oracledb", "psycopg"])
    def test_date_stays_date(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_DATE, driver)
        assert isinstance(result, date)


# ── datetime passthrough ─────────────────────────────────────────────

class TestDatetimePassthrough:
    """datetime should pass through for ALL drivers."""

    @pytest.mark.parametrize("driver", DRIVERS)
    def test_datetime_passes_through(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_DATETIME, driver)
        assert isinstance(result, datetime)
        assert result == _TEST_DATETIME


# ── list / dict coercion ─────────────────────────────────────────────

class TestCollectionCoercion:
    """list/dict must become JSON strings for drivers that don't handle them."""

    @pytest.mark.parametrize("driver", ["sqlite3", "pymysql", "pymssql", "oracledb"])
    def test_list_becomes_json(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_LIST, driver)
        assert isinstance(result, str)
        assert json.loads(result) == _TEST_LIST

    @pytest.mark.parametrize("driver", ["sqlite3", "pymysql", "pymssql", "oracledb"])
    def test_dict_becomes_json(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_DICT, driver)
        assert isinstance(result, str)
        assert json.loads(result) == _TEST_DICT

    @pytest.mark.parametrize("val", [_TEST_LIST, _TEST_DICT])
    def test_psycopg_passes_through(self, val: list | dict) -> None:
        result = coerce_for_binding(val, "psycopg")
        assert result is val


# ── bytes coercion ───────────────────────────────────────────────────

class TestBytesCoercion:
    """bytes should pass through for all drivers (all handle BLOB/BINARY)."""

    @pytest.mark.parametrize("driver", DRIVERS)
    def test_bytes_passes_through(self, driver: str) -> None:
        result = coerce_for_binding(_TEST_BYTES, driver)
        assert isinstance(result, bytes)


# ── Passthrough for basic types ──────────────────────────────────────

class TestBasicPassthrough:
    """int, float, str, bool, None should pass through unchanged."""

    @pytest.mark.parametrize("val", [42, 3.14, "hello", True, False])
    @pytest.mark.parametrize("driver", DRIVERS)
    def test_basic_passthrough(self, val: object, driver: str) -> None:
        result = coerce_for_binding(val, driver)
        assert result == val
        assert type(result) is type(val)
