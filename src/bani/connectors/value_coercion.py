"""Shared value coercion for all sink data writers.

When Arrow's ``value.as_py()`` returns a Python object, the target
database driver may not know how to bind it.  Each driver has a
different set of natively supported types.  This module provides a
single ``coerce_for_binding`` function that every data writer calls
so that edge-case types (``Decimal``, ``UUID``, ``time``, ``timedelta``,
``list``, ``dict``, ``bytes``) are handled in one place.

**Extension model**: each connector calls ``register_driver_profile``
at module level to declare what its driver handles natively.  The
coercion engine is generic — adding a sixth connector requires zero
changes to this file.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Callable
from uuid import UUID


# ---------------------------------------------------------------------------
# Driver profile dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DriverProfile:
    """Declares what Python types a database driver can bind natively.

    Set a capability to ``True`` if the driver handles that type out of
    the box; ``False`` if it needs coercion.  Any capability not listed
    defaults to ``True`` (pass-through).

    ``custom_coercions`` is an optional dict mapping a type name (same
    keys as the bool fields, without the ``_ok`` suffix) to a callable
    ``(value) -> coerced_value``.  Use it when the generic fallback
    (e.g. ``str(uuid)``) isn't right for your driver — for example,
    Oracle promotes ``time`` to a ``datetime`` instead of isoformat.
    """

    decimal: bool = True
    uuid: bool = True
    date: bool = True
    time: bool = True
    timedelta: bool = True
    list_ok: bool = True
    dict_ok: bool = True
    bytes: bool = True
    datetime: bool = True
    custom_coercions: tuple[tuple[str, Callable[[Any], Any]], ...] = ()


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_DRIVER_REGISTRY: dict[str, DriverProfile] = {}


def register_driver_profile(driver_name: str, profile: DriverProfile) -> None:
    """Register a driver's capability profile.

    Called once per connector module at import time.  For example::

        from bani.connectors.value_coercion import (
            DriverProfile, register_driver_profile,
        )

        register_driver_profile("pymysql", DriverProfile(
            decimal=False,
            uuid=False,
            timedelta=False,
            list=False,
            dict=False,
        ))

    Args:
        driver_name: Short identifier used in ``coerce_for_binding``
            (e.g. ``"sqlite3"``, ``"pymysql"``).
        profile: The capability profile for this driver.
    """
    _DRIVER_REGISTRY[driver_name] = profile


# ---------------------------------------------------------------------------
# Coercion engine
# ---------------------------------------------------------------------------

def coerce_for_binding(py_val: Any, driver: str) -> Any:
    """Coerce a Python value so the target DB driver can bind it.

    Call this on every non-None value returned by ``arrow_scalar.as_py()``
    before passing it to ``cursor.execute`` / ``cursor.executemany``.

    Args:
        py_val: The Python value from ``value.as_py()``.
        driver: The driver name passed to ``register_driver_profile``.

    Returns:
        A value the driver can bind without error.
    """
    profile = _DRIVER_REGISTRY.get(driver)
    if profile is None:
        # Unknown driver → pass through everything (safest default)
        return py_val

    customs = dict(profile.custom_coercions)

    # --- Boolean ---
    if isinstance(py_val, bool):
        # Oracle uses NUMBER(1) for booleans
        if driver == "oracledb":
            return 1 if py_val else 0
        return py_val

    # --- Decimal ---
    if isinstance(py_val, Decimal):
        if not profile.decimal:
            return customs.get("decimal", float)(py_val)
        return py_val

    # --- UUID ---
    if isinstance(py_val, UUID):
        if not profile.uuid:
            return customs.get("uuid", str)(py_val)
        return py_val

    # --- time (without date) ---
    if isinstance(py_val, time) and not isinstance(py_val, datetime):
        if not profile.time:
            coercer = customs.get("time", lambda v: v.isoformat())
            return coercer(py_val)
        return py_val

    # --- timedelta ---
    if isinstance(py_val, timedelta):
        if not profile.timedelta:
            coercer = customs.get("timedelta", _timedelta_to_hms)
            return coercer(py_val)
        return py_val

    # --- date (not datetime) ---
    if isinstance(py_val, date) and not isinstance(py_val, datetime):
        if not profile.date:
            return customs.get("date", lambda v: v.isoformat())(py_val)
        return py_val

    # --- list / dict (JSON-like) ---
    if isinstance(py_val, list):
        if not profile.list_ok:
            return customs.get("list", json.dumps)(py_val)
        return py_val
    if isinstance(py_val, dict):
        if not profile.dict_ok:
            return customs.get("dict", json.dumps)(py_val)
        return py_val

    # --- bytes ---
    if isinstance(py_val, bytes):
        if not profile.bytes:
            return customs.get("bytes", lambda v: v.hex())(py_val)
        return py_val

    # Everything else: pass through unchanged
    return py_val


# ---------------------------------------------------------------------------
# Default coercion helpers (used as fallbacks when no custom_coercion given)
# ---------------------------------------------------------------------------

def _timedelta_to_hms(val: timedelta) -> str:
    """Convert a timedelta to ``[-]H:MM:SS`` string."""
    total_secs = int(val.total_seconds())
    hours, rem = divmod(abs(total_secs), 3600)
    mins, secs = divmod(rem, 60)
    sign = "-" if total_secs < 0 else ""
    return f"{sign}{hours}:{mins:02d}:{secs:02d}"
