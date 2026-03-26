"""Shared default-value translation for all sink connectors.

Each source database stores column defaults in its own syntax
(``now()``, ``CURRENT_TIMESTAMP``, ``GETDATE()``, ``SYSDATE``,
``nextval(...)``, ``gen_random_uuid()``, etc.).  When creating a
table on a *different* database, those defaults must be translated
to the target dialect or dropped entirely.

**Extension model**: each connector calls ``register_dialect_defaults``
at module level to declare its timestamp expression and temporal type
keywords.  The translation engine is generic — adding a sixth dialect
requires zero changes to this file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


# ---------------------------------------------------------------------------
# Dialect config dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DialectDefaultConfig:
    """Per-dialect configuration for default-value translation.

    Attributes:
        timestamp_expression: The SQL expression this dialect uses for
            "current timestamp" (e.g. ``"NOW()"``, ``"GETDATE()"``).
        temporal_keywords: Substrings that identify a column type as
            temporal.  Case-insensitive matching is used.
        reject_function_calls: If ``True``, any default containing ``(``
            that isn't a recognised timestamp will be dropped.  SQLite
            needs this because it only accepts constant expressions.
        extra_filter: Optional callable ``(raw_default) -> bool``.
            Return ``True`` to drop the default.  Use for dialect-specific
            non-portable markers beyond the shared set.
    """

    timestamp_expression: str = "CURRENT_TIMESTAMP"
    temporal_keywords: tuple[str, ...] = ()
    reject_function_calls: bool = False
    extra_filter: Callable[[str], bool] | None = None


# ---------------------------------------------------------------------------
# Shared constants (dialect-independent)
# ---------------------------------------------------------------------------

# Timestamp default expressions recognised across all dialects
_TIMESTAMP_DEFAULTS = frozenset({
    "now()",
    "current_timestamp",
    "current_timestamp()",
    "getdate()",
    "sysdate",
    "localtimestamp",
})

# Substrings that mark a default as non-portable (skip entirely)
_NON_PORTABLE_MARKERS = (
    "nextval(",
    "::",
    "gen_random_uuid()",
    "sys_guid()",
    "auto_increment",
    "generated",
    "newid()",
    "newsequentialid()",
)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_DIALECT_REGISTRY: dict[str, DialectDefaultConfig] = {}


def register_dialect_defaults(
    dialect: str, config: DialectDefaultConfig
) -> None:
    """Register a dialect's default-translation configuration.

    Called once per connector module at import time.  For example::

        from bani.connectors.default_translation import (
            DialectDefaultConfig, register_dialect_defaults,
        )

        register_dialect_defaults("mysql", DialectDefaultConfig(
            timestamp_expression="CURRENT_TIMESTAMP",
            temporal_keywords=("datetime", "timestamp", "date", "time"),
        ))

    Args:
        dialect: Short identifier (e.g. ``"mysql"``, ``"oracle"``).
        config: The configuration for this dialect.
    """
    _DIALECT_REGISTRY[dialect] = config


# ---------------------------------------------------------------------------
# Translation engine
# ---------------------------------------------------------------------------

def translate_default(
    raw_default: str,
    target_dialect: str,
    target_col_type: str,
) -> str | None:
    """Translate a column default for the target database.

    Args:
        raw_default: The raw default expression from introspection
            (e.g. ``"now()"``, ``"'pending'"``, ``"nextval(...)"``,
            ``"CURRENT_TIMESTAMP"``).
        target_dialect: Dialect name as registered via
            ``register_dialect_defaults``.
        target_col_type: The resolved DDL column type on the target
            (e.g. ``"BIGINT"``, ``"TEXT"``, ``"DATETIME"``).

    Returns:
        The translated default expression ready to embed in DDL
        (e.g. ``"CURRENT_TIMESTAMP"``), or ``None`` if the default
        should be skipped entirely.
    """
    config = _DIALECT_REGISTRY.get(target_dialect)

    default = str(raw_default).strip()
    dl = default.lower().strip()

    # 1. Non-portable defaults → always skip
    for marker in _NON_PORTABLE_MARKERS:
        if marker in dl:
            return None

    # 2. Dialect-specific extra filter
    if config is not None and config.extra_filter is not None:
        if config.extra_filter(dl):
            return None

    # 3. Timestamp functions → translate, but only if column is temporal
    if dl in _TIMESTAMP_DEFAULTS:
        if config is None:
            # Unknown dialect — can't safely translate
            return None
        col_upper = target_col_type.upper()
        if any(kw.upper() in col_upper for kw in config.temporal_keywords):
            return config.timestamp_expression
        # Non-temporal column (e.g. TEXT from SQLite) → skip
        return None

    # 4. Dialect rejects arbitrary function calls in DEFAULT?
    if config is not None and config.reject_function_calls and "(" in default:
        return None

    # 5. Everything else → pass through as-is
    return default
