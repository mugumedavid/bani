"""Shared fixtures and helpers for CLI tests."""

from __future__ import annotations

import re


def strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from text.

    CLI help output includes ANSI codes for formatting (bold, color)
    which interfere with substring matching in tests. This strips
    them so assertions work regardless of terminal environment.
    """
    return re.sub(r"\x1b\[[0-9;]*m", "", text)
