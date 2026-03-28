"""Settings routes (Section 20.3).

Application settings are stored in ``~/.bani/settings.json`` and
managed via GET/PUT endpoints.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Depends

from bani.ui.auth import verify_token
from bani.ui.models import SettingsModel

router = APIRouter(tags=["settings"], dependencies=[Depends(verify_token)])

_SETTINGS_PATH = Path("~/.bani/settings.json").expanduser()


def _load_settings() -> SettingsModel:
    """Load settings from disk, returning defaults if the file is missing.

    Returns:
        A SettingsModel with the persisted (or default) values.
    """
    if _SETTINGS_PATH.exists():
        try:
            data = json.loads(_SETTINGS_PATH.read_text(encoding="utf-8"))
            return SettingsModel(**data)
        except (json.JSONDecodeError, TypeError):
            pass
    return SettingsModel()


def _save_settings(settings: SettingsModel) -> None:
    """Persist settings to ``~/.bani/settings.json``.

    Args:
        settings: The settings to persist.
    """
    _SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _SETTINGS_PATH.write_text(
        json.dumps(settings.model_dump(), indent=2),
        encoding="utf-8",
    )


@router.get("/settings", response_model=SettingsModel)
async def get_settings() -> SettingsModel:
    """Retrieve current application settings.

    Returns:
        Current settings values.
    """
    return _load_settings()


@router.put("/settings", response_model=SettingsModel)
async def update_settings(body: SettingsModel) -> SettingsModel:
    """Update application settings.

    Args:
        body: New settings values.

    Returns:
        The updated settings.
    """
    _save_settings(body)
    return body
