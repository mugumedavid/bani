#!/usr/bin/env python3
"""Build a Linux AppImage for Bani.

Usage:
    python pkg_build/build_appimage.py
"""

from __future__ import annotations

import os
import subprocess
import urllib.request
from pathlib import Path

from pkg_build.build_common import REPO_ROOT, assemble, get_version

APPIMAGETOOL_URL = (
    "https://github.com/AppImage/appimagetool/releases/download/"
    "continuous/appimagetool-x86_64.AppImage"
)


def _ensure_appimagetool() -> Path:
    """Download appimagetool if not present."""
    tool = REPO_ROOT / "build" / "appimagetool"
    if tool.exists():
        return tool

    tool.parent.mkdir(parents=True, exist_ok=True)
    print("Downloading appimagetool...")
    urllib.request.urlretrieve(APPIMAGETOOL_URL, tool)
    tool.chmod(0o755)
    return tool


def build_appimage(arch: str = "x86_64") -> Path:
    """Build a Linux AppImage.

    Args:
        arch: Target architecture (x86_64 or aarch64).

    Returns:
        Path to the built AppImage file.
    """
    version = get_version()
    import shutil

    # Assemble the installation
    install_dir = assemble("linux", arch)

    # Create AppDir structure
    appdir = REPO_ROOT / "build" / "Bani.AppDir"
    if appdir.exists():
        shutil.rmtree(appdir)
    appdir.mkdir(parents=True)

    # Copy installation into AppDir
    for item in install_dir.iterdir():
        dest = appdir / item.name
        if item.is_dir():
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)

    # Create AppRun script
    apprun = appdir / "AppRun"
    apprun.write_text(
        "#!/bin/sh\n"
        'SELF="$(readlink -f "$0")"\n'
        'APPDIR="$(dirname "$SELF")"\n'
        'exec "$APPDIR/python/bin/python3" -m bani.cli.app "$@"\n'
    )
    apprun.chmod(0o755)

    # Create .desktop file
    desktop = appdir / "bani.desktop"
    desktop.write_text(
        "[Desktop Entry]\n"
        "Type=Application\n"
        "Name=Bani\n"
        "Comment=Database migration engine\n"
        "Exec=bani\n"
        "Icon=bani\n"
        "Categories=Development;Database;\n"
        "Terminal=true\n"
    )

    # Create a simple icon (1x1 pixel PNG — placeholder)
    icon = appdir / "bani.png"
    if not icon.exists():
        # Minimal valid PNG (1x1 transparent pixel)
        import base64

        png_data = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4"
            "nGNgYPgPAAEDAQAIicLsAAAAASUVORK5CYII="
        )
        icon.write_bytes(png_data)

    # Build AppImage
    appimagetool = _ensure_appimagetool()
    output = REPO_ROOT / "build" / f"Bani-{version}-{arch}.AppImage"

    print("\nBuilding AppImage...")
    env = os.environ.copy()
    env["ARCH"] = arch
    subprocess.run(
        [str(appimagetool), str(appdir), str(output)],
        env=env,
        check=True,
    )

    print(f"Built: {output}")
    return output


if __name__ == "__main__":
    build_appimage()
