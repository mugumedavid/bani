#!/usr/bin/env python3
"""Build a macOS .pkg installer for Bani (unsigned).

Usage:
    python pkg_build/build_macos.py
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from pkg_build.build_common import REPO_ROOT, assemble, get_version

# macOS .app Info.plist template
_INFO_PLIST = """\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>Bani</string>
    <key>CFBundleDisplayName</key>
    <string>Bani</string>
    <key>CFBundleIdentifier</key>
    <string>dev.bani.app</string>
    <key>CFBundleVersion</key>
    <string>{version}</string>
    <key>CFBundleShortVersionString</key>
    <string>{version}</string>
    <key>CFBundleExecutable</key>
    <string>bani-launcher</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon</string>
    <key>LSMinimumSystemVersion</key>
    <string>11.0</string>
    <key>LSUIElement</key>
    <true/>
    <key>NSHighResolutionCapable</key>
    <true/>
</dict>
</plist>
"""


def _create_app_bundle(payload: Path, opt_bani: Path) -> None:
    """Create a Bani.app bundle in the pkg payload."""
    version = get_version()

    app_dir = payload / "Applications" / "Bani.app"
    contents = app_dir / "Contents"
    macos = contents / "MacOS"
    resources = contents / "Resources"
    macos.mkdir(parents=True)
    resources.mkdir(parents=True)

    # Info.plist
    (contents / "Info.plist").write_text(_INFO_PLIST.format(version=version))

    # Launcher script
    launcher = macos / "bani-launcher"
    launcher.write_text(
        "#!/bin/sh\nexec /opt/bani/python/bin/python3 -m bani.desktop.menubar\n"
    )
    launcher.chmod(0o755)

    print(f"Created {app_dir}")


def build_pkg(arch: str = "aarch64") -> Path:
    """Build a macOS .pkg installer.

    Args:
        arch: Target architecture (aarch64 or x86_64).

    Returns:
        Path to the built .pkg file.
    """
    version = get_version()
    arch_label = "arm64" if arch == "aarch64" else "x86_64"

    # Assemble the installation
    install_dir = assemble("darwin", arch)

    # Create pkg payload structure
    import shutil

    payload = REPO_ROOT / "build" / "pkg-payload"
    if payload.exists():
        shutil.rmtree(payload)

    # Install runtime to /opt/bani/
    opt_bani = payload / "opt" / "bani"
    shutil.copytree(install_dir, opt_bani)

    # Install rumps + pyobjc for the menu bar app
    pip_bin = opt_bani / "python" / "bin" / "pip3"
    subprocess.run(
        [
            str(pip_bin),
            "install",
            "--no-cache-dir",
            "rumps>=0.4.0",
            "pyobjc-framework-Cocoa>=9.0",
        ],
        check=True,
    )

    # Create Bani.app bundle in /Applications/
    _create_app_bundle(payload, opt_bani)

    # Create symlink in /usr/local/bin/
    usr_bin = payload / "usr" / "local" / "bin"
    usr_bin.mkdir(parents=True)
    (usr_bin / "bani").symlink_to("/opt/bani/bin/bani")

    # Create post-install script
    scripts_dir = REPO_ROOT / "build" / "pkg-scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    postinstall = scripts_dir / "postinstall"
    postinstall.write_text(
        "#!/bin/sh\n"
        "chmod +x /opt/bani/bin/bani\n"
        "echo 'Bani installed to /opt/bani/'\n"
        "echo 'Run: bani --help'\n"
    )
    postinstall.chmod(0o755)

    # Build component package
    component_pkg = REPO_ROOT / "build" / "bani-component.pkg"
    print("\nBuilding component package...")
    subprocess.run(
        [
            "pkgbuild",
            "--root",
            str(payload),
            "--scripts",
            str(scripts_dir),
            "--identifier",
            "dev.bani.pkg",
            "--version",
            version,
            str(component_pkg),
        ],
        check=True,
    )

    # Build product package (wraps the component)
    output = REPO_ROOT / "build" / f"bani-{version}-macos-{arch_label}.pkg"
    print("Building product package...")
    subprocess.run(
        [
            "productbuild",
            "--package",
            str(component_pkg),
            str(output),
        ],
        check=True,
    )

    # Clean up component
    component_pkg.unlink(missing_ok=True)

    print(f"Built: {output}")
    return output


if __name__ == "__main__":
    build_pkg()
